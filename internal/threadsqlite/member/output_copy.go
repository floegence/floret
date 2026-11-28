package member

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"slices"
	"unicode/utf8"

	"github.com/floegence/floret/v7/storage/spi"
)

// InstallOutput imports reachable boundaries in prefix order, writing each
// physical chunk once. Only the last boundary can remain open. A failed proof
// aborts the enclosing member transaction, including any copied chunks.
func (journal *Journal) InstallOutput(stream string, at int64, boundaries []OutputBoundary, read func(int64) (OutputBlock, error)) ([]Output, error) {
	if len(boundaries) == 0 || read == nil {
		return nil, spi.ErrInvalidArgument
	}
	output, err := journal.createOutputObject(stream, at)
	if err != nil {
		return nil, err
	}
	digest := sha256.New()
	result := make([]Output, 0, len(boundaries))
	for index, boundary := range boundaries {
		if boundary.Blocks < output.Blocks || boundary.Retained < output.Retained || boundary.Observed < output.Observed || !boundary.Sealed && index != len(boundaries)-1 {
			return nil, ErrContentIntegrity
		}
		for output.Blocks < boundary.Blocks {
			if err := journal.ctx.Err(); err != nil {
				return nil, err
			}
			block, err := read(output.Blocks)
			if err != nil {
				return nil, err
			}
			if block.Number != output.Blocks || block.Start != output.Retained || len(block.Bytes) == 0 || len(block.Bytes) > BlockBytes || int64(len(block.Bytes)) > boundary.Retained-block.Start || !utf8.Valid(block.Bytes) {
				return nil, ErrContentIntegrity
			}
			if err := journal.insertOutputBlock(output, block.Bytes); err != nil {
				return nil, err
			}
			digest.Write(block.Bytes)
			output.Blocks++
			output.Retained += int64(len(block.Bytes))
		}
		if output.Retained != boundary.Retained || hex.EncodeToString(digest.Sum(nil)) != boundary.Digest {
			return nil, ErrContentIntegrity
		}
		output.OutputBoundary = boundary
		if err := output.saveHash(digest); err != nil {
			return nil, err
		}
		if boundary.HashState != nil && !bytes.Equal(boundary.HashState, output.HashState) {
			return nil, ErrContentIntegrity
		}
		if _, err := output.hash(); err != nil {
			return nil, err
		}
		output, err = journal.insertOutputBoundary(output, at)
		if err != nil {
			return nil, err
		}
		result = append(result, output)
	}
	return result, nil
}

func (journal *Journal) ForkOutput(target *Journal, stream string, pins []int64, at int64) ([]Output, error) {
	if target == nil || target.thread == journal.thread || len(pins) == 0 {
		return nil, spi.ErrInvalidArgument
	}
	bySeq := map[int64]Output{}
	ordered := make([]int64, 0, len(pins))
	selected := make([]int64, len(pins))
	for index, pin := range pins {
		output, err := journal.OutputAt(stream, pin)
		if err != nil {
			return nil, err
		}
		if !output.Sealed && output.Reason == "" {
			output.Reason = "interrupted"
		}
		output.Sealed = true
		selected[index] = output.BoundarySeq
		if _, exists := bySeq[output.BoundarySeq]; !exists {
			bySeq[output.BoundarySeq] = output
			ordered = append(ordered, output.BoundarySeq)
		}
	}
	slices.Sort(ordered)
	boundaries := make([]OutputBoundary, len(ordered))
	for index, seq := range ordered {
		boundaries[index] = bySeq[seq].OutputBoundary
	}
	longest := bySeq[ordered[len(ordered)-1]]
	copied, err := target.InstallOutput(stream, at, boundaries, func(number int64) (OutputBlock, error) {
		return journal.OutputBlock(longest, number)
	})
	if err != nil {
		return nil, err
	}
	for index, seq := range ordered {
		bySeq[seq] = copied[index]
	}
	result := make([]Output, len(selected))
	for index, seq := range selected {
		result[index] = bySeq[seq]
	}
	return result, nil
}

func (journal *Journal) BlockAtOffset(output Output, offset int64) (OutputBlock, error) {
	if offset < 0 || offset >= output.Retained {
		return OutputBlock{}, spi.ErrInvalidArgument
	}
	var number int64
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT block_no FROM `+journal.database+`content_blocks WHERE object_seq=? AND part=0 AND start_byte<=? ORDER BY start_byte DESC LIMIT 1`, output.ObjectSeq, offset).Scan(&number)
	if errors.Is(err, sql.ErrNoRows) || err == nil && number >= output.Blocks {
		return OutputBlock{}, ErrContentIntegrity
	}
	if err != nil {
		return OutputBlock{}, err
	}
	block, err := journal.OutputBlock(output, number)
	if err == nil && (offset < block.Start || offset-block.Start >= int64(len(block.Bytes))) {
		return OutputBlock{}, ErrContentIntegrity
	}
	return block, err
}
