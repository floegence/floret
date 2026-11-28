package member

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"

	"github.com/floegence/floret/v7/storage/spi"
)

const BlockBytes = 64 << 10

var ErrContentIntegrity = errors.New("thread content integrity mismatch")

type immutableContent struct {
	ObjectSeq int64
	Bytes     int64
	SHA256    [sha256.Size]byte
}

func writeImmutableBlocks(ctx context.Context, tx *sql.Tx, database string, ref immutableContent, source io.Reader) error {
	if ctx == nil || tx == nil || source == nil || ref.ObjectSeq <= 0 || ref.Bytes < 0 {
		return spi.ErrInvalidArgument
	}
	var kind string
	var occupied bool
	if err := tx.QueryRowContext(ctx, `SELECT kind,EXISTS(SELECT 1 FROM `+database+`content_blocks WHERE object_seq=seq AND part=0) FROM `+database+`history_events WHERE seq=?`, ref.ObjectSeq).Scan(&kind, &occupied); err != nil {
		return err
	}
	if kind != "content_created" {
		return ErrContentIntegrity
	}
	if occupied {
		return spi.ErrConflict
	}
	insert, err := tx.PrepareContext(ctx, `INSERT INTO `+database+`content_blocks(object_seq,part,block_no,start_byte,bytes,sha256) VALUES(?,0,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer insert.Close()
	return visitImmutableBlocks(ctx, ref, source, func(offset int64, block []byte) error {
		checksum := sha256.Sum256(block)
		_, err := insert.ExecContext(ctx, ref.ObjectSeq, offset/BlockBytes, offset, block, checksum[:])
		return err
	})
}

func visitImmutableBlocks(ctx context.Context, ref immutableContent, source io.Reader, visit func(int64, []byte) error) error {
	digest := sha256.New()
	buffer := make([]byte, max(1, min(int64(BlockBytes), ref.Bytes)))
	for offset := int64(0); offset < ref.Bytes; {
		if err := ctx.Err(); err != nil {
			return err
		}
		block := buffer[:min(int64(len(buffer)), ref.Bytes-offset)]
		if _, err := io.ReadFull(source, block); err != nil {
			return errors.Join(ErrContentIntegrity, err)
		}
		if visit != nil {
			if err := visit(offset, block); err != nil {
				return err
			}
		}
		digest.Write(block)
		offset += int64(len(block))
	}
	if count, err := io.ReadFull(source, buffer[:1]); count != 0 || err != io.EOF || !bytes.Equal(digest.Sum(nil), ref.SHA256[:]) {
		return errors.Join(ErrContentIntegrity, err)
	}
	return ctx.Err()
}

func readImmutableRange(ctx context.Context, tx *sql.Tx, database string, ref immutableContent, start int64, limit int) ([]byte, error) {
	if ctx == nil || tx == nil || ref.ObjectSeq <= 0 || start < 0 || start > ref.Bytes || limit < 0 || limit > BlockBytes {
		return nil, spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	end := start + min(int64(limit), ref.Bytes-start)
	if start == end {
		return []byte{}, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT block_no,start_byte,bytes,sha256 FROM `+database+`content_blocks
WHERE object_seq=? AND part=0 AND block_no BETWEEN ? AND ? ORDER BY block_no`, ref.ObjectSeq, start/BlockBytes, (end-1)/BlockBytes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]byte, 0, end-start)
	for rows.Next() {
		var number, offset int64
		var block, digest []byte
		if err := rows.Scan(&number, &offset, &block, &digest); err != nil {
			return nil, err
		}
		position := start + int64(len(result))
		if number != position/BlockBytes || offset != number*BlockBytes || int64(len(block)) != min(int64(BlockBytes), ref.Bytes-offset) {
			return nil, ErrContentIntegrity
		}
		checksum := sha256.Sum256(block)
		if !bytes.Equal(checksum[:], digest) {
			return nil, ErrContentIntegrity
		}
		result = append(result, block[position-offset:min(int64(len(block)), end-offset)]...)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if int64(len(result)) != end-start {
		return nil, ErrContentIntegrity
	}
	return result, nil
}
