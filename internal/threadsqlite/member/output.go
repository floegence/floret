package member

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"math"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

type OutputBoundary struct {
	Observed  int64  `json:"observed_bytes"`
	Retained  int64  `json:"retained_bytes"`
	Blocks    int64  `json:"blocks"`
	Digest    string `json:"digest"`
	Sealed    bool   `json:"sealed"`
	Reason    string `json:"reason,omitempty"`
	HashState []byte `json:"hash_state"`
}

type Output struct {
	ObjectSeq   int64
	BoundarySeq int64
	OutputBoundary
}

type OutputBlock struct {
	Number int64
	Start  int64
	Bytes  []byte
}

func validRetentionReason(reason string) bool {
	switch reason {
	case "", "output_limit", "storage_limit", "storage_error", "capture_error", "cancelled", "interrupted":
		return true
	}
	return false
}

func (boundary OutputBoundary) hash() (hash.Hash, error) {
	if boundary.Retained < 0 || boundary.Observed < boundary.Retained || boundary.Blocks < 0 || boundary.Blocks > boundary.Retained || (boundary.Blocks == 0) != (boundary.Retained == 0) || !validRetentionReason(boundary.Reason) || boundary.Reason == "" && boundary.Observed != boundary.Retained {
		return nil, ErrContentIntegrity
	}
	digest := sha256.New()
	if err := digest.(encoding.BinaryUnmarshaler).UnmarshalBinary(boundary.HashState); err != nil || hex.EncodeToString(digest.Sum(nil)) != boundary.Digest {
		return nil, errors.Join(ErrContentIntegrity, err)
	}
	return digest, nil
}

func (boundary *OutputBoundary) saveHash(digest hash.Hash) error {
	state, err := digest.(encoding.BinaryMarshaler).MarshalBinary()
	if err == nil {
		boundary.HashState, boundary.Digest = state, hex.EncodeToString(digest.Sum(nil))
	}
	return err
}

func (journal *Journal) insertOutputBoundary(output Output, at int64) (Output, error) {
	revision, err := journal.writeRevision()
	if err != nil {
		return Output{}, err
	}
	payload, err := json.Marshal(output.OutputBoundary)
	if err != nil {
		return Output{}, err
	}
	err = journal.tx.QueryRowContext(journal.ctx, `INSERT INTO `+journal.database+`history_events(event_id,commit_rev,kind,schema_ver,at_ns,object_seq,payload) VALUES(?,?,'output_boundary',1,?,?,?) RETURNING seq`, rand.Text(), revision, at, output.ObjectSeq, payload).Scan(&output.BoundarySeq)
	return output, err
}

func (journal *Journal) createOutputObject(stream string, at int64) (Output, error) {
	if stream == "" {
		return Output{}, spi.ErrInvalidArgument
	}
	revision, err := journal.writeRevision()
	if err != nil {
		return Output{}, err
	}
	var output Output
	err = journal.tx.QueryRowContext(journal.ctx, `INSERT INTO `+journal.database+`history_events(event_id,commit_rev,kind,schema_ver,at_ns,scope_key,request_key,payload) VALUES(?,?,'output_created',1,?,'output',?,?) RETURNING seq`, rand.Text(), revision, at, stream, []byte(`{"media_type":"text/plain;charset=utf-8"}`)).Scan(&output.ObjectSeq)
	if err != nil {
		return Output{}, err
	}
	if err := output.saveHash(sha256.New()); err != nil {
		return Output{}, err
	}
	return output, nil
}

func (journal *Journal) CreateOutput(stream string, at int64) (Output, error) {
	output, err := journal.createOutputObject(stream, at)
	if err != nil {
		return Output{}, err
	}
	return journal.insertOutputBoundary(output, at)
}

// OutputAt selects the last committed boundary, or the exact boundary already
// pinned by an inherited reference. No message timestamp participates.
func (journal *Journal) OutputAt(stream string, pin int64) (Output, error) {
	if stream == "" || pin < 0 {
		return Output{}, spi.ErrInvalidArgument
	}
	events := journal.database + "history_events"
	query := `SELECT edge.seq,edge.object_seq,edge.schema_ver,edge.payload FROM ` + events + ` edge WHERE edge.kind='output_boundary' AND edge.object_seq=(SELECT seq FROM ` + events + ` WHERE scope_key='output' AND request_key=? AND kind='output_created' AND schema_ver=1)`
	args := []any{stream}
	if pin == 0 {
		query += ` ORDER BY edge.commit_rev DESC,edge.seq DESC LIMIT 1`
	} else {
		query += ` AND edge.seq=?`
		args = append(args, pin)
	}
	var output Output
	var version int
	var raw []byte
	err := journal.tx.QueryRowContext(journal.ctx, query, args...).Scan(&output.BoundarySeq, &output.ObjectSeq, &version, &raw)
	if err != nil {
		return Output{}, err
	}
	if version != 1 {
		return Output{}, ErrContentIntegrity
	}
	if err := storagecodec.DecodeJSON(raw, &output.OutputBoundary); err != nil {
		return Output{}, errors.Join(ErrContentIntegrity, err)
	}
	_, err = output.hash()
	return output, err
}

// AppendOutput is called after the domain owner has checked the invocation and
// reserved its quota in the same transaction. A retention failure still advances
// the observed watermark but can never create a hole in the retained prefix.
func (journal *Journal) AppendOutput(stream string, expected, at int64, text []byte, reason string) (Output, error) {
	if journal.readOnly || expected < 0 || len(text) > BlockBytes || !utf8.Valid(text) || !validRetentionReason(reason) {
		return Output{}, spi.ErrInvalidArgument
	}
	output, err := journal.OutputAt(stream, 0)
	if err != nil {
		return Output{}, err
	}
	if output.Observed != expected || output.Sealed || expected > math.MaxInt64-int64(len(text)) {
		return Output{}, spi.ErrConflict
	}
	if len(text) == 0 && (reason == "" || reason == output.Reason) {
		return output, nil
	}
	if output.Reason == "" {
		output.Reason = reason
	}
	if output.Reason == "" {
		digest, err := output.hash()
		if err != nil {
			return Output{}, err
		}
		if err := journal.insertOutputBlock(output, text); err != nil {
			return Output{}, err
		}
		digest.Write(text)
		if err := output.saveHash(digest); err != nil {
			return Output{}, err
		}
		output.Blocks++
		output.Retained += int64(len(text))
	}
	output.Observed += int64(len(text))
	return journal.insertOutputBoundary(output, at)
}

func (journal *Journal) insertOutputBlock(output Output, text []byte) error {
	digest := sha256.Sum256(text)
	_, err := journal.tx.ExecContext(journal.ctx, `INSERT INTO `+journal.database+`content_blocks(object_seq,part,block_no,start_byte,bytes,sha256) VALUES(?,0,?,?,?,?)`, output.ObjectSeq, output.Blocks, output.Retained, text, digest[:])
	return err
}

func (journal *Journal) SealOutput(stream string, at int64, reason string) (Output, error) {
	if !validRetentionReason(reason) {
		return Output{}, spi.ErrInvalidArgument
	}
	output, err := journal.OutputAt(stream, 0)
	if err != nil || output.Sealed {
		return output, err
	}
	output.Sealed = true
	if output.Reason == "" {
		output.Reason = reason
	}
	return journal.insertOutputBoundary(output, at)
}

func (journal *Journal) OutputBlock(output Output, number int64) (OutputBlock, error) {
	if number < 0 || number >= output.Blocks {
		return OutputBlock{}, spi.ErrInvalidArgument
	}
	block := OutputBlock{Number: number}
	var checksum []byte
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT start_byte,bytes,sha256 FROM `+journal.database+`content_blocks WHERE object_seq=? AND part=0 AND block_no=?`, output.ObjectSeq, number).Scan(&block.Start, &block.Bytes, &checksum)
	if errors.Is(err, sql.ErrNoRows) {
		return OutputBlock{}, ErrContentIntegrity
	}
	if err != nil {
		return OutputBlock{}, err
	}
	digest := sha256.Sum256(block.Bytes)
	if !bytes.Equal(digest[:], checksum) || !utf8.Valid(block.Bytes) || block.Start < 0 || block.Start > output.Retained || int64(len(block.Bytes)) > output.Retained-block.Start || (number == 0) != (block.Start == 0) || (number+1 == output.Blocks) != (int64(len(block.Bytes)) == output.Retained-block.Start) {
		return OutputBlock{}, ErrContentIntegrity
	}
	return block, nil
}
