package member

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

func (journal *Journal) StoreTodo(at int64, raw []byte) error {
	return journal.appendCheckpoint("todo_updated", at, raw)
}

func (journal *Journal) StoreContinuation(at int64, raw []byte) error {
	if err := journal.ClearContinuation(); err != nil {
		return err
	}
	return journal.appendCheckpoint("provider_continuation", at, raw)
}

func (journal *Journal) ClearContinuation() error {
	if journal.readOnly {
		return spi.ErrInvalidArgument
	}
	ref, _, found, err := journal.checkpoint("provider_continuation")
	if err != nil || !found {
		return err
	}
	if _, err := journal.writeRevision(); err != nil {
		return err
	}
	if _, err := journal.tx.ExecContext(journal.ctx, `DELETE FROM `+journal.database+`history_events WHERE kind='provider_continuation'`); err != nil {
		return err
	}
	if ref != nil {
		return journal.RemoveBody("provider_continuation", *ref)
	}
	return nil
}

func (journal *Journal) appendCheckpoint(kind string, at int64, raw []byte) error {
	if journal.readOnly || !json.Valid(raw) {
		return spi.ErrInvalidArgument
	}
	var object any
	payload := raw
	if len(raw) > 16384 {
		ref, err := journal.PutBody(kind, at, raw)
		if err != nil {
			return err
		}
		object = ref.Boundary
		payload, err = json.Marshal(ref)
		if err != nil {
			return err
		}
	}
	rev, err := journal.writeRevision()
	if err != nil {
		return err
	}
	_, err = journal.tx.ExecContext(journal.ctx, `INSERT INTO `+journal.database+`history_events(event_id,commit_rev,kind,schema_ver,at_ns,object_seq,payload) VALUES(?,?,?,1,?,?,?)`, rand.Text(), rev, kind, at, object, payload)
	return err
}

func (journal *Journal) Todo() ([]byte, bool, error) {
	return journal.checkpointBytes("todo_updated")
}

func (journal *Journal) Continuation() ([]byte, bool, error) {
	return journal.checkpointBytes("provider_continuation")
}

func (journal *Journal) checkpoint(kind string) (*BodyRef, []byte, bool, error) {
	var raw []byte
	var boundary sql.NullInt64
	var version int
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT schema_ver,object_seq,payload FROM `+journal.database+`history_events WHERE kind=? ORDER BY seq DESC LIMIT 1`, kind).Scan(&version, &boundary, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false, nil
	}
	if err != nil || version != 1 {
		return nil, nil, false, errors.Join(ErrContentIntegrity, err)
	}
	if !boundary.Valid {
		return nil, raw, true, nil
	}
	var ref BodyRef
	if err := storagecodec.DecodeJSON(raw, &ref); err != nil || ref.Boundary != boundary.Int64 {
		return nil, nil, false, errors.Join(ErrContentIntegrity, err)
	}
	return &ref, nil, true, nil
}

func (journal *Journal) checkpointBytes(kind string) ([]byte, bool, error) {
	ref, raw, found, err := journal.checkpoint(kind)
	if err != nil || !found || ref == nil {
		return raw, found, err
	}
	var result bytes.Buffer
	if err := journal.CopyBody(kind, *ref, &result); err != nil {
		return nil, false, err
	}
	return result.Bytes(), true, nil
}
