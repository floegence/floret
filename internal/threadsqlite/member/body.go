package member

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

type BodyRef struct {
	Object   int64  `json:"object_seq"`
	Boundary int64  `json:"boundary_seq"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Digest   string `json:"sha256"`
}

type sealedBody struct {
	Observed int64  `json:"observed_bytes"`
	Retained int64  `json:"retained_bytes"`
	Blocks   int64  `json:"blocks"`
	Digest   string `json:"digest"`
	Sealed   bool   `json:"sealed"`
}

func bodyBoundary(size int64, digest string) sealedBody {
	var blocks int64
	if size > 0 {
		blocks = 1 + (size-1)/BlockBytes
	}
	return sealedBody{size, size, blocks, digest, true}
}

func (journal *Journal) PutBody(class string, at int64, content []byte) (BodyRef, error) {
	return journal.PutBodyStream(class, at, int64(len(content)), sha256.Sum256(content), bytes.NewReader(content))
}

func (journal *Journal) PutBodyStream(class string, at, size int64, digest [sha256.Size]byte, source io.Reader) (BodyRef, error) {
	if class == "" || size < 0 || source == nil {
		return BodyRef{}, spi.ErrInvalidArgument
	}
	ref := BodyRef{End: size, Digest: hex.EncodeToString(digest[:])}
	events := journal.database + "history_events"
	scope := "body:" + class
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT seq FROM `+events+` WHERE scope_key=? AND request_key=? AND kind='content_created' AND schema_ver=1`, scope, ref.Digest).Scan(&ref.Object)
	if err == nil {
		if err := journal.tx.QueryRowContext(journal.ctx, `SELECT seq FROM `+events+` WHERE kind='output_boundary' AND object_seq=? ORDER BY commit_rev DESC,seq DESC LIMIT 1`, ref.Object).Scan(&ref.Boundary); err != nil {
			return BodyRef{}, errors.Join(ErrContentIntegrity, err)
		}
		content, err := journal.body(class, ref)
		if err != nil {
			return BodyRef{}, err
		}
		return ref, visitImmutableBlocks(journal.ctx, content, source, nil)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return BodyRef{}, err
	}
	revision, err := journal.writeRevision()
	if err != nil {
		return BodyRef{}, err
	}
	err = journal.tx.QueryRowContext(journal.ctx, `INSERT INTO `+events+`(event_id,commit_rev,kind,schema_ver,at_ns,scope_key,request_key,payload) VALUES(?,?,'content_created',1,?,?,?,?) RETURNING seq`, rand.Text(), revision, at, scope, ref.Digest, []byte(`{}`)).Scan(&ref.Object)
	if err != nil {
		return BodyRef{}, err
	}
	if err := writeImmutableBlocks(journal.ctx, journal.tx, journal.database, immutableContent{ref.Object, ref.End, digest}, source); err != nil {
		return BodyRef{}, err
	}
	raw, err := json.Marshal(bodyBoundary(ref.End, ref.Digest))
	if err != nil {
		return BodyRef{}, err
	}
	err = journal.tx.QueryRowContext(journal.ctx, `INSERT INTO `+events+`(event_id,commit_rev,kind,schema_ver,at_ns,object_seq,payload) VALUES(?,?,'output_boundary',1,?,?,?) RETURNING seq`, rand.Text(), revision, at, ref.Object, raw).Scan(&ref.Boundary)
	return ref, err
}

func (journal *Journal) body(class string, ref BodyRef) (immutableContent, error) {
	var content immutableContent
	if ref.Object <= 0 || ref.Boundary <= ref.Object || ref.Start != 0 || ref.End < 0 || len(ref.Digest) != sha256.Size*2 {
		return content, ErrContentIntegrity
	}
	if _, err := hex.Decode(content.SHA256[:], []byte(ref.Digest)); err != nil || hex.EncodeToString(content.SHA256[:]) != ref.Digest {
		return content, ErrContentIntegrity
	}
	var raw []byte
	events := journal.database + "history_events"
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT boundary.payload FROM `+events+` boundary JOIN `+events+` object ON object.seq=boundary.object_seq WHERE boundary.seq=? AND boundary.kind='output_boundary' AND boundary.schema_ver=1 AND object.seq=? AND object.kind='content_created' AND object.schema_ver=1 AND object.scope_key=? AND object.request_key=?`, ref.Boundary, ref.Object, "body:"+class, ref.Digest).Scan(&raw)
	if err != nil {
		return content, errors.Join(ErrContentIntegrity, err)
	}
	var bound sealedBody
	if err := storagecodec.DecodeJSON(raw, &bound); err != nil || bound != bodyBoundary(ref.End, ref.Digest) {
		return content, errors.Join(ErrContentIntegrity, err)
	}
	content.ObjectSeq, content.Bytes = ref.Object, ref.End
	return content, nil
}

func (journal *Journal) ReadBody(class string, ref BodyRef, start int64, limit int) ([]byte, error) {
	content, err := journal.body(class, ref)
	if err != nil {
		return nil, err
	}
	return readImmutableRange(journal.ctx, journal.tx, journal.database, content, start, limit)
}

func (journal *Journal) CopyBody(class string, ref BodyRef, destination io.Writer) error {
	if destination == nil {
		return spi.ErrInvalidArgument
	}
	content, err := journal.body(class, ref)
	if err != nil {
		return err
	}
	hash := sha256.New()
	writer := io.MultiWriter(destination, hash)
	for start := int64(0); start < ref.End; {
		data, err := readImmutableRange(journal.ctx, journal.tx, journal.database, content, start, BlockBytes)
		if err != nil {
			return err
		}
		if _, err := writer.Write(data); err != nil {
			return err
		}
		start += int64(len(data))
	}
	if !bytes.Equal(hash.Sum(nil), content.SHA256[:]) {
		return ErrContentIntegrity
	}
	return nil
}

func (journal *Journal) RemoveBody(class string, ref BodyRef) error {
	if _, err := journal.body(class, ref); err != nil {
		return err
	}
	if _, err := journal.writeRevision(); err != nil {
		return err
	}
	if _, err := journal.tx.ExecContext(journal.ctx, `DELETE FROM `+journal.database+`content_blocks WHERE object_seq=?`, ref.Object); err != nil {
		return err
	}
	_, err := journal.tx.ExecContext(journal.ctx, `DELETE FROM `+journal.database+`history_events WHERE seq IN (?,?)`, ref.Boundary, ref.Object)
	return err
}
