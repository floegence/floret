package member

import (
	"crypto/rand"
	"database/sql"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

func (journal *Journal) LinkWorkOutput(entry, stream string, pin int64) error {
	if entry == "" || stream == "" || pin < 0 {
		return spi.ErrInvalidArgument
	}
	output, err := journal.OutputAt(stream, pin)
	if err != nil {
		return err
	}
	seq := output.ObjectSeq
	if pin != 0 {
		if !output.Sealed {
			return ErrContentIntegrity
		}
		seq = output.BoundarySeq
	}
	revision, err := journal.writeRevision()
	if err != nil {
		return err
	}
	result, err := journal.tx.ExecContext(journal.ctx, `UPDATE `+journal.database+`history_events SET object_seq=? WHERE event_id=? AND entry_no IS NOT NULL AND kind='host_work' AND object_seq IS NULL AND commit_rev=?`, seq, entry, revision)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return spi.ErrConflict
	}
	return err
}

func (journal *Journal) WorkOutput(entry, stream string) (Output, bool, error) {
	events := journal.database + "history_events"
	var seq int64
	var kind string
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT target.seq,target.kind FROM `+events+` e JOIN `+events+` target ON target.seq=e.object_seq WHERE e.event_id=? AND e.kind='host_work' AND e.entry_no IS NOT NULL`, entry).Scan(&seq, &kind)
	if err != nil {
		return Output{}, false, err
	}
	switch kind {
	case "output_created":
		result, err := journal.OutputAt(stream, 0)
		if err == nil && result.ObjectSeq != seq {
			return Output{}, false, ErrContentIntegrity
		}
		return result, false, err
	case "output_boundary":
		result, err := journal.OutputAt(stream, seq)
		if err == nil && !result.Sealed {
			return Output{}, false, ErrContentIntegrity
		}
		return result, true, err
	default:
		return Output{}, false, ErrContentIntegrity
	}
}

func (journal *Journal) OutputCounts() (streams, artifacts, bindings int, err error) {
	events := journal.database + "history_events"
	err = journal.tx.QueryRowContext(journal.ctx, `SELECT (SELECT count(*) FROM `+events+` WHERE scope_key='output'),(SELECT count(*) FROM `+events+` WHERE scope_key='artifact'),(SELECT count(*) FROM `+events+` WHERE kind='host_work' AND object_seq IS NOT NULL)`).Scan(&streams, &artifacts, &bindings)
	return
}

func (journal *Journal) BindArtifact(id, entry string, boundary, at int64) error {
	if id == "" || entry == "" || boundary <= 0 {
		return spi.ErrInvalidArgument
	}
	events := journal.database + "history_events"
	revision, err := journal.writeRevision()
	if err != nil {
		return err
	}
	result, err := journal.tx.ExecContext(journal.ctx, `INSERT INTO `+events+`(event_id,commit_rev,kind,schema_ver,at_ns,scope_key,request_key,subject_seq,object_seq,payload) SELECT ?,?,'artifact_retained',1,?,'artifact',?,subject.seq,boundary.seq,x'7b7d' FROM `+events+` subject JOIN `+events+` boundary ON boundary.seq=? AND boundary.kind='output_boundary' WHERE subject.event_id=? AND subject.entry_no IS NOT NULL AND subject.commit_rev=?`, rand.Text(), revision, at, id, boundary, entry, revision)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return ErrContentIntegrity
	}
	return err
}

type ArtifactSource struct {
	Entry  string
	At     int64
	Stream string
	Output Output
	Body   BodyRef
}

func (journal *Journal) ArtifactEntry(id string) (string, bool, error) {
	var entry string
	events := journal.database + "history_events"
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT subject.event_id FROM `+events+` owner JOIN `+events+` subject ON subject.seq=owner.subject_seq AND subject.entry_no IS NOT NULL WHERE owner.scope_key='artifact' AND owner.request_key=? AND owner.kind='artifact_retained'`, id).Scan(&entry)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return entry, err == nil, err
}

func (journal *Journal) Artifact(id string, size int64, digest string) (ArtifactSource, error) {
	var source ArtifactSource
	var class, key, kind string
	var edge, object int64
	events := journal.database + "history_events"
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT owner.at_ns,subject.event_id,boundary.seq,object.seq,object.kind,object.scope_key,object.request_key FROM `+events+` owner JOIN `+events+` subject ON subject.seq=owner.subject_seq AND subject.entry_no IS NOT NULL JOIN `+events+` boundary ON boundary.seq=owner.object_seq AND boundary.kind='output_boundary' JOIN `+events+` object ON object.seq=boundary.object_seq WHERE owner.scope_key='artifact' AND owner.request_key=? AND owner.kind='artifact_retained' AND owner.schema_ver=1`, id).Scan(&source.At, &source.Entry, &edge, &object, &kind, &class, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return source, spi.ErrNotFound
	}
	if err != nil {
		return source, err
	}
	switch {
	case kind == "content_created" && class == "body:artifact" && key == digest:
		source.Body = BodyRef{Object: object, Boundary: edge, End: size, Digest: digest}
		_, err = journal.body("artifact", source.Body)
	case kind == "output_created" && class == "output":
		source.Stream = key
		source.Output, err = journal.OutputAt(key, edge)
		if err == nil && (!source.Output.Sealed || source.Output.Retained != size || source.Output.Digest != digest) {
			err = ErrContentIntegrity
		}
	default:
		err = ErrContentIntegrity
	}
	return source, err
}
