package member

import (
	"bytes"
	"database/sql"
	"errors"
	"math"

	"github.com/floegence/floret/v7/storage/spi"
)

type EntryEvent struct {
	CallSource                             string
	ViewFlags                              int64
	RecoveryGroup                          string
	ID, ParentID, Kind, Turn, Run          string
	Number, Depth, At                      int64
	ModelInput                             bool
	KeepParent                             bool
	RequestScope, RequestKey               string
	Attempt, State                         string
	SubjectID                              string
	Marker, ContentSource, ReasoningSource string
	Payload                                []byte
}

func (journal *Journal) Thread() string { return journal.thread.String() }

func (journal *Journal) Head() (int64, string, error) {
	events := journal.database + "history_events"
	var last int64
	var leafID sql.NullString
	var leafSeq sql.NullInt64
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT (SELECT coalesce(max(entry_no),-1) FROM `+events+` WHERE entry_no IS NOT NULL),h.leaf_seq,e.event_id FROM `+journal.database+`floret_thread_header h LEFT JOIN `+events+` e ON e.seq=h.leaf_seq AND e.entry_no IS NOT NULL WHERE h.singleton=1`).Scan(&last, &leafSeq, &leafID)
	if err != nil || last == math.MaxInt64 || leafSeq.Valid != leafID.Valid || (last < 0) == leafID.Valid {
		return 0, "", errors.Join(ErrContentIntegrity, err)
	}
	return last + 1, leafID.String, nil
}

func (journal *Journal) AppendEntry(event EntryEvent) (int64, error) {
	if journal.readOnly || event.ID == "" || event.Kind == "" || event.Depth < 1 || (event.Depth == 1) != (event.ParentID == "") || event.Number < 0 || (event.RequestKey == "") != (event.RequestScope == "") || (event.State == "") != (event.Attempt == "") || len(event.Payload) == 0 || len(event.Payload) > 16384 {
		return 0, spi.ErrInvalidArgument
	}
	events := journal.database + "history_events"
	var count int64
	if err := journal.tx.QueryRowContext(journal.ctx, `SELECT coalesce(max(entry_no),-1) FROM `+events+` WHERE entry_no IS NOT NULL`).Scan(&count); err != nil {
		return 0, err
	}
	if count == math.MaxInt64 || event.Number != count+1 {
		return 0, spi.ErrConflict
	}
	var parent, previous, subject, ordinal sql.NullInt64
	if event.ParentID != "" {
		var seq, depth int64
		err := journal.tx.QueryRowContext(journal.ctx, `SELECT entry_no,seq,path_depth,CASE model_input WHEN 1 THEN seq ELSE model_prev_seq END FROM `+events+` WHERE event_id=? AND entry_no IS NOT NULL`, event.ParentID).Scan(&parent, &seq, &depth, &previous)
		if err != nil || depth != event.Depth-1 {
			return 0, errors.Join(ErrContentIntegrity, err)
		}
		if event.KeepParent {
			previous = sql.NullInt64{Int64: seq, Valid: true}
		}
	}
	if event.SubjectID != "" {
		if err := journal.tx.QueryRowContext(journal.ctx, `SELECT seq FROM `+events+` WHERE event_id=? AND entry_no IS NOT NULL`, event.SubjectID).Scan(&subject); err != nil {
			return 0, errors.Join(ErrContentIntegrity, err)
		}
	}
	if event.Attempt != "" {
		var last string
		var open bool
		var seq int64
		err := journal.tx.QueryRowContext(journal.ctx, `SELECT seq,ordinal,state,work_head FROM `+events+` WHERE attempt_id=? ORDER BY ordinal DESC LIMIT 1`, event.Attempt).Scan(&seq, &ordinal, &last, &open)
		if errors.Is(err, sql.ErrNoRows) {
			if event.State != "prepared" && event.State != "unknown" {
				return 0, ErrContentIntegrity
			}
			ordinal = sql.NullInt64{Valid: true}
		} else if err != nil {
			return 0, err
		} else {
			valid := last == "prepared" && (event.State == "dispatching" || event.State == "cancelled" || event.State == "rejected") || last == "dispatching" && (event.State == "completed" || event.State == "failed" || event.State == "cancelled" || event.State == "unknown")
			if !valid || !open || ordinal.Int64 == math.MaxInt64 {
				return 0, ErrContentIntegrity
			}
			if _, err := journal.tx.ExecContext(journal.ctx, `UPDATE `+events+` SET work_head=0 WHERE seq=?`, seq); err != nil {
				return 0, err
			}
			ordinal.Int64++
		}
	}
	rev, err := journal.writeRevision()
	if err != nil {
		return 0, err
	}
	open := event.State == "prepared" || event.State == "dispatching"
	if event.Kind == "turn_marker" && event.Turn != "" {
		switch event.Marker {
		case "started", "save_point", "waiting", "completed", "failed", "aborted":
			if _, err := journal.tx.ExecContext(journal.ctx, `UPDATE `+events+` SET work_head=0 WHERE kind='turn_marker' AND turn_id=? AND work_head=1`, event.Turn); err != nil {
				return 0, err
			}
			open = event.Marker == "started" || event.Marker == "save_point" || event.Marker == "waiting"
		}
	}
	var seq int64
	err = journal.tx.QueryRowContext(journal.ctx, `INSERT INTO `+events+`(event_id,entry_no,path_depth,model_input,commit_rev,kind,schema_ver,at_ns,turn_id,run_id,parent_no,model_prev_seq,subject_seq,scope_key,request_key,attempt_id,ordinal,state,work_head,payload,turn_marker,content_source,reasoning_source,recovery_group,call_source,view_flags) VALUES(?,?,?,?,?,?,1,?,nullif(?,''),nullif(?,''),?,?,?,nullif(?,''),nullif(?,''),nullif(?,''),?,nullif(?,''),?,?,nullif(?,''),nullif(?,''),nullif(?,''),nullif(?,''),nullif(?,''),?) RETURNING seq`, event.ID, event.Number, event.Depth, event.ModelInput, rev, event.Kind, event.At, event.Turn, event.Run, parent, previous, subject, event.RequestScope, event.RequestKey, event.Attempt, ordinal, event.State, open, event.Payload, event.Marker, event.ContentSource, event.ReasoningSource, event.RecoveryGroup, event.CallSource, event.ViewFlags).Scan(&seq)
	return seq, err
}

func (journal *Journal) Entry(id string) (EntryEvent, error) {
	if journal.readOnly {
		journal.entries.Lock()
		if journal.entries.closed {
			journal.entries.Unlock()
			return EntryEvent{}, sql.ErrTxDone
		}
		event, ok := journal.entries.byID[id]
		journal.entries.Unlock()
		if ok {
			event.Payload = bytes.Clone(event.Payload)
			return event, journal.ctx.Err()
		}
	}
	return journal.entry("item.event_id=?", id)
}
func (journal *Journal) EntryNumber(number int64) (EntryEvent, error) {
	if journal.readOnly {
		journal.entries.Lock()
		if journal.entries.closed {
			journal.entries.Unlock()
			return EntryEvent{}, sql.ErrTxDone
		}
		event, ok := journal.entries.byID[journal.entries.byNumber[number]]
		journal.entries.Unlock()
		if ok {
			event.Payload = bytes.Clone(event.Payload)
			return event, journal.ctx.Err()
		}
	}
	return journal.entry("item.entry_no=?", number)
}
func (journal *Journal) entrySelect() string {
	events := journal.database + "history_events"
	return `SELECT item.event_id,coalesce(parent.event_id,''),item.kind,coalesce(item.turn_id,''),coalesce(item.run_id,''),item.entry_no,item.path_depth,item.at_ns,item.model_input,coalesce(item.scope_key,''),coalesce(item.request_key,''),coalesce(item.attempt_id,''),coalesce(item.state,''),coalesce(subject.event_id,''),item.payload,item.schema_ver,coalesce(item.turn_marker,''),coalesce(item.content_source,''),coalesce(item.reasoning_source,''),coalesce(item.recovery_group,''),coalesce(item.call_source,''),item.view_flags FROM ` + events + ` item LEFT JOIN ` + events + ` parent ON parent.entry_no=item.parent_no LEFT JOIN ` + events + ` subject ON subject.seq=item.subject_seq WHERE item.entry_no IS NOT NULL AND `
}
func (journal *Journal) entry(filter string, arg any) (EntryEvent, error) {
	return journal.scanEntry(journal.tx.QueryRowContext(journal.ctx, journal.entrySelect()+filter, arg))
}
func (journal *Journal) scanEntry(row interface{ Scan(...any) error }) (EntryEvent, error) {
	var item EntryEvent
	var version int
	err := row.Scan(&item.ID, &item.ParentID, &item.Kind, &item.Turn, &item.Run, &item.Number, &item.Depth, &item.At, &item.ModelInput, &item.RequestScope, &item.RequestKey, &item.Attempt, &item.State, &item.SubjectID, &item.Payload, &version, &item.Marker, &item.ContentSource, &item.ReasoningSource, &item.RecoveryGroup, &item.CallSource, &item.ViewFlags)
	if err == nil && version != 1 {
		return EntryEvent{}, ErrContentIntegrity
	}
	if err == nil && journal.readOnly {
		journal.entries.Lock()
		defer journal.entries.Unlock()
		if journal.entries.closed {
			return EntryEvent{}, sql.ErrTxDone
		}
		if _, ok := journal.entries.byID[item.ID]; !ok {
			cost := 512 + len(item.Payload) + len(item.ID) + len(item.ParentID) + len(item.Turn) + len(item.Run) + len(item.RequestKey) + len(item.RequestScope) + len(item.Attempt) + len(item.SubjectID) + len(item.ContentSource) + len(item.ReasoningSource) + len(item.CallSource)
			if journal.entries.byID == nil || journal.entries.bytes+cost > 2<<20 || len(journal.entries.byID) == 512 {
				journal.entries.byID = make(map[string]EntryEvent)
				journal.entries.byNumber = make(map[int64]string)
				journal.entries.bytes = 0
			}
			if cost <= 2<<20 {
				stored := item
				stored.Payload = bytes.Clone(item.Payload)
				journal.entries.byID[item.ID] = stored
				journal.entries.byNumber[item.Number] = item.ID
				journal.entries.bytes += cost
			}
		}
	}
	return item, err
}

func (journal *Journal) SetLeaf(id string) error {
	var leaf sql.NullInt64
	if id != "" {
		if err := journal.tx.QueryRowContext(journal.ctx, `SELECT seq FROM `+journal.database+`history_events WHERE event_id=? AND entry_no IS NOT NULL`, id).Scan(&leaf); err != nil {
			return err
		}
	}
	if _, err := journal.writeRevision(); err != nil {
		return err
	}
	_, err := journal.tx.ExecContext(journal.ctx, `UPDATE `+journal.database+`floret_thread_header SET leaf_seq=? WHERE singleton=1`, leaf)
	return err
}
