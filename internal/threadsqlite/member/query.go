package member

import (
	"database/sql"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

func (journal *Journal) QueryReady(leaf string) (bool, error) {
	var ready bool
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT read_version=1 AND read_rev>0 AND ((read_leaf_seq IS NULL AND ?='') OR EXISTS(SELECT 1 FROM `+journal.database+`history_events WHERE seq=read_leaf_seq AND event_id=? AND entry_no IS NOT NULL)) FROM `+journal.database+`floret_thread_header WHERE singleton=1`, leaf, leaf).Scan(&ready)
	return ready, err
}

func (journal *Journal) PublishReadLeaf() error {
	revision, err := journal.writeRevision()
	if err != nil {
		return err
	}
	_, err = journal.tx.ExecContext(journal.ctx, `UPDATE `+journal.database+`floret_thread_header SET read_version=1,read_leaf_seq=leaf_seq,read_rev=? WHERE singleton=1`, revision)
	return err
}

func (journal *Journal) Revision() (int64, error) {
	var revision int64
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT commit_rev FROM `+journal.database+`floret_thread_header WHERE singleton=1`).Scan(&revision)
	return revision, err
}

type SnapshotMark struct {
	File              string
	Revision, Entries int64
}

func (journal *Journal) SnapshotMark() (mark SnapshotMark, err error) {
	err = journal.tx.QueryRowContext(journal.ctx, `SELECT hex(file),commit_rev,(SELECT coalesce(max(entry_no),-1)+1 FROM `+journal.database+`history_events WHERE entry_no IS NOT NULL) FROM `+journal.database+`floret_thread_header WHERE singleton=1`).Scan(&mark.File, &mark.Revision, &mark.Entries)
	return mark, err
}

func (journal *Journal) VisitTurn(turn string, markersOnly bool, visit func(EntryEvent) error) error {
	if turn == "" {
		return nil
	}
	condition := ""
	if markersOnly {
		condition = ` AND kind IN ('turn_marker','turn_cancel_requested')`
	}
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT entry_no FROM `+journal.database+`history_events WHERE turn_id=? AND entry_no IS NOT NULL`+condition+` ORDER BY seq`, turn)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var number int64
		if err := rows.Scan(&number); err != nil {
			return err
		}
		event, err := journal.EntryNumber(number)
		if err != nil {
			return err
		}
		if err := visit(event); err != nil {
			return err
		}
	}
	return rows.Err()
}

type ReadItem struct {
	Kind, ID, First, Last, Turn, Run, Role, ToolName, ToolKind, Status string
	Position, At                                                       int64
	Completed, Elapsed                                                 *int64
	Summary                                                            []byte
}

func (journal *Journal) PutReadItem(item ReadItem) error {
	if journal.readOnly || item.ID == "" || item.First == "" || item.Last == "" || item.Kind != "message" && item.Kind != "tool" {
		return spi.ErrInvalidArgument
	}
	rev, err := journal.writeRevision()
	if err != nil {
		return err
	}
	events := journal.database + "history_events"
	result, err := journal.tx.ExecContext(journal.ctx, `INSERT INTO `+journal.database+`read_items(generation,view_kind,item_id,position,first_seq,last_seq,source_rev,turn_id,run_id,role,tool_name,tool_kind,status,at_ns,completed_ns,elapsed_ns,fold_flags,preview,summary) SELECT h.read_gen,?,?,?,a.seq,b.seq,?,nullif(?,''),nullif(?,''),nullif(?,''),nullif(?,''),nullif(?,''),nullif(?,''),?,?,?,0,'',? FROM `+journal.database+`floret_thread_header h,`+events+` a,`+events+` b WHERE h.singleton=1 AND a.event_id=? AND b.event_id=? AND a.entry_no IS NOT NULL AND b.entry_no IS NOT NULL ON CONFLICT(generation,view_kind,item_id) DO UPDATE SET last_seq=excluded.last_seq,source_rev=excluded.source_rev,status=excluded.status,completed_ns=excluded.completed_ns,elapsed_ns=excluded.elapsed_ns,summary=excluded.summary WHERE first_seq=excluded.first_seq AND position=excluded.position`, item.Kind, item.ID, item.Position, rev, item.Turn, item.Run, item.Role, item.ToolName, item.ToolKind, item.Status, item.At, item.Completed, item.Elapsed, item.Summary, item.First, item.Last)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.Join(ErrContentIntegrity, err)
	}
	return nil
}

func (journal *Journal) RemoveQueryItems() error {
	if journal.readOnly {
		return spi.ErrInvalidArgument
	}
	_, err := journal.tx.ExecContext(journal.ctx, `DELETE FROM `+journal.database+`read_items WHERE generation=(SELECT read_gen FROM `+journal.database+`floret_thread_header WHERE singleton=1) AND view_kind IN ('message','tool')`)
	return err
}

func (journal *Journal) QueryItems(kind, id string, visit func(ReadItem) error) error {
	if kind != "message" && kind != "tool" || visit == nil {
		return spi.ErrInvalidArgument
	}
	filter := ""
	args := []any{kind}
	if id != "" {
		filter = ` AND i.item_id=?`
		args = append(args, id)
	}
	events := journal.database + "history_events"
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT i.view_kind,i.item_id,i.position,a.event_id,b.event_id,coalesce(i.turn_id,''),coalesce(i.run_id,''),coalesce(i.role,''),coalesce(i.tool_name,''),coalesce(i.tool_kind,''),coalesce(i.status,''),i.at_ns,i.completed_ns,i.elapsed_ns,i.summary FROM `+journal.database+`read_items i JOIN `+journal.database+`floret_thread_header h ON h.singleton=1 AND h.read_gen=i.generation JOIN `+events+` a ON a.seq=i.first_seq JOIN `+events+` b ON b.seq=i.last_seq WHERE i.view_kind=?`+filter+` ORDER BY i.position`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item ReadItem
		var done, elapsed sql.NullInt64
		if err := rows.Scan(&item.Kind, &item.ID, &item.Position, &item.First, &item.Last, &item.Turn, &item.Run, &item.Role, &item.ToolName, &item.ToolKind, &item.Status, &item.At, &done, &elapsed, &item.Summary); err != nil {
			return err
		}
		if done.Valid {
			item.Completed = &done.Int64
		}
		if elapsed.Valid {
			item.Elapsed = &elapsed.Int64
		}
		if err := visit(item); err != nil {
			return err
		}
	}
	return rows.Err()
}
