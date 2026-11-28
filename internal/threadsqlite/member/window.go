package member

import (
	"database/sql"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"strings"
)

type WindowItem struct {
	ID, First, Last, Turn, Run, Role, Tool, Status string
	Position, Depth                                int
	Hidden                                         bool
	Summary                                        []byte
}

func (journal *Journal) WindowValid() (bool, error) {
	var valid bool
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT window_valid FROM `+journal.database+`floret_thread_header WHERE singleton=1`).Scan(&valid)
	return valid, err
}

func (journal *Journal) MarkWindow(valid bool) error {
	if journal.readOnly {
		return spi.ErrInvalidArgument
	}
	_, err := journal.tx.ExecContext(journal.ctx, `UPDATE `+journal.database+`floret_thread_header SET window_valid=? WHERE singleton=1`, valid)
	return err
}

func (journal *Journal) ModelSources() ([]string, error) {
	events := journal.database + "history_events"
	var before, latest sql.NullString
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT a.event_id,b.event_id FROM `+journal.database+`floret_thread_header h LEFT JOIN `+events+` a ON a.seq=h.model_preceding_seq LEFT JOIN `+events+` b ON b.seq=h.model_current_seq WHERE h.singleton=1`).Scan(&before, &latest)
	if err != nil {
		return nil, err
	}
	if before.Valid && !latest.Valid {
		return nil, ErrContentIntegrity
	}
	var ids []string
	for _, v := range []sql.NullString{before, latest} {
		if v.Valid {
			ids = append(ids, v.String)
		}
	}
	return ids, nil
}

func (journal *Journal) SetModelSources(ids []string) error {
	if journal.readOnly || len(ids) > 2 {
		return spi.ErrInvalidArgument
	}
	var refs [2]sql.NullInt64
	for i, id := range ids {
		if err := journal.tx.QueryRowContext(journal.ctx, `SELECT seq FROM `+journal.database+`history_events WHERE event_id=? AND entry_no IS NOT NULL`, id).Scan(&refs[2-len(ids)+i]); err != nil {
			return err
		}
	}
	_, err := journal.tx.ExecContext(journal.ctx, `UPDATE `+journal.database+`floret_thread_header SET model_preceding_seq=?,model_current_seq=? WHERE singleton=1`, refs[0], refs[1])
	return err
}

func (journal *Journal) windowScope() string {
	return `generation=(SELECT read_gen FROM ` + journal.database + `floret_thread_header WHERE singleton=1) AND view_kind='window'`
}

func (journal *Journal) PutWindow(item WindowItem, create bool) error {
	if journal.readOnly || item.ID == "" || item.First == "" || item.Last == "" || item.Role == "" {
		return spi.ErrInvalidArgument
	}
	rev, err := journal.writeRevision()
	if err != nil {
		return err
	}
	events := journal.database + "history_events"
	table := journal.database + "read_items"
	var res sql.Result
	if create {
		res, err = journal.tx.ExecContext(journal.ctx, `INSERT INTO `+table+`(generation,view_kind,item_id,position,first_seq,last_seq,source_rev,turn_id,run_id,role,tool_name,status,at_ns,fold_flags,preview,summary) SELECT h.read_gen,'window',?,(SELECT coalesce(max(position),-1)+1 FROM `+table+` WHERE `+journal.windowScope()+`),a.seq,b.seq,?,a.turn_id,a.run_id,?,nullif(?,''),nullif(?,''),a.at_ns,?,'',? FROM `+journal.database+`floret_thread_header h,`+events+` a,`+events+` b WHERE h.singleton=1 AND a.event_id=? AND b.event_id=? AND a.entry_no IS NOT NULL AND b.entry_no IS NOT NULL`, item.ID, rev, item.Role, item.Tool, item.Status, item.Hidden, item.Summary, item.First, item.Last)
	} else {
		res, err = journal.tx.ExecContext(journal.ctx, `UPDATE `+table+` SET last_seq=(SELECT seq FROM `+events+` WHERE event_id=? AND entry_no IS NOT NULL),source_rev=?,tool_name=nullif(?,''),status=nullif(?,''),fold_flags=?,summary=? WHERE `+journal.windowScope()+` AND item_id=? AND role=? AND first_seq=(SELECT seq FROM `+events+` WHERE event_id=?)`, item.Last, rev, item.Tool, item.Status, item.Hidden, item.Summary, item.ID, item.Role, item.First)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return errors.Join(ErrContentIntegrity, err)
	}
	return nil
}

func (journal *Journal) ClearWindow() error {
	if journal.readOnly {
		return spi.ErrInvalidArgument
	}
	_, err := journal.tx.ExecContext(journal.ctx, `DELETE FROM `+journal.database+`read_items WHERE `+journal.windowScope())
	return err
}

func (journal *Journal) WindowSlots() (int, int, error) {
	var slots, hidden int
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT (SELECT coalesce(max(position),-1)+1 FROM `+journal.database+`read_items WHERE `+journal.windowScope()+`),(SELECT count(*) FROM `+journal.database+`read_items WHERE `+journal.windowScope()+` AND fold_flags=1)`).Scan(&slots, &hidden)
	return slots, hidden, err
}

func (journal *Journal) WindowPosition(id string) (int, error) {
	var pos int
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT position FROM `+journal.database+`read_items WHERE `+journal.windowScope()+` AND item_id=? AND fold_flags=0`, id).Scan(&pos)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, spi.ErrNotFound
	}
	return pos, err
}

type WindowRangeFilter struct {
	Kinds                 []string
	Turn, Run             string
	Exclude               uint32
	Settled, Intermediate bool
	ResponseDepth         int64
	Anchor, Start, End    int
}

func (journal *Journal) WindowRange(filter WindowRangeFilter, visit func(role, status string, facts uint32, count int) error) (first, last int, err error) {
	if len(filter.Kinds) == 0 || filter.Anchor < filter.Start || filter.Anchor >= filter.End || visit == nil {
		return 0, 0, spi.ErrInvalidArgument
	}
	scope := journal.windowScope() + " AND fold_flags=0"
	table := journal.database + "read_items"
	events := journal.database + "history_events"
	facts := `coalesce(json_extract(CAST(summary AS TEXT),'$.facts'),0)`
	eligible := `coalesce(turn_id,'')=? AND coalesce(run_id,'')=? AND role IN (` + strings.TrimSuffix(strings.Repeat("?,", len(filter.Kinds)), ",") + `) AND (` + facts + ` & ?)=0`
	args := []any{filter.Turn, filter.Run}
	for _, kind := range filter.Kinds {
		args = append(args, kind)
	}
	args = append(args, filter.Exclude)
	if filter.Settled {
		eligible += ` AND (role!='tool' OR coalesce(status,'') IN ('success','error','declined','canceled','unknown'))`
	}
	if filter.Intermediate {
		eligible += ` AND (role!='assistant' OR (coalesce(json_extract(CAST(summary AS TEXT),'$.response'),0)=1 AND (SELECT path_depth FROM ` + events + ` WHERE seq=first_seq)<?))`
		args = append(args, filter.ResponseDepth)
	}
	bound := func(start, end int, op string, missing int) (int, error) {
		var v sql.NullInt64
		query := `SELECT ` + op + `(position) FROM ` + table + ` WHERE ` + scope + ` AND position>=? AND position<=? AND NOT (` + eligible + `)`
		err := journal.tx.QueryRowContext(journal.ctx, query, append([]any{start, end}, args...)...).Scan(&v)
		if !v.Valid {
			return missing, err
		}
		return int(v.Int64), err
	}
	before, err := bound(filter.Start, filter.Anchor, "max", filter.Start-1)
	if err != nil {
		return 0, 0, err
	}
	after, err := bound(filter.Anchor, filter.End-1, "min", filter.End)
	if err != nil {
		return 0, 0, err
	}
	if before == filter.Anchor || after == filter.Anchor {
		return 0, 0, spi.ErrNotFound
	}
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT role,coalesce(status,''),`+facts+`,count(*),min(position),max(position) FROM `+table+` WHERE `+scope+` AND position>? AND position<? GROUP BY 1,2,3`, before, after)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	first, last = after, before
	for rows.Next() {
		var role, status string
		var mask uint32
		var count, lo, hi int
		if err := rows.Scan(&role, &status, &mask, &count, &lo, &hi); err != nil {
			return 0, 0, err
		}
		first, last = min(first, lo), max(last, hi)
		if err := visit(role, status, mask, count); err != nil {
			return 0, 0, err
		}
	}
	return first, last, rows.Err()
}

func (journal *Journal) WindowAnchorBefore(depth int) (string, error) {
	var id string
	events := journal.database + "history_events"
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT i.item_id FROM `+journal.database+`read_items i JOIN `+events+` e ON e.seq=i.first_seq WHERE `+journal.windowScope()+` AND fold_flags=0 AND e.path_depth<? AND (i.role!='interaction' OR (SELECT path_depth FROM `+events+` WHERE seq=i.last_seq)<?) ORDER BY i.position DESC LIMIT 1`, depth, depth).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

func (journal *Journal) HiddenBefore(slot int) (int, error) {
	var count int
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT count(*) FROM `+journal.database+`read_items WHERE `+journal.windowScope()+` AND fold_flags=1 AND position<?`, slot).Scan(&count)
	return count, err
}

func (journal *Journal) WindowItems(first, limit int, visit func(WindowItem) error) error {
	return journal.scanWindowItems("position>=? ORDER BY position LIMIT ?", visit, first, limit)
}

func (journal *Journal) SelectedWindowItem(id string) (WindowItem, error) {
	var found WindowItem
	err := journal.scanWindowItems("i.item_id=?", func(row WindowItem) error { found = row; return nil }, id)
	if err == nil && found.ID == "" {
		return WindowItem{}, spi.ErrNotFound
	}
	return found, err
}

func (journal *Journal) scanWindowItems(filter string, visit func(WindowItem) error, args ...any) error {
	events := journal.database + "history_events"
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT i.item_id,i.position,a.event_id,b.event_id,a.path_depth,coalesce(a.turn_id,''),coalesce(a.run_id,''),i.role,coalesce(i.tool_name,''),coalesce(i.status,''),i.summary FROM `+journal.database+`read_items i JOIN `+events+` a ON a.seq=i.first_seq JOIN `+events+` b ON b.seq=i.last_seq WHERE `+journal.windowScope()+` AND fold_flags=0 AND `+filter, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item WindowItem
		if err := rows.Scan(&item.ID, &item.Position, &item.First, &item.Last, &item.Depth, &item.Turn, &item.Run, &item.Role, &item.Tool, &item.Status, &item.Summary); err != nil {
			return err
		}
		if err := visit(item); err != nil {
			return err
		}
	}
	return rows.Err()
}
