package member

import (
	"database/sql"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/storage/spi"
)

func (journal *Journal) VisitModelPath(leaf string, visit func(EntryEvent) error) error {
	if visit == nil {
		return spi.ErrInvalidArgument
	}
	events := journal.database + "history_events"
	var next sql.NullInt64
	if leaf == "" {
		_, id, err := journal.Head()
		if err != nil || id == "" {
			return err
		}
		leaf = id
	}
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT CASE model_input WHEN 1 THEN seq ELSE model_prev_seq END FROM `+events+` WHERE event_id=? AND entry_no IS NOT NULL`, leaf).Scan(&next)
	if err != nil {
		return err
	}
	var numbers []int64
	for next.Valid {
		rows, err := journal.tx.QueryContext(journal.ctx, `WITH RECURSIVE path(seq) AS (VALUES(?) UNION ALL SELECT e.model_prev_seq FROM `+events+` e JOIN path ON e.seq=path.seq WHERE e.model_prev_seq IS NOT NULL LIMIT 128) SELECT path.seq,e.model_prev_seq,e.entry_no FROM path LEFT JOIN `+events+` e ON e.seq=path.seq ORDER BY path.seq DESC`, next.Int64)
		if err != nil {
			return err
		}
		before := len(numbers)
		for rows.Next() {
			var seq int64
			var previous, number sql.NullInt64
			if err := rows.Scan(&seq, &previous, &number); err != nil {
				return errors.Join(err, rows.Close())
			}
			if !next.Valid || seq != next.Int64 || !number.Valid || previous.Valid && (previous.Int64 <= 0 || previous.Int64 >= seq) {
				return errors.Join(ErrContentIntegrity, rows.Close())
			}
			numbers = append(numbers, number.Int64)
			next = previous
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		if before == len(numbers) {
			return ErrContentIntegrity
		}
	}
	for _, number := range slices.Backward(numbers) {
		event, err := journal.EntryNumber(number)
		if err != nil {
			return err
		}
		if err := visit(event); err != nil {
			return err
		}
	}
	return nil
}

func (journal *Journal) VisitPathAfter(leaf string, after int64, visit func(EntryEvent) error) error {
	if leaf == "" || after < 0 || visit == nil {
		return spi.ErrInvalidArgument
	}
	events := journal.database + "history_events"
	var count int64
	if err := journal.tx.QueryRowContext(journal.ctx, `SELECT path_depth FROM `+events+` WHERE event_id=? AND entry_no IS NOT NULL`, leaf).Scan(&count); err != nil {
		return err
	}
	if after >= count {
		return nil
	}
	rows, err := journal.tx.QueryContext(journal.ctx, `WITH RECURSIVE path(number,parent,depth) AS (SELECT entry_no,parent_no,path_depth FROM `+events+` WHERE event_id=? AND entry_no IS NOT NULL UNION ALL SELECT p.entry_no,p.parent_no,p.path_depth FROM `+events+` p JOIN path ON path.parent=p.entry_no WHERE path.depth>? AND p.entry_no<path.number AND p.path_depth=path.depth-1) SELECT number FROM path ORDER BY depth`, leaf, after+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	var lastID string
	depth := after
	for rows.Next() {
		var number int64
		if err := rows.Scan(&number); err != nil {
			return err
		}
		entry, err := journal.EntryNumber(number)
		if err != nil {
			return err
		}
		depth++
		if entry.Depth != depth || (lastID != "" && entry.ParentID != lastID) || (depth == 1 && entry.ParentID != "") {
			return ErrContentIntegrity
		}
		if err := visit(entry); err != nil {
			return err
		}
		lastID = entry.ID
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if depth != count || lastID != leaf {
		return ErrContentIntegrity
	}
	return nil
}

func (journal *Journal) Ancestor(id string, depth int64) (EntryEvent, error) {
	events := journal.database + "history_events"
	if depth < 1 {
		return EntryEvent{}, spi.ErrInvalidArgument
	}
	var number, current int64
	if err := journal.tx.QueryRowContext(journal.ctx, `SELECT entry_no,path_depth FROM `+events+` WHERE event_id=? AND entry_no IS NOT NULL`, id).Scan(&number, &current); err != nil {
		return EntryEvent{}, err
	}
	for current >= depth && number >= current-1 {
		target := number - (current - depth)
		var edge, edgeDepth int64
		var parent sql.NullInt64
		err := journal.tx.QueryRowContext(journal.ctx, `SELECT entry_no,path_depth,parent_no FROM `+events+` WHERE entry_no IS NOT NULL AND (parent_no IS NULL OR parent_no != entry_no-1) AND entry_no>? AND entry_no<=? ORDER BY entry_no DESC LIMIT 1`, target, number).Scan(&edge, &edgeDepth, &parent)
		if errors.Is(err, sql.ErrNoRows) {
			entry, err := journal.EntryNumber(target)
			if err != nil || entry.Depth != depth {
				return EntryEvent{}, errors.Join(ErrContentIntegrity, err)
			}
			return entry, nil
		}
		if err != nil {
			return EntryEvent{}, err
		}
		if edgeDepth != current-(number-edge) || !parent.Valid || parent.Int64 < 0 || parent.Int64 >= edge {
			break
		}
		number, current = parent.Int64, edgeDepth-1
	}
	return EntryEvent{}, ErrContentIntegrity
}
