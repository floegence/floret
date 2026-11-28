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
