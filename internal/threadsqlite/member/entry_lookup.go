package member

import (
	"database/sql"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

func (journal *Journal) ParentRecoveryThrough() (int64, error) {
	var through int64
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT CASE WHEN h.parent_checked_seq IS NULL THEN 0 ELSE e.entry_no+1 END FROM `+journal.database+`floret_thread_header h LEFT JOIN `+journal.database+`history_events e ON e.seq=h.parent_checked_seq WHERE h.singleton=1`).Scan(&through)
	return through, err
}

func (journal *Journal) CheckParentRecovery(count int64) error {
	if journal.readOnly || count < 0 {
		return spi.ErrInvalidArgument
	}
	var seq sql.NullInt64
	if count > 0 {
		if err := journal.tx.QueryRowContext(journal.ctx, `SELECT seq FROM `+journal.database+`history_events WHERE entry_no=?`, count-1).Scan(&seq); err != nil {
			return err
		}
	}
	if _, err := journal.writeRevision(); err != nil {
		return err
	}
	_, err := journal.tx.ExecContext(journal.ctx, `UPDATE `+journal.database+`floret_thread_header SET parent_checked_seq=? WHERE singleton=1`, seq)
	return err
}

func (journal *Journal) VisitRecoveryEntries(group string, visit func(EntryEvent) error) error {
	if group != "queue" && group != "parent" && group != "host_work" || visit == nil {
		return spi.ErrInvalidArgument
	}
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT entry_no FROM `+journal.database+`history_events WHERE recovery_group=? ORDER BY entry_no`, group)
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

func (journal *Journal) ActiveTurnEntry() (string, error) {
	var id string
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT CASE count(*) WHEN 1 THEN max(event_id) ELSE '' END FROM (SELECT event_id FROM `+journal.database+`history_events WHERE kind='turn_marker' AND work_head=1 LIMIT 2)`).Scan(&id)
	return id, err
}

func (journal *Journal) VisitInteractionSources(visit func(EntryEvent) error) error {
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT entry_no FROM `+journal.database+`history_events WHERE kind IN ('interaction_requested','tool_call','tool_result') AND entry_no IS NOT NULL ORDER BY entry_no`)
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

func (journal *Journal) AttemptEntry(attempt string) (string, error) {
	var id string
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT event_id FROM `+journal.database+`history_events WHERE attempt_id=? ORDER BY ordinal DESC LIMIT 1`, attempt).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

func (journal *Journal) TurnRequestEntry(key string) (string, error) {
	var id string
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT event_id FROM `+journal.database+`history_events WHERE scope_key='turn' AND request_key=? AND kind='turn_marker' AND turn_marker='started'`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", spi.ErrNotFound
	}
	return id, err
}

func (journal *Journal) VisitOutputSources(source string, through int, visit func(string) (bool, error)) error {
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT event_id FROM `+journal.database+`history_events WHERE (content_source=? OR reasoning_source=?) AND entry_no<=? ORDER BY entry_no`, source, source, through)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		done, err := visit(id)
		if err != nil || done {
			return err
		}
	}
	return rows.Err()
}

func (journal *Journal) SourcePage(source string, after, through, limit int) ([]EntryEvent, bool, error) {
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT entry_no FROM `+journal.database+`history_events WHERE (content_source=? OR reasoning_source=?) AND entry_no>? AND entry_no<=? ORDER BY entry_no LIMIT ?`, source, source, after, through, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var events []EntryEvent
	for rows.Next() {
		if len(events) == limit {
			return events, true, nil
		}
		var number int64
		if err := rows.Scan(&number); err != nil {
			return nil, false, err
		}
		event, err := journal.EntryNumber(number)
		if err != nil {
			return nil, false, err
		}
		events = append(events, event)
	}
	return events, false, rows.Err()
}
