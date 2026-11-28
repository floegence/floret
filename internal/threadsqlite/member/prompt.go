package member

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"math"

	"github.com/floegence/floret/v7/storage/spi"
)

type PromptFact struct {
	Scope, Kind, Turn, Run string
	Number                 int64
	At                     sql.NullInt64
	Payload                []byte
}

func (journal *Journal) AppendPrompt(fact PromptFact) error {
	if fact.Scope == "" || fact.Number < 1 || len(fact.Payload) == 0 || len(fact.Payload) > 16384 {
		return spi.ErrInvalidArgument
	}
	switch fact.Kind {
	case "provider_segment", "provider_tools", "provider_request", "provider_response":
	default:
		return spi.ErrInvalidArgument
	}
	events := journal.database + "history_events"
	var last int64
	if err := journal.tx.QueryRowContext(journal.ctx, `SELECT coalesce(max(prompt_no),0) FROM `+events+` WHERE prompt_scope=? AND kind=? AND prompt_no IS NOT NULL`, fact.Scope, fact.Kind).Scan(&last); err != nil {
		return err
	}
	if last == math.MaxInt64 || last+1 != fact.Number {
		return spi.ErrConflict
	}
	revision, err := journal.writeRevision()
	if err != nil {
		return err
	}
	_, err = journal.tx.ExecContext(journal.ctx, `INSERT INTO `+events+`(event_id,kind,schema_ver,at_ns,commit_rev,prompt_scope,prompt_no,turn_id,run_id,payload) VALUES(?,?,1,?,?,?,?,nullif(?,''),nullif(?,''),?)`, rand.Text(), fact.Kind, fact.At, revision, fact.Scope, fact.Number, fact.Turn, fact.Run, fact.Payload)
	return err
}

func (journal *Journal) Prompt(scope, kind string, number int64) (PromptFact, error) {
	fact := PromptFact{Scope: scope, Kind: kind, Number: number}
	var version int
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT schema_ver,at_ns,coalesce(turn_id,''),coalesce(run_id,''),payload FROM `+journal.database+`history_events WHERE prompt_scope=? AND kind=? AND prompt_no=?`, scope, kind, number).Scan(&version, &fact.At, &fact.Turn, &fact.Run, &fact.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return fact, spi.ErrNotFound
	}
	if err == nil && version != 1 {
		err = ErrContentIntegrity
	}
	return fact, err
}

func (journal *Journal) PromptCount() (int64, error) {
	var count int64
	err := journal.tx.QueryRowContext(journal.ctx, `SELECT count(*) FROM `+journal.database+`history_events WHERE prompt_no IS NOT NULL`).Scan(&count)
	return count, err
}

func (journal *Journal) VisitPromptPayloads(scope string, others bool, visit func(kind string, payload []byte) error) error {
	if visit == nil || scope == "" {
		return spi.ErrInvalidArgument
	}
	condition := `prompt_scope=?`
	if others {
		condition = `prompt_scope<>?`
	}
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT kind,schema_ver,payload FROM `+journal.database+`history_events WHERE prompt_no IS NOT NULL AND `+condition, scope)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var version int
		var raw []byte
		if err := rows.Scan(&kind, &version, &raw); err != nil {
			return err
		}
		if version != 1 {
			return ErrContentIntegrity
		}
		if err := visit(kind, raw); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (journal *Journal) RemovePromptScope(scope string) error {
	if scope == "" {
		return spi.ErrInvalidArgument
	}
	if _, err := journal.writeRevision(); err != nil {
		return err
	}
	_, err := journal.tx.ExecContext(journal.ctx, `DELETE FROM `+journal.database+`history_events WHERE prompt_no IS NOT NULL AND prompt_scope=?`, scope)
	return err
}
