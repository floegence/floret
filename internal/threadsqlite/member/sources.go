package member

import (
	"strconv"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	WindowTurn int64 = 1 << iota
	WindowGoal
	WindowContext
	WindowModel
	WindowInteraction
	WindowResponse
	WindowSummary
)

type SourceSelection struct {
	Flag                    int64
	Turn, Run, Output, Call string
	Kinds                   []string
	Reverse                 bool
}

func (journal *Journal) Sources(input SourceSelection, through int, visit func(EntryEvent) (bool, error)) error {
	if through < 0 || visit == nil {
		return spi.ErrInvalidArgument
	}
	where := []string{"entry_no IS NOT NULL", "entry_no<=?"}
	args := []any{through}
	if input.Flag != 0 {
		switch input.Flag {
		case WindowTurn, WindowGoal, WindowContext, WindowModel, WindowInteraction, WindowResponse, WindowSummary:
		default:
			return spi.ErrInvalidArgument
		}
		where = append(where, "(view_flags & "+strconv.FormatInt(input.Flag, 10)+") != 0")
	}
	if input.Output != "" {
		where = append(where, "(content_source=? OR reasoning_source=?)")
		args = append(args, input.Output, input.Output)
	}
	if input.Call != "" {
		where = append(where, "call_source=?")
		args = append(args, input.Call)
	}
	if input.Turn != "" {
		where = append(where, "turn_id=?")
		args = append(args, input.Turn)
	}
	if input.Run != "" {
		where = append(where, "run_id=?")
		args = append(args, input.Run)
	}
	if len(input.Kinds) > 0 {
		where = append(where, "kind IN ("+strings.TrimSuffix(strings.Repeat("?,", len(input.Kinds)), ",")+")")
		for _, kind := range input.Kinds {
			args = append(args, kind)
		}
	}
	order := " ASC"
	if input.Reverse {
		order = " DESC"
	}
	rows, err := journal.tx.QueryContext(journal.ctx, `SELECT entry_no FROM `+journal.database+`history_events WHERE `+strings.Join(where, " AND ")+" ORDER BY entry_no"+order, args...)
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
		stop, err := visit(event)
		if err != nil || stop {
			return err
		}
	}
	return rows.Err()
}
