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
	where := []string{"item.entry_no<=?"}
	args := []any{through}
	if input.Flag != 0 {
		switch input.Flag {
		case WindowTurn, WindowGoal, WindowContext, WindowModel, WindowInteraction, WindowResponse, WindowSummary:
		default:
			return spi.ErrInvalidArgument
		}
		where = append(where, "(item.view_flags & "+strconv.FormatInt(input.Flag, 10)+") != 0")
	}
	if input.Output != "" {
		where = append(where, "(item.content_source=? OR item.reasoning_source=?)")
		args = append(args, input.Output, input.Output)
	}
	if input.Call != "" {
		where = append(where, "item.call_source=?")
		args = append(args, input.Call)
	}
	if input.Turn != "" {
		where = append(where, "item.turn_id=?")
		args = append(args, input.Turn)
	}
	if input.Run != "" {
		where = append(where, "item.run_id=?")
		args = append(args, input.Run)
	}
	if len(input.Kinds) > 0 {
		where = append(where, "item.kind IN ("+strings.TrimSuffix(strings.Repeat("?,", len(input.Kinds)), ",")+")")
		for _, kind := range input.Kinds {
			args = append(args, kind)
		}
	}
	order := " ASC"
	if input.Reverse {
		order = " DESC"
	}
	rows, err := journal.tx.QueryContext(journal.ctx, journal.entrySelect()+strings.Join(where, " AND ")+" ORDER BY item.entry_no"+order, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		event, err := journal.scanEntry(rows)
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
