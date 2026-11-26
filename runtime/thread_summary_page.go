package runtime

import (
	"context"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadSummaryResult contains either the canonical summary or a read error
// for an authorized thread. A failed read does not expose partial data.
type ThreadSummaryResult struct {
	ThreadID identity.ThreadID
	Summary  ThreadSummary
	Err      error
}

// ReadThreadSummaryPage reads up to 200 distinct candidates in host order.
// authorize must return a currently authorized subset of the supplied IDs.
// It is called before reading and again before returning nonempty results.
// Neither a candidate ID nor a previous page confers access.
func ReadThreadSummaryPage(ctx context.Context, reader ThreadSummaryReader, candidates []identity.ThreadID, authorize func(context.Context, []identity.ThreadID) ([]identity.ThreadID, error)) ([]ThreadSummaryResult, error) {
	if ctx == nil || reader == nil || authorize == nil || len(candidates) > 200 {
		return nil, threadquery.ErrInvalidQuery
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := slices.Clone(candidates)
	seen := make(map[identity.ThreadID]bool, len(ids))
	for _, id := range ids {
		if parsed, err := identity.ParseThreadID(id.String()); err != nil || parsed != id || seen[id] {
			return nil, threadquery.ErrInvalidQuery
		}
		seen[id] = true
	}
	check := func(page []identity.ThreadID, permitted map[identity.ThreadID]bool) (map[identity.ThreadID]bool, error) {
		granted, err := authorize(ctx, slices.Clone(page))
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(granted) > len(page) {
			return nil, threadquery.ErrInvalidQuery
		}
		out := make(map[identity.ThreadID]bool, len(granted))
		for _, id := range granted {
			if !permitted[id] || out[id] {
				return nil, threadquery.ErrInvalidQuery
			}
			out[id] = true
		}
		return out, nil
	}
	allowed, err := check(ids, seen)
	if err != nil || len(allowed) == 0 {
		return nil, err
	}
	rows := make([]ThreadSummaryResult, 0, len(allowed))
	read := make([]identity.ThreadID, 0, len(allowed))
	for _, id := range ids {
		if !allowed[id] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		summary, err := reader.Summary(ctx, id)
		if err == nil && summary.ID != id {
			err = ErrAuthorityCorrupt
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			summary = ThreadSummary{}
		}
		rows = append(rows, ThreadSummaryResult{ThreadID: id, Summary: summary, Err: err})
		read = append(read, id)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	final, err := check(read, allowed)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(rows, func(row ThreadSummaryResult) bool { return !final[row.ThreadID] }), nil
}
