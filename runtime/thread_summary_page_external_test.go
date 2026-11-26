package runtime_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/threadquery"
)

type summaryPageRead func(context.Context, identity.ThreadID) (runtime.ThreadSummary, error)

func (f summaryPageRead) Summary(ctx context.Context, id identity.ThreadID) (runtime.ThreadSummary, error) {
	return f(ctx, id)
}

func TestAuthorizedSummaryPage(t *testing.T) {
	ids := []identity.ThreadID{"z", "a", "c", "b"}
	var reads []identity.ThreadID
	reader := summaryPageRead(func(_ context.Context, id identity.ThreadID) (runtime.ThreadSummary, error) {
		reads = append(reads, id)
		if id == "c" {
			return runtime.ThreadSummary{}, runtime.ErrAuthorityCorrupt
		}
		return runtime.ThreadSummary{ID: id}, nil
	})
	checks := 0
	rows, err := runtime.ReadThreadSummaryPage(t.Context(), reader, ids, func(_ context.Context, page []identity.ThreadID) ([]identity.ThreadID, error) {
		checks++
		if checks == 1 {
			return []identity.ThreadID{"b", "z", "c"}, nil
		}
		if !slices.Equal(page, []identity.ThreadID{"z", "c", "b"}) {
			t.Fatalf("publication check: %v", page)
		}
		return []identity.ThreadID{"c", "z"}, nil
	})
	if err != nil || checks != 2 || !slices.Equal(reads, []identity.ThreadID{"z", "c", "b"}) || len(rows) != 2 || rows[0].Summary.ID != "z" || rows[1].ThreadID != "c" || !errors.Is(rows[1].Err, runtime.ErrAuthorityCorrupt) {
		t.Fatalf("rows=%v checks=%d reads=%v err=%v", rows, checks, reads, err)
	}
	for _, answer := range [][]identity.ThreadID{nil, {"other"}, {"z", "z"}} {
		reads = nil
		rows, err := runtime.ReadThreadSummaryPage(t.Context(), reader, ids, func(context.Context, []identity.ThreadID) ([]identity.ThreadID, error) { return answer, nil })
		if len(rows) != 0 || len(reads) != 0 || (len(answer) > 0) != errors.Is(err, threadquery.ErrInvalidQuery) {
			t.Fatalf("answer=%v rows=%v reads=%v err=%v", answer, rows, reads, err)
		}
	}
}

func TestSummaryPageCancellationAndInvalidCandidates(t *testing.T) {
	allow := func(_ context.Context, ids []identity.ThreadID) ([]identity.ThreadID, error) { return ids, nil }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reads := 0
	reader := summaryPageRead(func(_ context.Context, id identity.ThreadID) (runtime.ThreadSummary, error) {
		reads++
		cancel()
		return runtime.ThreadSummary{ID: id}, nil
	})
	for _, ids := range [][]identity.ThreadID{{""}, {"a", "a"}, make([]identity.ThreadID, 201)} {
		if _, err := runtime.ReadThreadSummaryPage(ctx, reader, ids, allow); !errors.Is(err, threadquery.ErrInvalidQuery) {
			t.Fatalf("invalid page: %v", err)
		}
	}
	rows, err := runtime.ReadThreadSummaryPage(ctx, reader, []identity.ThreadID{"a", "b"}, allow)
	if len(rows) != 0 || reads != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("rows=%v reads=%d err=%v", rows, reads, err)
	}
}
