package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

type countingLifecycleProvider struct {
	*preparedLifecycleProvider
	count func(context.Context) (int64, bool)
}
type countingLifecycleHandle struct {
	provider.PreparedRequest
	count func(context.Context) (int64, bool)
}

func (h countingLifecycleHandle) CountPreparedInput(ctx context.Context) (int64, bool) {
	return h.count(ctx)
}
func (p countingLifecycleProvider) PrepareRequest(ctx context.Context, req provider.Request) (provider.PreparedRequest, error) {
	h, err := p.preparedLifecycleProvider.PrepareRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return countingLifecycleHandle{h, p.count}, nil
}

func TestOrdinaryPressureCountBeforeCompaction(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		estimate, count          int64
		available, compact, stop bool
		calls                    int
	}{
		{"verified below", 368225, 289000, true, false, false, 1}, {"verified above", 368225, 291000, true, true, false, 1},
		{"exact threshold", 368225, 290430, true, true, false, 1}, {"unavailable", 368225, 0, false, true, false, 1},
		{"below without count", 280000, 0, false, false, false, 0}, {"stop during count", 368225, 0, false, false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			p := countingLifecycleProvider{newPreparedLifecycleProvider(preparedBehavior{estimate: preparedEstimate(tc.estimate), events: preparedDone("original")}, preparedBehavior{estimate: preparedEstimate(10), events: preparedDone("replacement")}), func(context.Context) (int64, bool) {
				calls++
				if tc.stop {
					cancel()
				}
				return tc.count, tc.available
			}}
			recorder := &event.Recorder{}
			e := newTestEngine(p, recorder)
			store := session.NewMemoryStore()
			e.Store = store
			if err := store.AppendTranscript("run", session.Message{Role: session.User, Content: "older", EntryID: "u1"}, session.Message{Role: session.Assistant, Content: strings.Repeat("retained evidence ", 65000), EntryID: "a1"}, session.Message{Role: session.User, Content: "recent", EntryID: "u2"}, session.Message{Role: session.Assistant, Content: "recent result", EntryID: "a2"}, session.Message{Role: session.User, Content: "new", EntryID: "u3"}); err != nil {
				t.Fatal(err)
			}
			e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
			e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 322700, ReservedOutputTokens: 3228, ReservedSummaryTokens: 80, RecentTailTokens: 20, RecentUserTokens: 20}
			got := e.Run(ctx, "")
			if calls != tc.calls {
				t.Fatalf("calls=%d want %d, result=%s", calls, tc.calls, got.Status)
			}
			if tc.stop {
				if got.Status == engine.Completed || got.Metrics.Compactions != 0 {
					t.Fatalf("continued after stop: %+v", got)
				}
				assertPreparedHandlesTerminated(t, p.preparedLifecycleProvider, []int{0})
				return
			}
			want := "original"
			streams := []int{1}
			if tc.compact {
				want = "replacement"
				streams = []int{0, 1}
			}
			if got.Status != engine.Completed || got.Output != want || (got.Metrics.Compactions != 0) != tc.compact {
				for _, ev := range recorder.Snapshot() {
					t.Log(ev.Type, ev.Message)
				}
				t.Fatalf("status=%s output=%s compactions=%d err=%v", got.Status, got.Output, got.Metrics.Compactions, got.Err)
			}
			assertPreparedHandlesTerminated(t, p.preparedLifecycleProvider, streams)
		})
	}
}
