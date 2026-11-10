package engine_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/testing/harness"
)

// Present the last immutable request as written by the previous projection.
// The actual history and the provider's opaque response remain unchanged.
type priorProjectionStore struct{ cache.Store }

func (s priorProjectionStore) ProviderRequests(ctx context.Context, scope string) ([]cache.ProviderRequestRecord, error) {
	records, err := s.Store.ProviderRequests(ctx, scope)
	for i := range records {
		records[i].ContextProjectionRevision = "provider-context.v11"
		records[i].RenderLineageKey = "released-render-lineage"
	}
	return records, err
}

func TestProjectionUpgradePreservesOnlyMatchingNativeHistory(t *testing.T) {
	for _, policy := range []provider.StateReusePolicy{provider.StateReuseExactSurface, provider.StateReuseCanonicalHistory} {
		for _, interrupted := range []bool{false, true} {
			name := string(policy) + "/unchanged"
			if interrupted {
				name = string(policy) + "/interruption_inserted"
			}
			t.Run(name, func(t *testing.T) {
				prompt := cache.NewMemoryStore()
				state := &provider.State{Kind: "native", ID: "opaque-history", ReusePolicy: policy, Attributes: map[string]string{"receipt": "native-search-and-reasoning"}}
				first := newTestEngine(harness.NewScriptedProvider(harness.Step(harness.Text("answer"), provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: state})), &event.Recorder{})
				first.Prompt = prompt
				if err := first.Store.AppendTranscript("run", session.Message{Role: session.User, Content: "earlier"}, session.Message{Role: session.Assistant, Content: "partial"}); err != nil {
					t.Fatal(err)
				}
				result := first.Run(t.Context(), "first")
				if result.Err != nil {
					t.Fatal(result.Err)
				}
				history := append([]session.Message(nil), result.Messages...)
				if interrupted {
					history = append(history[:2:2], append([]session.Message{{Role: session.User, Kind: session.MessageKindTurnInterruption, Content: `{"type":"turn_interruption"}`}}, history[2:]...)...)
				}
				p := harness.NewScriptedProvider(harness.Step(harness.Text("next"), harness.Done()))
				second := newTestEngine(p, &event.Recorder{})
				second.Prompt = priorProjectionStore{Store: prompt}
				second.Options.RunID, second.Options.TurnID, second.Options.PromptScopeID = "second", "second", "run"
				second.Options.PreviousProviderState = result.ProviderState
				if err := second.Store.AppendTranscript("second", history...); err != nil {
					t.Fatal(err)
				}
				if got := second.Run(t.Context(), "continue"); got.Err != nil {
					t.Fatal(got.Err)
				}
				if len(p.Requests) != 1 {
					t.Fatalf("requests=%d", len(p.Requests))
				}
				request := p.Requests[0]
				wantReuse := policy == provider.StateReuseCanonicalHistory && !interrupted
				if wantReuse {
					if request.PreviousState == nil || request.PreviousState.Attributes["receipt"] != "native-search-and-reasoning" {
						t.Fatal("unchanged history lost opaque provider facts on projection upgrade")
					}
				} else if request.PreviousState != nil {
					t.Fatal("incompatible state crossed projection upgrade")
				}
				if request.RawPlan.CanonicalLineageReset != interrupted {
					t.Fatalf("reset=%v interrupted=%v", request.RawPlan.CanonicalLineageReset, interrupted)
				}
				if request.RawPlan.PreviousResponseID != "" {
					t.Fatal("server continuation crossed render boundary")
				}
			})
		}
	}
}
