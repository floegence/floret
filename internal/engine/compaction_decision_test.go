package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/observation"
)

type decisionFailingCompactor struct{ err error }

func (c decisionFailingCompactor) Compact(context.Context, CompactionRequest) (compaction.Result, []session.Message, error) {
	return compaction.Result{}, nil, c.err
}

func TestCompactionFailureAndCancellationKeepTriggerEvidence(t *testing.T) {
	for _, cause := range []error{errors.New("summary unavailable"), context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			rec := &event.Recorder{}
			e := &Engine{sink: rec, compactor: decisionFailingCompactor{cause}}
			policy := contextpolicy.Policy{ContextWindowTokens: 1000, ReservedOutputTokens: 100}
			request := provider.Request{Provider: "test", Model: "model", ContextPolicy: policy, RequestEstimate: contextpolicy.RequestEstimate{EstimatedInputTokens: 1000, Source: "fixture", Method: contextpolicy.EstimateMethodProviderRenderedPayload}}
			request.RawPlan.PayloadHash = "frozen"
			request.ContextPressure = contextpolicy.PressureFromProjectedRequest(request.RequestEstimate, contextpolicy.RequestDeltaEstimate{}, policy)
			candidate := compactionRequestSnapshot(request, nil)
			_, _, _, err := e.runCompaction(t.Context(), Options{RunID: "run", ThreadID: "thread", TurnID: "turn", Model: "model", ContextPolicy: policy}, 1, []session.Message{{Role: session.User, Content: "old"}, {Role: session.Assistant, Content: "answer"}, {Role: session.User, Content: "new"}}, NewContextPressureTracker("scope"), 1, false, compaction.TriggerPreRequest, compaction.ReasonThreshold, contextpolicy.Usage{InputTokens: 1000}, nil, request.ContextPressure, ManualCompactionRequest{}, candidate, ContextCompactDebugNextActionProviderRequest)
			if !errors.Is(err, cause) {
				t.Fatalf("err=%v", err)
			}
			var terminal map[string]any
			for _, ev := range rec.Events {
				if ev.Type == event.ContextCompact {
					terminal = ev.Metadata.(map[string]any)
				}
			}
			d, ok := terminal["decision"].(*observation.CompactionDecision)
			if !ok || d.Before == nil || d.Before.PayloadFingerprint != "frozen" || d.After != nil {
				t.Fatalf("lost evidence=%+v", terminal)
			}
			want := "failed"
			if errors.Is(cause, context.Canceled) {
				want = "cancelled"
			}
			if terminal["phase"] != want {
				t.Fatalf("terminal=%+v", terminal)
			}
		})
	}
}
