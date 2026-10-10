package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/testing/harness"
)

func TestInvalidContextBudgetStopsBeforeProviderOrCompaction(t *testing.T) {
	for _, window := range []int64{3891, 4096} {
		p := harness.NewScriptedProvider(harness.Step(harness.Text("never sent"), harness.Done()))
		rec := &event.Recorder{}
		compactor := &countingLocalCompactor{manager: engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}}
		e := newTestEngine(p, rec)
		e.Compactor = compactor
		e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: window, MaxOutputTokens: 4096}
		got := e.Run(context.Background(), "weather")
		if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "invalid context budget") {
			t.Fatalf("window %d: result=%#v, want invalid context budget", window, got)
		}
		if len(p.Requests) != 0 || compactor.calls != 0 || len(eventsOfType(rec.Events, event.ContextCompact)) != 0 {
			t.Fatalf("window %d: impossible budget attempted provider or compaction", window)
		}
	}
}

func TestFixedContextOverheadStopsBeforeCompaction(t *testing.T) {
	p := &estimatingProvider{
		Provider:  harness.NewScriptedProvider(harness.Step(harness.Text("never sent"), harness.Done())),
		estimates: []provider.TokenEstimate{{PrefixTokens: 800, MessageTokens: 300, ToolDefinitionTokens: 200, EstimatedInputTokens: 1300, Source: "request_estimator_test", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative}},
	}
	rec := &event.Recorder{}
	compactor := &countingLocalCompactor{manager: engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}}
	e := newTestEngine(p, rec)
	e.Compactor = compactor
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 1000, ReservedOutputTokens: 100}
	got := e.Run(context.Background(), "weather")
	if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrFixedContextOverBudget) {
		t.Fatalf("result=%#v, want fixed overhead failure", got)
	}
	if len(p.Provider.(*harness.ScriptedProvider).Requests) != 0 || compactor.calls != 0 || len(eventsOfType(rec.Events, event.ContextCompact)) != 0 {
		t.Fatal("fixed overhead attempted provider or compaction")
	}
}
