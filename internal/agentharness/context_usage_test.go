package agentharness

import (
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"testing"
)

func TestContextUsageProjectionBoundaries(t *testing.T) {
	model := ThreadContextModel{Provider: "test", Model: "model"}
	policy := ThreadContextPolicy{ContextWindowTokens: 1000}
	measured := observation.ContextStatus{Phase: observation.ContextPhaseProviderUsage, Usage: observation.ProviderUsage{Available: true, WindowInputTokens: 100}}
	estimate := observation.ContextStatus{Phase: observation.ContextPhaseProjectedRequest}
	p := contextUsageProjection{}
	p.ApplyPolicy(model, policy)
	p.ApplyStatus(measured)
	p.ApplyPolicy(model, policy)
	p.ApplyStatus(estimate)
	if p.Usage.Confirmed == nil || p.Usage.Estimate == nil {
		t.Fatal("new turn lost measurement")
	}
	cloned := event.CloneThreadContextUsage(p.Usage)
	cloned.Confirmed.Usage.WindowInputTokens = 999
	if p.Usage.Confirmed.Usage.WindowInputTokens != 100 {
		t.Fatal("snapshot is not detached")
	}
	missing := measured
	missing.Usage.Available = false
	p.ApplyStatus(missing)
	if p.Usage.Confirmed.Usage.WindowInputTokens != 100 || p.Usage.Estimate.Usage.Available {
		t.Fatal("missing usage changed measurement")
	}
	for _, phase := range []string{"start", "failed", "cancelled", "noop"} {
		p.ApplyCompaction(sessiontree.ThreadContextCompaction{Phase: phase})
		if p.Usage.Confirmed == nil {
			t.Fatalf("%s cleared measurement", phase)
		}
	}
	p.ApplyCompaction(sessiontree.ThreadContextCompaction{Phase: "complete"})
	if p.Usage == nil || p.Usage.Confirmed != nil || p.Usage.Estimate != nil {
		t.Fatal("successful compaction did not clear")
	}
	for _, change := range []string{"model", "budget"} {
		p.ApplyStatus(measured)
		if change == "model" {
			model.Model = "other"
		} else {
			policy.ContextWindowTokens = 2000
		}
		p.ApplyPolicy(model, policy)
		if p.Usage.Confirmed != nil {
			t.Fatalf("%s did not clear", change)
		}
	}
}
