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
	p.applyPolicy(model, policy)
	p.applyStatus(measured)
	p.applyPolicy(model, policy)
	p.applyStatus(estimate)
	if p.usage.Confirmed == nil || p.usage.Estimate == nil {
		t.Fatal("new turn lost measurement")
	}
	cloned := event.CloneThreadContextUsage(p.usage)
	cloned.Confirmed.Usage.WindowInputTokens = 999
	if p.usage.Confirmed.Usage.WindowInputTokens != 100 {
		t.Fatal("snapshot is not detached")
	}
	missing := measured
	missing.Usage.Available = false
	p.applyStatus(missing)
	if p.usage.Confirmed.Usage.WindowInputTokens != 100 || p.usage.Estimate.Usage.Available {
		t.Fatal("missing usage changed measurement")
	}
	for _, phase := range []string{"start", "failed", "cancelled", "noop"} {
		p.applyCompaction(sessiontree.ThreadContextCompaction{Phase: phase})
		if p.usage.Confirmed == nil {
			t.Fatalf("%s cleared measurement", phase)
		}
	}
	p.applyCompaction(sessiontree.ThreadContextCompaction{Phase: "complete"})
	if p.usage == nil || p.usage.Confirmed != nil || p.usage.Estimate != nil {
		t.Fatal("successful compaction did not clear")
	}
	for _, change := range []string{"model", "budget"} {
		p.applyStatus(measured)
		if change == "model" {
			model.Model = "other"
		} else {
			policy.ContextWindowTokens = 2000
		}
		p.applyPolicy(model, policy)
		if p.usage.Confirmed != nil {
			t.Fatalf("%s did not clear", change)
		}
	}
}
