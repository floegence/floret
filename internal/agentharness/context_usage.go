package agentharness

import (
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

// contextUsageProjection folds committed context facts for both live execution
// and canonical reads. It retains only the last measurement and current estimate.
type contextUsageProjection struct {
	model  ThreadContextModel
	policy ThreadContextPolicy
	usage  *event.ThreadContextUsage
}

func (p *contextUsageProjection) applyPolicy(model ThreadContextModel, policy ThreadContextPolicy) {
	if p.usage == nil || p.model != model || p.policy != policy {
		p.usage = &event.ThreadContextUsage{}
	}
	p.model, p.policy = model, policy
}

func (p *contextUsageProjection) applyStatus(status observation.ContextStatus) {
	if p.usage == nil {
		p.usage = &event.ThreadContextUsage{}
	}
	if status.Phase == observation.ContextPhaseProviderUsage && status.Usage.Available {
		p.usage.Confirmed = &status
		p.usage.Estimate = nil
	} else {
		p.usage.Estimate = &status
	}
}

func (p *contextUsageProjection) applyCompaction(compact sessiontree.ThreadContextCompaction) {
	if compact.Phase == string(observation.CompactionPhaseComplete) {
		p.usage = &event.ThreadContextUsage{}
	}
}
