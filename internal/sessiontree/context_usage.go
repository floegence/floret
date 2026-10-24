package sessiontree

import (
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/observation"
)

// ContextUsageProjection folds committed context facts for both live execution
// and canonical reads. It retains only the last measurement and current estimate.
type ContextUsageProjection struct {
	Model  ThreadContextModel
	Policy ThreadContextPolicy
	Usage  *event.ThreadContextUsage
}

func (p *ContextUsageProjection) ApplyPolicy(model ThreadContextModel, policy ThreadContextPolicy) {
	if p.Usage == nil || p.Model != model || p.Policy != policy {
		p.Usage = &event.ThreadContextUsage{}
	}
	p.Model, p.Policy = model, policy
}

func (p *ContextUsageProjection) ApplyStatus(status observation.ContextStatus) {
	if p.Usage == nil {
		p.Usage = &event.ThreadContextUsage{}
	}
	if status.Phase == observation.ContextPhaseProviderUsage && status.Usage.Available && status.Usage.HasInputUsage() {
		p.Usage.Confirmed = &status
		p.Usage.Estimate = nil
	} else {
		p.Usage.Estimate = &status
	}
}

func (p *ContextUsageProjection) ApplyCompaction(compact ThreadContextCompaction) {
	if compact.Phase == string(observation.CompactionPhaseComplete) {
		p.Usage = &event.ThreadContextUsage{}
	}
}
