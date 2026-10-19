package runtime

import (
	"github.com/floegence/floret/v7/internal/event"
)

func cloneThreadContextSnapshot(in *ThreadContextSnapshot) *ThreadContextSnapshot {
	if in == nil {
		return nil
	}
	out := *in
	if in.ModelContext != nil {
		model := *in.ModelContext
		model.Snapshot = *runtimeModelContext(sessionModelContext(&in.ModelContext.Snapshot))
		model.ChangedKeys = append([]string(nil), model.ChangedKeys...)
		model.RemovedKeys = append([]string(nil), model.RemovedKeys...)
		if model.UsedAt != nil {
			at := *model.UsedAt
			model.UsedAt = &at
		}
		out.ModelContext = &model
	}
	if in.ContextUsage != nil {
		out.ContextUsage = runtimeThreadContextUsage(&event.ThreadContextUsage{Confirmed: in.ContextUsage.Confirmed, Estimate: in.ContextUsage.Estimate})
	}
	if in.Usage != nil {
		out.Usage = event.CloneThreadContextUsage(&event.ThreadContextUsage{Estimate: in.Usage}).Estimate
	}
	if in.UsageTotals != nil {
		totals := *in.UsageTotals
		out.UsageTotals = &totals
	}
	out.Compactions = make([]ThreadContextCompaction, len(in.Compactions))
	copy(out.Compactions, in.Compactions)
	for i := range out.Compactions {
		out.Compactions[i].Decision = in.Compactions[i].Decision.Clone()
	}
	return &out
}
