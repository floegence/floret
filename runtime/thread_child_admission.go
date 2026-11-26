package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
)

// Child ownership is immutable. At the point of mutation, also fence the
// concrete execution that delegated the command, using Delete's lock order.
type childAdmission struct {
	parent *threadRuntimeState
	source ThreadExecutionTarget
}

func (s *threadRuntimeService) childAdmission(ctx context.Context, source *ThreadExecutionTarget, child identity.ThreadID) (*childAdmission, error) {
	if source == nil {
		return nil, nil
	}
	if source.ThreadID == "" || source.TurnID == "" || source.RunID == "" || source.ThreadID == child {
		return nil, ErrRequestConflict
	}
	meta, err := s.host.store.repo.Thread(ctx, child.String())
	if err != nil {
		return nil, runtimeHostError(err)
	}
	if meta.ParentThreadID != source.ThreadID.String() {
		return nil, ErrRequestConflict
	}
	return &childAdmission{s.runtime(source.ThreadID), *source}, nil
}

func (g *childAdmission) apply(ctx context.Context, child *threadRuntimeState, mutate func() error) error {
	if g == nil {
		return child.apply(ctx, mutate)
	}
	first, second := g.parent, child
	if first.threadID > second.threadID {
		first, second = second, first
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()
	p := g.parent
	if p.closed || p.deleting || p.deleted || p.state.turnID != g.source.TurnID || p.state.runID != g.source.RunID || p.state.view.Activity != ThreadActivityActive || p.state.cancel == nil || p.state.executionSealed || p.state.view.Cancellation != nil {
		return ErrRequestConflict
	}
	return child.applyLocked(ctx, mutate)
}
