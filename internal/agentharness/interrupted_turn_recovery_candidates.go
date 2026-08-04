package agentharness

import (
	"context"
	"errors"

	"github.com/floegence/floret/v3/internal/sessiontree"
)

// InterruptedTurnRecoveryCandidate identifies a thread that needs host-side
// recovery binding. It carries identity only; recovery authority stays in the
// runtime capability issued for that exact thread.
type InterruptedTurnRecoveryCandidate struct {
	ThreadID       string
	ParentThreadID string
}

type interruptedTurnRecoveryCandidateReader interface {
	ListInterruptedTurnRecoveryCandidates(context.Context) ([]sessiontree.InterruptedTurnRecoveryCandidate, error)
}

// ListInterruptedTurnRecoveryCandidates performs one store-wide read of the
// canonical authority state and returns deterministic recovery candidates.
func (h *AgentHarness) ListInterruptedTurnRecoveryCandidates(ctx context.Context) ([]InterruptedTurnRecoveryCandidate, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness is nil")
	}
	reader, ok := h.options.Repo.(interruptedTurnRecoveryCandidateReader)
	if !ok {
		return nil, errors.New("session tree repo does not support interrupted turn recovery candidate discovery")
	}
	items, err := reader.ListInterruptedTurnRecoveryCandidates(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]InterruptedTurnRecoveryCandidate, 0, len(items))
	for _, item := range items {
		out = append(out, InterruptedTurnRecoveryCandidate{ThreadID: item.ThreadID, ParentThreadID: item.ParentThreadID})
	}
	return out, nil
}
