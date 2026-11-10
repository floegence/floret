package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadTurnFailureInput identifies one exact execution on the active path.
// Identities locate facts and never grant access.
type ThreadTurnFailureInput struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	TurnID   identity.TurnID   `json:"turn_id"`
	RunID    identity.RunID    `json:"run_id"`
}

// ThreadTurnFailureReader reads complete canonical failure evidence on explicit
// demand, including historical turns after continuation and restart. The host
// must freshly authorize Scope. A turn without a failure returns nil; a stale
// execution identity is unavailable. Evidence is untrusted, not a retry signal.
type ThreadTurnFailureReader interface {
	ReadTurnFailure(context.Context, threadquery.Scope, ThreadTurnFailureInput) (*ThreadTurnFailure, error)
}

var _ ThreadTurnFailureReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadTurnFailure(ctx context.Context, scope threadquery.Scope, input ThreadTurnFailureInput) (*ThreadTurnFailure, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return nil, err
	}
	if input.TurnID == "" || input.RunID == "" {
		return nil, threadquery.ErrInvalidQuery
	}
	reader, ok := service.host.store.repo.(sessiontree.CanonicalTurnReadRepo)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	read, err := reader.ReadCanonicalTurn(ctx, input.ThreadID.String(), input.TurnID.String())
	if err != nil {
		return nil, runtimeHostError(err)
	}
	var projection threadLifecycleProjection
	for _, item := range read.Turn.Entries {
		projection.observe(item.Entry)
	}
	turnID, runID, _, _, failure := projection.snapshot()
	if turnID != input.TurnID || runID != input.RunID {
		return nil, threadquery.ErrRecordUnavailable
	}
	return cloneThreadTurnFailure(failure), nil
}
