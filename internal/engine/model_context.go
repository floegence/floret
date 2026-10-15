package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

var ErrModelContextUnavailable = errors.New("model context unavailable")

// admitModelContext runs once at a logical request boundary, before budgets or
// provider preparation. Transport retries replay that prepared request unchanged.
func (e *Engine) admitModelContext(ctx context.Context, opts Options, history []session.Message) ([]session.Message, error) {
	next, err := session.NormalizeModelContext(opts.toolSurface.modelContextSnapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrModelContextUnavailable, err)
	}
	if next == nil || session.EqualModelContext(session.CurrentModelContext(history), next) {
		return history, nil
	}
	message := session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: next}
	if opts.AdmitModelContext != nil {
		message, err = opts.AdmitModelContext(ctx, next)
		if err != nil {
			return nil, withFailureOrigin(err, FailureOriginStorage)
		}
	} else {
		message = e.stableMessage(opts.RunID, message)
	}
	if err := e.store.AppendTranscript(opts.RunID, message); err != nil {
		return nil, withFailureOrigin(err, FailureOriginStorage)
	}
	return append(history, message), nil
}

func modelContextEntryID(history []session.Message) string {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].ModelContextSnapshot != nil {
			return history[i].EntryID
		}
	}
	return ""
}

func modelContextFixedTokens(history []session.Message) int64 {
	snapshot := session.CurrentModelContext(history)
	if snapshot == nil {
		return 0
	}
	return contextpolicy.EstimateTextTokens(session.RenderModelContext(nil, snapshot)) + 8
}
