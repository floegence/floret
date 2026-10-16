package engine

import (
	"context"
	"github.com/floegence/floret/v7/internal/session"
)

// A logical request boundary is the sole admission point. Natural completion
// does not drain pending child events or force another provider request.
func (e *Engine) admitParentEvents(ctx context.Context, opts Options, history []session.Message) ([]session.Message, error) {
	if opts.AdmitParentEvents == nil {
		return history, nil
	}
	message, err := opts.AdmitParentEvents(ctx)
	if err != nil {
		return nil, withFailureOrigin(err, FailureOriginStorage)
	}
	if message.EntryID == "" {
		return history, nil
	}
	if err := e.store.AppendTranscript(opts.RunID, message); err != nil {
		return nil, withFailureOrigin(err, FailureOriginStorage)
	}
	return append(history, message), nil
}
