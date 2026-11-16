package tools

import (
	"context"

	"github.com/floegence/floret/v7/internal/toolcontext"
)

// YieldSignal closes when the current model fragment ends, including steering.
// A tool that observes independently managed work may return its current facts
// early when this signal closes. This is not cancellation, completion, or
// permission to detach work: effects remain bound to ctx and the host's existing
// work lifecycle. Tools without a safe early result should finish normally.
// Outside a managed fragment it may be nil; ctx.Done remains authoritative for
// Stop, shutdown, deadlines, and host cancellation.
func YieldSignal(ctx context.Context) <-chan struct{} {
	return toolcontext.Yield(ctx)
}
