package agentharness

import (
	"context"
	"errors"
	"time"
)

// GracefulCancellation is the runtime owner's cancellation cause. One absolute
// deadline bounds every in-flight effect and its existing result finalizer.
type GracefulCancellation struct {
	Deadline time.Time
}

func (*GracefulCancellation) Error() string { return "user requested graceful cancellation" }
func (*GracefulCancellation) Unwrap() error { return context.Canceled }

func gracefulCancellation(ctx context.Context) (*GracefulCancellation, bool) {
	var cause *GracefulCancellation
	if ctx == nil || !errors.As(context.Cause(ctx), &cause) {
		return nil, false
	}
	return cause, true
}

func effectSettlementContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if cause, ok := gracefulCancellation(ctx); ok {
		return context.WithDeadline(context.WithoutCancel(ctx), cause.Deadline)
	}
	return ctx, func() {}
}
