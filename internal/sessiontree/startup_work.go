package sessiontree

import "context"

type startupWorkKey struct{}

func WithStartupWork(ctx context.Context, observer func(StartupPhase, int64, int64)) context.Context {
	return context.WithValue(ctx, startupWorkKey{}, observer)
}
func reportStartupWork(ctx context.Context, phase StartupPhase, completed, total int64) {
	if observer, ok := ctx.Value(startupWorkKey{}).(func(StartupPhase, int64, int64)); ok && observer != nil {
		observer(phase, completed, total)
	}
}
