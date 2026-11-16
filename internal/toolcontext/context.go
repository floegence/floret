// Package toolcontext carries the engine's execution boundaries into tool dispatch.
package toolcontext

import "context"

type turnKey struct{}
type yieldKey struct{}

func WithTurn(fragment, turn context.Context) context.Context {
	return context.WithValue(fragment, turnKey{}, turn)
}

func Turn(ctx context.Context) context.Context {
	if turn, ok := ctx.Value(turnKey{}).(context.Context); ok {
		return turn
	}
	return ctx
}

func WithYield(ctx context.Context, signal <-chan struct{}) context.Context {
	return context.WithValue(ctx, yieldKey{}, signal)
}

func Yield(ctx context.Context) <-chan struct{} {
	signal, _ := ctx.Value(yieldKey{}).(<-chan struct{})
	return signal
}
