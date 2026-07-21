package runtime

import (
	"context"
	"testing"

	"github.com/floegence/floret/internal/agentharness"
)

type runtimeEffectContextKey struct{}

func TestRuntimeEffectAuthorizationGatePreservesSelectedContext(t *testing.T) {
	gate := EffectAuthorizationGateFunc(func(ctx context.Context, _ EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(context.WithValue(ctx, runtimeEffectContextKey{}, "selected"), EffectAuthorizationProof{})
	})
	adapted := runtimeEffectAuthorizationGate(gate)
	if adapted == nil {
		t.Fatal("runtime effect gate adapter is nil")
	}
	_, err := adapted.Dispatch(context.Background(), agentharness.EffectAuthorizationRequest{}, func(ctx context.Context, _ agentharness.EffectAuthorizationProof) (agentharness.EffectDispatchResult, error) {
		if got := ctx.Value(runtimeEffectContextKey{}); got != "selected" {
			t.Fatalf("adapted context value = %v, want selected", got)
		}
		return agentharness.EffectDispatchResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
