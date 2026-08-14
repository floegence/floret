package agentharness

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v4/internal/engine"
)

func TestNormalizeCancelledEngineResultOverridesLateProviderFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := normalizeCancelledEngineResult(ctx, engine.Result{
		Status:        engine.Failed,
		Err:           errors.New("provider stream closed after cancellation"),
		FailureOrigin: engine.FailureOriginProvider,
	})

	if result.Status != engine.Cancelled || !errors.Is(result.Err, context.Canceled) || result.FailureOrigin != engine.FailureOriginCancelled {
		t.Fatalf("normalized result = %#v", result)
	}
}
