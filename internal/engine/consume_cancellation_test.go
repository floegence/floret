package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/internal/provider"
)

func TestProviderStreamCloseErrorPrefersContextCancellation(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := providerStreamCloseError(ctx, &provider.StreamValidator{})
		if !errors.Is(err, context.Canceled) || errors.Is(err, provider.ErrStreamMissingTerminal) {
			t.Fatalf("provider stream close error = %v, want context cancellation", err)
		}
	})

	t.Run("active", func(t *testing.T) {
		err := providerStreamCloseError(context.Background(), &provider.StreamValidator{})
		if !errors.Is(err, provider.ErrStreamMissingTerminal) {
			t.Fatalf("provider stream close error = %v, want missing terminal", err)
		}
	})
}
