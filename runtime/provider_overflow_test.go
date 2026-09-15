package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	internalprovider "github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/provider"
)

func TestPublicProviderOverflowReachesEngineOnBothErrorPaths(t *testing.T) {
	cause := &provider.ProviderHTTPError{Provider: "DeepSeek", StatusCode: 413, Message: "request too large"}
	wrapped := fmt.Errorf("gateway: %w", errors.Join(provider.ErrContextOverflow, cause))
	for _, asynchronous := range []bool{false, true} {
		t.Run(fmt.Sprintf("event=%t", asynchronous), func(t *testing.T) {
			stream, err := streamModelGateway(t.Context(), func(context.Context) (<-chan modelEvent, error) {
				if !asynchronous {
					return nil, wrapped
				}
				out := make(chan modelEvent, 1)
				out <- modelEvent{Type: modelEventType(provider.EventError), Err: wrapped}
				close(out)
				return out, nil
			})
			if asynchronous {
				err = (<-stream).Err
			}
			if !errors.Is(err, internalprovider.ErrContextOverflow) {
				t.Fatalf("engine lost overflow classification: %v", err)
			}
			var httpError *provider.ProviderHTTPError
			if !errors.Is(err, provider.ErrContextOverflow) || !errors.As(err, &httpError) || httpError != cause {
				t.Fatal("public classification or HTTP details lost")
			}
		})
	}
}
