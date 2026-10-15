package engine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

func TestModelContextNetworkRetriesDoNotCollectOrRenderAgain(t *testing.T) {
	p := &recoveryProvider{failCount: 2}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
	collections := 0
	e.Options.ToolSurfaceProvider = func(ctx context.Context, req engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
		if req.Phase != "provider_request" {
			return engine.ToolSurface{}, nil
		}
		collections++
		return engine.ToolSurface{ModelContextSnapshot: &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "date", Title: "Date", Text: "2026-10-15"}}}}, nil
	}
	result := e.Run(t.Context(), "work")
	if result.Status != engine.Completed || collections != 1 || len(p.requests) != 3 {
		t.Fatalf("result=%+v collections=%d attempts=%d", result, collections, len(p.requests))
	}
	for _, request := range p.requests[1:] {
		if !reflect.DeepEqual(request.Messages, p.requests[0].Messages) {
			t.Fatal("retry changed frozen history")
		}
	}
}

func TestModelContextAdmissionFailureNeverDispatchesProvider(t *testing.T) {
	p := &recoveryProvider{}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ToolSurfaceProvider = func(context.Context, engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
		return engine.ToolSurface{ModelContextSnapshot: &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "date", Title: "Date", Text: "2026-10-15"}}}}, nil
	}
	failure := errors.New("snapshot transaction failed")
	e.Options.AdmitModelContext = func(context.Context, *session.ModelContextSnapshot) (session.Message, error) {
		return session.Message{}, failure
	}
	result := e.Run(t.Context(), "work")
	if result.Status != engine.Failed || result.FailureOrigin != engine.FailureOriginStorage || !errors.Is(result.Err, failure) || len(p.requests) != 0 {
		t.Fatalf("result=%+v requests=%d", result, len(p.requests))
	}
}

func TestModelContextFixedCapacityFailsBeforeCompaction(t *testing.T) {
	p := &recoveryProvider{}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 500, ReservedOutputTokens: 100, ReservedSummaryTokens: 50}
	e.Options.ToolSurfaceProvider = func(context.Context, engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
		return engine.ToolSurface{ModelContextSnapshot: &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "rules", Title: "Rules", Text: strings.Repeat("fact ", 1000)}}}}, nil
	}
	result := e.Run(t.Context(), "work")
	if !errors.Is(result.Err, engine.ErrFixedContextOverBudget) || result.Metrics.Compactions != 0 || len(p.requests) != 0 {
		t.Fatalf("result=%+v", result)
	}
}
