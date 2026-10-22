package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	kernelstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/testing/harness"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

type countedExecutionHistory struct {
	cache.Store
	reads int
}

func (s *countedExecutionHistory) ProviderRequests(ctx context.Context, scope string) ([]cache.ProviderRequestRecord, error) {
	s.reads++
	return s.Store.ProviderRequests(ctx, scope)
}
func (s *countedExecutionHistory) LatestProviderRequest(ctx context.Context, scope string) (cache.ProviderRequestRecord, bool, error) {
	return cache.LatestProviderRequest(ctx, s.Store, scope)
}
func (s *countedExecutionHistory) CheckpointProviderRequest(ctx context.Context, segments []cache.Segment, toolsets []cache.ToolsetSnapshot, request cache.ProviderRequestRecord) error {
	return s.Store.(cache.ProviderRequestCheckpointer).CheckpointProviderRequest(ctx, segments, toolsets, request)
}

func TestExecutionReadsTurnHistoryOnceAcrossToolSteps(t *testing.T) {
	for _, count := range []int{1, 12} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			p := harness.NewScriptedProvider()
			for i := range count {
				p.Steps = append(p.Steps, harness.Step(harness.Tool(fmt.Sprint(i), "read", fmt.Sprintf("{\"value\":\"%d\"}", i)), harness.DoneReason("tool_calls")))
			}
			p.Steps = append(p.Steps, harness.Step(harness.Text("done"), harness.Done()))
			e := newTestEngine(p, &event.Recorder{})
			history := &countedExecutionHistory{Store: cache.NewMemoryStore()}
			e.Prompt = history
			mustRegister(t, e.Tools, stringTool("read", "Read", true, tools.PermissionSpec{Mode: tools.PermissionAllow}, func(context.Context, string) (string, error) { return "content", nil }))
			e.Options.ToolSurfaceProvider = func(context.Context, engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
				return engine.ToolSurface{SystemPrompt: "Stable", Tools: e.Tools}, nil
			}
			result := e.Run(t.Context(), "inspect")
			if result.Status != engine.Completed || result.Output != "done" || len(p.Requests) != count+1 {
				t.Fatalf("result=%+v requests=%d", result, len(p.Requests))
			}
			if history.reads != 1 {
				t.Fatalf("full request-history reads=%d, want one per execution", history.reads)
			}
		})
	}
}

// The timed path runs the real Engine with SQLite checkpoints and deterministic
// provider output. Historical request surfaces are large; canonical conversation
// stays short to isolate preparation amplification from tokenization growth.
func BenchmarkSQLiteExecutionRequestHistory(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			backend, err := storagebridge.Open(b.Context(), storagebridge.Source(publicstorage.SQLite(b.TempDir()+"/history.sqlite")))
			if err != nil {
				b.Fatal(err)
			}
			defer backend.Close()
			kernel, err := kernelstorage.NewBackendKernel(b.Context(), backend, time.Now)
			if err != nil {
				b.Fatal(err)
			}
			makeEngine := func() *testEngine {
				e := newTestEngine(harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done())), &event.Recorder{})
				e.Prompt = kernel
				e.SystemPrompt = strings.Repeat("Synthetic system instructions. ", 560)
				e.Options.ToolSurfaceProvider = func(context.Context, engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
					return engine.ToolSurface{SystemPrompt: e.SystemPrompt}, nil
				}
				return e
			}
			input := engine.RunInput{RunID: "seed", ThreadID: "scope", TurnID: "seed", PromptScopeID: "scope", History: []session.Message{{Role: session.User, Content: "Inspect", EntryID: "user"}}}
			if result := makeEngine().RunTurn(b.Context(), input); result.Status != engine.Completed {
				b.Fatalf("seed: %+v", result)
			}
			request, found, err := kernel.LatestProviderRequest(b.Context(), "scope")
			if err != nil || !found {
				b.Fatal(err)
			}
			for i := 1; i < count; i++ {
				request.ID = fmt.Sprint(i)
				request.RunID = request.ID
				request.TurnID = request.ID
				if err := kernel.AppendProviderRequest(b.Context(), request); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				input.RunID = fmt.Sprintf("measured-%d", i)
				input.TurnID = input.RunID
				if result := makeEngine().RunTurn(b.Context(), input); result.Status != engine.Completed || result.Output != "done" {
					b.Fatalf("execution: %+v", result)
				}
			}
		})
	}
}
