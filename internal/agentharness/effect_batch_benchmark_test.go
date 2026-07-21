package agentharness

import (
	"context"
	"fmt"
	"testing"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/sessiontree"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/tools"
)

// BenchmarkEffectBatch measures the complete canonical effect path from turn
// admission through concurrent dispatch, exact result finalization, journal
// commit, and terminal turn settlement. Fixture construction and verification
// are excluded from the timed section.
func BenchmarkEffectBatch(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("memory/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				thread := newEffectBatchBenchmarkThread(b, size)
				b.StartTimer()

				result, err := thread.Run(context.Background(), "run effect batch", RunOptions{
					RunID:  "run-benchmark",
					TurnID: "turn-benchmark",
				})

				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if result.Status != engine.Completed {
					b.Fatalf("turn status = %q, want %q", result.Status, engine.Completed)
				}
				journal, err := thread.Journal(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if got := countEntries(journal.Entries, sessiontree.EntryToolResult); got != size {
					b.Fatalf("tool results = %d, want %d", got, size)
				}
			}
		})
	}
}

type effectBenchmarkArgs struct {
	Value string `json:"value"`
}

func newEffectBatchBenchmarkThread(tb testing.TB, size int) *Thread {
	tb.Helper()
	calls := make([]provider.ToolCall, 0, size)
	for index := 0; index < size; index++ {
		calls = append(calls, provider.ToolCall{
			ID:   fmt.Sprintf("call-%d", index),
			Name: "benchmark_effect",
			Args: fmt.Sprintf(`{"value":"value-%d"}`, index),
		})
	}
	toolStep := scriptharness.Step(
		provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls},
		scriptharness.DoneReason("tool_calls"),
	)
	scriptedProvider := scriptharness.NewScriptedProvider(
		toolStep,
		scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()),
	)
	harness := newTestHarness(scriptedProvider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	harness.options.TitleGenerator = nil
	harness.options.LoopLimits.MaxToolCalls = size
	mustRegister(harness.options.Tools, tools.Define[effectBenchmarkArgs](
		tools.Definition{
			Name:        "benchmark_effect",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("benchmark value")}, []string{"value"}),
			Effects:     []tools.Effect{tools.EffectShell},
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, invocation tools.Invocation[effectBenchmarkArgs]) (tools.Result, error) {
			return tools.Result{Text: invocation.Args.Value}, nil
		},
	))
	thread, err := harness.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread-benchmark"})
	if err != nil {
		tb.Fatal(err)
	}
	return thread
}
