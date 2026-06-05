package promptcache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/session"
)

func BenchmarkBuildPlanTenThousandSegmentsWithCompactionWindow(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	toolset, _, err := EnsureToolset(ctx, store, "turn-0", "thread", "openai", "model", []ToolDefinition{{Name: "read"}}, nil, time.Time{})
	if err != nil {
		b.Fatal(err)
	}
	history := make([]session.Message, 0, 10000)
	for i := 0; i < 10000; i++ {
		history = append(history, session.Message{Role: session.User, Content: fmt.Sprintf("message %05d", i), EntryID: fmt.Sprintf("entry-%05d", i)})
	}
	history[9000] = session.Message{
		Role:                 session.User,
		Content:              "summary through 09000",
		EntryID:              "entry-09000",
		Kind:                 session.MessageKindCompactionSummary,
		CompactionID:         "compaction-09",
		CompactionGeneration: 9,
		CompactionWindowID:   "window-09",
	}
	input := BuildInput{
		RunID:         "turn-1",
		SessionID:     "thread",
		Provider:      "openai",
		Model:         "model",
		SystemPrompt:  "system",
		History:       history,
		Toolset:       toolset,
		ContextPolicy: contextpolicy.Policy{ContextWindowTokens: 128000, MaxOutputTokens: 4096, RecentTailTokens: 4096},
	}
	if _, _, err := BuildPlan(ctx, store, input); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, _, err := BuildPlan(ctx, store, input)
		if err != nil {
			b.Fatal(err)
		}
		if plan.CompactionGeneration != 9 || plan.CompactionWindowID != "window-09" {
			b.Fatalf("compaction window missing from plan: %#v", plan)
		}
	}
}
