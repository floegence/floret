package runtime

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/sessiontree"
)

func TestNewEngineFromConfigRunsFakeProvider(t *testing.T) {
	e, err := NewEngine(config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		FakeResponse: "configured",
		RunID:        "run",
		SystemPrompt: "test",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
}

func TestNewEngineUsesConfiguredPromptCacheDir(t *testing.T) {
	dir := t.TempDir()
	e, err := NewEngine(config.Config{
		Provider:             config.ProviderFake,
		Model:                "fake-model",
		FakeResponse:         "configured",
		RunID:                "run",
		PromptCacheDir:       dir,
		PromptCacheRetention: "in_memory",
		SystemPrompt:         "test",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if _, err := os.Stat(filepath.Join(dir, promptCachePathForTest("run"), "raw_segments.jsonl")); err != nil {
		t.Fatalf("prompt cache raw segment file missing: %v", err)
	}
}

func TestNewHarnessRunsDurableThreadWithExplicitPolicies(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	h, err := NewHarness(config.Config{
		Provider:                config.ProviderFake,
		Model:                   "fake-model",
		FakeResponse:            "configured",
		SystemPrompt:            "test",
		MaxEmptyProviderRetries: 7,
		NoProgressLimit:         8,
		DuplicateToolLimit:      9,
	}, HarnessOptions{
		Store: repo,
		LoopLimits: agentharness.LoopLimits{
			WallTime: 250 * time.Millisecond,
		},
		NewID: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "hello", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ID != "thread" || snap.Status != string(engine.Completed) || !snap.CanAppendMessage ||
		len(snap.Messages) != 2 || snap.Messages[0].Content != "hello" || snap.Messages[1].Content != "configured" {
		t.Fatalf("durable thread snapshot = %#v", snap)
	}
}

func TestNewHarnessWithProviderMapsExplicitPoliciesToTurn(t *testing.T) {
	ctx := context.Background()
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("configured"), harness.Done()))
	h := NewHarnessWithProvider(config.Config{
		Provider:                config.ProviderFake,
		Model:                   "fake-model",
		SystemPrompt:            "test",
		MaxEmptyProviderRetries: 7,
		NoProgressLimit:         8,
		DuplicateToolLimit:      9,
	}, scripted, HarnessOptions{
		Store: sessiontree.NewMemoryRepo(),
		TurnPolicy: agentharness.TurnPolicy{
			ContextPolicy: contextpolicy.Policy{
				ContextWindowTokens: 4096,
				MaxOutputTokens:     123,
				RecentTailTokens:    512,
			},
			CacheRetention:        promptcache.RetentionLong,
			HostedToolDefinitions: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}},
		},
		LoopLimits: agentharness.LoopLimits{
			MaxEmptyProviderRetries: 3,
			NoProgressLimit:         4,
			DuplicateToolLimit:      5,
			WallTime:                250 * time.Millisecond,
		},
		NewID: deterministicIDs(),
	})
	thread, err := h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "hello", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	req := scripted.Requests[0]
	if req.MaxOutputTokens != 123 || req.ContextPolicy.ContextWindowTokens != 4096 {
		t.Fatalf("context policy not mapped into request: %#v", req.ContextPolicy)
	}
	if req.Cache.Retention != promptcache.RetentionLong {
		t.Fatalf("cache retention = %q, want long", req.Cache.Retention)
	}
	if len(req.HostedTools) != 1 || req.HostedTools[0].Name != "web_search" {
		t.Fatalf("hosted tools = %#v", req.HostedTools)
	}
}

func deterministicIDs() func(string) string {
	var seq int
	return func(prefix string) string {
		seq++
		return prefix + "-deterministic"
	}
}

func promptCachePathForTest(value string) string {
	return "id_" + base64.RawURLEncoding.EncodeToString([]byte(value))
}
