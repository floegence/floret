package promptcache

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/session"
)

func TestBuildPlanReusesPersistedSegmentsAcrossStores(t *testing.T) {
	root := t.TempDir()
	store := NewFileStore(root)
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, newToolset, err := EnsureToolset(context.Background(), store, "run", "session", "openai", "model", []ToolDefinition{{Name: "read", Description: "Read"}}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !newToolset {
		t.Fatalf("first toolset should be new")
	}
	input := BuildInput{
		RunID:          "run",
		SessionID:      "session",
		Provider:       "openai",
		Model:          "model",
		SystemPrompt:   "system",
		History:        []session.Message{{Role: session.User, Content: "hello"}},
		Toolset:        toolset,
		CacheNamespace: "ns",
		Now:            now,
	}
	first, _, err := BuildPlan(context.Background(), store, input)
	if err != nil {
		t.Fatal(err)
	}
	secondStore := NewFileStore(root)
	second, _, err := BuildPlan(context.Background(), secondStore, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.PrefixHash != second.PrefixHash || first.PayloadHash != second.PayloadHash {
		t.Fatalf("hashes changed across store reload: first=%#v second=%#v", first, second)
	}
	if second.NewSegments != 0 || second.ReusedSegments != len(second.Segments) {
		t.Fatalf("second plan did not reuse all segments: %#v", second)
	}
	if first.NewSegments != 2 || first.ReusedSegments != 1 {
		t.Fatalf("first plan should create message segments and reference persisted toolset: %#v", first)
	}
}

func TestBuildPlanAppendsNewSystemSegmentWhenPromptChanges(t *testing.T) {
	store := NewMemoryStore()
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, _, err := EnsureToolset(context.Background(), store, "run", "session", "openai", "model", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	input := BuildInput{
		RunID:        "run",
		SessionID:    "session",
		Provider:     "openai",
		Model:        "model",
		SystemPrompt: "system v1",
		History:      []session.Message{{Role: session.User, Content: "hello"}},
		Toolset:      toolset,
		Now:          now,
	}
	first, _, err := BuildPlan(context.Background(), store, input)
	if err != nil {
		t.Fatal(err)
	}
	input.SystemPrompt = "system v2"
	second, messages, err := BuildPlan(context.Background(), store, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.PrefixHash == second.PrefixHash {
		t.Fatalf("prefix hash did not change after system prompt changed")
	}
	if second.NewSegments != 1 {
		t.Fatalf("changed system prompt should append exactly one new segment: %#v", second)
	}
	if len(messages) == 0 || messages[0].Role != session.System || messages[0].Content != "system v2" {
		t.Fatalf("request messages did not use new system prompt: %#v", messages)
	}
	segments, err := store.Segments(context.Background(), "run", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	var sawV1, sawV2 bool
	for _, seg := range segments {
		if seg.Kind != SegmentSystem {
			continue
		}
		if seg.Message.Content == "system v1" {
			sawV1 = true
		}
		if seg.Message.Content == "system v2" {
			sawV2 = true
		}
	}
	if !sawV1 || !sawV2 {
		t.Fatalf("system prompt changes should append, not rewrite: %#v", segments)
	}
}

func TestToolsetSnapshotFreezesInitialDefinitions(t *testing.T) {
	store := NewMemoryStore()
	first, _, err := EnsureToolset(context.Background(), store, "run", "session", "openai", "model", []ToolDefinition{
		{Name: "z", Description: "last"},
		{Name: "a", Description: "first"},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	second, newToolset, err := EnsureToolset(context.Background(), store, "run", "session", "openai", "model", []ToolDefinition{
		{Name: "a", Description: "changed"},
		{Name: "new", Description: "new"},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if newToolset {
		t.Fatalf("second ensure should reuse active toolset")
	}
	if first.Fingerprint != second.Fingerprint || len(second.Tools) != 2 || second.Tools[0].Name != "a" || second.Tools[0].Description != "first" {
		t.Fatalf("toolset was not frozen: first=%#v second=%#v", first, second)
	}
	third, err := ActivateToolset(context.Background(), store, "run", "session", "openai", "model", []ToolDefinition{{Name: "new"}}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if third.Epoch != 2 || third.Fingerprint == first.Fingerprint {
		t.Fatalf("explicit activation did not create a new epoch: %#v", third)
	}
}

func TestCanonicalSegmentHashIgnoresToolRegistrationOrder(t *testing.T) {
	a := NormalizeTools([]ToolDefinition{{Name: "z"}, {Name: "a"}})
	b := NormalizeTools([]ToolDefinition{{Name: "a"}, {Name: "z"}})
	rawA, err := CanonicalJSON(map[string]any{"tools": a})
	if err != nil {
		t.Fatal(err)
	}
	rawB, err := CanonicalJSON(map[string]any{"tools": b})
	if err != nil {
		t.Fatal(err)
	}
	if rawA != rawB || StableHash(rawA) != StableHash(rawB) {
		t.Fatalf("canonical tool raw/hash differs:\n%s\n%s", rawA, rawB)
	}
}
