package cache

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v6/internal/session"
	"github.com/floegence/floret/v6/internal/session/contextpolicy"
)

func TestValidateCanonicalLineageAllowsOnlyAppendWithinGeneration(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	envelopeTools := []ToolDefinition{{Name: "read", Description: "Read"}}
	firstHistory := []session.Message{
		{Role: session.User, Content: "inspect"},
		{Role: session.Assistant, Content: "checking", Reasoning: "reasoning-1"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "call-1", ToolName: "read", ToolArgs: `{"path":"README.md"}`},
		{Role: session.Tool, Content: "contents", ToolCallID: "call-1", ToolName: "read"},
	}
	first := lineagePlan("test", "model", "ns", "tool", "system", "user", "assistant", "call", "result")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &first, "stable system", envelopeTools, nil, map[string]any{"model": "test"}, firstHistory); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(ctx, store, PromptScopeRef{PromptScopeID: "thread", RunID: "run-1", ThreadID: "thread", TurnID: "turn-1"}, 1, "test", "model", CachePolicy{}, first); err != nil {
		t.Fatal(err)
	}

	appended := append(append([]session.Message(nil), firstHistory...), session.Message{Role: session.Assistant, Content: "done", Reasoning: "reasoning-2"})
	next := lineagePlan("test", "model", "ns", "tool", "system", "user", "assistant", "call", "result", "done")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &next, "stable system", envelopeTools, nil, map[string]any{"model": "test"}, appended); err != nil {
		t.Fatalf("append-only suffix rejected: %v", err)
	}
	if next.CanonicalMessageCount != len(appended) || next.CanonicalPrefixHash == first.CanonicalPrefixHash {
		t.Fatalf("canonical lineage did not advance: first=%#v next=%#v", first, next)
	}

	mutated := append([]session.Message(nil), appended...)
	mutated[1].Reasoning = "rewritten reasoning"
	mutatedPlan := lineagePlan("test", "model", "ns", "tool", "system", "user-mutated")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &mutatedPlan, "stable system", envelopeTools, nil, map[string]any{"model": "test"}, mutated); !errors.Is(err, ErrContextPrefixDrift) {
		t.Fatalf("mutated prefix error = %v, want ErrContextPrefixDrift", err)
	}
	rewrittenEnvelope := lineagePlan("test", "model", "ns", "tool", "new-system")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &rewrittenEnvelope, "rewritten system", envelopeTools, nil, map[string]any{"model": "test"}, appended); !errors.Is(err, ErrContextEnvelopeChanged) {
		t.Fatalf("rewritten envelope error = %v, want ErrContextEnvelopeChanged", err)
	}
	newModel := lineagePlan("test", "other", "other-ns", "other-tool", "other-system", "other-user")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &newModel, "stable system", envelopeTools, nil, map[string]any{"model": "other"}, appended); err != nil {
		t.Fatalf("new model render lineage rejected: %v", err)
	}
}

func TestValidateCanonicalLineageKeepsIndependentModelRenderPrefixes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	firstHistory := []session.Message{{Role: session.User, Content: "one"}, {Role: session.Assistant, Content: "a-one"}}
	firstA := lineagePlan("deepseek", "flash", "flash-ns", "flash-tools", "flash-system", "flash-one", "flash-a-one")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &firstA, "system", nil, nil, map[string]any{"model": "flash"}, firstHistory); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(ctx, store, PromptScopeRef{PromptScopeID: "thread", RunID: "run-a1", ThreadID: "thread", TurnID: "turn-a1"}, 1, "deepseek", "flash", CachePolicy{Namespace: "flash-ns"}, firstA); err != nil {
		t.Fatal(err)
	}

	secondHistory := append(append([]session.Message(nil), firstHistory...), session.Message{Role: session.User, Content: "two"}, session.Message{Role: session.Assistant, Content: "a-two"})
	firstB := lineagePlan("deepseek", "pro", "pro-ns", "pro-tools", "pro-system", "pro-one", "pro-a-one", "pro-two", "pro-a-two")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &firstB, "system", nil, nil, map[string]any{"model": "pro"}, secondHistory); err != nil {
		t.Fatalf("A to B switch rejected: %v", err)
	}
	if firstB.CanonicalLineageReset {
		t.Fatal("model switch unexpectedly reset canonical lineage")
	}
	if _, err := RecordRequest(ctx, store, PromptScopeRef{PromptScopeID: "thread", RunID: "run-b", ThreadID: "thread", TurnID: "turn-b"}, 1, "deepseek", "pro", CachePolicy{Namespace: "pro-ns"}, firstB); err != nil {
		t.Fatal(err)
	}

	thirdHistory := append(append([]session.Message(nil), secondHistory...), session.Message{Role: session.User, Content: "three"})
	secondA := lineagePlan("deepseek", "flash", "flash-ns", "flash-tools", "flash-system", "flash-one", "flash-a-one", "flash-two", "flash-a-two", "flash-three")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &secondA, "system", nil, nil, map[string]any{"model": "flash"}, thirdHistory); err != nil {
		t.Fatalf("A to B to A switch rejected: %v", err)
	}
	if !slices.Equal(firstA.SegmentIDs, secondA.SegmentIDs[:len(firstA.SegmentIDs)]) {
		t.Fatalf("returning A render prefix changed: first=%#v second=%#v", firstA.SegmentIDs, secondA.SegmentIDs)
	}

	driftedA := lineagePlan("deepseek", "flash", "flash-ns", "flash-tools-changed", "flash-system", "flash-one")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &driftedA, "system", nil, nil, map[string]any{"model": "flash"}, thirdHistory); !errors.Is(err, ErrContextRenderLineageChanged) {
		t.Fatalf("render prefix drift error = %v, want ErrContextRenderLineageChanged", err)
	}
}

func TestValidateTurnModelFreezesProviderAndModelWithinTurn(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.AppendProviderRequest(ctx, ProviderRequestRecord{
		ID: "request", PromptScopeID: "thread", RunID: "run", TurnID: "turn", Provider: "deepseek", Model: "flash",
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTurnModel(ctx, store, "thread", "turn", "deepseek", "flash"); err != nil {
		t.Fatalf("same turn model rejected: %v", err)
	}
	if err := ValidateTurnModel(ctx, store, "thread", "turn", "deepseek", "pro"); !errors.Is(err, ErrTurnModelDrift) {
		t.Fatalf("same turn model drift error = %v, want ErrTurnModelDrift", err)
	}
	if err := ValidateTurnModel(ctx, store, "thread", "next-turn", "deepseek", "pro"); err != nil {
		t.Fatalf("new turn model switch rejected: %v", err)
	}
}

func TestValidateCanonicalLineageRequiresCommittedCompactionForNewGeneration(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	history := []session.Message{{Role: session.User, Content: "first"}}
	first := lineagePlan("test", "model", "ns", "system", "first")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &first, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(ctx, store, PromptScopeRef{PromptScopeID: "thread", RunID: "run-1", ThreadID: "thread", TurnID: "turn-1"}, 1, "test", "model", CachePolicy{}, first); err != nil {
		t.Fatal(err)
	}
	invalid := lineagePlan("test", "model", "ns", "system", "first")
	invalid.CompactionGeneration = 1
	if err := ValidateCanonicalLineage(ctx, store, "thread", &invalid, "new system", nil, nil, nil, history); !errors.Is(err, ErrContextPrefixDrift) {
		t.Fatalf("uncommitted generation error = %v, want ErrContextPrefixDrift", err)
	}
	valid := lineagePlan("test", "model", "ns", "system", "summary")
	valid.CompactionGeneration = 1
	valid.CompactionWindowID = "window-1"
	valid.CompactionEntryID = "compaction-1"
	if err := ValidateCanonicalLineage(ctx, store, "thread", &valid, "new system", nil, nil, nil, []session.Message{{Role: session.System, Kind: session.MessageKindCompactionSummary, Content: "summary"}}); err != nil {
		t.Fatalf("committed compaction generation rejected: %v", err)
	}
	if !valid.CanonicalLineageReset {
		t.Fatal("new compaction generation did not fence provider continuation state")
	}
}

func TestValidateCanonicalLineageMigratesLegacyProjection(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.AppendProviderRequest(ctx, ProviderRequestRecord{
		ID: "legacy", PromptScopeID: "thread", RunID: "run-legacy", Step: 1,
		Provider: "test", Model: "model", SegmentIDs: []string{"system-old", "message-old"}, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	exact := lineagePlan("test", "model", "ns", "system-old", "message-old", "message-new")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &exact, "system", nil, nil, nil, []session.Message{{Role: session.User, Content: "hello"}}); !errors.Is(err, ErrContextLineageMigrationNeeded) {
		t.Fatalf("legacy drift error = %v, want migration", err)
	}
}

func lineagePlan(providerName, model, namespace string, segmentIDs ...string) RawPlan {
	return RawPlan{
		Version: Version, CacheNamespace: namespace, SegmentIDs: append([]string(nil), segmentIDs...),
		ContextProjectionRevision: ContextProjectionRevision,
		RenderLineageKey:          renderLineageKey(providerName, model, Version, namespace),
	}
}

func TestBuildPlanReusesPersistedSegmentsAcrossStores(t *testing.T) {
	root := t.TempDir()
	store := NewFileStore(root)
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, newToolset, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", []ToolDefinition{{Name: "read", Description: "Read"}}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if !newToolset {
		t.Fatalf("first toolset should be new")
	}
	input := BuildInput{
		PromptScopeID:  "thread",
		RunID:          "run",
		ThreadID:       "thread",
		TurnID:         "run",
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
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	input := BuildInput{
		PromptScopeID: "thread",
		RunID:         "run",
		ThreadID:      "thread",
		TurnID:        "run",
		Provider:      "openai",
		Model:         "model",
		SystemPrompt:  "system v1",
		History:       []session.Message{{Role: session.User, Content: "hello"}},
		Toolset:       toolset,
		Now:           now,
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
	segments, err := store.Segments(context.Background(), "thread", "openai", "model")
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

func TestRecordRequestStoresEngineInjectedRequestEstimateAndPressure(t *testing.T) {
	store := NewMemoryStore()
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "run",
		ThreadID:      "thread",
		TurnID:        "run",
		Provider:      "openai",
		Model:         "model",
		Toolset:       toolset,
		History:       []session.Message{{Role: session.User, Content: "hello"}},
		Now:           now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.RequestEstimate != (contextpolicy.RequestEstimate{}) {
		t.Fatalf("build plan should not estimate provider request tokens: %#v", plan.RequestEstimate)
	}
	if plan.ProjectedPressure != (contextpolicy.ContextPressure{}) {
		t.Fatalf("build plan should not derive projected pressure: %#v", plan.ProjectedPressure)
	}
	policy := contextpolicy.Policy{
		ContextWindowTokens:   1000000,
		MaxOutputTokens:       384000,
		ReservedOutputTokens:  4096,
		ReservedSummaryTokens: 20000,
	}
	plan.RequestEstimate = contextpolicy.RequestEstimate{
		EstimatedInputTokens: 1234,
		Source:               "request_estimator_test",
		Method:               contextpolicy.EstimateMethodProviderRenderedPayload,
		Confidence:           contextpolicy.EstimateConservative,
	}.Normalized(policy)
	plan.ProjectedPressure = contextpolicy.PressureFromProjectedRequest(plan.RequestEstimate, contextpolicy.RequestDeltaEstimate{}, policy)
	if _, err := RecordRequest(context.Background(), store, PromptScopeRef{PromptScopeID: "thread", RunID: "run", ThreadID: "thread", TurnID: "run"}, 1, "openai", "model", CachePolicy{}, plan); err != nil {
		t.Fatal(err)
	}
	requests, err := store.ProviderRequests(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(requests))
	}
	if requests[0].RequestEstimate.EstimatedInputTokens != 1234 ||
		requests[0].RequestEstimate.Source != "request_estimator_test" ||
		requests[0].RequestEstimate.Method != contextpolicy.EstimateMethodProviderRenderedPayload {
		t.Fatalf("stored request estimate missing request estimate data: %#v", requests[0].RequestEstimate)
	}
	if requests[0].ProjectedPressure.ProjectedInputTokens != 1234 ||
		requests[0].ProjectedPressure.OutputHeadroomTokens != 384000 ||
		requests[0].ProjectedPressure.RequestSafeLimit != 616000 ||
		requests[0].ProjectedPressure.Source != contextpolicy.PressureSourceFullRequestEstimate ||
		requests[0].ProjectedPressure.EstimateMethod != contextpolicy.EstimateMethodProviderRenderedPayload {
		t.Fatalf("stored projected pressure missing budget fields: %#v", requests[0].ProjectedPressure)
	}
}

func TestLatestPressureAnchorAcrossMemoryAndFileStores(t *testing.T) {
	ctx := context.Background()
	type anchorStore interface {
		AppendProviderResponse(context.Context, ProviderResponseRecord) error
		LatestPressureAnchor(context.Context, string, string, string) (PressureAnchorState, bool, error)
	}
	for _, tc := range []struct {
		name  string
		store anchorStore
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "file", store: NewFileStore(t.TempDir())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
			old := PressureAnchorState{
				PromptScopeID:      "thread",
				ThreadID:           "thread",
				Provider:           "openai",
				Model:              "model",
				RequestID:          "turn-1:req:1",
				LastMessageEntryID: "entry-1",
				WindowInputTokens:  100,
				EstimateSource:     "request_estimator_test",
				EstimateMethod:     contextpolicy.EstimateMethodProviderRenderedPayload,
				Confidence:         contextpolicy.EstimateConservative,
				CreatedAt:          now,
			}
			newer := old
			newer.RequestID = "turn-2:req:1"
			newer.LastMessageEntryID = "entry-2"
			newer.WindowInputTokens = 200
			newer.CreatedAt = now.Add(time.Minute)
			other := newer
			other.PromptScopeID = "other-thread"
			other.ThreadID = "other-thread"
			other.WindowInputTokens = 999
			other.CreatedAt = now.Add(2 * time.Minute)

			for _, resp := range []ProviderResponseRecord{
				{RequestID: "turn-1:req:1", PromptScopeID: "thread", RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", PressureAnchor: old},
				{RequestID: "turn-other:req:1", PromptScopeID: "other-thread", RunID: "turn-other", ThreadID: "other-thread", TurnID: "turn-other", PressureAnchor: other},
				{RequestID: "turn-2:req:1", PromptScopeID: "thread", RunID: "turn-2", ThreadID: "thread", TurnID: "turn-2", PressureAnchor: newer},
			} {
				if err := tc.store.AppendProviderResponse(ctx, resp); err != nil {
					t.Fatal(err)
				}
			}

			got, ok, err := tc.store.LatestPressureAnchor(ctx, "thread", "openai", "model")
			if err != nil {
				t.Fatal(err)
			}
			if !ok ||
				got.RequestID != "turn-2:req:1" ||
				got.WindowInputTokens != 200 ||
				got.LastMessageEntryID != "entry-2" ||
				got.EstimateSource != "request_estimator_test" ||
				got.EstimateMethod != contextpolicy.EstimateMethodProviderRenderedPayload {
				t.Fatalf("latest anchor = %#v ok=%v", got, ok)
			}
			if _, ok, err := tc.store.LatestPressureAnchor(ctx, "thread", "anthropic", "model"); err != nil || ok {
				t.Fatalf("mismatched provider anchor ok=%v err=%v", ok, err)
			}
		})
	}
}

func TestBuildPlanReusesSegmentsAcrossTurnsInSameThread(t *testing.T) {
	store := NewMemoryStore()
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "turn-1", "thread", "turn-1", "openai", "model", nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "turn-1",
		ThreadID:      "thread",
		TurnID:        "turn-1",
		Provider:      "openai",
		Model:         "model",
		History:       []session.Message{{Role: session.User, Content: "hello"}},
		Toolset:       toolset,
		Now:           now,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "turn-2",
		ThreadID:      "thread",
		TurnID:        "turn-2",
		Provider:      "openai",
		Model:         "model",
		History: []session.Message{
			{Role: session.User, Content: "hello"},
			{Role: session.Assistant, Content: "hi"},
		},
		Toolset: toolset,
		Now:     now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.ReusedSegments < len(first.Segments) {
		t.Fatalf("second turn should reuse same thread ledger segments: first=%#v second=%#v", first, second)
	}
	if first.Segments[len(first.Segments)-1].PromptScopeID != "thread" ||
		first.Segments[len(first.Segments)-1].CreatedByRunID != "turn-1" ||
		first.Segments[len(first.Segments)-1].CreatedByTurnID != "turn-1" {
		t.Fatalf("message segment should be stored under stable prompt scope with creation identity: %#v", first.Segments)
	}
}

func TestFileStoreKeepsExactRawPrefixAcrossTurnsInSameThread(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(t.TempDir())
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, _, err := EnsureToolset(ctx, store, "thread", "turn-1", "thread", "turn-1", "openai", "model", []ToolDefinition{{Name: "read"}}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := BuildPlan(ctx, store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "turn-1",
		ThreadID:      "thread",
		TurnID:        "turn-1",
		Provider:      "openai",
		Model:         "model",
		History:       []session.Message{{Role: session.User, Content: "hello"}},
		Toolset:       toolset,
		Now:           now,
	})
	if err != nil {
		t.Fatal(err)
	}
	reloaded := NewFileStore(store.root)
	active, ok, err := reloaded.ActiveToolset(ctx, "thread", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("active toolset missing after reload")
	}
	second, _, err := BuildPlan(ctx, reloaded, BuildInput{
		PromptScopeID: "thread",
		RunID:         "turn-2",
		ThreadID:      "thread",
		TurnID:        "turn-2",
		Provider:      "openai",
		Model:         "model",
		History: []session.Message{
			{Role: session.User, Content: "hello"},
			{Role: session.Assistant, Content: "hi"},
		},
		Toolset: active,
		Now:     now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(first.SegmentIDs, second.SegmentIDs[:len(first.SegmentIDs)]) {
		t.Fatalf("segment id prefix changed: first=%#v second=%#v", first.SegmentIDs, second.SegmentIDs)
	}
	if !equalStrings(segmentRaws(first.Segments), segmentRaws(second.Segments[:len(first.Segments)])) {
		t.Fatalf("raw string prefix changed")
	}
	if second.NewSegments != 1 {
		t.Fatalf("second turn should append only new suffix: %#v", second)
	}
}

func TestPromptCacheDeletePromptScopesAcrossMemoryAndFileStores(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		store interface {
			Store
			Deleter
		}
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "file", store: NewFileStore(t.TempDir())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 6, 4, 12, 30, 0, 0, time.UTC)
			if err := tc.store.AppendSegment(ctx, Segment{ID: "seg-1", PromptScopeID: "scope-1", CreatedByRunID: "run-1", CreatedByTurnID: "turn-1", ThreadID: "thread-1", Provider: "openai", Model: "model", Kind: SegmentSystem, Raw: "system", CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AppendToolset(ctx, ToolsetSnapshot{ID: "toolset-1", PromptScopeID: "scope-1", CreatedByRunID: "run-1", CreatedByTurnID: "turn-1", ThreadID: "thread-1", Provider: "openai", Model: "model", Epoch: 1, CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AppendProviderRequest(ctx, ProviderRequestRecord{ID: "run-1:req:1", PromptScopeID: "scope-1", RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", Provider: "openai", Model: "model", CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AppendProviderResponse(ctx, ProviderResponseRecord{RequestID: "run-1:req:1", PromptScopeID: "scope-1", RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", ProviderResponseID: "resp-1", CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AppendSegment(ctx, Segment{ID: "seg-2", PromptScopeID: "scope-2", CreatedByRunID: "run-2", CreatedByTurnID: "turn-2", ThreadID: "thread-2", Provider: "openai", Model: "model", Kind: SegmentUserMessage, Raw: "user", CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if err := tc.store.DeletePromptScopes(ctx, "scope-1"); err != nil {
				t.Fatal(err)
			}
			if segments, err := tc.store.Segments(ctx, "scope-1", "openai", "model"); err != nil || len(segments) != 0 {
				t.Fatalf("deleted segments = %#v err=%v", segments, err)
			}
			if _, ok, err := tc.store.ActiveToolset(ctx, "scope-1", "openai", "model"); err != nil || ok {
				t.Fatalf("deleted toolset ok=%v err=%v", ok, err)
			}
			if requests, err := tc.store.ProviderRequests(ctx, "scope-1"); err != nil || len(requests) != 0 {
				t.Fatalf("deleted requests = %#v err=%v", requests, err)
			}
			if responses, err := tc.store.ProviderResponses(ctx, "scope-1"); err != nil || len(responses) != 0 {
				t.Fatalf("deleted responses = %#v err=%v", responses, err)
			}
			if segments, err := tc.store.Segments(ctx, "scope-2", "openai", "model"); err != nil || len(segments) != 1 {
				t.Fatalf("kept prompt scope segments = %#v err=%v", segments, err)
			}
		})
	}
}

func TestBuildPlanReusedRawSegmentCarriesCurrentEntryRef(t *testing.T) {
	store := NewMemoryStore()
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "turn-1", "thread", "turn-1", "openai", "model", nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "turn-1",
		ThreadID:      "thread",
		TurnID:        "turn-1",
		Provider:      "openai",
		Model:         "model",
		History:       []session.Message{{Role: session.User, Content: "same", EntryID: "entry-a", ParentEntryID: "parent-a"}},
		Toolset:       toolset,
		Now:           now,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "turn-2",
		ThreadID:      "thread",
		TurnID:        "turn-2",
		Provider:      "openai",
		Model:         "model",
		History:       []session.Message{{Role: session.User, Content: "same", EntryID: "entry-b", ParentEntryID: "parent-b"}},
		Toolset:       toolset,
		Now:           now,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstUser := first.Segments[len(first.Segments)-1]
	secondUser := second.Segments[len(second.Segments)-1]
	if firstUser.ID != secondUser.ID || firstUser.Raw != secondUser.Raw {
		t.Fatalf("raw segment should be reused for identical provider payload: first=%#v second=%#v", firstUser, secondUser)
	}
	if secondUser.EntryID != "entry-b" || secondUser.ParentEntryID != "parent-b" {
		t.Fatalf("reused segment in current plan should carry current entry ref: %#v", secondUser)
	}
	stored, err := store.Segments(context.Background(), "thread", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	if stored[len(stored)-1].EntryID != "entry-a" {
		t.Fatalf("stored immutable segment should not be rewritten: %#v", stored)
	}
}

func TestToolsetSnapshotFreezesInitialDefinitions(t *testing.T) {
	store := NewMemoryStore()
	first, _, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", []ToolDefinition{
		{Name: "z", Description: "last"},
		{Name: "a", Description: "first"},
	}, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	second, newToolset, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", []ToolDefinition{
		{Name: "a", Description: "changed"},
		{Name: "new", Description: "new"},
	}, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if newToolset {
		t.Fatalf("second ensure should reuse active toolset")
	}
	if first.Fingerprint != second.Fingerprint || len(second.Tools) != 2 || second.Tools[0].Name != "a" || second.Tools[0].Description != "first" {
		t.Fatalf("toolset was not frozen: first=%#v second=%#v", first, second)
	}
	third, err := ActivateToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", []ToolDefinition{{Name: "new"}}, nil, time.Time{})
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

func TestEnsureToolsetRejectsInvalidToolDefinitions(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	cases := []struct {
		name   string
		tools  []ToolDefinition
		hosted []HostedToolDefinition
		want   string
	}{
		{name: "empty local name", tools: []ToolDefinition{{Name: ""}}, want: "name is required"},
		{name: "duplicate local", tools: []ToolDefinition{{Name: "read"}, {Name: " read "}}, want: "duplicate tool name"},
		{name: "reserved local", tools: []ToolDefinition{{Name: "ask_user"}}, want: "reserved"},
		{name: "empty hosted name", hosted: []HostedToolDefinition{{Type: "web_search"}}, want: "name is required"},
		{name: "empty hosted type", hosted: []HostedToolDefinition{{Name: "web_search"}}, want: "type is required"},
		{name: "duplicate hosted name", hosted: []HostedToolDefinition{{Name: "web_search", Type: "web_search"}, {Name: "web_search", Type: "search"}}, want: "duplicate hosted tool name"},
		{name: "reserved hosted", hosted: []HostedToolDefinition{{Name: "task_complete", Type: "control"}}, want: "reserved"},
		{name: "local hosted conflict", tools: []ToolDefinition{{Name: "web_search"}}, hosted: []HostedToolDefinition{{Name: "web_search", Type: "web_search"}}, want: "both a local tool and a provider-hosted tool"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			promptScopeID := "thread-" + strings.ReplaceAll(tt.name, " ", "-")
			runID := "run-" + strings.ReplaceAll(tt.name, " ", "-")
			_, _, err := EnsureToolset(ctx, store, promptScopeID, runID, promptScopeID, runID, "openai", "model", tt.tools, tt.hosted, time.Time{})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNormalizeToolsCheckedAllowsEngineControlDefinitionsOnlyWhenMarked(t *testing.T) {
	_, err := NormalizeToolsChecked([]ToolDefinition{{Name: "ask_user"}}, ToolsetOptions{AllowControlTools: true})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("plain reserved control should fail: %v", err)
	}
	defs, err := NormalizeToolsChecked([]ToolDefinition{{Name: "ask_user", Annotations: map[string]any{"kind": "control"}}}, ToolsetOptions{AllowControlTools: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Name != "ask_user" {
		t.Fatalf("defs = %#v", defs)
	}
}

func TestConcurrentBuildPlanRecordRequestAcrossSessionsIsIsolated(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	run := func(runID, threadID, toolName, content string) {
		defer wg.Done()
		toolset, _, err := EnsureCurrentToolset(ctx, store, threadID, runID, threadID, runID, "openai", "model", []ToolDefinition{{Name: toolName}}, nil, time.Time{})
		if err != nil {
			errs <- err
			return
		}
		plan, _, err := BuildPlan(ctx, store, BuildInput{
			PromptScopeID: threadID,
			RunID:         runID,
			ThreadID:      threadID,
			TurnID:        runID,
			Provider:      "openai",
			Model:         "model",
			Toolset:       toolset,
			History:       []session.Message{{Role: session.User, Content: content}},
		})
		if err != nil {
			errs <- err
			return
		}
		if _, err := RecordRequest(ctx, store, PromptScopeRef{PromptScopeID: threadID, RunID: runID, ThreadID: threadID, TurnID: runID}, 1, "openai", "model", CachePolicy{}, plan); err != nil {
			errs <- err
			return
		}
	}
	wg.Add(2)
	go run("turn-a", "thread-a", "read_a", "message a")
	go run("turn-b", "thread-b", "read_b", "message b")
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct {
		runID    string
		threadID string
		toolName string
		content  string
	}{
		{"turn-a", "thread-a", "read_a", "message a"},
		{"turn-b", "thread-b", "read_b", "message b"},
	} {
		requests, err := store.ProviderRequests(ctx, item.threadID)
		if err != nil {
			t.Fatal(err)
		}
		if len(requests) != 1 || requests[0].ThreadID != item.threadID {
			t.Fatalf("%s requests = %#v", item.runID, requests)
		}
		segments, err := store.Segments(ctx, item.threadID, "openai", "model")
		if err != nil {
			t.Fatal(err)
		}
		var sawTool, sawMessage bool
		for _, seg := range segments {
			if strings.Contains(seg.Raw, item.toolName) {
				sawTool = true
			}
			if seg.Message.Content == item.content {
				sawMessage = true
			}
			if strings.Contains(seg.Raw, "read_a") && item.toolName != "read_a" || strings.Contains(seg.Raw, "read_b") && item.toolName != "read_b" {
				t.Fatalf("%s saw cross-session tool segment: %#v", item.threadID, seg)
			}
			if seg.Message.Content == "message a" && item.content != "message a" || seg.Message.Content == "message b" && item.content != "message b" {
				t.Fatalf("%s saw cross-session message segment: %#v", item.threadID, seg)
			}
		}
		if !sawTool || !sawMessage {
			t.Fatalf("%s missing own tool/message segments: %#v", item.threadID, segments)
		}
	}
	if _, _, err := EnsureCurrentToolset(ctx, store, "thread-c", "turn-c", "thread-c", "turn-c", "openai", "model", []ToolDefinition{{Name: "x"}, {Name: "x"}}, nil, time.Time{}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate check regressed: %v", err)
	}
}

func TestHostedToolsAffectToolsetAndPayloadHash(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	tools := []ToolDefinition{{Name: "read"}}
	hostedA := []HostedToolDefinition{{Name: "web_search", Type: "web_search", Options: map[string]any{"limit": 3}}}
	hostedB := []HostedToolDefinition{{Name: "web_search", Type: "web_search", Options: map[string]any{"limit": 5}}}

	toolsetA, _, err := EnsureCurrentToolset(ctx, store, "thread", "turn-a", "thread", "turn-a", "openai", "model", tools, hostedA, now)
	if err != nil {
		t.Fatal(err)
	}
	planA, _, err := BuildPlan(ctx, store, BuildInput{PromptScopeID: "thread", RunID: "turn-a", ThreadID: "thread", TurnID: "turn-a", Provider: "openai", Model: "model", Toolset: toolsetA, HostedTools: hostedA, History: []session.Message{{Role: session.User, Content: "search"}}})
	if err != nil {
		t.Fatal(err)
	}
	toolsetB, changed, err := EnsureCurrentToolset(ctx, store, "thread", "turn-b", "thread", "turn-b", "openai", "model", tools, hostedB, now)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || toolsetA.Fingerprint == toolsetB.Fingerprint {
		t.Fatalf("hosted tool change should rotate toolset: a=%#v b=%#v changed=%v", toolsetA, toolsetB, changed)
	}
	planB, _, err := BuildPlan(ctx, store, BuildInput{PromptScopeID: "thread", RunID: "turn-b", ThreadID: "thread", TurnID: "turn-b", Provider: "openai", Model: "model", Toolset: toolsetB, HostedTools: hostedB, History: []session.Message{{Role: session.User, Content: "search"}}})
	if err != nil {
		t.Fatal(err)
	}
	if planA.HostedToolsetHash == "" || planA.HostedToolsetHash == planB.HostedToolsetHash {
		t.Fatalf("hosted hash should differ: a=%#v b=%#v", planA, planB)
	}
	if planA.PayloadHash == planA.PrefixHash || planA.PayloadHash == planB.PayloadHash {
		t.Fatalf("payload hash should include hosted hash: a=%#v b=%#v", planA, planB)
	}
}

func TestCompactionSegmentKindAndWindowComeFromStructuredMessageKind(t *testing.T) {
	store := NewMemoryStore()
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", nil, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "run",
		ThreadID:      "thread",
		TurnID:        "run",
		Provider:      "openai",
		Model:         "model",
		Toolset:       toolset,
		History: []session.Message{
			{Role: session.User, Content: "summary without magic words", Kind: session.MessageKindCompactionSummary, CompactionID: "c1", CompactionGeneration: 3, CompactionWindowID: "w3"},
			{Role: session.User, Content: "continue"},
			{Role: session.System, Content: "This content says compacted but is ordinary system context."},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.CompactionGeneration != 3 || plan.CompactionWindowID != "w3" || plan.CompactionEntryID != "c1" {
		t.Fatalf("plan compaction window missing: %#v", plan)
	}
	var structured, heuristic bool
	for _, seg := range plan.Segments {
		if seg.Kind == SegmentCompaction && seg.Message.Content == "summary without magic words" && seg.CompactionWindowID == "w3" {
			structured = true
		}
		if seg.Kind == SegmentCompaction && strings.Contains(seg.Message.Content, "compacted but is ordinary") {
			heuristic = true
		}
	}
	if !structured || heuristic {
		t.Fatalf("compaction kind should be structured only: %#v", plan.Segments)
	}
}

func TestActiveCompactionWindowUsesLatestStructuredSummary(t *testing.T) {
	store := NewMemoryStore()
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", nil, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "run",
		ThreadID:      "thread",
		TurnID:        "run",
		Provider:      "openai",
		Model:         "model",
		Toolset:       toolset,
		History: []session.Message{
			{Role: session.Assistant, Content: "old summary", Kind: session.MessageKindCompactionSummary, CompactionID: "c1", CompactionGeneration: 1, CompactionWindowID: "w1"},
			{Role: session.User, Content: "middle"},
			{Role: session.User, Content: "new summary", Kind: session.MessageKindCompactionSummary, CompactionID: "c2", CompactionGeneration: 2, CompactionWindowID: "w2"},
			{Role: session.User, Content: "continue"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.CompactionGeneration != 2 || plan.CompactionWindowID != "w2" || plan.CompactionEntryID != "c2" {
		t.Fatalf("plan should use latest compaction window: %#v", plan)
	}
}

func TestActiveCompactionWindowUsesUserCheckpointSummary(t *testing.T) {
	store := NewMemoryStore()
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", nil, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := BuildPlan(context.Background(), store, BuildInput{
		PromptScopeID: "thread",
		RunID:         "run",
		ThreadID:      "thread",
		TurnID:        "run",
		Provider:      "openai",
		Model:         "model",
		Toolset:       toolset,
		History: []session.Message{
			{Role: session.User, Content: "checkpoint summary", Kind: session.MessageKindCompactionSummary, CompactionID: "c1", CompactionGeneration: 4, CompactionWindowID: "w4"},
			{Role: session.User, Content: "tail user"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.CompactionGeneration != 4 || plan.CompactionWindowID != "w4" || plan.CompactionEntryID != "c1" {
		t.Fatalf("plan should find compaction window from user checkpoint: %#v", plan)
	}
}

func TestReusedCompactionSegmentRefreshesWindowMetadata(t *testing.T) {
	store := NewMemoryStore()
	now := time.Date(2026, 6, 2, 1, 2, 3, 0, time.UTC)
	toolset, _, err := EnsureToolset(context.Background(), store, "thread", "run", "thread", "run", "openai", "model", nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	input := BuildInput{
		PromptScopeID: "thread",
		RunID:         "run",
		ThreadID:      "thread",
		TurnID:        "run",
		Provider:      "openai",
		Model:         "model",
		Toolset:       toolset,
		History: []session.Message{
			{Role: session.User, Content: "summary", Kind: session.MessageKindCompactionSummary, CompactionID: "c1", CompactionGeneration: 1, CompactionWindowID: "w1"},
		},
	}
	first, _, err := BuildPlan(context.Background(), store, input)
	if err != nil {
		t.Fatal(err)
	}
	input.History = []session.Message{
		{Role: session.User, Content: "summary", Kind: session.MessageKindCompactionSummary, CompactionID: "c2", CompactionGeneration: 2, CompactionWindowID: "w2"},
	}
	second, _, err := BuildPlan(context.Background(), store, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Segments[len(first.Segments)-1].CompactionWindowID != "w1" {
		t.Fatalf("first plan window = %#v", first.Segments[len(first.Segments)-1])
	}
	if second.Segments[len(second.Segments)-1].CompactionWindowID != "w2" || second.Segments[len(second.Segments)-1].CompactionGeneration != 2 {
		t.Fatalf("reused segment metadata should refresh to latest window: %#v", second.Segments[len(second.Segments)-1])
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
