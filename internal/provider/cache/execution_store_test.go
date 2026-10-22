package cache

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

type executionTestStore struct {
	*MemoryStore
	reads, checkpoints int
	readErr, writeErr  error
}

func (s *executionTestStore) ProviderRequests(ctx context.Context, scope string) ([]ProviderRequestRecord, error) {
	s.reads++
	if s.readErr != nil {
		return nil, s.readErr
	}
	return s.MemoryStore.ProviderRequests(ctx, scope)
}
func (s *executionTestStore) CheckpointProviderRequest(ctx context.Context, segments []Segment, toolsets []ToolsetSnapshot, request ProviderRequestRecord) error {
	s.checkpoints++
	if s.writeErr != nil {
		return s.writeErr
	}
	return s.MemoryStore.CheckpointProviderRequest(ctx, segments, toolsets, request)
}
func executionTestSurface(t *testing.T, text string) TurnSurfaceSnapshot {
	t.Helper()
	surface, err := normalizeTurnSurface(TurnSurfaceSnapshot{Provider: "test", Model: "model", SystemPrompt: text, Tools: []ToolDefinition{{Name: "read", InputSchema: map[string]any{"type": "object"}}}, AdapterVersion: Version, StateCompatibilityKey: "test:v1"})
	if err != nil {
		t.Fatal(err)
	}
	return surface
}
func TestExecutionFactsReadOnceAndAdvanceOnlyAfterCommit(t *testing.T) {
	ctx := t.Context()
	base := &executionTestStore{MemoryStore: NewMemoryStore()}
	store := NewExecutionStore(base, "scope", "turn")
	first := executionTestSurface(t, "initial")
	for range 3 {
		if _, found, err := InitialTurnSurface(ctx, store, "scope", "turn"); err != nil || found {
			t.Fatalf("empty: %v %v", found, err)
		}
		if err := ValidateTurnModel(ctx, store, "scope", "turn", "test", "model"); err != nil {
			t.Fatal(err)
		}
	}
	if base.reads != 1 {
		t.Fatalf("reads=%d, want one", base.reads)
	}
	staged := NewPreparedStore(store)
	if err := staged.AppendToolset(ctx, ToolsetSnapshot{PromptScopeID: "scope", Provider: "test", Model: "model", Tools: first.Tools}); err != nil {
		t.Fatal(err)
	}
	request := ProviderRequestRecord{PromptScopeID: "scope", TurnID: "turn", Provider: "test", Model: "model", TurnSurface: first}
	writeErr := errors.New("checkpoint failed")
	base.writeErr = writeErr
	if err := staged.AppendProviderRequest(ctx, request); !errors.Is(err, writeErr) {
		t.Fatal(err)
	}
	if _, found, err := InitialTurnSurface(ctx, staged, "scope", "turn"); err != nil || found {
		t.Fatalf("failed candidate leaked: %v %v", found, err)
	}
	if _, found, err := base.ActiveToolset(ctx, "scope", "test", "model"); err != nil || found {
		t.Fatalf("partial toolset: %v %v", found, err)
	}
	base.writeErr = nil
	if err := staged.AppendProviderRequest(ctx, request); err != nil {
		t.Fatal(err)
	}
	first.Tools[0].Name = "caller-mutated"
	for range 3 {
		got, found, err := InitialTurnSurface(ctx, staged, "scope", "turn")
		if err != nil || !found || got.Tools[0].Name != "read" {
			t.Fatalf("surface=%+v found=%v err=%v", got, found, err)
		}
		got.Tools[0].InputSchema["type"] = "mutated"
		if err := ValidateTurnModel(ctx, staged, "scope", "turn", "test", "model"); err != nil {
			t.Fatal(err)
		}
	}
	if base.reads != 1 || base.checkpoints != 2 {
		t.Fatalf("reads=%d checkpoints=%d", base.reads, base.checkpoints)
	}
	refreshed := executionTestSurface(t, "refreshed")
	got, found, err := ResolveTurnSurface(ctx, staged, "scope", "turn", refreshed, true)
	if err != nil || !found || got.SystemPrompt != "refreshed" {
		t.Fatalf("refresh: %+v %v", got, err)
	}
	request.TurnSurface = got
	if err := store.AppendProviderRequest(ctx, request); err != nil {
		t.Fatal(err)
	}
	original, _, err := InitialTurnSurface(ctx, store, "scope", "turn")
	if err != nil || original.SystemPrompt != "initial" {
		t.Fatalf("original changed: %+v %v", original, err)
	}
	restarted := NewExecutionStore(base, "scope", "turn")
	restored, found, err := InitialTurnSurface(ctx, restarted, "scope", "turn")
	if err != nil || !found || !reflect.DeepEqual(original, restored) || base.reads != 2 {
		t.Fatalf("restart=%+v reads=%d err=%v", restored, base.reads, err)
	}
	request.Model = "drift"
	if err := store.AppendProviderRequest(ctx, request); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []Store{store, NewExecutionStore(base, "scope", "turn")} {
		for _, model := range []string{"model", "drift"} {
			if err := ValidateTurnModel(ctx, candidate, "scope", "turn", "test", model); !errors.Is(err, ErrTurnModelDrift) {
				t.Fatalf("drift lost for %s: %v", model, err)
			}
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := InitialTurnSurface(cancelled, store, "scope", "turn"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if err := store.(Deleter).DeletePromptScopes(ctx, "scope"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := InitialTurnSurface(ctx, store, "scope", "turn"); err != nil || found {
		t.Fatalf("delete=%v %v", found, err)
	}
}

func TestExecutionFactsPreserveMatchingAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name             string
		turn, run, model string
		surface          bool
		drift            bool
	}{
		{"exact", "turn", "run", "other", true, true},
		{"run fallback", "", "turn", "other", true, false},
		{"trimmed surface only", " turn ", "run", "other", true, false},
		{"turn beats run", "other", "turn", "other", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &executionTestStore{MemoryStore: NewMemoryStore()}
			surface := executionTestSurface(t, "first")
			if err := base.AppendProviderRequest(t.Context(), ProviderRequestRecord{PromptScopeID: "scope", TurnID: tc.turn, RunID: tc.run, Provider: "test", Model: tc.model, TurnSurface: surface}); err != nil {
				t.Fatal(err)
			}
			store := NewExecutionStore(base, "scope", "turn")
			_, found, err := InitialTurnSurface(t.Context(), store, "scope", "turn")
			if err != nil || found != tc.surface {
				t.Fatalf("surface: %v %v", found, err)
			}
			if err := ValidateTurnModel(t.Context(), store, "scope", "turn", "test", "model"); errors.Is(err, ErrTurnModelDrift) != tc.drift {
				t.Fatalf("model: %v", err)
			}
			if _, found, err := InitialTurnSurface(t.Context(), store, "other", "turn"); err != nil || found {
				t.Fatalf("scope leak: %v %v", found, err)
			}
		})
	}
	for _, badHash := range []string{"", "tampered"} {
		t.Run("hash="+badHash, func(t *testing.T) {
			base := &executionTestStore{MemoryStore: NewMemoryStore()}
			surface := executionTestSurface(t, "first")
			surface.Hash = badHash
			if err := base.AppendProviderRequest(t.Context(), ProviderRequestRecord{PromptScopeID: "scope", TurnID: "turn", TurnSurface: surface}); err != nil {
				t.Fatal(err)
			}
			store := NewExecutionStore(base, "scope", "turn")
			base.readErr = errors.New("read failed")
			if _, _, err := InitialTurnSurface(t.Context(), store, "scope", "turn"); !errors.Is(err, base.readErr) {
				t.Fatal(err)
			}
			base.readErr = nil
			if _, _, err := InitialTurnSurface(t.Context(), store, "scope", "turn"); err == nil {
				t.Fatal("invalid surface accepted")
			}
			if base.reads != 2 {
				t.Fatalf("read failure cached: %d", base.reads)
			}
		})
	}
}

func TestLineageFastPathPreservesReturningSurfaceChecks(t *testing.T) {
	ctx := t.Context()
	base := &executionTestStore{MemoryStore: NewMemoryStore()}
	history := []session.Message{{Role: session.User, Content: "hello"}}
	check := func(system string, segments ...string) (RawPlan, error) {
		plan := lineagePlan("test", "model", "ns", append([]string{system}, segments...)...)
		err := ValidateCanonicalLineage(ctx, base, "scope", &plan, system, nil, nil, nil, history)
		return plan, err
	}
	first, err := check("first", "message")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(ctx, base, PromptScopeRef{PromptScopeID: "scope", RunID: "run", TurnID: "turn"}, 1, "test", "model", CachePolicy{}, first); err != nil {
		t.Fatal(err)
	}
	base.reads = 0
	if _, err := check("first", "message"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ProviderSurfaceChanges(ctx, base, "scope", first.RenderLineageKey, TurnSurfaceSnapshot{Provider: "test", Model: "model"}); err != nil {
		t.Fatal(err)
	}
	if base.reads != 0 {
		t.Fatalf("stable request read all history %d times", base.reads)
	}
	second, err := check("second", "message")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(ctx, base, PromptScopeRef{PromptScopeID: "scope", RunID: "run", TurnID: "turn"}, 2, "test", "model", CachePolicy{}, second); err != nil {
		t.Fatal(err)
	}
	if _, err := check("first", "changed"); !errors.Is(err, ErrContextRenderLineageChanged) {
		t.Fatalf("returning lineage drift accepted: %v", err)
	}
	if _, err := check("first", "message"); err != nil {
		t.Fatalf("valid returning surface: %v", err)
	}
}
