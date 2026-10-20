package cache

import (
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestToolRenderingRevisionChangesSurfaceOnceWithoutChangingHistory(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()
	history := []session.Message{{Role: session.User, Content: "keep history"}}
	old := lineagePlan("test", "model", "ns", "old-tools")
	old.RenderLineageKey = HashStrings("test", "model", Version, "ns", ContextProjectionRevision)
	if err := ValidateCanonicalLineage(ctx, store, "thread", &old, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	surface := TurnSurfaceSnapshot{Provider: "test", Model: "model", StateCompatibilityKey: "stable"}
	if _, err := RecordProviderRequest(ctx, store, ProviderRequestSnapshot{PromptScopeID: "thread", RunID: "old", ThreadID: "thread", TurnID: "old", Step: 1, Provider: "test", Model: "model", RawPlan: old, TurnSurface: surface}); err != nil {
		t.Fatal(err)
	}
	records, err := store.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	next := lineagePlan("test", "model", "ns", "ordered-tools")
	if err := ValidateCanonicalLineage(ctx, store, "thread", &next, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	changed, identityChanged, err := ProviderSurfaceChanges(ctx, store, "thread", next.RenderLineageKey, surface)
	if err != nil || !changed || identityChanged || next.CanonicalLineageReset {
		t.Fatalf("surface=%v identity=%v reset=%v err=%v", changed, identityChanged, next.CanonicalLineageReset, err)
	}
	if next.CanonicalHistoryPrefixHash != old.CanonicalHistoryPrefixHash {
		t.Fatal("canonical history changed")
	}
	if _, err := RecordProviderRequest(ctx, store, ProviderRequestSnapshot{PromptScopeID: "thread", RunID: "new", ThreadID: "thread", TurnID: "new", Step: 1, Provider: "test", Model: "model", RawPlan: next, TurnSurface: surface}); err != nil {
		t.Fatal(err)
	}
	changed, identityChanged, err = ProviderSurfaceChanges(ctx, store, "thread", next.RenderLineageKey, surface)
	if err != nil || changed || identityChanged {
		t.Fatalf("repeated boundary: %v %v %v", changed, identityChanged, err)
	}
	after, err := store.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records[0], after[0]) {
		t.Fatal("rewrote historical request")
	}
}
