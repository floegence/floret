package cache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestMemoryLatestRequestUsesScopeAppendOrderAndDetachedValues(t *testing.T) {
	store := NewMemoryStore()
	if _, found, err := store.LatestProviderRequest(t.Context(), "thread"); err != nil || found {
		t.Fatalf("empty: %v %v", found, err)
	}
	for i := range 1000 {
		request := ProviderRequestRecord{ID: fmt.Sprint(i), PromptScopeID: "thread", SegmentIDs: []string{"segment"}, Attempt: i + 1}
		if err := store.AppendProviderRequest(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if err := store.CheckpointProviderRequest(t.Context(), nil, nil, ProviderRequestRecord{ID: "other", PromptScopeID: "unrelated"}); err != nil {
			t.Fatal(err)
		}
	}
	got, found, err := LatestProviderRequest(t.Context(), NewPreparedStore(store), "thread")
	if err != nil || !found || got.ID != "999" {
		t.Fatalf("latest: %+v %v %v", got, found, err)
	}
	all, err := store.ProviderRequests(t.Context(), "thread")
	if err != nil || !reflect.DeepEqual(got, all[len(all)-1]) {
		t.Fatalf("full parity: %v", err)
	}
	got.SegmentIDs[0] = "changed"
	again, _, err := store.LatestProviderRequest(t.Context(), "thread")
	if err != nil || again.SegmentIDs[0] != "segment" {
		t.Fatal("caller mutated stored request")
	}
	if err := store.DeletePromptScopes(t.Context(), "unrelated"); err != nil {
		t.Fatal(err)
	}
	again, found, err = store.LatestProviderRequest(t.Context(), "thread")
	if err != nil || !found || again.ID != "999" {
		t.Fatalf("reindexed: %+v %v", again, err)
	}
	if err := store.DeletePromptScopes(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LatestProviderRequest(t.Context(), "thread"); err != nil || found {
		t.Fatalf("deleted: %v %v", found, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := store.LatestProviderRequest(ctx, "thread"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}
