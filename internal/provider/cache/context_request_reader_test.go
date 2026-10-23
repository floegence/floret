package cache

import (
	"context"
	"errors"
	"testing"
)

func TestContextRequestLookupPreservesIdentityAppendOrderAndDeletion(t *testing.T) {
	s := NewMemoryStore()
	for _, r := range []ProviderRequestRecord{
		{ID: "old", PromptScopeID: "scope", ThreadID: "thread", ModelContextEntryID: "context"},
		{ID: "latest-match", PromptScopeID: "scope", ThreadID: "thread", ModelContextEntryID: "context"},
		{ID: "different-thread", PromptScopeID: "scope", ThreadID: "other", ModelContextEntryID: "context"},
		{ID: "different-context", PromptScopeID: "scope", ThreadID: "thread", ModelContextEntryID: "other"},
		{ID: "different-scope", PromptScopeID: "other", ThreadID: "thread", ModelContextEntryID: "context"},
	} {
		if err := s.AppendProviderRequest(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	check := func(store Store) {
		t.Helper()
		got, found, err := LatestModelContextRequest(t.Context(), store, "scope", "thread", "context")
		if err != nil || !found || got.ID != "latest-match" {
			t.Fatalf("got=%+v found=%v err=%v", got, found, err)
		}
	}
	check(s)
	check(NewPreparedStore(s))
	if _, found, err := LatestModelContextRequest(t.Context(), s, "scope", "thread", "unused"); err != nil || found {
		t.Fatalf("absent: %v %v", found, err)
	}
	if err := s.DeletePromptScopes(t.Context(), "other"); err != nil {
		t.Fatal(err)
	}
	check(s)
	if err := s.DeletePromptScopes(t.Context(), "scope"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := LatestModelContextRequest(t.Context(), s, "scope", "thread", "context"); err != nil || found {
		t.Fatalf("deleted: %v %v", found, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := LatestModelContextRequest(ctx, s, "scope", "thread", "context"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
