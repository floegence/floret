package agentharness

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/sessiontree"
)

func TestRootThreadInventoryProjectionCacheReusesExactRevisionAndIsolatesCallers(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "thread", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	clockReads := 0
	harness := New(Options{Repo: repo, Now: func() time.Time {
		clockReads++
		return now
	}})

	first, err := harness.ListRootThreadInventory(ctx, ListRootThreadSummariesOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].ProjectionFingerprint == [32]byte{} || clockReads != 1 {
		t.Fatalf("first inventory=%#v clock reads=%d", first, clockReads)
	}
	first[0].Overview.Thread.Title = "caller mutation"

	second, err := harness.ListRootThreadInventory(ctx, ListRootThreadSummariesOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].Overview.Thread.Title != "" {
		t.Fatalf("cached projection leaked caller mutation: %#v", second)
	}
	if clockReads != 1 {
		t.Fatalf("unchanged projection was rebuilt; clock reads=%d, want 1", clockReads)
	}
}

func TestRootThreadInventoryProjectionCacheInvalidatesChangedSourceFacts(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "thread", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	clockReads := 0
	harness := New(Options{Repo: repo, Now: func() time.Time {
		clockReads++
		return now
	}})
	first, err := harness.ListRootThreadInventory(ctx, ListRootThreadSummariesOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SetThreadTitle(ctx, sessiontree.SetThreadTitleRequest{
		ThreadID: "thread", Title: "after", Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	second, err := harness.ListRootThreadInventory(ctx, ListRootThreadSummariesOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].Overview.Thread.Title != "after" {
		t.Fatalf("changed projection was not rebuilt: %#v", second)
	}
	if second[0].ProjectionFingerprint == first[0].ProjectionFingerprint || clockReads != 2 {
		t.Fatalf("cache invalidation first=%#v second=%#v clock reads=%d", first[0], second[0], clockReads)
	}
}
