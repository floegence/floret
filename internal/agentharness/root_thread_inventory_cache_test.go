package agentharness

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessionlifecycle"
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

func TestRootThreadInventoryProjectsFreshDurableTurnLeaseAsRunning(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn-interrupted", sessiontree.TurnStarted, map[string]string{"run_id": "run-interrupted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn-interrupted", sessiontree.TurnAborted, map[string]string{"run_id": "run-interrupted", "recoverable": "true"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-active", RunID: "run-active", OwnerID: "owner-active",
		RequestFingerprint: "request-active", Now: now,
		Input: session.Message{Role: session.User, Content: "continue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.RenewTurnLease(ctx, admitted.Lease); err != nil {
		t.Fatal(err)
	}

	// Durable renewal commits before the process-local registry advances. A
	// reader in this window must recognize both proofs as one active lineage.
	harness := New(Options{
		Repo: repo, Now: func() time.Time { return now },
		TurnExecutions: testTurnExecutionRegistry(admitted.Lease),
	})
	inventory, err := harness.ListRootThreadInventory(ctx, ListRootThreadSummariesOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory) != 1 {
		t.Fatalf("inventory length = %d, want 1", len(inventory))
	}
	thread := inventory[0].Overview.Thread
	if thread.Status != "running" || thread.Phase != sessionlifecycle.PhaseTurn || thread.LatestTurnID != "turn-active" || thread.Recoverable {
		t.Fatalf("fresh durable turn projection = %#v, want active turn running without stale interruption", thread)
	}
}

func TestRootThreadInventoryDoesNotProjectExpiredTurnLeaseAsRunning(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-expired", RunID: "run-expired", OwnerID: "owner-expired",
		RequestFingerprint: "request-expired", Now: now,
		Input: session.Message{Role: session.User, Content: "inspect"},
	})
	if err != nil {
		t.Fatal(err)
	}

	harness := New(Options{
		Repo: repo, Now: func() time.Time { return now.Add(2 * time.Minute) },
		TurnExecutions: testTurnExecutionRegistry(admitted.Lease),
	})
	inventory, err := harness.ListRootThreadInventory(ctx, ListRootThreadSummariesOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	thread := inventory[0].Overview.Thread
	if thread.Status != "interrupted" || thread.Phase != sessionlifecycle.PhaseIdle || !thread.Recoverable {
		t.Fatalf("expired durable turn projection = %#v, want recoverable interruption", thread)
	}
}

func testTurnExecutionRegistry(active sessiontree.TurnLease) *TurnExecutionRegistry {
	return &TurnExecutionRegistry{
		Register:   func(sessiontree.TurnLease) error { return nil },
		Renew:      func(sessiontree.TurnLease, sessiontree.TurnLease) error { return nil },
		Unregister: func(sessiontree.TurnLease) {},
		Active: func(threadID string) (sessiontree.TurnLease, bool) {
			return active, threadID == active.ThreadID
		},
	}
}
