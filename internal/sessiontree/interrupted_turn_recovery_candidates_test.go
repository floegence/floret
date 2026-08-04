package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
)

func TestListInterruptedTurnRecoveryCandidatesReturnsStableRootAndChildOrder(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, meta := range []ThreadMeta{
		{ID: "root-old", CreatedAt: now.Add(-2 * time.Minute)},
		{ID: "root-new", CreatedAt: now.Add(-time.Minute)},
		{ID: "child", ParentThreadID: "root-new", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "/root/child", CreatedAt: now.Add(-90 * time.Second)},
	} {
		if _, err := repo.CreateThread(ctx, meta); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range []AdmitTurnRequest{
		{ThreadID: "root-old", TurnID: "turn-old", RunID: "run-old", OwnerID: "owner-old", Input: session.Message{Role: session.User, Content: "old"}, RequestFingerprint: "old", Now: now},
		{ThreadID: "root-new", TurnID: "turn-new", RunID: "run-new", OwnerID: "owner-new", Input: session.Message{Role: session.User, Content: "new"}, RequestFingerprint: "new", Now: now},
		{ThreadID: "child", TurnID: "turn-child", RunID: "run-child", OwnerID: "owner-child", Input: session.Message{Role: session.User, Content: "child"}, RequestFingerprint: "child", Now: now},
	} {
		if _, err := repo.AdmitTurn(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.ListInterruptedTurnRecoveryCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []InterruptedTurnRecoveryCandidate{
		{ThreadID: "root-new"},
		{ThreadID: "child", ParentThreadID: "root-new"},
		{ThreadID: "root-old"},
	}
	if len(got) != len(want) {
		t.Fatalf("candidate count = %d, want %d: %#v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("candidate %d = %#v, want %#v", index, got[index], want[index])
		}
	}
	second, err := repo.ListInterruptedTurnRecoveryCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != len(got) {
		t.Fatalf("repeat candidate count = %d, want %d", len(second), len(got))
	}
	for index := range got {
		if second[index] != got[index] {
			t.Fatalf("repeat candidate %d = %#v, want %#v", index, second[index], got[index])
		}
	}
}

func TestListInterruptedTurnRecoveryCandidatesSkipsNonTurnLeasesAndRejectsDrift(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	mutation, err := repo.AcquireTurnLease(ctx, TurnLease{ThreadID: "thread", Purpose: TurnLeasePurposeMutation, MutationID: "mutation", MutationKind: CompactionMutationKind, OwnerID: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.ListInterruptedTurnRecoveryCandidates(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("mutation lease candidates = %#v, err=%v, want empty", got, err)
	}
	repo.leases["thread"] = TurnLease{ThreadID: "thread", Purpose: TurnLeasePurposeTurn, TurnID: "missing", OwnerID: mutation.OwnerID, Generation: mutation.Generation, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(time.Minute)}
	if _, err := repo.ListInterruptedTurnRecoveryCandidates(ctx); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("drift candidates err=%v, want ErrAuthorityCorrupt", err)
	}
}
