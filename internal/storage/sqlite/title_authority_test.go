package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

type titleAuthorityTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	Thread(context.Context, string) (sessiontree.ThreadMeta, error)
	AdmitTurn(context.Context, sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error)
	FinishTurn(context.Context, sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error)
	SetThreadTitle(context.Context, sessiontree.SetThreadTitleRequest) (sessiontree.SetThreadTitleResult, error)
	DeleteRootTree(context.Context, string) (sessiontree.DeleteRootTreeResult, error)
}

func TestThreadTitleAuthorityMatchesMemory(t *testing.T) {
	now := time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "title-authority.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for backend, repo := range map[string]titleAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(backend, func(t *testing.T) {
			initial := sessiontree.ThreadMeta{
				ID: "thread", ForkedFromThreadID: "source", ForkedFromEntryID: "source-entry",
				ForkOperationID: "fork-operation", ForkOperationNodeID: "fork-node",
			}
			created, err := repo.CreateThread(context.Background(), initial)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(context.Background(), sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				RequestFingerprint: "admit", Input: session.Message{Role: session.User, Content: "hello"}, Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}

			manualDuringTurn := sessiontree.SetThreadTitleRequest{
				ThreadID: "thread", Mode: sessiontree.ThreadTitleMutationManual, Title: "manual",
				Status: sessiontree.ThreadTitleReady, Source: sessiontree.ThreadTitleSourceHost, Now: now.Add(time.Minute),
			}
			if _, err := repo.SetThreadTitle(context.Background(), manualDuringTurn); !errors.Is(err, sessiontree.ErrActiveTurn) {
				t.Fatalf("manual title without active proof err=%v, want ErrActiveTurn", err)
			}
			unchanged, err := repo.Thread(context.Background(), "thread")
			if err != nil || unchanged.Title != "" || !unchanged.TitleUpdatedAt.IsZero() {
				t.Fatalf("title changed without active proof: %#v err=%v", unchanged, err)
			}

			automatic := sessiontree.SetThreadTitleRequest{
				ThreadID: "thread", Mode: sessiontree.ThreadTitleMutationAutomatic, Title: "provider title",
				Status: sessiontree.ThreadTitleReady, Source: sessiontree.ThreadTitleSourceProvider, Now: now.Add(2 * time.Minute),
			}
			leaseCtx := sessiontree.ContextWithTurnLease(context.Background(), admitted.Lease)
			automaticResult, err := repo.SetThreadTitle(leaseCtx, automatic)
			if err != nil || !automaticResult.Changed || automaticResult.Thread.TitleSource != sessiontree.ThreadTitleSourceProvider {
				t.Fatalf("automatic title result=%#v err=%v", automaticResult, err)
			}
			if _, err := repo.FinishTurn(context.Background(), sessiontree.FinishTurnRequest{
				Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
				OutcomeFingerprint: "finish", Now: now.Add(3 * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}

			manual := automatic
			manual.Mode = sessiontree.ThreadTitleMutationManual
			manual.Source = sessiontree.ThreadTitleSourceHost
			manual.Now = now.Add(4 * time.Minute)
			manualResult, err := repo.SetThreadTitle(context.Background(), manual)
			if err != nil || !manualResult.Changed || manualResult.Thread.TitleSource != sessiontree.ThreadTitleSourceHost {
				t.Fatalf("manual title must establish host authority even when text matches: result=%#v err=%v", manualResult, err)
			}
			idempotent := manual
			idempotent.Now = now.Add(5 * time.Minute)
			replayed, err := repo.SetThreadTitle(context.Background(), idempotent)
			if err != nil || replayed.Changed || !replayed.Thread.TitleUpdatedAt.Equal(manual.Now) {
				t.Fatalf("idempotent manual title result=%#v err=%v", replayed, err)
			}
			if replayed.Thread.ForkedFromThreadID != created.ForkedFromThreadID ||
				replayed.Thread.ForkedFromEntryID != created.ForkedFromEntryID ||
				replayed.Thread.ForkOperationID != created.ForkOperationID ||
				replayed.Thread.ForkOperationNodeID != created.ForkOperationNodeID {
				t.Fatalf("title mutation changed fork lineage: before=%#v after=%#v", created, replayed.Thread)
			}

			if _, err := repo.DeleteRootTree(context.Background(), "thread"); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.SetThreadTitle(context.Background(), manual); !errors.Is(err, sessiontree.ErrThreadDeleted) {
				t.Fatalf("deleted thread title err=%v, want ErrThreadDeleted", err)
			}
		})
	}
}
