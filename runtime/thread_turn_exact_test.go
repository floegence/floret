package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v2/internal/session"
	"github.com/floegence/floret/v2/internal/sessiontree"
	"github.com/floegence/floret/v2/internal/storage"
	"github.com/floegence/floret/v2/internal/storage/sqlite"
)

func TestThreadReadHostReadThreadTurnEnforcesRootBinding(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, threadID := range []ThreadID{"first", "second"} {
		if _, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: threadID}); err != nil {
			t.Fatal(err)
		}
	}
	admitCompletedRuntimeTurn(t, ctx, store, "first", "turn", "run")
	admitCompletedRuntimeMessage(t, ctx, store, "first", "turn-rich", "run-rich", session.Message{
		Role:        session.User,
		Attachments: []session.MessageAttachment{{ResourceRef: "attachment:one", Name: "one.txt", MIMEType: "text/plain", SizeBytes: 3}},
		References:  []session.MessageReference{{ReferenceID: "ref", Kind: session.MessageReferenceText, Label: "Selection", Text: "quoted"}},
	})
	host, err := mustTestCapabilities(t, store).read.NewHost(ctx, "first")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := host.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "first", TurnID: "turn"})
	if err != nil || turn.TurnID != "turn" || turn.RunID != "run" {
		t.Fatalf("exact root turn=%#v err=%v", turn, err)
	}
	if _, err := host.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "first", TurnID: "missing"}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("missing turn err=%v, want ErrTurnNotFound", err)
	}
	if _, err := host.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "second", TurnID: "turn"}); err == nil || errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("wrong bound root err=%v, want bound mismatch distinct from ErrTurnNotFound", err)
	}
	rich, err := host.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "first", TurnID: "turn-rich"})
	if err != nil || len(rich.UserAttachments) != 1 || rich.UserAttachments[0].ResourceRef != "attachment:one" ||
		len(rich.UserReferences) != 1 || rich.UserReferences[0].ReferenceID != "ref" {
		t.Fatalf("rich exact turn=%#v err=%v", rich, err)
	}
	if err := maintenance.DeleteThread(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "first", TurnID: "turn"}); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("deleted root exact err=%v, want ErrThreadDeleted", err)
	}
}

func TestThreadReadHostReadThreadTurnReturnsNotFoundForEmptyThreadAcrossStores(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var store *Store
			var err error
			if backend == "sqlite" {
				store, err = openSQLiteStoreForTest(filepath.Join(t.TempDir(), "floret.db"))
			} else {
				store = NewMemoryStore()
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			maintenance, err := newTestMaintenanceHost(t, store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: "empty"}); err != nil {
				t.Fatal(err)
			}
			host, err := mustTestCapabilities(t, store).read.NewHost(ctx, "empty")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "empty", TurnID: "missing"}); !errors.Is(err, ErrTurnNotFound) || errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("empty thread exact read err=%v, want ErrTurnNotFound only", err)
			}
		})
	}
}

func TestReadThreadTurnAppliesLiveInterruptedOverlayAcrossStores(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "sqlite_reopen"} {
		t.Run(backend, func(t *testing.T) {
			var store *Store
			var path string
			var err error
			clock := &interruptedRecoveryTestClock{now: time.Date(2026, time.July, 26, 10, 0, 0, 0, time.UTC)}
			if backend == "sqlite_reopen" {
				path = filepath.Join(t.TempDir(), "floret.db")
				repo, openErr := sqlite.Open(path, sqlite.WithLeasePolicy(sessiontree.DefaultLeasePolicy), sqlite.WithAuthorityClock(clock.Now))
				if openErr != nil {
					t.Fatal(openErr)
				}
				store = &Store{repo: repo, prompt: repo, forkOperations: repo, agentTodos: repo, rootAuthority: repo}
			} else {
				repo, openErr := sessiontree.NewMemoryRepoWithLeasePolicy(sessiontree.DefaultLeasePolicy, clock.Now)
				if openErr != nil {
					t.Fatal(openErr)
				}
				store = NewMemoryStore()
				store.repo, store.rootAuthority, store.agentTodos, store.forkOperations = repo, repo, repo, storage.NewMemoryForkOperationStore(repo)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			authority := store.repo.(sessiontree.TurnAuthorityRepo)
			admitted, err := authority.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request",
				Input: session.Message{Role: session.User, Content: "input"},
			})
			if err != nil {
				t.Fatal(err)
			}
			clock.Set(admitted.Lease.ExpiresAt.Add(sessiontree.DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond))
			if backend == "sqlite_reopen" {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = openSQLiteStoreForTest(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { _ = store.Close() })
			maintenance, err := newTestMaintenanceHost(t, store)
			if err != nil {
				t.Fatal(err)
			}
			turn, err := maintenance.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "thread", TurnID: "turn"})
			if err != nil || turn.Status != TurnStatusInterrupted || turn.Projection.Status != TurnStatusRunning ||
				!turn.Recoverable || turn.Failure == nil || turn.Failure.Code != ThreadTurnFailureInterrupted {
				t.Fatalf("live interrupted exact=%#v err=%v", turn, err)
			}
		})
	}
}

func TestSubAgentReadHostReadThreadTurnEnforcesDescendantAuthorityAndLifecycle(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var store *Store
			var err error
			if backend == "sqlite" {
				store, err = openSQLiteStoreForTest(filepath.Join(t.TempDir(), "floret.db"))
			} else {
				store = NewMemoryStore()
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			for _, rootID := range []string{"root", "unrelated"} {
				if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: rootID}); err != nil {
					t.Fatal(err)
				}
			}
			publishTestSubAgentFixture(t, ctx, store, "publish-child-a", "root", "child-a", "")
			completeTestSubAgentFixture(t, ctx, store, "root", "child-a")
			publishTestSubAgentFixture(t, ctx, store, "publish-child-b", "child-a", "child-b", "")
			completeTestSubAgentFixture(t, ctx, store, "child-a", "child-b")
			publishTestSubAgentFixture(t, ctx, store, "publish-sibling", "root", "sibling", "")
			completeTestSubAgentFixture(t, ctx, store, "root", "sibling")

			rootRead := newTestSubAgentReadHost(t, store, "root")
			for _, test := range []struct {
				threadID ThreadID
				turnID   TurnID
			}{{"child-a", "fixture-turn:child-a"}, {"child-b", "fixture-turn:child-b"}} {
				turn, err := rootRead.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: test.threadID, TurnID: test.turnID})
				if err != nil || turn.TurnID != test.turnID {
					t.Fatalf("root descendant exact read thread=%q turn=%#v err=%v", test.threadID, turn, err)
				}
			}

			childRead := newTestSubAgentReadHost(t, store, "child-a")
			if _, err := childRead.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "child-b", TurnID: "missing"}); !errors.Is(err, ErrTurnNotFound) {
				t.Fatalf("authorized missing turn err=%v, want ErrTurnNotFound", err)
			}
			for _, rejected := range []ThreadID{"root", "child-a", "sibling", "unrelated", "missing"} {
				if _, err := childRead.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: rejected, TurnID: "fixture-turn:child-b"}); !errors.Is(err, ErrSubAgentNotFound) {
					t.Fatalf("rejected target %q err=%v, want ErrSubAgentNotFound", rejected, err)
				}
			}

			closeRepo := store.repo.(sessiontree.SubAgentCloseAuthorityRepo)
			closeReq := sessiontree.PrepareSubAgentCloseRequest{
				CloseOperationID: "close-child-b", ParentThreadID: "child-a", TargetThreadID: "child-b", Reason: "test",
			}
			if _, err := closeRepo.PrepareSubAgentClose(ctx, closeReq); err != nil {
				t.Fatal(err)
			}
			assertSubAgentExactTurn(t, ctx, rootRead, "child-b", "fixture-turn:child-b")
			if _, err := closeRepo.FinishSubAgentClose(ctx, sessiontree.FinishSubAgentCloseRequest{
				CloseOperationID: closeReq.CloseOperationID, ParentThreadID: closeReq.ParentThreadID,
				TargetThreadID: closeReq.TargetThreadID, Reason: closeReq.Reason,
			}); err != nil {
				t.Fatal(err)
			}
			assertSubAgentExactTurn(t, ctx, rootRead, "child-b", "fixture-turn:child-b")
		})
	}
}

func admitCompletedRuntimeTurn(t *testing.T, ctx context.Context, store *Store, threadID, turnID, runID string) {
	t.Helper()
	admitCompletedRuntimeMessage(t, ctx, store, threadID, turnID, runID, session.Message{Role: session.User, Content: "input"})
}

func admitCompletedRuntimeMessage(t *testing.T, ctx context.Context, store *Store, threadID, turnID, runID string, input session.Message) {
	t.Helper()
	authority := store.repo.(sessiontree.TurnAuthorityRepo)
	admitted, err := authority.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: "owner-" + turnID,
		RequestFingerprint: "request-" + turnID, Input: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: runID, TerminalEntryID: "terminal-" + turnID,
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "outcome-" + turnID,
	}); err != nil {
		t.Fatal(err)
	}
}

func assertSubAgentExactTurn(t *testing.T, ctx context.Context, host *SubAgentReadHost, threadID ThreadID, turnID TurnID) {
	t.Helper()
	turn, err := host.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: threadID, TurnID: turnID})
	if err != nil || turn.TurnID != turnID {
		t.Fatalf("subagent exact read thread=%q turn=%#v err=%v", threadID, turn, err)
	}
}
