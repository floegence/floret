package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

type providerContextFixtureRepo interface {
	Repo
	RuntimeTurnRepo
	ProviderContextRepo
	ProviderStateReader
}

func providerContextFixture(t *testing.T, repo providerContextFixtureRepo) ProviderContextCommit {
	t.Helper()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "input", RequestFingerprint: "input", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "inspect"}}); err != nil {
		t.Fatal(err)
	}
	return ProviderContextCommit{ThreadID: "thread", TurnID: "turn", RunID: "run", CompatibilityKey: "account:model", State: &provider.State{Kind: "native", ID: "complete", ReusePolicy: provider.StateReuseCanonicalHistory}, Now: time.Now(), Entries: []Entry{
		RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, Content: "Inspecting."}, time.Time{}),
		RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "call", ToolName: "inspect", ToolArgs: `{}`}, time.Time{}),
	}}
}

func TestProviderContextCommitRollsBackCanonicalOutputAndState(t *testing.T) {
	backend := newMigrationTestBackend()
	injected := errors.New("completed response write failed")
	failing := &toggleFailBackend{Backend: backend, err: injected}
	repo, err := NewBackendRepo(t.Context(), failing, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request := providerContextFixture(t, repo)
	before := cloneMigrationRecords(backend.records)
	beforeEntries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	failing.setFail(true)
	if _, err := repo.CommitProviderContext(t.Context(), request); !errors.Is(err, injected) {
		t.Fatalf("commit=%v", err)
	}
	afterEntries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeEntries, afterEntries) || !reflect.DeepEqual(before, backend.records) {
		t.Fatal("failed response installed partial canonical output")
	}
	if _, err := repo.ProviderState(t.Context(), "thread"); !errors.Is(err, ErrProviderStateNotFound) {
		t.Fatalf("failed response installed state: %v", err)
	}
	failing.setFail(false)
	entries, err := repo.CommitProviderContext(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := repo.ProviderState(t.Context(), "thread")
	if err != nil || saved.LeafEntryID != entries[1].ID {
		t.Fatalf("state is not anchored to complete output: %+v %v", saved, err)
	}
	if _, err := NewBackendRepo(t.Context(), failing, time.Now); err != nil {
		t.Fatal(err)
	}
}

func TestProviderContextValidationRollsBackEarlierEntry(t *testing.T) {
	repo := NewMemoryRepo()
	request := providerContextFixture(t, repo)
	before, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	request.Entries[1].ParentID = "missing-parent"
	if _, err := repo.CommitProviderContext(t.Context(), request); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("validation=%v", err)
	}
	after, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("validation failure left partial output or indexes")
	}
	request.Entries[1].ParentID = ""
	request.State.ReusePolicy = "unsupported"
	if _, err := repo.CommitProviderContext(t.Context(), request); err == nil {
		t.Fatal("accepted unknown state policy")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request.State.ReusePolicy = provider.StateReuseCanonicalHistory
	if _, err := repo.CommitProviderContext(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}

func TestProviderContextAndStopShareOneCanonicalWinner(t *testing.T) {
	for i := 0; i < 30; i++ {
		repo := NewMemoryRepo()
		request := providerContextFixture(t, repo)
		stop := testCancelTurnRequest(time.Now())
		stop.ClearProviderState, stop.PreserveProviderState = false, true
		var commitErr, stopErr error
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() { defer workers.Done(); <-start; _, commitErr = repo.CommitProviderContext(t.Context(), request) }()
		go func() { defer workers.Done(); <-start; _, stopErr = repo.CancelTurn(t.Context(), stop) }()
		close(start)
		workers.Wait()
		if stopErr != nil {
			t.Fatal(stopErr)
		}
		if commitErr != nil && !errors.Is(commitErr, ErrStaleAuthority) && !errors.Is(commitErr, context.Canceled) {
			t.Fatal(commitErr)
		}
		saved, err := repo.ProviderState(t.Context(), "thread")
		if commitErr == nil {
			if err != nil || saved.State.ID != "complete" || saved.LeafEntryID != stop.TerminalEntryID || saved.CreatedByRunID != "run" {
				t.Fatalf("stop lost committed history: %+v %v", saved, err)
			}
		} else if !errors.Is(err, ErrProviderStateNotFound) {
			t.Fatal("stop installed uncommitted state")
		}
		request.State.ID = "late"
		if _, err := repo.CommitProviderContext(t.Context(), request); !errors.Is(err, ErrStaleAuthority) {
			t.Fatalf("late commit=%v", err)
		}
	}
}

func TestProviderContextBackendCancellationAndPanicRollBack(t *testing.T) {
	for _, mode := range []string{"cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			underlying := newMigrationTestBackend()
			backend := &domainCommitBoundaryBackend{Backend: underlying}
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			request := providerContextFixture(t, repo)
			before, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			records := cloneMigrationRecords(underlying.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backend.afterCallback = func() error {
				current, err := repo.domainMemory.EncodeMemoryState()
				if err != nil {
					return err
				}
				if string(current) != string(before) {
					t.Fatal("response became authoritative before backend commit")
				}
				if mode == "panic" {
					panic("response transaction panic")
				}
				cancel()
				return ctx.Err()
			}
			panicked := false
			func() {
				defer func() { panicked = recover() != nil }()
				_, err = repo.CommitProviderContext(ctx, request)
			}()
			if mode == "panic" && !panicked || mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("panic=%v err=%v", panicked, err)
			}
			after, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) || !reflect.DeepEqual(records, underlying.records) {
				t.Fatal("aborted response changed canonical state")
			}
		})
	}
}

func TestPartialProviderOutputCannotAdmitCallsOrProviderState(t *testing.T) {
	repo := NewMemoryRepo()
	req := providerContextFixture(t, repo)
	req.Partial = true
	before, _ := repo.EncodeMemoryState()
	if _, err := repo.CommitProviderContext(t.Context(), req); err == nil {
		t.Fatal("partial output changed provider state")
	}
	req.State = nil
	if _, err := repo.CommitProviderContext(t.Context(), req); !errors.Is(err, ErrInvalidThreadAuthority) {
		t.Fatalf("partial output admitted calls: %v", err)
	}
	after, _ := repo.EncodeMemoryState()
	if string(before) != string(after) {
		t.Fatal("invalid partial output changed canonical facts")
	}
	req.Entries = req.Entries[:1]
	req.Entries[0].Message.ContentSourceID = "output:partial"
	entries, err := repo.CommitProviderContext(t.Context(), req)
	if err != nil || len(entries) != 1 || entries[0].Message.ContentSourceID != "output:partial" {
		t.Fatalf("partial commit=%+v %v", entries, err)
	}
}
