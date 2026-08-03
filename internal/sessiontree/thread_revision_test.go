package sessiontree_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
	. "github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

var (
	_ ThreadRevisionReader  = (*BackendRepo)(nil)
	_ ThreadRevisionUpdater = (*BackendRepo)(nil)
)

func TestBackendRepoThreadRevisionContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		source publicstorage.Source
	}{
		{name: "memory", source: publicstorage.Memory()},
		{name: "sqlite", source: publicstorage.SQLite(filepath.Join(t.TempDir(), "revision.db"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend, repo := openRevisionRepo(t, test.source)
			defer backend.Close()

			if err := repo.CurrentThreadView(ctx, "missing", func(*MemoryRepo, ThreadRevision) error {
				t.Fatal("missing current view invoked callback")
				return nil
			}); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("missing current view error = %v, want ErrThreadNotFound", err)
			}
			if err := repo.CurrentThreadView(ctx, "missing", nil); err == nil {
				t.Fatal("nil current view callback succeeded")
			}
			if _, err := repo.CurrentThreadRevision(ctx, "missing"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("missing current revision error = %v, want ErrThreadNotFound", err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "root"}); err != nil {
				t.Fatal(err)
			}
			createdRevision := currentRevision(t, repo, "root")
			if createdRevision != 1 {
				t.Fatalf("created revision = %d, want 1", createdRevision)
			}
			viewCalls := 0
			if err := repo.CurrentThreadView(ctx, "root", func(memory *MemoryRepo, revision ThreadRevision) error {
				viewCalls++
				if revision != createdRevision {
					t.Fatalf("current view revision = %d, want %d", revision, createdRevision)
				}
				meta, err := memory.Thread(ctx, "root")
				if err != nil {
					return err
				}
				if meta.ID != "root" {
					t.Fatalf("current view thread = %#v", meta)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if viewCalls != 1 {
				t.Fatalf("current view callbacks = %d, want 1", viewCalls)
			}
			created := stateAt(t, repo, "root", createdRevision)
			if created.Thread == nil || created.Thread.ID != "root" || len(created.Entries) != 0 || created.CommittedAt.IsZero() ||
				!containsRevisionDomain(created.ChangedDomains, ThreadRevisionDomainThread) {
				t.Fatalf("created state = %#v", created)
			}

			entry, err := repo.Append(ctx, Entry{
				ID: "entry", ThreadID: "root", TurnID: "turn", Type: EntryUserMessage,
				Message:  session.Message{Role: session.User, Content: "original"},
				Metadata: map[string]string{"version": "original"},
			}, AppendOptions{ID: "entry"})
			if err != nil {
				t.Fatal(err)
			}
			entryRevision := currentRevision(t, repo, "root")
			if entryRevision != createdRevision+1 {
				t.Fatalf("entry revision = %d, want %d", entryRevision, createdRevision+1)
			}
			entry.Metadata["version"] = "caller-mutated"
			old := stateAt(t, repo, "root", createdRevision)
			if len(old.Entries) != 0 {
				t.Fatalf("historical state changed after append: %#v", old.Entries)
			}
			withEntry := stateAt(t, repo, "root", entryRevision)
			if !containsRevisionDomain(withEntry.ChangedDomains, ThreadRevisionDomainJournal) {
				t.Fatalf("entry revision domains = %#v", withEntry.ChangedDomains)
			}
			withEntry.Entries[0].Metadata["version"] = "reader-mutated"
			withEntryAgain := stateAt(t, repo, "root", entryRevision)
			if got := withEntryAgain.Entries[0].Metadata["version"]; got != "original" {
				t.Fatalf("stored historical entry was mutable through reader: %q", got)
			}

			if err := repo.UpdateDomainAtRevision(ctx, "root", createdRevision, func(*MemoryRepo, spi.WriteTx) error {
				t.Fatal("stale CAS invoked mutation")
				return nil
			}); !errors.Is(err, ErrThreadRevisionConflict) {
				t.Fatalf("stale CAS error = %v, want ErrThreadRevisionConflict", err)
			}
			if got := currentRevision(t, repo, "root"); got != entryRevision {
				t.Fatalf("stale CAS advanced revision to %d", got)
			}
			if err := repo.UpdateDomainAtRevision(ctx, "root", entryRevision, func(memory *MemoryRepo, _ spi.WriteTx) error {
				_, err := memory.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "root", Title: "Bound title", Now: time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			titleRevision := currentRevision(t, repo, "root")
			if titleRevision != entryRevision+1 {
				t.Fatalf("title revision = %d, want %d", titleRevision, entryRevision+1)
			}
			if got := stateAt(t, repo, "root", titleRevision).Thread.Title; got != "Bound title" {
				t.Fatalf("title = %q", got)
			}
			if err := repo.UpdateDomainAtRevision(ctx, "root", titleRevision, func(*MemoryRepo, spi.WriteTx) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if got := currentRevision(t, repo, "root"); got != titleRevision {
				t.Fatalf("no-op CAS advanced revision to %d", got)
			}

			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "child", ParentThreadID: "root", ParentTurnID: "turn", TaskName: "worker", AgentPath: "/root/worker"}); err != nil {
				t.Fatal(err)
			}
			parentPublicationRevision := currentRevision(t, repo, "root")
			childCreatedRevision := currentRevision(t, repo, "child")
			if parentPublicationRevision != titleRevision+1 || childCreatedRevision != 1 {
				t.Fatalf("publication revisions parent=%d child=%d", parentPublicationRevision, childCreatedRevision)
			}
			if children := stateAt(t, repo, "root", parentPublicationRevision).Children; len(children) != 1 || children[0].ThreadID != "child" {
				t.Fatalf("parent child publication = %#v", children)
			}
			if _, err := repo.Append(ctx, Entry{ID: "child-entry", ThreadID: "child", Type: EntryCustom}, AppendOptions{ID: "child-entry"}); err != nil {
				t.Fatal(err)
			}
			if got := currentRevision(t, repo, "root"); got != parentPublicationRevision {
				t.Fatalf("child execution advanced parent revision to %d", got)
			}
			if got := currentRevision(t, repo, "child"); got != childCreatedRevision+1 {
				t.Fatalf("child execution revision = %d", got)
			}

			if _, err := repo.DeleteRootTree(ctx, "root"); err != nil {
				t.Fatal(err)
			}
			deletedRevision := currentRevision(t, repo, "root")
			if deletedRevision != parentPublicationRevision+1 {
				t.Fatalf("deleted revision = %d, want %d", deletedRevision, parentPublicationRevision+1)
			}
			deleted := stateAt(t, repo, "root", deletedRevision)
			if deleted.Thread != nil || deleted.Tombstone == nil || deleted.Tombstone.ThreadID != "root" || len(deleted.Entries) != 0 || len(deleted.Children) != 0 {
				t.Fatalf("deleted state retained queryable lifecycle: %#v", deleted)
			}
			if !containsRevisionDomain(deleted.ChangedDomains, ThreadRevisionDomainDeleted) {
				t.Fatalf("deleted revision domains = %#v", deleted.ChangedDomains)
			}
			if err := repo.CurrentThreadView(ctx, "root", func(*MemoryRepo, ThreadRevision) error {
				t.Fatal("deleted current view invoked callback")
				return nil
			}); !errors.Is(err, ErrThreadDeleted) {
				t.Fatalf("deleted current view error = %v, want ErrThreadDeleted", err)
			}
			if _, err := repo.ThreadStateAtRevision(ctx, "root", parentPublicationRevision); !errors.Is(err, ErrRevisionUnavailable) {
				t.Fatalf("pre-delete revision error = %v, want ErrRevisionUnavailable", err)
			}
			if _, err := repo.DeleteRootTree(ctx, "root"); err != nil {
				t.Fatal(err)
			}
			if got := currentRevision(t, repo, "root"); got != deletedRevision {
				t.Fatalf("delete replay advanced revision to %d", got)
			}
		})
	}
}

func containsRevisionDomain(domains []ThreadRevisionDomain, want ThreadRevisionDomain) bool {
	for _, domain := range domains {
		if domain == want {
			return true
		}
	}
	return false
}

func TestBackendRepoThreadRevisionSurvivesSQLiteRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.db")
	backend, repo := openRevisionRepo(t, publicstorage.SQLite(path))
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{ID: "entry", ThreadID: "thread", Type: EntryCustom}, AppendOptions{ID: "entry"}); err != nil {
		t.Fatal(err)
	}
	revision := currentRevision(t, repo, "thread")
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedBackend, reopened := openRevisionRepo(t, publicstorage.SQLite(path))
	defer reopenedBackend.Close()
	if got := currentRevision(t, reopened, "thread"); got != revision {
		t.Fatalf("revision after restart = %d, want %d", got, revision)
	}
	if state := stateAt(t, reopened, "thread", revision); len(state.Entries) != 1 || state.Entries[0].ID != "entry" {
		t.Fatalf("state after restart = %#v", state)
	}
}

func TestBackendRepoThreadRevisionCASCommitsExactlyOneConcurrentMutation(t *testing.T) {
	for _, test := range []struct {
		name   string
		source publicstorage.Source
	}{
		{name: "memory", source: publicstorage.Memory()},
		{name: "sqlite", source: publicstorage.SQLite(filepath.Join(t.TempDir(), "concurrent.db"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend, repo := openRevisionRepo(t, test.source)
			defer backend.Close()
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			expected := currentRevision(t, repo, "thread")
			var invoked atomic.Int32
			start := make(chan struct{})
			errs := make(chan error, 2)
			var wait sync.WaitGroup
			for index := 0; index < 2; index++ {
				index := index
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					errs <- repo.UpdateDomainAtRevision(ctx, "thread", expected, func(memory *MemoryRepo, _ spi.WriteTx) error {
						invoked.Add(1)
						_, err := memory.Append(ctx, Entry{ID: "winner-" + string(rune('a'+index)), ThreadID: "thread", Type: EntryCustom}, AppendOptions{ID: "winner-" + string(rune('a'+index))})
						return err
					})
				}()
			}
			close(start)
			wait.Wait()
			close(errs)
			succeeded, conflicted := 0, 0
			for err := range errs {
				switch {
				case err == nil:
					succeeded++
				case errors.Is(err, ErrThreadRevisionConflict):
					conflicted++
				default:
					t.Fatalf("concurrent CAS error = %v", err)
				}
			}
			if succeeded != 1 || conflicted != 1 || invoked.Load() != 1 {
				t.Fatalf("CAS results succeeded=%d conflicted=%d invoked=%d", succeeded, conflicted, invoked.Load())
			}
			state := stateAt(t, repo, "thread", expected+1)
			if len(state.Entries) != 1 || currentRevision(t, repo, "thread") != expected+1 {
				t.Fatalf("concurrent CAS state = %#v", state)
			}
		})
	}
}

func TestBackendRepoThreadRevisionCASRollsBackMutationAndRevisionOnError(t *testing.T) {
	for _, test := range []struct {
		name   string
		source publicstorage.Source
	}{
		{name: "memory", source: publicstorage.Memory()},
		{name: "sqlite", source: publicstorage.SQLite(filepath.Join(t.TempDir(), "rollback.db"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend, repo := openRevisionRepo(t, test.source)
			defer backend.Close()
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			expected := currentRevision(t, repo, "thread")
			injected := errors.New("injected mutation failure")
			err := repo.UpdateDomainAtRevision(ctx, "thread", expected, func(memory *MemoryRepo, tx spi.WriteTx) error {
				if _, appendErr := memory.Append(ctx, Entry{ID: "rolled-back", ThreadID: "thread", Type: EntryCustom}, AppendOptions{ID: "rolled-back"}); appendErr != nil {
					return appendErr
				}
				if putErr := tx.Put("revision-atomicity", []byte("side-effect"), []byte("must-rollback")); putErr != nil {
					return putErr
				}
				return injected
			})
			if !errors.Is(err, injected) {
				t.Fatalf("CAS error = %v, want injected failure", err)
			}
			if got := currentRevision(t, repo, "thread"); got != expected {
				t.Fatalf("failed CAS advanced revision to %d", got)
			}
			if state := stateAt(t, repo, "thread", expected); len(state.Entries) != 0 {
				t.Fatalf("failed CAS committed entries: %#v", state.Entries)
			}
			if err := backend.View(ctx, func(tx spi.ReadTx) error {
				_, getErr := tx.Get("revision-atomicity", []byte("side-effect"))
				if !errors.Is(getErr, spi.ErrNotFound) {
					t.Fatalf("failed CAS committed side record: %v", getErr)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBackendRepoThreadRevisionCASPublishesParentAndChildAtomically(t *testing.T) {
	for _, test := range []struct {
		name   string
		source publicstorage.Source
	}{
		{name: "memory", source: publicstorage.Memory()},
		{name: "sqlite", source: publicstorage.SQLite(filepath.Join(t.TempDir(), "publication.db"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend, repo := openRevisionRepo(t, test.source)
			defer backend.Close()
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "parent"}); err != nil {
				t.Fatal(err)
			}
			parentRevision := currentRevision(t, repo, "parent")
			err := repo.UpdateDomainAtRevision(ctx, "parent", parentRevision, func(memory *MemoryRepo, tx spi.WriteTx) error {
				if _, createErr := memory.CreateThread(ctx, ThreadMeta{
					ID: "child", ParentThreadID: "parent", ParentTurnID: "turn",
					TaskName: "worker", AgentPath: "/parent/worker",
				}); createErr != nil {
					return createErr
				}
				return tx.Put("revision-atomicity", []byte("publication"), []byte("committed"))
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := currentRevision(t, repo, "parent"); got != parentRevision+1 {
				t.Fatalf("parent revision = %d, want %d", got, parentRevision+1)
			}
			if got := currentRevision(t, repo, "child"); got != 1 {
				t.Fatalf("child revision = %d, want 1", got)
			}
			parent := stateAt(t, repo, "parent", parentRevision+1)
			if len(parent.Children) != 1 || parent.Children[0].ThreadID != "child" || !containsRevisionDomain(parent.ChangedDomains, ThreadRevisionDomainSubAgent) {
				t.Fatalf("parent publication = %#v", parent)
			}
			child := stateAt(t, repo, "child", 1)
			if child.Thread == nil || child.Thread.ID != "child" || !containsRevisionDomain(child.ChangedDomains, ThreadRevisionDomainThread) {
				t.Fatalf("child creation = %#v", child)
			}
			if err := backend.View(ctx, func(tx spi.ReadTx) error {
				value, getErr := tx.Get("revision-atomicity", []byte("publication"))
				if getErr != nil {
					return getErr
				}
				if string(value) != "committed" {
					t.Fatalf("publication side record = %q", value)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func openRevisionRepo(t *testing.T, source publicstorage.Source) (spi.Backend, *BackendRepo) {
	t.Helper()
	backend, err := storagebridge.Open(context.Background(), storagebridge.Source(source))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewBackendRepo(context.Background(), backend, DefaultLeasePolicy, time.Now)
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	return backend, repo
}

func currentRevision(t *testing.T, repo *BackendRepo, threadID string) ThreadRevision {
	t.Helper()
	revision, err := repo.CurrentThreadRevision(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func stateAt(t *testing.T, repo *BackendRepo, threadID string, revision ThreadRevision) ThreadRevisionState {
	t.Helper()
	state, err := repo.ThreadStateAtRevision(context.Background(), threadID, revision)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
