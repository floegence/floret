package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestCanonicalQueryContinuationCacheHasBoundedResidency(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	_, repo := openV31ReviewFixture(t, memory)
	for i := range 96 {
		id := fmt.Sprintf("thread-%03d", i)
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if len(repo.queryThreads) > 32 {
		t.Fatalf("retained %d thread continuations", len(repo.queryThreads))
	}
}

func TestCanonicalColdAppendDoesNotReplayHistoryInWriteTransaction(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained history"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	repo.queryThreads = nil
	bounded := &appendBoundedBackend{Backend: backend}
	repo.backend = bounded
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new input"}}, AppendOptions{ID: "new"}); err != nil {
		t.Fatal(err)
	}
	if bounded.reads > 100 {
		t.Fatalf("cold append read %d records in its write transaction", bounded.reads)
	}
}

func TestCanonicalQueryContinuationDoesNotRetainOversizedLifecycle(t *testing.T) {
	_, repo := openV31ReviewFixture(t, newMemoryRepo(time.Now))
	state := newQueryThread()
	state.lifecycle.started = map[string]bool{strings.Repeat("x", queryContinuationMaxBytes): true}
	repo.installQueryThreads(map[string]*queryThread{"history": state})
	if len(repo.queryThreads) != 0 {
		t.Fatal("oversized lifecycle remained resident")
	}
}

// Trigger only after a cold reconstruction releases an immutable entry read.
type coldQueryReadBackend struct {
	spi.Backend
	onRead           func()
	historySnapshots int
}

type coldQueryReadTx struct {
	spi.ReadTx
	history bool
}

func (tx *coldQueryReadTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == backendDomainV32Namespace && bytes.HasPrefix(key, backendDomainKey(backendDomainRecordEntry)) {
		tx.history = true
	}
	return tx.ReadTx.Get(namespace, key)
}

func (tx *coldQueryReadTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace == backendDomainV32Namespace && bytes.HasPrefix(request.Start, backendDomainKey(backendDomainRecordEntry)) {
		tx.history = true
	}
	return tx.ReadTx.Scan(request)
}

func (backend *coldQueryReadBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	history := false
	err := backend.Backend.View(ctx, func(tx spi.ReadTx) error {
		observed := &coldQueryReadTx{ReadTx: tx}
		err := read(observed)
		history = observed.history
		return err
	})
	if history {
		backend.historySnapshots++
	}
	if history && backend.onRead != nil {
		onRead := backend.onRead
		backend.onRead = nil
		onRead()
	}
	return err
}

func TestCanonicalColdAppendRechecksConcurrentMutationAndCancellation(t *testing.T) {
	for _, mode := range []string{"same-thread", "other-thread", "cancel", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			for _, id := range []string{"history", "other"} {
				if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
					t.Fatal(err)
				}
				if _, err := memory.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained"}}, AppendOptions{ID: "old"}); err != nil {
					t.Fatal(err)
				}
			}
			base, repo := openV31ReviewFixture(t, memory)
			for _, id := range []string{"history", "other"} {
				if err := repo.PrepareQueryThread(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			}
			repo.queryThreads = nil
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("rollback cold append")
			var backend spi.Backend = base
			if mode == "rollback" {
				backend = &domainCommitBoundaryBackend{Backend: base, afterCallback: func() error { return injected }}
			}
			observed := &coldQueryReadBackend{Backend: backend}
			called := false
			observed.onRead = func() {
				called = true
				if mode == "cancel" {
					cancel()
					return
				}
				if mode == "rollback" {
					return
				}
				id := "history"
				if mode == "other-thread" {
					id = "other"
				}
				if _, err := repo.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "concurrent"}}, AppendOptions{ID: "concurrent"}); err != nil {
					t.Fatal(err)
				}
			}
			repo.backend = observed
			_, err := repo.Append(ctx, Entry{ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new"}}, AppendOptions{ID: "new"})
			if !called {
				t.Fatal("cold history was not read outside the mutation")
			}
			if mode == "cancel" || mode == "rollback" {
				want := injected
				if mode == "cancel" {
					want = context.Canceled
				}
				if !errors.Is(err, want) {
					t.Fatalf("append=%v want %v", err, want)
				}
				if _, err := repo.Entry(t.Context(), "history", "new"); !errors.Is(err, ErrEntryNotFound) {
					t.Fatalf("failed append became visible: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			entries, err := repo.Path(t.Context(), "history", "")
			if err != nil {
				t.Fatal(err)
			}
			want := 2
			if mode == "same-thread" {
				want = 3
			}
			if len(entries) != want || entries[len(entries)-1].ID != "new" {
				t.Fatalf("concurrent history lost: %+v", entries)
			}
		})
	}
}

func TestCanonicalChronologicalProjectionReadsEachWindowOnce(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	base, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := base.View(t.Context(), func(tx spi.ReadTx) error {
		reader := &countQueryProjectionTx{ReadTx: tx}
		meta, err := readPublishedThreadMeta(reader, backendDomainV32Format, "thread")
		if err != nil {
			return err
		}
		if err := visitCanonicalPathMetadata(t.Context(), reader, "thread", meta.LeafID, func(entry Entry) error {
			if entry.ID != fmt.Sprint(count) {
				t.Fatalf("chronological entry %d = %s", count, entry.ID)
			}
			count++
			return nil
		}); err != nil {
			return err
		}
		if reader.reads > 4*count {
			t.Fatalf("projection read %d records for %d entries", reader.reads, count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 512 {
		t.Fatalf("visited %d entries", count)
	}
}

type countQueryProjectionTx struct {
	spi.ReadTx
	reads int
}

func (tx *countQueryProjectionTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.reads++
	return tx.ReadTx.Get(namespace, key)
}

func TestCanonicalColdContinuationUsesBoundedReadSnapshots(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		if _, err := memory.Append(t.Context(), Entry{ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	base, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
	repo.queryThreads = nil
	observed := &coldQueryReadBackend{Backend: base}
	repo.backend = observed
	if _, err := repo.loadQueryContinuation(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
	if observed.historySnapshots != 4 {
		t.Fatalf("512-entry reconstruction used %d snapshots, want four bounded pages", observed.historySnapshots)
	}
}
