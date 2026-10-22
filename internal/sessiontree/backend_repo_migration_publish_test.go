package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type migrationInterleaveBackend struct {
	spi.Backend
	afterRead       func() error
	maxScanRecords  int
	maxPublishReads int
}

func (backend *migrationInterleaveBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	err := backend.Backend.View(ctx, read)
	if err == nil && backend.afterRead != nil {
		hook := backend.afterRead
		backend.afterRead = nil
		err = hook()
	}
	return err
}

func (backend *migrationInterleaveBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		counted := &migrationPublicationTx{WriteTx: tx}
		if err := mutate(counted); err != nil {
			return err
		}
		backend.maxScanRecords = max(backend.maxScanRecords, counted.scanRecords)
		if counted.published {
			backend.maxPublishReads = max(backend.maxPublishReads, counted.reads)
		}
		return nil
	})
}

type migrationPublicationTx struct {
	spi.WriteTx
	reads, scanRecords int
	published          bool
}

func (tx *migrationPublicationTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.reads++
	return tx.WriteTx.Get(namespace, key)
}
func (tx *migrationPublicationTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	page, err := tx.WriteTx.Scan(request)
	tx.scanRecords += len(page.Records)
	return page, err
}
func (tx *migrationPublicationTx) Put(namespace string, key, value []byte) error {
	if namespace == backendDomainV32Namespace {
		if record, err := decodeBackendDomainRecord(backendDomainV32Format, value); err == nil && record.Kind == backendDomainRecordThread {
			tx.published = true
		}
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestV32MigrationValidationYieldsBeforeAtomicPublication(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	for i := range 1024 {
		if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprintf("e-%04d", i), ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "preserved fact"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	base, repo := openV31ReviewFixture(t, memory)
	backend := &migrationInterleaveBackend{Backend: base}
	repo.backend = backend
	// Stage in normal batches, then exercise the complete source/candidate
	// validation with a foreground write between its independent read views.
	for {
		staged := false
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			staged, err = repo.stageSourceThread(t.Context(), tx, "history")
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if staged {
			break
		}
	}
	interleaved := false
	backend.afterRead = func() error {
		if !repo.mu.TryLock() {
			return errors.New("migration validation held the repository lock")
		}
		repo.mu.Unlock()
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "foreground"}); err != nil {
			return err
		}
		interleaved = true
		return nil
	}
	ctx, release, err := repo.migrationGate.acquire(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	err = repo.publishSourceThread(ctx, "history")
	release()
	if err != nil {
		t.Fatal(err)
	}
	if !interleaved {
		t.Fatal("migration never yielded to foreground work")
	}
	if backend.maxScanRecords > backendThreadEntryPageSize || backend.maxPublishReads > 16 {
		t.Fatalf("unbounded write transaction: scanned=%d publication_reads=%d", backend.maxScanRecords, backend.maxPublishReads)
	}
	if _, err := repo.Thread(t.Context(), "foreground"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Thread(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
}

func TestV32MigrationIsolatesCorruptThreadAndResumesExplicitRetry(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	for _, id := range []string{"a-corrupt", "z-healthy"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Append(t.Context(), Entry{ID: "entry", ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: id}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	key := backendDomainEntryKey("a-corrupt", 0)
	original := append([]byte(nil), backend.records[backendDomainV31Namespace][string(key)]...)
	backend.records[backendDomainV31Namespace][string(key)] = []byte("corrupt source")
	var failure error
	for range 20 {
		_, failure = repo.MigrateNextThread(t.Context())
		if failure != nil {
			break
		}
	}
	if !errors.Is(failure, ErrAuthorityCorrupt) {
		t.Fatalf("migration failure = %v", failure)
	}
	if _, err := repo.Entry(t.Context(), "z-healthy", "entry"); err != nil {
		t.Fatalf("healthy thread blocked: %v", err)
	}
	if _, err := repo.ThreadPrepared(t.Context(), "a-corrupt"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt readiness = %v", err)
	}
	if string(backend.records[backendDomainV31Namespace][string(key)]) != "corrupt source" {
		t.Fatal("failed source was changed")
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ThreadPrepared(t.Context(), "a-corrupt"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("restart lost isolated failure: %v", err)
	}
	// Simulate an explicit external recovery, never an automatic repair.
	backend.records[backendDomainV31Namespace][string(key)] = original
	if err := repo.PrepareThread(t.Context(), "a-corrupt"); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if !repo.HasPendingMigration() {
			break
		}
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if repo.HasPendingMigration() {
		t.Fatal("successful retry did not unblock cleanup")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error { return migrationFailureRemaining(tx) }); err != nil {
		t.Fatal(err)
	}
}
