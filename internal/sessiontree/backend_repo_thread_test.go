package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func TestBackendRepoThreadScopedRuntimeOperations(t *testing.T) {
	repo := newSizedMutationRepo(t, 3)
	repo.EnableThreadScopedBackend()
	if repo.domainMemory != nil {
		t.Fatal("thread-scoped backend retained the startup-wide domain memory")
	}
	repo.mu.Lock()
	repo.verifiedThreadLeaves = map[string]string{}
	repo.mu.Unlock()

	meta, err := repo.Thread(t.Context(), "active")
	if err != nil {
		t.Fatalf("load and validate one thread: %v", err)
	}
	if meta.LeafID != "active-0394" {
		t.Fatalf("active leaf = %q, want active-0394", meta.LeafID)
	}

	meta.UpdatedAt = meta.UpdatedAt.Add(time.Second)
	if err := repo.UpdateThread(t.Context(), meta); err != nil {
		t.Fatalf("update one thread: %v", err)
	}
	entry, err := repo.Append(t.Context(), Entry{
		ID: "active-scoped-append", ThreadID: "active", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "scoped"},
	}, AppendOptions{})
	if err != nil {
		t.Fatalf("append to one thread: %v", err)
	}
	if entry.ID != "active-scoped-append" {
		t.Fatalf("appended entry = %+v", entry)
	}
	entries, err := repo.Entries(t.Context(), "active")
	if err != nil {
		t.Fatalf("reload one thread journal: %v", err)
	}
	if len(entries) != 396 || entries[len(entries)-1].ID != entry.ID {
		t.Fatalf("thread journal length/leaf = %d/%q", len(entries), entries[len(entries)-1].ID)
	}

	threads, err := repo.ListThreads(t.Context(), ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		t.Fatalf("global thread list fallback: %v", err)
	}
	if len(threads) != 4 {
		t.Fatalf("listed %d threads, want four", len(threads))
	}
	for _, thread := range threads {
		if thread.ID != "active" {
			continue
		}
		if want := fmt.Sprintf("%s-scoped-append", thread.ID); thread.LeafID != want {
			t.Fatalf("active thread leaf = %q, want %q", thread.LeafID, want)
		}
		return
	}
	t.Fatalf("active thread missing from global list: %+v", threads)
}

func TestBackendRepoV32UsesPerRootRecordsAndThreadScopedOwner(t *testing.T) {
	backend := newMigrationTestBackend()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		memory := newMemoryRepo(time.Now)
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "existing-v32-root"}); err != nil {
			return err
		}
		return saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV32Format)
	}); err != nil {
		t.Fatalf("initialize v32 state: %v", err)
	}

	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var current bool
		var err error
		repo, current, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err != nil {
			return err
		}
		if !current {
			return errors.New("v32 store was not recognized as current")
		}
		return nil
	}); err != nil {
		t.Fatalf("recognize v32 store: %v", err)
	}

	created, entry, err := repo.CreateThreadWithInitialEntry(t.Context(), ThreadMeta{ID: "v32-root"}, Entry{
		ID: "v32-entry", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "v32"},
	})
	if err != nil {
		t.Fatalf("create v32 thread: %v", err)
	}
	if created.LeafID != entry.ID {
		t.Fatalf("created v32 thread = %+v, entry = %+v", created, entry)
	}
	threads, err := repo.ListThreads(t.Context(), ListThreadsOptions{})
	if err != nil || len(threads) != 2 || threads[0].ID != created.ID {
		t.Fatalf("list v32 roots = %+v, err=%v", threads, err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		if _, err := tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordRootIndex)); !errors.Is(err, spi.ErrNotFound) {
			return errors.New("v32 wrote a monolithic root index")
		}
		if _, err := tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordRootThread, "existing-v32-root")); err != nil {
			return errors.New("v32 root creation dropped existing root membership")
		}
		if _, err := tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordRootThread, created.ID)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("validate v32 root membership: %v", err)
	}
}

func TestBackendRepoThreadScopedQueryLoadsOnlyMatchedThreads(t *testing.T) {
	repo := newSizedMutationRepo(t, 3)
	if err := repo.backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var current bool
		var err error
		repo, current, err = NewCurrentBackendRepoInTransaction(t.Context(), repo.backend, tx, repo.now, false)
		if err == nil && !current {
			return errors.New("fixture did not install the current canonical owner")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareQueryThread(t.Context(), "active"); err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{"active"}}
	page, err := repo.Query(t.Context(), scope, threadquery.QueryInput{
		Object: threadquery.Messages,
		Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "context"}},
		Limit:  5,
	})
	if err != nil {
		t.Fatalf("query indexed messages: %v", err)
	}
	if len(page.Records) != 5 || page.Total != 395 {
		t.Fatalf("query returned %d of %d records, want 5 of 395", len(page.Records), page.Total)
	}
	if len(repo.verifiedThreadLeaves) != 1 || repo.verifiedThreadLeaves["active"] == "" {
		t.Fatalf("query validated unrelated threads: %v", repo.verifiedThreadLeaves)
	}
	read, err := repo.Read(t.Context(), scope, threadquery.ReadInput{Ref: page.Records[0].Ref})
	if err != nil {
		t.Fatalf("read query record: %v", err)
	}
	if read.Record.ThreadID != "active" || len(read.Chunks) == 0 {
		t.Fatalf("unexpected query read result: %+v", read)
	}
	if err := repo.backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(queryManifestNamespace, []byte("active"))
	}); err != nil {
		t.Fatalf("remove query readiness marker: %v", err)
	}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); !errors.Is(err, threadquery.ErrThreadQueryIndexNotReady) {
		t.Fatalf("query with a missing index manifest = %v, want index-not-ready", err)
	}
	if err := repo.PrepareQueryThread(t.Context(), "active"); err != nil {
		t.Fatalf("rebuild one thread query index: %v", err)
	}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); err != nil {
		t.Fatalf("query after thread index preparation: %v", err)
	}
}

func TestBackendRepoThreadScopedCreationLoadsMetadataOnlyInventory(t *testing.T) {
	repo := newSizedMutationRepo(t, 12)
	repo.EnableThreadScopedBackend()
	created, initial, err := repo.CreateThreadWithInitialEntry(t.Context(), ThreadMeta{ID: "new-fast-thread"}, Entry{
		ID: "new-fast-entry", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "ready immediately"},
	})
	if err != nil {
		t.Fatalf("create thread with initial entry: %v", err)
	}
	if created.ID != "new-fast-thread" || created.LeafID != initial.ID || initial.ThreadID != created.ID {
		t.Fatalf("created thread/entry = %+v / %+v", created, initial)
	}
	if repo.domainMemory != nil {
		t.Fatal("thread-scoped creation retained a global domain snapshot")
	}
	entries, err := repo.Entries(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("read new thread: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != initial.ID {
		t.Fatalf("new thread entries = %+v", entries)
	}
}

func TestBackendRepoV31ThreadPreparationPublishesOnlyRequestedThread(t *testing.T) {
	backend := newMigrationTestBackend()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		memory := newMemoryRepo(time.Now)
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "legacy-thread"}); err != nil {
			return err
		}
		if _, err := memory.Append(t.Context(), Entry{ID: "legacy-entry", ThreadID: "legacy-thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "legacy"}}, AppendOptions{}); err != nil {
			return err
		}
		return saveCompleteBackendDomain(tx, memory, backendDomainV31Format)
	}); err != nil {
		t.Fatalf("initialize v31 state: %v", err)
	}

	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var current bool
		var err error
		repo, current, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err != nil {
			return err
		}
		if !current {
			return errors.New("v31 store was not recognized as current")
		}
		repo.EnableThreadScopedBackend()
		return nil
	}); err != nil {
		t.Fatalf("open v31 store: %v", err)
	}
	if err := repo.PrepareThread(t.Context(), "legacy-thread"); err != nil {
		t.Fatalf("prepare legacy thread: %+v", err)
	}
	entries, err := repo.Entries(t.Context(), "legacy-thread")
	if err != nil || len(entries) != 1 || entries[0].ID != "legacy-entry" {
		t.Fatalf("prepared legacy entries = %+v, err=%v", entries, err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		if _, err := tx.Get(backendDomainV31Namespace, backendDomainEntryKey("legacy-thread", 0)); err != nil {
			return errors.New("legacy source entry was removed")
		}
		if _, err := tx.Get(backendDomainV32Namespace, backendDomainEntryKey("legacy-thread", 0)); err != nil {
			return errors.New("prepared v32 entry is missing")
		}
		return nil
	}); err != nil {
		t.Fatalf("validate per-thread publication: %v", err)
	}
}

func TestBackendRepoV31MigrationResumesByCursorAndCleansSource(t *testing.T) {
	backend := newMigrationTestBackend()
	const threadCount = backendThreadEntryPageSize*2 + 7
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		memory := newMemoryRepo(time.Now)
		for index := 0; index < threadCount; index++ {
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: fmt.Sprintf("legacy-%03d", index)}); err != nil {
				return err
			}
		}
		return saveCompleteBackendDomain(tx, memory, backendDomainV31Format)
	}); err != nil {
		t.Fatalf("initialize v31 migration fixture: %v", err)
	}
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var current bool
		var err error
		repo, current, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err != nil {
			return err
		}
		if !current {
			return errors.New("v31 store was not recognized as current")
		}
		repo.EnableThreadScopedBackend()
		return nil
	}); err != nil {
		t.Fatalf("open v31 migration fixture: %v", err)
	}
	iterations := 0
	for repo.HasPendingMigration() {
		progress, err := repo.MigrateNextThread(t.Context())
		if err != nil {
			t.Fatalf("migration iteration %d: %v", iterations, err)
		}
		if !progress {
			if repo.HasPendingMigration() {
				t.Fatalf("migration stopped before source cleanup at iteration %d", iterations)
			}
			break
		}
		iterations++
		if iterations > threadCount*3 {
			t.Fatal("migration did not converge")
		}
	}
	if iterations <= threadCount {
		t.Fatalf("migration iterations=%d, want bounded publish plus cleanup checkpoints", iterations)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		if _, err := tx.Get(backendDomainV31Namespace, backendDomainKey(backendDomainRecordManifest)); !errors.Is(err, spi.ErrNotFound) {
			return fmt.Errorf("legacy manifest remains after cleanup: %v", err)
		}
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
		if err != nil || !found {
			if err == nil {
				err = errors.New("v32 manifest missing after cleanup")
			}
			return err
		}
		var manifest backendDomainManifest
		if err := decodeBackendDomainValue(backendDomainV32Format, record, &manifest); err != nil {
			return err
		}
		if manifest.SourceVersion != 0 || manifest.MigrationCursor != nil || manifest.MigrationPhase != "" {
			return fmt.Errorf("migration manifest not finalized: %+v", manifest)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepoThreadScopedForkSurvivesSourceDeletion(t *testing.T) {
	repo := newSizedMutationRepo(t, 0)
	forked, err := repo.Fork(t.Context(), ForkOptions{
		SourceThreadID: "active", NewThreadID: "active-fork", Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("fork active thread: %v", err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "active"); err != nil {
		t.Fatalf("delete fork source: %v", err)
	}
	repo.EnableThreadScopedBackend()

	loaded, err := repo.Thread(t.Context(), forked.ID)
	if err != nil {
		t.Fatalf("load independent fork after source deletion: %v", err)
	}
	if loaded.ForkedFromThreadID != "active" || loaded.LeafID == "" {
		t.Fatalf("fork authority = %+v; want retained source identity and copied leaf", loaded)
	}
	entries, err := repo.Entries(t.Context(), forked.ID)
	if err != nil {
		t.Fatalf("read copied fork journal: %v", err)
	}
	if len(entries) == 0 || entries[len(entries)-1].ID != loaded.LeafID {
		t.Fatalf("fork entries = %d, leaf=%q", len(entries), loaded.LeafID)
	}
}

func TestCanonicalMediaInventoryForThreadDoesNotLoadUnrelatedJournals(t *testing.T) {
	repo := newSizedMutationRepo(t, 1)
	repo.EnableThreadScopedBackend()
	if err := repo.backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(backendDomainV31Format.namespace, backendDomainEntryKey("history-001", 0), []byte("corrupt unrelated journal"))
	}); err != nil {
		t.Fatalf("corrupt unrelated journal fixture: %v", err)
	}

	entries, err := repo.CanonicalMediaEntriesForThread(t.Context(), "active")
	if err != nil {
		t.Fatalf("read target thread media inventory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("target media inventory leaked %d entries, want none", len(entries))
	}
}

func TestDeferredBackendFacadePreservesThreadScopedReads(t *testing.T) {
	owner := newSizedMutationRepo(t, 1)
	owner.EnableThreadScopedBackend()
	facade := NewDeferredBackendRepo(owner.backend, owner.now, nil)
	if err := facade.InstallDelegate(owner); err != nil {
		t.Fatalf("install current thread-scoped owner: %v", err)
	}
	if err := owner.backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(backendDomainV31Format.namespace, backendDomainEntryKey("history-001", 0), []byte("corrupt unrelated journal"))
	}); err != nil {
		t.Fatalf("corrupt unrelated journal fixture: %v", err)
	}

	meta, err := facade.Thread(t.Context(), "active")
	if err != nil {
		t.Fatalf("thread read through deferred facade: %v", err)
	}
	if meta.ID != "active" || meta.LeafID == "" {
		t.Fatalf("thread metadata = %+v", meta)
	}
	if _, err := facade.CanonicalMediaEntriesForThread(t.Context(), "active"); err != nil {
		t.Fatalf("thread media read through deferred facade: %v", err)
	}
}

func TestThreadScopedHotThreadDoesNotRescanItsJournal(t *testing.T) {
	repo := newSizedMutationRepo(t, 0)
	repo.EnableThreadScopedBackend()
	repo.parentEvents = true
	counter := &threadScanCountingBackend{Backend: repo.backend}
	repo.backend = counter
	if _, err := repo.Thread(t.Context(), "active"); err != nil {
		t.Fatalf("cold thread validation: %v", err)
	}
	if counter.count() == 0 {
		t.Fatal("cold thread read did not scan the journal")
	}
	counter.reset()
	if _, err := repo.Append(t.Context(), Entry{
		ID: "active-hot-append", ThreadID: "active", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "hot append"},
	}, AppendOptions{}); err != nil {
		t.Fatalf("append to validated hot thread: %v", err)
	}
	if counter.count() != 0 {
		t.Fatalf("hot thread append rescanned %d backend pages, want zero", counter.count())
	}
}

type threadScanCountingBackend struct {
	spi.Backend
	mu    sync.Mutex
	scans int
}

func (backend *threadScanCountingBackend) reset() {
	backend.mu.Lock()
	backend.scans = 0
	backend.mu.Unlock()
}

func (backend *threadScanCountingBackend) scan() {
	backend.mu.Lock()
	backend.scans++
	backend.mu.Unlock()
}

func (backend *threadScanCountingBackend) count() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.scans
}

func (backend *threadScanCountingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error {
		return read(threadScanCountingReadTx{ReadTx: tx, backend: backend})
	})
}

func (backend *threadScanCountingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(threadScanCountingWriteTx{WriteTx: tx, backend: backend})
	})
}

type threadScanCountingReadTx struct {
	spi.ReadTx
	backend *threadScanCountingBackend
}

func (tx threadScanCountingReadTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.backend.scan()
	return tx.ReadTx.Scan(request)
}

type threadScanCountingWriteTx struct {
	spi.WriteTx
	backend *threadScanCountingBackend
}

func (tx threadScanCountingWriteTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.backend.scan()
	return tx.WriteTx.Scan(request)
}

func TestBackendRepoScopedRootCreationPreservesInventoryOrder(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	repo.EnableThreadScopedBackend()
	for i, id := range []string{"first", "second", "third"} {
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: id, CreatedAt: time.Unix(int64(i+1), 0)}); err != nil {
			t.Fatal(err)
		}
	}
	roots, err := repo.ListThreads(t.Context(), ListThreadsOptions{RootOnly: true})
	if err != nil || len(roots) != 3 || roots[0].ID != "third" || roots[2].ID != "first" {
		t.Fatalf("root inventory=%+v, err=%v", roots, err)
	}
	if _, err := repo.PendingAutomaticThreadTitles(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func openV31ReviewFixture(t *testing.T, memory *MemoryRepo) (*migrationTestBackend, *BackendRepo) {
	t.Helper()
	backend := newMigrationTestBackend()
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := saveCompleteBackendDomain(tx, memory, backendDomainV31Format); err != nil {
			return err
		}
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return backend, repo
}

func TestV32MigrationPreservesPublishedDependencyAndSequence(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "child", ParentThreadID: "root", TaskName: "child", HostProfileRef: "test"}); err != nil {
		t.Fatal(err)
	}
	_, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "root"); err != nil {
		t.Fatal(err)
	}
	entry, err := repo.Append(t.Context(), Entry{ThreadID: "root", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new fact after publication"}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareThread(t.Context(), "child"); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Entry(t.Context(), "root", entry.ID)
	if err != nil || got.Message.Content != entry.Message.Content {
		t.Fatalf("published fact overwritten: %+v, %v", got, err)
	}
	next, err := repo.Append(t.Context(), Entry{ThreadID: "root", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "later fact"}}, AppendOptions{})
	if err != nil || next.ID == entry.ID {
		t.Fatalf("sequence regressed: %+v, %v", next, err)
	}
}

func TestV32MigrationCleanupRetainsExactBodyReferenceCounts(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	for _, id := range []string{"a", "b"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "shared body"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	for batch := 0; repo.HasPendingMigration(); batch++ {
		if batch >= 64 {
			t.Fatal("migration did not finish after restarting each batch")
		}
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
		// Reopen between every committed batch, including source cleanup.
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		got, found, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV32Format)
		if err != nil {
			return err
		}
		if !found || len(got.threads) != 2 {
			return errors.New("migration lost threads")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeferredRepoForwardsMigrationToInstalledOwner(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	backend, owner := openV31ReviewFixture(t, memory)
	facade := NewDeferredBackendRepo(backend, time.Now, func(context.Context) error { return nil })
	if err := facade.InstallDelegate(owner); err != nil {
		t.Fatal(err)
	}
	if !facade.HasPendingMigration() {
		t.Fatal("facade hid pending migration")
	}
	if changed, err := facade.MigrateNextThread(t.Context()); err != nil || !changed {
		t.Fatalf("migration=%v,%v", changed, err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, err := tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordThread, "root"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestV32MigrationPreservesDeletedIdentity(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "deleted-root", OriginRequestKey: "create-deleted-root", OriginFingerprint: "original"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.DeleteRootTreeWithRequest(t.Context(), "deleted-root", "delete-root", "delete-fingerprint"); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "deleted-root"}); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("recreate unpublished deleted identity = %v", err)
	}
	for repo.HasPendingMigration() {
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		current, found, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV32Format)
		if err != nil || !found {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if current.tombstones["deleted-root"] != memory.tombstones["deleted-root"] {
			return errors.New("migration lost deletion provenance")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := repo.DeleteRootTreeWithRequest(t.Context(), "deleted-root", "delete-root", "delete-fingerprint")
	if err != nil || !result.Replayed {
		t.Fatalf("delete replay = %+v, %v", result, err)
	}
}

func TestV32MigrationRejectsOrphansBeforeCleanup(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return putBackendDomainRecord(tx, backendDomainV31Format, backendDomainKey("unexpected", "orphan"), "unexpected", "orphan", "", 0, map[string]string{"fact": "must survive"})
	}); err != nil {
		t.Fatal(err)
	}
	var migrationErr error
	for attempt := 0; attempt < 20 && repo.HasPendingMigration(); attempt++ {
		if _, migrationErr = repo.MigrateNextThread(t.Context()); migrationErr != nil {
			break
		}
	}
	if !errors.Is(migrationErr, ErrAuthorityCorrupt) {
		t.Fatalf("orphan source migration = %v", migrationErr)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, err := tx.Get(backendDomainV31Namespace, backendDomainKey("unexpected", "orphan"))
		return err
	}); err != nil {
		t.Fatalf("failed migration removed source record: %v", err)
	}
}

func TestV32MigrationRecognizesMalformedManifest(t *testing.T) {
	for _, manifest := range []backendDomainManifest{
		{Version: backendDomainV32Version, SourceVersion: 99},
		{Version: backendDomainV32Version, MigrationPhase: "cleanup"},
		{Version: backendDomainV32Version, SourceVersion: backendDomainV31Version, MigrationPhase: "unknown"},
		{Version: backendDomainV32Version, SourceVersion: backendDomainV31Version, MigrationCursor: []byte("cursor-without-phase")},
		{Version: backendDomainV32Version, Sequence: -1},
	} {
		t.Run(fmt.Sprintf("%+v", manifest), func(t *testing.T) {
			backend := newMigrationTestBackend()
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return putBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
			}); err != nil {
				t.Fatal(err)
			}
			err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				_, _, err := NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			})
			if !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("invalid manifest accepted: %v", err)
			}
		})
	}
}

func TestV32MigrationDeletionIncludesUnpublishedChildren(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "child", ParentThreadID: "root", TaskName: "child", HostProfileRef: "test"}); err != nil {
		t.Fatal(err)
	}
	_, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "root"); err != nil {
		t.Fatal(err)
	}
	deleted, err := repo.DeleteRootTreeWithRequest(t.Context(), "root", "delete-tree", "fingerprint")
	if err != nil || len(deleted.ThreadIDs) != 2 {
		t.Fatalf("delete partially published tree = %+v, %v", deleted, err)
	}
	for repo.HasPendingMigration() {
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"root", "child"} {
		if _, err := repo.Thread(t.Context(), id); !errors.Is(err, ErrThreadDeleted) {
			t.Fatalf("deleted thread %q resurrected: %v", id, err)
		}
		tombstone, err := repo.ThreadTombstone(t.Context(), id)
		if err != nil || tombstone.RootThreadID != "root" || tombstone.DeleteRequestKey != "delete-tree" {
			t.Fatalf("deletion provenance %q = %+v, %v", id, tombstone, err)
		}
	}
	listed, err := repo.ListThreads(t.Context(), ListThreadsOptions{IncludeArchived: true})
	if err != nil || len(listed) != 0 {
		t.Fatalf("deleted threads returned in inventory: %+v, %v", listed, err)
	}
}

func TestV32MigrationStagesBoundedPagesBeforeAtomicPublication(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "large-thread"}); err != nil {
		t.Fatal(err)
	}
	const count = backendThreadEntryPageSize*2 + 9
	for i := 0; i < count; i++ {
		if _, err := memory.Append(t.Context(), Entry{
			ThreadID: "large-thread", Type: EntryUserMessage,
			Message:  session.Message{Role: session.User, Content: "retained historical fact"},
			Metadata: map[string]string{"batch-size": strings.Repeat("x", 4000)},
		}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	lastOrdinal := 0
	for batch := 0; ; batch++ {
		if batch > count {
			t.Fatal("staging did not converge")
		}
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
		published := false
		if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
			_, published, _ = readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, "large-thread"), backendDomainRecordThread, "large-thread", "", 0)
			checkpoint, found, err := readThreadMigrationCheckpoint(tx, "large-thread")
			if err != nil {
				return err
			}
			if !published {
				if !found || checkpoint.NextOrdinal <= lastOrdinal || checkpoint.NextOrdinal-lastOrdinal > backendThreadEntryPageSize {
					return fmt.Errorf("invalid staging progress: %+v, previous %d", checkpoint, lastOrdinal)
				}
				if batch == 0 && checkpoint.NextOrdinal >= backendThreadEntryPageSize {
					return errors.New("byte budget did not bound the first page")
				}
				lastOrdinal = checkpoint.NextOrdinal
			}
			current, _, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV32Format)
			if err != nil {
				return err
			}
			if !published && (len(current.threads) != 0 || len(current.entries) != 0) {
				return errors.New("unpublished journal became canonical")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if published {
			break
		}
		// Restart on each durable cursor, not only after complete threads.
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := repo.Entries(t.Context(), "large-thread")
	if err != nil || len(entries) != count {
		t.Fatalf("published entries: %d, %v", len(entries), err)
	}
}

type failMigrationCheckpointTx struct{ spi.WriteTx }

func (tx failMigrationCheckpointTx) Put(namespace string, key, value []byte) error {
	if namespace == backendThreadMigrationNamespace {
		return errors.New("injected checkpoint failure")
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestV32MigrationCheckpointFailureRollsBackBodyOwnership(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "body"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	before := cloneMigrationRecords(backend.records)
	err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		_, err := repo.stageSourceThread(t.Context(), failMigrationCheckpointTx{tx}, "thread")
		return err
	})
	if err == nil || !equalMigrationRecords(before, backend.records) {
		t.Fatalf("checkpoint failure changed canonical bytes: %v", err)
	}
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
}
