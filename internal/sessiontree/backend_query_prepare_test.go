package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

type queryPreparationFailureBackend struct {
	spi.Backend
	writes int
	failAt int
}

func (backend *queryPreparationFailureBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(queryPreparationFailureTx{WriteTx: tx, backend: backend}) })
}

type queryPreparationFailureTx struct {
	spi.WriteTx
	backend *queryPreparationFailureBackend
}

func (tx queryPreparationFailureTx) Put(namespace string, key, value []byte) error {
	if namespace == queryDocumentNamespace {
		tx.backend.writes++
		if tx.backend.writes == tx.backend.failAt {
			return errors.New("injected index write failure")
		}
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestV32QueryPreparationNeverExposesPartialIndex(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	for i := range 130 {
		if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprintf("entry-%03d", i), ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained searchable fact"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	base, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
	backend := &queryPreparationFailureBackend{Backend: base, failAt: 70}
	repo.backend = backend
	if err := repo.PrepareQueryThread(t.Context(), "history"); err == nil {
		t.Fatal("index write failure was ignored")
	}
	scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{"history"}}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); !errors.Is(err, threadquery.ErrThreadQueryIndexNotReady) {
		t.Fatalf("partial query = %v", err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "new-input", ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new canonical fact"}}, AppendOptions{}); err != nil {
		t.Fatalf("canonical append while indexing = %v", err)
	}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); !errors.Is(err, threadquery.ErrThreadQueryIndexNotReady) {
		t.Fatalf("append published incomplete index = %v", err)
	}
	backend.failAt = 0
	if err := repo.PrepareQueryThread(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); err != nil {
		t.Fatal(err)
	}
}

func TestV32BackgroundPreparationIncludesQueryIndexes(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	for _, id := range []string{"first", "second"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Append(t.Context(), Entry{ID: "entry", ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "searchable"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	_, repo := openV31ReviewFixture(t, memory)
	for iteration := 0; ; iteration++ {
		if iteration > 40 {
			t.Fatal("background preparation did not finish")
		}
		more, err := repo.MigrateNextThread(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
	}
	scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{"first", "second"}}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if ready, err := repo.ThreadPrepared(t.Context(), id); err != nil || !ready {
			t.Fatalf("readiness %s = %v, %v", id, ready, err)
		}
	}
}

func TestV32ReadinessSurvivesValidationCacheEvictionAndRestart(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	ids := make([]string, backendVerifiedThreadCacheLimit+3)
	for i := range ids {
		ids[i] = fmt.Sprintf("thread-%03d", i)
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: ids[i]}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	for _, id := range ids {
		if err := repo.PrepareThread(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		if err := repo.PrepareQueryThread(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	if len(repo.verifiedThreadLeaves) != backendVerifiedThreadCacheLimit {
		t.Fatal("fixture did not evict validation cache entries")
	}
	for _, phase := range []string{"evicted", "reopened"} {
		if phase == "reopened" {
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
		}
		bounded := &boundedPageBackend{Backend: backend}
		repo.backend = bounded
		for _, id := range ids {
			if ready, err := repo.ThreadPrepared(t.Context(), id); err != nil || !ready {
				t.Fatalf("%s readiness %s = %v, %v", phase, id, ready, err)
			}
		}
		if bounded.gets > 4*len(ids) {
			t.Fatalf("%s readiness read %d records", phase, bounded.gets)
		}
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(queryManifestNamespace, []byte(ids[0]))
	}); err != nil {
		t.Fatal(err)
	}
	if ready, err := repo.ThreadPrepared(t.Context(), ids[0]); err != nil || ready {
		t.Fatalf("missing index readiness = %v, %v", ready, err)
	}
}

type queryDeleteCountingBackend struct {
	spi.Backend
	maxDeletes int
	batches    int
	failBatch  int
}
type queryDeleteCountingTx struct {
	spi.WriteTx
	deletes int
}

func (tx *queryDeleteCountingTx) Delete(namespace string, key []byte) error {
	if namespace == queryDocumentNamespace {
		tx.deletes++
	}
	return tx.WriteTx.Delete(namespace, key)
}
func (backend *queryDeleteCountingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		counted := &queryDeleteCountingTx{WriteTx: tx}
		if err := mutate(counted); err != nil {
			return err
		}
		backend.maxDeletes = max(backend.maxDeletes, counted.deletes)
		if counted.deletes > 0 {
			backend.batches++
			if backend.batches == backend.failBatch {
				return errors.New("injected cleanup failure")
			}
		}
		return nil
	})
}

func TestQueryPreparationReclaimsMetadataInBoundedTransactions(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	base, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
	if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
		for i := range 300 {
			// Interrupted derived metadata may be corrupt and must be rebuildable.
			if err := tx.Put(queryDocumentNamespace, []byte(fmt.Sprintf("history\x00stale-%03d", i)), []byte("invalid derived record")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	backend := &queryDeleteCountingBackend{Backend: base, failBatch: 2}
	repo.backend = backend
	if err := repo.PrepareQueryThread(t.Context(), "history"); err == nil {
		t.Fatal("cleanup failure ignored")
	}
	if backend.maxDeletes > 128 || backend.batches != 2 {
		t.Fatalf("cleanup batches=%d largest=%d", backend.batches, backend.maxDeletes)
	}
	scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{"history"}}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); !errors.Is(err, threadquery.ErrThreadQueryIndexNotReady) {
		t.Fatalf("partial cleanup query=%v", err)
	}
	backend.failBatch = 0
	if err := repo.PrepareQueryThread(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
	if backend.maxDeletes > 128 {
		t.Fatalf("cleanup transaction deleted %d documents", backend.maxDeletes)
	}
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 5}); err != nil {
		t.Fatal(err)
	}
}
