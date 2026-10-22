package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOpenRollsBackDomainWhenLogicalSchemaWriteFails(t *testing.T) {
	ctx := context.Background()
	underlying, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := json.Marshal(logicalSchemaEnvelope{Version: previousLogicalSchemaVersion, Fingerprint: previousLogicalSchemaFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if err := underlying.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), previous)
	}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("logical schema write failed")
	source := publicstorage.NewSource(startupBackendSource{backend: startupFailingBackend{Backend: underlying, err: injected}})
	host, err := Open(ctx, Options{Storage: source})
	if err != nil {
		t.Fatalf("Open error=%v, want Host availability before background failure", err)
	}
	if err := host.startup.wait(ctx); !errors.Is(err, injected) {
		t.Fatalf("background migration error=%v, want injected failure", err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := underlying.View(ctx, func(tx spi.ReadTx) error {
		if got, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil || !bytes.Equal(got, previous) {
			t.Fatalf("logical schema changed after rollback: value=%s err=%v", got, err)
		}
		for _, record := range []struct {
			namespace string
			key       []byte
		}{
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))},
		} {
			if _, err := tx.Get(record.namespace, record.key); !errors.Is(err, spi.ErrNotFound) {
				t.Fatalf("partial startup record %x survived rollback: %v", record.key, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRollsBackDomainWhenStartupPanics(t *testing.T) {
	ctx := context.Background()
	underlying, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer underlying.Close()
	previous, err := json.Marshal(logicalSchemaEnvelope{Version: previousLogicalSchemaVersion, Fingerprint: previousLogicalSchemaFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if err := underlying.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), previous)
	}); err != nil {
		t.Fatal(err)
	}
	source := publicstorage.NewSource(startupBackendSource{backend: startupPanickingBackend{Backend: underlying}})
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Open did not propagate startup panic")
			}
		}()
		_, _ = Open(ctx, Options{Storage: source})
	}()
	if err := underlying.View(ctx, func(tx spi.ReadTx) error {
		if got, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil || !bytes.Equal(got, previous) {
			t.Fatalf("logical schema changed after panic rollback: value=%s err=%v", got, err)
		}
		for _, record := range []struct {
			namespace string
			key       []byte
		}{
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))},
		} {
			if _, err := tx.Get(record.namespace, record.key); !errors.Is(err, spi.ErrNotFound) {
				t.Fatalf("partial startup record survived panic rollback: %v", err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDefersInventoryAuthorityCorruptionToInventoryRead(t *testing.T) {
	path := t.TempDir() + "/floret.sqlite"
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	threadID := "corrupt-inventory-thread"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: threadID, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete("floret.domain.sessiontree.v17", storagecodec.Tuple(storagecodec.TupleString("root_thread"), storagecodec.TupleString(threadID)))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	host, err = Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatalf("Host startup should not scan unrelated canonical records: %v", err)
	}
	if err := host.PrepareThread(t.Context(), identity.ThreadID(threadID)); err != nil {
		t.Fatalf("prepare independent thread: %v", err)
	}
	readiness, err := host.Readiness(t.Context(), identity.ThreadID(threadID))
	if err != nil || !readiness.Ready {
		t.Fatalf("unrelated thread readiness = %#v, %v", readiness, err)
	}
	threads, err := host.store.repo.(sessiontree.ThreadListRepo).ListThreads(t.Context(), sessiontree.ListThreadsOptions{})
	if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) || len(threads) != 0 {
		t.Fatalf("corrupt current inventory = %+v, %v; want authority corruption", threads, err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestReadinessDoesNotPrepareTheRequestedThread(t *testing.T) {
	path := t.TempDir() + "/floret.sqlite"
	threadID := "thread-readiness"
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: threadID, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{
		ThreadID: threadID, Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "ready"},
	}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	readiness, err := host.Readiness(t.Context(), identity.ThreadID(threadID))
	if err != nil || !readiness.Ready {
		t.Fatalf("valid thread readiness = %#v, %v", readiness, err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		key := storagecodec.Tuple(storagecodec.TupleString("entry"), storagecodec.TupleString(threadID), storagecodec.TupleOrdinal(0))
		return tx.Put("floret.domain.sessiontree.v17", key, []byte("corrupt thread entry"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	host, err = Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	readiness, err = host.Readiness(t.Context(), identity.ThreadID(threadID))
	if !errors.Is(err, ErrAuthorityCorrupt) || readiness.Ready {
		t.Fatalf("corrupt canonical readiness = %#v, %v; want authority corruption", readiness, err)
	}
	host.threadPreparations.mu.Lock()
	preparation := host.threadPreparations.threads[identity.ThreadID(threadID)]
	host.threadPreparations.mu.Unlock()
	if preparation != nil {
		t.Fatal("readiness started thread preparation")
	}
	if err := host.PrepareThread(t.Context(), identity.ThreadID(threadID)); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("prepare corrupt thread = %v; want authority corruption", err)
	}
	readiness, err = host.Readiness(t.Context(), identity.ThreadID(threadID))
	if !errors.Is(err, ErrAuthorityCorrupt) || readiness.Ready {
		t.Fatalf("failed preparation readiness = %#v, %v", readiness, err)
	}
}

func TestReadinessRechecksCanonicalThreadAfterDeletion(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(t.TempDir() + "/floret.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	const threadID = "thread-prepared-then-deleted"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: threadID}); err != nil {
		t.Fatal(err)
	}
	if err := host.PrepareThread(t.Context(), threadID); err != nil {
		t.Fatal(err)
	}
	deleter := host.store.repo.(interface {
		DeleteRootTree(context.Context, string) (sessiontree.DeleteRootTreeResult, error)
	})
	if _, err := deleter.DeleteRootTree(t.Context(), threadID); err != nil {
		t.Fatal(err)
	}
	status, err := host.Readiness(t.Context(), threadID)
	if err != nil || status.Ready {
		t.Fatalf("deleted thread readiness = %+v, %v", status, err)
	}
}

func TestShutdownCancelsAndJoinsStorageStartup(t *testing.T) {
	path := storageUpgradeFixture(t)
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	blocking := &startupBlockingInitializationBackend{
		Backend: backend,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer close(blocking.release)
	host, err := Open(t.Context(), Options{Storage: publicstorage.NewSource(startupBackendSource{backend: blocking})})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	select {
	case <-blocking.entered:
	case <-time.After(time.Second):
		t.Fatal("startup worker did not enter its blocked backend operation")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := host.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown did not cancel and join startup: %v", err)
	}
	if status, err := host.StorageMigrationStatus(context.Background()); !errors.Is(err, ErrHostClosed) || status.State != "" {
		t.Fatalf("status after shutdown = %#v, %v; want closed Host", status, err)
	}
}

type startupBackendSource struct{ backend spi.Backend }

func (source startupBackendSource) Open(context.Context) (spi.Backend, error) {
	return source.backend, nil
}

type startupFailingBackend struct {
	spi.Backend
	err error
}

type startupPanickingBackend struct{ spi.Backend }

type startupBlockingInitializationBackend struct {
	spi.Backend
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (backend *startupBlockingInitializationBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	if backend.calls.Add(1) == 2 {
		close(backend.entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-backend.release:
			return errors.New("test startup release")
		}
	}
	return backend.Backend.Update(ctx, mutate)
}

func (backend startupPanickingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := mutate(tx); err != nil {
			return err
		}
		panic("injected startup panic")
	})
}

func (startupPanickingBackend) Close() error { return nil }

func (backend startupFailingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(startupFailingWriteTx{WriteTx: tx, err: backend.err})
	})
}

func (startupFailingBackend) Close() error { return nil }

type startupFailingWriteTx struct {
	spi.WriteTx
	err error
}

func (tx startupFailingWriteTx) Put(namespace string, key, value []byte) error {
	if namespace == logicalSchemaNamespace && bytes.Equal(key, []byte(logicalSchemaKey)) {
		return tx.err
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestLogicalV9UpgradeRetiresTermsInSameTransaction(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("write_failure=", fail), func(t *testing.T) {
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			old, _ := json.Marshal(logicalSchemaEnvelope{Version: legacyV9LogicalSchemaVersion, Fingerprint: legacyV9LogicalSchemaFingerprint})
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if err := tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), old); err != nil {
					return err
				}
				return tx.Put("floret.query.documents.v1", []byte("old-document"), []byte("old-terms"))
			}); err != nil {
				t.Fatal(err)
			}
			var source spi.Backend = backend
			injected := errors.New("logical version commit failed")
			if fail {
				source = startupFailingBackend{Backend: backend, err: injected}
			}
			host, err := Open(t.Context(), Options{Storage: publicstorage.NewSource(startupBackendSource{backend: source}), DeferExecution: true})
			if err != nil {
				t.Fatal(err)
			}
			defer host.Shutdown(t.Context())
			err = host.startup.wait(t.Context())
			if fail && !errors.Is(err, injected) || !fail && err != nil {
				t.Fatalf("startup=%v", err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				state, err := inspectLogicalSchemaTransaction(tx)
				if err != nil {
					return err
				}
				_, termsErr := tx.Get("floret.query.documents.v1", []byte("old-document"))
				if fail {
					if state != logicalSchemaV9 || termsErr != nil {
						t.Fatalf("partial upgrade: %s %v", state, termsErr)
					}
				} else if state != logicalSchemaCurrent || !errors.Is(termsErr, spi.ErrNotFound) {
					t.Fatalf("incomplete upgrade: %s %v", state, termsErr)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLogicalV9RejectsFingerprintDrift(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	raw, _ := json.Marshal(logicalSchemaEnvelope{Version: legacyV9LogicalSchemaVersion, Fingerprint: "unknown"})
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), raw) }); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectLogicalSchema(t.Context(), backend); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("drift accepted: %v", err)
	}
}

func TestPrepareThreadCancellationLeavesHostPreparationRunning(t *testing.T) {
	path := storageUpgradeFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path), StartupProgress: StartupProgressFunc(func(phase StartupPhase) {
		if phase == StartupPhaseMigrating {
			once.Do(func() { close(entered); <-release })
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	<-entered
	waitContext, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- host.PrepareThread(waitContext, "thread-startup-wait") }()
	deadline := time.Now().Add(time.Second)
	for {
		host.threadPreparations.mu.Lock()
		preparation := host.threadPreparations.threads["thread-startup-wait"]
		host.threadPreparations.mu.Unlock()
		if preparation != nil {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("thread preparation never started")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled wait = %v", err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("cancelling a wait did not return promptly")
	}
	host.threadPreparations.mu.Lock()
	preparation := host.threadPreparations.threads["thread-startup-wait"]
	host.threadPreparations.mu.Unlock()
	select {
	case <-preparation.done:
		close(release)
		t.Fatal("caller cancellation stopped Host-owned preparation")
	default:
	}
	close(release)
	select {
	case <-preparation.done:
		if errors.Is(preparation.err, context.Canceled) {
			t.Fatal("Host preparation inherited caller cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Host preparation did not settle")
	}
}
