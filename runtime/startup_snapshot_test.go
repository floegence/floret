package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

type snapshotStartupBackend struct {
	spi.Backend
	wholeReads atomic.Int64
	maxRange   atomic.Int64
}
type snapshotStartupRead struct {
	spi.ReadTx
	owner *snapshotStartupBackend
}
type snapshotStartupWrite struct {
	snapshotStartupRead
	write spi.WriteTx
}

func (backend *snapshotStartupBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(snapshotStartupRead{tx, backend}) })
}
func (backend *snapshotStartupBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(snapshotStartupWrite{snapshotStartupRead{tx, backend}, tx}) })
}
func (tx snapshotStartupRead) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == "floret.domain" && bytes.Equal(key, storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))) {
		tx.owner.wholeReads.Add(1)
		return nil, errors.New("startup attempted whole snapshot read")
	}
	return tx.ReadTx.Get(namespace, key)
}
func (tx snapshotStartupRead) NamespaceHasRecords(namespace string) (bool, error) {
	return tx.ReadTx.(spi.BoundedRecordReader).NamespaceHasRecords(namespace)
}

func (tx snapshotStartupRead) RecordSize(namespace string, key []byte) (int64, error) {
	return tx.ReadTx.(spi.BoundedRecordReader).RecordSize(namespace, key)
}
func (tx snapshotStartupRead) ReadRecordRange(namespace string, key []byte, offset int64, length int) ([]byte, error) {
	for previous := tx.owner.maxRange.Load(); int64(length) > previous; previous = tx.owner.maxRange.Load() {
		if tx.owner.maxRange.CompareAndSwap(previous, int64(length)) {
			break
		}
	}
	if length > 64<<10 {
		return nil, errors.New("unbounded snapshot range read")
	}
	return tx.ReadTx.(spi.BoundedRecordReader).ReadRecordRange(namespace, key, offset, length)
}
func (tx snapshotStartupWrite) Put(namespace string, key, value []byte) error {
	return tx.write.Put(namespace, key, value)
}
func (tx snapshotStartupWrite) Delete(namespace string, key []byte) error {
	return tx.write.Delete(namespace, key)
}

func snapshotStartupFixture(t *testing.T) *snapshotStartupBackend {
	t.Helper()
	underlying, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	memory := sessiontree.NewMemoryRepo()
	if _, err := memory.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "snapshot-thread", CreatedAt: time.Unix(1900000000, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(t.Context(), sessiontree.AcceptTurnRequest{
		ThreadID: "snapshot-thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "accept", InputRequestFingerprint: "input",
		Input: session.Message{Role: session.User, Content: strings.Repeat("preserved body 中文 ", 50000)}, Now: time.Unix(1900000000, 0).UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.FinishTurn(t.Context(), sessiontree.FinishTurnRequest{
		ThreadID: "snapshot-thread", TurnID: "turn", RunID: "run", TerminalEntryID: "end",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "done", Now: time.Unix(1900000000, 0).UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		t.Fatal(err)
	}
	// Preserve the exact released full-path inventory beside the snapshot.
	meta, err := memory.Thread(t.Context(), "snapshot-thread")
	if err != nil {
		t.Fatal(err)
	}
	path, err := memory.Path(t.Context(), "snapshot-thread", meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := json.Marshal(struct {
		Version int                                   `json:"version"`
		Items   []sessiontree.RootThreadInventoryItem `json:"items"`
	}{1, []sessiontree.RootThreadInventoryItem{{Meta: meta, Path: path}}})
	if err != nil {
		t.Fatal(err)
	}
	roots, err := storagecodec.EncodeEnvelope("sessiontree-root-thread-inventory", inventory)
	if err != nil {
		t.Fatal(err)
	}
	if err := underlying.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := tx.Put("floret.domain", storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state")), raw); err != nil {
			return err
		}
		return tx.Put("floret.domain", storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory")), roots)
	}); err != nil {
		t.Fatal(err)
	}
	return &snapshotStartupBackend{Backend: underlying}
}

func TestOpenMigratesMonolithicSourceWithoutWholeSnapshotRead(t *testing.T) {
	backend := snapshotStartupFixture(t)
	host, err := Open(t.Context(), Options{Storage: publicstorage.NewSource(startupBackendSource{backend: backend})})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if backend.wholeReads.Load() != 0 {
		t.Fatalf("whole reads=%d", backend.wholeReads.Load())
	}
	if _, err := host.store.repo.Thread(t.Context(), "snapshot-thread"); err != nil {
		t.Fatal(err)
	}
}

type snapshotPausedBackend struct {
	spi.Backend
	updates atomic.Int64
	entered chan struct{}
	once    sync.Once
}

func (backend *snapshotPausedBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	if backend.updates.Add(1) > 1 {
		backend.once.Do(func() { close(backend.entered) })
		<-ctx.Done()
		return ctx.Err()
	}
	return backend.Backend.Update(ctx, mutate)
}
func TestOpenSnapshotIdentityWaitCancelsAndShutdownJoinsWorker(t *testing.T) {
	backend := &snapshotPausedBackend{Backend: snapshotStartupFixture(t), entered: make(chan struct{})}
	host, err := Open(t.Context(), Options{Storage: publicstorage.NewSource(startupBackendSource{backend: backend})})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	select {
	case <-backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("source scan did not start")
	}
	status, err := host.StorageMigrationStatus(t.Context())
	if err != nil || status.State != StorageMigrationRunning {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := host.store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "new"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("identity wait ignored cancellation: %v", err)
	}
	shutdown, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	if err := host.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
}
