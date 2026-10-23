package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
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

func TestOpenReportsVerifyingForFreshAndCurrentStores(t *testing.T) {
	path := t.TempDir() + "/floret.sqlite"
	for _, name := range []string{"fresh", "current"} {
		t.Run(name, func(t *testing.T) {
			var phases []StartupPhase
			host, err := Open(t.Context(), Options{
				Storage: publicstorage.SQLite(path),
				StartupProgress: StartupProgressFunc(func(phase StartupPhase) {
					phases = append(phases, phase)
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := host.startup.wait(t.Context()); err != nil {
				t.Fatal(err)
			}
			want := []StartupPhase{StartupPhaseVerifying}
			if !reflect.DeepEqual(phases, want) {
				t.Fatalf("startup phases = %v, want %v", phases, want)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenReturnsWhileBackgroundStoragePreparationIsBlocked(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	counts := &startupScanCounts{}
	path := storageUpgradeFixture(t)
	source := publicstorage.NewSource(startupCountingSource{source: publicstorage.SQLite(path), counts: counts})
	host, err := Open(t.Context(), Options{Storage: source, StartupProgress: StartupProgressFunc(func(phase StartupPhase) {
		if phase != StartupPhaseMigrating {
			return
		}
		close(entered)
		<-release
	})})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := counts.totalScans(); got != 0 {
		close(release)
		_ = host.Shutdown(context.Background())
		t.Fatalf("Open scanned %d backend pages before returning, want no domain scan", got)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		_ = host.Shutdown(context.Background())
		t.Fatal("background storage preparation did not start")
	}
	status, err := host.StorageMigrationStatus(t.Context())
	if err != nil {
		t.Fatalf("storage status while worker is blocked: %v", err)
	}
	if status.State != StorageMigrationRunning || status.Phase != StartupPhaseMigrating {
		t.Fatalf("storage status = %#v, want running migration", status)
	}
	waitCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := host.PrepareThread(waitCtx, identity.ThreadID("thread-startup-wait")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled thread preparation = %v, want context.Canceled", err)
	}
	close(release)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown after migration settles: %v", err)
	}
}

func TestOpenFreshStoreMakesThreadOperationsImmediatelyAvailable(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: publicstorage.Memory()})
	if err != nil {
		t.Fatalf("Open fresh store: %v", err)
	}
	status, err := host.StorageMigrationStatus(t.Context())
	if err != nil || status.State != StorageMigrationReady {
		t.Fatalf("fresh store status = %#v, err=%v", status, err)
	}
	meta, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread-immediately-ready"})
	if err != nil {
		t.Fatalf("create first thread without waiting for background startup: %v", err)
	}
	if meta.ID != "thread-immediately-ready" {
		t.Fatalf("created thread = %+v", meta)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestOpenCurrentStoreUsesBoundedFormatRecognition(t *testing.T) {
	path := t.TempDir() + "/floret.sqlite"
	first, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	counts := &startupScanCounts{}
	source := publicstorage.NewSource(startupCountingSource{source: publicstorage.SQLite(path), counts: counts})
	second, err := Open(t.Context(), Options{Storage: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, err := second.StorageMigrationStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StorageMigrationReady {
		t.Fatalf("current store status = %#v, want ready", status)
	}
	if err := second.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := counts.fullV9Scans(); got != 0 {
		t.Fatalf("full v11 scans = %d, want 0", got)
	}
}

func TestOpenCurrentStoreCompletesWithoutHistoricalBodyReads(t *testing.T) {
	for _, count := range []int{8, 128} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			path := t.TempDir() + "/current.sqlite"
			first, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
			if err != nil {
				t.Fatal(err)
			}
			defer first.Shutdown(context.Background())
			if _, err := first.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "history"}); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				if _, err := first.store.repo.Append(t.Context(), sessiontree.Entry{ID: fmt.Sprint(i), ThreadID: "history", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("retained fact ", 4096)}}, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := first.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				counts := &startupScanCounts{}
				source := publicstorage.NewSource(startupCountingSource{source: publicstorage.SQLite(path), counts: counts})
				host, err := Open(t.Context(), Options{Storage: source, EnableParentEvents: true})
				if err != nil {
					t.Fatal(err)
				}
				defer host.Shutdown(context.Background())
				// Include the entire background worker, not only the synchronous Open.
				if err := host.startup.wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				if got := counts.recordsRead(); got > 80 {
					t.Fatalf("complete restart read %d records for %d messages", got, count)
				}
				if got := counts.valueBytesRead(); got > 32<<10 {
					t.Fatalf("complete restart read %d bytes of historical bodies", got)
				}
				t.Logf("complete restart: %d records, %d bytes", counts.recordsRead(), counts.valueBytesRead())
				if err := host.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestOpenCurrentStoreReadsConstantRecordsWithLargeHistory(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	const domainNamespace = "floret.domain.sessiontree.v16"
	const promptNamespace = "floret.domain.prompt.v2"
	const threadCount = 10_000
	largeBody := bytes.Repeat([]byte{'x'}, 8<<20)
	logical, err := json.Marshal(logicalSchemaEnvelope{Version: logicalSchemaVersion, Fingerprint: logicalSchemaFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := storagecodec.EncodeEnvelope("sessiontree-v31-record", []byte(`{"version":31,"kind":"manifest","value":{"version":31,"sequence":0}}`))
	if err != nil {
		t.Fatal(err)
	}
	promptSchema, err := storagecodec.EncodeEnvelope("prompt_schema", []byte("2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := tx.Put(domainNamespace, storagecodec.Tuple(storagecodec.TupleString("manifest")), manifest); err != nil {
			return err
		}
		if err := tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), logical); err != nil {
			return err
		}
		if err := tx.Put(promptNamespace, storagecodec.Tuple(storagecodec.TupleString("schema")), promptSchema); err != nil {
			return err
		}
		for index := range threadCount {
			threadID := fmt.Sprintf("thread-%05d", index)
			value := []byte(fmt.Sprintf(`{"version":31,"kind":"thread","id":%q,"value":{"id":%q}}`, threadID, threadID))
			encoded, err := storagecodec.EncodeEnvelope("sessiontree-v31-record", value)
			if err != nil {
				return err
			}
			key := storagecodec.Tuple(storagecodec.TupleString("thread"), storagecodec.TupleString(threadID))
			if err := tx.Put(domainNamespace, key, encoded); err != nil {
				return err
			}
		}
		return tx.Put("floret.domain.sessiontree.bodies.v30", []byte("large-body"), largeBody)
	}); err != nil {
		t.Fatal(err)
	}
	counts := &startupScanCounts{}
	source := publicstorage.NewSource(startupCountingSource{
		source: publicstorage.NewSource(startupBackendSource{backend: backend}), counts: counts,
	})
	host, err := Open(t.Context(), Options{Storage: source})
	if err != nil {
		t.Fatalf("Open current store: %v", err)
	}
	if got := counts.recordsRead(); got > 8 {
		_ = host.Shutdown(context.Background())
		t.Fatalf("Open read %d backend records for %d threads, want a fixed bounded metadata read", got, threadCount)
	}
	if got := counts.valueBytesRead(); got > 4<<10 {
		_ = host.Shutdown(context.Background())
		t.Fatalf("Open read %d record bytes with an 8 MiB body present, want bounded metadata only", got)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

type startupCountingSource struct {
	source publicstorage.Source
	counts *startupScanCounts
}

func (source startupCountingSource) Open(ctx context.Context) (spi.Backend, error) {
	backend, err := storagebridge.Open(ctx, storagebridge.Source(source.source))
	if err != nil {
		return nil, err
	}
	return startupCountingBackend{Backend: backend, counts: source.counts}, nil
}

type startupCountingBackend struct {
	spi.Backend
	counts *startupScanCounts
}

func (backend startupCountingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error {
		return read(startupCountingReadTx{ReadTx: tx, counts: backend.counts})
	})
}

func (backend startupCountingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(startupCountingWriteTx{WriteTx: tx, counts: backend.counts})
	})
}

type startupScanCounts struct {
	mu          sync.Mutex
	fullV9      int
	scanCount   int
	readRecords int
	bytesRead   int64
}

func (counts *startupScanCounts) record(request spi.ScanRequest) {
	counts.mu.Lock()
	counts.scanCount++
	if request.Namespace != "floret.domain.sessiontree.v16" || request.Limit != 256 {
		counts.mu.Unlock()
		return
	}
	counts.fullV9++
	counts.mu.Unlock()
}

func (counts *startupScanCounts) fullV9Scans() int {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	return counts.fullV9
}

func (counts *startupScanCounts) totalScans() int {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	return counts.scanCount
}

func (counts *startupScanCounts) recordGet(value []byte) {
	counts.mu.Lock()
	counts.readRecords++
	counts.bytesRead += int64(len(value))
	counts.mu.Unlock()
}

func (counts *startupScanCounts) recordPage(page spi.ScanPage) {
	counts.mu.Lock()
	counts.readRecords += len(page.Records)
	for _, record := range page.Records {
		counts.bytesRead += int64(len(record.Key) + len(record.Value))
	}
	counts.mu.Unlock()
}

func (counts *startupScanCounts) recordsRead() int {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	return counts.readRecords
}

func (counts *startupScanCounts) valueBytesRead() int64 {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	return counts.bytesRead
}

type startupCountingReadTx struct {
	spi.ReadTx
	counts *startupScanCounts
}

func (tx startupCountingReadTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.counts.record(request)
	page, err := tx.ReadTx.Scan(request)
	tx.counts.recordPage(page)
	return page, err
}

func (tx startupCountingReadTx) Get(namespace string, key []byte) ([]byte, error) {
	value, err := tx.ReadTx.Get(namespace, key)
	if err == nil {
		tx.counts.recordGet(value)
	}
	return value, err
}

type startupCountingWriteTx struct {
	spi.WriteTx
	counts *startupScanCounts
}

func (tx startupCountingWriteTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.counts.record(request)
	page, err := tx.WriteTx.Scan(request)
	tx.counts.recordPage(page)
	return page, err
}

func (tx startupCountingWriteTx) Get(namespace string, key []byte) ([]byte, error) {
	value, err := tx.WriteTx.Get(namespace, key)
	if err == nil {
		tx.counts.recordGet(value)
	}
	return value, err
}
