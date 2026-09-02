package runtime

import (
	"context"
	"sync"
	"testing"

	"github.com/floegence/floret/v7/internal/storagebridge"
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
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			if len(phases) != 1 || phases[0] != StartupPhaseVerifying {
				t.Fatalf("startup phases = %v, want [%s]", phases, StartupPhaseVerifying)
			}
		})
	}
}

func TestOpenCurrentStoreDecodesV9Once(t *testing.T) {
	path := t.TempDir() + "/floret.sqlite"
	first, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
	if err != nil {
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
	if err := second.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := counts.fullV9Scans(); got != 1 {
		t.Fatalf("full v9 scans = %d, want 1", got)
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
	mu     sync.Mutex
	fullV9 int
}

func (counts *startupScanCounts) record(request spi.ScanRequest) {
	if request.Namespace != "floret.domain.sessiontree.v9" || request.Limit != 256 {
		return
	}
	counts.mu.Lock()
	counts.fullV9++
	counts.mu.Unlock()
}

func (counts *startupScanCounts) fullV9Scans() int {
	counts.mu.Lock()
	defer counts.mu.Unlock()
	return counts.fullV9
}

type startupCountingReadTx struct {
	spi.ReadTx
	counts *startupScanCounts
}

func (tx startupCountingReadTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.counts.record(request)
	return tx.ReadTx.Scan(request)
}

type startupCountingWriteTx struct {
	spi.WriteTx
	counts *startupScanCounts
}

func (tx startupCountingWriteTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.counts.record(request)
	return tx.WriteTx.Scan(request)
}
