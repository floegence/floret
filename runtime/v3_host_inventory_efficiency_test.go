package runtime_test

import (
	"context"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

type inventoryCountingSource struct {
	source    storage.Source
	viewCalls atomic.Int64
}

func (source *inventoryCountingSource) Open(ctx context.Context) (spi.Backend, error) {
	backend, err := storagebridge.Open(ctx, storagebridge.Source(source.source))
	if err != nil {
		return nil, err
	}
	return &inventoryCountingBackend{Backend: backend, viewCalls: &source.viewCalls}, nil
}

type inventoryCountingBackend struct {
	spi.Backend
	viewCalls *atomic.Int64
}

func (backend *inventoryCountingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	backend.viewCalls.Add(1)
	return backend.Backend.View(ctx, read)
}

func TestV3ListThreadsReadsCanonicalDomainOncePerPage(t *testing.T) {
	for _, test := range []struct {
		name   string
		source func(*testing.T) storage.Source
	}{
		{name: "memory", source: func(*testing.T) storage.Source { return storage.Memory() }},
		{name: "sqlite", source: func(t *testing.T) storage.Source {
			return storage.SQLite(filepath.Join(t.TempDir(), "floret.db"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			testV3ListThreadsReadsCanonicalDomainOncePerPage(t, test.source(t))
		})
	}
}

func testV3ListThreadsReadsCanonicalDomainOncePerPage(t *testing.T, backendSource storage.Source) {
	t.Helper()
	ctx := context.Background()
	source := &inventoryCountingSource{source: backendSource}
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.NewSource(source),
		IDSource: &deterministicIDs{threads: []identity.ThreadID{
			"thread-inventory-1",
			"thread-inventory-2",
			"thread-inventory-3",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()

	for index := 0; index < 3; index++ {
		if _, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
			LogicalRequestID: identity.LogicalRequestID("create-inventory-" + strconv.Itoa(index+1)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	thread, err := host.Thread(ctx, "thread-inventory-2")
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := mustThreadLifecycle(t, thread)
	if _, err := lifecycle.SetTitle(ctx, runtime.SetThreadTitleCommand{
		LogicalRequestID: "title-inventory-2", Title: "Inventory two",
	}); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := mustThreadReader(t, thread).Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	direct := bootstrap.Thread

	source.viewCalls.Store(0)
	page, err := host.Threads().ListThreads(ctx, runtime.ListThreadsOptions{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Threads) != 3 {
		t.Fatalf("thread count = %d, want 3", len(page.Threads))
	}
	var listed runtime.ThreadSnapshot
	for _, item := range page.Threads {
		if item.Thread.ID == direct.ID {
			listed = item.Thread
			break
		}
	}
	if !reflect.DeepEqual(listed, direct) {
		t.Fatalf("listed thread = %#v, want exact snapshot %#v", listed, direct)
	}
	if calls := source.viewCalls.Load(); calls != 1 {
		t.Fatalf("canonical domain reads = %d, want 1 bounded page snapshot", calls)
	}
}
