package runtime_test

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/florettest"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/internal/storagecodec"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

type inventoryCountingSource struct {
	source          storage.Source
	viewCalls       atomic.Int64
	fullDomainReads atomic.Int64
}

func (source *inventoryCountingSource) Open(ctx context.Context) (spi.Backend, error) {
	backend, err := storagebridge.Open(ctx, storagebridge.Source(source.source))
	if err != nil {
		return nil, err
	}
	return &inventoryCountingBackend{
		Backend: backend, viewCalls: &source.viewCalls, fullDomainReads: &source.fullDomainReads,
	}, nil
}

type inventoryCountingBackend struct {
	spi.Backend
	viewCalls       *atomic.Int64
	fullDomainReads *atomic.Int64
}

func (backend *inventoryCountingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	backend.viewCalls.Add(1)
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error {
		return read(inventoryCountingReadTx{ReadTx: tx, fullDomainReads: backend.fullDomainReads})
	})
}

type inventoryCountingReadTx struct {
	spi.ReadTx
	fullDomainReads *atomic.Int64
}

func (tx inventoryCountingReadTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == "floret.domain" && bytes.Equal(key, storagecodec.Tuple(
		storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"),
	)) {
		tx.fullDomainReads.Add(1)
	}
	return tx.ReadTx.Get(namespace, key)
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

func TestV3ThreadBootstrapReadsCanonicalDomainOnce(t *testing.T) {
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
			ctx := context.Background()
			source := &inventoryCountingSource{source: test.source(t)}
			host, err := runtime.Open(ctx, runtime.Options{
				Storage: storage.NewSource(source),
				IDSource: &deterministicIDs{
					threads: []identity.ThreadID{"thread-bootstrap"},
					turns:   []identity.TurnID{"turn-bootstrap"},
					runs:    []identity.RunID{"run-bootstrap"},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = host.Shutdown(context.Background()) }()

			created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
				LogicalRequestID: "create-bootstrap",
			})
			if err != nil {
				t.Fatal(err)
			}
			thread, err := host.Thread(ctx, created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			gateway := florettest.NewScriptedGateway(
				provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
				provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "bootstrap response"}, {Type: provider.EventDone}}},
			)
			agent, err := runtime.NewAgent(config.AgentConfig{
				Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
				Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			}, gateway)
			if err != nil {
				t.Fatal(err)
			}
			executor, err := thread.TurnExecutor(agent)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := executor.StartTurn(ctx, runtime.StartTurnCommand{
				LogicalRequestID: "start-bootstrap", UserMessage: runtime.TurnInput{Text: "bootstrap request"},
			}); err != nil {
				t.Fatal(err)
			}
			reader := mustThreadReader(t, thread)

			source.viewCalls.Store(0)
			source.fullDomainReads.Store(0)
			bootstrap, err := reader.Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 20})
			if err != nil {
				t.Fatal(err)
			}
			if bootstrap.Thread.ID != created.ThreadID || bootstrap.Overview.LatestTurn == nil || len(bootstrap.Turns.Turns) != 1 {
				t.Fatalf("bootstrap=%#v, want complete canonical thread and turn", bootstrap)
			}
			if calls := source.viewCalls.Load(); calls != 0 {
				t.Fatalf("backend views=%d, want memory-authority bootstrap without SQL", calls)
			}
			if reads := source.fullDomainReads.Load(); reads != 0 {
				t.Fatalf("full session-tree domain reads=%d, want memory-authority bootstrap without blob read", reads)
			}
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
		}, turns: []identity.TurnID{"turn-inventory-2"}, runs: []identity.RunID{"run-inventory-2"}},
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
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "inventory response"}, {Type: provider.EventDone}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-inventory-2", UserMessage: runtime.TurnInput{Text: "inventory request"},
	}); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := mustThreadReader(t, thread).Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	direct := bootstrap.Overview

	source.viewCalls.Store(0)
	source.fullDomainReads.Store(0)
	page, err := host.Threads().ListThreads(ctx, runtime.ListThreadsOptions{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Threads) != 3 {
		t.Fatalf("thread count = %d, want 3", len(page.Threads))
	}
	var listed runtime.ThreadListItem
	for _, item := range page.Threads {
		if item.Thread.ID == direct.Thread.ID {
			listed = item
			continue
		}
		if item.LatestTurn != nil {
			t.Fatalf("empty thread %q latest turn = %+v, want nil", item.Thread.ID, item.LatestTurn)
		}
	}
	if !reflect.DeepEqual(listed.Thread, direct.Thread) {
		t.Fatalf("listed thread = %#v, want exact thread %#v", listed.Thread, direct.Thread)
	}
	if listed.LatestTurn == nil || direct.LatestTurn == nil {
		t.Fatalf("listed latest turn = %+v, want exact latest turn %+v", listed.LatestTurn, direct.LatestTurn)
	}
	if listed.LatestTurn.Projection.ProjectedAt.IsZero() || listed.LatestTurn.Projection.ProjectedAt.Location() != time.UTC {
		t.Fatalf("listed projection time = %v, want non-zero UTC", listed.LatestTurn.Projection.ProjectedAt)
	}
	listedLatest := *listed.LatestTurn
	directLatest := *direct.LatestTurn
	listedLatest.Projection.ProjectedAt = time.Time{}
	directLatest.Projection.ProjectedAt = time.Time{}
	if !reflect.DeepEqual(listedLatest, directLatest) {
		t.Fatalf("listed latest turn = %+v, want exact latest turn %+v", listedLatest, directLatest)
	}
	if calls := source.viewCalls.Load(); calls != 0 {
		t.Fatalf("backend views = %d, want memory-authority inventory without SQL", calls)
	}
	if reads := source.fullDomainReads.Load(); reads != 0 {
		t.Fatalf("full session-tree domain reads = %d, want lightweight inventory record only", reads)
	}
}
