package runtime_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/identity"
	legacy "github.com/floegence/floret/v3/internal/storage/sqlite"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/provider"
	floretruntime "github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

type capturingSource struct {
	next    spi.Source
	backend spi.Backend
}

func TestOpenClassifiesExactV16SQLiteAsMigrationRequired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}
	host, err := floretruntime.Open(context.Background(), floretruntime.Options{Storage: storage.SQLite(path)})
	if host != nil || !errors.Is(err, floretruntime.ErrMigrationRequired) {
		t.Fatalf("host=%#v err=%v", host, err)
	}
	var migrationRequired *floretruntime.MigrationRequiredError
	if !errors.As(err, &migrationRequired) || migrationRequired.Version != "16" {
		t.Fatalf("migration error = %#v (%v)", migrationRequired, err)
	}
}

func (source *capturingSource) Open(ctx context.Context) (spi.Backend, error) {
	backend, err := source.next.Open(ctx)
	if err == nil {
		source.backend = backend
	}
	return backend, err
}

type completingGateway struct {
	requests chan provider.Request
}

func (gateway *completingGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "complete", StateCompatibilityKey: "test:complete:v1"}
}

func (gateway *completingGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}

func (gateway *completingGateway) Stream(_ context.Context, request provider.Request) (<-chan provider.Event, error) {
	gateway.requests <- request
	events := make(chan provider.Event, 2)
	events <- provider.Event{Type: provider.EventDelta, Text: "done"}
	events <- provider.Event{
		Type: provider.EventDone, Reason: "stop",
		ResponseState: &provider.State{Kind: "response", ID: "state-" + string(request.TurnID)},
	}
	close(events)
	return events, nil
}

func TestHostIssuesV3IdentityBoundHandlesAndOwnsBackend(t *testing.T) {
	ctx := context.Background()
	source := &capturingSource{next: officialSPISource{source: storage.Memory()}}
	host, err := floretruntime.Open(ctx, floretruntime.Options{
		Storage: storage.NewSource(source),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-1", "thread-2"},
			turns:   []identity.TurnID{"turn-1"},
			runs:    []identity.RunID{"run-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	created, err := host.Threads().CreateThread(ctx, floretruntime.CreateThreadCommand{LogicalRequestID: "create-1"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := &completingGateway{requests: make(chan provider.Request, 1)}
	agent, err := floretruntime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Answer precisely.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	started, err := turns.StartTurn(ctx, floretruntime.StartTurnCommand{
		LogicalRequestID: "turn-request-1", UserMessage: floretruntime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := <-gateway.requests
	if request.ThreadID != created.ThreadID || request.TurnID != started.TurnID || request.RunID != started.RunID || len(request.Messages) == 0 {
		t.Fatalf("provider request = %#v", request)
	}
	reader := mustThreadReader(t, thread)
	lifecycle := mustThreadLifecycle(t, thread)
	view, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if view.Thread.LatestTurnID != started.TurnID || view.Thread.LatestRunID != started.RunID {
		t.Fatalf("thread snapshot = %#v", view)
	}

	forked, err := lifecycle.Fork(ctx, floretruntime.ForkThreadCommand{LogicalRequestID: "fork-1"})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := host.Thread(ctx, forked.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	destinationLifecycle := mustThreadLifecycle(t, destination)
	if _, err := destinationLifecycle.Delete(ctx, floretruntime.DeleteThreadCommand{LogicalRequestID: "delete-1"}); err != nil {
		t.Fatal(err)
	}

	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := source.backend.View(ctx, func(spi.ReadTx) error { return nil }); !errors.Is(err, spi.ErrClosed) {
		t.Fatalf("backend after Host.Shutdown = %v", err)
	}
}

type officialSPISource struct{ source storage.Source }

func (source officialSPISource) Open(ctx context.Context) (spi.Backend, error) {
	return storagebridge.Open(ctx, storagebridge.Source(source.source))
}
