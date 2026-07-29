package runtime_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/floegence/floret/v2/config"
	legacy "github.com/floegence/floret/v2/internal/storage/sqlite"
	"github.com/floegence/floret/v2/provider"
	floretruntime "github.com/floegence/floret/v2/runtime"
	"github.com/floegence/floret/v2/storage"
)

type capturingSource struct {
	next    storage.Source
	backend storage.Backend
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

func (source *capturingSource) Open(ctx context.Context) (storage.Backend, error) {
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

func TestHostIssuesIdentityBoundHandlesAndOwnsBackend(t *testing.T) {
	ctx := context.Background()
	source := &capturingSource{next: storage.Memory()}
	host, err := floretruntime.Open(ctx, floretruntime.Options{Storage: source})
	if err != nil {
		t.Fatal(err)
	}

	creator, err := host.ThreadCreator(floretruntime.ThreadID("thread-1"), floretruntime.CreateIntentID("create-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.Create(ctx); err != nil {
		t.Fatal(err)
	}
	titleEditor, err := host.ThreadTitleEditor(ctx, floretruntime.ThreadID("thread-1"))
	if err != nil {
		t.Fatal(err)
	}
	if titled, err := titleEditor.Set(ctx, "Primary thread"); err != nil || titled.Title != "Primary thread" {
		t.Fatalf("set title result=%#v err=%v", titled, err)
	}

	gateway := &completingGateway{requests: make(chan provider.Request, 1)}
	agent, err := floretruntime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Answer precisely.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := host.TurnRunner(ctx, floretruntime.ThreadID("thread-1"), agent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, floretruntime.TurnRequest{
		RunID: "run-1", TurnID: "turn-1", Input: floretruntime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != floretruntime.TurnStatusCompleted || result.Output != "done" {
		t.Fatalf("turn result = %#v", result)
	}
	request := <-gateway.requests
	if request.ThreadID != "thread-1" || request.TurnID != "turn-1" || len(request.Messages) == 0 {
		t.Fatalf("provider request = %#v", request)
	}

	reader, err := host.ThreadReader(ctx, floretruntime.ThreadID("thread-1"))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := reader.ReadTurn(ctx, floretruntime.TurnID("turn-1"))
	if err != nil {
		t.Fatal(err)
	}
	if turn.RunID != "run-1" || projectedAssistantText(turn.Projection) != "done" {
		t.Fatalf("canonical turn = %#v", turn)
	}
	forker, err := host.ThreadForker(ctx, floretruntime.ThreadID("thread-1"))
	if err != nil {
		t.Fatal(err)
	}
	forked, err := forker.Fork(ctx, floretruntime.ThreadForkRequest{OperationID: "fork-1", DestinationThreadID: "thread-2"})
	if err != nil || forked.Thread.ID != "thread-2" {
		t.Fatalf("fork result=%#v err=%v", forked, err)
	}
	deleter, err := host.ThreadDeleter(ctx, floretruntime.ThreadID("thread-2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := deleter.Delete(ctx); err != nil {
		t.Fatal(err)
	}

	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.backend.View(ctx, func(storage.ReadTx) error { return nil }); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("backend after Host.Close = %v", err)
	}
}

func projectedAssistantText(projection floretruntime.ThreadTurnProjection) string {
	var text string
	for _, segment := range projection.Segments {
		if segment.Kind == floretruntime.ThreadTurnProjectionSegmentAssistantText {
			text += segment.Text
		}
	}
	return text
}

func TestBoundRequestsContainNoThreadIdentity(t *testing.T) {
	if _, exists := reflect.TypeOf(floretruntime.TurnRequest{}).FieldByName("ThreadID"); exists {
		t.Fatal("TurnRequest repeats the bound ThreadID")
	}
	if _, exists := reflect.TypeOf(floretruntime.ThreadForkRequest{}).FieldByName("SourceThreadID"); exists {
		t.Fatal("ThreadForkRequest repeats the bound source ThreadID")
	}
	for _, request := range []any{
		floretruntime.SpawnSubAgent{}, floretruntime.SendSubAgentInput{},
		floretruntime.WaitSubAgents{}, floretruntime.CloseSubAgent{},
	} {
		if _, exists := reflect.TypeOf(request).FieldByName("ParentThreadID"); exists {
			t.Fatalf("%T repeats the bound parent ThreadID", request)
		}
	}
	method, exists := reflect.TypeOf((*floretruntime.ThreadCreator)(nil)).MethodByName("Create")
	if !exists || method.Type.NumIn() != 2 {
		t.Fatalf("ThreadCreator.Create signature = %v", method.Type)
	}
}
