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
	overview, err := reader.ReadOverview(ctx)
	if err != nil || overview.LatestTurn == nil || overview.LatestTurn.TurnID != "turn-1" {
		t.Fatalf("thread overview = %#v, err = %v", overview, err)
	}
	turns, err := reader.ListTurns(ctx, floretruntime.ThreadTurnsRequest{Tail: 10})
	if err != nil || len(turns.Turns) != 1 || turns.Turns[0].TurnID != "turn-1" {
		t.Fatalf("thread turns = %#v, err = %v", turns, err)
	}
	detail, err := reader.ListDetailEvents(ctx, floretruntime.ThreadDetailRequest{Limit: 100})
	if err != nil || len(detail.Events) == 0 {
		t.Fatalf("thread detail = %#v, err = %v", detail, err)
	}
	if targets, err := reader.ListPendingToolTargets(ctx); err != nil || len(targets) != 0 {
		t.Fatalf("pending tool targets = %#v, err = %v", targets, err)
	}
	if todos, err := reader.ReadAgentTodos(ctx); err != nil || todos.ThreadID != "thread-1" {
		t.Fatalf("Agent todos = %#v, err = %v", todos, err)
	}
	if contextSnapshot, err := reader.ReadContext(ctx); err != nil || contextSnapshot.ThreadID != "thread-1" {
		t.Fatalf("thread context = %#v, err = %v", contextSnapshot, err)
	}
	if approvals, err := reader.ReadApprovalQueue(ctx); err != nil || approvals.RootThreadID != "thread-1" {
		t.Fatalf("approval queue = %#v, err = %v", approvals, err)
	}
	if projection, err := reader.ReadProjection(ctx, "turn-1", "run-1"); err != nil || projectedAssistantText(projection) != "done" {
		t.Fatalf("turn projection = %#v, err = %v", projection, err)
	}
	if artifact, err := reader.ReadArtifact(ctx, "missing"); !errors.Is(err, floretruntime.ErrArtifactNotFound) || artifact != (floretruntime.ArtifactContent{}) {
		t.Fatalf("missing artifact = %#v, err = %v", artifact, err)
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
		requestType := reflect.TypeOf(request)
		if _, exists := requestType.FieldByName("ParentThreadID"); exists {
			t.Fatalf("%T repeats the bound parent ThreadID", request)
		}
	}
	for _, request := range []any{
		floretruntime.ThreadTurnsRequest{}, floretruntime.ThreadDetailRequest{},
		floretruntime.AgentTodoUpdateRequest{}, floretruntime.ApprovalResolutionRequest{},
		floretruntime.ActivePendingToolCompletion{}, floretruntime.ActivePendingToolSettlement{},
	} {
		requestType := reflect.TypeOf(request)
		if _, exists := requestType.FieldByName("ThreadID"); exists {
			t.Fatalf("%T repeats the bound ThreadID", request)
		}
	}
	method, exists := reflect.TypeOf((*floretruntime.ThreadCreator)(nil)).MethodByName("Create")
	if !exists || method.Type.NumIn() != 2 {
		t.Fatalf("ThreadCreator.Create signature = %v", method.Type)
	}
}

func TestHostHandlesExposeCompleteDurableLifecycleSurface(t *testing.T) {
	ctx := context.Background()
	var reader *floretruntime.ThreadReader
	_, _ = reader.ReadOverview(ctx)
	_, _ = reader.ListTurns(ctx, floretruntime.ThreadTurnsRequest{})
	_, _ = reader.ListDetailEvents(ctx, floretruntime.ThreadDetailRequest{})
	_, _ = reader.ListPendingToolTargets(ctx)
	_, _ = reader.ReadAgentTodos(ctx)
	_, _ = reader.ReadContext(ctx)
	_, _ = reader.ReadApprovalQueue(ctx)
	_, _ = reader.ReadProjection(ctx, "turn-1", "run-1")
	_, _ = reader.ReadArtifact(ctx, "artifact-1")

	var runner *floretruntime.TurnRunner
	_, _ = runner.Retry(ctx, floretruntime.RetryRequest{})
	_, _ = runner.CompletePendingTool(ctx, floretruntime.ActivePendingToolCompletion{})
	_, _ = runner.ResolveApproval(ctx, floretruntime.ApprovalResolutionRequest{})
	_, _ = runner.UpdateAgentTodos(ctx, floretruntime.AgentTodoUpdateRequest{})
	_, _ = runner.SettlePendingTool(ctx, floretruntime.ActivePendingToolSettlement{})

	var manager *floretruntime.SubAgentManager
	_, _ = manager.SettlePendingTool(ctx, floretruntime.PendingToolSettlementRequest{})

	var subagents *floretruntime.SubAgentReader
	_, _ = subagents.ReadTurn(ctx, "child-1", "turn-1")
	_, _ = subagents.ListTurns(ctx, "child-1", floretruntime.ThreadTurnsRequest{})
	_, _ = subagents.ListPendingToolTargets(ctx, "child-1")
	_, _ = subagents.ReadArtifact(ctx, "child-1", "artifact-1")
}
