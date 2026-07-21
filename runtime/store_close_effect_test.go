package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/storage/sqlite"
	"github.com/floegence/floret/tools"
)

func TestStoreCloseWaitsForDispatchedEffectBeforeTerminalMemory(t *testing.T) {
	policy := sessiontree.LeasePolicy{TTL: 2 * time.Second, RenewInterval: 300 * time.Millisecond, ClockSkewAllowance: 100 * time.Millisecond}
	repo, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	store.repo = repo
	store.rootAuthority = repo
	store.agentTodos = repo
	store.forkOperations = storage.NewMemoryForkOperationStore(repo)
	runStoreCloseEffectScenario(t, store, repo, policy)
}

func TestStoreCloseWaitsForDispatchedEffectBeforeTerminalSQLite(t *testing.T) {
	policy := sessiontree.LeasePolicy{TTL: 2 * time.Second, RenewInterval: 300 * time.Millisecond, ClockSkewAllowance: 100 * time.Millisecond}
	path := filepath.Join(t.TempDir(), "store-close-effect.db")
	repo, err := sqlite.Open(path, sqlite.WithLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{
		repo: repo, prompt: repo, forkOperations: repo, agentTodos: repo, rootAuthority: repo,
		deleteCleanup: func(context.Context, []string) error { return nil }, close: repo.Close,
	}
	store.self = store
	store.initLifetime()
	runStoreCloseEffectScenario(t, store, nil, policy)

	reopened, err := sqlite.Open(path, sqlite.WithLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertClosedEffectJournal(t, reopened)
}

func runStoreCloseEffectScenario(t *testing.T, store *Store, repo sessiontree.Repo, policy sessiontree.LeasePolicy) {
	t.Helper()
	ctx := context.Background()
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("thread-close-effect")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "shell", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, Effects: []tools.Effect{tools.EffectShell}},
		nil, nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			close(started)
			<-release
			return tools.Result{Text: "late output"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 2)
		if req.Step == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-close-effect", Name: "shell", Args: `{"text":"late"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "unexpected"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("store close effect"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("close-effect"),
		Store: store, Tools: registry, EffectAuthorizationGate: allowRuntimeEffectGate{}, IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, runErr := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread-close-effect", TurnID: "turn-close-effect", RunID: "run-close-effect", Input: TurnInput{Text: "run shell"}})
		runDone <- runErr
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("effect handler did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case runErr := <-runDone:
		close(release)
		t.Fatalf("run completed before late handler release: %v", runErr)
	case closeErr := <-closeDone:
		close(release)
		t.Fatalf("store close completed before late handler release: %v", closeErr)
	case <-time.After(2 * policy.TTL):
	}
	close(release)
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("run error=%v, want context.Canceled", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not finish after late handler release")
	}
	select {
	case closeErr := <-closeDone:
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("store close did not wait for effect finalization")
	}
	if repo != nil {
		assertClosedEffectJournal(t, repo)
	}
}

func assertClosedEffectJournal(t *testing.T, repo interface {
	Entries(context.Context, string) ([]sessiontree.Entry, error)
}) {
	t.Helper()
	entries, err := repo.Entries(context.Background(), "thread-close-effect")
	if err != nil {
		t.Fatal(err)
	}
	toolResults := 0
	terminalMarkers := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "call-close-effect" && entry.Message.Content == "late output" {
			toolResults++
		}
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn-close-effect" && entry.TurnStatus == sessiontree.TurnAborted {
			terminalMarkers++
		}
	}
	if toolResults != 1 || terminalMarkers != 1 {
		t.Fatalf("effect journal tool_results=%d terminal_markers=%d entries=%#v", toolResults, terminalMarkers, entries)
	}
}
