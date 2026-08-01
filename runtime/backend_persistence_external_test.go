package runtime_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
)

func TestSQLiteBackendPersistsCanonicalThreadsAcrossHostRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret-v3.db")

	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.SQLite(path),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-1", "thread-2"},
			turns:   []identity.TurnID{"turn-1"},
			runs:    []identity.RunID{"run-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-1"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Answer precisely.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, &completingGateway{requests: make(chan provider.Request, 1)})
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	started, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request-1", UserMessage: runtime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := mustThreadLifecycle(t, thread)
	forked, err := lifecycle.Fork(ctx, runtime.ForkThreadCommand{LogicalRequestID: "fork-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	reopened, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.SQLite(path),
		IDSource: &deterministicIDs{
			turns: []identity.TurnID{"turn-2"}, runs: []identity.RunID{"run-2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Shutdown(context.Background()) })
	reopenedThread, err := reopened.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader := mustThreadReader(t, reopenedThread)
	view, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if view.Thread.ID != created.ThreadID || view.Thread.LatestTurnID != started.TurnID || view.Thread.LatestRunID != started.RunID {
		t.Fatalf("restarted thread = %#v", view)
	}
	if fork, err := reopened.Thread(ctx, forked.ThreadID); err != nil || fork.ID() != forked.ThreadID {
		t.Fatalf("restarted fork = %#v, err = %v", fork, err)
	}

	restartedGateway := &completingGateway{requests: make(chan provider.Request, 1)}
	restartedAgent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Answer precisely.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, restartedGateway)
	if err != nil {
		t.Fatal(err)
	}
	restartedTurns, err := reopenedThread.TurnExecutor(restartedAgent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restartedTurns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request-2", UserMessage: runtime.TurnInput{Text: "again"},
	}); err != nil {
		t.Fatal(err)
	}
	request := <-restartedGateway.requests
	if request.PreviousState == nil || request.PreviousState.ID != "state-turn-1" {
		t.Fatalf("restarted provider state = %#v", request.PreviousState)
	}
}
