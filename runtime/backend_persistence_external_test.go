package runtime_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/provider"
	"github.com/floegence/floret/v2/runtime"
	"github.com/floegence/floret/v2/storage"
)

func TestSQLiteBackendPersistsCanonicalThreadsAcrossHostRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret-v2.db")

	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	creator, err := host.ThreadCreator("thread-1", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.Create(ctx); err != nil {
		t.Fatal(err)
	}
	titles, err := host.ThreadTitleEditor(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := titles.Set(ctx, "Persistent thread"); err != nil {
		t.Fatal(err)
	}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Answer precisely.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, &completingGateway{requests: make(chan provider.Request, 1)})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := host.TurnRunner(ctx, "thread-1", agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, runtime.TurnRequest{RunID: "run-1", TurnID: "turn-1", Input: runtime.TurnInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	forker, err := host.ThreadForker(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forker.Fork(ctx, runtime.ThreadForkRequest{OperationID: "fork-1", DestinationThreadID: "thread-2"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reader, err := reopened.ThreadReader(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	thread, err := reader.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if thread.ID != "thread-1" {
		t.Fatalf("thread id = %q", thread.ID)
	}
	if thread.Title != "Persistent thread" {
		t.Fatalf("thread title = %q", thread.Title)
	}
	turn, err := reader.ReadTurn(ctx, "turn-1")
	if err != nil {
		t.Fatal(err)
	}
	if turn.RunID != "run-1" || projectedAssistantText(turn.Projection) != "done" {
		t.Fatalf("restarted turn = %#v", turn)
	}
	forkReader, err := reopened.ThreadReader(ctx, "thread-2")
	if err != nil {
		t.Fatal(err)
	}
	if fork, err := forkReader.Read(ctx); err != nil || fork.ID != "thread-2" {
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
	restartedRunner, err := reopened.TurnRunner(ctx, "thread-1", restartedAgent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restartedRunner.Run(ctx, runtime.TurnRequest{RunID: "run-2", TurnID: "turn-2", Input: runtime.TurnInput{Text: "again"}}); err != nil {
		t.Fatal(err)
	}
	request := <-restartedGateway.requests
	if request.PreviousState == nil || request.PreviousState.ID != "state-turn-1" {
		t.Fatalf("restarted provider state = %#v", request.PreviousState)
	}
}
