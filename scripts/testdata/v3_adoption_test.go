// Package adoption_test exercises the released v3 API from a blank module.
package adoption_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v4/config"
	"github.com/floegence/floret/v4/florettest"
	"github.com/floegence/floret/v4/identity"
	"github.com/floegence/floret/v4/provider"
	"github.com/floegence/floret/v4/runtime"
	"github.com/floegence/floret/v4/storage"
)

func TestPublishedMemoryHostAndSubAgent(t *testing.T) {
	ctx := context.Background()
	gateway := scriptedGateway("parent done", "child done")
	agent := adoptionAgent(t, gateway)
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-root"})
	if err != nil {
		t.Fatal(err)
	}
	root, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := root.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	started, err := executor.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "root-turn", UserMessage: runtime.TurnInput{Text: "coordinate"},
	})
	if err != nil {
		t.Fatalf("root turn = %#v, err = %v", started, err)
	}
	subAgents, err := root.SubAgentManager(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	spawned, err := subAgents.SpawnSubAgent(ctx, runtime.SpawnSubAgentCommand{
		LogicalRequestID: "publish-child", ParentTurnID: started.TurnID, TaskName: "child",
		Input: runtime.TurnInput{Text: "complete the delegated work"}, ForkMode: runtime.SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSubAgent(t, ctx, subAgents, spawned.Child.ThreadID, runtime.SubAgentStatusCompleted)
	children, err := subAgents.List(ctx)
	if err != nil || len(children) != 1 || children[0].ThreadID != spawned.Child.ThreadID {
		t.Fatalf("children = %#v, err = %v", children, err)
	}
	rootReader, err := root.Reader()
	if err != nil {
		t.Fatal(err)
	}
	child, err := rootReader.Child(ctx, spawned.Child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	detail, err := child.ReadDetail(ctx, runtime.ThreadDetailRequest{IncludeRaw: true})
	if err != nil || detail.Snapshot.LastMessage != "child done" {
		t.Fatalf("child detail = %#v, err = %v", detail, err)
	}
	reader, err := rootReader.Descendant(ctx, spawned.Child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	childTurns, err := reader.ListTurns(ctx, runtime.ThreadTurnsRequest{Tail: 10})
	if err != nil || len(childTurns.Turns) != 1 || childTurns.Turns[0].UserMessageOrigin != runtime.ThreadUserMessageOriginDelegatedMission {
		t.Fatalf("child turns = %#v, err = %v", childTurns, err)
	}
	turn, err := reader.ReadTurn(ctx, childTurns.Turns[0].TurnID)
	if err != nil || turn.RunID != childTurns.Turns[0].RunID {
		t.Fatalf("child turn = %#v, err = %v", turn, err)
	}
}

func waitForSubAgent(t *testing.T, ctx context.Context, subAgents runtime.SubAgentManager, childThreadID identity.ThreadID, want runtime.SubAgentStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		children, err := subAgents.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, child := range children {
			if child.ThreadID == childThreadID && child.Status == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subagent %q did not reach status %q", childThreadID, want)
}

func TestPublishedSQLiteRestartUsesCanonicalRead(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-thread"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := thread.TurnExecutor(adoptionAgent(t, scriptedGateway("persisted")))
	if err != nil {
		t.Fatal(err)
	}
	started, err := executor.StartTurn(ctx, runtime.StartTurnCommand{LogicalRequestID: "turn", UserMessage: runtime.TurnInput{Text: "persist"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	reopened, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Shutdown(context.Background()) }()
	reopenedThread, err := reopened.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := reopenedThread.Reader()
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := reader.Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 20})
	if err != nil || bootstrap.Thread.LatestTurnID != started.TurnID || bootstrap.Thread.LatestRunID != started.RunID {
		t.Fatalf("bootstrap = %#v, err = %v", bootstrap, err)
	}
	page, err := reopened.Threads().ListThreads(ctx, runtime.ListThreadsOptions{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	var listed *runtime.ThreadListItem
	for index := range page.Threads {
		if page.Threads[index].Thread.ID == created.ThreadID {
			listed = &page.Threads[index]
			break
		}
	}
	if listed == nil || listed.LatestTurn == nil || listed.LatestTurn.TurnID != started.TurnID || listed.LatestTurn.RunID != started.RunID {
		t.Fatalf("listed thread = %#v, want latest turn %q/%q", listed, started.TurnID, started.RunID)
	}
	subscription, err := reader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: bootstrap.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscription.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublishedAdmissionPlanExecutesAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-admission"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := thread.TurnExecutor(adoptionAgent(t, scriptedGateway()))
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := executor.AdmitTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "admit", UserMessage: runtime.TurnInput{Text: "execute after restart"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	reopened, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Shutdown(context.Background()) }()
	reopenedThread, err := reopened.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reopenedExecutor, err := reopenedThread.TurnExecutor(adoptionAgent(t, scriptedGateway("executed")))
	if err != nil {
		t.Fatal(err)
	}
	executed, err := reopenedExecutor.ExecuteAdmission(ctx, admitted.Receipt, runtime.ExecutionContext{})
	if err != nil || executed.TurnID != admitted.TurnID || executed.RunID != admitted.RunID {
		t.Fatalf("executed = %#v, err = %v", executed, err)
	}
}

func TestPublishedOfficialProviderConstructor(t *testing.T) {
	gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
		Provider: "openai", Model: "gpt-4.1-mini", BaseURL: "https://api.openai.com/v1",
		APIKey: "test-only", StateCompatibilityKey: "openai:gpt-4.1-mini:chat-completions:v1",
		Capabilities: provider.Capabilities{
			Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors,
		},
	})
	if err != nil || gateway == nil {
		t.Fatalf("gateway = %T, err = %v", gateway, err)
	}
}

func adoptionAgent(t *testing.T, gateway provider.Gateway) *runtime.Agent {
	t.Helper()
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "adoption", Name: "Adoption Agent"},
		SystemPrompt: "Complete the requested task.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentSubAgentTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func scriptedGateway(responses ...string) *florettest.ScriptedGateway {
	steps := make([]florettest.Step, 0, len(responses))
	for _, response := range responses {
		steps = append(steps, florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: response},
			{Type: provider.EventDone, Reason: "stop"},
		}})
	}
	return florettest.NewScriptedGateway(
		provider.Identity{Provider: "adoption", Model: "scripted", StateCompatibilityKey: "adoption:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...,
	)
}
