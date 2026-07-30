// Package adoption_test exercises the released v3 API from a blank module.
package adoption_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/florettest"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
)

func TestPublishedMemoryHostAndSubAgent(t *testing.T) {
	ctx := context.Background()
	gateway := scriptedGateway("parent done", "child done")
	agent := adoptionAgent(t, gateway)
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	creator, err := host.ThreadCreator("root", "create-root")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.Create(ctx); err != nil {
		t.Fatal(err)
	}
	runner, err := host.TurnRunner(ctx, "root", agent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, runtime.TurnRequest{
		RunID: "root-run", TurnID: "root-turn", Input: runtime.TurnInput{Text: "coordinate"},
	})
	if err != nil || result.Output != "parent done" {
		t.Fatalf("root turn = %#v, err = %v", result, err)
	}
	manager, err := host.SubAgentManager(ctx, "root", agent)
	if err != nil {
		t.Fatal(err)
	}
	child, err := manager.Spawn(ctx, runtime.SpawnSubAgent{
		PublicationID: "publish-child", ParentTurnID: "root-turn", ThreadID: "child",
		TaskName: "child", Message: "complete the delegated work", ForkMode: runtime.SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	waited, err := manager.Wait(ctx, runtime.WaitSubAgents{ChildThreadIDs: []runtime.ThreadID{child.ThreadID}, Timeout: 2 * time.Second})
	if err != nil || waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].LastMessage != "child done" {
		t.Fatalf("child wait = %#v, err = %v", waited, err)
	}
	reader, err := host.SubAgentReader(ctx, "root")
	if err != nil {
		t.Fatal(err)
	}
	children, err := reader.List(ctx)
	if err != nil || len(children) != 1 || children[0].ThreadID != "child" {
		t.Fatalf("children = %#v, err = %v", children, err)
	}
	turns, err := reader.ListTurns(ctx, child.ThreadID, runtime.ThreadTurnsRequest{Tail: 10})
	if err != nil || len(turns.Turns) != 1 || turns.Turns[0].UserMessageOrigin != runtime.ThreadUserMessageOriginDelegatedMission {
		t.Fatalf("child turns = %#v, err = %v", turns, err)
	}
	turn, err := reader.ReadTurn(ctx, child.ThreadID, turns.Turns[0].TurnID)
	if err != nil || turn.RunID != turns.Turns[0].RunID {
		t.Fatalf("child turn = %#v, err = %v", turn, err)
	}
}

func TestPublishedSQLiteRestartUsesCanonicalRead(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	creator, err := host.ThreadCreator("thread", "create-thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.Create(ctx); err != nil {
		t.Fatal(err)
	}
	runner, err := host.TurnRunner(ctx, "thread", adoptionAgent(t, scriptedGateway("persisted")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, runtime.TurnRequest{RunID: "run", TurnID: "turn", Input: runtime.TurnInput{Text: "persist"}}); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reader, err := reopened.ThreadReader(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	overview, err := reader.ReadOverview(ctx)
	if err != nil || overview.LatestTurn == nil || overview.LatestTurn.TurnID != "turn" {
		t.Fatalf("overview = %#v, err = %v", overview, err)
	}
	turn, err := reader.ReadTurn(ctx, "turn")
	if err != nil || turn.RunID != "run" || assistantText(turn.Projection) != "persisted" {
		t.Fatalf("canonical turn = %#v, err = %v", turn, err)
	}
	if _, err := reader.ReadTurn(ctx, "missing"); !errors.Is(err, runtime.ErrTurnNotFound) {
		t.Fatalf("missing turn error = %v", err)
	}
}

func TestPublishedBackendContract(t *testing.T) {
	florettest.RunBackendContract(t, storage.Memory())
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

func assistantText(projection runtime.ThreadTurnProjection) string {
	var text string
	for _, segment := range projection.Segments {
		if segment.Kind == runtime.ThreadTurnProjectionSegmentAssistantText {
			text += segment.Text
		}
	}
	return text
}

// compileRecoverySurface keeps the exact recovery constructors in the blank
// module gate without fabricating non-canonical durable targets.
func compileRecoverySurface(ctx context.Context, host *runtime.Host, pending runtime.PendingToolRecoveryTarget, interrupted runtime.InterruptedTurnRecoveryTarget) {
	_, _ = host.PendingToolRecovery(ctx, pending, nil)
	_, _ = host.InterruptedTurnRecovery(ctx, interrupted, nil)
}
