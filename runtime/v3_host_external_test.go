package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/florettest"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
)

func TestV3SubscriptionMessageStrictJSON(t *testing.T) {
	valid := `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2}}`
	var message runtime.SubscriptionMessage
	if err := json.Unmarshal([]byte(valid), &message); err != nil {
		t.Fatal(err)
	}
	gap, ok := message.Gap()
	if !ok || gap.LastDeliveredRevision != 1 || gap.ResyncAtRevision != 2 {
		t.Fatalf("decoded gap = %#v", message)
	}
	encoded, err := json.Marshal(message)
	if err != nil || string(encoded) != valid {
		t.Fatalf("encoded gap = %s, err = %v", encoded, err)
	}

	for name, payload := range map[string]string{
		"unknown envelope field":   `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2},"extra":true}`,
		"duplicate envelope field": `{"type":"gap","type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2}}`,
		"unknown variant field":    `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2,"extra":true}}`,
		"duplicate variant field":  `{"type":"gap","value":{"last_delivered_revision":1,"last_delivered_revision":1,"resync_at_revision":2}}`,
		"trailing data":            `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2}} {}`,
		"unknown variant":          `{"type":"other","value":{}}`,
		"missing value":            `{"type":"gap"}`,
		"invalid gap":              `{"type":"gap","value":{"last_delivered_revision":2,"resync_at_revision":2}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(payload), &message); err == nil {
				t.Fatalf("accepted invalid subscription JSON: %s", payload)
			}
		})
	}
}

type deterministicIDs struct {
	threads []identity.ThreadID
	turns   []identity.TurnID
	runs    []identity.RunID
}

func (source *deterministicIDs) NewThreadID() (identity.ThreadID, error) {
	value := source.threads[0]
	source.threads = source.threads[1:]
	return value, nil
}

func (source *deterministicIDs) NewTurnID() (identity.TurnID, error) {
	value := source.turns[0]
	source.turns = source.turns[1:]
	return value, nil
}

func (source *deterministicIDs) NewRunID() (identity.RunID, error) {
	value := source.runs[0]
	source.runs = source.runs[1:]
	return value, nil
}

func TestV3HostAllocatesAndReplaysCanonicalIdentities(t *testing.T) {
	ctx := context.Background()
	ids := &deterministicIDs{
		threads: []identity.ThreadID{"thread-allocated"},
		turns:   []identity.TurnID{"turn-allocated", "turn-retry"},
		runs:    []identity.RunID{"run-allocated", "run-retry"},
	}
	path := filepath.Join(t.TempDir(), "floret.db")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}

	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
		LogicalRequestID: "create-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ThreadID != "thread-allocated" || created.Receipt.Replayed {
		t.Fatalf("create result = %#v", created)
	}
	replayed, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
		LogicalRequestID: "create-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ThreadID != created.ThreadID || !replayed.Receipt.Replayed {
		t.Fatalf("create replay = %#v", replayed)
	}

	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := thread.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision <= 0 || snapshot.Thread.ThroughOrdinal != 0 || snapshot.Thread.ID != created.ThreadID {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retried"}, {Type: provider.EventDone}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.Turns(agent)
	if err != nil {
		t.Fatal(err)
	}
	started, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request",
		UserMessage:      runtime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.TurnID != "turn-allocated" || started.RunID != "run-allocated" || started.Receipt.Replayed {
		t.Fatalf("start result = %#v", started)
	}
	replayedTurn, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request",
		UserMessage:      runtime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayedTurn.TurnID != started.TurnID || replayedTurn.RunID != started.RunID || !replayedTurn.Receipt.Replayed {
		t.Fatalf("turn replay = %#v", replayedTurn)
	}
	_, err = turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request",
		UserMessage:      runtime.TurnInput{Text: "changed"},
	})
	var conflict *runtime.RequestConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("conflicting replay error = %T %v", err, err)
	}
	retried, err := turns.RetryTurn(ctx, runtime.RetryTurnCommand{LogicalRequestID: "retry-request", Reason: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	if retried.TurnID != "turn-retry" || retried.RunID != "run-retry" || retried.Receipt.Replayed {
		t.Fatalf("retry result = %#v", retried)
	}
	replayedRetry, err := turns.RetryTurn(ctx, runtime.RetryTurnCommand{LogicalRequestID: "retry-request", Reason: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	if replayedRetry.TurnID != retried.TurnID || replayedRetry.RunID != retried.RunID || !replayedRetry.Receipt.Replayed {
		t.Fatalf("retry replay = %#v", replayedRetry)
	}

	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Snapshot(ctx); !errors.Is(err, runtime.ErrHostClosed) {
		t.Fatalf("snapshot after shutdown = %v", err)
	}

	restarted, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: &deterministicIDs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Shutdown(context.Background()) }()
	restartedCreate, err := restarted.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-request"})
	if err != nil {
		t.Fatal(err)
	}
	if restartedCreate.ThreadID != created.ThreadID || !restartedCreate.Receipt.Replayed {
		t.Fatalf("restart create replay = %#v", restartedCreate)
	}
	restartedThread, err := restarted.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	restartedGateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
	)
	restartedAgent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, restartedGateway)
	if err != nil {
		t.Fatal(err)
	}
	restartedTurns, err := restartedThread.Turns(restartedAgent)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := restartedTurns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request", UserMessage: runtime.TurnInput{Text: "hello"},
	}); err != nil || result.TurnID != started.TurnID || !result.Receipt.Replayed {
		t.Fatalf("restart turn replay = %#v err=%v", result, err)
	}
	if result, err := restartedTurns.RetryTurn(ctx, runtime.RetryTurnCommand{
		LogicalRequestID: "retry-request", Reason: "retry",
	}); err != nil || result.TurnID != retried.TurnID || !result.Receipt.Replayed {
		t.Fatalf("restart retry replay = %#v err=%v", result, err)
	}
}

func TestV3SubAgentMutationsReplayAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	ids := &deterministicIDs{threads: []identity.ThreadID{"parent-thread", "child-thread"}}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-parent"})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "spawned"}, {Type: provider.EventDone}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "interrupted"}, {Type: provider.EventDone}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := parent.SubAgents(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	spawnCommand := runtime.SpawnSubAgentCommand{
		LogicalRequestID: "spawn-child", TaskName: "worker", Input: runtime.TurnInput{Text: "start"},
		ForkMode: runtime.SubAgentForkNone,
	}
	spawned, err := subAgents.SpawnSubAgent(ctx, spawnCommand)
	if err != nil {
		t.Fatal(err)
	}
	if spawned.Child.ThreadID != "child-thread" || spawned.Receipt.Replayed || spawned.Receipt.ThreadID != "child-thread" {
		t.Fatalf("spawn result = %#v", spawned)
	}
	waitForV3SubAgentStatus(t, ctx, subAgents, spawned.Child.ThreadID, runtime.SubAgentStatusCompleted)
	child, err := parent.Child(ctx, spawned.Child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if child.ID() != spawned.Child.ThreadID {
		t.Fatalf("child id = %q, want %q", child.ID(), spawned.Child.ThreadID)
	}
	detail, err := child.ReadDetail(ctx, runtime.ThreadDetailRequest{IncludeRaw: true})
	if err != nil || detail.Snapshot.ThreadID != spawned.Child.ThreadID {
		t.Fatalf("child detail = %#v err=%v", detail, err)
	}
	if targets, err := child.ListPendingToolTargets(ctx); err != nil || len(targets) != 0 {
		t.Fatalf("child pending targets = %#v err=%v", targets, err)
	}
	descendant, err := parent.DescendantReader(ctx, spawned.Child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	page, err := descendant.ListTurns(ctx, runtime.ThreadTurnsRequest{Tail: 10})
	if err != nil || len(page.Turns) != 1 {
		t.Fatalf("descendant turns = %#v err=%v", page, err)
	}
	turn, err := descendant.ReadTurn(ctx, page.Turns[0].TurnID)
	if err != nil || turn.RunID != page.Turns[0].RunID {
		t.Fatalf("descendant turn = %#v err=%v", turn, err)
	}
	if _, err := descendant.ReadTurn(ctx, "missing-turn"); !errors.Is(err, runtime.ErrTurnNotFound) {
		t.Fatalf("missing descendant turn error = %v", err)
	}
	if _, err := descendant.ReadArtifact(ctx, "missing-artifact"); !errors.Is(err, runtime.ErrArtifactNotFound) {
		t.Fatalf("missing descendant artifact error = %v", err)
	}
	if _, err := parent.Child(ctx, parent.ID()); !errors.Is(err, runtime.ErrSubAgentNotFound) {
		t.Fatalf("parent bound as child error = %v", err)
	}
	spawnReplay, err := subAgents.SpawnSubAgent(ctx, spawnCommand)
	if err != nil || !spawnReplay.Receipt.Replayed || spawnReplay.Child.ThreadID != spawned.Child.ThreadID {
		t.Fatalf("spawn replay = %#v err=%v", spawnReplay, err)
	}
	changedSpawn := spawnCommand
	changedSpawn.TaskDescription = "changed"
	if _, err := subAgents.SpawnSubAgent(ctx, changedSpawn); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed spawn replay = %v", err)
	}

	sendCommand := runtime.SendSubAgentMessageCommand{
		LogicalRequestID: "send-child", ChildThreadID: spawned.Child.ThreadID, Input: runtime.TurnInput{Text: "continue"},
	}
	sent, err := subAgents.SendSubAgentMessage(ctx, sendCommand)
	if err != nil {
		t.Fatal(err)
	}
	waitForV3SubAgentStatus(t, ctx, subAgents, spawned.Child.ThreadID, runtime.SubAgentStatusCompleted)
	sendReplay, err := subAgents.SendSubAgentMessage(ctx, sendCommand)
	if err != nil || !sendReplay.Receipt.Replayed || sendReplay.Child.ThreadID != sent.Child.ThreadID {
		t.Fatalf("send replay = %#v err=%v", sendReplay, err)
	}
	changedSend := sendCommand
	changedSend.Input.Text = "changed"
	if _, err := subAgents.SendSubAgentMessage(ctx, changedSend); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed send replay = %v", err)
	}

	interruptCommand := runtime.InterruptSubAgentCommand{
		LogicalRequestID: "interrupt-child", ChildThreadID: spawned.Child.ThreadID, Input: runtime.TurnInput{Text: "stop and reconsider"},
	}
	interrupted, err := subAgents.InterruptSubAgent(ctx, interruptCommand)
	if err != nil {
		t.Fatal(err)
	}
	waitForV3SubAgentStatus(t, ctx, subAgents, spawned.Child.ThreadID, runtime.SubAgentStatusCompleted)
	interruptReplay, err := subAgents.InterruptSubAgent(ctx, interruptCommand)
	if err != nil || !interruptReplay.Receipt.Replayed || interruptReplay.Child.ThreadID != interrupted.Child.ThreadID {
		t.Fatalf("interrupt replay = %#v err=%v", interruptReplay, err)
	}
	changedInterrupt := interruptCommand
	changedInterrupt.Input.Text = "changed"
	if _, err := subAgents.InterruptSubAgent(ctx, changedInterrupt); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed interrupt replay = %v", err)
	}

	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := subAgents.SendSubAgentMessage(ctx, runtime.SendSubAgentMessageCommand{
		LogicalRequestID: "after-shutdown", ChildThreadID: spawned.Child.ThreadID, Input: runtime.TurnInput{Text: "late"},
	}); !errors.Is(err, runtime.ErrHostClosed) {
		t.Fatalf("send after shutdown = %v", err)
	}

	restarted, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: &deterministicIDs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Shutdown(context.Background()) }()
	restartedParent, err := restarted.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	restartedGateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
	)
	restartedAgent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, restartedGateway)
	if err != nil {
		t.Fatal(err)
	}
	restartedSubAgents, err := restartedParent.SubAgents(ctx, restartedAgent)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := restartedSubAgents.SpawnSubAgent(ctx, spawnCommand); err != nil || !result.Receipt.Replayed || result.Child.ThreadID != spawned.Child.ThreadID {
		t.Fatalf("restart spawn replay = %#v err=%v", result, err)
	}
	if result, err := restartedSubAgents.SendSubAgentMessage(ctx, sendCommand); err != nil || !result.Receipt.Replayed || result.Child.ThreadID != sent.Child.ThreadID {
		t.Fatalf("restart send replay = %#v err=%v", result, err)
	}
	if result, err := restartedSubAgents.InterruptSubAgent(ctx, interruptCommand); err != nil || !result.Receipt.Replayed || result.Child.ThreadID != interrupted.Child.ThreadID {
		t.Fatalf("restart interrupt replay = %#v err=%v", result, err)
	}
}

func waitForV3SubAgentStatus(t *testing.T, ctx context.Context, subAgents *runtime.SubAgents, childThreadID identity.ThreadID, want runtime.SubAgentStatus) {
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

func TestV3SubscriptionSuspendsWithOneGapOnTransientOverflow(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.SQLite(filepath.Join(t.TempDir(), "floret.db")),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-gap"},
			turns:   []identity.TurnID{"turn-gap"},
			runs:    []identity.RunID{"run-gap"},
		},
		SubscriptionBuffer: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-gap"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := thread.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := thread.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: view.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "one"},
			{Type: provider.EventDelta, Text: "two"},
			{Type: provider.EventDelta, Text: "three"},
			{Type: provider.EventDone},
		}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.Turns(agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-gap", UserMessage: runtime.TurnInput{Text: "hello"},
	}); err != nil {
		t.Fatal(err)
	}
	message, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gap, ok := message.Gap()
	if !ok || message.Kind() != runtime.SubscriptionMessageGap ||
		gap.LastDeliveredRevision != view.Revision || gap.ResyncAtRevision <= view.Revision {
		t.Fatalf("overflow message = %#v", message)
	}
	if _, err := subscription.Next(ctx); !errors.Is(err, runtime.ErrSubscriptionStale) {
		t.Fatalf("next after gap = %v", err)
	}
}

func TestV3SubscriptionGapFreezesResyncRevisionAtOverflow(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.Memory(),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-gap-freeze"},
			turns:   []identity.TurnID{"turn-gap-first"},
			runs:    []identity.RunID{"run-gap-first"},
		},
		SubscriptionBuffer: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-gap-freeze"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := thread.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := thread.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: initial.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "one"},
			{Type: provider.EventDelta, Text: "two"},
			{Type: provider.EventDone},
		}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.Turns(agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "gap-first", UserMessage: runtime.TurnInput{Text: "first"},
	}); err != nil {
		t.Fatal(err)
	}
	atTurnEnd, err := thread.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := thread.DeleteThread(ctx, runtime.DeleteThreadCommand{LogicalRequestID: "gap-delete"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Receipt.Revision <= atTurnEnd.Revision {
		t.Fatalf("delete did not advance revision: turn_end=%d delete=%d", atTurnEnd.Revision, deleted.Receipt.Revision)
	}
	message, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gap, ok := message.Gap()
	if !ok || gap.ResyncAtRevision <= initial.Revision || gap.ResyncAtRevision > atTurnEnd.Revision ||
		gap.ResyncAtRevision >= deleted.Receipt.Revision {
		t.Fatalf("gap = %#v, want overflow revision after %d and no later than turn end %d, before delete %d",
			gap, initial.Revision, atTurnEnd.Revision, deleted.Receipt.Revision)
	}
	if _, err := subscription.Next(ctx); !errors.Is(err, runtime.ErrSubscriptionStale) {
		t.Fatalf("next after frozen gap = %v", err)
	}
}

func TestV3ForkAndDeleteReplayUseFloretOwnedTombstones(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage:  storage.SQLite(filepath.Join(t.TempDir(), "floret.db")),
		IDSource: &deterministicIDs{threads: []identity.ThreadID{"thread-source", "thread-fork"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-source"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	forked, err := thread.ForkThread(ctx, runtime.ForkThreadCommand{LogicalRequestID: "fork-request"})
	if err != nil {
		t.Fatal(err)
	}
	if forked.ThreadID != "thread-fork" || forked.Receipt.Replayed {
		t.Fatalf("fork result = %#v", forked)
	}
	forkReplay, err := thread.ForkThread(ctx, runtime.ForkThreadCommand{LogicalRequestID: "fork-request"})
	if err != nil || forkReplay.ThreadID != forked.ThreadID || !forkReplay.Receipt.Replayed {
		t.Fatalf("fork replay = %#v err=%v", forkReplay, err)
	}

	destination, err := host.Thread(ctx, forked.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := destination.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := destination.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: view.Revision})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := destination.DeleteThread(ctx, runtime.DeleteThreadCommand{LogicalRequestID: "delete-request"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Receipt.Revision <= view.Revision || deleted.Receipt.Replayed {
		t.Fatalf("delete result = %#v", deleted)
	}
	message, err := subscription.Next(ctx)
	durable, durableOK := message.Durable()
	deletedEvent, deletedOK := durable.Deleted()
	_, changeOK := durable.Change()
	if err != nil || !durableOK || !deletedOK || changeOK ||
		durable.Revision() != deleted.Receipt.Revision || deletedEvent.ThreadID != forked.ThreadID {
		t.Fatalf("deleted subscription message = %#v err=%v", message, err)
	}
	if _, err := subscription.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription after Deleted = %v", err)
	}

	tombstoned, err := host.Thread(ctx, forked.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tombstoned.Snapshot(ctx); !errors.Is(err, runtime.ErrThreadDeleted) {
		t.Fatalf("tombstone snapshot = %v", err)
	}
	replayedDelete, err := tombstoned.DeleteThread(ctx, runtime.DeleteThreadCommand{LogicalRequestID: "delete-request"})
	if err != nil || !replayedDelete.Receipt.Replayed || replayedDelete.Receipt.Revision != deleted.Receipt.Revision {
		t.Fatalf("delete replay = %#v err=%v", replayedDelete, err)
	}
	replaySubscription, err := tombstoned.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: view.Revision})
	if err != nil {
		t.Fatal(err)
	}
	replayedMessage, err := replaySubscription.Next(ctx)
	replayedDurable, durableOK := replayedMessage.Durable()
	_, deletedOK = replayedDurable.Deleted()
	if err != nil || !durableOK || !deletedOK {
		t.Fatalf("new subscription Deleted replay = %#v err=%v", replayedMessage, err)
	}
	eofSubscription, err := tombstoned.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: deleted.Receipt.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eofSubscription.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("new subscription after final revision = %v", err)
	}
}
