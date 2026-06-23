package agentharness

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
)

func TestSubAgentLifecycleRunsChildThreadWithIsolatedPromptScope(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("child done"), scriptharness.Done()))
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	spawned, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ParentTurnID:   "parent-turn",
		ThreadID:       "child",
		TaskName:       "Review API",
		Message:        "review the runtime API",
		HostProfileRef: "reviewer",
		ForkMode:       SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if spawned.ThreadID != "child" || spawned.ParentThreadID != "parent" || spawned.Path != "/root/review_api" {
		t.Fatalf("spawned snapshot = %#v", spawned)
	}

	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusCompleted {
		t.Fatalf("waited = %#v", waited)
	}
	if waited.Snapshots[0].LastMessage != "child done" || waited.Snapshots[0].LatestTurnID == "" {
		t.Fatalf("completed snapshot = %#v", waited.Snapshots[0])
	}
	if len(provider.Requests) != 1 {
		t.Fatalf("provider requests = %#v", provider.Requests)
	}
	req := provider.Requests[0]
	if req.ThreadID != "child" || req.PromptScopeID != "child" {
		t.Fatalf("child request identity = %#v", req)
	}

	listed, err := h.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ThreadID != "child" || listed[0].HostProfileRef != "reviewer" {
		t.Fatalf("listed = %#v", listed)
	}
}

func TestWaitSubAgentsRequiresAllTargetsToReachTerminalState(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("fast done"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Hang()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "fast",
		TaskName:       "fast",
		Message:        "finish",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"fast"},
		Timeout:        2 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "slow",
		TaskName:       "slow",
		Message:        "hang",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}

	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"fast", "slow"},
		Timeout:        50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !waited.TimedOut || len(waited.Snapshots) != 2 {
		t.Fatalf("waited = %#v", waited)
	}
	if waited.Snapshots[0].Status != SubAgentStatusCompleted || waited.Snapshots[1].Status != SubAgentStatusRunning {
		t.Fatalf("wait should not complete while any target is still running: %#v", waited.Snapshots)
	}
}

func TestCloseSubAgentCancelsChildAndRejectsFurtherInput(t *testing.T) {
	ctx := context.Background()
	provider := newBlockingProvider()
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "start",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("child run did not start")
	}

	closed, err := h.CloseSubAgent(ctx, CloseSubAgentOptions{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != SubAgentStatusClosed || !closed.Closed || closed.CanSendInput || closed.CanClose {
		t.Fatalf("closed snapshot = %#v", closed)
	}
	if _, err := h.SendSubAgentInput(ctx, SendSubAgentInputOptions{
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		Message:        "keep going",
	}); !errors.Is(err, ErrSubAgentClosed) {
		t.Fatalf("send after close err = %v", err)
	}
}

func TestWaitSubAgentsDoesNotCompleteWhileFollowUpInputIsQueued(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("first done"), scriptharness.Done()),
		scriptharness.Step(
			provider.StreamEvent{Type: provider.Delta, Reason: "150ms"},
			scriptharness.Text("second done"),
			scriptharness.Done(),
		),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "first",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        2 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SendSubAgentInput(ctx, SendSubAgentInputOptions{
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		Message:        "second",
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusRunning {
		t.Fatalf("wait should not complete while follow-up input is active: %#v", waited)
	}
}

func TestWaitSubAgentsReturnsWhenChildIsWaitingForInput(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Tool("ask", "ask_user", `{"question":"Need parent input?"}`), scriptharness.DoneReason("tool_calls")),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "blocked worker",
		Message:        "ask if blocked",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusWaiting {
		t.Fatalf("wait should return a waiting child for parent steering: %#v", waited)
	}
	if waited.Snapshots[0].WaitingPrompt != "Need parent input?" {
		t.Fatalf("waiting snapshot = %#v", waited.Snapshots[0])
	}
}

func TestSubAgentSnapshotUsesJournalAfterControllerRestart(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	now := time.Date(2026, 6, 23, 9, 0, 0, 0, time.UTC)
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID:        "parent",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID:             "child",
		ParentThreadID: "parent",
		TaskName:       "worker",
		AgentPath:      "/root/worker",
		Status:         string(SubAgentStatusRunning),
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "child", "turn-1", sessiontree.TurnStarted, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, sessiontree.Entry{
		ThreadID: "child",
		TurnID:   "turn-1",
		Type:     sessiontree.EntryAssistantMessage,
		Message:  session.Message{Role: session.Assistant, Content: "done after restart"},
	}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "child", "turn-1", sessiontree.TurnCompleted, nil); err != nil {
		t.Fatal(err)
	}

	restarted := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	waited, err := restarted.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusCompleted {
		t.Fatalf("waited = %#v", waited)
	}
	if waited.Snapshots[0].LastMessage != "done after restart" {
		t.Fatalf("snapshot = %#v", waited.Snapshots[0])
	}
}

func TestSpawnSubAgentAllowsConcurrentDuplicatePathWithDistinctThreadIDs(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Hang()),
		scriptharness.Step(scriptharness.Hang()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, childID := range []string{"child-a", "child-b"} {
		childID := childID
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
				ParentThreadID: "parent",
				ThreadID:       childID,
				TaskName:       "same task",
				Message:        "run",
				ForkMode:       SubAgentForkNone,
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		t.Fatalf("spawn duplicate path err = %v", err)
	}
	if successes != 2 {
		t.Fatalf("concurrent spawn successes=%d", successes)
	}
	listed, err := h.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Path != "/root/same_task" || listed[1].Path != "/root/same_task" || listed[0].ThreadID == listed[1].ThreadID {
		t.Fatalf("listed = %#v", listed)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, item := range listed {
		if _, err := h.CloseSubAgent(closeCtx, CloseSubAgentOptions{ParentThreadID: "parent", ChildThreadID: item.ThreadID}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQueuedSubAgentInputSurvivesHarnessRestart(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	firstProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("first done"), scriptharness.Done()))
	h := newTestHarness(firstProvider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "first",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.appendSubAgentInput(ctx, "child", "second", engine.RunLabels{
		Host:        map[string]string{"role": "worker"},
		Correlation: map[string]string{"parent": "parent"},
	}, false); err != nil {
		t.Fatal(err)
	}

	secondProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("second done"), scriptharness.Done()))
	restarted := newTestHarness(secondProvider, repo, cache.NewMemoryStore())
	waited, err := restarted.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusCompleted || waited.Snapshots[0].QueuedInputs != 0 {
		t.Fatalf("waited = %#v", waited)
	}
	if len(secondProvider.Requests) != 1 || secondProvider.Requests[0].Messages[len(secondProvider.Requests[0].Messages)-1].Content != "second" {
		t.Fatalf("provider requests = %#v", secondProvider.Requests)
	}
	if secondProvider.Requests[0].Labels.Host["role"] != "worker" || secondProvider.Requests[0].Labels.Correlation["parent"] != "parent" {
		t.Fatalf("labels = %#v", secondProvider.Requests[0].Labels)
	}
}

func TestConsumedSubAgentInputWithoutUserMessageIsRecoveredAfterRestart(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	firstProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("first done"), scriptharness.Done()))
	h := newTestHarness(firstProvider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "first",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	input, err := h.appendSubAgentInput(ctx, "child", "recover me", engine.RunLabels{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.appendSubAgentInputState(ctx, "child", input.entryID, subAgentInputStateConsumed, map[string]string{subAgentInputTurnIDKey: "turn-crashed"}); err != nil {
		t.Fatal(err)
	}

	secondProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("recovered"), scriptharness.Done()))
	restarted := newTestHarness(secondProvider, repo, cache.NewMemoryStore())
	waited, err := restarted.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(secondProvider.Requests) != 1 {
		t.Fatalf("waited=%#v requests=%#v", waited, secondProvider.Requests)
	}
	messages := secondProvider.Requests[0].Messages
	if len(messages) == 0 || messages[len(messages)-1].Content != "recover me" {
		t.Fatalf("recovered request messages = %#v", messages)
	}
}

func TestCloseSubAgentCancelsDurablePendingInputs(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()))
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "first",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	input, err := h.appendSubAgentInput(ctx, "child", "after close", engine.RunLabels{}, false)
	if err != nil {
		t.Fatal(err)
	}
	ctrl, err := h.ensureSubAgentController(ctx, sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", AgentPath: "/root/worker"}, h.cacheThread("child"))
	if err != nil {
		t.Fatal(err)
	}
	ctrl.mu.Lock()
	ctrl.queue = []subagentInput{input}
	ctrl.mu.Unlock()
	if _, err := h.CloseSubAgent(ctx, CloseSubAgentOptions{ParentThreadID: "parent", ChildThreadID: "child"}); err != nil {
		t.Fatal(err)
	}

	restarted := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("unexpected"), scriptharness.Done())), repo, cache.NewMemoryStore())
	listed, err := restarted.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].QueuedInputs != 0 || listed[0].Status != SubAgentStatusClosed {
		t.Fatalf("listed = %#v", listed)
	}
}

func TestSubAgentInputPendingIsCancelledWhenEnqueueFindsClosedChild(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()))
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "first",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	meta, err := h.resolveSubAgentMeta(ctx, "parent", "child")
	if err != nil {
		t.Fatal(err)
	}
	ctrl, err := h.ensureSubAgentController(ctx, meta, h.cacheThread("child"))
	if err != nil {
		t.Fatal(err)
	}
	ctrl.mu.Lock()
	ctrl.closed = true
	ctrl.mu.Unlock()
	if _, err := h.SendSubAgentInput(ctx, SendSubAgentInputOptions{
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		Message:        "do not run",
	}); !errors.Is(err, ErrSubAgentClosed) {
		t.Fatalf("send err = %v", err)
	}

	restarted := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("unexpected"), scriptharness.Done())), repo, cache.NewMemoryStore())
	listed, err := restarted.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].QueuedInputs != 0 {
		t.Fatalf("listed = %#v", listed)
	}
}

func TestWaitSubAgentsReturnsInterruptedChildAfterRestart(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	now := time.Date(2026, 6, 23, 9, 0, 0, 0, time.UTC)
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID:             "child",
		ParentThreadID: "parent",
		TaskName:       "worker",
		AgentPath:      "/root/worker",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "child", "turn-1", sessiontree.TurnStarted, nil); err != nil {
		t.Fatal(err)
	}

	restarted := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	if _, err := restarted.ResumeThread(ctx, "child", ResumeOptions{}); err != nil {
		t.Fatal(err)
	}
	waited, err := restarted.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusInterrupted {
		t.Fatalf("waited = %#v", waited)
	}
}
