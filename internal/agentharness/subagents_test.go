package agentharness

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage/sqlite"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

type canonicalTurnOnlyRepo struct {
	*sessiontree.MemoryRepo
}

func (r canonicalTurnOnlyRepo) Entries(context.Context, string) ([]sessiontree.Entry, error) {
	return nil, errors.New("full journal scan is forbidden")
}

func (r canonicalTurnOnlyRepo) Path(context.Context, string, string) ([]sessiontree.Entry, error) {
	return nil, errors.New("active path fallback is forbidden")
}

func TestReadTurnDetailEventsUsesCanonicalTurnIndex(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 20, 15, 30, 0, 0, time.UTC)
	memory := sessiontree.NewMemoryRepo()
	if _, err := memory.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		RequestFingerprint: "request", Input: session.Message{Role: session.User, Content: "inspect"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	harness := New(Options{Repo: canonicalTurnOnlyRepo{MemoryRepo: memory}, Now: func() time.Time { return now }})
	detail, found, err := harness.ReadTurnDetailEvents(ctx, "thread", "turn", "run", true)
	if err != nil || !found {
		t.Fatalf("ReadTurnDetailEvents found=%v err=%v", found, err)
	}
	user := firstSubAgentDetailEvent(detail.Events, SubAgentDetailEventUserMessage)
	if user.Message == nil || user.Message.Content != "inspect" {
		t.Fatalf("canonical user detail = %#v", user)
	}
}

func TestTurnAdmissionReplayIncludesPostTerminalSettlementAndAllAssistantText(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 20, 19, 0, 0, 0, time.UTC)
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		RequestFingerprint: "request", Input: session.Message{Role: session.User, Content: "inspect"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, admitted.Lease)
	for _, text := range []string{"first ", "second"} {
		if _, err := repo.Append(leaseCtx, sessiontree.Entry{
			ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryAssistantMessage,
			Message: session.Message{Role: session.Assistant, Content: text},
		}, sessiontree.AppendOptions{Now: now}); err != nil {
			t.Fatal(err)
		}
	}
	finished, err := repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "outcome", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	settlement, _, err := pendingToolSettlementAuthorityEntry("thread", PendingToolSettlement{
		TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "terminal", Handle: "terminal:job:1",
		Status: PendingToolSettledCompleted, Summary: "completed", Output: "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err = repo.Append(ctx, settlement, sessiontree.AppendOptions{ParentID: finished.Terminal.ID, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	replayedAdmission, found, err := repo.ReadTurnAdmission(ctx, "thread", "turn", "run")
	if err != nil || !found || replayedAdmission.Terminal == nil {
		t.Fatalf("replayed admission=%#v found=%v err=%v", replayedAdmission, found, err)
	}
	harness := New(Options{Repo: repo, Now: func() time.Time { return now }})
	result, err := harness.cacheThread("thread").turnAdmissionReplayResult(ctx, replayedAdmission, "turn", "run")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "first second" {
		t.Fatalf("replay result=%#v", result)
	}
	if !slices.ContainsFunc(result.CanonicalEvents, func(event SubAgentDetailEvent) bool {
		return event.ID == settlement.ID && event.Kind == SubAgentDetailEventToolResult
	}) {
		t.Fatalf("replay omitted post-terminal settlement %q: %#v", settlement.ID, result.CanonicalEvents)
	}
}

type countingSubAgentProvider struct {
	calls *atomic.Int64
}

func (p countingSubAgentProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	p.calls.Add(1)
	events := make(chan provider.StreamEvent, 2)
	events <- provider.StreamEvent{Type: provider.Delta, Text: "done"}
	events <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	close(events)
	return events, nil
}

func TestSubAgentLifecycleRunsChildThreadWithIsolatedPromptScope(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("child done"), scriptharness.Done()))
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, h.options.Repo, "parent", "seed", session.Message{Role: session.User, Content: "seed"}); err != nil {
		t.Fatal(err)
	}

	spawned, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID:  "parent",
		ParentTurnID:    "parent-turn",
		ThreadID:        "child",
		TaskName:        "Review API",
		TaskDescription: "Review the runtime API boundary.",
		Message:         "review the runtime API",
		HostProfileRef:  "reviewer",
		ForkMode:        SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if spawned.ThreadID != "child" || spawned.ParentThreadID != "parent" || spawned.Path != "/root/review_api" || spawned.TaskDescription != "Review the runtime API boundary." {
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
	if waited.Snapshots[0].TaskDescription != "Review the runtime API boundary." || waited.Snapshots[0].LastMessage != "child done" || waited.Snapshots[0].LatestTurnID == "" {
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
	if len(listed) != 1 || listed[0].ThreadID != "child" || listed[0].TaskDescription != "Review the runtime API boundary." || listed[0].HostProfileRef != "reviewer" {
		t.Fatalf("listed = %#v", listed)
	}
}

func TestPublishSubAgentPendingToolCompletionDefersExecutionToWait(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("child continued"), scriptharness.Done()))
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn",
		TaskName: "worker", AgentPath: "/root/worker", Lifecycle: sessiontree.ThreadLifecycleOpen,
	}); err != nil {
		t.Fatal(err)
	}
	appendPendingToolResultFixture(t, ctx, repo, "child", "turn-pending")
	opts := PublishSubAgentPendingToolCompletionOptions{
		InputRequestID: "completion-input-1", ParentThreadID: "parent", ChildThreadID: "child",
		Target: sessiontree.PendingToolSettlementTarget{
			ThreadID: "child", TurnID: "turn-pending", RunID: "run-1",
			ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
		},
		Status: PendingToolCompleted, Summary: "background work completed", Output: "ok",
		Message: "continue with the completed background result",
	}
	published, err := h.PublishSubAgentPendingToolCompletion(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if published.QueuedInputs != 1 || len(provider.Requests) != 0 {
		t.Fatalf("published snapshot=%#v provider requests=%#v", published, provider.Requests)
	}
	replayed, err := h.PublishSubAgentPendingToolCompletion(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.QueuedInputs != 1 || len(provider.Requests) != 0 {
		t.Fatalf("replayed snapshot=%#v provider requests=%#v", replayed, provider.Requests)
	}
	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusCompleted || len(provider.Requests) != 1 {
		t.Fatalf("waited=%#v provider requests=%#v", waited, provider.Requests)
	}
	request := provider.Requests[0]
	if request.ThreadID != "child" || request.Messages[len(request.Messages)-1].Content != "continue with the completed background result" {
		t.Fatalf("child continuation request = %#v", request)
	}
}

func TestClosedSubAgentRequestReplayUsesDurableLedgers(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "worker", AgentPath: "/root/worker",
	}); err != nil {
		t.Fatal(err)
	}
	appendPendingToolResultFixture(t, ctx, repo, "child", "turn-pending")

	input := SendSubAgentInputOptions{
		InputRequestID: "input-replay", ParentThreadID: "parent", ChildThreadID: "child", Message: "continue",
	}
	if _, err := h.SendSubAgentInput(ctx, input); err != nil {
		t.Fatal(err)
	}
	completion := PublishSubAgentPendingToolCompletionOptions{
		InputRequestID: "completion-replay", ParentThreadID: "parent", ChildThreadID: "child",
		Target: sessiontree.PendingToolSettlementTarget{
			ThreadID: "child", TurnID: "turn-pending", RunID: "run-1",
			ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
		},
		Status: PendingToolCompleted, Summary: "completed", Output: "ok", Message: "continue after completion",
	}
	if _, err := h.PublishSubAgentPendingToolCompletion(ctx, completion); err != nil {
		t.Fatal(err)
	}
	closed, err := h.CloseSubAgent(ctx, CloseSubAgentOptions{
		CloseOperationID: "close-child", ParentThreadID: "parent", ChildThreadID: "child", Reason: "done",
	})
	if err != nil || !closed.Closed {
		t.Fatalf("closed=%#v err=%v", closed, err)
	}

	if replayed, err := h.SendSubAgentInput(ctx, input); err != nil || !replayed.Closed {
		t.Fatalf("closed input replay=%#v err=%v", replayed, err)
	}
	changedInput := input
	changedInput.Message = "changed"
	if _, err := h.SendSubAgentInput(ctx, changedInput); !errors.Is(err, sessiontree.ErrSubAgentRequestConflict) || !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("changed closed input err=%v, want durable request conflict", err)
	}
	if replayed, err := h.PublishSubAgentPendingToolCompletion(ctx, completion); err != nil || !replayed.Closed {
		t.Fatalf("closed completion replay=%#v err=%v", replayed, err)
	}
	changedCompletion := completion
	changedCompletion.Output = "changed"
	if _, err := h.PublishSubAgentPendingToolCompletion(ctx, changedCompletion); !errors.Is(err, sessiontree.ErrSubAgentRequestConflict) || !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("changed closed completion err=%v, want durable request conflict", err)
	}
	newInput := input
	newInput.InputRequestID = "new-input"
	if _, err := h.SendSubAgentInput(ctx, newInput); !errors.Is(err, ErrSubAgentClosed) {
		t.Fatalf("new closed input err=%v, want ErrSubAgentClosed", err)
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
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "start",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		_, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second})
		waitDone <- err
	}()
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("child run did not start")
	}

	closed, err := h.CloseSubAgent(ctx, CloseSubAgentOptions{CloseOperationID: "close-child", ParentThreadID: "parent", ChildThreadID: "child", Reason: "test_close"})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != SubAgentStatusClosed || !closed.Closed || closed.CanSendInput || closed.CanClose {
		t.Fatalf("closed snapshot = %#v", closed)
	}
	if err := <-waitDone; err != nil {
		t.Fatal(err)
	}
	if _, err := h.SendSubAgentInput(ctx, SendSubAgentInputOptions{InputRequestID: "test-input-" + h.nextID("input"),
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		Message:        "keep going",
	}); !errors.Is(err, ErrSubAgentClosed) {
		t.Fatalf("send after close err = %v", err)
	}
}

func TestCloseSubAgentsStopsUnfinishedChildrenAndKeepsCompletedHistory(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Hang()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "completed",
		TaskName:       "completed",
		Message:        "finish",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"completed"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("completed wait=%#v err=%v", waited, err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "running",
		TaskName:       "running",
		Message:        "hang",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}

	result, err := h.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("close result = %#v", result)
	}
	byID := map[string]SubAgentSnapshot{}
	for _, snapshot := range result {
		byID[snapshot.ThreadID] = snapshot
	}
	if byID["completed"].Status != SubAgentStatusCompleted || byID["completed"].Closed {
		t.Fatalf("completed child should remain completed history: %#v", byID["completed"])
	}
	if _, err := h.CloseSubAgent(ctx, CloseSubAgentOptions{CloseOperationID: "close-running", ParentThreadID: "parent", ChildThreadID: "running", Reason: "parent_stop"}); err != nil {
		t.Fatal(err)
	}
	result, err = h.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range result {
		byID[snapshot.ThreadID] = snapshot
	}
	if byID["running"].Status != SubAgentStatusClosed || !byID["running"].Closed || byID["running"].CanClose {
		t.Fatalf("running child should be closed: %#v", byID["running"])
	}
	detail, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "running"})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, ev := range detail.Events {
		if ev.Type == subAgentLifecycleEntryKind && ev.Metadata[subAgentLifecycleReasonKey] == "parent_stop" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("running detail missing parent_stop lifecycle: %#v", detail.Events)
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
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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
	if _, err := h.SendSubAgentInput(ctx, SendSubAgentInputOptions{InputRequestID: "test-input-" + h.nextID("input"),
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
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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

func TestReadSubAgentDetailProjectsToolAndApprovalEvents(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(
			scriptharness.Text("checking"),
			scriptharness.Tool("write-1", "write_file", `{"value":"danger"}`),
			scriptharness.DoneReason("tool_calls"),
		),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(context.Context, EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error) {
		return EffectDispatchResult{}, ErrEffectUnauthorized
	})
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name:        "write_file",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
			Effects:     []tools.Effect{tools.EffectWrite},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "should not run"}, nil
		},
	))
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "attempt write",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusFailed {
		t.Fatalf("waited = %#v", waited)
	}
	detail, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Events) < 2 || detail.Events[0].Kind != SubAgentDetailEventTurnMarker || detail.Events[1].Kind != SubAgentDetailEventUserMessage || detail.Events[1].Message == nil || detail.Events[1].Message.Content != "attempt write" {
		t.Fatalf("detail should start at delegated input: %#v", detail.Events)
	}
	if detail.Events[0].ActivityTimeline == nil {
		t.Fatalf("input detail should have activity timeline: %#v", detail.Events[0])
	}
	if err := observation.ValidateActivityTimeline(*detail.Events[0].ActivityTimeline); err != nil {
		t.Fatalf("input activity timeline invalid: %v", err)
	}
	call := firstSubAgentDetailEvent(detail.Events, SubAgentDetailEventToolCall)
	if call.ToolCall == nil || call.Type != string(sessiontree.EntryToolCall) || call.ToolCall.Name != "write_file" || call.ToolCall.ArgsHash == "" {
		t.Fatalf("tool call detail = %#v", call)
	}
	if call.ActivityTimeline != nil {
		t.Fatalf("completed tool call row should not duplicate result activity: %#v", call.ActivityTimeline)
	}
	approval := firstSubAgentDetailEvent(detail.Events, SubAgentDetailEventApproval)
	if approval.Approval == nil || approval.Type != "tool_approval_requested" || approval.Approval.State != "requested" || approval.Approval.ToolName != "write_file" || approval.Approval.ArgsHash == "" {
		t.Fatalf("approval detail = %#v", approval)
	}
	if approval.ActivityTimeline == nil {
		t.Fatalf("approval detail should have activity timeline: %#v", approval)
	}
	if err := observation.ValidateActivityTimeline(*approval.ActivityTimeline); err != nil {
		t.Fatalf("approval activity timeline invalid: %v", err)
	}
	if len(approval.ActivityTimeline.Items) != 1 || approval.ActivityTimeline.Items[0].Status != observation.ActivityStatusWaiting {
		t.Fatalf("approval activity timeline = %#v", approval.ActivityTimeline)
	}
	if _, ok := approval.Approval.Metadata["resources"]; ok {
		t.Fatalf("approval detail must not expose raw resources: %#v", approval.Approval.Metadata)
	}
	result := firstSubAgentDetailEvent(detail.Events, SubAgentDetailEventToolResult)
	if result.ToolResult == nil || !strings.Contains(result.ToolResult.Content, ErrEffectUnauthorized.Error()) || result.ToolResult.Status != string(observation.ActivityStatusError) {
		t.Fatalf("tool result detail = %#v event=%#v", result.ToolResult, result)
	}
	if result.ActivityTimeline == nil {
		t.Fatalf("tool result detail should have activity timeline: %#v", result)
	}
	if err := observation.ValidateActivityTimeline(*result.ActivityTimeline); err != nil {
		t.Fatalf("tool result activity timeline invalid: %v", err)
	}
	if len(result.ActivityTimeline.Items) != 1 || result.ActivityTimeline.Items[0].Status != observation.ActivityStatusError {
		t.Fatalf("tool result activity timeline = %#v", result.ActivityTimeline)
	}
}

func TestReadSubAgentDetailRawMessageContentContract(t *testing.T) {
	ctx := context.Background()
	longMission := "inspect the complete handoff output " + strings.Repeat("mission context ", 80) + "mission tail"
	longAnswer := "complete subagent report " + strings.Repeat("evidence section ", 80) + "https://example.test/full-final-output"
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text(longAnswer), scriptharness.Done()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "raw contract",
		Message:        longMission,
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}

	previewOnly, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	inputPreview := firstSubAgentDetailEvent(previewOnly.Events, SubAgentDetailEventUserMessage)
	if inputPreview.Message == nil || inputPreview.Message.Content != "" || inputPreview.Message.Preview == "" || !strings.HasSuffix(inputPreview.Message.Preview, "...") {
		t.Fatalf("preview input should omit raw content and keep bounded preview: %#v", inputPreview)
	}
	if strings.Contains(inputPreview.Message.Preview, "mission tail") {
		t.Fatalf("preview input exposed tail raw content: %q", inputPreview.Message.Preview)
	}
	assistantPreview := firstSubAgentDetailEvent(previewOnly.Events, SubAgentDetailEventAssistantMessage)
	if assistantPreview.Message == nil || assistantPreview.Message.Content != "" || assistantPreview.Message.Preview == "" || !strings.HasSuffix(assistantPreview.Message.Preview, "...") {
		t.Fatalf("preview assistant should omit raw content and keep bounded preview: %#v", assistantPreview)
	}
	if strings.Contains(assistantPreview.Message.Preview, "full-final-output") {
		t.Fatalf("preview assistant exposed tail raw content: %q", assistantPreview.Message.Preview)
	}

	raw, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	inputRaw := firstSubAgentDetailEvent(raw.Events, SubAgentDetailEventUserMessage)
	if inputRaw.Message == nil || inputRaw.Message.Content != longMission || inputRaw.Message.Preview == "" || inputRaw.Message.Preview == inputRaw.Message.Content {
		t.Fatalf("raw input should keep full content and bounded preview: %#v", inputRaw)
	}
	assistantRaw := firstSubAgentDetailEvent(raw.Events, SubAgentDetailEventAssistantMessage)
	if assistantRaw.Message == nil || assistantRaw.Message.Content != longAnswer || assistantRaw.Message.Preview == "" || assistantRaw.Message.Preview == assistantRaw.Message.Content {
		t.Fatalf("raw assistant should keep full content and bounded preview: %#v", assistantRaw)
	}
}

func TestReadSubAgentDetailContextWindowComesFromModelPolicyNotForkMode(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("none done"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("fork done"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("changed done"), scriptharness.Done()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	h.options.TurnPolicy.ContextPolicy = contextpolicy.Policy{
		ContextWindowTokens:  512000,
		ReservedOutputTokens: 32000,
	}
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, h.options.Repo, "parent", "seed", session.Message{Role: session.User, Content: "seed"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		childID  string
		forkMode SubAgentForkMode
	}{
		{childID: "child-none", forkMode: SubAgentForkNone},
		{childID: "child-fork", forkMode: SubAgentForkFullPath},
	} {
		if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
			ParentThreadID: "parent",
			ThreadID:       tc.childID,
			TaskName:       tc.childID,
			Message:        "work",
			ForkMode:       tc.forkMode,
		}); err != nil {
			t.Fatalf("spawn %s: %v", tc.childID, err)
		}
		if waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{tc.childID}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
			t.Fatalf("wait %s: waited=%#v err=%v", tc.childID, waited, err)
		}
		detail, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: tc.childID})
		if err != nil {
			t.Fatalf("detail %s: %v", tc.childID, err)
		}
		if detail.Context.Policy.ContextWindowTokens != 512000 || detail.Context.Policy.ReservedOutputTokens != 32000 {
			t.Fatalf("%s context policy = %#v", tc.childID, detail.Context.Policy)
		}
		if detail.Context.Usage == nil || detail.Context.Usage.ContextPressure.ContextWindowTokens != 512000 {
			t.Fatalf("%s context usage = %#v", tc.childID, detail.Context.Usage)
		}
	}
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 768000
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "child-changed",
		TaskName:       "changed",
		Message:        "work changed",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child-changed"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("wait changed: waited=%#v err=%v", waited, err)
	}
	changed, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child-changed"})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Context.Policy.ContextWindowTokens != 768000 {
		t.Fatalf("changed context policy = %#v", changed.Context.Policy)
	}
}

func TestReadThreadContextFailsClosedOnInvalidJournalData(t *testing.T) {
	ctx := context.Background()
	validStatus := observation.ContextStatus{
		RunID:      "run-1",
		ThreadID:   "thread",
		TurnID:     "turn-1",
		Step:       1,
		Phase:      observation.ContextPhaseProjectedRequest,
		Provider:   "fake",
		Model:      "fake-model",
		ObservedAt: time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC),
		Status:     observation.ContextStatusStable,
	}
	validPolicy := subAgentContextPolicyMetadata("fake", "fake-model", contextpolicy.Policy{ContextWindowTokens: 256000})
	tests := []struct {
		name    string
		entries []sessiontree.Entry
		want    string
	}{
		{
			name: "malformed policy",
			entries: []sessiontree.Entry{{
				ThreadID: "thread",
				TurnID:   "turn-1",
				Type:     sessiontree.EntryCustom,
				Metadata: map[string]string{
					subAgentDetailKindKey:      subAgentContextPolicyEntryKind,
					subAgentContextProviderKey: "fake",
					subAgentContextModelKey:    "fake-model",
					subAgentContextPolicyKey:   "{",
				},
			}},
			want: "decode thread context policy",
		},
		{
			name: "malformed status",
			entries: []sessiontree.Entry{
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: validPolicy},
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: map[string]string{subAgentDetailKindKey: subAgentContextStatusEntryKind, subAgentContextStatusKey: "{"}},
			},
			want: "decode thread context status",
		},
		{
			name: "missing policy",
			entries: []sessiontree.Entry{{
				ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom,
				Metadata: map[string]string{subAgentDetailKindKey: subAgentContextStatusEntryKind, subAgentContextStatusKey: mustSubAgentMetadataJSON(validStatus)},
			}},
			want: "missing its policy",
		},
		{
			name: "thread identity mismatch",
			entries: []sessiontree.Entry{
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: validPolicy},
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: map[string]string{subAgentDetailKindKey: subAgentContextStatusEntryKind, subAgentContextStatusKey: mustSubAgentMetadataJSON(func() observation.ContextStatus { status := validStatus; status.ThreadID = "other"; return status }())}},
			},
			want: "status identity mismatch",
		},
		{
			name: "run identity mismatch",
			entries: []sessiontree.Entry{
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run-other"}},
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: validPolicy},
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: map[string]string{subAgentDetailKindKey: subAgentContextStatusEntryKind, subAgentContextStatusKey: mustSubAgentMetadataJSON(validStatus)}},
			},
			want: "run identity mismatch",
		},
		{
			name: "missing compaction operation",
			entries: []sessiontree.Entry{
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: validPolicy},
				{ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryCustom, Metadata: map[string]string{
					subAgentDetailKindKey: subAgentContextCompactionEntryKind,
					subAgentContextCompactionKey: mustSubAgentMetadataJSON(ThreadContextCompaction{
						RunID: "run-1", ThreadID: "thread", TurnID: "turn-1",
						Phase: string(observation.CompactionPhaseNoop), Status: string(observation.CompactionStatusNoop),
					}),
				}},
			},
			want: "requires operation id",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := sessiontree.NewMemoryRepo()
			h := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
			if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			for _, entry := range tc.entries {
				if _, err := repo.Append(ctx, entry, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := h.ReadThreadContext(ctx, "thread"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ReadThreadContext err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestReadSubAgentDetailEnforcesOwnershipAndPagination(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "other"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "work",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "other", ChildThreadID: "child"}); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("wrong parent err = %v", err)
	}
	defaultDetail, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if got := firstSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventUserMessage); got.Message == nil || got.Message.Content != "" || got.Message.Preview != "work" || got.Metadata[subAgentDetailRawOmitted] != "true" {
		t.Fatalf("default detail should expose only safe input preview: %#v", got)
	}
	first, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child", Limit: 1, IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 1 || first.NextOrdinal != first.Events[0].Ordinal || !first.HasMore {
		t.Fatalf("first page = %#v", first)
	}
	second, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child", AfterOrdinal: first.NextOrdinal, Limit: 1, IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Events) != 1 || second.Events[0].Ordinal <= first.NextOrdinal {
		t.Fatalf("second page skipped or repeated event: first=%#v second=%#v", first, second)
	}
}

func TestSubAgentRunTimeoutClampsToMaximum(t *testing.T) {
	h := newTestHarness(scriptharness.NewScriptedProvider(), sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	h.options.SubAgentRunTimeout = MaxSubAgentWaitTimeout + time.Hour
	ctx, cancel := h.subAgentRunContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("subagent run context should have a deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > MaxSubAgentWaitTimeout {
		t.Fatalf("subagent run timeout was not clamped: %s", remaining)
	}
}

func TestSubAgentRunTimeoutStopsExecutionAndKeepsDetailReadable(t *testing.T) {
	ctx := context.Background()
	provider := newBlockingProvider()
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	h.options.SubAgentRunTimeout = 20 * time.Millisecond
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "hang",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{
		ParentThreadID: "parent",
		ChildThreadIDs: []string{"child"},
		Timeout:        2 * time.Second,
	})
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("child run did not start")
	}
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusCancelled {
		t.Fatalf("waited = %#v", waited)
	}
	if !waited.Snapshots[0].CanSendInput || !waited.Snapshots[0].CanClose {
		t.Fatalf("timeout should stop the turn without closing the subagent: %#v", waited.Snapshots[0])
	}
	detail, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	marker := lastSubAgentDetailEvent(detail.Events, SubAgentDetailEventTurnMarker)
	if marker.TurnMarker == nil || marker.TurnMarker.Status == "" || marker.TurnMarker.Metadata[subAgentTerminalReasonKey] != subAgentRunTimeoutReason {
		t.Fatalf("timeout detail should retain terminal marker: %#v", detail.Events)
	}
}

func TestCloseSubAgentRecordsLifecycleDetail(t *testing.T) {
	ctx := context.Background()
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "work",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.CloseSubAgent(ctx, CloseSubAgentOptions{CloseOperationID: "close-child-detail", ParentThreadID: "parent", ChildThreadID: "child", Reason: "parent_close"}); err != nil {
		t.Fatal(err)
	}
	detail, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, ev := range detail.Events {
		if ev.Type == subAgentLifecycleEntryKind && ev.Metadata[subAgentLifecycleActionKey] == "closed" && ev.Metadata[subAgentLifecycleReasonKey] == "parent_close" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("close lifecycle detail missing: %#v", detail.Events)
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
		ID:              "child",
		ParentThreadID:  "parent",
		TaskName:        "worker",
		TaskDescription: "Continue the worker after restart.",
		AgentPath:       "/root/worker",
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}
	admission, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "child", TurnID: "turn-1", RunID: "run-1", OwnerID: "interrupted-owner",
		Input: session.Message{Role: session.User, Content: "finished input"}, RequestFingerprint: "completed-child-fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, admission.Lease)
	if _, err := repo.Append(leaseCtx, sessiontree.Entry{
		ThreadID: "child",
		TurnID:   "turn-1",
		Type:     sessiontree.EntryAssistantMessage,
		Message:  session.Message{Role: session.Assistant, Content: "done after restart"},
	}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(leaseCtx, sessiontree.FinishTurnRequest{
		Lease: admission.Lease, RunID: "run-1", TerminalEntryID: "terminal-restart", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "completed-child-outcome", Now: now,
	}); err != nil {
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
	if waited.Snapshots[0].TaskDescription != "Continue the worker after restart." || waited.Snapshots[0].LastMessage != "done after restart" {
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
			_, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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
		if _, err := h.CloseSubAgent(closeCtx, CloseSubAgentOptions{CloseOperationID: "close-" + item.ThreadID, ParentThreadID: "parent", ChildThreadID: item.ThreadID, Reason: "test_cleanup"}); err != nil {
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
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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
	if _, replayed, err := repo.PublishSubAgentInput(ctx, sessiontree.PublishSubAgentInputRequest{
		InputRequestID:     "input-second",
		RequestFingerprint: "input-second-fingerprint",
		ParentThreadID:     "parent",
		ChildThreadID:      "child",
		Message:            session.Message{Role: session.User, Content: "second"},
		HostLabels:         map[string]string{"role": "worker"},
		CorrelationLabels:  map[string]string{"parent": "parent"},
	}); err != nil {
		t.Fatal(err)
	} else if replayed {
		t.Fatal("first durable input publication unexpectedly replayed")
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

func TestSQLiteSubAgentAdmissionHasSingleLeaseOwnerAcrossHarnesses(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	firstStore, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstStore.Close() })
	secondStore, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	if _, err := firstStore.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	childMeta := sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "child", AgentPath: "/root/child", ForkMode: string(SubAgentForkNone)}
	published, err := firstStore.PublishSubAgent(ctx, sessiontree.PublishSubAgentRequest{
		PublicationID:      "publication-child",
		RequestFingerprint: "publication-child-fingerprint",
		ParentThreadID:     "parent",
		ChildMeta:          childMeta,
		Message:            session.Message{Role: session.User, Content: "work once"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	first := newTestHarness(countingSubAgentProvider{calls: &calls}, firstStore, firstStore)
	second := newTestHarness(countingSubAgentProvider{calls: &calls}, secondStore, secondStore)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, harness := range []*AgentHarness{first, second} {
		go func(h *AgentHarness) {
			<-start
			result, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: 2 * time.Second})
			if err == nil && result.TimedOut {
				err = errors.New("subagent wait timed out")
			}
			errs <- err
		}(harness)
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
	entries, err := firstStore.Entries(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	userEntries := make([]sessiontree.Entry, 0, 1)
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryUserMessage {
			userEntries = append(userEntries, entry)
		}
	}
	if len(userEntries) != 1 || userEntries[0].Metadata[subAgentAdmittedInputIDKey] != published.Input.SubAgentInputID {
		t.Fatalf("canonical user entries = %#v, want one admission for %q", userEntries, published.Input.SubAgentInputID)
	}
}

func TestSubAgentReadsDoNotActivateQueuedInputAfterRestart(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	now := time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PublishSubAgent(ctx, sessiontree.PublishSubAgentRequest{
		PublicationID:      "publication-child",
		RequestFingerprint: "publication-child-fingerprint",
		ParentThreadID:     "parent",
		ChildMeta: sessiontree.ThreadMeta{
			ID:             "child",
			ParentThreadID: "parent",
			TaskName:       "worker",
			AgentPath:      "/root/worker",
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		Message: session.Message{Role: session.User, Content: "queued work"},
		Now:     now,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := repo.Entries(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}

	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("unexpected"), scriptharness.Done()))
	restarted := newTestHarness(provider, repo, cache.NewMemoryStore())
	listed, err := restarted.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ThreadID != "child" || listed[0].QueuedInputs != 1 {
		t.Fatalf("listed = %#v", listed)
	}
	detail, err := restarted.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if detail.Snapshot.QueuedInputs != 1 {
		t.Fatalf("detail snapshot = %#v", detail.Snapshot)
	}
	after, err := repo.Entries(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("read changed journal entry count: before=%d after=%d", len(before), len(after))
	}
	for index := range before {
		if before[index].ID != after[index].ID || before[index].RawHash != after[index].RawHash {
			t.Fatalf("read changed journal entry %d: before=%#v after=%#v", index, before[index], after[index])
		}
	}
	if lease, active, err := repo.ActiveTurnLease(ctx, "child"); err != nil {
		t.Fatal(err)
	} else if active {
		t.Fatalf("read acquired child turn lease: %#v", lease)
	}
	restarted.mu.Lock()
	controllers := len(restarted.subagents)
	restarted.mu.Unlock()
	if controllers != 0 {
		t.Fatalf("read created %d subagent controllers", controllers)
	}
	if len(provider.Requests) != 0 {
		t.Fatalf("read called provider: %#v", provider.Requests)
	}
}

func TestAdmitSubAgentInputWritesCanonicalUserMessage(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	published, err := repo.PublishSubAgent(ctx, sessiontree.PublishSubAgentRequest{
		PublicationID:      "publication-child",
		RequestFingerprint: "publication-child-fingerprint",
		ParentThreadID:     "parent",
		ChildMeta: sessiontree.ThreadMeta{
			ID:             "child",
			ParentThreadID: "parent",
			TaskName:       "worker",
			AgentPath:      "/root/worker",
		},
		Message: session.Message{Role: session.User, Content: "canonical input"},
	})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitSubAgentInput(ctx, sessiontree.AdmitSubAgentInputRequest{
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		TurnID:         "turn-child",
		RunID:          "run-child",
		OwnerID:        "owner-child",
	})
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Input.State != sessiontree.SubAgentInputAdmitted || admitted.Input.SubAgentInputID != published.Input.SubAgentInputID {
		t.Fatalf("admitted input = %#v", admitted.Input)
	}
	if admitted.UserMessage.Type != sessiontree.EntryUserMessage || admitted.UserMessage.Message.Role != session.User || admitted.UserMessage.Message.Content != "canonical input" {
		t.Fatalf("canonical user message = %#v", admitted.UserMessage)
	}
	if admitted.UserMessage.Metadata[subAgentAdmittedInputIDKey] != published.Input.SubAgentInputID {
		t.Fatalf("canonical user message metadata = %#v", admitted.UserMessage.Metadata)
	}
	inputs, err := repo.ListSubAgentInputs(ctx, "child", sessiontree.SubAgentInputAdmitted)
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 1 || inputs[0].SubAgentInputID != published.Input.SubAgentInputID {
		t.Fatalf("admitted durable inputs = %#v", inputs)
	}
	if err := repo.ReleaseTurnLease(ctx, admitted.Lease); err != nil {
		t.Fatal(err)
	}
}

func firstSubAgentDetailEvent(events []SubAgentDetailEvent, kind SubAgentDetailEventKind) SubAgentDetailEvent {
	for _, event := range events {
		if event.Kind == kind {
			return event
		}
	}
	return SubAgentDetailEvent{}
}

func lastSubAgentDetailEvent(events []SubAgentDetailEvent, kind SubAgentDetailEventKind) SubAgentDetailEvent {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == kind {
			return events[i]
		}
	}
	return SubAgentDetailEvent{}
}

func TestCloseSubAgentCancelsDurablePendingInputs(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()))
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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
	input, replayed, err := repo.PublishSubAgentInput(ctx, sessiontree.PublishSubAgentInputRequest{
		InputRequestID:     "input-after-close",
		RequestFingerprint: "input-after-close-fingerprint",
		ParentThreadID:     "parent",
		ChildThreadID:      "child",
		Message:            session.Message{Role: session.User, Content: "after close"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed {
		t.Fatal("first durable input publication unexpectedly replayed")
	}
	if _, err := h.CloseSubAgent(ctx, CloseSubAgentOptions{CloseOperationID: "close-child-pending", ParentThreadID: "parent", ChildThreadID: "child", Reason: "test_close"}); err != nil {
		t.Fatal(err)
	}
	cancelled, err := repo.ListSubAgentInputs(ctx, "child", sessiontree.SubAgentInputCancelled)
	if err != nil {
		t.Fatal(err)
	}
	if len(cancelled) != 1 || cancelled[0].SubAgentInputID != input.SubAgentInputID {
		t.Fatalf("cancelled durable inputs = %#v, want %q", cancelled, input.SubAgentInputID)
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

func TestProcessLocalControllerFlagCannotCreateSubAgentCloseAuthority(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()))
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"),
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
	if _, err := h.SendSubAgentInput(ctx, SendSubAgentInputOptions{InputRequestID: "test-input-" + h.nextID("input"),
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		Message:        "do not run",
	}); err != nil {
		t.Fatalf("durable send was incorrectly fenced by process-local controller state: %v", err)
	}

	restarted := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("unexpected"), scriptharness.Done())), repo, cache.NewMemoryStore())
	listed, err := restarted.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].QueuedInputs != 1 || listed[0].Closed {
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
	if _, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "child", TurnID: "turn-1", RunID: "run-1", OwnerID: "interrupted-owner",
		Input: session.Message{Role: session.User, Content: "unfinished"}, RequestFingerprint: "interrupted-child-restart",
	}); err != nil {
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

func TestSubAgentOperationsRequireCanonicalParentThread(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
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
	if err := repo.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}

	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	assertParentMissing := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("%s err = %v, want ErrThreadNotFound", name, err)
		}
	}

	_, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{PublicationID: "test-publication-" + h.nextID("publication"), ParentThreadID: "parent", ThreadID: "new-child", TaskName: "new", Message: "work", ForkMode: SubAgentForkNone})
	assertParentMissing("SpawnSubAgent", err)
	_, err = h.SendSubAgentInput(ctx, SendSubAgentInputOptions{InputRequestID: "test-input-" + h.nextID("input"), ParentThreadID: "parent", ChildThreadID: "child", Message: "continue"})
	assertParentMissing("SendSubAgentInput", err)
	_, err = h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: time.Millisecond})
	assertParentMissing("WaitSubAgents", err)
	_, err = h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child"})
	assertParentMissing("ReadSubAgentDetail", err)
	_, err = h.ListSubAgents(ctx, "parent")
	assertParentMissing("ListSubAgents", err)
	_, err = h.CloseSubAgent(ctx, CloseSubAgentOptions{CloseOperationID: "close-missing", ParentThreadID: "parent", ChildThreadID: "child", Reason: "test"})
	assertParentMissing("CloseSubAgent", err)
}
