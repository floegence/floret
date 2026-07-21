package agentharness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/tools"
)

func TestEffectApprovalWaitReturnsCanonicalCancellationDeterministically(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	repo, err := sessiontree.NewMemoryRepoWithLeasePolicy(sessiontree.DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admission", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	item := sessiontree.ApprovalPreflightItem{
		EffectRequestFingerprint: "effect-request", ApprovalRequestFingerprint: "approval-request",
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "write_file", ArgumentHash: "arguments",
		},
		ToolKind: "local", Step: 1, BatchSize: 1, Effects: []string{"write"}, Destructive: true,
	}
	item.EffectAttemptID = sessiontree.ApprovalEffectAttemptID(item.Invocation)
	item.RequestedEntry = sessiontree.Entry{
		ID: sessiontree.ApprovalRequestedEntryID(item.EffectAttemptID), ThreadID: "thread", TurnID: "turn",
		Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "requested"},
	}
	prepared, err := repo.PrepareApprovalBatch(ctx, sessiontree.PrepareApprovalBatchRequest{
		Lease: admitted.Lease, Items: []sessiontree.ApprovalPreflightItem{item}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CancelApprovalBatch(ctx, sessiontree.CancelApprovalBatchRequest{
		Lease: admitted.Lease, RunID: "run",
		CancellationFingerprint: approvalBatchCancellationFingerprint(admitted.Lease, "run", "turn_cancelled"), Now: now,
	}); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name string
		ctx  func() context.Context
	}{
		{name: "caller already cancelled", ctx: func() context.Context {
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			return cancelled
		}},
		{name: "canonical cancellation wins first", ctx: context.Background},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			approval := &effectApproval{
				authority: repo, queue: prepared.Queue, record: prepared.Approvals[0],
				now: func() time.Time { return now },
			}
			receipt, err := approval.wait(testCase.ctx())
			if !errors.Is(err, context.Canceled) || receipt != (sessiontree.ApprovalDecisionReceipt{}) {
				t.Fatalf("receipt=%#v err=%v, want zero receipt and context.Canceled", receipt, err)
			}
		})
	}
}

func TestApprovalApprovedEventFailurePreventsHandlerDispatch(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	repo := &approvalEventFailureRepo{MemoryRepo: base, failApprovalState: "approved", failApprovalAt: 1}
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("write-1", "write_file", `{"value":"notes.md"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("recovered"), harness.Done()),
	)
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	var handlers atomic.Int32
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "write_file", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}, Effects: []tools.Effect{tools.EffectWrite},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			handlers.Add(1)
			return tools.Result{Text: "unexpected"}, nil
		},
	))
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	type turnOutcome struct {
		result TurnResult
		err    error
	}
	done := make(chan turnOutcome, 1)
	go func() {
		result, err := thread.Run(ctx, "write", RunOptions{RunID: "run", TurnID: "turn"})
		done <- turnOutcome{result: result, err: err}
	}()
	queue := waitForApprovalQueue(t, ctx, h, "thread")
	pending := queue.Approvals[0]
	if _, err := h.ResolveApproval(ctx, ResolveApprovalOptions{
		DecisionID: "decision", ExpectedRootThreadID: queue.RootThreadID, ExpectedGeneration: queue.Generation,
		ExpectedRevision: queue.Revision, ExpectedCurrent: sessiontree.ApprovalIdentity{
			ApprovalID: pending.ApprovalID, ThreadID: pending.ThreadID, TurnID: pending.TurnID, RunID: pending.RunID,
			ToolCallID: pending.ToolCallID, EffectAttemptID: pending.EffectAttemptID,
		},
		ExpectedApprovalRevision: pending.Revision, Decision: sessiontree.ApprovalDecisionApprove,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-done:
		if outcome.err == nil || outcome.result.Status != engine.Failed || !strings.Contains(outcome.err.Error(), "injected approved entry persistence failure") {
			t.Fatalf("result=%#v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("turn did not finish after approved detail failure")
	}
	if handlers.Load() != 0 {
		t.Fatalf("handler calls=%d, want 0", handlers.Load())
	}
	approval, err := base.Approval(ctx, pending.ApprovalID)
	if err != nil || approval.State != sessiontree.ApprovalFailed || approval.Reason != sessiontree.ApprovalReasonAuthorizationUnavailable {
		t.Fatalf("approval=%#v err=%v", approval, err)
	}
	detail, found, err := h.ReadTurnDetailEvents(ctx, "thread", "turn", "run", true)
	if err != nil || !found {
		t.Fatalf("detail found=%v err=%v", found, err)
	}
	failedDetails := 0
	for _, item := range detail.Events {
		if item.Kind == SubAgentDetailEventApproval && item.Approval != nil && item.Approval.State == "approved" {
			t.Fatalf("failed approved persistence exposed approved detail: %#v", item)
		}
		if item.Kind == SubAgentDetailEventApproval && item.Approval != nil && item.Approval.State == "failed" &&
			item.Approval.Reason == sessiontree.ApprovalReasonAuthorizationUnavailable {
			failedDetails++
		}
	}
	if failedDetails != 1 {
		t.Fatalf("failed approval details=%d, want exactly one: %#v", failedDetails, detail.Events)
	}
}

func TestApprovalDecisionNotifiesDifferentHarness(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("write-1", "write_file", `{"value":"notes.md"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("completed"), harness.Done()),
	)
	runner := newTestHarness(provider, repo, cache.NewMemoryStore())
	resolver := newTestHarness(harness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	var handlers atomic.Int32
	mustRegister(runner.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "write_file", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}, Effects: []tools.Effect{tools.EffectWrite},
		}, nil, nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			handlers.Add(1)
			return tools.Result{Text: "written"}, nil
		},
	))
	thread, err := runner.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result TurnResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := thread.Run(ctx, "write", RunOptions{RunID: "run", TurnID: "turn"})
		done <- outcome{result: result, err: err}
	}()
	queue := waitForApprovalQueue(t, ctx, resolver, "thread")
	pending := queue.Approvals[0]
	if _, err := resolver.ResolveApproval(ctx, ResolveApprovalOptions{
		DecisionID: "decision", ExpectedRootThreadID: queue.RootThreadID, ExpectedGeneration: queue.Generation,
		ExpectedRevision: queue.Revision, ExpectedCurrent: sessiontree.ApprovalIdentity{
			ApprovalID: pending.ApprovalID, ThreadID: pending.ThreadID, TurnID: pending.TurnID, RunID: pending.RunID,
			ToolCallID: pending.ToolCallID, EffectAttemptID: pending.EffectAttemptID,
		},
		ExpectedApprovalRevision: pending.Revision, Decision: sessiontree.ApprovalDecisionApprove,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.result.Status != engine.Completed || got.result.Output != "completed" || handlers.Load() != 1 {
			t.Fatalf("result=%#v err=%v handlers=%d", got.result, got.err, handlers.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner harness did not observe resolver harness decision")
	}
}

func TestApprovalDispatchReplayDoesNotRepeatApprovedEventOrHandler(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	repo := &approvalEventFailureRepo{MemoryRepo: base, forceCommitReplay: true}
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("write-1", "write_file", `{"value":"notes.md"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	var handlers atomic.Int32
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "write_file", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}, Effects: []tools.Effect{tools.EffectWrite},
		}, nil, nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			handlers.Add(1)
			return tools.Result{Text: "unexpected"}, nil
		},
	))
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(ctx, "write", RunOptions{RunID: "run", TurnID: "turn"})
		done <- err
	}()
	queue := waitForApprovalQueue(t, ctx, h, "thread")
	pending := queue.Approvals[0]
	if _, err := h.ResolveApproval(ctx, ResolveApprovalOptions{
		DecisionID: "decision", ExpectedRootThreadID: queue.RootThreadID, ExpectedGeneration: queue.Generation,
		ExpectedRevision: queue.Revision, ExpectedCurrent: sessiontree.ApprovalIdentity{
			ApprovalID: pending.ApprovalID, ThreadID: pending.ThreadID, TurnID: pending.TurnID, RunID: pending.RunID,
			ToolCallID: pending.ToolCallID, EffectAttemptID: pending.EffectAttemptID,
		}, ExpectedApprovalRevision: pending.Revision, Decision: sessiontree.ApprovalDecisionApprove,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, sessiontree.ErrEffectOutcomeUnknown) {
			t.Fatalf("run err=%v, want committed replay failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replayed dispatch did not return")
	}
	if handlers.Load() != 0 {
		t.Fatalf("handler calls=%d, want 0", handlers.Load())
	}
	if repo.markUnknownCalls.Load() != 1 {
		t.Fatalf("mark unknown calls=%d, want 1", repo.markUnknownCalls.Load())
	}
	if state, _ := repo.markedUnknownState.Load().(string); state != string(sessiontree.EffectAttemptUnknown) {
		t.Fatalf("marked effect state=%q, want unknown", state)
	}
	entries, err := base.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	approved := 0
	terminalMarkers := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentApprovalStateKey] == "approved" {
			approved++
		}
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn" && isTerminalTurnMarker(entry.TurnStatus) {
			terminalMarkers++
		}
	}
	if approved != 1 {
		t.Fatalf("approved entries=%d, want one stable committed entry", approved)
	}
	if terminalMarkers != 1 {
		t.Fatalf("terminal markers=%d, want 1", terminalMarkers)
	}
	admission, found, err := base.ReadTurnAdmission(ctx, "thread", "turn", "run")
	if err != nil {
		t.Fatal(err)
	}
	if !found || admission.Terminal == nil || admission.Terminal.Terminal.TurnStatus != sessiontree.TurnFailed ||
		admission.Terminal.Terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] != sessiontree.TurnFailureEffectOutcomeUnknown {
		t.Fatalf("replayed dispatch terminal=%#v found=%v", admission.Terminal, found)
	}
	if _, active, err := base.ActiveTurnLease(ctx, "thread"); err != nil || active {
		t.Fatalf("replayed dispatch retained active lease: active=%v err=%v", active, err)
	}
}

func waitForApprovalQueue(t *testing.T, ctx context.Context, h *AgentHarness, threadID string) ApprovalQueueSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		queue, err := h.ReadApprovalQueue(ctx, ReadApprovalQueueOptions{ThreadID: threadID})
		if err != nil {
			t.Fatal(err)
		}
		if len(queue.Approvals) != 0 {
			return queue
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for approval")
		}
		time.Sleep(time.Millisecond)
	}
}

type approvalEventFailureRepo struct {
	*sessiontree.MemoryRepo
	mu                 sync.Mutex
	failApprovalState  string
	failApprovalAt     int
	approvalStateSeen  int
	forceCommitReplay  bool
	markUnknownCalls   atomic.Int32
	markedUnknownState atomic.Value
}

func (r *approvalEventFailureRepo) CommitApprovalDispatch(ctx context.Context, req sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	r.mu.Lock()
	fail := false
	if r.failApprovalState == "approved" {
		r.approvalStateSeen++
		fail = r.failApprovalAt > 0 && r.approvalStateSeen == r.failApprovalAt
	}
	r.mu.Unlock()
	if fail {
		return sessiontree.CommitApprovalDispatchResult{}, errors.New("injected approved entry persistence failure")
	}
	result, err := r.MemoryRepo.CommitApprovalDispatch(ctx, req)
	if err == nil && r.forceCommitReplay {
		result.Replayed = true
	}
	return result, err
}

func (r *approvalEventFailureRepo) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	result, err := r.MemoryRepo.MarkEffectUnknown(ctx, req)
	if err == nil {
		r.markUnknownCalls.Add(1)
		r.markedUnknownState.Store(string(result.State))
	}
	return result, err
}
