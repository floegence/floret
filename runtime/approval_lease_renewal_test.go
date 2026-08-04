package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/provider/cache"
	"github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/internal/storage"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/internal/testing/tooltest"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/tools"
)

func TestApprovedEffectDispatchUsesRenewedLeaseAuthority(t *testing.T) {
	policy := sessiontree.LeasePolicy{
		TTL:                60 * time.Second,
		RenewInterval:      100 * time.Millisecond,
		ClockSkewAllowance: 20 * time.Millisecond,
	}
	tests := []struct {
		name  string
		store func(*testing.T) *runtimeStore
	}{
		{
			name: "memory",
			store: func(t *testing.T) *runtimeStore {
				repo, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				prompt := cache.NewMemoryStore()
				store := &runtimeStore{
					repo: repo, prompt: prompt, forkOperations: storage.NewMemoryForkOperationStore(repo),
					agentTodos: repo, rootAuthority: repo,
					deleteCleanup: func(ctx context.Context, threadIDs []string) error {
						return prompt.DeletePromptScopes(ctx, threadIDs...)
					},
				}
				store.self = store
				store.initLifetime()
				return store
			},
		},
		{
			name: "backend_sqlite",
			store: func(t *testing.T) *runtimeStore {
				ctx := context.Background()
				backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(filepath.Join(t.TempDir(), "approval-renewal.db"))))
				if err != nil {
					t.Fatal(err)
				}
				kernel, err := storage.NewBackendKernel(ctx, backend, policy, time.Now)
				if err != nil {
					_ = backend.Close()
					t.Fatal(err)
				}
				store := &runtimeStore{
					repo: kernel, prompt: kernel, forkOperations: kernel, agentTodos: kernel, rootAuthority: kernel,
					deleteCleanup: func(context.Context, []string) error { return nil }, close: backend.Close,
				}
				store.self = store
				store.initLifetime()
				return store
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runApprovedEffectDispatchAcrossLeaseRenewal(t, test.store(t))
		})
	}
}

func TestWaitingApprovalCancellationUsesRenewedLeaseAuthority(t *testing.T) {
	policy := sessiontree.LeasePolicy{
		TTL:                60 * time.Second,
		RenewInterval:      100 * time.Millisecond,
		ClockSkewAllowance: 20 * time.Millisecond,
	}
	for _, test := range []struct {
		name  string
		store func(*testing.T) *runtimeStore
	}{
		{
			name: "memory",
			store: func(t *testing.T) *runtimeStore {
				repo, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				prompt := cache.NewMemoryStore()
				store := &runtimeStore{
					repo: repo, prompt: prompt, forkOperations: storage.NewMemoryForkOperationStore(repo),
					agentTodos: repo, rootAuthority: repo,
					deleteCleanup: func(ctx context.Context, threadIDs []string) error {
						return prompt.DeletePromptScopes(ctx, threadIDs...)
					},
				}
				store.self = store
				store.initLifetime()
				return store
			},
		},
		{
			name: "backend_sqlite",
			store: func(t *testing.T) *runtimeStore {
				ctx := context.Background()
				backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(filepath.Join(t.TempDir(), "approval-cancel-renewal.db"))))
				if err != nil {
					t.Fatal(err)
				}
				kernel, err := storage.NewBackendKernel(ctx, backend, policy, time.Now)
				if err != nil {
					_ = backend.Close()
					t.Fatal(err)
				}
				store := &runtimeStore{
					repo: kernel, prompt: kernel, forkOperations: kernel, agentTodos: kernel, rootAuthority: kernel,
					deleteCleanup: func(context.Context, []string) error { return nil }, close: backend.Close,
				}
				store.self = store
				store.initLifetime()
				return store
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runWaitingApprovalCancellationAcrossLeaseRenewal(t, test.store(t))
		})
	}
}

func runWaitingApprovalCancellationAcrossLeaseRenewal(t *testing.T, store *runtimeStore) {
	t.Helper()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	ctx := context.Background()
	registry := tools.NewRegistry()
	var handlerCalls atomic.Int32
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name: "write_note", InputSchema: runtimeEchoSchema(), Effects: []tools.Effect{tools.EffectWrite},
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			handlerCalls.Add(1)
			return tools.Result{Text: "wrote " + inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(_ context.Context, req modelRequest) (<-chan modelEvent, error) {
		events := make(chan modelEvent, 2)
		events <- modelEvent{Type: modelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-cancel", Name: "write_note", Args: `{"text":"notes.md"}`}}}
		events <- modelEvent{Type: modelEventDone, Reason: "tool_calls"}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("approval cancellation renewal"), modelGateway: gateway,
		modelGatewayIdentity: runtimeGatewayIdentity("approval-cancel-renewal"), store: store, Tools: registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: func(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
			return tooltest.PermissionDecisionAllow, nil
		}},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, createThreadRequest{ThreadID: "thread-cancel-renewal"}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	type runOutcome struct {
		result TurnResult
		err    error
	}
	done := make(chan runOutcome, 1)
	go func() {
		result, runErr := host.RunTurn(runCtx, runTurnRequest{
			ThreadID: "thread-cancel-renewal", TurnID: "turn-cancel-renewal", RunID: "run-cancel-renewal",
			Input: TurnInput{Text: "write a note"},
		})
		done <- runOutcome{result: result, err: runErr}
	}()
	pendingQueue := waitRuntimeApprovalForCall(t, ctx, host, "thread-cancel-renewal", "call-cancel")
	pendingApprovalID := pendingQueue.Items[0].ApprovalID
	waitRuntimeLeaseHeartbeat(t, store.repo, "thread-cancel-renewal", 0)
	cancelRun()
	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, context.Canceled) || outcome.result.Status != TurnStatusCancelled {
			authority := store.repo.(sessiontree.ApprovalAuthorityRepo)
			approval, approvalErr := authority.Approval(ctx, pendingApprovalID)
			lease, active, leaseErr := store.repo.(sessiontree.TurnLeaseRepo).ActiveTurnLease(ctx, "thread-cancel-renewal")
			t.Fatalf("cancelled run result=%#v err=%v approval=%#v approval_err=%v active_lease=%#v active=%v lease_err=%v", outcome.result, outcome.err, approval, approvalErr, lease, active, leaseErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled run did not settle after lease renewal")
	}
	queue := waitRuntimeApprovalQueue(t, ctx, host, "thread-cancel-renewal", 0)
	if queue.CurrentApprovalID != "" || handlerCalls.Load() != 0 {
		t.Fatalf("queue=%#v handler_calls=%d", queue, handlerCalls.Load())
	}
}

func runApprovedEffectDispatchAcrossLeaseRenewal(t *testing.T, store *runtimeStore) {
	t.Helper()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	ctx := context.Background()
	registry := tools.NewRegistry()
	var handlerCalls atomic.Int32
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name: "write_note", InputSchema: runtimeEchoSchema(), Effects: []tools.Effect{tools.EffectWrite},
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			handlerCalls.Add(1)
			return tools.Result{Text: "wrote " + inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(_ context.Context, req modelRequest) (<-chan modelEvent, error) {
		events := make(chan modelEvent, 3)
		if req.Step <= 3 {
			callID := "call-" + string(rune('0'+req.Step))
			events <- modelEvent{Type: modelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: callID, Name: "write_note", Args: `{"text":"notes.md"}`}}}
			events <- modelEvent{Type: modelEventDone, Reason: "tool_calls"}
		} else {
			events <- modelEvent{Type: modelEventDelta, Text: "done"}
			events <- modelEvent{Type: modelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("approval renewal"), modelGateway: gateway,
		modelGatewayIdentity: runtimeGatewayIdentity("approval-renewal"), store: store, Tools: registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: func(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
			return tooltest.PermissionDecisionAllow, nil
		}},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, createThreadRequest{ThreadID: "thread-renewal"}); err != nil {
		t.Fatal(err)
	}
	type runOutcome struct {
		result TurnResult
		err    error
	}
	done := make(chan runOutcome, 1)
	go func() {
		result, err := host.RunTurn(ctx, runTurnRequest{
			ThreadID: "thread-renewal", TurnID: "turn-renewal", RunID: "run-renewal",
			Input: TurnInput{Text: "write three notes"},
		})
		done <- runOutcome{result: result, err: err}
	}()

	approvalIDs := make([]string, 0, 3)
	for index, callID := range []string{"call-1", "call-2", "call-3"} {
		queue := waitRuntimeApprovalForCall(t, ctx, host, "thread-renewal", callID)
		approvalIDs = append(approvalIDs, queue.Items[0].ApprovalID)
		if index == 2 {
			waitRuntimeLeaseHeartbeat(t, store.repo, "thread-renewal", 0)
		}
		resolved := resolveRuntimeApproval(t, ctx, host, queue, queue.Items[0], "decision-"+callID, ApprovalDecisionApprove)
		if resolved.Receipt.State != "decision_submitted" || resolved.Approval.State != "decision_submitted" {
			t.Fatalf("approval result = %#v", resolved)
		}
	}

	select {
	case outcome := <-done:
		if outcome.err != nil || outcome.result.Status != TurnStatusCompleted {
			t.Fatalf("run result=%#v err=%v, want completed after renewed approval authority", outcome.result, outcome.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not finish after approved effect dispatch")
	}
	if got := handlerCalls.Load(); got != 3 {
		t.Fatalf("effect handler calls=%d, want 3", got)
	}
	authority, ok := store.repo.(sessiontree.ApprovalAuthorityRepo)
	if !ok {
		t.Fatal("runtime store does not expose approval authority")
	}
	for _, approvalID := range approvalIDs {
		record, err := authority.Approval(ctx, approvalID)
		if err != nil {
			t.Fatal(err)
		}
		if record.State != sessiontree.ApprovalApproved {
			t.Fatalf("approval %q state=%q, want approved", approvalID, record.State)
		}
	}
}

func waitRuntimeApprovalForCall(t *testing.T, ctx context.Context, host interface {
	ReadApprovalQueue(context.Context, readApprovalQueueRequest) (ApprovalQueue, error)
}, threadID identity.ThreadID, callID string) ApprovalQueue {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		queue, err := host.ReadApprovalQueue(ctx, readApprovalQueueRequest{ThreadID: threadID})
		if err != nil {
			t.Fatal(err)
		}
		if len(queue.Items) == 1 && queue.Items[0].ToolCallID == callID {
			return queue
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for approval %q: %#v", callID, queue)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitRuntimeLeaseHeartbeat(t *testing.T, repo sessiontree.Repo, threadID string, previous int64) int64 {
	t.Helper()
	leaseRepo, ok := repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		t.Fatal("runtime store does not expose turn lease authority")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		lease, active, err := leaseRepo.ActiveTurnLease(context.Background(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if active && lease.Heartbeat > previous {
			return lease.Heartbeat
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for lease heartbeat after %d: active=%v lease=%#v", previous, active, lease)
		}
		time.Sleep(time.Millisecond)
	}
}
