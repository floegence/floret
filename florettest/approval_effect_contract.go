package florettest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

// RunApprovalEffectContract exercises Floret's public durable approval and
// effect-authority lifecycle. It deliberately uses no host UI or product
// authorization policy; those remain consumer responsibilities.
func RunApprovalEffectContract(t *testing.T) {
	t.Helper()

	t.Run("waiting and reject", func(t *testing.T) {
		var handlerCalls atomic.Int32
		var gateCalls atomic.Int32
		host := newApprovalContractHost(t, approvalContractOptions{
			handlerCalls: &handlerCalls,
			gate: runtime.EffectAuthorizationGateFunc(func(context.Context, runtime.EffectAuthorizationRequest, runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
				gateCalls.Add(1)
				return runtime.EffectDispatchResult{}, errors.New("gate must not run for a rejected approval")
			}),
		})
		outcome := runContractTurnAsync(context.Background(), host, 1)
		queue := waitForContractApproval(t, host)
		if len(queue.Items) != 1 || queue.Items[0].State != "requested" || handlerCalls.Load() != 0 || gateCalls.Load() != 0 {
			t.Fatalf("waiting queue=%#v handler_calls=%d gate_calls=%d", queue, handlerCalls.Load(), gateCalls.Load())
		}
		resolveContractApproval(t, host, queue, runtime.ApprovalDecisionReject, "reject-contract-effect")
		finished := waitContractOutcome(t, outcome)
		if !errors.Is(finished.err, runtime.ErrEffectUnauthorized) || finished.result.Status != runtime.TurnStatusFailed || handlerCalls.Load() != 0 || gateCalls.Load() != 0 {
			t.Fatalf("rejected result=%#v err=%v handler_calls=%d gate_calls=%d", finished.result, finished.err, handlerCalls.Load(), gateCalls.Load())
		}
		assertContractApprovalQueueEmpty(t, host)
	})

	t.Run("cancel", func(t *testing.T) {
		var handlerCalls atomic.Int32
		host := newApprovalContractHost(t, approvalContractOptions{handlerCalls: &handlerCalls})
		ctx, cancel := context.WithCancel(context.Background())
		outcome := runContractTurnAsync(ctx, host, 1)
		_ = waitForContractApproval(t, host)
		cancel()
		finished := waitContractOutcome(t, outcome)
		if !errors.Is(finished.err, context.Canceled) || finished.result.Status != runtime.TurnStatusCancelled || handlerCalls.Load() != 0 {
			t.Fatalf("cancelled result=%#v err=%v handler_calls=%d", finished.result, finished.err, handlerCalls.Load())
		}
		assertContractApprovalQueueEmpty(t, host)
	})

	t.Run("one-shot proof", func(t *testing.T) {
		var handlerCalls atomic.Int32
		approvalID := make(chan string, 1)
		secondUse := make(chan error, 1)
		host := newApprovalContractHost(t, approvalContractOptions{
			handlerCalls: &handlerCalls,
			gate: runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
				id, err := contractApprovalID(ctx, approvalID)
				if err != nil {
					return runtime.EffectDispatchResult{}, err
				}
				proof := contractAuthorizationProof(req, id)
				first, firstErr := effect(ctx, proof)
				_, secondErr := effect(ctx, proof)
				secondUse <- secondErr
				return first, firstErr
			}),
		})
		outcome := runContractTurnAsync(context.Background(), host, 1)
		queue := waitForContractApproval(t, host)
		resolveContractApproval(t, host, queue, runtime.ApprovalDecisionApprove, "approve-one-shot")
		approvalID <- queue.Items[0].ApprovalID
		finished := waitContractOutcome(t, outcome)
		if finished.err != nil || finished.result.Status != runtime.TurnStatusCompleted || handlerCalls.Load() != 1 {
			t.Fatalf("one-shot result=%#v err=%v handler_calls=%d", finished.result, finished.err, handlerCalls.Load())
		}
		if err := <-secondUse; !errors.Is(err, runtime.ErrEffectDispatchConsumed) {
			t.Fatalf("second authorized effect error=%v, want ErrEffectDispatchConsumed", err)
		}
	})

	t.Run("identity mismatch", func(t *testing.T) {
		var handlerCalls atomic.Int32
		approvalID := make(chan string, 1)
		host := newApprovalContractHost(t, approvalContractOptions{
			handlerCalls: &handlerCalls,
			gate: runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
				id, err := contractApprovalID(ctx, approvalID)
				if err != nil {
					return runtime.EffectDispatchResult{}, err
				}
				proof := contractAuthorizationProof(req, id)
				proof.ThreadID = "mismatched-thread"
				return effect(ctx, proof)
			}),
		})
		outcome := runContractTurnAsync(context.Background(), host, 1)
		queue := waitForContractApproval(t, host)
		resolveContractApproval(t, host, queue, runtime.ApprovalDecisionApprove, "approve-mismatch")
		approvalID <- queue.Items[0].ApprovalID
		finished := waitContractOutcome(t, outcome)
		if !errors.Is(finished.err, runtime.ErrInvalidAuthorizationProof) || finished.result.Status != runtime.TurnStatusFailed || handlerCalls.Load() != 0 {
			t.Fatalf("identity mismatch result=%#v err=%v handler_calls=%d", finished.result, finished.err, handlerCalls.Load())
		}
	})

	t.Run("permission change", func(t *testing.T) {
		var handlerCalls atomic.Int32
		var deny atomic.Bool
		approvalID := make(chan string, 1)
		var gateCalls atomic.Int32
		host := newApprovalContractHost(t, approvalContractOptions{
			handlerCalls: &handlerCalls,
			deny:         &deny,
			twoCalls:     true,
			gate: runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
				gateCalls.Add(1)
				if req.Permission.Mode == tools.PermissionDeny {
					return runtime.EffectDispatchResult{}, runtime.ErrEffectUnauthorized
				}
				id, err := contractApprovalID(ctx, approvalID)
				if err != nil {
					return runtime.EffectDispatchResult{}, err
				}
				return effect(ctx, contractAuthorizationProof(req, id))
			}),
		})
		outcome := runContractTurnAsync(context.Background(), host, 1)
		queue := waitForContractApproval(t, host)
		resolveContractApproval(t, host, queue, runtime.ApprovalDecisionApprove, "approve-before-change")
		approvalID <- queue.Items[0].ApprovalID
		finished := waitContractOutcome(t, outcome)
		if !errors.Is(finished.err, runtime.ErrEffectUnauthorized) || finished.result.Status != runtime.TurnStatusFailed || handlerCalls.Load() != 1 || gateCalls.Load() != 2 || !deny.Load() {
			t.Fatalf("permission change result=%#v err=%v handler_calls=%d gate_calls=%d deny=%t", finished.result, finished.err, handlerCalls.Load(), gateCalls.Load(), deny.Load())
		}
		assertContractApprovalQueueEmpty(t, host)
	})
}

type approvalContractOptions struct {
	handlerCalls *atomic.Int32
	deny         *atomic.Bool
	twoCalls     bool
	gate         runtime.EffectAuthorizationGate
}

type approvalContractArgs struct {
	Value string `json:"value"`
}

func newApprovalContractHost(t testing.TB, options approvalContractOptions) *runtime.TurnExecutionHost {
	t.Helper()
	if options.handlerCalls == nil {
		options.handlerCalls = &atomic.Int32{}
	}
	permissionFor := func(tools.PermissionRequest) (tools.PermissionSpec, error) {
		if options.deny != nil && options.deny.Load() {
			return tools.PermissionSpec{Mode: tools.PermissionDeny}, nil
		}
		return tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}}, nil
	}
	registry := tools.NewRegistry(tools.Define[approvalContractArgs](
		tools.Definition{
			Name: "contract_effect", Description: "Exercises durable approval authority.",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("resource")}, []string{"value"}),
			Effects:     []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}},
			PermissionFor: permissionFor,
		},
		nil,
		func(inv tools.Invocation[approvalContractArgs]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{{Kind: "file", Value: inv.Args.Value}}, nil
		},
		func(_ context.Context, inv tools.Invocation[approvalContractArgs]) (tools.Result, error) {
			options.handlerCalls.Add(1)
			if options.deny != nil {
				options.deny.Store(true)
			}
			return tools.Result{Text: "handled " + inv.Args.Value}, nil
		},
	))
	steps := []ModelStep{{Events: []runtime.ModelEvent{
		{Type: runtime.ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "contract-effect-1", Name: "contract_effect", Args: `{"value":"notes.md"}`}}},
		{Type: runtime.ModelEventDone, Reason: "tool_calls"},
	}}}
	if options.twoCalls {
		steps = append(steps, ModelStep{Events: []runtime.ModelEvent{
			{Type: runtime.ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "contract-effect-2", Name: "contract_effect", Args: `{"value":"notes.md"}`}}},
			{Type: runtime.ModelEventDone, Reason: "tool_calls"},
		}})
	}
	steps = append(steps, ModelStep{Events: []runtime.ModelEvent{
		{Type: runtime.ModelEventDelta, Text: "approval contract complete"},
		{Type: runtime.ModelEventDone, Reason: "stop"},
	}})
	return newContractTurnHostWithOptions(t, runtime.TurnExecutionHostOptions{
		ModelGateway: NewScriptedModelGateway(steps...), Tools: registry, EffectAuthorizationGate: options.gate,
	})
}

func contractAuthorizationProof(req runtime.EffectAuthorizationRequest, approvalID string) runtime.EffectAuthorizationProof {
	return runtime.EffectAuthorizationProof{
		EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
		LeaseOwnerID: req.LeaseOwnerID, LeaseGeneration: req.LeaseGeneration,
		PolicyRevision: "florettest-policy-v1", ApprovalID: approvalID,
		AuditReference: "florettest-audit:" + req.EffectAttemptID,
		AuditHash:      "florettest-audit-hash", AuthorizedAt: time.Now().UTC(),
	}
}

func contractApprovalID(ctx context.Context, approvalIDs <-chan string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case approvalID := <-approvalIDs:
		return approvalID, nil
	}
}

func waitForContractApproval(t testing.TB, host *runtime.TurnExecutionHost) runtime.ApprovalQueue {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		queue, err := host.ReadApprovalQueue(context.Background(), runtime.ReadApprovalQueueRequest{ThreadID: "contract-thread"})
		if err != nil {
			t.Fatalf("read approval queue: %v", err)
		}
		if len(queue.Items) > 0 {
			return queue
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for approval")
		}
		time.Sleep(time.Millisecond)
	}
}

func resolveContractApproval(t testing.TB, host *runtime.TurnExecutionHost, queue runtime.ApprovalQueue, decision runtime.ApprovalDecision, decisionID string) runtime.ResolveApprovalResult {
	t.Helper()
	approval := queue.Items[0]
	result, err := host.ResolveApproval(context.Background(), runtime.ResolveApprovalRequest{
		DecisionID: decisionID, ExpectedRootThreadID: queue.RootThreadID,
		ExpectedGeneration: queue.Generation, ExpectedRevision: queue.Revision,
		ExpectedCurrent: runtime.ApprovalIdentity{
			ApprovalID: approval.ApprovalID, ThreadID: approval.ThreadID, TurnID: approval.TurnID,
			RunID: approval.RunID, ToolCallID: approval.ToolCallID, EffectAttemptID: approval.EffectAttemptID,
		},
		ExpectedApprovalRevision: approval.Revision, Decision: decision,
	})
	if err != nil {
		t.Fatalf("resolve approval: %v", err)
	}
	return result
}

func runContractTurnAsync(ctx context.Context, host *runtime.TurnExecutionHost, turn int) <-chan contractTurnOutcome {
	done := make(chan contractTurnOutcome, 1)
	go func() {
		result, err := runContractTurn(ctx, host, turn)
		done <- contractTurnOutcome{result: result, err: err}
	}()
	return done
}

func waitContractOutcome(t testing.TB, done <-chan contractTurnOutcome) contractTurnOutcome {
	t.Helper()
	select {
	case outcome := <-done:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for contract turn")
		return contractTurnOutcome{}
	}
}

func assertContractApprovalQueueEmpty(t testing.TB, host *runtime.TurnExecutionHost) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		queue, err := host.ReadApprovalQueue(context.Background(), runtime.ReadApprovalQueueRequest{ThreadID: "contract-thread"})
		if err != nil {
			t.Fatalf("read settled approval queue: %v", err)
		}
		if len(queue.Items) == 0 && strings.TrimSpace(queue.CurrentApprovalID) == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("approval queue did not settle: %s", fmt.Sprint(queue))
		}
		time.Sleep(time.Millisecond)
	}
}
