package composition

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v2/config"
	floretruntime "github.com/floegence/floret/v2/runtime"
	"github.com/floegence/floret/v2/tools"
)

type floretApprovalFixtureArgs struct {
	Path string `json:"path"`
}

type floretApprovalFixtureGateway struct{}

func (floretApprovalFixtureGateway) StreamModel(_ context.Context, request floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	events := make(chan floretruntime.ModelEvent, 2)
	if request.Step == 1 {
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
			ID: "approval-call", Name: "write_note", Args: `{"path":"notes/generated.txt"}`,
		}}}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "tool_calls"}
	} else {
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: "approved"}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "stop"}
	}
	close(events)
	return events, nil
}

type floretApprovalFixtureGate struct {
	grant chan string
	calls atomic.Int64
}

func (gate *floretApprovalFixtureGate) Dispatch(ctx context.Context, request floretruntime.EffectAuthorizationRequest, effect floretruntime.AuthorizedEffect) (floretruntime.EffectDispatchResult, error) {
	if request.Permission.Mode != tools.PermissionAsk {
		return floretruntime.EffectDispatchResult{}, floretruntime.ErrAuthorizationContract
	}
	gate.calls.Add(1)
	select {
	case <-ctx.Done():
		return floretruntime.EffectDispatchResult{}, ctx.Err()
	case approvalID := <-gate.grant:
		return effect(ctx, floretruntime.EffectAuthorizationProof{
			EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
			LeaseOwnerID: request.LeaseOwnerID, LeaseGeneration: request.LeaseGeneration,
			PolicyRevision: "generated-approval-v1", ApprovalID: approvalID,
			AuditReference: "generated-approval:" + request.EffectAttemptID,
			AuditHash:      "generated-approval-audit", AuthorizedAt: time.Now().UTC(),
		})
	}
}

func TestFloretGeneratedApprovalLifecycle(t *testing.T) {
	ctx := context.Background()
	var handlerCalls atomic.Int64
	registry := tools.NewRegistry(tools.Define[floretApprovalFixtureArgs](tools.Definition{
		Name:        "write_note",
		InputSchema: tools.StrictObject(map[string]any{"path": tools.String("note path")}, []string{"path"}),
		Effects:     []tools.Effect{tools.EffectWrite},
		Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}},
	}, nil, func(invocation tools.Invocation[floretApprovalFixtureArgs]) ([]tools.ResourceRef, error) {
		return []tools.ResourceRef{{Kind: "file", Value: invocation.Args.Path}}, nil
	}, func(context.Context, tools.Invocation[floretApprovalFixtureArgs]) (tools.Result, error) {
		handlerCalls.Add(1)
		return tools.Result{Text: "wrote note"}, nil
	}))
	gate := &floretApprovalFixtureGate{grant: make(chan string, 1)}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	composition, err := openFloretComposition(
		ctx,
		config.Config{SystemPrompt: "Use the write tool.", ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}},
		registry,
		gate,
		withFloretModelGateway(floretApprovalFixtureGateway{}, floretruntime.ModelGatewayIdentity{
			Provider: "generated", Model: "approval", StateCompatibilityKey: "generated:approval:v1",
		}, floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := composition.close(); err != nil {
			t.Error(err)
		}
	})

	threadID := floretruntime.ThreadID("approval-thread")
	creator, err := composition.bindThreadCreator(threadID, "create-approval-thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: threadID, CreateIntentID: "create-approval-thread"}); err != nil {
		t.Fatal(err)
	}
	turns, err := composition.bindTurnHost(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result floretruntime.TurnResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, runErr := turns.RunTurn(ctx, floretruntime.RunTurnRequest{
			ThreadID: threadID, TurnID: "approval-turn", RunID: "approval-run",
			Input: floretruntime.TurnInput{Text: "Write the note."},
		})
		done <- outcome{result: result, err: runErr}
	}()

	queue := waitForFloretGeneratedApproval(t, ctx, turns, threadID)
	approval := queue.Items[0]
	if approval.ToolCallID != "approval-call" || len(approval.Resources) != 1 || approval.Resources[0].Value != "notes/generated.txt" {
		t.Fatalf("approval = %#v", approval)
	}
	if _, err := turns.ResolveApproval(ctx, floretruntime.ResolveApprovalRequest{
		DecisionID: "approve-generated-call", ExpectedRootThreadID: queue.RootThreadID,
		ExpectedGeneration: queue.Generation, ExpectedRevision: queue.Revision,
		ExpectedCurrent: floretruntime.ApprovalIdentity{
			ApprovalID: approval.ApprovalID, ThreadID: approval.ThreadID, TurnID: approval.TurnID,
			RunID: approval.RunID, ToolCallID: approval.ToolCallID, EffectAttemptID: approval.EffectAttemptID,
		},
		ExpectedApprovalRevision: approval.Revision,
		Decision:                 floretruntime.ApprovalDecisionApprove,
	}); err != nil {
		t.Fatal(err)
	}
	gate.grant <- approval.ApprovalID

	select {
	case completed := <-done:
		if err := validateFloretTurnOutcome(completed.result, completed.err); err != nil {
			t.Fatal(err)
		}
		if completed.result.Output != "approved" {
			t.Fatalf("turn output = %q", completed.result.Output)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for approved turn")
	}
	if handlerCalls.Load() != 1 || gate.calls.Load() != 1 {
		t.Fatalf("handler calls=%d gate calls=%d", handlerCalls.Load(), gate.calls.Load())
	}
	settled, err := turns.ReadApprovalQueue(ctx, floretruntime.ReadApprovalQueueRequest{ThreadID: threadID})
	if err != nil {
		t.Fatal(err)
	}
	if len(settled.Items) != 0 {
		t.Fatalf("settled approval queue = %#v", settled)
	}
}

func waitForFloretGeneratedApproval(t *testing.T, ctx context.Context, turns floretTurnRunner, threadID floretruntime.ThreadID) floretruntime.ApprovalQueue {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		queue, err := turns.ReadApprovalQueue(ctx, floretruntime.ReadApprovalQueueRequest{ThreadID: threadID})
		if err != nil {
			t.Fatal(err)
		}
		if len(queue.Items) == 1 {
			return queue
		}
		if time.Now().After(deadline) {
			t.Fatal(fmt.Errorf("timed out waiting for approval queue: %#v", queue))
		}
		time.Sleep(time.Millisecond)
	}
}
