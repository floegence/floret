package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

type writeArgs struct {
	Path string `json:"path"`
	Text string `json:"text"`
}

type approvalGrant struct {
	approvalID string
}

type approvalGate struct {
	grant chan approvalGrant
}

func (g *approvalGate) Dispatch(ctx context.Context, req floretruntime.EffectAuthorizationRequest, effect floretruntime.AuthorizedEffect) (floretruntime.EffectDispatchResult, error) {
	if req.Permission.Mode != tools.PermissionAsk {
		return floretruntime.EffectDispatchResult{}, floretruntime.ErrAuthorizationContract
	}
	select {
	case <-ctx.Done():
		return floretruntime.EffectDispatchResult{}, ctx.Err()
	case grant := <-g.grant:
		return effect(ctx, floretruntime.EffectAuthorizationProof{
			EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
			ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
			LeaseOwnerID: req.LeaseOwnerID, LeaseGeneration: req.LeaseGeneration,
			PolicyRevision: "example-policy-v1", ApprovalID: grant.approvalID,
			AuditReference: "example-audit:" + req.EffectAttemptID,
			AuditHash:      "example-audit-hash", AuthorizedAt: time.Now().UTC(),
		})
	}
}

type toolGateway struct{}

func (toolGateway) StreamModel(_ context.Context, req floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	events := make(chan floretruntime.ModelEvent, 2)
	if req.Step == 1 {
		definitionVisible := false
		for _, definition := range req.Tools {
			if definition.Name == "write_note" {
				definitionVisible = true
				break
			}
		}
		if !definitionVisible {
			close(events)
			return nil, fmt.Errorf("provider request omitted write_note ToolDefinition")
		}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
			ID: "write-call", Name: "write_note", Args: `{"path":"notes/example.txt","text":"approved"}`,
		}}}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "tool_calls"}
	} else {
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: "The approved write completed."}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "stop"}
	}
	close(events)
	return events, nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	store := floretruntime.NewMemoryStore()
	defer store.Close()

	var createBinder *floretruntime.ThreadCreateHostBinder
	var turnBinder *floretruntime.TurnExecutionHostBinder
	var readBinder *floretruntime.ThreadReadHostBinder
	if err := floretruntime.ConfigureHostCapabilities(store, func(bootstrap *floretruntime.HostBootstrap) error {
		var configureErr error
		createBinder, configureErr = floretruntime.NewThreadCreateHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		turnBinder, configureErr = floretruntime.NewTurnExecutionHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		readBinder, configureErr = floretruntime.NewThreadReadHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		return err
	}

	const threadID = floretruntime.ThreadID("approval-thread")
	const createIntentID = floretruntime.CreateIntentID("create-approval-thread")
	createHost, err := createBinder.Bind(threadID, createIntentID)
	if err != nil {
		return err
	}
	if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: threadID, CreateIntentID: createIntentID}); err != nil {
		return err
	}

	var handlerCalls atomic.Int64
	registry := tools.NewRegistry(tools.Define[writeArgs](
		tools.Definition{
			Name: "write_note", Title: "Write note", Description: "Writes text to a host-owned note.",
			InputSchema: tools.StrictObject(map[string]any{
				"path": tools.String("note path"), "text": tools.String("note text"),
			}, []string{"path", "text"}),
			Effects: []tools.Effect{tools.EffectWrite},
			Permission: tools.PermissionSpec{
				Mode: tools.PermissionAsk, ResourceKinds: []string{"file"},
			},
		},
		nil,
		func(inv tools.Invocation[writeArgs]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{{Kind: "file", Value: inv.Args.Path}}, nil
		},
		func(_ context.Context, inv tools.Invocation[writeArgs]) (tools.Result, error) {
			handlerCalls.Add(1)
			return tools.Result{Text: fmt.Sprintf("wrote %d bytes to %s", len(inv.Args.Text), inv.Args.Path)}, nil
		},
	))
	gate := &approvalGate{grant: make(chan approvalGrant, 1)}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	turnFactory, err := turnBinder.Bind(threadID)
	if err != nil {
		return err
	}
	turnHost, err := turnFactory.NewHost(ctx, floretruntime.TurnExecutionHostOptions{
		Config: config.Config{
			SystemPrompt:  "Use the write tool when asked.",
			ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
		},
		ModelGateway: toolGateway{},
		ModelGatewayIdentity: floretruntime.ModelGatewayIdentity{
			Provider: "approval-example", Model: "local-scripted-model", StateCompatibilityKey: "approval-example:v1",
		},
		ModelGatewayCapabilities: floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning},
		Tools:                    registry, EffectAuthorizationGate: gate,
	})
	if err != nil {
		return err
	}

	type outcome struct {
		result floretruntime.TurnResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, runErr := turnHost.RunTurn(ctx, floretruntime.RunTurnRequest{
			ThreadID: threadID, TurnID: "approval-turn", RunID: "approval-run",
			Input: floretruntime.TurnInput{Text: "Write the approved note."},
		})
		done <- outcome{result: result, err: runErr}
	}()

	var queue floretruntime.ApprovalQueue
	deadline := time.Now().Add(2 * time.Second)
	for len(queue.Items) == 0 {
		queue, err = turnHost.ReadApprovalQueue(ctx, floretruntime.ReadApprovalQueueRequest{ThreadID: threadID})
		if err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for approval")
		}
		time.Sleep(time.Millisecond)
	}
	approval := queue.Items[0]
	if len(approval.Resources) != 1 || approval.Resources[0].Kind != "file" || approval.Resources[0].Value != "notes/example.txt" {
		return fmt.Errorf("unexpected approval resources: %#v", approval.Resources)
	}
	if len(approval.Effects) != 1 || approval.Effects[0] != string(tools.EffectWrite) {
		return fmt.Errorf("unexpected approval effects: %#v", approval.Effects)
	}
	if _, err := turnHost.ResolveApproval(ctx, floretruntime.ResolveApprovalRequest{
		DecisionID: "approve-write-call", ExpectedRootThreadID: queue.RootThreadID,
		ExpectedGeneration: queue.Generation, ExpectedRevision: queue.Revision,
		ExpectedCurrent: floretruntime.ApprovalIdentity{
			ApprovalID: approval.ApprovalID, ThreadID: approval.ThreadID, TurnID: approval.TurnID,
			RunID: approval.RunID, ToolCallID: approval.ToolCallID, EffectAttemptID: approval.EffectAttemptID,
		},
		ExpectedApprovalRevision: approval.Revision,
		Decision:                 floretruntime.ApprovalDecisionApprove,
	}); err != nil {
		return err
	}
	gate.grant <- approvalGrant{approvalID: approval.ApprovalID}

	completed := <-done
	if completed.err != nil {
		return completed.err
	}
	if handlerCalls.Load() != 1 || completed.result.Status != floretruntime.TurnStatusCompleted {
		return fmt.Errorf("effect calls=%d status=%s", handlerCalls.Load(), completed.result.Status)
	}
	readHost, err := readBinder.NewHost(ctx, threadID)
	if err != nil {
		return err
	}
	settledQueue, err := readHost.ReadApprovalQueue(ctx, floretruntime.ReadApprovalQueueRequest{ThreadID: threadID})
	if err != nil {
		return err
	}
	if len(settledQueue.Items) != 0 {
		return fmt.Errorf("approval queue still contains %d item(s)", len(settledQueue.Items))
	}
	fmt.Printf("tool=%s resource=%s approval=%s status=%s\n",
		approval.ToolName, approval.Resources[0].Value, approval.ApprovalID, completed.result.Status)
	return nil
}
