package composition

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

type nestedPendingCatalog struct {
	roots []floretruntime.ThreadID
}

func (catalog nestedPendingCatalog) ListFloretRootThreadIDs(context.Context) ([]floretruntime.ThreadID, error) {
	return append([]floretruntime.ThreadID(nil), catalog.roots...), nil
}

type nestedPendingReconciler struct {
	parent floretruntime.ThreadID
	target floretruntime.PendingToolSettlementTarget
}

func (reconciler *nestedPendingReconciler) ReconcileFloretPendingTool(_ context.Context, parentThreadID floretruntime.ThreadID, target floretruntime.PendingToolSettlementTarget) (floretruntime.PendingToolSettlementRequest, bool, error) {
	reconciler.parent = parentThreadID
	reconciler.target = target
	return floretruntime.PendingToolSettlementRequest{
		Target: target, Status: floretruntime.PendingToolSettlementCompleted,
		Summary: "external work completed before restart", Output: "done",
	}, true, nil
}

type nestedPendingGateway struct {
	mu    sync.Mutex
	steps map[floretruntime.ThreadID]int
}

func (gateway *nestedPendingGateway) StreamModel(_ context.Context, request floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	gateway.mu.Lock()
	if gateway.steps == nil {
		gateway.steps = make(map[floretruntime.ThreadID]int)
	}
	gateway.steps[request.ThreadID]++
	step := gateway.steps[request.ThreadID]
	gateway.mu.Unlock()

	events := make(chan floretruntime.ModelEvent, 2)
	if request.ThreadID == "grandchild" && step == 1 {
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
			ID: "pending-call", Name: "start_job", Args: `{"name":"index"}`,
		}}}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "tool_calls"}
	} else {
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: "done"}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "stop"}
	}
	close(events)
	return events, nil
}

type nestedPendingGate struct{}

func (nestedPendingGate) Dispatch(ctx context.Context, request floretruntime.EffectAuthorizationRequest, effect floretruntime.AuthorizedEffect) (floretruntime.EffectDispatchResult, error) {
	return effect(ctx, floretruntime.EffectAuthorizationProof{
		EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
		ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
		LeaseOwnerID: request.LeaseOwnerID, LeaseGeneration: request.LeaseGeneration,
		PolicyRevision: "fixture-v1", AuditReference: "fixture:" + request.EffectAttemptID,
		AuditHash: "fixture-audit", AuthorizedAt: time.Now().UTC(),
	})
}

type nestedPendingArgs struct {
	Name string `json:"name"`
}

func TestProductionRecoveryTraversesGrandchildAndSettlesCanonicalPendingTarget(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "floret.db")
	gateway := &nestedPendingGateway{}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	registry := tools.NewRegistry(tools.Define[nestedPendingArgs](tools.Definition{
		Name: "start_job", InputSchema: tools.StrictObject(map[string]any{"name": tools.String("job name")}, []string{"name"}),
		Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
	}, nil, nil, func(context.Context, tools.Invocation[nestedPendingArgs]) (tools.Result, error) {
		return tools.Result{Pending: &tools.PendingToolResult{
			Handle: "job:index:1", State: tools.PendingToolResultRunning,
			Summary: "indexing", Instruction: "reconcile after restart",
		}}, nil
	}))
	cfg := config.Config{SystemPrompt: "Complete the assigned task.", ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}
	composition, err := openFloretComposition(
		ctx, databasePath, floretruntime.SQLiteStartupRequest{}, nil, nestedPendingCatalog{}, nil, cfg,
		withFloretModelGateway(gateway, floretruntime.ModelGatewayIdentity{
			Provider: "fixture", Model: "scripted", StateCompatibilityKey: "fixture:scripted:v1",
		}, floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning}),
		withFloretEffectfulTools(registry, nestedPendingGate{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	rootID := floretruntime.ThreadID("root")
	creator, err := composition.bindThreadCreator(rootID, "create-root")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: rootID, CreateIntentID: "create-root"}); err != nil {
		t.Fatal(err)
	}
	childHost, err := composition.bindSubAgentHost(ctx, rootID)
	if err != nil {
		t.Fatal(err)
	}
	child, err := childHost.SpawnSubAgent(ctx, floretruntime.SpawnSubAgentRequest{
		PublicationID: "publish-child", ParentThreadID: rootID, ParentTurnID: "root-turn", ThreadID: "child",
		TaskName: "child", TaskDescription: "create a nested task", Message: "finish child work", HostProfileRef: "fixture", ForkMode: floretruntime.SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	child = waitForFixtureSubAgent(t, ctx, childHost, rootID, child.ThreadID)

	grandchildHost, err := composition.bindSubAgentHost(ctx, child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := grandchildHost.SpawnSubAgent(ctx, floretruntime.SpawnSubAgentRequest{
		PublicationID: "publish-grandchild", ParentThreadID: child.ThreadID, ParentTurnID: child.LatestTurnID, ThreadID: "grandchild",
		TaskName: "grandchild", TaskDescription: "start external work", Message: "start the job", HostProfileRef: "fixture", ForkMode: floretruntime.SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForFixtureSubAgent(t, ctx, grandchildHost, child.ThreadID, grandchild.ThreadID)
	if err := composition.close(); err != nil {
		t.Fatal(err)
	}

	reconciler := &nestedPendingReconciler{}
	recovered, err := openFloretComposition(
		ctx, databasePath, floretruntime.SQLiteStartupRequest{}, nil,
		nestedPendingCatalog{roots: []floretruntime.ThreadID{rootID}}, reconciler,
		config.Config{Provider: config.ProviderFake, Model: "fake", FakeResponse: "done", SystemPrompt: "test"},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.close(); err != nil {
			t.Error(err)
		}
	})
	if reconciler.parent != child.ThreadID || reconciler.target.ThreadID != grandchild.ThreadID || reconciler.target.ToolCallID != "pending-call" {
		t.Fatalf("reconciled wrong descendant target: parent=%q target=%#v", reconciler.parent, reconciler.target)
	}
	reader, err := recovered.subAgentRead.NewHost(ctx, child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	targets, err := reader.ListPendingToolSettlementTargets(ctx, floretruntime.ListSubAgentPendingToolSettlementTargetsRequest{
		ParentThreadID: child.ThreadID, ChildThreadID: grandchild.ThreadID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		t.Fatalf("recovery left pending targets: %#v", targets)
	}
}

func waitForFixtureSubAgent(t *testing.T, ctx context.Context, host floretSubAgentRunner, parentThreadID, childThreadID floretruntime.ThreadID) floretruntime.SubAgentSnapshot {
	t.Helper()
	result, err := host.WaitSubAgents(ctx, floretruntime.WaitSubAgentsRequest{
		ParentThreadID: parentThreadID, ChildThreadIDs: []floretruntime.ThreadID{childThreadID}, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TimedOut || len(result.Snapshots) != 1 || result.Snapshots[0].Status != floretruntime.SubAgentStatusCompleted {
		t.Fatal(fmt.Errorf("unexpected SubAgent result: %#v", result))
	}
	return result.Snapshots[0]
}
