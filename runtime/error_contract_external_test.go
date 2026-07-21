package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

type publicErrorCapabilitySet struct {
	create   *floretruntime.ThreadCreateHostBinder
	turn     *floretruntime.TurnExecutionHostBinder
	recovery *floretruntime.PendingToolRecoveryHostBinder
	subagent *floretruntime.SubAgentHostBinder
}

func configurePublicErrorCapabilities(t *testing.T, store *floretruntime.Store) publicErrorCapabilitySet {
	t.Helper()
	var capabilities publicErrorCapabilitySet
	err := floretruntime.ConfigureHostCapabilities(store, func(bootstrap *floretruntime.HostBootstrap) error {
		var err error
		capabilities.create, err = floretruntime.NewThreadCreateHostBinder(bootstrap)
		if err != nil {
			return err
		}
		capabilities.turn, err = floretruntime.NewTurnExecutionHostBinder(bootstrap)
		if err != nil {
			return err
		}
		capabilities.recovery, err = floretruntime.NewPendingToolRecoveryHostBinder(bootstrap)
		if err != nil {
			return err
		}
		capabilities.subagent, err = floretruntime.NewSubAgentHostBinder(bootstrap)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return capabilities
}

func TestPublicOpenSQLiteStoreReportsTypedUnsupportedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsupported.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE product_owned_data (id TEXT PRIMARY KEY)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = floretruntime.OpenSQLiteStore(path)
	var unsupported *floretruntime.UnsupportedStoreSchemaError
	if !errors.As(err, &unsupported) {
		t.Fatalf("OpenSQLiteStore error = %v, want UnsupportedStoreSchemaError", err)
	}
	if unsupported.ObservedVersion != "" || unsupported.ObservedFingerprint != "" {
		t.Fatalf("observed unsupported schema = %#v, want absent Floret metadata", unsupported)
	}
	if unsupported.CurrentVersion == "" || unsupported.CurrentFingerprint == "" {
		t.Fatalf("current schema identity = %#v, want exact public identity", unsupported)
	}
}

func TestPublicRuntimeErrorsSupportErrorsIsWithoutInternalImports(t *testing.T) {
	ctx := context.Background()
	store := floretruntime.NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	capabilities := configurePublicErrorCapabilities(t, store)
	for _, threadID := range []floretruntime.ThreadID{"retry", "pending", "busy", "parent"} {
		createIntentID := floretruntime.CreateIntentID("public-error-create:" + string(threadID))
		createHost, err := capabilities.create.Bind(threadID, createIntentID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: threadID, CreateIntentID: createIntentID}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("no retry target", func(t *testing.T) {
		host := newPublicFakeTurnHost(t, capabilities.turn, "retry")
		if _, err := host.RetryTurn(ctx, floretruntime.RetryTurnRequest{ThreadID: "retry"}); !errors.Is(err, floretruntime.ErrNoRetryTarget) {
			t.Fatalf("RetryTurn error = %v, want ErrNoRetryTarget", err)
		}
	})

	t.Run("typed request conflict", func(t *testing.T) {
		conflicting, err := capabilities.create.Bind("other", "public-error-create:retry")
		if err != nil {
			t.Fatal(err)
		}
		_, err = conflicting.CreateThread(ctx, floretruntime.CreateThreadRequest{
			ThreadID: "other", CreateIntentID: "public-error-create:retry",
		})
		var conflict *floretruntime.RequestConflictError
		if !errors.Is(err, floretruntime.ErrRequestConflict) || !errors.As(err, &conflict) {
			t.Fatalf("create conflict err=%v, want typed RequestConflictError", err)
		}
		if conflict.Operation != "root_create" || conflict.RequestID != "public-error-create:retry" {
			t.Fatalf("create conflict identity = %#v", conflict)
		}
	})

	t.Run("new intent for live thread", func(t *testing.T) {
		const intentID = floretruntime.CreateIntentID("public-error-create:retry-again")
		conflicting, err := capabilities.create.Bind("retry", intentID)
		if err != nil {
			t.Fatal(err)
		}
		_, err = conflicting.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: "retry", CreateIntentID: intentID})
		var conflict *floretruntime.RequestConflictError
		if !errors.Is(err, floretruntime.ErrRequestConflict) || !errors.As(err, &conflict) {
			t.Fatalf("create conflict err=%v, want typed RequestConflictError", err)
		}
		if conflict.Operation != "root_create" || conflict.RequestID != string(intentID) {
			t.Fatalf("create conflict identity = %#v", conflict)
		}
	})

	t.Run("pending settlement targets", func(t *testing.T) {
		host, effectGate := newPublicPendingTurnHost(t, capabilities.turn, "pending")
		if _, err := host.RunTurn(ctx, floretruntime.RunTurnRequest{
			RunID:    "run-1",
			ThreadID: "pending",
			TurnID:   "turn-1",
			Input:    floretruntime.TurnInput{Text: "run tools"},
		}); err != nil {
			t.Fatal(err)
		}
		settlement, err := capabilities.recovery.NewThreadHost(ctx, "pending", nil)
		if err != nil {
			t.Fatal(err)
		}
		unknown := publicSettlementRequest("missing", "missing_tool", "pending:missing", floretruntime.PendingToolSettlementCompleted)
		if _, err := settlement.SettlePendingTool(ctx, unknown); !errors.Is(err, floretruntime.ErrPendingToolNotFound) {
			t.Fatalf("unknown settlement error = %v, want ErrPendingToolNotFound", err)
		}
		ordinary := publicSettlementRequest("ordinary-1", "ordinary_tool", "ordinary:job:1", floretruntime.PendingToolSettlementCompleted)
		if _, err := settlement.SettlePendingTool(ctx, ordinary); !errors.Is(err, floretruntime.ErrPendingToolNotActive) {
			t.Fatalf("ordinary settlement error = %v, want ErrPendingToolNotActive", err)
		}
		pending := publicSettlementRequest("pending-1", "pending_tool", "pending:job:1", floretruntime.PendingToolSettlementCompleted)
		pending.Target.EffectAttemptID = effectGate.effectAttemptID("pending-1")
		if _, err := settlement.SettlePendingTool(ctx, pending); err != nil {
			t.Fatal(err)
		}
		pending.Status = floretruntime.PendingToolSettlementFailed
		pending.Summary = "failed"
		if _, err := settlement.SettlePendingTool(ctx, pending); !errors.Is(err, floretruntime.ErrPendingToolSettlementConflict) {
			t.Fatalf("conflicting settlement error = %v, want ErrPendingToolSettlementConflict", err)
		}
	})

	t.Run("active mutation", func(t *testing.T) {
		gateway := newBlockingPublicGateway()
		factory, err := capabilities.turn.Bind("busy")
		if err != nil {
			t.Fatal(err)
		}
		host, err := factory.NewHost(ctx, floretruntime.TurnExecutionHostOptions{
			Config:               publicGatewayConfig(),
			ModelGateway:         gateway,
			ModelGatewayIdentity: publicGatewayIdentity(),
		})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := host.RunTurn(ctx, floretruntime.RunTurnRequest{
				RunID:    "busy-run",
				ThreadID: "busy",
				TurnID:   "busy-turn",
				Input:    floretruntime.TurnInput{Text: "wait"},
			})
			done <- err
		}()
		select {
		case <-gateway.entered:
		case <-time.After(2 * time.Second):
			gateway.releaseRun()
			t.Fatal("timed out waiting for active turn")
		}
		settlement, err := capabilities.recovery.NewThreadHost(ctx, "busy", nil)
		if err != nil {
			gateway.releaseRun()
			t.Fatal(err)
		}
		request := publicSettlementRequest("pending-1", "pending_tool", "pending:busy", floretruntime.PendingToolSettlementCompleted)
		request.Target.ThreadID = "busy"
		request.Target.TurnID = "busy-turn"
		request.Target.RunID = "busy-run"
		if _, err := settlement.SettlePendingTool(ctx, request); !errors.Is(err, floretruntime.ErrThreadBusy) {
			gateway.releaseRun()
			t.Fatalf("active settlement error = %v, want ErrThreadBusy", err)
		} else {
			var busy *floretruntime.AuthorityBusyError
			if !errors.As(err, &busy) || busy.Kind != floretruntime.AuthorityBusyTurn {
				gateway.releaseRun()
				t.Fatalf("active settlement busy classification = %#v err=%v", busy, err)
			}
		}
		gateway.releaseRun()
		if err := <-done; err != nil {
			t.Fatalf("active turn completion: %v", err)
		}
	})

	t.Run("closed subagent", func(t *testing.T) {
		factory, err := capabilities.subagent.Bind("parent")
		if err != nil {
			t.Fatal(err)
		}
		host, err := factory.NewHost(ctx, floretruntime.SubAgentHostOptions{Config: publicFakeConfig()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := host.SpawnSubAgent(ctx, floretruntime.SpawnSubAgentRequest{
			PublicationID:  "public-error-child-publication",
			ParentThreadID: "parent",
			ThreadID:       "child",
			TaskName:       "worker",
			Message:        "finish",
			ForkMode:       floretruntime.SubAgentForkNone,
		}); err != nil {
			t.Fatal(err)
		}
		waited, err := host.WaitSubAgents(ctx, floretruntime.WaitSubAgentsRequest{
			ParentThreadID: "parent",
			ChildThreadIDs: []floretruntime.ThreadID{"child"},
			Timeout:        2 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if waited.TimedOut {
			t.Fatal("timed out waiting for subagent")
		}
		if _, err := host.CloseSubAgent(ctx, floretruntime.CloseSubAgentRequest{
			CloseOperationID: "public-error-close-child",
			ParentThreadID:   "parent",
			ChildThreadID:    "child",
			Reason:           "done",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := host.SendSubAgentInput(ctx, floretruntime.SendSubAgentInputRequest{
			InputRequestID: "public-error-child-input",
			ParentThreadID: "parent",
			ChildThreadID:  "child",
			Message:        "again",
		}); !errors.Is(err, floretruntime.ErrSubAgentClosed) {
			t.Fatalf("SendSubAgentInput error = %v, want ErrSubAgentClosed", err)
		}
	})
}

func TestTerminalRunTurnReturnReleasesAuthorityForPendingToolRecovery(t *testing.T) {
	stores := []struct {
		name string
		open func(*testing.T) *floretruntime.Store
	}{
		{name: "memory", open: func(*testing.T) *floretruntime.Store { return floretruntime.NewMemoryStore() }},
		{name: "sqlite", open: func(t *testing.T) *floretruntime.Store {
			store, err := floretruntime.OpenSQLiteStore(filepath.Join(t.TempDir(), "recovery-after-cancel.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}
	for _, storeCase := range stores {
		t.Run(storeCase.name, func(t *testing.T) {
			ctx := context.Background()
			store := storeCase.open(t)
			t.Cleanup(func() { _ = store.Close() })
			capabilities := configurePublicErrorCapabilities(t, store)
			const threadID = floretruntime.ThreadID("recovery-after-cancel")
			const turnID = floretruntime.TurnID("turn-1")
			const runID = floretruntime.RunID("run-1")
			createIntentID := floretruntime.CreateIntentID("create:recovery-after-cancel")
			createHost, err := capabilities.create.Bind(threadID, createIntentID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: threadID, CreateIntentID: createIntentID}); err != nil {
				t.Fatal(err)
			}

			registry := tools.NewRegistry()
			if err := registry.Register(tools.Define[publicErrorArgs](tools.Definition{
				Name:        "pending_tool",
				InputSchema: tools.StrictObject(map[string]any{"text": tools.String("text")}, []string{"text"}),
				Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			}, nil, nil, func(context.Context, tools.Invocation[publicErrorArgs]) (tools.Result, error) {
				return tools.Result{Pending: &tools.PendingToolResult{
					Handle: "pending:cancel:1", State: tools.PendingToolResultRunning, Summary: "running", Instruction: "wait",
				}}, nil
			})); err != nil {
				t.Fatal(err)
			}
			gateway := newCancelAfterPendingGateway()
			effectGate := &publicAllowEffectGate{byCallID: make(map[string]string)}
			factory, err := capabilities.turn.Bind(threadID)
			if err != nil {
				t.Fatal(err)
			}
			host, err := factory.NewHost(ctx, floretruntime.TurnExecutionHostOptions{
				Config:                  publicGatewayConfig(),
				ModelGateway:            gateway,
				ModelGatewayIdentity:    publicGatewayIdentity(),
				Tools:                   registry,
				EffectAuthorizationGate: effectGate,
			})
			if err != nil {
				t.Fatal(err)
			}

			runCtx, cancelRun := context.WithCancel(ctx)
			done := make(chan struct {
				result floretruntime.TurnResult
				err    error
			}, 1)
			go func() {
				result, runErr := host.RunTurn(runCtx, floretruntime.RunTurnRequest{
					RunID: runID, ThreadID: threadID, TurnID: turnID,
					Input: floretruntime.TurnInput{Text: "start pending work"},
				})
				done <- struct {
					result floretruntime.TurnResult
					err    error
				}{result: result, err: runErr}
			}()
			select {
			case <-gateway.waiting:
			case <-time.After(2 * time.Second):
				cancelRun()
				t.Fatal("turn did not reach the provider request after the pending tool result")
			}
			recovery, err := capabilities.recovery.NewThreadHost(ctx, threadID, nil)
			if err != nil {
				t.Fatal(err)
			}
			request := publicSettlementRequest("pending-1", "pending_tool", "pending:cancel:1", floretruntime.PendingToolSettlementFailed)
			request.Target.ThreadID = threadID
			request.Target.TurnID = turnID
			request.Target.RunID = runID
			request.Target.EffectAttemptID = effectGate.effectAttemptID("pending-1")
			request.Summary = "canceled"
			if _, err := recovery.SettlePendingTool(ctx, request); !errors.Is(err, floretruntime.ErrThreadBusy) {
				t.Fatalf("recovery settlement before terminal RunTurn return error=%v, want ErrThreadBusy", err)
			}
			cancelRun()
			var outcome struct {
				result floretruntime.TurnResult
				err    error
			}
			select {
			case outcome = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("RunTurn did not return after cancellation")
			}
			if !errors.Is(outcome.err, context.Canceled) || outcome.result.Status != floretruntime.TurnStatusCancelled {
				t.Fatalf("RunTurn result=%#v err=%v, want cancelled", outcome.result, outcome.err)
			}

			if _, err := recovery.SettlePendingTool(ctx, request); err != nil {
				t.Fatalf("recovery settlement immediately after RunTurn returned: %v", err)
			}
			next, err := host.RunTurn(ctx, floretruntime.RunTurnRequest{
				RunID: "run-2", ThreadID: threadID, TurnID: "turn-2",
				Input: floretruntime.TurnInput{Text: "continue"},
			})
			if err != nil || next.Status != floretruntime.TurnStatusCompleted {
				t.Fatalf("next turn after recovery result=%#v err=%v, want completed", next, err)
			}
		})
	}
}

func TestPublicOpenSQLiteStoreMapsInvalidAuthorityGraph(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "invalid-authority.db")
	store, err := floretruntime.OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := configurePublicErrorCapabilities(t, store)
	create, err := capabilities.create.Bind("root", "create-root")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: "root", CreateIntentID: "create-root"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE threads
		SET parent_thread_id = 'missing-parent', parent_turn_id = 'parent-turn', task_name = 'child', agent_path = 'child'
		WHERE id = 'root'`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := floretruntime.OpenSQLiteStore(path)
	if reopened != nil {
		_ = reopened.Close()
		t.Fatal("OpenSQLiteStore opened an invalid authority graph")
	}
	if !errors.Is(err, floretruntime.ErrThreadAuthorityInvariant) {
		t.Fatalf("OpenSQLiteStore error = %v, want ErrThreadAuthorityInvariant", err)
	}
}

type publicErrorArgs struct {
	Text string `json:"text"`
}

func newPublicPendingTurnHost(t *testing.T, binder *floretruntime.TurnExecutionHostBinder, threadID floretruntime.ThreadID) (*floretruntime.TurnExecutionHost, *publicAllowEffectGate) {
	t.Helper()
	registry := tools.NewRegistry()
	definition := func(name string) tools.Definition {
		return tools.Definition{
			Name:        name,
			InputSchema: tools.StrictObject(map[string]any{"text": tools.String("text")}, []string{"text"}),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		}
	}
	if err := registry.Register(tools.Define[publicErrorArgs](definition("ordinary_tool"), nil, nil, func(context.Context, tools.Invocation[publicErrorArgs]) (tools.Result, error) {
		return tools.Result{Text: "done"}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tools.Define[publicErrorArgs](definition("pending_tool"), nil, nil, func(context.Context, tools.Invocation[publicErrorArgs]) (tools.Result, error) {
		return tools.Result{Pending: &tools.PendingToolResult{
			Handle:      "pending:job:1",
			State:       tools.PendingToolResultRunning,
			Summary:     "running",
			Instruction: "wait",
		}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	gateway := &publicToolGateway{}
	effectGate := &publicAllowEffectGate{byCallID: make(map[string]string)}
	factory, err := binder.Bind(threadID)
	if err != nil {
		t.Fatal(err)
	}
	host, err := factory.NewHost(context.Background(), floretruntime.TurnExecutionHostOptions{
		Config:                  publicGatewayConfig(),
		ModelGateway:            gateway,
		ModelGatewayIdentity:    publicGatewayIdentity(),
		Tools:                   registry,
		EffectAuthorizationGate: effectGate,
	})
	if err != nil {
		t.Fatal(err)
	}
	return host, effectGate
}

type publicAllowEffectGate struct {
	mu       sync.Mutex
	byCallID map[string]string
}

func (g *publicAllowEffectGate) Dispatch(ctx context.Context, req floretruntime.EffectAuthorizationRequest, effect floretruntime.AuthorizedEffect) (floretruntime.EffectDispatchResult, error) {
	g.mu.Lock()
	g.byCallID[req.ToolCallID] = req.EffectAttemptID
	g.mu.Unlock()
	return effect(ctx, floretruntime.EffectAuthorizationProof{
		EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
		LeaseOwnerID: req.LeaseOwnerID, LeaseGeneration: req.LeaseGeneration,
		PolicyRevision: "public-test-v1", AuditReference: "public-test-audit-" + req.EffectAttemptID,
		AuditHash: "public-test-audit-hash", AuthorizedAt: time.Now(),
	})
}

func (g *publicAllowEffectGate) effectAttemptID(callID string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.byCallID[callID]
}

func newPublicFakeTurnHost(t *testing.T, binder *floretruntime.TurnExecutionHostBinder, threadID floretruntime.ThreadID) *floretruntime.TurnExecutionHost {
	t.Helper()
	factory, err := binder.Bind(threadID)
	if err != nil {
		t.Fatal(err)
	}
	host, err := factory.NewHost(context.Background(), floretruntime.TurnExecutionHostOptions{Config: publicFakeConfig()})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func publicSettlementRequest(toolCallID, toolName, handle string, status floretruntime.PendingToolSettlementStatus) floretruntime.PendingToolSettlementRequest {
	return floretruntime.PendingToolSettlementRequest{
		Target: floretruntime.PendingToolSettlementTarget{
			ThreadID:   "pending",
			TurnID:     "turn-1",
			RunID:      "run-1",
			ToolCallID: toolCallID,
			ToolName:   toolName,
			Handle:     handle,
		},
		Status:  status,
		Summary: "settled",
	}
}

func publicFakeConfig() config.Config {
	return config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		FakeResponse: "done",
		SystemPrompt: "test",
	}
}

func publicGatewayConfig() config.Config {
	return config.Config{
		SystemPrompt: "test",
		ContextPolicy: config.ContextPolicy{
			ContextWindowTokens: config.DefaultContextWindowTokens,
		},
	}
}

func publicGatewayIdentity() floretruntime.ModelGatewayIdentity {
	return floretruntime.ModelGatewayIdentity{
		Provider:              "public-error-test",
		Model:                 "fake-model",
		StateCompatibilityKey: "public-error-test:fake-model",
	}
}

type publicToolGateway struct {
	mu   sync.Mutex
	step int
}

type cancelAfterPendingGateway struct {
	mu      sync.Mutex
	step    int
	waiting chan struct{}
	once    sync.Once
}

func newCancelAfterPendingGateway() *cancelAfterPendingGateway {
	return &cancelAfterPendingGateway{waiting: make(chan struct{})}
}

func (g *cancelAfterPendingGateway) StreamModel(ctx context.Context, _ floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	g.mu.Lock()
	g.step++
	step := g.step
	g.mu.Unlock()
	if step == 1 {
		events := make(chan floretruntime.ModelEvent, 2)
		events <- floretruntime.ModelEvent{
			Type: floretruntime.ModelEventToolCalls,
			ToolCalls: []tools.ToolCall{
				{ID: "pending-1", Name: "pending_tool", Args: `{"text":"pending"}`},
			},
		}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "tool_calls"}
		close(events)
		return events, nil
	}
	if step == 2 {
		g.once.Do(func() { close(g.waiting) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	events := make(chan floretruntime.ModelEvent, 2)
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: "done"}
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "stop"}
	close(events)
	return events, nil
}

func (g *publicToolGateway) StreamModel(context.Context, floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	g.mu.Lock()
	g.step++
	step := g.step
	g.mu.Unlock()
	events := make(chan floretruntime.ModelEvent, 2)
	switch step {
	case 1:
		events <- floretruntime.ModelEvent{
			Type: floretruntime.ModelEventToolCalls,
			ToolCalls: []tools.ToolCall{
				{ID: "ordinary-1", Name: "ordinary_tool", Args: `{"text":"ordinary"}`},
				{ID: "pending-1", Name: "pending_tool", Args: `{"text":"pending"}`},
			},
		}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "tool_calls"}
	default:
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: "done"}
		events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "stop"}
	}
	close(events)
	return events, nil
}

type blockingPublicGateway struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingPublicGateway() *blockingPublicGateway {
	return &blockingPublicGateway{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *blockingPublicGateway) StreamModel(ctx context.Context, _ floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-g.release:
	}
	events := make(chan floretruntime.ModelEvent, 2)
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: "done"}
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "stop"}
	close(events)
	return events, nil
}

func (g *blockingPublicGateway) releaseRun() {
	select {
	case <-g.release:
	default:
		close(g.release)
	}
}
