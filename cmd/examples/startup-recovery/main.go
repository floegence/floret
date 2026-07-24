package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

const (
	recoveryThreadID = floretruntime.ThreadID("recovery-thread")
	recoveryTurnID   = floretruntime.TurnID("interrupted-turn")
	recoveryRunID    = floretruntime.RunID("interrupted-run")
	pendingCallID    = "pending-call"
	pendingToolName  = "start_job"
	pendingHandle    = "job:example:1"
)

var leasePolicy = floretruntime.StoreLeasePolicy{
	TTL: 300 * time.Millisecond, RenewInterval: 100 * time.Millisecond, ClockSkewAllowance: 50 * time.Millisecond,
}

type jobArgs struct {
	Name string `json:"name"`
}

type crashGateway struct {
	step int
}

func (g *crashGateway) StreamModel(_ context.Context, _ floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	g.step++
	if g.step == 2 {
		os.Exit(0)
	}
	events := make(chan floretruntime.ModelEvent, 2)
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
		ID: pendingCallID, Name: pendingToolName, Args: `{"name":"index"}`,
	}}}
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "tool_calls"}
	close(events)
	return events, nil
}

type allowGate struct{}

func (allowGate) Dispatch(ctx context.Context, req floretruntime.EffectAuthorizationRequest, effect floretruntime.AuthorizedEffect) (floretruntime.EffectDispatchResult, error) {
	return effect(ctx, floretruntime.EffectAuthorizationProof{
		EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
		LeaseOwnerID: req.LeaseOwnerID, LeaseGeneration: req.LeaseGeneration,
		PolicyRevision: "startup-recovery-example-v1",
		AuditReference: "startup-recovery:" + req.EffectAttemptID,
		AuditHash:      "startup-recovery-audit-hash", AuthorizedAt: time.Now().UTC(),
	})
}

func main() {
	var databasePath string
	var crashChild bool
	flag.StringVar(&databasePath, "db", "", "path to the SQLite store")
	flag.BoolVar(&crashChild, "crash-child", false, "run the intentionally interrupted child process")
	flag.Parse()

	if crashChild {
		if databasePath == "" {
			log.Fatal("-db is required in crash-child mode")
		}
		if err := runCrashChild(context.Background(), databasePath); err != nil {
			log.Fatal(err)
		}
		return
	}

	cleanup := func() {}
	if databasePath == "" {
		directory, err := os.MkdirTemp("", "floret-startup-recovery-")
		if err != nil {
			log.Fatal(err)
		}
		cleanup = func() { _ = os.RemoveAll(directory) }
		databasePath = filepath.Join(directory, "floret.db")
	}
	defer cleanup()

	ctx := context.Background()
	launchInterruptedChild := func(path string) error {
		command := exec.CommandContext(ctx, os.Args[0], "-crash-child", "-db", path)
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		return command.Run()
	}
	if err := runRecovery(ctx, databasePath, launchInterruptedChild); err != nil {
		log.Fatal(err)
	}
}

func runCrashChild(ctx context.Context, databasePath string) error {
	store, err := openRecoveryStore(ctx, databasePath)
	if err != nil {
		return err
	}
	// Deliberately do not defer Store.Close: the second gateway call exits the
	// process to leave canonical recovery work for the next host instance.

	var createBinder *floretruntime.ThreadCreateHostBinder
	var turnBinder *floretruntime.TurnExecutionHostBinder
	if err := floretruntime.ConfigureHostCapabilities(store, func(bootstrap *floretruntime.HostBootstrap) error {
		var configureErr error
		createBinder, configureErr = floretruntime.NewThreadCreateHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		turnBinder, configureErr = floretruntime.NewTurnExecutionHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		return err
	}

	const createIntentID = floretruntime.CreateIntentID("create-recovery-thread")
	createHost, err := createBinder.Bind(recoveryThreadID, createIntentID)
	if err != nil {
		return err
	}
	if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{
		ThreadID: recoveryThreadID, CreateIntentID: createIntentID,
	}); err != nil {
		return err
	}

	registry := tools.NewRegistry(tools.Define[jobArgs](
		tools.Definition{
			Name: pendingToolName, Title: "Start job", Description: "Starts host-owned pending work.",
			InputSchema: tools.StrictObject(map[string]any{"name": tools.String("job name")}, []string{"name"}),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil, nil,
		func(context.Context, tools.Invocation[jobArgs]) (tools.Result, error) {
			return tools.Result{Pending: &tools.PendingToolResult{
				Handle: pendingHandle, State: tools.PendingToolResultRunning,
				Summary: "Indexing started", Instruction: "Wait for the host to settle this job.",
			}}, nil
		},
	))
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	factory, err := turnBinder.Bind(recoveryThreadID)
	if err != nil {
		return err
	}
	host, err := factory.NewHost(ctx, floretruntime.TurnExecutionHostOptions{
		Config: config.Config{
			SystemPrompt:  "Start the requested job.",
			ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
		},
		ModelGateway: &crashGateway{},
		ModelGatewayIdentity: floretruntime.ModelGatewayIdentity{
			Provider: "recovery-example", Model: "local-scripted-model", StateCompatibilityKey: "recovery-example:v1",
		},
		ModelGatewayCapabilities: floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning},
		Tools:                    registry, EffectAuthorizationGate: allowGate{},
	})
	if err != nil {
		return err
	}
	_, err = host.RunTurn(ctx, floretruntime.RunTurnRequest{
		ThreadID: recoveryThreadID, TurnID: recoveryTurnID, RunID: recoveryRunID,
		Input: floretruntime.TurnInput{Text: "Start the indexing job."},
	})
	return err
}

func runRecovery(ctx context.Context, databasePath string, launchInterruptedChild func(string) error) error {
	if launchInterruptedChild == nil {
		return fmt.Errorf("interrupted child launcher is required")
	}
	if err := launchInterruptedChild(databasePath); err != nil {
		return fmt.Errorf("run interrupted child: %w", err)
	}
	time.Sleep(leasePolicy.TTL + leasePolicy.ClockSkewAllowance + 100*time.Millisecond)

	store, err := openRecoveryStore(ctx, databasePath)
	if err != nil {
		return err
	}
	defer store.Close()

	var interruptedBinder *floretruntime.InterruptedTurnRecoveryHostBinder
	var pendingBinder *floretruntime.PendingToolRecoveryHostBinder
	var readBinder *floretruntime.ThreadReadHostBinder
	if err := floretruntime.ConfigureHostCapabilities(store, func(bootstrap *floretruntime.HostBootstrap) error {
		var configureErr error
		interruptedBinder, configureErr = floretruntime.NewInterruptedTurnRecoveryHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		pendingBinder, configureErr = floretruntime.NewPendingToolRecoveryHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		readBinder, configureErr = floretruntime.NewThreadReadHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		return err
	}

	readHost, err := readBinder.NewHost(ctx, recoveryThreadID)
	if err != nil {
		return err
	}
	detail, err := readHost.ListThreadDetailEvents(ctx, floretruntime.ListThreadDetailEventsRequest{
		ThreadID: recoveryThreadID, IncludeRaw: false,
	})
	if err != nil {
		return err
	}
	var effectAttemptID string
	for _, event := range detail.Events {
		if event.ToolResult != nil && event.ToolResult.CallID == pendingCallID {
			effectAttemptID = event.ToolResult.EffectAttemptID
		}
	}
	if effectAttemptID == "" {
		return fmt.Errorf("canonical pending tool result omitted effect attempt identity")
	}

	factory, err := interruptedBinder.BindThread(ctx, recoveryThreadID)
	if err != nil {
		return err
	}
	recoveryHost, err := factory.NewHost(ctx, nil)
	if err != nil {
		return err
	}
	recovered, err := recoveryHost.RecoverInterruptedTurn(ctx)
	if err != nil {
		return err
	}
	if recovered.Status != floretruntime.TurnStatusInterrupted {
		return fmt.Errorf("recovered turn status=%s", recovered.Status)
	}

	pendingHost, err := pendingBinder.NewThreadHost(ctx, recoveryThreadID, nil)
	if err != nil {
		return err
	}
	settled, err := pendingHost.SettlePendingTool(ctx, floretruntime.PendingToolSettlementRequest{
		Target: floretruntime.PendingToolSettlementTarget{
			ThreadID: recoveryThreadID, TurnID: recoveryTurnID, RunID: recoveryRunID,
			ToolCallID: pendingCallID, ToolName: pendingToolName, Handle: pendingHandle,
			EffectAttemptID: effectAttemptID,
		},
		Status:  floretruntime.PendingToolSettlementCompleted,
		Summary: "Indexing completed after restart.",
		Output:  "indexed 12 documents",
	})
	if err != nil {
		return err
	}

	page, err := readHost.ListThreadTurns(ctx, floretruntime.ListThreadTurnsRequest{ThreadID: recoveryThreadID, Tail: 1})
	if err != nil {
		return err
	}
	if len(page.Turns) != 1 || page.Turns[0].Status != floretruntime.TurnStatusInterrupted {
		return fmt.Errorf("canonical reread did not preserve interrupted lifecycle: %#v", page.Turns)
	}
	settledDetail, err := readHost.ListThreadDetailEvents(ctx, floretruntime.ListThreadDetailEventsRequest{
		ThreadID: recoveryThreadID, IncludeRaw: false,
	})
	if err != nil {
		return err
	}
	settlementFound := false
	for _, event := range settledDetail.Events {
		if event.ID == settled.Event.ID && event.ToolResult != nil && event.Metadata["state"] == "completed" {
			settlementFound = true
			break
		}
	}
	if !settlementFound {
		return fmt.Errorf("canonical reread omitted completed pending-tool settlement")
	}
	fmt.Printf("turn=%s recovery=%s pending_tool=completed canonical_status=%s\n",
		recovered.TurnID, recovered.Status, page.Turns[0].Status)
	return nil
}

func openRecoveryStore(ctx context.Context, databasePath string) (*floretruntime.Store, error) {
	storeOption := floretruntime.WithSQLiteStoreLeasePolicy(leasePolicy)
	inspection, err := floretruntime.InspectSQLiteStore(ctx, databasePath, storeOption)
	if err != nil {
		return nil, err
	}
	if inspection.State == floretruntime.SQLiteStoreStateMissing || inspection.State == floretruntime.SQLiteStoreStateEmpty {
		return floretruntime.OpenSQLiteStore(ctx, databasePath, floretruntime.SQLiteStoreOpenRequest{ExpectedState: inspection.State}, storeOption)
	}
	if inspection.State == floretruntime.SQLiteStoreStateUpgradeable {
		const operationID = "startup-recovery-store-migration"
		result, err := floretruntime.MigrateSQLiteStore(ctx, databasePath, floretruntime.SQLiteStoreMigrationRequest{
			OperationID: operationID,
			Mode:        floretruntime.SQLiteStoreMigrationApply,
			ExpectedSchema: floretruntime.StoreSchemaIdentity{
				Version: inspection.Observed.Version, Fingerprint: inspection.Observed.Fingerprint,
			},
		}, storeOption)
		if err != nil {
			return nil, err
		}
		if result.OperationID != operationID || result.Status != floretruntime.SQLiteStoreMaintenanceReady ||
			!result.Changed || !result.Committed || result.RolledBack {
			return nil, fmt.Errorf("store migration outcome operation=%q status=%q changed=%t committed=%t rolled_back=%t",
				result.OperationID, result.Status, result.Changed, result.Committed, result.RolledBack)
		}
	}
	verification, err := floretruntime.VerifySQLiteStore(ctx, databasePath, storeOption)
	if err != nil {
		return nil, err
	}
	if verification.Inspection.State != floretruntime.SQLiteStoreStateCurrent ||
		verification.Inspection.LeasePolicyState != floretruntime.SQLiteStoreLeasePolicyMatches {
		return nil, fmt.Errorf("store verification state=%q lease=%q", verification.Inspection.State, verification.Inspection.LeasePolicyState)
	}
	for _, check := range verification.Checks {
		if !check.Passed {
			return nil, fmt.Errorf("store verification check %q failed", check.Code)
		}
	}
	return floretruntime.OpenSQLiteStore(ctx, databasePath, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState:  verification.Inspection.State,
		ExpectedSchema: verification.Inspection.Observed,
	}, storeOption)
}
