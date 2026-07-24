package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
)

const (
	threadID = floretruntime.ThreadID("example-thread")
	turnID   = floretruntime.TurnID("example-turn")
	runID    = floretruntime.RunID("example-run")
)

func main() {
	var databasePath string
	flag.StringVar(&databasePath, "db", "", "path to the SQLite store")
	flag.Parse()

	cleanup := func() {}
	if databasePath == "" {
		directory, err := os.MkdirTemp("", "floret-minimal-host-")
		if err != nil {
			log.Fatal(err)
		}
		cleanup = func() { _ = os.RemoveAll(directory) }
		databasePath = filepath.Join(directory, "floret.db")
	}
	defer cleanup()

	if err := run(context.Background(), databasePath); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, databasePath string) error {
	store, err := openStore(ctx, databasePath)
	if err != nil {
		return err
	}

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
		_ = store.Close()
		return err
	}

	const createIntentID = floretruntime.CreateIntentID("create-example-thread")
	createHost, err := createBinder.Bind(threadID, createIntentID)
	if err != nil {
		_ = store.Close()
		return err
	}
	if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{
		ThreadID: threadID, CreateIntentID: createIntentID,
	}); err != nil {
		_ = store.Close()
		return err
	}

	turnFactory, err := turnBinder.Bind(threadID)
	if err != nil {
		_ = store.Close()
		return err
	}
	turnHost, err := turnFactory.NewHost(ctx, floretruntime.TurnExecutionHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "The durable turn is complete.",
			SystemPrompt: "Answer briefly.",
		},
	})
	if err != nil {
		_ = store.Close()
		return err
	}
	result, err := turnHost.RunTurn(ctx, floretruntime.RunTurnRequest{
		ThreadID: threadID,
		TurnID:   turnID,
		RunID:    runID,
		Input:    floretruntime.TurnInput{Text: "Confirm durable execution."},
	})
	if err != nil {
		_ = store.Close()
		return err
	}
	if err := store.Close(); err != nil {
		return err
	}

	reopened, err := openStore(ctx, databasePath)
	if err != nil {
		return err
	}
	defer reopened.Close()
	var readBinder *floretruntime.ThreadReadHostBinder
	if err := floretruntime.ConfigureHostCapabilities(reopened, func(bootstrap *floretruntime.HostBootstrap) error {
		var configureErr error
		readBinder, configureErr = floretruntime.NewThreadReadHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		return err
	}
	readHost, err := readBinder.NewHost(ctx, threadID)
	if err != nil {
		return err
	}
	projection, err := readHost.ReadTurnProjection(ctx, floretruntime.ReadTurnProjectionRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID,
	})
	if err != nil {
		return err
	}

	fmt.Printf("thread=%s turn=%s status=%s output=%q segments=%d\n",
		threadID, turnID, projection.Status, result.Output, len(projection.Segments))
	return nil
}

func openStore(ctx context.Context, databasePath string) (*floretruntime.Store, error) {
	inspection, err := floretruntime.InspectSQLiteStore(ctx, databasePath)
	if err != nil {
		return nil, err
	}
	if inspection.State == floretruntime.SQLiteStoreStateMissing || inspection.State == floretruntime.SQLiteStoreStateEmpty {
		return floretruntime.OpenSQLiteStore(ctx, databasePath, floretruntime.SQLiteStoreOpenRequest{ExpectedState: inspection.State})
	}
	if inspection.State == floretruntime.SQLiteStoreStateUpgradeable {
		const operationID = "minimal-durable-host-startup"
		result, err := floretruntime.MigrateSQLiteStore(ctx, databasePath, floretruntime.SQLiteStoreMigrationRequest{
			OperationID: operationID,
			Mode:        floretruntime.SQLiteStoreMigrationApply,
			ExpectedSchema: floretruntime.StoreSchemaIdentity{
				Version: inspection.Observed.Version, Fingerprint: inspection.Observed.Fingerprint,
			},
		})
		if err != nil {
			return nil, err
		}
		if result.OperationID != operationID || result.Status != floretruntime.SQLiteStoreMaintenanceReady ||
			!result.Changed || !result.Committed || result.RolledBack {
			return nil, fmt.Errorf("store migration outcome operation=%q status=%q changed=%t committed=%t rolled_back=%t",
				result.OperationID, result.Status, result.Changed, result.Committed, result.RolledBack)
		}
	}
	verification, err := floretruntime.VerifySQLiteStore(ctx, databasePath)
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
	})
}
