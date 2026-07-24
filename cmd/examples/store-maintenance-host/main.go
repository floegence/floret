package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	floretruntime "github.com/floegence/floret/runtime"
)

type maintenanceFunctions struct {
	inspect func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error)
	verify  func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreVerification, error)
	migrate func(context.Context, string, floretruntime.SQLiteStoreMigrationRequest, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreMigrationResult, error)
}

var publicMaintenanceFunctions = maintenanceFunctions{
	inspect: floretruntime.InspectSQLiteStore,
	verify:  floretruntime.VerifySQLiteStore,
	migrate: floretruntime.MigrateSQLiteStore,
}

type typedOutcome struct {
	StoreState      floretruntime.SQLiteStoreState
	OperationStatus floretruntime.SQLiteStoreMaintenanceStatus
	Reason          floretruntime.SQLiteStoreReason
	Retryable       bool
	SafeToRetry     bool
	Committed       bool
	RolledBack      bool
}

type fileSnapshot struct {
	Name   string
	Exists bool
	Size   int64
	SHA256 [sha256.Size]byte
}

func main() {
	var databasePath string
	flag.StringVar(&databasePath, "db", "", "path to the SQLite store")
	flag.Parse()

	cleanup := func() {}
	if databasePath == "" {
		directory, err := os.MkdirTemp("", "floret-store-maintenance-")
		if err != nil {
			log.Fatal(err)
		}
		cleanup = func() { _ = os.RemoveAll(directory) }
		databasePath = filepath.Join(directory, "floret.db")
	}
	defer cleanup()

	if err := run(context.Background(), databasePath, os.Stdout, publicMaintenanceFunctions); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, databasePath string, output io.Writer, api maintenanceFunctions) error {
	if err := validateMaintenanceFunctions(api); err != nil {
		return err
	}
	store, err := floretruntime.OpenSQLiteStore(databasePath)
	if err != nil {
		return err
	}
	if err := store.Close(); err != nil {
		return err
	}

	beforeInspect, err := snapshotStoreFiles(databasePath)
	if err != nil {
		return err
	}
	inspection, err := api.inspect(ctx, databasePath)
	if err != nil {
		return err
	}
	afterInspect, err := snapshotStoreFiles(databasePath)
	if err != nil {
		return err
	}
	if !equalFileSnapshots(beforeInspect, afterInspect) {
		return fmt.Errorf("inspection changed store files")
	}
	if inspection.State != floretruntime.SQLiteStoreStateCurrent || inspection.Kind != floretruntime.SQLiteStoreKindFloret {
		return fmt.Errorf("unexpected current-store inspection state=%s kind=%s", inspection.State, inspection.Kind)
	}
	fmt.Fprintf(output, "inspect state=%s reason=%s zero_write=true\n", inspection.State, inspection.Reason)

	plan, planProgress, err := runMigrationOperation(ctx, api, databasePath, "store-maintenance-plan", floretruntime.SQLiteStoreMigrationPlan, inspection.Observed)
	if err != nil {
		return err
	}
	if plan.Status != floretruntime.SQLiteStoreMaintenanceReady || plan.Changed || plan.Committed || plan.RolledBack {
		return fmt.Errorf("unexpected migration plan result: %#v", plan)
	}
	fmt.Fprintf(output, "migrate operation=%s mode=%s status=%s changed=%t progress_events=%d\n",
		plan.OperationID, plan.Mode, plan.Status, plan.Changed, len(planProgress))

	apply, applyProgress, err := runMigrationOperation(ctx, api, databasePath, "store-maintenance-apply", floretruntime.SQLiteStoreMigrationApply, inspection.Observed)
	if err != nil {
		return err
	}
	if apply.Status != floretruntime.SQLiteStoreMaintenanceReady || apply.Changed || apply.Committed || apply.RolledBack {
		return fmt.Errorf("unexpected current-store apply result: %#v", apply)
	}
	fmt.Fprintf(output, "migrate operation=%s mode=%s status=%s changed=%t progress_events=%d\n",
		apply.OperationID, apply.Mode, apply.Status, apply.Changed, len(applyProgress))

	beforeVerify, err := snapshotStoreFiles(databasePath)
	if err != nil {
		return err
	}
	verification, err := api.verify(ctx, databasePath)
	if err != nil {
		return err
	}
	afterVerify, err := snapshotStoreFiles(databasePath)
	if err != nil {
		return err
	}
	if !equalFileSnapshots(beforeVerify, afterVerify) {
		return fmt.Errorf("verification changed store files")
	}
	for _, check := range verification.Checks {
		if !check.Passed {
			return fmt.Errorf("verification check %s failed", check.Code)
		}
	}
	fmt.Fprintf(output, "verify state=%s checks=%d zero_write=true\n", verification.Inspection.State, len(verification.Checks))
	return nil
}

func runMigrationOperation(
	ctx context.Context,
	api maintenanceFunctions,
	databasePath string,
	operationID string,
	mode floretruntime.SQLiteStoreMigrationMode,
	expected floretruntime.StoreSchemaIdentity,
) (floretruntime.SQLiteStoreMigrationResult, []floretruntime.SQLiteStoreMaintenanceProgress, error) {
	progress := make([]floretruntime.SQLiteStoreMaintenanceProgress, 0, 4)
	result, err := api.migrate(ctx, databasePath, floretruntime.SQLiteStoreMigrationRequest{
		OperationID: operationID,
		Mode:        mode,
		ExpectedSchema: floretruntime.StoreSchemaIdentity{
			Version: expected.Version, Fingerprint: expected.Fingerprint,
		},
		Progress: func(update floretruntime.SQLiteStoreMaintenanceProgress) {
			progress = append(progress, update)
		},
	})
	if progressErr := validateMonotonicProgress(operationID, progress); progressErr != nil {
		return result, progress, progressErr
	}
	return result, progress, err
}

func validateMonotonicProgress(operationID string, progress []floretruntime.SQLiteStoreMaintenanceProgress) error {
	if len(progress) == 0 {
		return fmt.Errorf("migration operation %q emitted no progress", operationID)
	}
	phaseRank := map[floretruntime.SQLiteStoreMaintenancePhase]int{
		floretruntime.SQLiteStoreMaintenancePreflight: 0,
		floretruntime.SQLiteStoreMaintenanceWaiting:   1,
		floretruntime.SQLiteStoreMaintenanceMigrating: 2,
		floretruntime.SQLiteStoreMaintenanceVerifying: 3,
	}
	var previousSequence uint64
	previousStep := 0
	previousPhase := -1
	committed := false
	rolledBack := false
	for _, update := range progress {
		if update.OperationID != operationID {
			return fmt.Errorf("progress operation identity changed from %q to %q", operationID, update.OperationID)
		}
		rank, known := phaseRank[update.Phase]
		if !known || update.Sequence <= previousSequence || update.Step < previousStep || rank < previousPhase {
			return fmt.Errorf("migration progress is not monotonic")
		}
		if (committed && !update.Committed) || (rolledBack && !update.RolledBack) || (update.Committed && update.RolledBack) {
			return fmt.Errorf("migration progress terminal flags regressed")
		}
		previousSequence = update.Sequence
		previousStep = update.Step
		previousPhase = rank
		committed = update.Committed
		rolledBack = update.RolledBack
	}
	return nil
}

func typedMaintenanceOutcome(inspection floretruntime.SQLiteStoreInspection, migration floretruntime.SQLiteStoreMigrationResult, err error) typedOutcome {
	outcome := typedOutcome{
		StoreState: inspection.State, Reason: inspection.Reason,
		Retryable: inspection.Retryable, SafeToRetry: inspection.SafeToRetry,
	}
	if migration.Status != "" || migration.Before.State != "" {
		outcome.StoreState = migration.Before.State
		outcome.OperationStatus = migration.Status
		outcome.Reason = migration.Reason
		outcome.Retryable = migration.Retryable
		outcome.SafeToRetry = migration.SafeToRetry
		outcome.Committed = migration.Committed
		outcome.RolledBack = migration.RolledBack
	}
	var maintenanceErr *floretruntime.SQLiteStoreMaintenanceError
	if errors.As(err, &maintenanceErr) {
		outcome.Reason = maintenanceErr.Reason
		outcome.Retryable = maintenanceErr.Retryable
		outcome.SafeToRetry = maintenanceErr.SafeToRetry
		if maintenanceErr.Reason == floretruntime.SQLiteStoreReasonBusy {
			outcome.StoreState = floretruntime.SQLiteStoreStateBusy
		}
	}
	return outcome
}

func inspectTypedOutcome(ctx context.Context, api maintenanceFunctions, databasePath string) (typedOutcome, error) {
	inspection, err := api.inspect(ctx, databasePath)
	return typedMaintenanceOutcome(inspection, floretruntime.SQLiteStoreMigrationResult{}, err), err
}

func migrateTypedOutcome(
	ctx context.Context,
	api maintenanceFunctions,
	databasePath string,
	request floretruntime.SQLiteStoreMigrationRequest,
) (typedOutcome, error) {
	migration, err := api.migrate(ctx, databasePath, request)
	return typedMaintenanceOutcome(floretruntime.SQLiteStoreInspection{}, migration, err), err
}

func snapshotStoreFiles(databasePath string) ([]fileSnapshot, error) {
	paths := []string{databasePath, databasePath + "-wal", databasePath + "-shm", databasePath + "-journal"}
	snapshots := make([]fileSnapshot, 0, len(paths))
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			snapshots = append(snapshots, fileSnapshot{Name: filepath.Base(path)})
			continue
		}
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, fileSnapshot{
			Name: filepath.Base(path), Exists: true, Size: info.Size(), SHA256: sha256.Sum256(content),
		})
	}
	return snapshots, nil
}

func equalFileSnapshots(left, right []fileSnapshot) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateMaintenanceFunctions(api maintenanceFunctions) error {
	if api.inspect == nil || api.verify == nil || api.migrate == nil {
		return fmt.Errorf("store maintenance public API functions are required")
	}
	return nil
}
