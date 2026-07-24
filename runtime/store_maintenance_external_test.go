package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	floretruntime "github.com/floegence/floret/runtime"
)

func TestPublicOpenSQLiteStoreCreatesOnlyFromEmptyFacts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "missing", "store.db")
	inspection, err := floretruntime.InspectSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: inspection.State,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := floretruntime.InspectSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != floretruntime.SQLiteStoreStateCurrent || after.LeasePolicyState != floretruntime.SQLiteStoreLeasePolicyMatches {
		t.Fatalf("inspection after create = %#v", after)
	}
}

func TestPublicOpenSQLiteStoreOpensExactVerifiedCurrentStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "current.db")
	store, err := openPublicSQLiteStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := floretruntime.VerifySQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range report.Checks {
		if !check.Passed {
			t.Fatalf("verification check = %#v", check)
		}
	}
	reopened, err := floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState:  report.Inspection.State,
		ExpectedSchema: report.Inspection.Observed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicOpenSQLiteStoreRejectsChangedSchemaIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "stale.db")
	store, err := openPublicSQLiteStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := floretruntime.VerifySQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE schema_meta SET value = 'changed-after-verification' WHERE key = 'schema_fingerprint'`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState:  report.Inspection.State,
		ExpectedSchema: report.Inspection.Observed,
	})
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonFingerprint || maintenance.SafeToRetry {
		t.Fatalf("changed-schema open error = %#v, err=%v", maintenance, err)
	}
	var fingerprint string
	checkDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer checkDB.Close()
	if err := checkDB.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'schema_fingerprint'`).Scan(&fingerprint); err != nil {
		t.Fatal(err)
	}
	if fingerprint != "changed-after-verification" {
		t.Fatalf("stale open rewrote fingerprint = %q", fingerprint)
	}
}

func TestPublicOpenSQLiteStoreRejectsStaleMissingInspection(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "stale-missing.db")
	inspection, err := floretruntime.InspectSQLiteStore(ctx, path)
	if err != nil || inspection.State != floretruntime.SQLiteStoreStateMissing {
		t.Fatalf("missing inspection = %#v, err=%v", inspection, err)
	}
	store, err := openPublicSQLiteStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: inspection.State,
	})
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonInspectionStale || !maintenance.SafeToRetry {
		t.Fatalf("stale missing open error = %#v, err=%v", maintenance, err)
	}
}

func TestPublicOpenSQLiteStoreCurrentMissingPathCreatesNothing(t *testing.T) {
	ctx := context.Background()
	verifiedPath := filepath.Join(t.TempDir(), "verified.db")
	store, err := openPublicSQLiteStoreForTest(verifiedPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := floretruntime.VerifySQLiteStore(ctx, verifiedPath)
	if err != nil {
		t.Fatal(err)
	}

	parent := filepath.Join(t.TempDir(), "missing", "nested")
	path := filepath.Join(parent, "store.db")
	opened, err := floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState:  report.Inspection.State,
		ExpectedSchema: report.Inspection.Observed,
	})
	if opened != nil {
		_ = opened.Close()
		t.Fatal("stale current open returned a Store")
	}
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonInspectionStale {
		t.Fatalf("missing current open error = %#v, err=%v", maintenance, err)
	}
	for _, candidate := range []string{parent, path, path + "-wal", path + "-shm", path + "-journal"} {
		if _, statErr := os.Stat(candidate); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("missing current open created %q: %v", candidate, statErr)
		}
	}
	inspection, inspectErr := floretruntime.InspectSQLiteStore(ctx, path)
	if inspectErr != nil || inspection.State != floretruntime.SQLiteStoreStateMissing {
		t.Fatalf("inspection after rejected current open = %#v, err=%v", inspection, inspectErr)
	}
	if _, statErr := os.Stat(parent); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("post-failure inspection created parent: %v", statErr)
	}
}

func TestPublicOpenSQLiteStoreCurrentEmptyPathChangesNothing(t *testing.T) {
	ctx := context.Background()
	verifiedPath := filepath.Join(t.TempDir(), "verified.db")
	store, err := openPublicSQLiteStoreForTest(verifiedPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := floretruntime.VerifySQLiteStore(ctx, verifiedPath)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "empty.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	opened, err := floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState:  report.Inspection.State,
		ExpectedSchema: report.Inspection.Observed,
	})
	if opened != nil {
		_ = opened.Close()
		t.Fatal("stale current open returned a Store")
	}
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonInspectionStale {
		t.Fatalf("empty current open error = %#v, err=%v", maintenance, err)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil || len(content) != 0 {
		t.Fatalf("empty current open changed database: len=%d err=%v", len(content), readErr)
	}
	for _, sidecar := range []string{path + "-wal", path + "-shm", path + "-journal"} {
		if _, statErr := os.Stat(sidecar); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("empty current open created %q: %v", sidecar, statErr)
		}
	}
	inspection, inspectErr := floretruntime.InspectSQLiteStore(ctx, path)
	if inspectErr != nil || inspection.State != floretruntime.SQLiteStoreStateEmpty {
		t.Fatalf("inspection after rejected current open = %#v, err=%v", inspection, inspectErr)
	}
	content, readErr = os.ReadFile(path)
	if readErr != nil || len(content) != 0 {
		t.Fatalf("post-failure inspection changed database: len=%d err=%v", len(content), readErr)
	}
}

func TestPublicOpenSQLiteStoreReportsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := floretruntime.OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "cancelled.db"), floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: floretruntime.SQLiteStoreStateMissing,
	})
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonCancelled {
		t.Fatalf("cancelled open error = %#v, err=%v", maintenance, err)
	}
}

func TestPublicOpenSQLiteStoreRejectsInvalidPreconditionsBeforeAccess(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		request floretruntime.SQLiteStoreOpenRequest
	}{
		{name: "unknown state", request: floretruntime.SQLiteStoreOpenRequest{ExpectedState: "unknown"}},
		{name: "missing with schema", request: floretruntime.SQLiteStoreOpenRequest{
			ExpectedState: floretruntime.SQLiteStoreStateMissing,
			ExpectedSchema: floretruntime.StoreSchemaIdentity{
				Version: "16", Fingerprint: "unexpected",
			},
		}},
		{name: "current without schema", request: floretruntime.SQLiteStoreOpenRequest{ExpectedState: floretruntime.SQLiteStoreStateCurrent}},
		{name: "memory", path: ":memory:", request: floretruntime.SQLiteStoreOpenRequest{ExpectedState: floretruntime.SQLiteStoreStateMissing}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "missing")
			path := test.path
			if path == "" {
				path = filepath.Join(root, "store.db")
			}
			_, err := floretruntime.OpenSQLiteStore(context.Background(), path, test.request)
			var maintenance *floretruntime.SQLiteStoreMaintenanceError
			if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
				maintenance.Reason != floretruntime.SQLiteStoreReasonInvalidRequest {
				t.Fatalf("invalid open error = %#v, err=%v", maintenance, err)
			}
			if test.path == "" {
				if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("invalid open created path: %v", err)
				}
			}
		})
	}
}

func TestPublicOpenSQLiteStoreClassifiesBusyAndIOFailures(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "busy.db")
	store, err := openPublicSQLiteStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := floretruntime.InspectSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), `ROLLBACK`)
	_, err = floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: inspection.State, ExpectedSchema: inspection.Observed,
	})
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Reason != floretruntime.SQLiteStoreReasonBusy ||
		!maintenance.Retryable || !maintenance.SafeToRetry {
		t.Fatalf("busy open error = %#v, err=%v", maintenance, err)
	}

	_, err = floretruntime.OpenSQLiteStore(ctx, t.TempDir(), floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: floretruntime.SQLiteStoreStateMissing,
	})
	maintenance = nil
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonIO {
		t.Fatalf("I/O open error = %#v, err=%v", maintenance, err)
	}
}

func TestPublicSQLiteStoreLeasePolicyOptionPersistsExactPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom-policy.db")
	policy := floretruntime.StoreLeasePolicy{
		TTL: 45 * time.Second, RenewInterval: 15 * time.Second, ClockSkewAllowance: 3 * time.Second,
	}
	ctx := context.Background()
	store, err := floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: floretruntime.SQLiteStoreStateMissing,
	}, floretruntime.WithSQLiteStoreLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := floretruntime.InspectSQLiteStore(ctx, path, floretruntime.WithSQLiteStoreLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != floretruntime.SQLiteStoreStateCurrent || inspection.LeasePolicyState != floretruntime.SQLiteStoreLeasePolicyMatches ||
		inspection.PersistedLeasePolicy == nil || *inspection.PersistedLeasePolicy != policy {
		t.Fatalf("inspection = %#v", inspection)
	}
	reopened, err := floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: inspection.State, ExpectedSchema: inspection.Observed,
	}, floretruntime.WithSQLiteStoreLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: inspection.State, ExpectedSchema: inspection.Observed,
	})
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonLeaseMismatch {
		t.Fatalf("default reopen error = %#v, err=%v", maintenance, err)
	}
}

func TestPublicInspectMissingSQLiteStoreIsNonCreating(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	path := filepath.Join(root, "store.db")
	inspection, err := floretruntime.InspectSQLiteStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != floretruntime.SQLiteStoreStateMissing || inspection.Exists {
		t.Fatalf("inspection = %#v", inspection)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect created parent: %v", err)
	}
}

func TestPublicVerifySQLiteStoreUsesDurableAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify.db")
	store, err := openPublicSQLiteStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := floretruntime.VerifySQLiteStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if report.Inspection.State != floretruntime.SQLiteStoreStateCurrent || len(report.Checks) < 2 {
		t.Fatalf("verification = %#v", report)
	}
}

func TestPublicSQLiteStoreLeasePolicyRejectsInvalidValues(t *testing.T) {
	policy := floretruntime.StoreLeasePolicy{TTL: time.Second, RenewInterval: time.Second}
	if err := policy.Validate(); err == nil {
		t.Fatal("invalid lease policy validated")
	}
	_, err := floretruntime.OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "invalid.db"), floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: floretruntime.SQLiteStoreStateMissing,
	}, floretruntime.WithSQLiteStoreLeasePolicy(policy))
	if err == nil {
		t.Fatal("OpenSQLiteStore accepted invalid lease policy")
	}
}

func TestPublicMaintenanceErrorsClassifyInvalidRequestsWithoutStringParsing(t *testing.T) {
	_, err := floretruntime.InspectSQLiteStore(context.Background(), ":memory:")
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationInspect ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonInvalidRequest || maintenance.Retryable || maintenance.SafeToRetry {
		t.Fatalf("inspection error = %#v, err=%v", maintenance, err)
	}

	_, err = floretruntime.MigrateSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "store.db"), floretruntime.SQLiteStoreMigrationRequest{})
	maintenance = nil
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationMigrate ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonInvalidRequest {
		t.Fatalf("migration error = %#v, err=%v", maintenance, err)
	}
}

func TestPublicInspectionDoesNotOfferReadyActionsForLeaseMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease-mismatch.db")
	store, err := openPublicSQLiteStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	requested := floretruntime.StoreLeasePolicy{
		TTL: 45 * time.Second, RenewInterval: 15 * time.Second, ClockSkewAllowance: 3 * time.Second,
	}
	inspection, err := floretruntime.InspectSQLiteStore(context.Background(), path, floretruntime.WithSQLiteStoreLeasePolicy(requested))
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != floretruntime.SQLiteStoreStateCurrent ||
		inspection.LeasePolicyState != floretruntime.SQLiteStoreLeasePolicyMismatch ||
		inspection.Reason != floretruntime.SQLiteStoreReasonLeaseMismatch {
		t.Fatalf("inspection = %#v", inspection)
	}
	for _, action := range inspection.Actions {
		if action == floretruntime.SQLiteStoreActionMigrate {
			t.Fatalf("lease mismatch offered migration: %#v", inspection.Actions)
		}
	}
}
