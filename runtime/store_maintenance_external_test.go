package runtime_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	floretruntime "github.com/floegence/floret/runtime"
)

func TestPublicSQLiteStoreLeasePolicyOptionPersistsExactPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom-policy.db")
	policy := floretruntime.StoreLeasePolicy{
		TTL: 45 * time.Second, RenewInterval: 15 * time.Second, ClockSkewAllowance: 3 * time.Second,
	}
	store, err := floretruntime.OpenSQLiteStore(path, floretruntime.WithSQLiteStoreLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := floretruntime.InspectSQLiteStore(context.Background(), path, floretruntime.WithSQLiteStoreLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != floretruntime.SQLiteStoreStateCurrent || inspection.LeasePolicyState != floretruntime.SQLiteStoreLeasePolicyMatches ||
		inspection.PersistedLeasePolicy == nil || *inspection.PersistedLeasePolicy != policy {
		t.Fatalf("inspection = %#v", inspection)
	}
	reopened, err := floretruntime.OpenSQLiteStore(path, floretruntime.WithSQLiteStoreLeasePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = floretruntime.OpenSQLiteStore(path)
	var mismatch *floretruntime.StoreLeasePolicyMismatchError
	if !errors.As(err, &mismatch) || mismatch.Persisted != policy {
		t.Fatalf("default reopen err=%v mismatch=%#v", err, mismatch)
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
	store, err := floretruntime.OpenSQLiteStore(path)
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
	_, err := floretruntime.OpenSQLiteStore(filepath.Join(t.TempDir(), "invalid.db"), floretruntime.WithSQLiteStoreLeasePolicy(policy))
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
	store, err := floretruntime.OpenSQLiteStore(path)
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
