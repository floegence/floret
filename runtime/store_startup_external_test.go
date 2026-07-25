package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	floretruntime "github.com/floegence/floret/runtime"
)

func TestStartSQLiteStoreCreatesAndReopensExactStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "store.db")

	created, err := floretruntime.StartSQLiteStore(ctx, path, floretruntime.SQLiteStartupRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if created.Store == nil || created.Inspection.State != floretruntime.SQLiteStoreStateMissing || created.Migration != nil {
		t.Fatalf("created startup = %#v", created)
	}
	if err := created.Store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := floretruntime.StartSQLiteStore(ctx, path, floretruntime.SQLiteStartupRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Store == nil || reopened.Inspection.State != floretruntime.SQLiteStoreStateCurrent ||
		reopened.Verification.Inspection.State != floretruntime.SQLiteStoreStateCurrent || reopened.Migration != nil {
		t.Fatalf("reopened startup = %#v", reopened)
	}
	if err := reopened.Store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartSQLiteStoreRefusesUpgradeableStoreWithoutWriting(t *testing.T) {
	path := writeUpgradeableSQLiteStore(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	result, err := floretruntime.StartSQLiteStore(context.Background(), path, floretruntime.SQLiteStartupRequest{})
	if result.Store != nil || result.Inspection.State != floretruntime.SQLiteStoreStateUpgradeable || result.Migration != nil {
		t.Fatalf("refused startup = %#v", result)
	}
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonMigrationAvailable || maintenance.Retryable || maintenance.SafeToRetry {
		t.Fatalf("refused startup error = %#v, err=%v", maintenance, err)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(after) != string(before) {
		t.Fatal("refused startup changed the sqlite database")
	}
}

func TestStartSQLiteStoreAppliesCompatibleMigrationWithStableIdentity(t *testing.T) {
	path := writeUpgradeableSQLiteStore(t)
	var progress []floretruntime.SQLiteStoreMaintenanceProgress
	result, err := floretruntime.StartSQLiteStore(context.Background(), path, floretruntime.SQLiteStartupRequest{
		MigrationPolicy:      floretruntime.SQLiteMigrationApplyCompatible,
		MigrationOperationID: "startup-migration-1",
		Progress: func(next floretruntime.SQLiteStoreMaintenanceProgress) {
			progress = append(progress, next)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Store == nil || result.Migration == nil || result.Migration.OperationID != "startup-migration-1" ||
		!result.Migration.Committed || result.Migration.RolledBack || result.Verification.Inspection.State != floretruntime.SQLiteStoreStateCurrent {
		t.Fatalf("migrated startup = %#v", result)
	}
	if len(progress) == 0 {
		t.Fatal("migrated startup emitted no progress")
	}
	if err := result.Store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartSQLiteStoreRejectsInvalidPolicyBeforeFilesystemAccess(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	path := filepath.Join(root, "store.db")
	_, err := floretruntime.StartSQLiteStore(context.Background(), path, floretruntime.SQLiteStartupRequest{
		MigrationPolicy: "automatic",
	})
	var maintenance *floretruntime.SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationOpen ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonInvalidRequest {
		t.Fatalf("invalid policy error = %#v, err=%v", maintenance, err)
	}
	if _, statErr := os.Stat(root); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("invalid policy accessed filesystem: %v", statErr)
	}

	_, err = floretruntime.StartSQLiteStore(context.Background(), path, floretruntime.SQLiteStartupRequest{
		MigrationPolicy: floretruntime.SQLiteMigrationApplyCompatible,
	})
	maintenance = nil
	if !errors.As(err, &maintenance) || maintenance.Operation != floretruntime.SQLiteStoreOperationMigrate ||
		maintenance.Reason != floretruntime.SQLiteStoreReasonInvalidRequest {
		t.Fatalf("missing migration identity error = %#v, err=%v", maintenance, err)
	}
}

func writeUpgradeableSQLiteStore(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "upgradeable.db")
	schema, err := os.ReadFile(filepath.Join("..", "internal", "storage", "sqlite", "testdata", "schema-v12.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), string(schema)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO schema_meta(key, value) VALUES
		('schema_version', '12'),
		('raw_encoder_version', '1'),
		('schema_fingerprint', '2586cafa8e761a8ed2e6d1227e6eb1a3f706332c590a8e7d1e6045f185520446')`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
