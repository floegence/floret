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
	if created.Store == nil || created.Inspection == nil || created.Inspection.State != floretruntime.SQLiteStoreStateMissing ||
		created.Verification != nil || created.Migration != nil {
		t.Fatalf("created startup = %#v", created)
	}
	if err := created.Store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := floretruntime.StartSQLiteStore(ctx, path, floretruntime.SQLiteStartupRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Store == nil || reopened.Inspection == nil || reopened.Inspection.State != floretruntime.SQLiteStoreStateCurrent ||
		reopened.Verification == nil || reopened.Verification.Inspection.State != floretruntime.SQLiteStoreStateCurrent || reopened.Migration != nil {
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
	if result.Store != nil || result.Inspection == nil || result.Inspection.State != floretruntime.SQLiteStoreStateUpgradeable ||
		result.Verification != nil || result.Migration != nil {
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

func TestStartSQLiteStoreAppliesCompatibleMigrationWithDerivedStableIdentity(t *testing.T) {
	path := writeUpgradeableSQLiteStore(t)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	run := func() (string, []floretruntime.SQLiteStartupProgress) {
		t.Helper()
		var progress []floretruntime.SQLiteStartupProgress
		result, startErr := floretruntime.StartSQLiteStore(context.Background(), path, floretruntime.SQLiteStartupRequest{
			MigrationPolicy: floretruntime.SQLiteMigrationApplyCompatible,
			Progress: func(next floretruntime.SQLiteStartupProgress) {
				progress = append(progress, next)
			},
		})
		if startErr != nil {
			t.Fatal(startErr)
		}
		if result.Store == nil || result.Migration == nil || result.Migration.OperationID == "" ||
			!result.Migration.Committed || result.Migration.RolledBack || result.Verification == nil ||
			result.Verification.Inspection.State != floretruntime.SQLiteStoreStateCurrent {
			t.Fatalf("migrated startup = %#v", result)
		}
		if err := result.Store.Close(); err != nil {
			t.Fatal(err)
		}
		return result.Migration.OperationID, progress
	}

	firstID, progress := run()
	assertSQLiteStartupPhases(t, progress,
		floretruntime.SQLiteStartupInspecting,
		floretruntime.SQLiteStartupMigrating,
		floretruntime.SQLiteStartupVerifying,
		floretruntime.SQLiteStartupOpening,
	)
	if !hasDetailedMigrationProgress(progress) {
		t.Fatal("migrated startup emitted no detailed maintenance progress")
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	secondID, _ := run()
	if secondID != firstID {
		t.Fatalf("derived migration id changed across retry: first=%q second=%q", firstID, secondID)
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

	started, err := floretruntime.StartSQLiteStore(context.Background(), path, floretruntime.SQLiteStartupRequest{
		MigrationPolicy: floretruntime.SQLiteMigrationApplyCompatible,
	})
	if err != nil || started.Store == nil || started.Inspection == nil || started.Inspection.State != floretruntime.SQLiteStoreStateMissing {
		t.Fatalf("apply-compatible startup without migration = %#v, err=%v", started, err)
	}
	if err := started.Store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartSQLiteStoreReinspectsStaleMigrationAndFastForwards(t *testing.T) {
	path := writeUpgradeableSQLiteStore(t)
	var migrated bool
	result, err := floretruntime.StartSQLiteStore(context.Background(), path, floretruntime.SQLiteStartupRequest{
		MigrationPolicy: floretruntime.SQLiteMigrationApplyCompatible,
		Progress: func(progress floretruntime.SQLiteStartupProgress) {
			if migrated || progress.Phase != floretruntime.SQLiteStartupMigrating || progress.Maintenance != nil {
				return
			}
			migrated = true
			inspection, inspectErr := floretruntime.InspectSQLiteStore(context.Background(), path)
			if inspectErr != nil {
				t.Fatal(inspectErr)
			}
			migration, migrateErr := floretruntime.MigrateSQLiteStore(context.Background(), path, floretruntime.SQLiteStoreMigrationRequest{
				OperationID:    "concurrent-migration",
				Mode:           floretruntime.SQLiteStoreMigrationApply,
				ExpectedSchema: inspection.Observed,
			})
			if migrateErr != nil || !migration.Committed {
				t.Fatalf("concurrent migration = %#v, err=%v", migration, migrateErr)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !migrated || result.Store == nil || result.Inspection == nil || result.Inspection.State != floretruntime.SQLiteStoreStateCurrent ||
		result.Verification == nil || result.Verification.Inspection.State != floretruntime.SQLiteStoreStateCurrent ||
		result.Migration == nil || result.Migration.Reason != floretruntime.SQLiteStoreReasonInspectionStale {
		t.Fatalf("recovered startup = %#v", result)
	}
	if err := result.Store.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertSQLiteStartupPhases(t *testing.T, progress []floretruntime.SQLiteStartupProgress, want ...floretruntime.SQLiteStartupPhase) {
	t.Helper()
	var got []floretruntime.SQLiteStartupPhase
	for _, update := range progress {
		if len(got) == 0 || got[len(got)-1] != update.Phase {
			got = append(got, update.Phase)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("startup phases = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("startup phases = %#v, want %#v", got, want)
		}
	}
}

func hasDetailedMigrationProgress(progress []floretruntime.SQLiteStartupProgress) bool {
	for _, update := range progress {
		if update.Phase == floretruntime.SQLiteStartupMigrating && update.Maintenance != nil {
			return true
		}
	}
	return false
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
