package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

func TestInspectMissingSQLiteStoreDoesNotCreatePath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing-parent")
	path := filepath.Join(root, "store.db")

	inspection, err := Inspect(context.Background(), path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != MaintenanceStateMissing || inspection.Exists || inspection.Empty {
		t.Fatalf("inspection = %#v", inspection)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing inspection created parent: %v", err)
	}
}

func TestInspectCurrentSQLiteStoreIsFileNeutral(t *testing.T) {
	store, path := openSQLiteStoreForTest(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before := sqliteFileSnapshot(t, path)

	inspection, err := Inspect(context.Background(), path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != MaintenanceStateCurrent || !inspection.Exists || inspection.Empty || !inspection.LeasePolicyMatches {
		t.Fatalf("inspection = %#v", inspection)
	}
	after := sqliteFileSnapshot(t, path)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("inspection changed sqlite files\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestInspectReadsUncheckpointedWALWithoutChangingStoreFiles(t *testing.T) {
	store, path := openSQLiteStoreForTest(t)
	defer store.Close()
	if _, err := store.db.Exec(`PRAGMA wal_autocheckpoint = 0`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE schema_meta SET value = 'tampered-live-wal' WHERE key = 'schema_fingerprint'`); err != nil {
		t.Fatal(err)
	}
	before := sqliteFileSnapshot(t, path)

	inspection, err := Inspect(context.Background(), path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != MaintenanceStateDrifted || inspection.Observed.Fingerprint != "tampered-live-wal" {
		t.Fatalf("inspection ignored the latest WAL schema fact = %#v", inspection)
	}
	if after := sqliteFileSnapshot(t, path); !reflect.DeepEqual(after, before) {
		t.Fatalf("inspection changed live sqlite files\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestVerifyReadsUncheckpointedWALWithoutChangingStoreFiles(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`PRAGMA wal_autocheckpoint = 0`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE threads SET parent_thread_id = 'missing' WHERE id = 'root'`); err != nil {
		t.Fatal(err)
	}
	before := sqliteFileSnapshot(t, path)

	report, err := Verify(ctx, path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if report.Inspection.State != MaintenanceStateCurrent || len(report.Checks) != 2 || report.Checks[1].Passed || report.Checks[1].Code != "thread_authority" {
		t.Fatalf("verification ignored the latest WAL authority fact = %#v", report)
	}
	if after := sqliteFileSnapshot(t, path); !reflect.DeepEqual(after, before) {
		t.Fatalf("verification changed live sqlite files\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestInspectExcludesUncommittedRollbackJournalWithoutChangingStoreFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollback-journal.db")
	createSchemaVersion14StoreForTest(t, path, nil)
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA journal_mode = DELETE`); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), `ROLLBACK`)
	if _, err := conn.ExecContext(context.Background(), `UPDATE schema_meta SET value = 'uncommitted' WHERE key = 'schema_fingerprint'`); err != nil {
		t.Fatal(err)
	}
	before := sqliteFileSnapshot(t, path)

	inspection, err := Inspect(context.Background(), path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != MaintenanceStateUpgradeable || inspection.Observed.Fingerprint != schemaFingerprintVersion14 {
		t.Fatalf("inspection admitted an uncommitted rollback-journal fact = %#v", inspection)
	}
	if after := sqliteFileSnapshot(t, path); !reflect.DeepEqual(after, before) {
		t.Fatalf("inspection changed rollback-journal sqlite files\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestInspectCorruptSQLiteStoreIsFileNeutral(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(path, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := sqliteFileSnapshot(t, path)

	inspection, err := Inspect(context.Background(), path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != MaintenanceStateCorrupt || inspection.Reason != string(MaintenanceErrorCorrupt) {
		t.Fatalf("inspection = %#v", inspection)
	}
	if after := sqliteFileSnapshot(t, path); !reflect.DeepEqual(after, before) {
		t.Fatalf("corrupt inspection changed sqlite files\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestPlanAndApplySQLiteStoreMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v14.db")
	createSchemaVersion14StoreForTest(t, path, nil)
	ctx := context.Background()
	beforeFiles := sqliteFileSnapshot(t, path)
	inspection, err := Inspect(ctx, path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != MaintenanceStateUpgradeable || inspection.Observed.Version != schemaVersion14 {
		t.Fatalf("inspection = %#v", inspection)
	}
	var planProgress []MaintenanceProgress
	plan, err := Migrate(ctx, path, MigrationRequest{
		Mode: MigrationModePlan, ExpectedSchema: inspection.Observed,
		LeasePolicy: sessiontree.DefaultLeasePolicy,
		Progress:    func(progress MaintenanceProgress) { planProgress = append(planProgress, progress) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != MaintenanceStatusReady || plan.Changed || plan.Committed || len(plan.Steps) != 2 {
		t.Fatalf("plan = %#v", plan)
	}
	if afterPlan := sqliteFileSnapshot(t, path); !reflect.DeepEqual(afterPlan, beforeFiles) {
		t.Fatalf("migration plan changed sqlite files\nbefore=%#v\nafter=%#v", beforeFiles, afterPlan)
	}
	assertMaintenanceProgress(t, planProgress)

	var applyProgress []MaintenanceProgress
	result, err := Migrate(ctx, path, MigrationRequest{
		Mode: MigrationModeApply, ExpectedSchema: inspection.Observed,
		LeasePolicy: sessiontree.DefaultLeasePolicy,
		Progress:    func(progress MaintenanceProgress) { applyProgress = append(applyProgress, progress) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != MaintenanceStatusReady || !result.Changed || !result.Committed || result.RolledBack || result.After.State != MaintenanceStateCurrent {
		t.Fatalf("migration result = %#v", result)
	}
	assertMaintenanceProgress(t, applyProgress)
}

func TestApplyPublishedSchemaMigrationInWALModeReturnsCommittedReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "published-v13-wal.db")
	createPublishedLegacyStore(t, path, 13)
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode = WAL`).Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal mode = %q, want wal", journalMode)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	inspection, err := Inspect(ctx, path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != MaintenanceStateUpgradeable || inspection.Observed.Version != schemaVersion13 {
		t.Fatalf("inspection = %#v", inspection)
	}
	result, err := Migrate(ctx, path, MigrationRequest{
		Mode:           MigrationModeApply,
		ExpectedSchema: inspection.Observed,
		LeasePolicy:    sessiontree.DefaultLeasePolicy,
	})
	if err != nil {
		t.Fatalf("WAL migration failed after committed=%t: %v", result.Committed, err)
	}
	if result.Status != MaintenanceStatusReady || !result.Changed || !result.Committed || result.RolledBack || result.After.State != MaintenanceStateCurrent {
		t.Fatalf("WAL migration result = %#v", result)
	}
	after, err := Inspect(ctx, path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != MaintenanceStateCurrent || !after.LeasePolicyMatches {
		t.Fatalf("post-migration inspection = %#v", after)
	}
}

func TestSQLiteStoreMigrationRejectsStaleInspectionWithoutChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale.db")
	createSchemaVersion14StoreForTest(t, path, nil)
	before := sqliteFileSnapshot(t, path)
	result, err := Migrate(context.Background(), path, MigrationRequest{
		Mode:           MigrationModeApply,
		ExpectedSchema: storageIdentityForTest(schemaVersion15, schemaFingerprintVersion15),
		LeasePolicy:    sessiontree.DefaultLeasePolicy,
	})
	if err == nil || result.Reason != "inspection_stale" || !result.SafeToRetry || result.Committed {
		t.Fatalf("stale migration result=%#v err=%v", result, err)
	}
	after := sqliteFileSnapshot(t, path)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stale migration changed sqlite files\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestVerifyCurrentSQLiteStoreChecksAuthority(t *testing.T) {
	store, path := openSQLiteStoreForTest(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(context.Background(), path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if report.Inspection.State != MaintenanceStateCurrent || len(report.Checks) != 2 {
		t.Fatalf("verification = %#v", report)
	}
	for _, check := range report.Checks {
		if !check.Passed {
			t.Fatalf("verification check = %#v", check)
		}
	}
}

func TestVerifyReportsInvalidAuthorityWithoutExposingStorageDetails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "invalid-authority.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "/root/child",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE threads SET parent_thread_id = 'missing' WHERE id = 'child'`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := Verify(ctx, path, sessiontree.DefaultLeasePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Checks) != 2 || report.Checks[1].Code != "thread_authority" || report.Checks[1].Passed {
		t.Fatalf("verification = %#v", report)
	}
	if report.Checks[1].SafeDetail != "thread authority graph is invalid" {
		t.Fatalf("unsafe verification detail = %q", report.Checks[1].SafeDetail)
	}
}

type sqliteSnapshot struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime int64
	Hash    [sha256.Size]byte
}

func sqliteFileSnapshot(t *testing.T, path string) []sqliteSnapshot {
	t.Helper()
	var snapshots []sqliteSnapshot
	for _, candidate := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		info, err := os.Stat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(candidate)
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, sqliteSnapshot{
			Name: filepath.Base(candidate), Size: info.Size(), Mode: info.Mode(),
			ModTime: info.ModTime().UnixNano(), Hash: sha256.Sum256(data),
		})
	}
	return snapshots
}

func assertMaintenanceProgress(t *testing.T, progress []MaintenanceProgress) {
	t.Helper()
	if len(progress) < 2 {
		t.Fatalf("progress = %#v", progress)
	}
	for index, item := range progress {
		if item.Sequence != uint64(index+1) {
			t.Fatalf("progress[%d].Sequence=%d", index, item.Sequence)
		}
	}
	last := progress[len(progress)-1]
	if last.Status != MaintenanceStatusReady {
		t.Fatalf("terminal progress = %#v", last)
	}
}

func storageIdentityForTest(version, fingerprint string) storage.StoreSchemaIdentity {
	return storage.StoreSchemaIdentity{Version: version, Fingerprint: fingerprint}
}
