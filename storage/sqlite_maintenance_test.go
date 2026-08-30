package storage_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v6/internal/storagebridge"
	"github.com/floegence/floret/v6/storage"
	_ "modernc.org/sqlite"
)

func TestSQLiteNewBackendUsesIncrementalAutoVacuum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "floret.db")
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(storage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	db := openMaintenanceTestDatabase(t, path)
	defer db.Close()
	var mode int
	if err := db.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != 2 {
		t.Fatalf("auto_vacuum=%d, want INCREMENTAL", mode)
	}
}

func TestMaintainSQLiteConvertsLegacyDatabaseAndPreservesRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db := openMaintenanceTestDatabase(t, path)
	createMaintenanceTestSchema(t, db, "NONE")
	large := make([]byte, 12<<20)
	if _, err := db.Exec(`INSERT INTO floret_backend_records(namespace, key, value) VALUES ('keep', X'01', X'CAFE'), ('discard', X'02', ?)`, large); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM floret_backend_records WHERE namespace = 'discard'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := storage.MaintainSQLite(t.Context(), path, storage.SQLiteMaintenancePolicy{
		MinimumFileBytes: 1, MinimumReclaimBytes: 1, MinimumReclaimRatio: 0.1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != storage.SQLiteMaintenanceActionVacuum {
		t.Fatalf("action=%q reason=%q, want vacuum", result.Action, result.Reason)
	}
	if result.Before.AutoVacuum != "none" || result.After.AutoVacuum != "incremental" {
		t.Fatalf("auto_vacuum before=%q after=%q", result.Before.AutoVacuum, result.After.AutoVacuum)
	}
	if result.After.FileBytes >= before.Size()/2 {
		t.Fatalf("file size after=%d, before=%d", result.After.FileBytes, before.Size())
	}
	db = openMaintenanceTestDatabase(t, path)
	defer db.Close()
	var value []byte
	if err := db.QueryRow(`SELECT value FROM floret_backend_records WHERE namespace = 'keep' AND key = X'01'`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if string(value) != string([]byte{0xca, 0xfe}) {
		t.Fatalf("preserved value=%x", value)
	}
}

func TestMaintainSQLiteSkipsBusyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy.db")
	db := openMaintenanceTestDatabase(t, path)
	createMaintenanceTestSchema(t, db, "NONE")
	if _, err := db.Exec(`BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db.Exec(`ROLLBACK`); _ = db.Close() }()
	result, err := storage.MaintainSQLite(t.Context(), path, storage.SQLiteMaintenancePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != storage.SQLiteMaintenanceActionNone || result.Reason != "database_busy" {
		t.Fatalf("result=%+v", result)
	}
}

func TestMaintainSQLiteSkipsOpenFloretRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open.db")
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(storage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	result, err := storage.MaintainSQLite(t.Context(), path, storage.SQLiteMaintenancePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != storage.SQLiteMaintenanceActionNone || result.Reason != "runtime_open" {
		t.Fatalf("result=%+v", result)
	}
}

func TestMaintainSQLiteRejectsNonFloretDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "other.db")
	db := openMaintenanceTestDatabase(t, path)
	if _, err := db.Exec(`CREATE TABLE other(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.MaintainSQLite(context.Background(), path, storage.SQLiteMaintenancePolicy{}); err == nil {
		t.Fatal("non-Floret database maintenance succeeded")
	}
}

func openMaintenanceTestDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func createMaintenanceTestSchema(t *testing.T, db *sql.DB, autoVacuum string) {
	t.Helper()
	for _, statement := range []string{
		`PRAGMA auto_vacuum = ` + autoVacuum,
		`CREATE TABLE floret_backend_metadata (name TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID`,
		`CREATE TABLE floret_backend_records (namespace TEXT NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key)) WITHOUT ROWID`,
		`INSERT INTO floret_backend_metadata(name, value) VALUES ('physical_schema', CAST('1' AS BLOB))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}
