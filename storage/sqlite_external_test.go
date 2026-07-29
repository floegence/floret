package storage_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/floegence/floret/v2/storage"
)

func TestSQLiteBackendContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "floret.db")
	runBackendContract(t, storage.SQLite(path))
	assertSQLitePhysicalSchema(t, path)
}

func TestSQLiteSourceRejectsMissingPath(t *testing.T) {
	if _, err := storage.SQLite("").Open(context.Background()); err == nil {
		t.Fatal("empty SQLite path opened successfully")
	}
}

func assertSQLitePhysicalSchema(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`
		SELECT name
		FROM sqlite_master
		WHERE type = 'table' AND name LIKE 'floret_%'
		ORDER BY name
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"floret_backend_metadata", "floret_backend_records"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("physical tables = %v, want %v", names, want)
	}
}
