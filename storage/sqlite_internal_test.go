package storage

import (
	"strings"
	"testing"
)

func TestSQLiteRecordsSeparatePayloadFromPrimaryIndex(t *testing.T) {
	opened, err := (sqliteSource{path: t.TempDir() + "/floret.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	backend := opened.(*sqliteBackend)
	defer backend.Close()
	var schema string
	if err := backend.db.QueryRow(`SELECT sql FROM sqlite_master WHERE name='floret_backend_records'`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToUpper(schema), "WITHOUT ROWID") {
		t.Fatal("large record payloads are stored in the primary index")
	}
	var version string
	if err := backend.db.QueryRow(`SELECT value FROM floret_backend_metadata WHERE name='physical_schema'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != "5" {
		t.Fatalf("physical schema=%q, want 5", version)
	}
}
