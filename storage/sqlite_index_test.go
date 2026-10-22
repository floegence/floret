package storage

import (
	"github.com/floegence/floret/v7/storage/spi"
	"testing"
)

func TestSQLiteHasNoFullTextIndex(t *testing.T) {
	opened, err := (sqliteSource{path: t.TempDir() + "/plain.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	backend := opened.(*sqliteBackend)
	var count int
	if err := backend.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE 'floret_index_%' OR lower(sql) LIKE '%virtual table%'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("search objects=%d err=%v", count, err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if _, ok := tx.(spi.IndexWriter); ok {
			t.Fatal("SQLite exposes removed full-text writer")
		}
		if _, ok := tx.(spi.IndexDocumentAppender); ok {
			t.Fatal("SQLite exposes removed term appender")
		}
		return tx.Put("canonical", []byte("one"), []byte("content is stored once"))
	}); err != nil {
		t.Fatal(err)
	}
}
