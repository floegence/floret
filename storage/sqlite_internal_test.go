package storage

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestSQLiteReadStatementIsBoundToTransaction(t *testing.T) {
	backend, err := (sqliteSource{path: t.TempDir() + "/floret.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put("test", []byte("key"), []byte("value")) }); err != nil {
		t.Fatal(err)
	}
	var saved *sqliteTx
	if err := backend.View(t.Context(), func(read spi.ReadTx) error {
		saved = read.(*sqliteTx)
		if _, err := read.Get("test", []byte("missing")); !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("missing=%v", err)
		}
		statement := saved.getStatement
		for range 3 {
			value, err := read.Get("test", []byte("key"))
			if err != nil || string(value) != "value" || saved.getStatement != statement {
				t.Fatalf("reused Get=%s, %v", value, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := saved.Get("test", []byte("key")); err == nil {
		t.Fatal("expired transaction remained readable")
	}
}

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

func TestSQLiteTransactionReusesWriteStatements(t *testing.T) {
	opened, err := (sqliteSource{path: t.TempDir() + "/floret.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	backend := opened.(*sqliteBackend)
	defer backend.Close()

	if err := backend.Update(t.Context(), func(write spi.WriteTx) error {
		tx := write.(*sqliteTx)
		if err := tx.Put("test", []byte("one"), []byte("value")); err != nil {
			return err
		}
		firstPut := tx.putStatement
		if firstPut == nil {
			t.Fatal("first Put did not prepare a reusable statement")
		}
		if err := tx.Put("test", []byte("two"), bytes.Repeat([]byte("v"), 8)); err != nil {
			return err
		}
		if tx.putStatement != firstPut {
			t.Fatal("Put prepared more than one statement in one transaction")
		}
		if err := tx.Delete("test", []byte("one")); err != nil {
			return err
		}
		firstDelete := tx.deleteStatement
		if firstDelete == nil {
			t.Fatal("first Delete did not prepare a reusable statement")
		}
		if err := tx.Delete("test", []byte("two")); err != nil {
			return err
		}
		if tx.deleteStatement != firstDelete {
			t.Fatal("Delete prepared more than one statement in one transaction")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
