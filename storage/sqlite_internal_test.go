package storage

import (
	"bytes"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
)

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
