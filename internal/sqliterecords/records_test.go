package sqliterecords

import (
	"bytes"
	"errors"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
	"testing"
)

func TestSQLiteReadStatementIsBoundToTransaction(t *testing.T) {
	backend := recordFixture(t)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put("test", []byte("key"), []byte("value")) }); err != nil {
		t.Fatal(err)
	}
	var saved *Tx
	if err := backend.View(t.Context(), func(read spi.ReadTx) error {
		saved = read.(*Tx)
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

func TestSQLiteTransactionReusesWriteStatements(t *testing.T) {
	backend := recordFixture(t)

	if err := backend.Update(t.Context(), func(write spi.WriteTx) error {
		tx := write.(*Tx)
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
func TestSQLiteIdenticalPutsDoNotUpdateRows(t *testing.T) {
	backend := recordFixture(t)
	if err := backend.Update(t.Context(), func(write spi.WriteTx) error {
		tx := write.(*Tx)
		records := []spi.Record{{Key: []byte("key"), Value: []byte("same")}}
		if err := tx.Put("records", records[0].Key, records[0].Value); err != nil {
			return err
		}
		var before, after int
		if err := tx.tx.QueryRowContext(t.Context(), "SELECT total_changes()").Scan(&before); err != nil {
			return err
		}
		if err := tx.Put("records", records[0].Key, records[0].Value); err != nil {
			return err
		}
		if err := storagebridge.WriteRecords(tx, "records", records); err != nil {
			return err
		}
		if err := tx.tx.QueryRowContext(t.Context(), "SELECT total_changes()").Scan(&after); err != nil {
			return err
		}
		if after != before {
			t.Fatalf("identical puts modified %d rows", after-before)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
