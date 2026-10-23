package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestSQLiteRecordBatchPreservesTransactionAndOwnership(t *testing.T) {
	backend, err := (sqliteSource{path: t.TempDir() + "/batch.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for key, value := range map[string]string{"a": "first", "b": "second", "empty": ""} {
			if err := tx.Put("records", []byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var saved spi.ReadTx
	request := storagebridge.RecordBatch{Namespace: "records", Keys: [][]byte{[]byte("b"), []byte("a"), []byte("b"), []byte("empty")}}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		saved = tx
		values, err := storagebridge.ReadRecords(tx, request)
		if err != nil {
			return err
		}
		if len(values) != 4 || string(values[0]) != "second" || string(values[1]) != "first" || string(values[2]) != "second" || len(values[3]) != 0 {
			t.Fatalf("batch=%q", values)
		}
		values[0][0] = 'X'
		if string(values[2]) != "second" {
			t.Fatal("duplicate keys share mutable bytes")
		}
		again, err := tx.Get("records", []byte("b"))
		if err != nil {
			return err
		}
		if string(again) != "second" {
			t.Fatal("caller modified stored value")
		}
		if _, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: "records", Keys: [][]byte{[]byte("a"), []byte("missing")}}); !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("missing=%v", err)
		}
		for _, bad := range []storagebridge.RecordBatch{{}, {Namespace: "records"}, {Namespace: "records", Keys: [][]byte{nil}}, {Namespace: "records", Keys: make([][]byte, 257)}} {
			if _, err := storagebridge.ReadRecords(tx, bad); !errors.Is(err, spi.ErrInvalidArgument) {
				t.Fatalf("invalid=%v", err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := storagebridge.ReadRecords(saved, request); !errors.Is(err, spi.ErrTransactionClosed) {
		t.Fatalf("closed=%v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	err = backend.View(ctx, func(tx spi.ReadTx) error { cancel(); _, err := storagebridge.ReadRecords(tx, request); return err })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled=%v", err)
	}
}
