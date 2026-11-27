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

func TestSQLiteWriteBatchPreservesTransactionAndOwnership(t *testing.T) {
	backend, err := (sqliteSource{path: t.TempDir() + "/write-batch.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	records := []spi.Record{{Key: []byte("a"), Value: []byte("first")}, {Key: []byte("b"), Value: []byte("second")}, {Key: []byte("a"), Value: []byte("last")}, {Key: []byte("empty"), Value: []byte{}}}
	var saved spi.WriteTx
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		saved = tx
		if err := storagebridge.WriteRecords(tx, "records", records); err != nil {
			return err
		}
		records[2].Value[0] = 'X'
		for key, want := range map[string]string{"a": "last", "b": "second", "empty": ""} {
			got, err := tx.Get("records", []byte(key))
			if err != nil || string(got) != want {
				t.Fatalf("%s=%q err=%v", key, got, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := storagebridge.WriteRecords(saved, "records", records); !errors.Is(err, spi.ErrTransactionClosed) {
		t.Fatalf("closed=%v", err)
	}
	failure := errors.New("rollback write set")
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := storagebridge.WriteRecords(tx, "records", records); err != nil {
			return err
		}
		return failure
	}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		got, err := tx.Get("records", []byte("a"))
		if err != nil || string(got) != "last" {
			t.Fatalf("rollback changed stored value: %q %v", got, err)
		}
		if err := storagebridge.WriteRecords(tx.(*sqliteTx), "records", records); !errors.Is(err, spi.ErrInvalidArgument) {
			t.Fatalf("read-only write=%v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for _, bad := range [][]spi.Record{nil, {{Key: []byte("new"), Value: []byte("must not write")}, {}}, make([]spi.Record, 257)} {
			if err := storagebridge.WriteRecords(tx, "records", bad); !errors.Is(err, spi.ErrInvalidArgument) {
				t.Fatalf("invalid=%v", err)
			}
		}
		if _, err := tx.Get("records", []byte("new")); !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("invalid batch partially wrote: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	err = backend.Update(ctx, func(tx spi.WriteTx) error { cancel(); return storagebridge.WriteRecords(tx, "records", records) })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled=%v", err)
	}
}
