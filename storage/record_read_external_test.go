package storage_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestBoundedRecordReaderContract(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			source := storage.Memory()
			if kind == "sqlite" {
				source = storage.SQLite(t.TempDir() + "/records.sqlite")
			}
			backend := openStorageBackend(t, t.Context(), source)
			defer backend.Close()
			value := bytes.Repeat([]byte{0, 0xff, 'a', 0xc3, 0xa9}, 1<<20)
			key := []byte("large")
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if err := tx.Put("records", key, value); err != nil {
					return err
				}
				return tx.Put("records", []byte("empty"), []byte{})
			}); err != nil {
				t.Fatal(err)
			}
			var retained spi.BoundedRecordReader
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				reader, ok := tx.(spi.BoundedRecordReader)
				if !ok {
					t.Fatal("built-in backend lacks bounded record reads")
				}
				retained = reader
				if found, err := reader.NamespaceHasRecords("records"); err != nil || !found {
					t.Fatalf("existing namespace = %v, %v", found, err)
				}
				if found, err := reader.NamespaceHasRecords("absent"); err != nil || found {
					t.Fatalf("missing namespace = %v, %v", found, err)
				}
				if _, err := reader.NamespaceHasRecords(""); !errors.Is(err, spi.ErrInvalidArgument) {
					t.Fatalf("empty namespace = %v", err)
				}
				for _, sample := range []struct {
					key  []byte
					size int64
				}{{key, int64(len(value))}, {[]byte("empty"), 0}} {
					if size, err := reader.RecordSize("records", sample.key); err != nil || size != sample.size {
						t.Fatalf("size for %q = %d, %v", sample.key, size, err)
					}
				}
				for _, offset := range []int64{0, 1, 4, int64(len(value)) - 3, int64(len(value)), math.MaxInt64} {
					got, err := reader.ReadRecordRange("records", key, offset, 17)
					var want []byte
					if offset < int64(len(value)) {
						want = value[offset:min(offset+17, int64(len(value)))]
					}
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("range at %d = %x, %v; want %x", offset, got, err, want)
					}
					if len(got) != 0 {
						got[0] ^= 0xff
					}
				}
				got, err := reader.ReadRecordRange("records", key, 0, 17)
				if err != nil || !bytes.Equal(got, value[:17]) {
					t.Fatalf("returned slices alias storage: %x, %v", got, err)
				}
				if got, err := reader.ReadRecordRange("records", []byte("empty"), 0, 17); err != nil || len(got) != 0 {
					t.Fatalf("empty record range = %x, %v", got, err)
				}
				for _, key := range [][]byte{nil, []byte("absent")} {
					want := spi.ErrNotFound
					if len(key) == 0 {
						want = spi.ErrInvalidArgument
					}
					if _, err := reader.RecordSize("records", key); !errors.Is(err, want) {
						t.Fatalf("size error = %v; want %v", err, want)
					}
					if _, err := reader.ReadRecordRange("records", key, 0, 17); !errors.Is(err, want) {
						t.Fatalf("range error = %v; want %v", err, want)
					}
				}
				for _, sample := range []struct {
					offset int64
					limit  int
				}{{-1, 17}, {0, 0}, {0, -1}} {
					if _, err := reader.ReadRecordRange("records", key, sample.offset, sample.limit); !errors.Is(err, spi.ErrInvalidArgument) {
						t.Fatalf("invalid range = %v", err)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := retained.RecordSize("records", key); !errors.Is(err, spi.ErrTransactionClosed) {
				t.Fatalf("expired size = %v", err)
			}
			if _, err := retained.ReadRecordRange("records", key, 0, 17); !errors.Is(err, spi.ErrTransactionClosed) {
				t.Fatalf("expired range = %v", err)
			}
			if _, err := retained.NamespaceHasRecords("records"); !errors.Is(err, spi.ErrTransactionClosed) {
				t.Fatalf("expired namespace = %v", err)
			}
			cancelled, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := backend.View(cancelled, func(tx spi.ReadTx) error {
				reader := tx.(spi.BoundedRecordReader)
				cancel()
				if _, err := reader.RecordSize("records", key); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled size = %v", err)
				}
				if _, err := reader.ReadRecordRange("records", key, 0, 17); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled range = %v", err)
				}
				if _, err := reader.NamespaceHasRecords("records"); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled namespace = %v", err)
				}
				return nil
			}); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled view = %v", err)
			}
		})
	}
}

func TestMemoryBackendSnapshotsShareOnlyImmutableValues(t *testing.T) {
	backend := openStorageBackend(t, t.Context(), storage.Memory())
	defer backend.Close()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put("records", []byte("key"), []byte("original"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(before spi.ReadTx) error {
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			if err := tx.Delete("records", []byte("key")); err != nil {
				return err
			}
			return tx.Put("records", []byte("new"), []byte("replacement"))
		}); err != nil {
			return err
		}
		value, err := before.Get("records", []byte("key"))
		if err != nil || string(value) != "original" {
			t.Fatalf("snapshot changed after commit: %q, %v", value, err)
		}
		if _, err := before.Get("records", []byte("new")); !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("snapshot observed future record: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
