package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func installV25DecisionSource(t *testing.T, backend *migrationTestBackend) *MemoryRepo {
	t.Helper()
	memory := installV24OutputSourceStore(t, backend)
	if err := migrateBackendDomainV24ToV25(t.Context(), memory); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV25(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	return memory
}

func TestBackendDomainV25ToV26PreservesHistory(t *testing.T) {
	backend := newMigrationTestBackend()
	before := installV25DecisionSource(t, backend)
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		after, found, err := loadBackendDomainV26(t.Context(), tx, time.Now)
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("missing v26 store")
		}
		for id, entries := range before.entries {
			if len(entries) != len(after.entries[id]) {
				t.Fatal("migration changed entry count")
			}
			for i, entry := range entries {
				got := after.entries[id][i]
				if entry.ID != got.ID || entry.Raw != got.Raw || entry.RawHash != got.RawHash || !reflect.DeepEqual(entry.Metadata, got.Metadata) {
					t.Fatalf("migration changed canonical payload %s/%d", id, i)
				}
			}
		}
		if _, _, err := loadBackendDomainV25(t.Context(), tx, time.Now); err == nil {
			t.Fatal("older writer accepted new envelope")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	current := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current, backend.records) {
		t.Fatal("current restart rewrote records")
	}
	fresh := newMigrationTestBackend()
	if _, err := NewBackendRepo(t.Context(), fresh, time.Now); err != nil {
		t.Fatal(err)
	}
	if err := fresh.View(t.Context(), func(tx spi.ReadTx) error {
		_, found, err := loadBackendDomainV26(t.Context(), tx, time.Now)
		if !found {
			t.Error("fresh store not initialized at v26")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackendDomainV25ToV26RollsBack(t *testing.T) {
	for _, failure := range []string{"write", "cancel", "panic", "source_drift", "future"} {
		t.Run(failure, func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := installV25DecisionSource(t, backend)
			ctx := t.Context()
			var target spi.Backend = backend
			switch failure {
			case "write":
				target = migrationFailingBackend{Backend: backend, err: errors.New("injected")}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "panic":
				target = migrationPanickingBackend{Backend: backend}
			case "source_drift":
				memory.entries["thread"][1].RunID = ""
				format := backendDomainV25Format
				format.validate = func(*MemoryRepo) error { return nil }
				if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomain(tx, memory, format) }); err != nil {
					t.Fatal(err)
				}
			case "future":
				format := backendDomainV26Format
				format.version++
				raw, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: format.version})
				if err != nil {
					t.Fatal(err)
				}
				if err := backend.Update(ctx, func(tx spi.WriteTx) error {
					return tx.Put(format.namespace, backendDomainKey(backendDomainRecordManifest), raw)
				}); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneMigrationRecords(backend.records)
			func() {
				if failure == "panic" {
					defer func() {
						if recover() == nil {
							t.Error("expected panic")
						}
					}()
				}
				if _, err := NewBackendRepo(ctx, target, time.Now); err == nil {
					t.Error("invalid startup succeeded")
				}
			}()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed migration mutated source")
			}
		})
	}
}
