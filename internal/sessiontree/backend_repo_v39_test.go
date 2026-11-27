package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func outputMigrationFixture(t *testing.T, version int) *migrationTestBackend {
	t.Helper()
	backend := newMigrationTestBackend()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		memory := newMemoryRepo(time.Now)
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "historical"}); err != nil {
			return err
		}
		if _, err := memory.Append(t.Context(), Entry{ID: "original", ThreadID: "historical", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "immutable historical fact"}}, AppendOptions{}); err != nil {
			return err
		}
		if err := saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV32Format); err != nil {
			return err
		}
		source, err := readV32Manifest(tx)
		if err != nil {
			return err
		}
		source.Version = version
		return putBackendDomainRecordContext(t.Context(), tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, source)
	}); err != nil {
		t.Fatal(err)
	}
	return backend
}
func TestOutputSchemaMigrationPreservesHistoryAndBoundedRestart(t *testing.T) {
	t.Run("domain38", func(t *testing.T) { testManifestUpgradeRestart(t, backendDomainV38Version) })
	t.Run("domain41", func(t *testing.T) { testManifestUpgradeRestart(t, backendDomainV41Version) })
}

func testManifestUpgradeRestart(t *testing.T, version int) {
	backend := outputMigrationFixture(t, version)
	before := cloneMigrationRecords(backend.records)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		_, current, err := NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err == nil && !current {
			return errors.New("not current")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := cloneMigrationRecords(backend.records)
	key := string(backendDomainKey(backendDomainRecordManifest))
	before[backendDomainV32Namespace][key] = after[backendDomainV32Namespace][key]
	if !reflect.DeepEqual(before, after) {
		t.Fatal("migration rewrote history")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		manifest, err := readV32Manifest(tx)
		if err != nil {
			return err
		}
		if manifest.Version != backendDomainV42Version {
			t.Fatal("not upgraded")
		}
		_, current, err := NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err == nil && !current {
			return errors.New("restart needs mutation")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, backend.records) {
		t.Fatal("current startup rewrote records")
	}
}
func TestOutputSchemaMigrationAtomicFailure(t *testing.T) {
	t.Run("domain38", func(t *testing.T) {
		testManifestUpgradeFailure(t, backendDomainV38Version, migrateBackendDomainV38ToV39)
	})
	t.Run("domain41", func(t *testing.T) { testManifestUpgradeFailure(t, backendDomainV41Version, migrateCurrentManifest) })
}

func testManifestUpgradeFailure(t *testing.T, version int, migrate func(context.Context, spi.WriteTx, backendDomainManifest) error) {
	for _, mode := range []string{"write", "cancel", "panic", "drift", "future"} {
		t.Run(mode, func(t *testing.T) {
			backend := outputMigrationFixture(t, version)
			before := cloneMigrationRecords(backend.records)
			failed := false
			func() {
				defer func() {
					if recover() != nil {
						failed = true
					}
				}()
				err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					source, err := readV32Manifest(tx)
					if err != nil {
						return err
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					switch mode {
					case "cancel":
						cancel()
					case "drift":
						source.Sequence++
					case "future":
						source.Version = version + 2
					}
					if mode == "write" || mode == "panic" {
						tx = capacityMigrationFaultTx{tx, mode}
					}
					return migrate(ctx, tx, source)
				})
				failed = err != nil
			}()
			if !failed || !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed migration modified authority")
			}
		})
	}
}
