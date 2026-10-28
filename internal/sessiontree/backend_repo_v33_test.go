package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
	"testing"
	"time"
)

func TestCapacitySchemaMigrationPreservesRecordsAndRestarts(t *testing.T) {
	backend := newMigrationTestBackend()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		memory := newMemoryRepo(time.Now)
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
			return err
		}
		if _, err := memory.Append(t.Context(), Entry{ID: "message", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained fact"}}, AppendOptions{}); err != nil {
			return err
		}
		return saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV32Format)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		_, current, err := NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err == nil && !current {
			return errors.New("migration did not publish current owner")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := cloneMigrationRecords(backend.records)
	key := string(backendDomainKey(backendDomainRecordManifest))
	manifest := after[backendDomainV32Namespace][key]
	if _, err := storagecodec.DecodeEnvelope(manifest, "sessiontree-v32-record"); err == nil {
		t.Fatal("old writer accepted new manifest")
	}
	before[backendDomainV32Namespace][key] = bytes.Clone(manifest)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("metadata migration rewrote historical records")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, current, err := NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err == nil && !current {
			return errors.New("restart not current")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, backend.records) {
		t.Fatal("restart wrote current state")
	}
}

type capacityMigrationFaultTx struct {
	spi.WriteTx
	mode string
}

func (tx capacityMigrationFaultTx) Put(ns string, key, value []byte) error {
	if ns == backendDomainV32Namespace && bytes.Equal(key, backendDomainKey(backendDomainRecordManifest)) {
		if tx.mode == "panic" {
			panic("injected migration panic")
		}
		return errors.New("injected migration write failure")
	}
	return tx.WriteTx.Put(ns, key, value)
}
func TestCapacitySchemaMigrationRollback(t *testing.T) {
	for _, mode := range []string{"write", "cancel", "panic", "drift", "future"} {
		t.Run(mode, func(t *testing.T) {
			backend := newMigrationTestBackend()
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return saveCompleteBackendDomainContext(t.Context(), tx, newMemoryRepo(time.Now), backendDomainV32Format)
			}); err != nil {
				t.Fatal(err)
			}
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
						source.Version = 34
					}
					if mode == "write" || mode == "panic" {
						tx = capacityMigrationFaultTx{tx, mode}
					}
					return migrateBackendDomainV32ToV33(ctx, tx, source)
				})
				failed = err != nil
			}()
			if !failed || !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed migration published state")
			}
		})
	}
}
