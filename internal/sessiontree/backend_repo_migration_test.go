package sessiontree_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
	. "github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

var migrationStateKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))
var migrationInventoryKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"))

func TestBackendRepoAutomaticallyPersistsV2DomainMigrationThroughV4(t *testing.T) {
	for _, test := range migrationBackendSources(t) {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend := openMigrationBackend(t, ctx, test.source)
			defer backend.Close()
			legacy := seedLegacyV2SubAgentState(t, ctx, backend)

			if _, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now); err != nil {
				t.Fatal(err)
			}
			first := readMigrationState(t, ctx, backend)
			if bytes.Equal(first, legacy) {
				t.Fatal("startup did not persist the v2 through v4 migration")
			}
			assertMigrationStateVersionAndAdmission(t, first, 4, "child\x00turn")

			if _, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now); err != nil {
				t.Fatal(err)
			}
			second := readMigrationState(t, ctx, backend)
			if !bytes.Equal(second, first) {
				t.Fatal("current v4 startup rewrote canonical domain state")
			}
		})
	}
}

func TestBackendRepoAutomaticallyMigratesV3InventoryAndRejectsCurrentDrift(t *testing.T) {
	for _, test := range migrationBackendSources(t) {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend := openMigrationBackend(t, ctx, test.source)
			defer backend.Close()
			legacyV3 := seedLegacyV3StateWithoutInventory(t, ctx, backend)

			repo, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			migrated := readMigrationState(t, ctx, backend)
			if bytes.Equal(migrated, legacyV3) {
				t.Fatal("startup did not persist the v3 to v4 migration")
			}
			assertMigrationStateVersionAndAdmission(t, migrated, 4, "")
			inventory := readMigrationRecord(t, ctx, backend, migrationInventoryKey)
			if len(inventory) == 0 {
				t.Fatal("v3 to v4 migration did not create root thread inventory")
			}
			if _, err := repo.ListRootThreadInventory(ctx, ListThreadsOptions{IncludeArchived: true, RootOnly: true}); err != nil {
				t.Fatal(err)
			}

			if _, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now); err != nil {
				t.Fatal(err)
			}
			if current := readMigrationState(t, ctx, backend); !bytes.Equal(current, migrated) {
				t.Fatal("current v4 startup rewrote canonical domain state")
			}

			if err := backend.Update(ctx, func(tx spi.WriteTx) error {
				return tx.Delete("floret.domain", migrationInventoryKey)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("missing current inventory error=%v, want ErrAuthorityCorrupt", err)
			}
			if current := readMigrationState(t, ctx, backend); !bytes.Equal(current, migrated) {
				t.Fatal("failed current inventory verification changed canonical domain state")
			}

			drifted, err := storagecodec.EncodeEnvelope(
				"sessiontree-root-thread-inventory", []byte(`{"version":1,"items":[]}`),
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(ctx, func(tx spi.WriteTx) error {
				return tx.Put("floret.domain", migrationInventoryKey, drifted)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("drifted current inventory error=%v, want ErrAuthorityCorrupt", err)
			}
			if current := readMigrationState(t, ctx, backend); !bytes.Equal(current, migrated) {
				t.Fatal("failed drift verification changed canonical domain state")
			}
		})
	}
}

func TestBackendRepoV3InventoryMigrationRollsBackBothRecordsOnSecondWriteFailure(t *testing.T) {
	for _, test := range migrationBackendSources(t) {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend := openMigrationBackend(t, ctx, test.source)
			defer backend.Close()
			legacyV3 := seedLegacyV3StateWithoutInventory(t, ctx, backend)
			injected := errors.New("injected inventory write failure")

			wrapped := targetedFailingMigrationBackend{Backend: backend, key: migrationInventoryKey, err: injected}
			if _, err := NewBackendRepo(ctx, wrapped, DefaultLeasePolicy, time.Now); !errors.Is(err, injected) {
				t.Fatalf("migration error=%v, want injected inventory failure", err)
			}
			if after := readMigrationState(t, ctx, backend); !bytes.Equal(after, legacyV3) {
				t.Fatal("failed inventory write committed the v4 domain state")
			}
			if recordExists(t, ctx, backend, migrationInventoryKey) {
				t.Fatal("failed inventory write left a partial inventory record")
			}
		})
	}
}

func TestBackendRepoDomainMigrationRollsBackOnWriteFailureAndCancellation(t *testing.T) {
	t.Run("write failure", func(t *testing.T) {
		for _, test := range migrationBackendSources(t) {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				backend := openMigrationBackend(t, ctx, test.source)
				defer backend.Close()
				legacy := seedLegacyV2SubAgentState(t, ctx, backend)
				injected := errors.New("injected migration write failure")

				if _, err := NewBackendRepo(ctx, failingMigrationBackend{Backend: backend, err: injected}, DefaultLeasePolicy, time.Now); !errors.Is(err, injected) {
					t.Fatalf("migration error=%v, want injected failure", err)
				}
				if after := readMigrationState(t, ctx, backend); !bytes.Equal(after, legacy) {
					t.Fatal("failed migration changed persisted v2 state")
				}
			})
		}
	})

	t.Run("cancelled startup", func(t *testing.T) {
		for _, test := range migrationBackendSources(t) {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				backend := openMigrationBackend(t, ctx, test.source)
				defer backend.Close()
				legacy := seedLegacyV2SubAgentState(t, ctx, backend)
				cancelled, cancel := context.WithCancel(ctx)
				cancel()

				if _, err := NewBackendRepo(cancelled, backend, DefaultLeasePolicy, time.Now); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled migration error=%v, want context.Canceled", err)
				}
				if after := readMigrationState(t, ctx, backend); !bytes.Equal(after, legacy) {
					t.Fatal("cancelled migration changed persisted v2 state")
				}
			})
		}
	})

	t.Run("panic", func(t *testing.T) {
		for _, test := range migrationBackendSources(t) {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				backend := openMigrationBackend(t, ctx, test.source)
				defer backend.Close()
				legacy := seedLegacyV2SubAgentState(t, ctx, backend)
				func() {
					defer func() {
						if recovered := recover(); recovered != "migration panic" {
							t.Fatalf("recovered panic=%#v", recovered)
						}
					}()
					_, _ = NewBackendRepo(ctx, panicMigrationBackend{Backend: backend}, DefaultLeasePolicy, time.Now)
				}()
				if after := readMigrationState(t, ctx, backend); !bytes.Equal(after, legacy) {
					t.Fatal("panicked migration changed persisted v2 state")
				}
			})
		}
	})
}

func migrationBackendSources(t *testing.T) []struct {
	name   string
	source publicstorage.Source
} {
	t.Helper()
	return []struct {
		name   string
		source publicstorage.Source
	}{
		{name: "memory", source: publicstorage.Memory()},
		{name: "sqlite", source: publicstorage.SQLite(filepath.Join(t.TempDir(), "migration.db"))},
	}
}

func openMigrationBackend(t *testing.T, ctx context.Context, source publicstorage.Source) spi.Backend {
	t.Helper()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(source))
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func seedLegacyV2SubAgentState(t *testing.T, ctx context.Context, backend spi.Backend) []byte {
	t.Helper()
	now := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	repo, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{
		ID: "child", ParentThreadID: "parent", TaskName: "worker", AgentPath: "/root/worker",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.PublishSubAgentInput(ctx, PublishSubAgentInputRequest{
		InputRequestID: "input", RequestFingerprint: "publish-fingerprint",
		ParentThreadID: "parent", ChildThreadID: "child",
		Message: session.Message{Role: session.User, Content: "continue"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AdmitSubAgentInput(ctx, AdmitSubAgentInputRequest{
		ParentThreadID: "parent", ChildThreadID: "child", TurnID: "turn", RunID: "run", OwnerID: "owner", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	var current []byte
	if err := repo.ViewDomain(ctx, func(memory *MemoryRepo, _ spi.ReadTx) error {
		current, err = memory.EncodeMemoryState()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(current, &state); err != nil {
		t.Fatal(err)
	}
	state["version"] = json.RawMessage("2")
	var admissions map[string]json.RawMessage
	if err := json.Unmarshal(state["turn_admissions"], &admissions); err != nil {
		t.Fatal(err)
	}
	delete(admissions, "child\x00turn")
	state["turn_admissions"], err = json.Marshal(admissions)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put("floret.domain", migrationStateKey, legacy)
	}); err != nil {
		t.Fatal(err)
	}
	return legacy
}

func readMigrationState(t *testing.T, ctx context.Context, backend spi.Backend) []byte {
	t.Helper()
	var encoded []byte
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		var err error
		encoded, err = tx.Get("floret.domain", migrationStateKey)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func readMigrationRecord(t *testing.T, ctx context.Context, backend spi.Backend, key []byte) []byte {
	t.Helper()
	var encoded []byte
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		var err error
		encoded, err = tx.Get("floret.domain", key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func recordExists(t *testing.T, ctx context.Context, backend spi.Backend, key []byte) bool {
	t.Helper()
	found := false
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		_, err := tx.Get("floret.domain", key)
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		found = err == nil
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return found
}

func seedLegacyV3StateWithoutInventory(t *testing.T, ctx context.Context, backend spi.Backend) []byte {
	t.Helper()
	repo, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 3, 13, 0, 0, 0, time.UTC)
	if _, err := repo.CreateRoot(ctx, CreateRootRequest{
		ThreadID: "migration-root", CreateIntentID: "create-migration-root", ContractVersion: "3",
		Meta: ThreadMeta{ID: "migration-root", CreatedAt: now, UpdatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	current := readMigrationState(t, ctx, backend)
	payload, err := storagecodec.DecodeEnvelope(current, "sessiontree")
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatal(err)
	}
	state["version"] = json.RawMessage("3")
	payload, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := tx.Put("floret.domain", migrationStateKey, legacy); err != nil {
			return err
		}
		return tx.Delete("floret.domain", migrationInventoryKey)
	}); err != nil {
		t.Fatal(err)
	}
	return legacy
}

func assertMigrationStateVersionAndAdmission(t *testing.T, encoded []byte, version int, admissionKey string) {
	t.Helper()
	payload, err := storagecodec.DecodeEnvelope(encoded, "sessiontree")
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		Version        int                        `json:"version"`
		TurnAdmissions map[string]json.RawMessage `json:"turn_admissions"`
	}
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatal(err)
	}
	if state.Version != version {
		t.Fatalf("persisted version=%d, want %d", state.Version, version)
	}
	if admissionKey != "" {
		if _, exists := state.TurnAdmissions[admissionKey]; !exists {
			t.Fatalf("persisted migration is missing admission %q", admissionKey)
		}
	}
}

type failingMigrationBackend struct {
	spi.Backend
	err error
}

func (backend failingMigrationBackend) Update(ctx context.Context, callback func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return callback(failingMigrationWriteTx{WriteTx: tx, err: backend.err})
	})
}

type failingMigrationWriteTx struct {
	spi.WriteTx
	err error
}

type targetedFailingMigrationBackend struct {
	spi.Backend
	key []byte
	err error
}

func (backend targetedFailingMigrationBackend) Update(ctx context.Context, callback func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return callback(targetedFailingMigrationWriteTx{WriteTx: tx, key: backend.key, err: backend.err})
	})
}

type targetedFailingMigrationWriteTx struct {
	spi.WriteTx
	key []byte
	err error
}

func (tx targetedFailingMigrationWriteTx) Put(namespace string, key, value []byte) error {
	if bytes.Equal(key, tx.key) {
		return tx.err
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func (tx failingMigrationWriteTx) Put(string, []byte, []byte) error {
	return tx.err
}

type panicMigrationBackend struct {
	spi.Backend
}

func (backend panicMigrationBackend) Update(ctx context.Context, callback func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return callback(panicMigrationWriteTx{WriteTx: tx})
	})
}

type panicMigrationWriteTx struct {
	spi.WriteTx
}

func (panicMigrationWriteTx) Put(string, []byte, []byte) error {
	panic("migration panic")
}
