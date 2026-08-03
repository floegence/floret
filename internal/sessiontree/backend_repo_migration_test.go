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

func TestBackendRepoAutomaticallyPersistsV2DomainMigration(t *testing.T) {
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
				t.Fatal("startup did not persist the v2 to v3 migration")
			}
			assertMigrationStateVersionAndAdmission(t, first, 3, "child\x00turn")

			if _, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now); err != nil {
				t.Fatal(err)
			}
			second := readMigrationState(t, ctx, backend)
			if !bytes.Equal(second, first) {
				t.Fatal("current v3 startup rewrote canonical domain state")
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
	if _, exists := state.TurnAdmissions[admissionKey]; !exists {
		t.Fatalf("persisted migration is missing admission %q", admissionKey)
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
