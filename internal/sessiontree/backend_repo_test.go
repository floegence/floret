package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	internalprovider "github.com/floegence/floret/v2/internal/provider"
	"github.com/floegence/floret/v2/internal/session"
	publicstorage "github.com/floegence/floret/v2/storage"
)

func TestBackendRepoMethodSetTracksCanonicalMemoryRepo(t *testing.T) {
	memoryType := reflect.TypeOf((*MemoryRepo)(nil))
	backendType := reflect.TypeOf((*BackendRepo)(nil))
	backendOnly := map[string]bool{"UpdateDomain": true, "ViewDomain": true}
	memoryOnly := map[string]bool{
		"CommitForkBatch": true, "EncodeMemoryState": true, "FailForkClaim": true,
		"ReleaseThreadAuthorityClaim": true,
	}

	for index := 0; index < memoryType.NumMethod(); index++ {
		memoryMethod := memoryType.Method(index)
		if memoryOnly[memoryMethod.Name] {
			continue
		}
		backendMethod, found := backendType.MethodByName(memoryMethod.Name)
		if !found {
			t.Errorf("BackendRepo is missing MemoryRepo.%s", memoryMethod.Name)
			continue
		}
		assertEquivalentMethodSignature(t, memoryMethod, backendMethod)
	}
	for index := 0; index < backendType.NumMethod(); index++ {
		method := backendType.Method(index)
		if backendOnly[method.Name] {
			continue
		}
		if _, found := memoryType.MethodByName(method.Name); !found {
			t.Errorf("BackendRepo has unclassified method %s", method.Name)
		}
	}
}

func assertEquivalentMethodSignature(t *testing.T, memory, backend reflect.Method) {
	t.Helper()
	if memory.Type.NumIn() != backend.Type.NumIn() || memory.Type.NumOut() != backend.Type.NumOut() || memory.Type.IsVariadic() != backend.Type.IsVariadic() {
		t.Errorf("%s signature differs: memory=%s backend=%s", memory.Name, memory.Type, backend.Type)
		return
	}
	for index := 1; index < memory.Type.NumIn(); index++ {
		if memory.Type.In(index) != backend.Type.In(index) {
			t.Errorf("%s argument %d differs: memory=%s backend=%s", memory.Name, index, memory.Type.In(index), backend.Type.In(index))
		}
	}
	for index := 0; index < memory.Type.NumOut(); index++ {
		if memory.Type.Out(index) != backend.Type.Out(index) {
			t.Errorf("%s result %d differs: memory=%s backend=%s", memory.Name, index, memory.Type.Out(index), backend.Type.Out(index))
		}
	}
}

func TestBackendRepoDomainStateIsIdenticalAcrossMemoryAndSQLite(t *testing.T) {
	memoryState := runBackendRepoDomainScript(t, publicstorage.Memory())
	sqliteState := runBackendRepoDomainScript(t, publicstorage.SQLite(filepath.Join(t.TempDir(), "domain.db")))
	if !bytes.Equal(memoryState, sqliteState) {
		t.Fatalf("canonical domain state differs across backends\nmemory: %s\nsqlite: %s", memoryState, sqliteState)
	}
}

func runBackendRepoDomainScript(t *testing.T, source publicstorage.Source) []byte {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	backend, err := source.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	repo, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateRoot(ctx, CreateRootRequest{
		ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "2",
		Meta: ThreadMeta{ID: "root", CreatedAt: now, UpdatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	entry := Entry{
		ID: "entry-1", ThreadID: "root", TurnID: "turn-1", Type: EntryUserMessage,
		CreatedAt: now.Add(time.Minute), Message: session.Message{Role: session.User, Content: "hello"},
	}
	if _, err := repo.Append(ctx, entry, AppendOptions{ID: entry.ID, Now: entry.CreatedAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "root", Title: "Canonical title", Now: now.Add(2 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	lease, err := repo.AcquireTurnLease(ctx, TurnLease{ThreadID: "root", TurnID: "turn-1", OwnerID: "runner-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.PutProviderState(ContextWithTurnLease(ctx, lease), ProviderStateRecord{
		ThreadID: "root", LeafEntryID: "entry-1", CompatibilityKey: "gateway/model",
		State:          internalprovider.State{Kind: "opaque", ID: "state-1", Attributes: map[string]string{"cursor": "next"}},
		CreatedByRunID: "run-1", CreatedByTurnID: "turn-1", UpdatedAt: now.Add(3 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	if err := repo.ViewDomain(ctx, func(memory *MemoryRepo, _ publicstorage.ReadTx) error {
		encoded, err = memory.EncodeMemoryState()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestBackendRepoRollsBackDomainMutationOnErrorAndPanic(t *testing.T) {
	for _, source := range []struct {
		name   string
		source publicstorage.Source
	}{
		{name: "memory", source: publicstorage.Memory()},
		{name: "sqlite", source: publicstorage.SQLite(filepath.Join(t.TempDir(), "rollback.db"))},
	} {
		t.Run(source.name, func(t *testing.T) {
			ctx := context.Background()
			backend, err := source.source.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			repo, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected rollback")
			if err := repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ publicstorage.WriteTx) error {
				if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "error-thread"}); err != nil {
					return err
				}
				return injected
			}); !errors.Is(err, injected) {
				t.Fatalf("error callback = %v", err)
			}
			func() {
				defer func() {
					if recovered := recover(); recovered != "domain panic" {
						t.Fatalf("recovered panic = %#v", recovered)
					}
				}()
				_ = repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ publicstorage.WriteTx) error {
					if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "panic-thread"}); err != nil {
						return err
					}
					panic("domain panic")
				})
			}()
			for _, threadID := range []string{"error-thread", "panic-thread"} {
				if _, err := repo.Thread(ctx, threadID); !errors.Is(err, ErrThreadNotFound) {
					t.Fatalf("rolled-back thread %q read error = %v", threadID, err)
				}
			}
		})
	}
}
