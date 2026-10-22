package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type metadataMutationBackend struct{ spi.Backend }

func (backend metadataMutationBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(boundedPageTx{ReadTx: tx, backend: &boundedPageBackend{}}) })
}
func (backend metadataMutationBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(metadataMutationTx{tx}) })
}

type metadataMutationTx struct{ spi.WriteTx }

func (tx metadataMutationTx) Scan(spi.ScanRequest) (spi.ScanPage, error) {
	return spi.ScanPage{}, errors.New("metadata mutation scanned historical records")
}
func (tx metadataMutationTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == canonicalBodyNamespace {
		return nil, errors.New("metadata mutation read a body")
	}
	return tx.WriteTx.Get(namespace, key)
}

func TestV32MetadataMutationsNeverLoadJournals(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained"}}, AppendOptions{ID: "entry"}); err != nil {
		t.Fatal(err)
	}
	base, owner := openV31ReviewFixture(t, memory)
	if err := owner.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	owner.backend = metadataMutationBackend{base}
	owner.threadMemory, owner.verifiedThreadLeaves = nil, nil
	repo := NewDeferredBackendRepo(owner.backend, time.Now, nil)
	if err := repo.InstallDelegate(owner); err != nil {
		t.Fatal(err)
	}
	title, err := repo.SetThreadTitle(t.Context(), SetThreadTitleRequest{ThreadID: "thread", Title: "A new title", Now: time.Now()})
	if err != nil || !title.Changed {
		t.Fatalf("title=%+v, %v", title, err)
	}
	meta := title.Thread
	meta.Archived = true
	if err := repo.UpdateThread(t.Context(), meta); err != nil {
		t.Fatal(err)
	}
	todo, err := repo.CompareAndSwapAgentTodoState(t.Context(), AgentTodoState{ThreadID: "thread"}, 0)
	if err != nil || todo.Version != 1 {
		t.Fatalf("todo=%+v, %v", todo, err)
	}
	if _, err := repo.CompareAndSwapAgentTodoState(t.Context(), AgentTodoState{ThreadID: "thread"}, 0); !errors.Is(err, ErrAgentTodoVersionConflict) {
		t.Fatalf("stale todo=%v", err)
	}
	if read, err := repo.ReadAgentTodoState(t.Context(), "thread"); err != nil || read.Version != 1 {
		t.Fatalf("read todo=%+v, %v", read, err)
	}
	state := ProviderStateRecord{ThreadID: "thread", LeafEntryID: "entry", CompatibilityKey: "test", State: provider.State{Kind: "test", ID: "state"}, CreatedByRunID: "run", CreatedByTurnID: "turn", UpdatedAt: time.Now()}
	if err := repo.PutProviderState(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	if read, err := repo.ProviderState(t.Context(), "thread"); err != nil || read.State.ID != "state" {
		t.Fatalf("provider state=%+v, %v", read, err)
	}
	if err := repo.DeleteProviderState(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ProviderState(t.Context(), "thread"); !errors.Is(err, ErrProviderStateNotFound) {
		t.Fatalf("deleted state=%v", err)
	}
	if owner.threadMemory != nil {
		t.Fatal("metadata operation loaded a journal")
	}
}
