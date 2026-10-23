package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type sparseReadBackend struct {
	spi.Backend
	gets, scans int
}
type sparseReadTx struct {
	spi.ReadTx
	owner *sparseReadBackend
}

func (b *sparseReadBackend) View(ctx context.Context, f func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return f(sparseReadTx{tx, b}) })
}
func (t sparseReadTx) Get(ns string, key []byte) ([]byte, error) {
	t.owner.gets++
	if ns == canonicalBodyNamespace {
		return nil, errors.New("sparse sources read a body")
	}
	return t.ReadTx.Get(ns, key)
}
func (t sparseReadTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	t.owner.scans++
	if req.Namespace != sparseSourcesNamespace {
		return spi.ScanPage{}, errors.New("sparse sources scanned complete history")
	}
	return t.ReadTx.Scan(req)
}
func TestSparseSourcesRetainBranchesAndStayBoundedAfterRestart(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 2048 {
		if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprint("message-", i), ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "preserved source"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := memory.Append(t.Context(), Entry{ID: "queue", ThreadID: "thread", Type: EntryQueueAdded, Payload: []byte("{}")}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := memory.MoveLeaf(t.Context(), "thread", "message-2047"); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ID: "branch", ThreadID: "thread", Type: EntryCustom}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	want, err := repo.QueueMetadata(t.Context(), "thread")
	if err != nil || len(want) != 1 || want[0].ID != "queue" {
		t.Fatalf("queue=%+v %v", want, err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	counted := &sparseReadBackend{Backend: backend}
	repo.backend = counted
	got, err := repo.QueueMetadata(t.Context(), "thread")
	if err != nil || !reflect.DeepEqual(got, want) || counted.gets > 40 || counted.scans != 1 {
		t.Fatalf("queue=%+v err=%v gets=%d scans=%d", got, err, counted.gets, counted.scans)
	}
	counted.gets, counted.scans = 0, 0
	events, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "thread", Limit: 10})
	if err != nil || len(events.Events) != 0 || counted.gets > 40 || counted.scans != 1 {
		t.Fatalf("parents=%+v err=%v gets=%d scans=%d", events, err, counted.gets, counted.scans)
	}
	// Simulate a released writer with no derived locator namespace.
	repo.backend = backend
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		page, err := tx.Scan(spi.ScanRequest{Namespace: sparseSourcesNamespace, Limit: 128})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			if err := tx.Delete(sparseSourcesNamespace, row.Key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err = repo.QueueMetadata(t.Context(), "thread")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("rebuilt=%+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.QueueMetadata(ctx, "thread"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
