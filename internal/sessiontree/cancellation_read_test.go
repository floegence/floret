package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type cancellationReadBudget struct {
	spi.Backend
	bytes int
}

type cancellationBudgetTx struct {
	spi.ReadTx
	budget *cancellationReadBudget
}

func (b *cancellationReadBudget) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return read(&cancellationBudgetTx{tx, b}) })
}
func (tx *cancellationBudgetTx) Get(namespace string, key []byte) ([]byte, error) {
	value, err := tx.ReadTx.Get(namespace, key)
	tx.budget.bytes += len(value)
	return value, err
}
func (tx *cancellationBudgetTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	page, err := tx.ReadTx.Scan(request)
	for _, record := range page.Records {
		tx.budget.bytes += len(record.Value)
	}
	return page, err
}

func TestReadTurnCancellationUsesOnlyCanonicalControlRecords(t *testing.T) {
	memory := NewMemoryRepo()
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: strings.Repeat("history", 128*1024)}}); err != nil {
		t.Fatal(err)
	}
	request := testCancelTurnRequest(time.Now())
	request.RequestOnly = true
	request.CancellationMetadata = map[string]string{"cancellation_source": "user_stop", "cancellation_mode": "graceful"}
	cancelled, err := memory.CancelTurn(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	stored, err := repo.Entry(t.Context(), "thread", cancelled.CancelRequest.ID)
	if err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	budget := &cancellationReadBudget{Backend: backend}
	repo.backend = budget
	for name, reader := range map[string]TurnCancellationReader{"memory": memory, "backend": repo} {
		t.Run(name, func(t *testing.T) {
			want := cancelled.CancelRequest
			if name == "backend" {
				want = stored
			}
			got, err := reader.ReadTurnCancellation(t.Context(), "thread", "turn")
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("cancel=%+v err=%v", got, err)
			}
			got.Metadata["cancellation_source"] = "changed"
			again, err := reader.ReadTurnCancellation(t.Context(), "thread", "turn")
			if err != nil || !reflect.DeepEqual(again, want) {
				t.Fatalf("mutated cancellation=%+v err=%v", again, err)
			}
			if got, err := reader.ReadTurnCancellation(t.Context(), "thread", "missing"); err != nil || got.ID != "" {
				t.Fatalf("absent=%+v err=%v", got, err)
			}
			if _, err := reader.ReadTurnCancellation(t.Context(), "missing", "turn"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("missing thread=%v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := reader.ReadTurnCancellation(ctx, "thread", "turn"); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read=%v", err)
			}
		})
	}
	if budget.bytes > 32*1024 || repo.threadMemory != nil {
		t.Fatalf("cancellation hydrated history: bytes=%d", budget.bytes)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("read changed canonical records")
	}
	for _, target := range []string{"missing", cancelled.CancelRequest.ParentID} {
		t.Run("corrupt_locator_"+target, func(t *testing.T) {
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return tx.Put(backendEntryIndexNamespace, backendDomainKey("turn", "thread", "turn"), []byte(`{"cancellation":"`+target+`"}`))
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadTurnCancellation(t.Context(), "thread", "turn"); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("corrupt locator=%v", err)
			}
		})
	}
}
