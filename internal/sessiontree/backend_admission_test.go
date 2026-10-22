package sessiontree

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32AdmissionRollbackReplayAndQueuePromotion(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "queued"}[queued], func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			backend, repo := openV31ReviewFixture(t, memory)
			if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			req := AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send-fp", InputRequestFingerprint: "input-fp", Input: session.Message{Role: session.User, Content: "hello canonical"}, Now: time.Unix(100, 0).UTC()}
			if queued {
				if _, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{{ID: "queue-added", Type: EntryQueueAdded, Payload: json.RawMessage(`{"id":"queued"}`)}}); err != nil {
					t.Fatal(err)
				}
				req.PromotedQueueID, req.PromotionRequestKey, req.PromotionRequestFingerprint = "queued", "promote", "promote-fp"
			}
			before := cloneMigrationRecords(backend.records)
			rollback := errors.New("admission rollback")
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
			if _, err := repo.AcceptTurn(t.Context(), req); !errors.Is(err, rollback) {
				t.Fatalf("rollback=%v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("partial admission survived")
			}
			repo.backend = backend
			accepted, err := repo.AcceptTurn(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			if accepted.Replayed || accepted.UserMessage.Message.Content != "hello canonical" {
				t.Fatalf("accepted=%+v", accepted)
			}
			if repo.threadMemory != nil {
				t.Fatal("acceptance loaded full journal")
			}
			replayed, err := repo.AcceptTurn(t.Context(), req)
			if err != nil || !replayed.Replayed || replayed.BaseLeafID != accepted.BaseLeafID {
				t.Fatalf("replay=%+v, %v", replayed, err)
			}
			if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, AppendOptions{ID: "terminal"}); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			read, found, err := repo.ReadAcceptedTurn(t.Context(), "thread", "turn", "run")
			if err != nil || !found || read.Terminal == nil || read.Terminal.Terminal.ID != "terminal" || read.UserMessage.Message.Content != "hello canonical" {
				t.Fatalf("read=%+v, %v, %v", read, found, err)
			}
			entry, found, err := repo.FindTurnRequest(t.Context(), "thread", "send")
			if err != nil || !found || entry.ID != accepted.TurnStarted.ID {
				t.Fatalf("request=%+v, %v, %v", entry, found, err)
			}
			if repo.threadMemory != nil {
				t.Fatal("replay read loaded full journal")
			}
		})
	}
}

func TestCanonicalLifecycleNestedTransactionKeepsPriorTurnFacts(t *testing.T) {
	base := CanonicalLifecycle{}
	base.Observe(Entry{Type: EntryTurnMarker, TurnID: "a", TurnStatus: TurnStarted}, true)
	first := base.transactionCopy()
	first.Observe(Entry{Type: EntryTurnMarker, TurnID: "b", TurnStatus: TurnStarted}, true)
	second := first.transactionCopy()
	second.Observe(Entry{Type: EntryTurnMarker, TurnID: "b", TurnStatus: TurnCompleted}, true)
	second.installTransaction()
	if !second.started["a"] || !second.started["b"] || !second.terminal["b"] {
		t.Fatalf("lost transaction prefix: %+v", second)
	}
}
