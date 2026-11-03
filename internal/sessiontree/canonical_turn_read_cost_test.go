package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestCanonicalTurnReadReusesValidatedCurrentTurn(t *testing.T) {
	repo := newMemoryRepo(time.Now)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "request", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		if _, err := repo.Append(t.Context(), RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, Content: fmt.Sprint(i)}, time.Time{}), AppendOptions{ID: fmt.Sprintf("message-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := repo.Thread(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	reader := repo.canonicalTurnReaderLocked()
	turnReads, traversed := 0, int64(0)
	readTurn, ancestor := reader.turn, reader.ancestor
	reader.turn = func(thread, turn string) ([]Entry, error) { turnReads++; return readTurn(thread, turn) }
	reader.ancestor = func(thread, descendant, source string) (bool, error) {
		traversed += max(0, repo.entryDepths[thread][descendant]-repo.entryDepths[thread][source])
		return ancestor(thread, descendant, source)
	}
	got, err := reader.read("thread", "turn", meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Turn.Entries) != 514 || !reflect.DeepEqual(got.Turn, got.LatestTurn) || !got.HasRetryTarget {
		t.Fatalf("incomplete turn: entries=%d retry=%v", len(got.Turn.Entries), got.HasRetryTarget)
	}
	if turnReads != 1 {
		t.Errorf("current turn decoded %d times, want once", turnReads)
	}
	if traversed > 2*int64(len(got.Turn.Entries)) {
		t.Errorf("ancestor traversal=%d for %d entries: repeated walks from the leaf", traversed, len(got.Turn.Entries))
	}
	got.Turn.Entries[0].Entry.Metadata["mutation"] = "caller"
	if got.LatestTurn.Entries[0].Entry.Metadata["mutation"] != "" {
		t.Error("returned turns share mutable entries")
	}
}

type canonicalReadCountingTx struct {
	spi.ReadTx
	gets int
}

func (tx *canonicalReadCountingTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.gets++
	return tx.ReadTx.Get(namespace, key)
}

func TestCanonicalTurnReadRetainsBranchIntegrityWithLinearReads(t *testing.T) {
	for _, count := range []int{32, 512} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "request", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				if _, err := memory.Append(t.Context(), RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, Content: fmt.Sprint(i)}, time.Time{}), AppendOptions{ID: fmt.Sprintf("message-%d", i)}); err != nil {
					t.Fatal(err)
				}
			}
			meta, err := memory.Thread(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			// A newer off-path sibling must not move the proven active descendant.
			if err := memory.MoveLeaf(t.Context(), "thread", "message-0"); err != nil {
				t.Fatal(err)
			}
			if _, err := memory.Append(t.Context(), RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, Content: "inactive"}, time.Time{}), AppendOptions{ID: "off-path"}); err != nil {
				t.Fatal(err)
			}
			if err := memory.MoveLeaf(t.Context(), "thread", meta.LeafID); err != nil {
				t.Fatal(err)
			}
			want, err := memory.ReadCanonicalTurn(t.Context(), "thread", "turn")
			if err != nil {
				t.Fatal(err)
			}
			backend, repo := openV31ReviewFixture(t, memory)
			if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				counted := &canonicalReadCountingTx{ReadTx: tx}
				got, err := newCanonicalTurnReader(t.Context(), counted).read("thread", "turn", meta.LeafID)
				if err != nil {
					return err
				}
				gotJSON, _ := json.Marshal(got)
				wantJSON, _ := json.Marshal(want)
				if string(gotJSON) != string(wantJSON) || len(got.Turn.Entries) != count+2 {
					t.Error("canonical reader changed branch membership or output")
				}
				if counted.gets > 14*(count+3)+100 {
					t.Errorf("read %d records for %d turn entries", counted.gets, count+3)
				}
				cancelled, cancel := context.WithCancel(t.Context())
				cancel()
				if _, err := newCanonicalTurnReader(cancelled, tx).read("thread", "turn", meta.LeafID); !errors.Is(err, context.Canceled) {
					t.Errorf("cancellation=%v", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return tx.Delete(backendEntryIndexNamespace, entryLocatorKey("thread", "message-3"))
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadCanonicalTurn(t.Context(), "thread", "turn"); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("missing ancestor accepted: %v", err)
			}
		})
	}
}
