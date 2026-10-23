package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type checkpointReadTx struct {
	spi.ReadTx
	blocks map[string]int
}

func (tx checkpointReadTx) Get(ns string, key []byte) ([]byte, error) {
	if ns == canonicalBodyNamespace && bytes.HasPrefix(key, backendDomainKey("block")) {
		tx.blocks[string(key)]++
	}
	return tx.ReadTx.Get(ns, key)
}

func TestCheckpointValidationReadsSamePathBodiesOnceAndResetsForBranches(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	add := func(e Entry) Entry {
		t.Helper()
		e.ThreadID = "thread"
		got, err := memory.Append(t.Context(), e, AppendOptions{ID: e.ID})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	var checkpoints []Entry
	for i := range 8 {
		user := add(Entry{ID: fmt.Sprint("user-", i), Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint("unique source ", i)}})
		checkpoints = append(checkpoints, add(Entry{ID: fmt.Sprint("compact-", i), Type: EntryCompaction, Summary: fmt.Sprint("unique summary ", i), CompactedThroughEntryID: user.ID}))
	}
	if err := memory.MoveLeaf(t.Context(), "thread", "user-0"); err != nil {
		t.Fatal(err)
	}
	branch := add(Entry{ID: "branch", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "branch source"}})
	checkpoints = append(checkpoints, add(Entry{ID: "branch-compact", Type: EntryCompaction, Summary: "branch summary", CompactedThroughEntryID: branch.ID}))
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		counted := checkpointReadTx{ReadTx: tx, blocks: map[string]int{}}
		var validation canonicalCheckpointValidation
		for i, checkpoint := range checkpoints {
			if err := validation.validate(t.Context(), counted, "thread", checkpoint); err != nil {
				return err
			}
			path, err := memory.Path(t.Context(), "thread", checkpoint.ID)
			if err != nil {
				return err
			}
			want, err := originalContextMessages(path)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(want, validation.context.Messages()) {
				t.Fatalf("checkpoint %d differs", i)
			}
			if i == 7 {
				if len(counted.blocks) != 16 {
					t.Fatalf("read %d unique blocks, want 16", len(counted.blocks))
				}
				for _, reads := range counted.blocks {
					if reads != 1 {
						t.Fatalf("repeated prefix body read %d times", reads)
					}
				}
			}
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := validation.validate(ctx, tx, "thread", checkpoints[0]); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointValidationPreservesRetrySelection(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	add := func(e Entry) Entry {
		t.Helper()
		e.ThreadID = "thread"
		got, err := memory.Append(t.Context(), e, AppendOptions{ID: e.ID})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	add(Entry{ID: "start", Type: EntryTurnMarker, TurnID: "first", RunID: "run", TurnStatus: TurnStarted})
	add(Entry{ID: "user", Type: EntryUserMessage, TurnID: "first", RunID: "run", Message: session.Message{Role: session.User, Content: "retained input"}})
	add(Entry{ID: "answer", Type: EntryAssistantMessage, TurnID: "first", RunID: "run", Message: session.Message{Role: session.Assistant, Content: "superseded response"}})
	add(Entry{ID: "done", Type: EntryTurnMarker, TurnID: "first", RunID: "run", TurnStatus: TurnCompleted})
	add(Entry{ID: "retry", Type: EntryTurnMarker, TurnID: "second", RunID: "run2", TurnStatus: TurnStarted, Metadata: map[string]string{RetrySourceTurnIDMetadataKey: "first", RetrySourceEntryIDMetadataKey: "user"}})
	add(Entry{ID: "final", Type: EntryAssistantMessage, TurnID: "second", RunID: "run2", Message: session.Message{Role: session.Assistant, Content: "corrected response"}})
	checkpoint := add(Entry{ID: "compact", Type: EntryCompaction, Summary: "retained corrected context", CompactedThroughEntryID: "final"})
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var validation canonicalCheckpointValidation
		if err := validation.validate(t.Context(), tx, "thread", checkpoint); err != nil {
			return err
		}
		if validation.leaf.ID != "" || len(validation.context.Messages()) != 0 {
			t.Fatal("Retry left a partial incremental prefix")
		}
		checkpoint.CompactedThroughEntryID = "absent"
		// Validation reads canonical source authority, so mutate a copied source only
		// when exercising the complete reducer's corruption classification directly.
		path, err := canonicalCompletePath(t.Context(), tx, "thread", "compact")
		if err != nil {
			return err
		}
		path[len(path)-1] = checkpoint
		if _, err := BuildContextChecked(path, ContextOptions{}); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
