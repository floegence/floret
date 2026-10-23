package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestCanonicalPageReadsSharedBodyOncePerTransaction(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "page"}); err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("shared 中🌸\r\n", 2000)
	for i := range 32 {
		if _, err := memory.Append(t.Context(), Entry{ThreadID: "page", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: body}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "page"); err != nil {
		t.Fatal(err)
	}
	counted := &bodyReadCountingBackend{Backend: backend}
	repo.backend = counted
	input := PathWindowOptions{ThreadID: "page", Direction: PathWindowBefore, Limit: 32}
	page, err := repo.PathEntriesWindow(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	wantBlocks := (len(body)-1)/canonicalBodyBlockBytes + 1
	if len(page.Entries) != 32 || counted.blocks != wantBlocks {
		t.Fatalf("shared page: entries=%d blocks=%d want=%d", len(page.Entries), counted.blocks, wantBlocks)
	}
	for i, entry := range page.Entries {
		if entry.ID != fmt.Sprint(i) || entry.Message.Content != body || ValidateEntryIntegrity(entry) != nil {
			t.Fatal("canonical page changed")
		}
	}
	// A later transaction must read current source authority, even when a
	// previous page validated the same digest. No process-wide cache is used.
	ref := canonicalBodyRef{SHA256: stableHash(body), Bytes: len(body)}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, 0))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PathEntriesWindow(t.Context(), input); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("stale body survived transaction: %v", err)
	}
}

func TestCanonicalBodyPageBudgetAndCancellation(t *testing.T) {
	backend := newMigrationTestBackend()
	values := []string{strings.Repeat("a", canonicalBodyPageBytes/2), strings.Repeat("b", canonicalBodyPageBytes/2), strings.Repeat("c", canonicalBodyPageBytes+1)}
	refs := make([]canonicalBodyRef, len(values))
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for i, value := range values {
			var err error
			refs[i], err = retainCanonicalBody(t.Context(), tx, value)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		reader := newCanonicalBodyPageReader(t.Context(), tx)
		for i, ref := range refs {
			value, err := reader.read(ref)
			if err != nil {
				return err
			}
			if value != values[i] {
				t.Fatal("body changed")
			}
		}
		if reader.bytes != canonicalBodyPageBytes || len(reader.bodies) != 2 {
			t.Fatalf("unbounded retention: bytes=%d bodies=%d", reader.bytes, len(reader.bodies))
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		reader.ctx = ctx
		if _, err := reader.read(refs[0]); !errors.Is(err, context.Canceled) {
			t.Fatalf("cached body ignored cancellation: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
