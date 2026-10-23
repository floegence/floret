package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func TestProjectionWindowReadsOnlySelectedCanonicalFields(t *testing.T) {
	memory := newMemoryRepo(func() time.Time { return time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC) })
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "projection"}); err != nil {
		t.Fatal(err)
	}
	for i := range 32 {
		entry := Entry{ThreadID: "projection", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "provider result", Activity: &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "read", Output: strings.Repeat("display 🌸", 700)}}}}
		if _, err := memory.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprint(i), Now: time.Date(2026, 10, 23, 0, 0, i, 0, time.UTC)}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "projection"); err != nil {
		t.Fatal(err)
	}
	counted := &bodyReadCountingBackend{Backend: backend}
	repo.backend = counted
	fields := func(PathMetadata) []string { return []string{"content"} }
	read := func(ctx context.Context, reader PathProjectionScanner) (PathProjectionPage, error) {
		var result PathProjectionPage
		err := reader.ScanPathProjection(ctx, "projection", "", func([]PathMetadata) error { return nil }, fields, func(page PathProjectionPage) error { result = page; return nil })
		return result, err
	}
	got, err := read(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	want, err := read(t.Context(), memory)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("projection differs from memory: %v", err)
	}
	if counted.blocks != 1 {
		t.Fatalf("unselected activity bodies read: blocks=%d", counted.blocks)
	}
	for _, source := range got.Entries {
		if source.Entry.Raw != "" || source.Entry.Message.Content != "provider result" || source.Entry.Message.Activity != nil || len(source.Sources) != 2 {
			t.Fatal("partial projection lost source locators or advertised complete content")
		}
	}
	// A selected source is still validated in every new transaction.
	ref := canonicalBodyRef{SHA256: stableHash("provider result"), Bytes: len("provider result")}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, 0))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := read(t.Context(), repo); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt selected source accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := read(ctx, repo); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
