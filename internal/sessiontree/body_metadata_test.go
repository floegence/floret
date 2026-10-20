package sessiontree

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestCanonicalBodyMetadataReadsNoBodyAndChecksActivePath(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("中文🌸\r\n", 10000)
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "sibling"}}, AppendOptions{ID: "sibling"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "root"); err != nil {
		t.Fatal(err)
	}
	repo.backend = failingBodyReadBackend{Backend: backend, failure: errors.New("metadata read source bytes")}
	metadata, err := repo.CanonicalBodyMetadata(t.Context(), "thread", "root", "content")
	if err != nil || metadata.Bytes != len(text) || metadata.SHA256 != stableHash(text) {
		t.Fatalf("metadata=%+v err=%v", metadata, err)
	}
	if _, err := repo.CanonicalBodyMetadata(t.Context(), "thread", "sibling", "content"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("inactive source=%v", err)
	}
	if _, err := repo.CanonicalBodyMetadata(t.Context(), "thread", "root", "unsupported"); !errors.Is(err, spi.ErrInvalidArgument) {
		t.Fatalf("unknown field=%v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.CanonicalBodyMetadata(ctx, "thread", "root", "content"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}
