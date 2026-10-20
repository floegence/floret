package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestCanonicalBodyReaderStreamsExactBytesWithoutMaterializing(t *testing.T) {
	for _, size := range []int{40 << 10, 400 << 10, 4 << 20, 40 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			backend := newMigrationTestBackend()
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			text := strings.Repeat("a中文🌸é\r\n\x00", size/15+1)
			if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{ID: "body"}); err != nil {
				t.Fatal(err)
			}
			ref := repo.domainMemory.entries["thread"][0].bodyFields["content"]
			releaseCachedBody(ref)
			counted := &bodyReadCountingBackend{Backend: backend}
			repo.backend = counted
			hash := sha256.New()
			for offset := 0; offset < len(text); {
				counted.blocks = 0
				part, err := repo.ReadCanonicalBody(t.Context(), "thread", "body", "content", offset, canonicalBodyBlockBytes)
				if err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(part)
				if part.Offset != offset || part.Bytes != len(text) || part.SHA256 != ref.SHA256 || len(part.Text) == 0 || len(part.Text) > canonicalBodyBlockBytes || len(raw) > canonicalBodyReadBytes || !utf8.ValidString(part.Text) || counted.blocks > 2 {
					t.Fatalf("unbounded or invalid chunk: offset=%d bytes=%d blocks=%d", offset, len(part.Text), counted.blocks)
				}
				hash.Write([]byte(part.Text))
				offset += len(part.Text)
			}
			if hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
				t.Fatal("body hash changed")
			}
			if _, cached := cachedBody(ref); cached {
				t.Fatal("chunk reader materialized the complete body")
			}
		})
	}
}

type bodyReadCountingBackend struct {
	spi.Backend
	blocks int
}
type bodyReadCountingTx struct {
	spi.ReadTx
	backend *bodyReadCountingBackend
}

func (backend *bodyReadCountingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(bodyReadCountingTx{tx, backend}) })
}
func (tx bodyReadCountingTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == canonicalBodyNamespace && strings.Contains(string(key), "block") {
		tx.backend.blocks++
	}
	return tx.ReadTx.Get(namespace, key)
}

func TestCanonicalBodyReaderRequiresCurrentPathAndValidOffsets(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	root := appendPathTestEntry(t, repo, "root", "")
	left := appendPathTestEntry(t, repo, "left", root.ID)
	right := appendPathTestEntry(t, repo, "right", root.ID)
	if _, err := repo.ReadCanonicalBody(t.Context(), "thread", left.ID, "content", 0, 16); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("inactive body readable: %v", err)
	}
	for _, test := range []struct {
		field         string
		offset, limit int
	}{{"unknown", 0, 16}, {"content", -1, 16}, {"content", 0, 3}, {"content", 0, canonicalBodyBlockBytes + 1}, {"content", 10000, 16}} {
		if _, err := repo.ReadCanonicalBody(t.Context(), "thread", right.ID, test.field, test.offset, test.limit); !errors.Is(err, spi.ErrInvalidArgument) {
			t.Fatalf("invalid read accepted: %+v %v", test, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.ReadCanonicalBody(ctx, "thread", root.ID, "content", 0, 16); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel ignored: %v", err)
	}
	if _, err := repo.ReadCanonicalBody(t.Context(), "missing", root.ID, "content", 0, 16); !errors.Is(err, ErrThreadNotFound) {
		t.Fatal(err)
	}
	part, err := repo.ReadCanonicalBody(t.Context(), "thread", right.ID, "content", 0, 16)
	if err != nil || part.Text != right.Message.Content {
		t.Fatalf("body=%q err=%v", part.Text, err)
	}
}
