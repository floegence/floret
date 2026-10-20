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
)

func TestCanonicalBodiesStayOutOfRetainedJournal(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("中文🌸\r\n", 50000)
	saved, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{ID: "large"})
	if err != nil {
		t.Fatal(err)
	}
	assertCompact := func(repo *BackendRepo) {
		t.Helper()
		retained := repo.domainMemory.entries["thread"][0]
		if retained.Message.Content != "" || retained.Raw != "" || retained.bodyFields["content"].Bytes != len(text) {
			t.Fatal("historical body retained in journal")
		}
	}
	assertCompact(repo)
	for range 2 {
		entry, err := repo.Entry(t.Context(), "thread", "large")
		if err != nil || !reflect.DeepEqual(saved, entry) {
			t.Fatalf("complete entry differs: %v", err)
		}
		page, err := repo.PathPage(t.Context(), "thread", "", "", 1)
		if err != nil || len(page.Entries) != 1 || !reflect.DeepEqual(saved, page.Entries[0]) {
			t.Fatalf("page differs: %v", err)
		}
		assertCompact(repo)
	}
	reopened, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	assertCompact(reopened)
	// Release the shared source cache so a real storage read must observe failure.
	releaseCachedBody(repo.domainMemory.entries["thread"][0].bodyFields["content"])
	failure := errors.New("body read failed")
	repo.backend = failingBodyReadBackend{Backend: backend, failure: failure}
	if _, err := repo.Entry(t.Context(), "thread", "large"); !errors.Is(err, failure) {
		t.Fatalf("body failure hidden: %v", err)
	}
	assertCompact(repo)
}

type failingBodyReadBackend struct {
	spi.Backend
	failure error
}
type failingBodyReadTx struct {
	spi.ReadTx
	failure error
}

func (b failingBodyReadBackend) View(ctx context.Context, callback func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return callback(failingBodyReadTx{tx, b.failure}) })
}
func (tx failingBodyReadTx) Get(ns string, key []byte) ([]byte, error) {
	if ns == canonicalBodyNamespace {
		return nil, tx.failure
	}
	return tx.ReadTx.Get(ns, key)
}

func TestCanonicalCompletePathMatchesOracleWithInactiveSiblings(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		parent := ""
		if i > 0 {
			parent = fmt.Sprint((i - 1) / 2)
		}
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, AppendOptions{ID: fmt.Sprint(i), ParentID: parent}); err != nil {
			t.Fatal(err)
		}
	}
	for _, leaf := range []string{"0", "1", "72", "999"} {
		want, err := pathLocked(repo.threads, repo.entries, "thread", leaf)
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.Path(t.Context(), "thread", leaf)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("path %s differs: %v", leaf, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.Path(ctx, "thread", ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read accepted: %v", err)
	}
}

func TestCanonicalBodyCacheEvictsWithinGlobalBudget(t *testing.T) {
	// Vary the limit only by supplying enough distinct source bytes, preserving
	// the production budget and the actual cache eviction path.
	text := strings.Repeat("x", 1<<20)
	for i := range 132 {
		value := fmt.Sprintf("%03d", i) + text
		ref := canonicalBodyRef{stableHash(value), len(value)}
		cacheBody(ref, value)
	}
	canonicalBodyCache.Lock()
	defer canonicalBodyCache.Unlock()
	if canonicalBodyCache.bytes > canonicalBodyCacheBytes || canonicalBodyCache.order.Len() != len(canonicalBodyCache.entries) {
		t.Fatal("cache budget or ownership mismatch")
	}
	var total int
	for _, element := range canonicalBodyCache.entries {
		total += len(element.Value.(cachedCanonicalBody).text)
	}
	if total != canonicalBodyCache.bytes {
		t.Fatal("cache accounting mismatch")
	}
}

func TestRootInventoryReadsBodiesOnlyForSelectedPage(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "older", CreatedAt: time.Unix(1, 0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "older", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("history", 1000)}}, AppendOptions{ID: "body"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "newer", CreatedAt: time.Unix(2, 0)}); err != nil {
		t.Fatal(err)
	}
	releaseCachedBody(repo.domainMemory.entries["older"][0].bodyFields["content"])
	failure := errors.New("unselected body was read")
	repo.backend = failingBodyReadBackend{Backend: backend, failure: failure}
	page, err := repo.ListRootThreadInventory(t.Context(), ListThreadsOptions{Limit: 1})
	if err != nil || len(page) != 1 || page[0].Meta.ID != "newer" {
		t.Fatalf("selected page len=%d err=%v", len(page), err)
	}
	if _, err := repo.ListRootThreadInventory(t.Context(), ListThreadsOptions{}); !errors.Is(err, failure) {
		t.Fatalf("selected body failure hidden: %v", err)
	}
}
