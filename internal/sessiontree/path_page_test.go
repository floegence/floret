package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func appendPathTestEntry(tb testing.TB, repo *MemoryRepo, id, parent string) Entry {
	tb.Helper()
	entry, err := repo.Append(tb.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "payload " + id}}, AppendOptions{ID: id, ParentID: parent})
	if err != nil {
		tb.Fatal(err)
	}
	return entry
}

func TestPathPageMatchesCanonicalPathAcrossBranches(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(17, 29))
	var journal []Entry
	for i := range 512 {
		parent := ""
		if i > 0 {
			parent = journal[random.IntN(i)].ID
		}
		journal = append(journal, appendPathTestEntry(t, repo, fmt.Sprint(i), parent))
	}
	for _, leaf := range journal {
		path, err := repo.Path(t.Context(), "thread", leaf.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{1, 3, 150} {
			cursor, remaining := "", len(path)
			for {
				page, err := repo.PathPage(t.Context(), "thread", leaf.ID, cursor, limit)
				if err != nil {
					t.Fatalf("leaf %s cursor %s: %v", leaf.ID, cursor, err)
				}
				if page.NewestOrdinal != int64(remaining) || len(page.Entries) != min(limit, remaining) {
					t.Fatalf("wrong page bounds: %+v remaining %d", page, remaining)
				}
				for _, entry := range page.Entries {
					remaining--
					if !reflect.DeepEqual(entry, path[remaining]) {
						t.Fatalf("entry mismatch at %d", remaining)
					}
				}
				if page.HasMore != (remaining > 0) {
					t.Fatal("incorrect continuation")
				}
				if !page.HasMore {
					break
				}
				cursor = page.NextEntryID
			}
		}
	}
}

func TestPathPageCursorAndIsolation(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if page, err := repo.PathPage(t.Context(), "thread", "", "", 1); err != nil || len(page.Entries) != 0 {
		t.Fatalf("empty page: %+v %v", page, err)
	}
	root := appendPathTestEntry(t, repo, "root", "")
	left := appendPathTestEntry(t, repo, "left", root.ID)
	right := appendPathTestEntry(t, repo, "right", root.ID)
	for _, cursor := range []string{left.ID, "missing"} {
		if _, err := repo.PathPage(t.Context(), "thread", right.ID, cursor, 1); !errors.Is(err, ErrEntryNotFound) {
			t.Fatalf("off-path cursor %s: %v", cursor, err)
		}
	}
	page, err := repo.PathPage(t.Context(), "thread", right.ID, root.ID, 1)
	if err != nil || page.NewestOrdinal != 0 || len(page.Entries) != 0 || page.HasMore {
		t.Fatalf("exclusive root cursor: %+v %v", page, err)
	}
	page, err = repo.PathPage(t.Context(), "thread", "", "", 1)
	if err != nil || page.Entries[0].ID != right.ID {
		t.Fatalf("current leaf: %+v %v", page, err)
	}
	page.Entries[0].Message.Content = "changed detached read"
	stored, err := repo.Entry(t.Context(), "thread", right.ID)
	if err != nil || stored.Message.Content != right.Message.Content {
		t.Fatal("page changed canonical entry")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.PathPage(ctx, "thread", "", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: %v", err)
	}
	if _, err := repo.PathPage(t.Context(), "missing", "", "", 1); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("missing thread: %v", err)
	}
	if _, err := repo.PathPage(t.Context(), "thread", "", "", 0); err == nil {
		t.Fatal("zero limit accepted")
	}
}

func TestPathPageRejectsBrokenAncestorOutsidePage(t *testing.T) {
	for _, mode := range []string{"missing_parent", "raw_hash", "depth"} {
		t.Run(mode, func(t *testing.T) {
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			appendPathTestEntry(t, repo, "root", "")
			appendPathTestEntry(t, repo, "tail", "")
			switch mode {
			case "missing_parent":
				repo.entries["thread"][0].ParentID = "missing"
			case "raw_hash":
				repo.entries["thread"][0].RawHash = "corrupt"
			case "depth":
				repo.entries["thread"][0].PathDepth = 2
			}
			// Loading a journal derives this index only after source validation.
			repo.rebuildPathIndexLocked("thread")
			if _, err := repo.PathPage(t.Context(), "thread", "", "", 1); err == nil {
				t.Fatal("page adopted corrupt ancestry outside its selected window")
			}
		})
	}
}

func TestBackendPathPagePreservesAnchorsAcrossAppendForkAndRestart(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/pages.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
			return err
		}
		for i := range 300 {
			appendPathTestEntry(t, memory, fmt.Sprint(i), "")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first, err := repo.PathPage(t.Context(), "thread", "", "", 150)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "later"}}, AppendOptions{ID: "later"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", EntryID: "200", NewThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	for _, restart := range []bool{false, true} {
		if restart {
			repo, err = NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
		}
		page, err := repo.PathPage(t.Context(), "thread", "", first.NextEntryID, 150)
		if err != nil || len(page.Entries) != 150 || page.HasMore || page.NewestOrdinal != 150 || page.Entries[0].ID != "149" || page.Entries[149].ID != "0" {
			t.Fatalf("stable historical page after restart=%t: %+v %v", restart, page, err)
		}
		path, err := repo.Path(t.Context(), "fork", "")
		if err != nil || len(path) != 201 {
			t.Fatalf("fork path: %d %v", len(path), err)
		}
		page, err = repo.PathPage(t.Context(), "fork", "", "", 150)
		if err != nil || len(page.Entries) != 150 || page.NewestOrdinal != 201 || !reflect.DeepEqual(page.Entries[0], path[200]) || !reflect.DeepEqual(page.Entries[149], path[51]) {
			t.Fatalf("fork page after restart=%t: %+v %v", restart, page, err)
		}
	}
}

func BenchmarkPathPageBoundedHistory(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(b.Context(), ThreadMeta{ID: "thread"}); err != nil {
				b.Fatal(err)
			}
			for i := range count {
				appendPathTestEntry(b, repo, fmt.Sprint(i), "")
			}
			cursor := fmt.Sprint(count / 2)
			for _, fullPath := range []bool{false, true} {
				b.Run(fmt.Sprintf("full_path_%t", fullPath), func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if fullPath {
							path, err := repo.Path(b.Context(), "thread", "")
							if err != nil || len(path) != count {
								b.Fatalf("full path: %d %v", len(path), err)
							}
						} else {
							page, err := repo.PathPage(b.Context(), "thread", "", cursor, 150)
							if err != nil || len(page.Entries) != 150 {
								b.Fatalf("page: %d %v", len(page.Entries), err)
							}
						}
					}
				})
			}
		})
	}
}
