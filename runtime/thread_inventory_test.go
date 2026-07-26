package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestThreadInventoryListsOnlyCanonicalRootsAcrossStores(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var store *Store
			var path string
			var err error
			if backend == "sqlite" {
				path = filepath.Join(t.TempDir(), "floret.db")
				store, err = openSQLiteStoreForTest(path)
			} else {
				store = NewMemoryStore()
			}
			if err != nil {
				t.Fatal(err)
			}
			initialStore := store
			t.Cleanup(func() { _ = initialStore.Close() })
			capabilities := mustTestCapabilities(t, store)
			for _, threadID := range []ThreadID{"root-a", "root-b", "root-c"} {
				req := testCreateThreadRequest(threadID)
				create, err := capabilities.create.Bind(req.ThreadID, req.CreateIntentID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := create.CreateThread(ctx, req); err != nil {
					t.Fatal(err)
				}
			}
			archived, err := store.repo.Thread(ctx, "root-b")
			if err != nil {
				t.Fatal(err)
			}
			archived.Archived = true
			if err := store.repo.UpdateThread(ctx, archived); err != nil {
				t.Fatal(err)
			}
			publishTestSubAgentFixture(t, ctx, store, "publish-inventory-child", "root-c", "child-newer-than-roots", "")

			assertRootInventoryPages(t, ctx, capabilities.inventory)
			if backend != "sqlite" {
				return
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openSQLiteStoreForTest(path)
			if err != nil {
				t.Fatal(err)
			}
			store = reopened
			t.Cleanup(func() { _ = reopened.Close() })
			capabilities = mustTestCapabilities(t, store)
			assertRootInventoryPages(t, ctx, capabilities.inventory)
		})
	}
}

func assertRootInventoryPages(t *testing.T, ctx context.Context, inventory *ThreadInventoryHost) {
	t.Helper()
	seen := map[ThreadID]bool{}
	var cursor ThreadInventoryCursor
	for {
		page, err := inventory.ListRootThreads(ctx, ListRootThreadsRequest{Cursor: cursor, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Threads) != 1 {
			t.Fatalf("root page=%#v", page)
		}
		threadID := page.Threads[0].ID
		if threadID == "child-newer-than-roots" || seen[threadID] {
			t.Fatalf("invalid root inventory thread %q in %#v", threadID, page)
		}
		seen[threadID] = true
		if !page.HasMore {
			if page.NextCursor != "" {
				t.Fatalf("terminal page has cursor: %#v", page)
			}
			break
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			t.Fatalf("invalid continuation: %#v", page)
		}
		cursor = page.NextCursor
	}
	if len(seen) != 3 || !seen["root-a"] || !seen["root-b"] || !seen["root-c"] {
		t.Fatalf("root inventory=%v", seen)
	}
}

func TestThreadInventoryCursorValidation(t *testing.T) {
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	inventory := mustTestCapabilities(t, store).inventory
	now := time.Now().UTC()
	valid, err := encodeThreadInventoryCursor(now, "root")
	if err != nil {
		t.Fatal(err)
	}
	wrongVersion := testThreadInventoryCursor(t, threadInventoryCursorPayload{
		Version: threadInventoryVersion + 1, Mode: threadInventoryMode, CreatedAt: now.Format(time.RFC3339Nano), ThreadID: "root",
	})
	wrongMode := testThreadInventoryCursor(t, threadInventoryCursorPayload{
		Version: threadInventoryVersion, Mode: "children", CreatedAt: now.Format(time.RFC3339Nano), ThreadID: "root",
	})
	for name, cursor := range map[string]ThreadInventoryCursor{
		"malformed":     "not-base64!",
		"tampered":      valid + "x",
		"wrong version": wrongVersion,
		"wrong mode":    wrongMode,
	} {
		t.Run(name, func(t *testing.T) {
			page, err := inventory.ListRootThreads(context.Background(), ListRootThreadsRequest{Cursor: cursor, Limit: 1})
			if !errors.Is(err, ErrInvalidThreadInventoryCursor) || len(page.Threads) != 0 {
				t.Fatalf("page=%#v err=%v", page, err)
			}
		})
	}
}

func testThreadInventoryCursor(t *testing.T, payload threadInventoryCursorPayload) ThreadInventoryCursor {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return ThreadInventoryCursor(base64.RawURLEncoding.EncodeToString(raw))
}
