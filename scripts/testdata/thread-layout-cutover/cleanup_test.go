//go:build floret_layout_cutover

package cutover

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	flstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOfflineCleanupChecksEveryAuthorAndRollsBack(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		ctx := t.Context()
		dir, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "source.sqlite")
		host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		backend, err := storagebridge.Open(ctx, storagebridge.Source(storage.SQLite(path)))
		if err != nil {
			t.Fatal(err)
		}
		kernel, err := flstorage.NewBackendKernel(ctx, backend, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		for _, thread := range []string{"alpha", "alpha-extra", "omega"} {
			if _, err := kernel.CreateThread(ctx, sessiontree.ThreadMeta{ID: thread}); err != nil {
				t.Fatal(err)
			}
			if err := kernel.CheckpointProviderRequest(ctx, nil, nil, cache.ProviderRequestRecord{ID: "one", PromptScopeID: thread, ThreadID: thread}); err != nil {
				t.Fatal(err)
			}
		}
		if mixed {
			if err := kernel.CheckpointProviderRequest(ctx, nil, nil, cache.ProviderRequestRecord{ID: "two", PromptScopeID: "omega", ThreadID: "alpha-extra"}); err != nil {
				t.Fatal(err)
			}
		}
		for _, thread := range []string{"alpha", "omega"} {
			if _, err := kernel.BackendRepo.DeleteRootTreeWithRequest(ctx, thread, "delete", "fingerprint"); err != nil {
				t.Fatal(err)
			}
		}
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
		expected := CleanupInventory{Live: 1, LiveScopes: 1, Deleted: 2, DeletedScopes: 2}
		if _, err := CleanupDeleted(ctx, path, expected, 100); !errors.Is(err, spi.ErrConflict) {
			t.Fatalf("wrong inventory did not abort: %v", err)
		}
		removed, err := CleanupDeleted(ctx, path, expected, 2)
		if mixed {
			if !errors.Is(err, spi.ErrConflict) || removed != 0 {
				t.Fatalf("mixed author: %d %v", removed, err)
			}
		} else {
			if err != nil || removed != 2 {
				t.Fatalf("remove: %d %v", removed, err)
			}
			expected.DeletedScopes = 0
			if removed, err := CleanupDeleted(ctx, path, expected, 0); removed != 0 || err != nil {
				t.Fatalf("replay: %d %v", removed, err)
			}
		}
		if err := WithSource(ctx, path, func(source *Source) error {
			tasks, err := source.ImportOrder(ctx)
			if err != nil {
				return err
			}
			if scopeInventory(tasks) != expected {
				t.Fatalf("unexpected remaining inventory: %+v", scopeInventory(tasks))
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
