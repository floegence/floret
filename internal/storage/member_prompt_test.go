package storage

import (
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestMemberKernelCheckpointsAndScopeDeletion(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "entry.sqlite")
	open := func(fresh bool) (*BackendKernel, *threadsqlite.Backend) {
		t.Helper()
		openCatalog := threadsqlite.OpenMemberCatalog
		if fresh {
			openCatalog = threadsqlite.CreateMemberCatalog
		}
		catalog, err := openCatalog(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		files, err := threadsqlite.NewMemberFiles(path+".d", catalog.Store, 4, 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		backend := &threadsqlite.Backend{ThreadViews: threadsqlite.ThreadViews{Catalog: catalog, Files: files}}
		t.Cleanup(func() {
			if err := backend.Close(); err != nil {
				t.Error(err)
			}
		})
		var kernel *BackendKernel
		err = backend.Update(ctx, func(tx spi.WriteTx) error {
			var ok bool
			if fresh {
				kernel, ok, err = NewFreshBackendKernelInTransaction(ctx, backend, tx, time.Now, true, nil)
			} else {
				kernel, ok, err = NewCurrentBackendKernelInTransaction(ctx, backend, tx, time.Now, true)
			}
			if err == nil && !ok {
				t.Fatal("kernel not initialized")
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return kernel, backend
	}
	kernel, backend := open(true)
	if err := backend.CreateThreadRecords(ctx, storagebridge.ThreadCreation{Thread: "owner"}, func(tx spi.WriteTx) error {
		if member.From(tx) == nil {
			t.Fatal("creation omitted member")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	raw := strings.Repeat("large original <fragment>\n", 3500)
	for _, scope := range []string{"owner", "sibling-scope"} {
		segment := cache.Segment{ID: "seg", ThreadID: "owner", PromptScopeID: scope, Raw: raw, Provider: "test", Model: "first"}
		tools := cache.ToolsetSnapshot{ID: "tools", ThreadID: "owner", PromptScopeID: scope, Provider: "test", Model: "first"}
		request := cache.ProviderRequestRecord{ID: "same", ThreadID: "owner", PromptScopeID: scope, TurnID: "turn", RunID: "run", LogicalRequestID: "logical", AttemptID: "one", AttemptEpoch: 1, Provider: "test", Model: "first", ModelContextEntryID: "earlier", SegmentIDs: []string{"seg"}, TurnSurface: cache.TurnSurfaceSnapshot{SystemPrompt: raw}}
		if err := kernel.CheckpointProviderRequest(ctx, []cache.Segment{segment}, []cache.ToolsetSnapshot{tools}, request); err != nil {
			t.Fatal(err)
		}
		response := cache.ProviderResponseRecord{ThreadID: "owner", PromptScopeID: scope, AttemptID: "one", PressureAnchor: cache.PressureAnchorState{PromptScopeID: scope, Provider: "test", Model: "first", WindowInputTokens: 55, RequestID: "same"}}
		if err := kernel.AppendProviderResponse(ctx, response); err != nil {
			t.Fatal(err)
		}
		request.Model, request.AttemptID, request.AttemptEpoch, request.ModelContextEntryID = "second", "two", 3, "later"
		if err := kernel.AppendProviderRequest(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	kernel, backend = open(false)
	verify := func(scope string) {
		t.Helper()
		segments, err := kernel.Segments(ctx, scope, "test", "first")
		if err != nil || len(segments) != 1 || segments[0].Raw != raw {
			t.Fatalf("segments %s: %v", scope, err)
		}
		latest, found, err := kernel.LatestProviderRequest(ctx, scope)
		if err != nil || !found || latest.AttemptID != "two" || !latest.CreatedAt.IsZero() {
			t.Fatalf("latest: %v", err)
		}
		requests, err := kernel.ProviderRequests(ctx, scope)
		if err != nil || len(requests) != 2 || !reflect.DeepEqual(latest, requests[1]) {
			t.Fatal("request ordering", err)
		}
		first, found, err := kernel.LatestModelContextRequest(ctx, scope, "owner", "earlier")
		if err != nil || !found || first.AttemptID != "one" {
			t.Fatal("model context request", err)
		}
		_, found, err = kernel.LatestModelContextRequest(ctx, scope, "foreign", "earlier")
		if err != nil || found {
			t.Fatal("foreign context request", err)
		}
		facts, err := kernel.ReadTurnRequestFacts(ctx, scope, "turn")
		if err != nil || !facts.ModelDrift || !facts.SurfaceFound || facts.Surface.SystemPrompt != raw || facts.Model != "first" {
			t.Fatal("turn facts", err)
		}
		tools, found, err := kernel.ActiveToolset(ctx, scope, "test", "first")
		if err != nil || !found || tools.ID != "tools" {
			t.Fatal("toolset", err)
		}
		evidence, found, err := kernel.ReadPressureEvidence(ctx, scope, "test", "first")
		if err != nil || !found || evidence.Anchor.WindowInputTokens != 55 || evidence.Request.ID != "same" {
			t.Fatal("pressure", err)
		}
	}
	verify("owner")
	verify("sibling-scope")
	epoch, err := kernel.RecoveredAttemptEpoch(ctx, "owner", "turn", "run", "logical")
	if err != nil || epoch != 3 {
		t.Fatal("attempt epoch", epoch, err)
	}
	countBodies := func() int {
		t.Helper()
		ref, err := backend.Catalog.Resolve(ctx, "owner")
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err := backend.Files.View(ctx, ref, func(tx *sql.Tx) error {
			return tx.QueryRow(`SELECT count(*) FROM history_events WHERE kind='content_created'`).Scan(&count)
		}); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if count := countBodies(); count != 2 {
		t.Fatalf("expected two shared objects: %d", count)
	}
	if err := kernel.DeletePromptScopes(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	verify("sibling-scope")
	if _, found, err := kernel.LatestProviderRequest(ctx, "owner"); err != nil || found || countBodies() != 2 {
		t.Fatal("scope deletion lost shared content or left facts", err)
	}
	if err := kernel.DeletePromptScopes(ctx, "sibling-scope"); err != nil {
		t.Fatal(err)
	}
	if count := countBodies(); count != 0 {
		t.Fatalf("left unowned provider content: %d", count)
	}
	if err := backend.ViewThreadRecords(ctx, storagebridge.ThreadRead{Thread: "owner"}, func(tx spi.ReadTx) error {
		_, err := member.From(tx).PutBody("readonly", 1, []byte("denied"))
		if !errors.Is(err, spi.ErrInvalidArgument) {
			t.Fatal("read allowed write", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
