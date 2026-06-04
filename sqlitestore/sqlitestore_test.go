package sqlitestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/compaction"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/storage"
)

func TestSQLiteStorePersistsSessionTreeAndForkAfterReopen(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	first, err := sessiontree.AppendMessage(ctx, store, "thread", "turn-1", session.Message{Role: session.User, Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := sessiontree.AppendMessage(ctx, store, "thread", "turn-1", session.Message{Role: session.Assistant, Content: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Raw == "" || first.RawHash == "" || second.Raw == "" || second.RawHash == "" {
		t.Fatalf("raw ledger fields missing: first=%#v second=%#v", first, second)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	meta, err := store.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != second.ID {
		t.Fatalf("leaf = %q, want %q", meta.LeafID, second.ID)
	}
	pathEntries, err := store.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := sessiontree.BuildContext(pathEntries, sessiontree.ContextOptions{}); len(got) != 2 || got[0].Content != "first" || got[1].Content != "second" {
		t.Fatalf("reopened context = %#v", got)
	}
	if err := store.MoveLeaf(ctx, "thread", first.ID); err != nil {
		t.Fatal(err)
	}
	branch, err := sessiontree.AppendMessage(ctx, store, "thread", "turn-2", session.Message{Role: session.Assistant, Content: "branch"})
	if err != nil {
		t.Fatal(err)
	}
	active, err := store.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if active[len(active)-1].ID != branch.ID || pathContainsID(active, second.ID) {
		t.Fatalf("move leaf should preserve old branch while activating new one: %#v", active)
	}
	oldBranch, err := store.Path(ctx, "thread", second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldBranch[len(oldBranch)-1].ID != second.ID {
		t.Fatalf("old branch should stay readable: %#v", oldBranch)
	}
	fork, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", EntryID: branch.ID, NewThreadID: "fork", Now: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	forkPath, err := store.Path(ctx, fork.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := sessiontree.BuildContext(forkPath, sessiontree.ContextOptions{}); len(got) != 2 || got[0].Content != "first" || got[1].Content != "branch" {
		t.Fatalf("fork context = %#v", got)
	}
	if fork.ForkedFromThreadID != "thread" || fork.ForkedFromEntryID != branch.ID {
		t.Fatalf("fork metadata = %#v", fork)
	}
}

func TestSQLiteStoreRejectsInvalidParentDetectsPathDamageAndSerializesConcurrentAppends(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	root, err := store.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ID: "root"}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("duplicate id err = %v", err)
	}
	meta, err := store.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != root.ID {
		t.Fatalf("duplicate append changed leaf: %#v", meta)
	}
	if _, err := store.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ParentID: "missing"}); !errors.Is(err, sessiontree.ErrInvalidParent) {
		t.Fatalf("invalid parent err = %v", err)
	}
	if _, err := store.Path(ctx, "missing", ""); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("missing thread path err = %v", err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "broken", LeafID: "missing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Path(ctx, "broken", ""); !errors.Is(err, sessiontree.ErrEntryNotFound) {
		t.Fatalf("missing leaf path err = %v", err)
	}
	child, err := store.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE entries SET parent_id = ? WHERE thread_id = ? AND id = ?`, child.ID, "thread", root.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Path(ctx, "thread", child.ID); !errors.Is(err, sessiontree.ErrInvalidParent) {
		t.Fatalf("cycle path err = %v", err)
	}

	store, _ = openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "concurrent"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Append(ctx, sessiontree.Entry{ThreadID: "concurrent", Type: sessiontree.EntryCustom, TurnID: fmt.Sprintf("turn-%02d", i)}, sessiontree.AppendOptions{})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := store.Entries(ctx, "concurrent")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 24 {
		t.Fatalf("entries = %d, want 24", len(entries))
	}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		if _, ok := seen[entry.ID]; ok {
			t.Fatalf("duplicate generated id: %#v", entries)
		}
		seen[entry.ID] = struct{}{}
	}
}

func TestSQLiteStoreForkRewritesCompactionEntryReferences(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	old, _ := sessiontree.AppendMessage(ctx, store, "source", "turn-1", session.Message{Role: session.User, Content: "old"})
	kept, _ := sessiontree.AppendMessage(ctx, store, "source", "turn-2", session.Message{Role: session.User, Content: "kept"})
	compacted, err := sessiontree.AppendCompaction(ctx, store, "source", "turn-2", compaction.Result{
		CompactionID:            "compaction-2",
		PreviousCompactionID:    "compaction-1",
		FirstKeptEntryID:        kept.ID,
		CompactedThroughEntryID: old.ID,
		Summary:                 "summary",
		SummarySchemaVersion:    compaction.SummarySchemaVersion,
		Trigger:                 compaction.TriggerPreRequest,
		Reason:                  compaction.ReasonThreshold,
		Phase:                   compaction.PhaseInstall,
	})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "source", EntryID: compacted.ID, NewThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	pathEntries, err := store.Path(ctx, fork.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	idx := slices.IndexFunc(pathEntries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryCompaction
	})
	if idx < 0 {
		t.Fatalf("fork path missing compaction: %#v", pathEntries)
	}
	entry := pathEntries[idx]
	if entry.FirstKeptEntryID == kept.ID || entry.CompactedThroughEntryID == old.ID {
		t.Fatalf("fork should rewrite entry references: %#v", entry)
	}
	if entry.PreviousCompactionID != "compaction-1" {
		t.Fatalf("previous compaction id should stay stable: %#v", entry)
	}
	if got := sessiontree.BuildContext(pathEntries, sessiontree.ContextOptions{}); len(got) != 2 || got[0].Content != "summary" || got[1].Content != "kept" {
		t.Fatalf("fork context = %#v", got)
	}
}

func TestSQLiteStorePromptMetadataDeleteSessionAndSchemaGuard(t *testing.T) {
	ctx := context.Background()
	store, dbPath := openSQLiteStoreForTest(t)
	var mode string
	if err := store.db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if strings.ToLower(mode) != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, "thread", "turn-1", session.Message{Role: session.User, Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 4, 11, 0, 0, 0, time.UTC)
	toolset, _, err := promptcache.EnsureToolset(ctx, store, "turn-1", "thread", "openai", "model", []promptcache.ToolDefinition{{Name: "read"}}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	input := promptcache.BuildInput{
		RunID:        "turn-1",
		SessionID:    "thread",
		Provider:     "openai",
		Model:        "model",
		SystemPrompt: "system",
		History:      []session.Message{{Role: session.User, Content: "hello"}},
		Toolset:      toolset,
		Now:          now,
	}
	firstPlan, _, err := promptcache.BuildPlan(ctx, store, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := promptcache.RecordRequest(ctx, store, "turn-1", "thread", 1, "openai", "model", promptcache.CachePolicy{}, firstPlan); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendProviderResponse(ctx, promptcache.ProviderResponseRecord{RequestID: "turn-1:request:1", ProviderResponseID: "provider-response"}); err != nil {
		t.Fatal(err)
	}
	created := now.Add(-time.Minute)
	if err := store.PutMetadata(ctx, storage.MetadataRecord{Namespace: "ns", ID: "thread", CreatedAt: created, UpdatedAt: now, Data: []byte(`{"title":"Thread"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutMetadata(ctx, storage.MetadataRecord{Namespace: "other", ID: "thread", CreatedAt: created, UpdatedAt: now, Data: []byte(`{"keep":true}`)}); err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Metadata(ctx, "ns", "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !metadata.CreatedAt.Equal(created) || string(metadata.Data) != `{"title":"Thread"}` {
		t.Fatalf("metadata = %#v", metadata)
	}
	list, err := store.ListMetadata(ctx, "ns")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != "thread" {
		t.Fatalf("metadata list = %#v", list)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	active, ok, err := store.ActiveToolset(ctx, "thread", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || active.Fingerprint != toolset.Fingerprint {
		t.Fatalf("active toolset = %#v ok=%v", active, ok)
	}
	secondPlan, _, err := promptcache.BuildPlan(ctx, store, input)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan.PrefixHash != secondPlan.PrefixHash || secondPlan.NewSegments != 0 {
		t.Fatalf("reopened plan should reuse stable raw prefix: first=%#v second=%#v", firstPlan, secondPlan)
	}
	requests, err := store.ProviderRequests(ctx, "turn-1")
	if err != nil {
		t.Fatal(err)
	}
	responses, err := store.ProviderResponses(ctx, "turn-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || len(responses) != 1 {
		t.Fatalf("provider records requests=%#v responses=%#v", requests, responses)
	}
	if err := store.DeleteSession(ctx, storage.DeleteSessionRequest{SessionID: "thread", PromptScopeIDs: []string{"turn-1"}, MetadataNamespaces: []string{"ns"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Thread(ctx, "thread"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("thread after delete err = %v", err)
	}
	if _, err := store.Metadata(ctx, "ns", "thread"); !errors.Is(err, storage.ErrMetadataNotFound) {
		t.Fatalf("metadata after delete err = %v", err)
	}
	if kept, err := store.Metadata(ctx, "other", "thread"); err != nil || string(kept.Data) != `{"keep":true}` {
		t.Fatalf("other namespace metadata should survive delete: kept=%#v err=%v", kept, err)
	}
	if segments, err := store.Segments(ctx, "thread", "openai", "model"); err != nil || len(segments) != 0 {
		t.Fatalf("segments after delete = %#v err=%v", segments, err)
	}
	if requests, err := store.ProviderRequests(ctx, "turn-1"); err != nil || len(requests) != 0 {
		t.Fatalf("requests after delete = %#v err=%v", requests, err)
	}
	if err := store.PutMetaValue(ctx, "schema_version", "999"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dbPath); err == nil || !strings.Contains(err.Error(), "unsupported sqlite store schema version") {
		t.Fatalf("unsupported schema open err = %v", err)
	}
}

func TestSQLiteStoreFailsClosedOnRawEncoderVersionMismatch(t *testing.T) {
	ctx := context.Background()
	store, dbPath := openSQLiteStoreForTest(t)
	if err := store.PutMetaValue(ctx, "raw_encoder_version", "999"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dbPath); err == nil || !strings.Contains(err.Error(), "unsupported sqlite store raw encoder version") {
		t.Fatalf("unsupported raw encoder open err = %v", err)
	}
}

func TestSQLiteStoreImportsLegacyPromptRunAtomically(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	runDir := filepath.Join(root, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	good := promptcache.Segment{ID: "seg", RunID: "run", Provider: "openai", Model: "model", Kind: promptcache.SegmentSystem, Raw: "system", CreatedAt: time.Now().UTC()}
	if err := writeJSONLines(filepath.Join(runDir, "raw_segments.jsonl"), good); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "requests.jsonl"), []byte("{bad json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := openSQLiteStoreForTest(t)
	summary, err := store.ImportPromptCache(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if summary.PromptRuns != 0 || summary.Skipped != 1 {
		t.Fatalf("summary = %#v", summary)
	}
	if segments, err := store.Segments(ctx, "run", "openai", "model"); err != nil || len(segments) != 0 {
		t.Fatalf("malformed run should not partially import segments=%#v err=%v", segments, err)
	}
}

func TestSQLiteStoreLegacyImportsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	treeRoot := filepath.Join(root, "tree")
	fileRepo := sessiontree.NewFileRepo(treeRoot)
	if _, err := fileRepo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, fileRepo, "thread", "turn-1", session.Message{Role: session.User, Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	promptRoot := filepath.Join(root, "prompt")
	fileStore := promptcache.NewFileStore(promptRoot)
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	if err := fileStore.AppendSegment(ctx, promptcache.Segment{ID: "seg", RunID: "thread", Provider: "openai", Model: "model", Kind: promptcache.SegmentSystem, Raw: "system", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := fileStore.AppendProviderResponse(ctx, promptcache.ProviderResponseRecord{RequestID: "turn-1:req:1", ProviderResponseID: "provider-response", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	store, _ := openSQLiteStoreForTest(t)
	firstTree, err := store.ImportSessionTree(ctx, treeRoot)
	if err != nil {
		t.Fatal(err)
	}
	secondTree, err := store.ImportSessionTree(ctx, treeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if firstTree.Threads != 1 || secondTree.Threads != 1 || secondTree.Conflicts != 0 {
		t.Fatalf("tree summaries first=%#v second=%#v", firstTree, secondTree)
	}
	firstPrompt, err := store.ImportPromptCache(ctx, promptRoot)
	if err != nil {
		t.Fatal(err)
	}
	secondPrompt, err := store.ImportPromptCache(ctx, promptRoot)
	if err != nil {
		t.Fatal(err)
	}
	if firstPrompt.PromptRuns != 2 || secondPrompt.PromptRuns != 2 || secondPrompt.Skipped != 0 || secondPrompt.Conflicts != 0 {
		t.Fatalf("prompt summaries first=%#v second=%#v", firstPrompt, secondPrompt)
	}
	segments, err := store.Segments(ctx, "thread", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	responses, err := store.ProviderResponses(ctx, "turn-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || len(responses) != 1 {
		t.Fatalf("duplicate import rows segments=%#v responses=%#v", segments, responses)
	}
}

func TestSQLiteStoreImportsLegacyResponseWithRequestIDRunFallback(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	runDir := filepath.Join(root, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	resp := promptcache.ProviderResponseRecord{RequestID: "run:req:7", ProviderResponseID: "provider-response"}
	if err := writeJSONLines(filepath.Join(runDir, "responses.jsonl"), resp); err != nil {
		t.Fatal(err)
	}
	store, _ := openSQLiteStoreForTest(t)
	summary, err := store.ImportPromptCache(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if summary.PromptRuns != 1 || summary.Skipped != 0 {
		t.Fatalf("summary = %#v", summary)
	}
	responses, err := store.ProviderResponses(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || responses[0].ProviderResponseID != "provider-response" {
		t.Fatalf("responses = %#v", responses)
	}
}

func openSQLiteStoreForTest(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func pathContainsID(entries []sessiontree.Entry, id string) bool {
	return slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.ID == id
	})
}

func writeJSONLines(path string, values ...any) error {
	var lines []string
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		lines = append(lines, string(data))
	}
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600)
}
