package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
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
	titleUpdatedAt := meta.UpdatedAt.Add(time.Minute)
	updatedAt := meta.UpdatedAt
	meta.Title = "Persist title metadata"
	meta.TitleStatus = sessiontree.ThreadTitleReady
	meta.TitleSource = sessiontree.ThreadTitleSourceProvider
	meta.TitleUpdatedAt = titleUpdatedAt
	if err := store.UpdateThread(ctx, meta); err != nil {
		t.Fatal(err)
	}
	meta, err = store.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Title != "Persist title metadata" || meta.TitleStatus != sessiontree.ThreadTitleReady || meta.TitleSource != sessiontree.ThreadTitleSourceProvider || !meta.TitleUpdatedAt.Equal(titleUpdatedAt) || !meta.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("thread title metadata = %#v", meta)
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

func TestSQLiteStorePersistsSubAgentThreadMetadata(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 6, 23, 9, 0, 0, 0, time.UTC)
	child := sessiontree.ThreadMeta{
		ID:              "child",
		ParentThreadID:  "parent",
		ParentTurnID:    "turn-parent",
		TaskName:        "review_api",
		TaskDescription: "Review the runtime API boundary.",
		AgentPath:       "/root/review_api",
		HostProfileRef:  "reviewer",
		ForkMode:        "full_path",
		Closed:          true,
		CreatedAt:       now,
		UpdatedAt:       now.Add(time.Minute),
		Status:          "closed",
	}
	if _, err := store.CreateThread(ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if got.ParentThreadID != child.ParentThreadID ||
		got.ParentTurnID != child.ParentTurnID ||
		got.TaskName != child.TaskName ||
		got.TaskDescription != child.TaskDescription ||
		got.AgentPath != child.AgentPath ||
		got.HostProfileRef != child.HostProfileRef ||
		got.ForkMode != child.ForkMode ||
		!got.Closed ||
		got.Status != child.Status {
		t.Fatalf("subagent metadata = %#v", got)
	}
	listed, err := reopened.ListThreads(ctx, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].TaskDescription != child.TaskDescription || listed[0].AgentPath != child.AgentPath || !listed[0].Closed {
		t.Fatalf("listed threads = %#v", listed)
	}
}

func TestSQLiteStoreMigratesV6SubAgentMetadataColumns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES
	('schema_version', '6'),
	('raw_encoder_version', '1');
CREATE TABLE threads (
	id TEXT PRIMARY KEY,
	leaf_id TEXT NOT NULL DEFAULT '',
	parent_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_entry_id TEXT NOT NULL DEFAULT '',
	archived INTEGER NOT NULL DEFAULT 0,
	title TEXT NOT NULL DEFAULT '',
	title_status TEXT NOT NULL DEFAULT '',
	title_source TEXT NOT NULL DEFAULT '',
	title_updated_at TEXT NOT NULL DEFAULT '',
	title_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	last_viewed_at TEXT NOT NULL DEFAULT ''
);
INSERT INTO threads(id, created_at, updated_at, status) VALUES('legacy', '2026-06-23T09:00:00Z', '2026-06-23T09:00:00Z', 'completed');
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	version, err := store.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %q, want %q", version, schemaVersion)
	}
	meta, err := store.Thread(ctx, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ParentTurnID != "" || meta.TaskName != "" || meta.TaskDescription != "" || meta.AgentPath != "" || meta.HostProfileRef != "" || meta.ForkMode != "" || meta.Closed {
		t.Fatalf("legacy subagent defaults = %#v", meta)
	}
}

func TestSQLiteStoreMigratesV7ForkModeColumn(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES
	('schema_version', '7'),
	('raw_encoder_version', '1');
CREATE TABLE threads (
	id TEXT PRIMARY KEY,
	leaf_id TEXT NOT NULL DEFAULT '',
	parent_thread_id TEXT NOT NULL DEFAULT '',
	parent_turn_id TEXT NOT NULL DEFAULT '',
	forked_from_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_entry_id TEXT NOT NULL DEFAULT '',
	task_name TEXT NOT NULL DEFAULT '',
	agent_path TEXT NOT NULL DEFAULT '',
	host_profile_ref TEXT NOT NULL DEFAULT '',
	closed INTEGER NOT NULL DEFAULT 0,
	archived INTEGER NOT NULL DEFAULT 0,
	title TEXT NOT NULL DEFAULT '',
	title_status TEXT NOT NULL DEFAULT '',
	title_source TEXT NOT NULL DEFAULT '',
	title_updated_at TEXT NOT NULL DEFAULT '',
	title_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	last_viewed_at TEXT NOT NULL DEFAULT ''
);
INSERT INTO threads(id, parent_thread_id, task_name, agent_path, created_at, updated_at)
VALUES('child', 'parent', 'worker', '/root/worker', '2026-06-28T09:00:00Z', '2026-06-28T09:00:00Z');
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	version, err := store.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %q, want %q", version, schemaVersion)
	}
	meta, err := store.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ForkMode != "" {
		t.Fatalf("legacy fork mode default = %q, want empty", meta.ForkMode)
	}
	if meta.TaskDescription != "" {
		t.Fatalf("legacy task description default = %q, want empty", meta.TaskDescription)
	}
}

func TestSQLiteStoreMigratesV8TaskDescriptionColumn(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES
	('schema_version', '8'),
	('raw_encoder_version', '1');
CREATE TABLE threads (
	id TEXT PRIMARY KEY,
	leaf_id TEXT NOT NULL DEFAULT '',
	parent_thread_id TEXT NOT NULL DEFAULT '',
	parent_turn_id TEXT NOT NULL DEFAULT '',
	forked_from_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_entry_id TEXT NOT NULL DEFAULT '',
	task_name TEXT NOT NULL DEFAULT '',
	agent_path TEXT NOT NULL DEFAULT '',
	host_profile_ref TEXT NOT NULL DEFAULT '',
	closed INTEGER NOT NULL DEFAULT 0,
	archived INTEGER NOT NULL DEFAULT 0,
	title TEXT NOT NULL DEFAULT '',
	title_status TEXT NOT NULL DEFAULT '',
	title_source TEXT NOT NULL DEFAULT '',
	title_updated_at TEXT NOT NULL DEFAULT '',
	title_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	last_viewed_at TEXT NOT NULL DEFAULT '',
	fork_mode TEXT NOT NULL DEFAULT ''
);
INSERT INTO threads(id, parent_thread_id, task_name, fork_mode, created_at, updated_at)
VALUES('child', 'parent', 'worker', 'none', '2026-06-30T09:00:00Z', '2026-06-30T09:00:00Z');
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	version, err := store.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %q, want %q", version, schemaVersion)
	}
	meta, err := store.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ForkMode != "none" {
		t.Fatalf("fork mode = %q, want none", meta.ForkMode)
	}
	if meta.TaskDescription != "" {
		t.Fatalf("legacy task description default = %q, want empty", meta.TaskDescription)
	}
}

func TestSQLiteStoreMigratesV3PromptCacheScopeColumns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES
	('schema_version', '3'),
	('raw_encoder_version', '1');
CREATE TABLE threads (
	id TEXT PRIMARY KEY,
	leaf_id TEXT NOT NULL DEFAULT '',
	parent_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_entry_id TEXT NOT NULL DEFAULT '',
	archived INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
INSERT INTO threads(id, created_at, updated_at) VALUES('thread', '2026-06-05T01:00:00Z', '2026-06-05T01:00:00Z');
CREATE TABLE entries (
	thread_id TEXT NOT NULL,
	id TEXT NOT NULL,
	ordinal INTEGER NOT NULL,
	parent_id TEXT NOT NULL DEFAULT '',
	type TEXT NOT NULL,
	turn_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	message_json TEXT NOT NULL DEFAULT '{}',
	raw TEXT NOT NULL DEFAULT '',
	raw_hash TEXT NOT NULL DEFAULT '',
	raw_encoder_version INTEGER NOT NULL DEFAULT 1,
	turn_status TEXT NOT NULL DEFAULT '',
	provider TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	compaction_id TEXT NOT NULL DEFAULT '',
	previous_compaction_id TEXT NOT NULL DEFAULT '',
	compacted_through_entry_id TEXT NOT NULL DEFAULT '',
	summary_schema_version TEXT NOT NULL DEFAULT '',
	compaction_generation INTEGER NOT NULL DEFAULT 0,
	compaction_window_id TEXT NOT NULL DEFAULT '',
	first_kept_entry_id TEXT NOT NULL DEFAULT '',
	summary TEXT NOT NULL DEFAULT '',
	compaction_trigger TEXT NOT NULL DEFAULT '',
	compaction_reason TEXT NOT NULL DEFAULT '',
	compaction_phase TEXT NOT NULL DEFAULT '',
	tokens_before INTEGER NOT NULL DEFAULT 0,
	tokens_after_estimate INTEGER NOT NULL DEFAULT 0,
	context_usage_before_json TEXT NOT NULL DEFAULT '{}',
	context_usage_after_json TEXT NOT NULL DEFAULT '{}',
	error TEXT NOT NULL DEFAULT '',
	metadata_json TEXT NOT NULL DEFAULT '{}',
	PRIMARY KEY (thread_id, id),
	UNIQUE (thread_id, ordinal),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
CREATE TABLE active_turn_leases (
	thread_id TEXT PRIMARY KEY,
	turn_id TEXT NOT NULL DEFAULT '',
	owner_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
CREATE TABLE prompt_segments (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	sequence INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);
CREATE INDEX prompt_segments_lookup_idx ON prompt_segments(run_id, provider, model, rowid);
CREATE TABLE prompt_toolsets (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	epoch INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);
CREATE INDEX prompt_toolsets_lookup_idx ON prompt_toolsets(run_id, provider, model, rowid);
CREATE TABLE prompt_requests (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);
CREATE INDEX prompt_requests_run_idx ON prompt_requests(run_id, rowid);
CREATE TABLE prompt_responses (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	request_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);
CREATE INDEX prompt_responses_run_idx ON prompt_responses(run_id, rowid);
CREATE TABLE metadata_records (
	namespace TEXT NOT NULL,
	id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	data_json TEXT NOT NULL,
	PRIMARY KEY(namespace, id)
);
INSERT INTO prompt_segments(id, run_id, provider, model, sequence, created_at, data_json)
VALUES('seg-1', 'thread', 'openai', 'model', 1, '2026-06-05T01:00:00Z', '{"id":"seg-1","prompt_scope_id":"thread","provider":"openai","model":"model","kind":"system","raw":"system","created_at":"2026-06-05T01:00:00Z"}');
INSERT INTO prompt_toolsets(id, run_id, provider, model, epoch, created_at, data_json)
VALUES('toolset-1', 'thread', 'openai', 'model', 1, '2026-06-05T01:00:00Z', '{"id":"toolset-1","prompt_scope_id":"thread","provider":"openai","model":"model","epoch":1,"created_at":"2026-06-05T01:00:00Z"}');
INSERT INTO prompt_requests(id, run_id, provider, model, created_at, data_json)
VALUES('req-1', 'thread', 'openai', 'model', '2026-06-05T01:00:00Z', '{"id":"req-1","prompt_scope_id":"thread","run_id":"turn-1","provider":"openai","model":"model","created_at":"2026-06-05T01:00:00Z"}');
INSERT INTO prompt_responses(request_id, run_id, created_at, data_json)
VALUES('req-1', 'thread', '2026-06-05T01:00:00Z', '{"request_id":"req-1","prompt_scope_id":"thread","run_id":"turn-1","created_at":"2026-06-05T01:00:00Z"}');
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	version, err := store.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %q, want %q", version, schemaVersion)
	}
	for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
		if ok, err := columnExists(ctx, store.db, table, "prompt_scope_id"); err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Fatalf("%s prompt_scope_id column missing after migration", table)
		}
		if ok, err := columnExists(ctx, store.db, table, "run_id"); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatalf("%s still has legacy run_id column", table)
		}
	}
	meta, err := store.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Title != "" || meta.Status != "" || meta.ParentTurnID != "" || meta.TaskName != "" || meta.AgentPath != "" || meta.HostProfileRef != "" || meta.Closed {
		t.Fatalf("legacy thread defaults = %#v", meta)
	}
	segments, err := store.Segments(ctx, "thread", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || segments[0].ID != "seg-1" {
		t.Fatalf("segments after migration = %#v", segments)
	}
	toolset, ok, err := store.ActiveToolset(ctx, "thread", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || toolset.ID != "toolset-1" {
		t.Fatalf("toolset after migration = %#v ok=%v", toolset, ok)
	}
	requests, err := store.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].ID != "req-1" {
		t.Fatalf("requests after migration = %#v", requests)
	}
	responses, err := store.ProviderResponses(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || responses[0].RequestID != "req-1" {
		t.Fatalf("responses after migration = %#v", responses)
	}
}

func TestSQLiteStoreAllowsDuplicateSubAgentPathWithDistinctThreadIDs(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Date(2026, 6, 23, 9, 0, 0, 0, time.UTC)
	first := sessiontree.ThreadMeta{
		ID:             "child-a",
		ParentThreadID: "parent",
		TaskName:       "review",
		AgentPath:      "/root/review",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if _, err := store.CreateThread(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{
		ID:             "child-b",
		ParentThreadID: "parent",
		TaskName:       "review",
		AgentPath:      "/root/review",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("duplicate subagent path should be allowed: %v", err)
	}
	listed, err := store.ListThreads(ctx, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, meta := range listed {
		if meta.ParentThreadID == "parent" && meta.AgentPath == "/root/review" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("duplicate subagent paths count = %d in %#v", count, listed)
	}
}

func TestSQLiteStoreRejectsDuplicateThreadAndForkIDs(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); !errors.Is(err, sessiontree.ErrThreadExists) {
		t.Fatalf("duplicate create err = %v, want ErrThreadExists", err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, "source", "turn-1", session.Message{Role: session.User, Content: "seed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "existing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "source", NewThreadID: "existing"}); !errors.Is(err, sessiontree.ErrThreadExists) {
		t.Fatalf("duplicate fork err = %v, want ErrThreadExists", err)
	}
	entries, err := store.Entries(ctx, "existing")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("duplicate fork polluted existing thread: %#v", entries)
	}
	source, err := store.Thread(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	if source.LeafID == "" {
		t.Fatalf("duplicate create overwrote source leaf: %#v", source)
	}
}

func TestSQLiteStoreListThreadsUsesStableCreatedAtOrder(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	older := time.Date(2026, 6, 5, 9, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	sameNewest := newer.Add(time.Hour)
	for _, meta := range []sessiontree.ThreadMeta{
		{ID: "older", CreatedAt: older},
		{ID: "beta", CreatedAt: sameNewest},
		{ID: "alpha", CreatedAt: sameNewest},
		{ID: "newer", CreatedAt: newer},
		{ID: "archived", CreatedAt: sameNewest.Add(time.Hour), Archived: true},
	} {
		if _, err := store.CreateThread(ctx, meta); err != nil {
			t.Fatalf("CreateThread(%s): %v", meta.ID, err)
		}
	}
	updatedOlder, err := store.Thread(ctx, "older")
	if err != nil {
		t.Fatal(err)
	}
	updatedOlder.UpdatedAt = sameNewest.Add(24 * time.Hour)
	if err := store.UpdateThread(ctx, updatedOlder); err != nil {
		t.Fatal(err)
	}

	firstPage, err := store.ListThreads(ctx, sessiontree.ListThreadsOptions{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := sqliteThreadIDs(firstPage); !slices.Equal(got, []string{"alpha", "beta"}) {
		t.Fatalf("first page ids=%v, want stable created_at order", got)
	}
	secondPage, err := store.ListThreads(ctx, sessiontree.ListThreadsOptions{
		AfterCreatedAt: firstPage[len(firstPage)-1].CreatedAt,
		AfterID:        firstPage[len(firstPage)-1].ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sqliteThreadIDs(secondPage); !slices.Equal(got, []string{"newer", "older"}) {
		t.Fatalf("second page ids=%v, want cursor after created_at/id", got)
	}
	withArchived, err := store.ListThreads(ctx, sessiontree.ListThreadsOptions{IncludeArchived: true, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := sqliteThreadIDs(withArchived); !slices.Equal(got, []string{"archived"}) {
		t.Fatalf("archived ids=%v, want archived included at created_at position", got)
	}
}

func TestSQLiteStoreTurnLeaseSerializesSameThreadAcrossStores(t *testing.T) {
	ctx := context.Background()
	first, path := openSQLiteStoreForTest(t)
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	lease := sessiontree.TurnLease{
		ThreadID:  "thread",
		TurnID:    "turn-1",
		OwnerID:   "owner-1",
		CreatedAt: time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC),
	}
	if err := first.AcquireTurnLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err := second.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-2", OwnerID: "owner-2"}); !errors.Is(err, sessiontree.ErrActiveTurn) {
		t.Fatalf("second acquire err = %v, want ErrActiveTurn", err)
	}
	active, ok, err := second.ActiveTurnLease(ctx, "thread")
	if err != nil || !ok {
		t.Fatalf("active lease ok=%v err=%v", ok, err)
	}
	if active.TurnID != lease.TurnID || active.OwnerID != lease.OwnerID || !active.CreatedAt.Equal(lease.CreatedAt) {
		t.Fatalf("active lease = %#v", active)
	}
	if err := second.ReleaseTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-1", OwnerID: "wrong-owner"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := first.ActiveTurnLease(ctx, "thread"); err != nil || !ok {
		t.Fatalf("wrong owner release cleared lease: ok=%v err=%v", ok, err)
	}
	if _, ok, err := first.ClearExpiredTurnLease(ctx, "thread", lease.CreatedAt.Add(-time.Second)); err != nil || ok {
		t.Fatalf("fresh lease should not clear: ok=%v err=%v", ok, err)
	}
	cleared, ok, err := second.ClearExpiredTurnLease(ctx, "thread", lease.CreatedAt.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("expired lease clear ok=%v err=%v", ok, err)
	}
	if cleared.OwnerID != lease.OwnerID || cleared.TurnID != lease.TurnID {
		t.Fatalf("cleared lease = %#v", cleared)
	}
	if _, ok, err := first.ActiveTurnLease(ctx, "thread"); err != nil || ok {
		t.Fatalf("lease should be cleared: ok=%v err=%v", ok, err)
	}
	if err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-3", OwnerID: "owner-3"}); err != nil {
		t.Fatalf("acquire after clear: %v", err)
	}
	if err := first.ReleaseTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-3", OwnerID: "owner-3"}); err != nil {
		t.Fatal(err)
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
		KeptUserEntryIDs:        []string{old.ID},
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
	if len(entry.KeptUserEntryIDs) != 1 || entry.KeptUserEntryIDs[0] == old.ID {
		t.Fatalf("fork should rewrite kept user entry references: %#v", entry)
	}
	if entry.PreviousCompactionID != "compaction-1" {
		t.Fatalf("previous compaction id should stay stable: %#v", entry)
	}
	if got := sessiontree.BuildContext(pathEntries, sessiontree.ContextOptions{}); len(got) != 2 ||
		got[0].Role != session.User ||
		got[0].Kind != session.MessageKindCompactionSummary ||
		!strings.Contains(got[0].Content, "old") ||
		!strings.Contains(got[0].Content, "summary") ||
		got[1].Content != "kept" {
		t.Fatalf("fork context = %#v", got)
	}
}

func TestSQLiteStorePromptMetadataDeleteThreadDataAndSchemaGuard(t *testing.T) {
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
	toolset, _, err := cache.EnsureToolset(ctx, store, "thread", "turn-1", "thread", "turn-1", "openai", "model", []cache.ToolDefinition{{Name: "read"}}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	input := cache.BuildInput{
		PromptScopeID: "thread",
		RunID:         "turn-1",
		ThreadID:      "thread",
		TurnID:        "turn-1",
		Provider:      "openai",
		Model:         "model",
		SystemPrompt:  "system",
		History:       []session.Message{{Role: session.User, Content: "hello"}},
		Toolset:       toolset,
		Now:           now,
	}
	firstPlan, _, err := cache.BuildPlan(ctx, store, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.RecordRequest(ctx, store, cache.PromptScopeRef{PromptScopeID: "thread", RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1"}, 1, "openai", "model", cache.CachePolicy{}, firstPlan); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendProviderResponse(ctx, cache.ProviderResponseRecord{RequestID: "turn-1:req:1", PromptScopeID: "thread", RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", ProviderResponseID: "provider-response", InputTokens: 100, OutputTokens: 20, ReasoningTokens: 3, CacheReadTokens: 10, CacheWriteTokens: 5, TotalTokens: 138, UsageSource: "native"}); err != nil {
		t.Fatal(err)
	}
	created := now.Add(-time.Minute)
	if err := store.PutMetadata(ctx, storage.MetadataRecord{Namespace: "ns", ID: "thread", CreatedAt: created, UpdatedAt: now, Data: []byte(`{"title":"Thread"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutMetadata(ctx, storage.MetadataRecord{Namespace: "other", ID: "thread", CreatedAt: created, UpdatedAt: now, Data: []byte(`{"keep":true}`)}); err != nil {
		t.Fatal(err)
	}
	artifactRef, err := store.PutToolOutput(ctx, artifact.ToolOutputArtifact{
		ThreadID:      "thread",
		TurnID:        "turn-1",
		RunID:         "turn-1",
		PromptScopeID: "thread",
		Step:          1,
		CallID:        "call-1",
		ToolName:      "read",
		Text:          "full durable tool output",
	})
	if err != nil {
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
	secondPlan, _, err := cache.BuildPlan(ctx, store, input)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan.PrefixHash != secondPlan.PrefixHash || secondPlan.NewSegments != 0 {
		t.Fatalf("reopened plan should reuse stable raw prefix: first=%#v second=%#v", firstPlan, secondPlan)
	}
	requests, err := store.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	responses, err := store.ProviderResponses(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || len(responses) != 1 {
		t.Fatalf("provider records requests=%#v responses=%#v", requests, responses)
	}
	if responses[0].InputTokens != 100 || responses[0].OutputTokens != 20 || responses[0].ReasoningTokens != 3 || responses[0].CacheReadTokens != 10 || responses[0].CacheWriteTokens != 5 || responses[0].TotalTokens != 138 || responses[0].UsageSource != "native" {
		t.Fatalf("provider response usage did not round trip: %#v", responses[0])
	}
	if text, ok, err := store.artifactText(ctx, artifactRef.ID); err != nil || !ok || text != "full durable tool output" {
		t.Fatalf("reopened artifact text=%q ok=%v err=%v", text, ok, err)
	}
	if err := store.DeleteThreadData(ctx, storage.DeleteThreadDataRequest{ThreadID: "thread", PromptScopeIDs: []string{"thread"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Thread(ctx, "thread"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("thread after delete err = %v", err)
	}
	if _, err := store.Metadata(ctx, "ns", "thread"); !errors.Is(err, storage.ErrMetadataNotFound) {
		t.Fatalf("metadata after delete err = %v", err)
	}
	if _, err := store.Metadata(ctx, "other", "thread"); !errors.Is(err, storage.ErrMetadataNotFound) {
		t.Fatalf("other namespace metadata after delete err = %v", err)
	}
	if segments, err := store.Segments(ctx, "thread", "openai", "model"); err != nil || len(segments) != 0 {
		t.Fatalf("segments after delete = %#v err=%v", segments, err)
	}
	if requests, err := store.ProviderRequests(ctx, "thread"); err != nil || len(requests) != 0 {
		t.Fatalf("requests after delete = %#v err=%v", requests, err)
	}
	if text, ok, err := store.artifactText(ctx, artifactRef.ID); err != nil || ok || text != "" {
		t.Fatalf("artifact after delete text=%q ok=%v err=%v", text, ok, err)
	}
	if err := store.DeleteThreadData(ctx, storage.DeleteThreadDataRequest{ThreadID: "thread"}); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("second delete err = %v, want ErrThreadNotFound", err)
	}
	if err := store.putMetaValue(ctx, "schema_version", "999"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dbPath); err == nil || !strings.Contains(err.Error(), "unsupported sqlite store schema version") {
		t.Fatalf("unsupported schema open err = %v", err)
	}
}

func TestSQLiteStoreLatestPressureAnchorRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	old := cache.PressureAnchorState{
		PromptScopeID:      "thread",
		ThreadID:           "thread",
		Provider:           "openai",
		Model:              "model",
		RequestID:          "turn-1:req:1",
		LastMessageEntryID: "entry-1",
		WindowInputTokens:  100,
		EstimateSource:     "request_estimator_test",
		EstimateMethod:     contextpolicy.EstimateMethodProviderRenderedPayload,
		Confidence:         contextpolicy.EstimateConservative,
		CreatedAt:          now,
	}
	newer := old
	newer.RequestID = "turn-2:req:1"
	newer.LastMessageEntryID = "entry-2"
	newer.WindowInputTokens = 200
	newer.CreatedAt = now.Add(time.Minute)
	other := newer
	other.PromptScopeID = "other-thread"
	other.ThreadID = "other-thread"
	other.RequestID = "turn-other:req:1"
	other.WindowInputTokens = 999
	other.CreatedAt = now.Add(2 * time.Minute)

	for _, resp := range []cache.ProviderResponseRecord{
		{RequestID: "turn-1:req:1", PromptScopeID: "thread", RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", PressureAnchor: old, CreatedAt: old.CreatedAt},
		{RequestID: "turn-other:req:1", PromptScopeID: "other-thread", RunID: "turn-other", ThreadID: "other-thread", TurnID: "turn-other", PressureAnchor: other, CreatedAt: other.CreatedAt},
		{RequestID: "turn-2:req:1", PromptScopeID: "thread", RunID: "turn-2", ThreadID: "thread", TurnID: "turn-2", PressureAnchor: newer, CreatedAt: newer.CreatedAt},
	} {
		if err := store.AppendProviderResponse(ctx, resp); err != nil {
			t.Fatal(err)
		}
	}

	got, ok, err := store.LatestPressureAnchor(ctx, "thread", "openai", "model")
	if err != nil {
		t.Fatal(err)
	}
	if !ok ||
		got.RequestID != "turn-2:req:1" ||
		got.WindowInputTokens != 200 ||
		got.LastMessageEntryID != "entry-2" ||
		got.EstimateSource != "request_estimator_test" ||
		got.EstimateMethod != contextpolicy.EstimateMethodProviderRenderedPayload {
		t.Fatalf("latest anchor = %#v ok=%v", got, ok)
	}
	if _, ok, err := store.LatestPressureAnchor(ctx, "thread", "anthropic", "model"); err != nil || ok {
		t.Fatalf("mismatched provider anchor ok=%v err=%v", ok, err)
	}
}

func TestSQLiteStorePromptCacheConcurrentThreadsAreIsolated(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	run := func(runID, threadID, toolName, content string) {
		defer wg.Done()
		toolset, _, err := cache.EnsureCurrentToolset(ctx, store, threadID, runID, threadID, runID, "openai", "model", []cache.ToolDefinition{{Name: toolName}}, nil, time.Time{})
		if err != nil {
			errs <- err
			return
		}
		plan, _, err := cache.BuildPlan(ctx, store, cache.BuildInput{
			PromptScopeID: threadID,
			RunID:         runID,
			ThreadID:      threadID,
			TurnID:        runID,
			Provider:      "openai",
			Model:         "model",
			Toolset:       toolset,
			History:       []session.Message{{Role: session.User, Content: content}},
		})
		if err != nil {
			errs <- err
			return
		}
		if _, err := cache.RecordRequest(ctx, store, cache.PromptScopeRef{PromptScopeID: threadID, RunID: runID, ThreadID: threadID, TurnID: runID}, 1, "openai", "model", cache.CachePolicy{}, plan); err != nil {
			errs <- err
			return
		}
	}
	wg.Add(2)
	go run("turn-a", "thread-a", "read_a", "message a")
	go run("turn-b", "thread-b", "read_b", "message b")
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct {
		runID    string
		threadID string
		toolName string
		content  string
	}{
		{"turn-a", "thread-a", "read_a", "message a"},
		{"turn-b", "thread-b", "read_b", "message b"},
	} {
		requests, err := store.ProviderRequests(ctx, item.threadID)
		if err != nil {
			t.Fatal(err)
		}
		if len(requests) != 1 || requests[0].ThreadID != item.threadID {
			t.Fatalf("%s requests = %#v", item.runID, requests)
		}
		segments, err := store.Segments(ctx, item.threadID, "openai", "model")
		if err != nil {
			t.Fatal(err)
		}
		var sawTool, sawMessage bool
		for _, seg := range segments {
			if strings.Contains(seg.Raw, item.toolName) {
				sawTool = true
			}
			if seg.Message.Content == item.content {
				sawMessage = true
			}
			if strings.Contains(seg.Raw, "read_a") && item.toolName != "read_a" || strings.Contains(seg.Raw, "read_b") && item.toolName != "read_b" {
				t.Fatalf("%s saw cross-thread tool segment: %#v", item.threadID, seg)
			}
			if seg.Message.Content == "message a" && item.content != "message a" || seg.Message.Content == "message b" && item.content != "message b" {
				t.Fatalf("%s saw cross-thread message segment: %#v", item.threadID, seg)
			}
		}
		if !sawTool || !sawMessage {
			t.Fatalf("%s missing own tool/message segments: %#v", item.threadID, segments)
		}
	}
}

func TestSQLiteStoreFailsClosedOnRawEncoderVersionMismatch(t *testing.T) {
	ctx := context.Background()
	store, dbPath := openSQLiteStoreForTest(t)
	if err := store.putMetaValue(ctx, "raw_encoder_version", "999"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dbPath); err == nil || !strings.Contains(err.Error(), "unsupported sqlite store raw encoder version") {
		t.Fatalf("unsupported raw encoder open err = %v", err)
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

func sqliteThreadIDs(threads []sessiontree.ThreadMeta) []string {
	out := make([]string, 0, len(threads))
	for _, thread := range threads {
		out = append(out, thread.ID)
	}
	return out
}
