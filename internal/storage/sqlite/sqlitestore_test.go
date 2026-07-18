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

	"github.com/floegence/floret/internal/provider"
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

func TestSQLiteStoreMigratesSchemaVersion11(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	entry, err := sessiontree.AppendMessage(ctx, store, "thread", "turn-1", session.Message{Role: session.User, Content: "preserved"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutProviderState(ctx, storage.ProviderStateRecord{
		ThreadID:         "thread",
		LeafEntryID:      entry.ID,
		CompatibilityKey: "provider:model:endpoint:route",
		State:            provider.State{Kind: "responses", ID: "response-1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.withImmediate(ctx, func(tx sqlRunner) error {
		if _, err := tx.ExecContext(ctx, `DROP TABLE agent_todo_states`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, schemaVersion11); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_fingerprint'`, schemaFingerprintVersion11)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	version, err := reopened.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("migrated schema version = %q, want %q", version, schemaVersion)
	}
	pathEntries, err := reopened.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pathEntries) != 1 || pathEntries[0].Message.Content != "preserved" {
		t.Fatalf("migrated path = %#v", pathEntries)
	}
	if _, err := reopened.ProviderState(ctx, "thread"); err != nil {
		t.Fatalf("read migrated provider state: %v", err)
	}
	todo, err := reopened.CompareAndSwapAgentTodoState(ctx, sessiontree.AgentTodoState{
		ThreadID: "thread",
		Items:    []sessiontree.AgentTodoItem{{Content: "migrated todo", Status: "pending"}},
	}, 0)
	if err != nil {
		t.Fatalf("write migrated todo state: %v", err)
	}
	if todo.Version != 1 || len(todo.Items) != 1 || todo.Items[0].Content != "migrated todo" {
		t.Fatalf("migrated todo state = %#v", todo)
	}
}

func TestSQLiteStoreRejectsUnknownSchemaVersion(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	if err := store.putMetaValue(ctx, "schema_version", "10"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), `unsupported sqlite store schema version "10"`) {
		t.Fatalf("unknown schema open err = %v", err)
	}
}

func TestSQLiteStoreVersion11MigrationRejectsWrongFingerprintWithoutChanges(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	if err := store.withImmediate(ctx, func(tx sqlRunner) error {
		if _, err := tx.ExecContext(ctx, `DROP TABLE agent_todo_states`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, schemaVersion11); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = 'wrong' WHERE key = 'schema_fingerprint'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), `unsupported sqlite store schema fingerprint "wrong"`) {
		t.Fatalf("wrong fingerprint open err = %v", err)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion11 {
		t.Fatalf("schema version after rejected migration = %q, want %q", version, schemaVersion11)
	}
	var todoTables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'agent_todo_states'`).Scan(&todoTables); err != nil {
		t.Fatal(err)
	}
	if todoTables != 0 {
		t.Fatalf("agent todo table count after rejected migration = %d, want 0", todoTables)
	}
}

func TestSQLiteStoreVersion11MigrationRollsBackOnMetadataFailure(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	if err := store.withImmediate(ctx, func(tx sqlRunner) error {
		if _, err := tx.ExecContext(ctx, `DROP TABLE agent_todo_states`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, schemaVersion11); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_fingerprint'`, schemaFingerprintVersion11); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `CREATE TRIGGER reject_schema_version_update
			BEFORE UPDATE ON schema_meta
			WHEN OLD.key = 'schema_version'
			BEGIN
				SELECT RAISE(ABORT, 'reject schema version update');
			END`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "update sqlite store schema version") {
		t.Fatalf("failed migration open err = %v", err)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion11 {
		t.Fatalf("schema version after rollback = %q, want %q", version, schemaVersion11)
	}
	var todoTables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'agent_todo_states'`).Scan(&todoTables); err != nil {
		t.Fatal(err)
	}
	if todoTables != 0 {
		t.Fatalf("agent todo table count after rollback = %d, want 0", todoTables)
	}
}

func TestSQLiteStoreRejectsUnversionedExistingSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE legacy_threads (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "without canonical schema metadata") {
		t.Fatalf("unversioned schema open err = %v", err)
	}
}

func TestSQLiteStoreRejectsMissingCanonicalSchemaFingerprint(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	if err := store.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM schema_meta WHERE key = 'schema_fingerprint'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "unsupported sqlite store schema fingerprint") {
		t.Fatalf("missing schema fingerprint open err = %v", err)
	}
}

func TestSQLiteStoreProviderStateRoundTripCorruptionForkAndDelete(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	leaf, err := sessiontree.AppendMessage(ctx, store, "thread", "turn-1", session.Message{Role: session.User, Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	want := storage.ProviderStateRecord{
		ThreadID:         "thread",
		LeafEntryID:      leaf.ID,
		CompatibilityKey: "provider-profile:model:endpoint:route",
		State: provider.State{
			Kind:       "responses",
			ID:         "response-state-1",
			Attributes: map[string]string{"cursor": "next"},
		},
		CreatedByRunID:  "run-1",
		CreatedByTurnID: "turn-1",
		UpdatedAt:       time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC),
	}
	if err := store.PutProviderState(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	got, err := store.ProviderState(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if got.ThreadID != want.ThreadID || got.LeafEntryID != want.LeafEntryID || got.CompatibilityKey != want.CompatibilityKey || got.State.ID != want.State.ID || got.State.Attributes["cursor"] != "next" || got.CreatedByRunID != "run-1" || got.CreatedByTurnID != "turn-1" || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("reopened provider state = %#v, want %#v", got, want)
	}
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderState(ctx, "fork"); !errors.Is(err, storage.ErrProviderStateNotFound) {
		t.Fatalf("fork provider state err = %v, want not found", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE provider_states SET state_json = '{' WHERE thread_id = 'thread'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderState(ctx, "thread"); err == nil || !strings.Contains(err.Error(), "decode provider state") {
		t.Fatalf("corrupt provider state err = %v", err)
	}
	if err := store.PutProviderState(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteThreadTreeData(ctx, storage.DeleteThreadTreeDataRequest{RootThreadID: "thread", ThreadIDs: []string{"thread"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderState(ctx, "thread"); !errors.Is(err, storage.ErrProviderStateNotFound) {
		t.Fatalf("deleted provider state err = %v, want not found", err)
	}
}

func TestSQLiteStorePersistsCompletedForkOperationAfterReopen(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	plan := []byte(`{"version":1,"root":{"destination_thread_id":"fork"}}`)
	record, created, err := store.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        "operation",
		RequestFingerprint: "fingerprint",
		State:              storage.ForkOperationPrepared,
		Plan:               plan,
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	finishedAt := now.Add(time.Minute)
	record.State = storage.ForkOperationCompleted
	record.Result = []byte(`{"operation_id":"operation","thread":{"id":"fork"}}`)
	record.UpdatedAt = finishedAt
	record.FinishedAt = finishedAt
	if err := store.UpdateForkOperation(ctx, record); err != nil {
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
	got, err := reopened.ForkOperation(ctx, "operation")
	if err != nil {
		t.Fatal(err)
	}
	if got.OperationID != record.OperationID || got.RequestFingerprint != record.RequestFingerprint || got.State != storage.ForkOperationCompleted || string(got.Plan) != string(record.Plan) || string(got.Result) != string(record.Result) || !got.CreatedAt.Equal(now) || !got.UpdatedAt.Equal(finishedAt) || !got.FinishedAt.Equal(finishedAt) {
		t.Fatalf("reopened fork operation = %#v, want %#v", got, record)
	}
	existing, created, err := reopened.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        "operation",
		RequestFingerprint: "different",
		State:              storage.ForkOperationPrepared,
		Plan:               []byte(`{"different":true}`),
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil || created || existing.RequestFingerprint != "fingerprint" || existing.State != storage.ForkOperationCompleted {
		t.Fatalf("existing fork operation created=%v record=%#v err=%v", created, existing, err)
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
		CompactionGeneration:    2,
		CompactionWindowID:      "window-2",
		FirstKeptEntryID:        kept.ID,
		KeptUserEntryIDs:        []string{old.ID},
		CompactedThroughEntryID: old.ID,
		Summary:                 "summary",
		SummarySchemaVersion:    compaction.SummarySchemaVersion,
		Trigger:                 compaction.TriggerPreRequest,
		Reason:                  compaction.ReasonThreshold,
		Phase:                   compaction.PhaseInstall,
		OperationID:             "op-2",
		RequestID:               "req-2",
		Source:                  "engine",
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
	if entry.CompactionOperationID != "op-2" || entry.CompactionRequestID != "req-2" || entry.CompactionSource != "engine" {
		t.Fatalf("typed compaction identity = %#v", entry)
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

func TestSQLiteStorePromptMetadataDeleteThreadTreeDataAndSchemaGuard(t *testing.T) {
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
	if err := store.DeleteThreadTreeData(ctx, storage.DeleteThreadTreeDataRequest{RootThreadID: "thread", ThreadIDs: []string{"thread"}, PromptScopeIDs: []string{"thread"}}); err != nil {
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
	if err := store.DeleteThreadTreeData(ctx, storage.DeleteThreadTreeDataRequest{RootThreadID: "thread", ThreadIDs: []string{"thread"}}); !errors.Is(err, sessiontree.ErrThreadNotFound) {
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

func TestSQLiteStoreDeleteThreadTreeDataDeletesNestedTree(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	seedSQLiteThreadTreeData(t, ctx, store, "parent", "")
	seedSQLiteThreadTreeData(t, ctx, store, "child", "parent")
	seedSQLiteThreadTreeData(t, ctx, store, "grandchild", "child")
	seedSQLiteThreadTreeData(t, ctx, store, "survivor", "")

	err := store.DeleteThreadTreeData(ctx, storage.DeleteThreadTreeDataRequest{
		RootThreadID:   "parent",
		ThreadIDs:      []string{"parent", "child", "grandchild"},
		PromptScopeIDs: []string{"parent", "child", "grandchild"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, threadID := range []string{"parent", "child", "grandchild"} {
		if _, err := store.Thread(ctx, threadID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("Thread(%q) err = %v, want ErrThreadNotFound", threadID, err)
		}
		assertSQLiteThreadTreeDataCount(t, ctx, store, threadID, 0)
	}
	if _, err := store.Thread(ctx, "survivor"); err != nil {
		t.Fatalf("survivor thread err = %v", err)
	}
	assertSQLiteThreadTreeDataCount(t, ctx, store, "survivor", 1)
}

func TestSQLiteStoreDeleteThreadTreeDataRollsBackOnDeleteFailure(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	seedSQLiteThreadTreeData(t, ctx, store, "parent", "")
	seedSQLiteThreadTreeData(t, ctx, store, "child", "parent")
	seedSQLiteThreadTreeData(t, ctx, store, "grandchild", "child")
	if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER fail_child_thread_delete
		BEFORE DELETE ON threads WHEN OLD.id = 'child'
		BEGIN
			SELECT RAISE(ABORT, 'injected thread tree delete failure');
		END`); err != nil {
		t.Fatal(err)
	}

	err := store.DeleteThreadTreeData(ctx, storage.DeleteThreadTreeDataRequest{
		RootThreadID:   "parent",
		ThreadIDs:      []string{"parent", "child", "grandchild"},
		PromptScopeIDs: []string{"parent", "child", "grandchild"},
	})
	if err == nil || !strings.Contains(err.Error(), "injected thread tree delete failure") {
		t.Fatalf("DeleteThreadTreeData err = %v, want injected failure", err)
	}
	for _, threadID := range []string{"parent", "child", "grandchild"} {
		if _, err := store.Thread(ctx, threadID); err != nil {
			t.Fatalf("Thread(%q) after rollback err = %v", threadID, err)
		}
		assertSQLiteThreadTreeDataCount(t, ctx, store, threadID, 1)
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

func seedSQLiteThreadTreeData(t *testing.T, ctx context.Context, store *Store, threadID, parentThreadID string) {
	t.Helper()
	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID, ParentThreadID: parentThreadID, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, threadID, "turn-1", session.Message{Role: session.User, Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: threadID, TurnID: "turn-1", OwnerID: "owner", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutMetadata(ctx, storage.MetadataRecord{Namespace: "thread", ID: threadID, CreatedAt: now, UpdatedAt: now, Data: []byte(`{"title":"thread"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSegment(ctx, cache.Segment{ID: "segment-" + threadID, PromptScopeID: threadID, ThreadID: threadID, Provider: "openai", Model: "model", Sequence: 1, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendToolset(ctx, cache.ToolsetSnapshot{ID: "toolset-" + threadID, PromptScopeID: threadID, ThreadID: threadID, Provider: "openai", Model: "model", Epoch: 1, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendProviderRequest(ctx, cache.ProviderRequestRecord{ID: "request-" + threadID, PromptScopeID: threadID, RunID: "run-1", ThreadID: threadID, TurnID: "turn-1", Step: 1, Provider: "openai", Model: "model", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendProviderResponse(ctx, cache.ProviderResponseRecord{RequestID: "request-" + threadID, PromptScopeID: threadID, RunID: "run-1", ThreadID: threadID, TurnID: "turn-1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutToolOutput(ctx, artifact.ToolOutputArtifact{ThreadID: threadID, TurnID: "turn-1", RunID: "run-1", PromptScopeID: threadID, CallID: "call-1", ToolName: "read", Text: "output"}); err != nil {
		t.Fatal(err)
	}
}

func assertSQLiteThreadTreeDataCount(t *testing.T, ctx context.Context, store *Store, threadID string, want int) {
	t.Helper()
	for _, item := range []struct {
		table  string
		column string
	}{
		{table: "entries", column: "thread_id"},
		{table: "active_turn_leases", column: "thread_id"},
		{table: "metadata_records", column: "id"},
		{table: "tool_output_artifacts", column: "thread_id"},
		{table: "prompt_segments", column: "prompt_scope_id"},
		{table: "prompt_toolsets", column: "prompt_scope_id"},
		{table: "prompt_requests", column: "prompt_scope_id"},
		{table: "prompt_responses", column: "prompt_scope_id"},
	} {
		var got int
		query := `SELECT COUNT(*) FROM ` + item.table + ` WHERE ` + item.column + ` = ?`
		if err := store.db.QueryRowContext(ctx, query, threadID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s rows for %q = %d, want %d", item.table, threadID, got, want)
		}
	}
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
