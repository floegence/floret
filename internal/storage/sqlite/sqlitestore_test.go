package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
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
	if fork.ParentThreadID != "" || fork.ForkedFromThreadID != "thread" || fork.ForkedFromEntryID != branch.ID {
		t.Fatalf("fork metadata = %#v", fork)
	}
}

func TestSQLiteStorePersistsSubAgentThreadMetadata(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 6, 23, 9, 0, 0, 0, time.UTC)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	child := sessiontree.ThreadMeta{
		ID:              "child",
		ParentThreadID:  "parent",
		ParentTurnID:    "turn-parent",
		TaskName:        "review_api",
		TaskDescription: "Review the runtime API boundary.",
		AgentPath:       "/root/review_api",
		HostProfileRef:  "reviewer",
		ForkMode:        "full_path",
		Lifecycle:       sessiontree.ThreadLifecycleClosed,
		CreatedAt:       now,
		UpdatedAt:       now.Add(time.Minute),
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
		got.Lifecycle != sessiontree.ThreadLifecycleClosed {
		t.Fatalf("subagent metadata = %#v", got)
	}
	listed, err := reopened.ListThreads(ctx, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	var listedChild sessiontree.ThreadMeta
	for _, meta := range listed {
		if meta.ID == child.ID {
			listedChild = meta
		}
	}
	if len(listed) != 2 || listedChild.TaskDescription != child.TaskDescription || listedChild.AgentPath != child.AgentPath || listedChild.Lifecycle != sessiontree.ThreadLifecycleClosed {
		t.Fatalf("listed threads = %#v", listed)
	}
}

func TestSQLiteStoreMigratesExactEmptySchemaVersion13(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v13.db")
	createSchemaVersion13StoreForTest(t, path, nil)

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	version, err := store.SchemaVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %q, want %q", version, schemaVersion)
	}
	var fingerprint string
	if err := store.db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_fingerprint'`).Scan(&fingerprint); err != nil {
		t.Fatal(err)
	}
	if fingerprint != schemaFingerprintVersion15 {
		t.Fatalf("schema fingerprint = %q, want %q", fingerprint, schemaFingerprintVersion15)
	}
}

func TestSQLiteStoreMigratesNonEmptySchemaVersion14CanonicalTurnIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v14-non-empty.db")
	createSchemaVersion14StoreForTest(t, path, func(db *sql.DB) {
		seedSchemaVersion14CanonicalTurn(t, db, false)
	})

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open migrated v14 store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	version, err := store.SchemaVersion(context.Background())
	if err != nil || version != schemaVersion {
		t.Fatalf("schema version=%q err=%v, want %q", version, err, schemaVersion)
	}
	entries, found, err := store.CanonicalTurnEntries(context.Background(), "thread", "turn", "run")
	if err != nil || !found || len(entries) != 1 || entries[0].ID != "started" {
		t.Fatalf("migrated canonical turn entries=%#v found=%v err=%v", entries, found, err)
	}
	assertSQLiteIndexExists(t, store.db, "entries_turn_ordinal_idx")
	assertSQLiteIndexExists(t, store.db, "entries_started_turn_unique_idx")
}

func TestSQLiteStoreRejectsCorruptVersion14DuplicateStartedAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v14-duplicate-started.db")
	createSchemaVersion14StoreForTest(t, path, func(db *sql.DB) {
		seedSchemaVersion14CanonicalTurn(t, db, true)
	})

	if _, err := Open(path); err == nil {
		t.Fatal("Open corrupt v14 store succeeded")
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
	if version != schemaVersion14 {
		t.Fatalf("failed migration version=%q, want unchanged %q", version, schemaVersion14)
	}
	var indexes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN ('entries_turn_ordinal_idx', 'entries_started_turn_unique_idx')`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 0 {
		t.Fatalf("failed migration left %d canonical turn indexes", indexes)
	}
}

func TestSQLiteCanonicalTurnQueryUsesTurnOrdinalIndex(t *testing.T) {
	store, _ := openSQLiteStoreForTest(t)
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN SELECT `+entryColumns+` FROM entries
		WHERE thread_id = ? AND turn_id = ? ORDER BY ordinal`, "thread", "turn")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "entries_turn_ordinal_idx") || strings.Contains(plan, "SCAN entries") {
		t.Fatalf("canonical turn query plan = %q", plan)
	}
}

func createSchemaVersion14StoreForTest(t *testing.T, path string, seed func(*sql.DB)) {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaVersion14SQL); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`,
		schemaVersion14, rawEncoderVersion, schemaFingerprintVersion14); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := persistLeasePolicy(context.Background(), db, sessiontree.DefaultLeasePolicy); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if seed != nil {
		seed(db)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func seedSchemaVersion14CanonicalTurn(t *testing.T, db *sql.DB, duplicate bool) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at) VALUES('thread', 'started', '2026-07-20T00:00:00Z', '2026-07-20T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	started := sessiontree.PrepareEntry(sessiontree.Entry{
		ThreadID: "thread", ID: "started", Type: sessiontree.EntryTurnMarker, TurnID: "turn",
		TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run"},
	})
	if _, err := db.Exec(`INSERT INTO entries(thread_id, id, ordinal, type, turn_id, created_at, raw, raw_hash, turn_status, metadata_json)
		VALUES('thread', 'started', 1, 'turn_marker', 'turn', '2026-07-20T00:00:00Z', ?, ?, 'started', '{"run_id":"run"}')`, started.Raw, started.RawHash); err != nil {
		t.Fatal(err)
	}
	if duplicate {
		other := sessiontree.PrepareEntry(sessiontree.Entry{
			ThreadID: "thread", ID: "duplicate", ParentID: "started", Type: sessiontree.EntryTurnMarker, TurnID: "turn",
			TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "other"},
		})
		if _, err := db.Exec(`INSERT INTO entries(thread_id, id, ordinal, parent_id, type, turn_id, created_at, raw, raw_hash, turn_status, metadata_json)
			VALUES('thread', 'duplicate', 2, 'started', 'turn_marker', 'turn', '2026-07-20T00:00:01Z', ?, ?, 'started', '{"run_id":"other"}')`, other.Raw, other.RawHash); err != nil {
			t.Fatal(err)
		}
	}
}

func assertSQLiteIndexExists(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, name).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("index %q count=%d, want 1", name, count)
	}
}

func TestSQLiteStoreConcurrentVersion13MigrationPersistsOneLeasePolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v13-concurrent-policy.db")
	createSchemaVersion13StoreForTest(t, path, nil)
	policies := []sessiontree.LeasePolicy{
		{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: time.Second},
		{TTL: 45 * time.Second, RenewInterval: 15 * time.Second, ClockSkewAllowance: 2 * time.Second},
	}
	type outcome struct {
		policy sessiontree.LeasePolicy
		store  *Store
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, len(policies))
	var wg sync.WaitGroup
	for _, policy := range policies {
		wg.Add(1)
		go func(policy sessiontree.LeasePolicy) {
			defer wg.Done()
			<-start
			store, err := Open(path, WithLeasePolicy(policy))
			outcomes <- outcome{policy: policy, store: store, err: err}
		}(policy)
	}
	close(start)
	wg.Wait()
	close(outcomes)

	var winner outcome
	var loser outcome
	for result := range outcomes {
		if result.err == nil {
			if winner.store != nil {
				t.Fatalf("multiple migration winners: %#v and %#v", winner.policy, result.policy)
			}
			winner = result
			continue
		}
		if loser.err != nil {
			t.Fatalf("multiple migration losers: %v and %v", loser.err, result.err)
		}
		loser = result
	}
	if winner.store == nil || loser.err == nil {
		t.Fatalf("migration outcomes winner=%#v loser=%#v", winner, loser)
	}
	winnerStore := winner.store
	t.Cleanup(func() { _ = winnerStore.Close() })
	var mismatch *storage.StoreLeasePolicyMismatchError
	if !errors.As(loser.err, &mismatch) || mismatch.Configured != loser.policy || mismatch.Persisted != winner.policy {
		t.Fatalf("migration loser err=%v mismatch=%#v, winner policy=%#v", loser.err, mismatch, winner.policy)
	}
	if err := winnerStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, WithLeasePolicy(winner.policy))
	if err != nil {
		t.Fatalf("reopen with winner policy: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := Open(path, WithLeasePolicy(loser.policy)); !errors.As(err, &mismatch) {
		t.Fatalf("reopen with loser policy err=%v, want StoreLeasePolicyMismatchError", err)
	}
}

func TestSQLiteStoreRejectsNonEmptySchemaVersion13WithoutChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v13-non-empty.db")
	createSchemaVersion13StoreForTest(t, path, func(db *sql.DB) {
		if _, err := db.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES('thread', '2026-07-18T00:00:00Z', '2026-07-18T00:00:00Z')`); err != nil {
			t.Fatal(err)
		}
	})

	_, err := Open(path)
	var unsupported *storage.UnsupportedStoreSchemaError
	if !errors.As(err, &unsupported) {
		t.Fatalf("Open error = %v, want UnsupportedStoreSchemaError", err)
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
	if version != schemaVersion13 {
		t.Fatalf("schema version after rejection = %q, want %q", version, schemaVersion13)
	}
	var threadCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM threads`).Scan(&threadCount); err != nil {
		t.Fatal(err)
	}
	if threadCount != 1 {
		t.Fatalf("thread count after rejection = %d, want 1", threadCount)
	}
	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode == "wal" {
		t.Fatalf("rejected schema persisted journal_mode=%q", journalMode)
	}
}

func TestSQLiteStoreRejectsUnsupportedSchemaShapesWithoutChanges(t *testing.T) {
	for _, tc := range []struct {
		name        string
		version     string
		fingerprint string
	}{
		{name: "old version", version: "12", fingerprint: "old"},
		{name: "unknown version", version: "999", fingerprint: "unknown"},
		{name: "alternate v13", version: schemaVersion13, fingerprint: "alternate-v13"},
		{name: "alternate v14", version: schemaVersion14, fingerprint: "alternate-v14"},
		{name: "alternate v15", version: schemaVersion, fingerprint: "alternate-v15"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unsupported.db")
			createSchemaVersion13StoreForTest(t, path, nil)
			db, err := sql.Open(driverName, path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, tc.version); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE schema_meta SET value = ? WHERE key = 'schema_fingerprint'`, tc.fingerprint); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			_, err = Open(path)
			var unsupported *storage.UnsupportedStoreSchemaError
			if !errors.As(err, &unsupported) {
				t.Fatalf("Open error = %v, want UnsupportedStoreSchemaError", err)
			}
			if unsupported.ObservedVersion != tc.version || unsupported.ObservedFingerprint != tc.fingerprint {
				t.Fatalf("unsupported schema error = %#v", unsupported)
			}

			db, err = sql.Open(driverName, path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var version, fingerprint string
			if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_fingerprint'`).Scan(&fingerprint); err != nil {
				t.Fatal(err)
			}
			if version != tc.version || fingerprint != tc.fingerprint {
				t.Fatalf("schema metadata changed to version=%q fingerprint=%q", version, fingerprint)
			}
		})
	}
}

func TestSQLiteStoreCanonicalFingerprintIsFrozen(t *testing.T) {
	if got := computedCanonicalSchemaFingerprint(); got != schemaFingerprintVersion15 {
		t.Fatalf("schemaFingerprintVersion15 = %q, want computed %q", schemaFingerprintVersion15, got)
	}
}

func createSchemaVersion13StoreForTest(t *testing.T, path string, seed func(*sql.DB)) {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaVersion13SQL); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`,
		schemaVersion13, rawEncoderVersion, schemaFingerprintVersion13); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if seed != nil {
		seed(db)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteStoreRejectsSchemaContractDrift(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	if _, err := store.db.ExecContext(ctx, `CREATE INDEX unexpected_threads_last_viewed_idx ON threads(last_viewed_at)`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "schema contract mismatch") {
		t.Fatalf("schema drift open err = %v", err)
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

	_, err = Open(path)
	var unsupported *storage.UnsupportedStoreSchemaError
	if !errors.As(err, &unsupported) || unsupported.ObservedVersion != "" || unsupported.ObservedFingerprint != "" {
		t.Fatalf("unversioned schema open err = %v, want typed unsupported empty shape", err)
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

	if _, err := Open(path); err == nil {
		t.Fatal("missing schema fingerprint unexpectedly opened")
	} else {
		var unsupported *storage.UnsupportedStoreSchemaError
		if !errors.As(err, &unsupported) {
			t.Fatalf("missing schema fingerprint open err = %v, want UnsupportedStoreSchemaError", err)
		}
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
	want := sessiontree.ProviderStateRecord{
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
	putProviderState := func(store *Store) {
		lease, err := store.AcquireTurnLease(ctx, sessiontree.TurnLease{
			ThreadID: "thread", Purpose: sessiontree.TurnLeasePurposeMutation, MutationID: "provider-state-test",
			MutationKind: "provider_state_test", OwnerID: "provider-state-test-owner",
		})
		if err != nil {
			t.Fatal(err)
		}
		leaseCtx := sessiontree.ContextWithTurnLease(ctx, lease)
		if err := store.PutProviderState(leaseCtx, want); err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseTurnLease(leaseCtx, lease); err != nil {
			t.Fatal(err)
		}
	}
	putProviderState(store)
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
	if _, err := store.ProviderState(ctx, "fork"); !errors.Is(err, sessiontree.ErrProviderStateNotFound) {
		t.Fatalf("fork provider state err = %v, want not found", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE provider_states SET state_json = '{' WHERE thread_id = 'thread'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderState(ctx, "thread"); err == nil || !strings.Contains(err.Error(), "decode provider state") {
		t.Fatalf("corrupt provider state err = %v", err)
	}
	putProviderState(store)
	if _, err := store.DeleteRootTree(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderState(ctx, "thread"); !errors.Is(err, sessiontree.ErrProviderStateNotFound) {
		t.Fatalf("deleted provider state err = %v, want not found", err)
	}
}

func TestSQLiteStorePersistsCompletedForkOperationAfterReopen(t *testing.T) {
	ctx := context.Background()
	store, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	planRecord := storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{NodeID: "root", SourceThreadID: "source", DestinationThreadID: "fork", ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source", "fork")},
	}
	plan := mustMarshalSQLiteForkOperationPlan(t, planRecord)
	record, created, err := store.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        "operation",
		RequestFingerprint: "fingerprint",
		SourceThreadIDs:    []string{"source"},
		AuthorityThreadIDs: []string{"source", "fork"},
		State:              storage.ForkOperationPrepared,
		Plan:               plan,
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	finishedAt := now.Add(time.Minute)
	record, replayed, err := store.CommitForkOperation(ctx, storage.ForkOperationCommitRequest{
		OperationID:        "operation",
		RequestFingerprint: "fingerprint",
		Plan:               plan,
		Nodes:              storage.ForkOperationPlanNodes(planRecord),
		Result:             []byte(`{"operation_id":"operation","thread_id":"fork"}`),
		FinishedAt:         finishedAt,
	})
	if err != nil || replayed {
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
	differentPlan := mustMarshalSQLiteForkOperationPlan(t, storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "different", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{NodeID: "root", SourceThreadID: "source", DestinationThreadID: "fork", ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source", "fork")},
	})
	existing, created, err := reopened.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        "operation",
		RequestFingerprint: "different",
		SourceThreadIDs:    []string{"source"},
		AuthorityThreadIDs: []string{"source", "fork"},
		State:              storage.ForkOperationPrepared,
		Plan:               differentPlan,
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil || created || existing.RequestFingerprint != "fingerprint" || existing.State != storage.ForkOperationCompleted {
		t.Fatalf("existing fork operation created=%v record=%#v err=%v", created, existing, err)
	}
}

func TestSQLiteStoreForkCommitIsAllNodeAtomicAcrossConnections(t *testing.T) {
	ctx := context.Background()
	first, path := openSQLiteStoreForTest(t)
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	leaves := map[string]string{}
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source-a", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	rootEntry, err := sessiontree.AppendMessage(ctx, first, "source-a", "seed", session.Message{Role: session.User, Content: "source-a"})
	if err != nil {
		t.Fatal(err)
	}
	leaves["source-a"] = rootEntry.ID
	childMeta, childEntry, err := first.CreateThreadWithInitialEntry(ctx, sessiontree.ThreadMeta{
		ID: "source-b", ParentThreadID: "source-a", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "child", CreatedAt: now, UpdatedAt: now,
	}, sessiontree.Entry{Type: sessiontree.EntryCustom, Message: session.Message{Role: session.User, Content: "source-b"}})
	if err != nil {
		t.Fatal(err)
	}
	leaves["source-b"] = childEntry.ID
	childMeta.Lifecycle = sessiontree.ThreadLifecycleClosed
	if err := first.UpdateThread(ctx, childMeta); err != nil {
		t.Fatal(err)
	}
	planRecord := storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "source-a", SourceEntryID: leaves["source-a"], SourceLeafEntryID: leaves["source-a"], DestinationThreadID: "destination-a",
			ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source-a", "destination-a"),
		},
		TerminalChildren: []storage.ForkOperationPlanNode{{
			NodeID: "terminal-child-1", SourceThreadID: "source-b", SourceEntryID: leaves["source-b"], SourceLeafEntryID: leaves["source-b"], DestinationThreadID: "destination-b",
			ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source-b", "destination-b"),
			DestinationMeta: &sessiontree.ForkDestinationMeta{ParentThreadID: "destination-a", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "child", Lifecycle: sessiontree.ThreadLifecycleClosed},
		}},
	}
	plan := mustMarshalSQLiteForkOperationPlan(t, planRecord)
	if _, created, err := first.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID: "operation", RequestFingerprint: "fingerprint",
		SourceThreadIDs:    []string{"source-a", "source-b"},
		AuthorityThreadIDs: []string{"source-a", "destination-a", "source-b", "destination-b"},
		State:              storage.ForkOperationPrepared, Plan: plan, CreatedAt: now, UpdatedAt: now,
	}); err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	nodes := storage.ForkOperationPlanNodes(planRecord)
	injected := errors.New("injected sqlite fork rewrite failure")
	failing := append([]sessiontree.ForkOptions(nil), nodes...)
	failing[1].RewriteEntry = func(sessiontree.Entry, sessiontree.ForkEntryIdentity) (sessiontree.Entry, error) {
		return sessiontree.Entry{}, injected
	}
	request := storage.ForkOperationCommitRequest{
		OperationID: "operation", RequestFingerprint: "fingerprint", Plan: plan, Nodes: failing,
		Result: []byte(`{"operation_id":"operation","thread_id":"destination-a"}`), FinishedAt: now.Add(time.Minute),
	}
	if _, _, err := second.CommitForkOperation(ctx, request); !errors.Is(err, injected) {
		t.Fatalf("CommitForkOperation error = %v, want injected failure", err)
	}
	for _, threadID := range []string{"destination-a", "destination-b"} {
		if _, err := first.Thread(ctx, threadID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("destination %q visible after rollback: %v", threadID, err)
		}
	}
	if _, err := sessiontree.AppendMessage(ctx, first, "source-a", "blocked", session.Message{Role: session.User, Content: "blocked"}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("prepared claim was released after transient failure: %v", err)
	}

	request.Nodes = nodes
	committed, replayed, err := second.CommitForkOperation(ctx, request)
	if err != nil || replayed || committed.State != storage.ForkOperationCompleted {
		t.Fatalf("CommitForkOperation record=%#v replayed=%v err=%v", committed, replayed, err)
	}
	for _, threadID := range []string{"destination-a", "destination-b"} {
		if _, err := first.Thread(ctx, threadID); err != nil {
			t.Fatalf("destination %q missing after commit: %v", threadID, err)
		}
	}
	if _, replayed, err := first.CommitForkOperation(ctx, request); err != nil || !replayed {
		t.Fatalf("completed replay replayed=%v err=%v", replayed, err)
	}
}

func TestSQLiteStorePrepareForkRejectsTombstonedDestination(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC)
	for _, threadID := range []string{"source", "deleted-destination"} {
		if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.DeleteRootTree(ctx, "deleted-destination"); err != nil {
		t.Fatal(err)
	}
	plan := mustMarshalSQLiteForkOperationPlan(t, storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{NodeID: "root", SourceThreadID: "source", DestinationThreadID: "deleted-destination", ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source", "deleted-destination")},
	})
	_, created, err := store.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID: "operation", RequestFingerprint: "fingerprint",
		SourceThreadIDs: []string{"source"}, AuthorityThreadIDs: []string{"source", "deleted-destination"},
		State: storage.ForkOperationPrepared, Plan: plan, CreatedAt: now, UpdatedAt: now,
	})
	if !errors.Is(err, sessiontree.ErrForkDestinationConflict) || created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	if _, err := store.ForkOperation(ctx, "operation"); !errors.Is(err, storage.ErrForkOperationNotFound) {
		t.Fatalf("rejected prepare persisted operation: %v", err)
	}
}

func TestSQLitePrepareForkOperationRejectsTerminalChildSetDriftWithoutPublishingClaims(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Date(2026, 7, 19, 13, 30, 0, 0, time.UTC)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	plan := mustMarshalSQLiteForkOperationPlan(t, storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "stale-operation", RequestFingerprint: "stale-fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{NodeID: "root", SourceThreadID: "source", DestinationThreadID: "destination", ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source", "destination")},
	})
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "late-terminal-child", ParentThreadID: "source", ParentTurnID: "parent-turn", TaskName: "late-child", AgentPath: "late-child",
		Lifecycle: sessiontree.ThreadLifecycleClosed, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	_, created, err := store.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID: "stale-operation", RequestFingerprint: "stale-fingerprint",
		SourceThreadIDs: []string{"source"}, AuthorityThreadIDs: []string{"source", "destination"},
		State: storage.ForkOperationPrepared, Plan: plan, CreatedAt: now, UpdatedAt: now,
	})
	if !errors.Is(err, sessiontree.ErrStaleAuthority) || created {
		t.Fatalf("PrepareForkOperation created=%v err=%v, want ErrStaleAuthority", created, err)
	}
	if _, err := store.ForkOperation(ctx, "stale-operation"); !errors.Is(err, storage.ErrForkOperationNotFound) {
		t.Fatalf("rejected prepare stored operation: %v", err)
	}
	if got := sqliteTableRowCount(t, store, "thread_authority_claims"); got != 0 {
		t.Fatalf("rejected prepare stored %d authority claims, want 0", got)
	}
	if _, err := store.Thread(ctx, "destination"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("rejected prepare published destination: %v", err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, "source", "turn", session.Message{Role: session.User, Content: "unclaimed"}); err != nil {
		t.Fatalf("rejected prepare retained source claim: %v", err)
	}
}

func TestSQLiteStoreAllowsDuplicateSubAgentPathWithDistinctThreadIDs(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Date(2026, 6, 23, 9, 0, 0, 0, time.UTC)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
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
	lease, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{
		ThreadID: "thread",
		TurnID:   "turn-1",
		OwnerID:  "owner-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-2", OwnerID: "owner-2"}); !errors.Is(err, sessiontree.ErrActiveTurn) {
		t.Fatalf("second acquire err = %v, want ErrActiveTurn", err)
	}
	active, ok, err := second.ActiveTurnLease(ctx, "thread")
	if err != nil || !ok || !sessiontree.SameTurnLease(active, lease) {
		t.Fatalf("active lease = %#v ok=%v err=%v, want %#v", active, ok, err, lease)
	}
	wrongOwner := lease
	wrongOwner.OwnerID = "wrong-owner"
	if err := second.ReleaseTurnLease(ctx, wrongOwner); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("wrong-owner release err = %v, want ErrStaleAuthority", err)
	}
	renewed, err := second.RenewTurnLease(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.Generation != lease.Generation || renewed.Heartbeat != lease.Heartbeat+1 || !renewed.ExpiresAt.After(lease.ExpiresAt) {
		t.Fatalf("renewed lease = %#v, previous %#v", renewed, lease)
	}
	if err := first.ReleaseTurnLease(ctx, lease); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("old heartbeat release err = %v, want ErrStaleAuthority", err)
	}
	if err := first.ReleaseTurnLease(ctx, renewed); err != nil {
		t.Fatal(err)
	}
	next, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-3", OwnerID: "owner-3"})
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if next.Generation <= renewed.Generation {
		t.Fatalf("next generation = %d, want > %d", next.Generation, renewed.Generation)
	}
	if err := first.ReleaseTurnLease(ctx, next); err != nil {
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

func TestSQLiteStorePathPageUsesActivePathOrdinals(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"root", "one", "two", "leaf"} {
		if _, err := store.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.PathPage(ctx, "thread", "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := pathEntryIDs(page.Entries); !slices.Equal(got, []string{"leaf", "two"}) || page.NewestOrdinal != 4 || !page.HasMore || page.NextEntryID != "two" {
		t.Fatalf("first path page = %#v", page)
	}
	page, err = store.PathPage(ctx, "thread", "", page.NextEntryID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := pathEntryIDs(page.Entries); !slices.Equal(got, []string{"one", "root"}) || page.NewestOrdinal != 2 || page.HasMore || page.NextEntryID != "" {
		t.Fatalf("second path page = %#v", page)
	}
	if _, err := store.PathPage(ctx, "thread", "", "missing", 2); !errors.Is(err, sessiontree.ErrEntryNotFound) {
		t.Fatalf("missing continuation err = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE entries SET parent_id = 'leaf' WHERE thread_id = 'thread' AND id = 'root'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PathPage(ctx, "thread", "", "", 2); !errors.Is(err, sessiontree.ErrInvalidParent) {
		t.Fatalf("cyclic path page err = %v", err)
	}
}

func pathEntryIDs(entries []sessiontree.Entry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.ID)
	}
	return out
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

func TestSQLiteStoreRootDeletePreservesHostMetadataAndCleansEngineData(t *testing.T) {
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
	artifactRef := seedSQLiteArtifactThroughEffect(t, ctx, store, "thread", "full durable tool output")
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
	if content, err := store.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: "thread", ArtifactID: artifactRef.ID}); err != nil || content.Text != "full durable tool output" || content.Ref != artifactRef {
		t.Fatalf("reopened artifact content=%#v err=%v", content, err)
	}
	if _, err := store.DeleteRootTree(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Thread(ctx, "thread"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("thread after delete err = %v", err)
	}
	if metadata, err := store.Metadata(ctx, "ns", "thread"); err != nil || string(metadata.Data) != `{"title":"Thread"}` {
		t.Fatalf("host metadata after canonical delete = %#v err=%v", metadata, err)
	}
	if metadata, err := store.Metadata(ctx, "other", "thread"); err != nil || string(metadata.Data) != `{"keep":true}` {
		t.Fatalf("other host metadata after canonical delete = %#v err=%v", metadata, err)
	}
	if segments, err := store.Segments(ctx, "thread", "openai", "model"); err != nil || len(segments) != 0 {
		t.Fatalf("segments after delete = %#v err=%v", segments, err)
	}
	if requests, err := store.ProviderRequests(ctx, "thread"); err != nil || len(requests) != 0 {
		t.Fatalf("requests after delete = %#v err=%v", requests, err)
	}
	if content, err := store.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: "thread", ArtifactID: artifactRef.ID}); !errors.Is(err, sessiontree.ErrThreadDeleted) || content != (sessiontree.ArtifactContent{}) {
		t.Fatalf("artifact after delete content=%#v err=%v", content, err)
	}
	if replayed, err := store.DeleteRootTree(ctx, "thread"); err != nil || !replayed.Replayed {
		t.Fatalf("second canonical delete = %#v err=%v, want replay", replayed, err)
	}
	if err := store.putMetaValue(ctx, "schema_version", "999"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dbPath); err == nil {
		t.Fatal("unsupported schema unexpectedly opened")
	} else {
		var unsupported *storage.UnsupportedStoreSchemaError
		if !errors.As(err, &unsupported) {
			t.Fatalf("unsupported schema open err = %v, want UnsupportedStoreSchemaError", err)
		}
	}
}

func TestSQLiteStoreDeleteRootTreeDeletesNestedCanonicalData(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	seedSQLiteThreadTreeData(t, ctx, store, "parent", "")
	seedSQLiteThreadTreeData(t, ctx, store, "child", "parent")
	seedSQLiteThreadTreeData(t, ctx, store, "grandchild", "child")
	seedSQLiteThreadTreeData(t, ctx, store, "survivor", "")

	deleted, err := store.DeleteRootTree(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(deleted.ThreadIDs, []string{"parent", "child", "grandchild"}) {
		t.Fatalf("deleted thread ids = %#v", deleted.ThreadIDs)
	}
	for _, threadID := range []string{"parent", "child", "grandchild"} {
		if _, err := store.Thread(ctx, threadID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("Thread(%q) err = %v, want ErrThreadNotFound", threadID, err)
		}
		assertSQLiteThreadTreeDataCount(t, ctx, store, threadID, 0)
		if _, err := store.Metadata(ctx, "thread", threadID); err != nil {
			t.Fatalf("host metadata for deleted canonical thread %q: %v", threadID, err)
		}
	}
	if _, err := store.Thread(ctx, "survivor"); err != nil {
		t.Fatalf("survivor thread err = %v", err)
	}
	assertSQLiteThreadTreeDataCount(t, ctx, store, "survivor", 1)
}

func TestSQLiteStoreDeleteRootTreeRollsBackOnDeleteFailure(t *testing.T) {
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

	_, err := store.DeleteRootTree(ctx, "parent")
	if err == nil || !strings.Contains(err.Error(), "injected thread tree delete failure") {
		t.Fatalf("DeleteRootTree err = %v, want injected failure", err)
	}
	for _, threadID := range []string{"parent", "child", "grandchild"} {
		if _, err := store.Thread(ctx, threadID); err != nil {
			t.Fatalf("Thread(%q) after rollback err = %v", threadID, err)
		}
		assertSQLiteThreadTreeDataCount(t, ctx, store, threadID, 1)
	}
}

func TestSQLiteStoreDeleteThreadTreeRejectsActiveRootOrChildAcrossConnections(t *testing.T) {
	for _, activeThreadID := range []string{"parent", "child"} {
		t.Run(activeThreadID, func(t *testing.T) {
			ctx := context.Background()
			first, path := openSQLiteStoreForTest(t)
			seedSQLiteThreadTreeData(t, ctx, first, "parent", "")
			seedSQLiteThreadTreeData(t, ctx, first, "child", "parent")
			second, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = second.Close() })
			if _, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: activeThreadID, TurnID: "turn-active", OwnerID: "owner"}); err != nil {
				t.Fatal(err)
			}

			if _, err := second.DeleteRootTree(ctx, "parent"); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
				t.Fatalf("DeleteRootTree err = %v, want ErrThreadAuthorityBusy", err)
			}
			for _, threadID := range []string{"parent", "child"} {
				if _, err := second.Thread(ctx, threadID); err != nil {
					t.Fatalf("Thread(%q) after rejected delete: %v", threadID, err)
				}
			}
		})
	}
}

func TestSQLiteStoreForkAuthorityClaimBlocksAdmissionAndDeleteAcrossConnections(t *testing.T) {
	for _, terminalState := range []storage.ForkOperationState{storage.ForkOperationCompleted, storage.ForkOperationFailed} {
		t.Run(string(terminalState), func(t *testing.T) {
			ctx := context.Background()
			first, path := openSQLiteStoreForTest(t)
			now := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
			if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			second, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = second.Close() })
			planRecord := storage.ForkOperationPlan{
				Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "fingerprint", PreparedAt: now,
				Root: storage.ForkOperationPlanNode{NodeID: "root", SourceThreadID: "source", DestinationThreadID: "fork", ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source", "fork")},
			}
			plan := mustMarshalSQLiteForkOperationPlan(t, planRecord)
			_, created, err := first.PrepareForkOperation(ctx, storage.ForkOperationRecord{
				OperationID:        "operation",
				RequestFingerprint: "fingerprint",
				SourceThreadIDs:    []string{"source"},
				AuthorityThreadIDs: []string{"source", "fork"},
				State:              storage.ForkOperationPrepared,
				Plan:               plan,
				CreatedAt:          now,
				UpdatedAt:          now,
			})
			if err != nil || !created {
				t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
			}

			if _, err := second.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "source", TurnID: "turn", OwnerID: "owner"}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
				t.Fatalf("AcquireTurnLease err = %v, want ErrThreadAuthorityBusy", err)
			}
			if _, err := second.DeleteRootTree(ctx, "source"); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
				t.Fatalf("DeleteRootTree err = %v, want ErrThreadAuthorityBusy", err)
			}

			finishedAt := now.Add(time.Minute)
			if terminalState == storage.ForkOperationCompleted {
				_, replayed, err := first.CommitForkOperation(ctx, storage.ForkOperationCommitRequest{
					OperationID:        "operation",
					RequestFingerprint: "fingerprint",
					Plan:               plan,
					Nodes:              storage.ForkOperationPlanNodes(planRecord),
					Result:             []byte(`{"operation_id":"operation","thread_id":"fork"}`),
					FinishedAt:         finishedAt,
				})
				if err != nil || replayed {
					t.Fatalf("CommitForkOperation replayed=%v err=%v", replayed, err)
				}
			} else {
				_, replayed, err := first.FailForkOperation(ctx, storage.ForkOperationFailureRequest{
					OperationID:        "operation",
					RequestFingerprint: "fingerprint",
					ErrorCode:          "injected",
					ErrorMessage:       "injected failure",
					FinishedAt:         finishedAt,
				})
				if err != nil || replayed {
					t.Fatalf("FailForkOperation replayed=%v err=%v", replayed, err)
				}
			}
			lease, err := second.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "source", TurnID: "turn", OwnerID: "owner"})
			if err != nil {
				t.Fatalf("AcquireTurnLease after terminal settlement: %v", err)
			}
			if err := second.ReleaseTurnLease(ctx, lease); err != nil {
				t.Fatal(err)
			}
			if _, err := second.DeleteRootTree(ctx, "source"); err != nil {
				t.Fatalf("DeleteRootTree after terminal settlement: %v", err)
			}
		})
	}
}

func TestSQLiteStorePrepareForkRejectsActiveSourceAcrossConnections(t *testing.T) {
	ctx := context.Background()
	first, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 7, 18, 19, 0, 0, 0, time.UTC)
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "source", TurnID: "turn", OwnerID: "owner"}); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	plan := mustMarshalSQLiteForkOperationPlan(t, storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{NodeID: "root", SourceThreadID: "source", DestinationThreadID: "fork", ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source", "fork")},
	})

	_, created, err := second.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        "operation",
		RequestFingerprint: "fingerprint",
		SourceThreadIDs:    []string{"source"},
		AuthorityThreadIDs: []string{"source", "fork"},
		State:              storage.ForkOperationPrepared,
		Plan:               plan,
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if !errors.Is(err, sessiontree.ErrActiveTurn) || created {
		t.Fatalf("PrepareForkOperation created=%v err=%v, want ErrActiveTurn", created, err)
	}
	if _, err := first.ForkOperation(ctx, "operation"); !errors.Is(err, storage.ErrForkOperationNotFound) {
		t.Fatalf("ForkOperation after rejected prepare err = %v, want not found", err)
	}
}

func TestSQLiteStoreForkAuthorityClaimBlocksUnownedMutationsAcrossConnections(t *testing.T) {
	ctx := context.Background()
	first, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 7, 18, 20, 0, 0, 0, time.UTC)
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	entry, err := sessiontree.AppendMessage(ctx, first, "source", "turn-1", session.Message{Role: session.User, Content: "source"})
	if err != nil {
		t.Fatal(err)
	}
	plan := mustMarshalSQLiteForkOperationPlan(t, storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "source", SourceEntryID: entry.ID, SourceLeafEntryID: entry.ID, DestinationThreadID: "fork",
			ArtifactClosure: emptySQLiteForkArtifactClosure(t, "source", "fork"),
		},
	})
	_, created, err := first.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        "operation",
		RequestFingerprint: "fingerprint",
		SourceThreadIDs:    []string{"source"},
		AuthorityThreadIDs: []string{"source", "fork"},
		State:              storage.ForkOperationPrepared,
		Plan:               plan,
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })

	if _, err := sessiontree.AppendMessage(ctx, second, "source", "turn-2", session.Message{Role: session.User, Content: "blocked"}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("AppendMessage err = %v, want ErrThreadAuthorityBusy", err)
	}
	meta, err := second.Thread(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	meta.Title = "blocked"
	if err := second.UpdateThread(ctx, meta); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("UpdateThread err = %v, want ErrThreadAuthorityBusy", err)
	}
	if _, err := second.CompareAndSwapAgentTodoState(ctx, sessiontree.AgentTodoState{ThreadID: "source"}, 0); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("CompareAndSwapAgentTodoState err = %v, want ErrThreadAuthorityBusy", err)
	}
	if err := second.MoveLeaf(ctx, "source", entry.ID); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("MoveLeaf err = %v, want ErrThreadAuthorityBusy", err)
	}
	if _, err := second.CreateThread(ctx, sessiontree.ThreadMeta{ID: "fork", CreatedAt: now, UpdatedAt: now}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("CreateThread destination err = %v, want ErrThreadAuthorityBusy", err)
	}
	if _, err := second.CreateThread(ctx, sessiontree.ThreadMeta{ID: "child", ParentThreadID: "source", TaskName: "child", AgentPath: "/root/child", CreatedAt: now, UpdatedAt: now}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("CreateThread child err = %v, want ErrThreadAuthorityBusy", err)
	}
	if _, err := second.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "source", NewThreadID: "other", Now: now}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("unowned Fork err = %v, want ErrThreadAuthorityBusy", err)
	}
	if _, err := second.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "source", NewThreadID: "fork", OperationID: "wrong", OperationNodeID: "root", Now: now}); !errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
		t.Fatalf("wrong-operation Fork err = %v, want ErrInvalidThreadAuthority", err)
	}
	if _, err := second.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "source", NewThreadID: "fork", OperationID: "operation", OperationNodeID: "root", Now: now}); !errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
		t.Fatalf("raw operation Fork err = %v, want ErrInvalidThreadAuthority", err)
	}

	finishedAt := now.Add(time.Minute)
	_, replayed, err := first.CommitForkOperation(ctx, storage.ForkOperationCommitRequest{
		OperationID:        "operation",
		RequestFingerprint: "fingerprint",
		Plan:               plan,
		Nodes: []sessiontree.ForkOptions{{
			SourceThreadID:       "source",
			EntryID:              entry.ID,
			EntryIDPinned:        true,
			ExpectedSourceLeafID: entry.ID,
			Position:             sessiontree.ForkAt,
			NewThreadID:          "fork",
			OperationID:          "operation",
			OperationNodeID:      "root",
			Now:                  now,
			ArtifactClosure:      emptySQLiteForkArtifactClosure(t, "source", "fork"),
		}},
		Result:     []byte(`{"operation_id":"operation","thread_id":"fork"}`),
		FinishedAt: finishedAt,
	})
	if err != nil || replayed {
		t.Fatalf("CommitForkOperation replayed=%v err=%v", replayed, err)
	}
	if _, err := sessiontree.AppendMessage(ctx, second, "source", "turn-2", session.Message{Role: session.User, Content: "allowed"}); err != nil {
		t.Fatalf("AppendMessage after release: %v", err)
	}
}

func TestSQLiteStoreTurnLeaseFencesJournalMutationAcrossConnections(t *testing.T) {
	ctx := context.Background()
	first, path := openSQLiteStoreForTest(t)
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	firstLease, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-1", OwnerID: "owner-1"})
	if err != nil {
		t.Fatal(err)
	}
	appendTurn := func(ctx context.Context, turnID string) error {
		_, err := sessiontree.AppendMessage(ctx, second, "thread", turnID, session.Message{Role: session.User, Content: turnID})
		return err
	}
	if err := appendTurn(ctx, "turn-1"); !errors.Is(err, sessiontree.ErrActiveTurn) {
		t.Fatalf("append without proof err = %v, want ErrActiveTurn", err)
	}
	wrongOwner := firstLease
	wrongOwner.OwnerID = "wrong"
	if err := appendTurn(sessiontree.ContextWithTurnLease(ctx, wrongOwner), "turn-1"); !errors.Is(err, sessiontree.ErrActiveTurn) {
		t.Fatalf("append with wrong owner err = %v, want ErrActiveTurn", err)
	}
	if err := appendTurn(sessiontree.ContextWithTurnLease(ctx, firstLease), "turn-2"); !errors.Is(err, sessiontree.ErrActiveTurn) {
		t.Fatalf("append with wrong turn err = %v, want ErrActiveTurn", err)
	}
	if err := appendTurn(sessiontree.ContextWithTurnLease(ctx, firstLease), "turn-1"); err != nil {
		t.Fatalf("append with exact proof: %v", err)
	}
	if err := first.ReleaseTurnLease(ctx, firstLease); err != nil {
		t.Fatal(err)
	}
	if err := appendTurn(sessiontree.ContextWithTurnLease(ctx, firstLease), "turn-1"); !errors.Is(err, sessiontree.ErrActiveTurn) {
		t.Fatalf("append with released proof err = %v, want ErrActiveTurn", err)
	}
	secondLease, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "thread", TurnID: "turn-2", OwnerID: "owner-2"})
	if err != nil {
		t.Fatal(err)
	}
	if err := appendTurn(sessiontree.ContextWithTurnLease(ctx, firstLease), "turn-1"); !errors.Is(err, sessiontree.ErrActiveTurn) {
		t.Fatalf("append with stale generation err = %v, want ErrActiveTurn", err)
	}
	if err := appendTurn(sessiontree.ContextWithTurnLease(ctx, secondLease), "turn-2"); err != nil {
		t.Fatalf("append with current generation: %v", err)
	}
}

func TestSQLiteStoreSubAgentCloseRejectsForeignLeaseAndCommitsAtomically(t *testing.T) {
	ctx := context.Background()
	first, path := openSQLiteStoreForTest(t)
	now := time.Date(2026, 7, 18, 21, 0, 0, 0, time.UTC)
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "child", AgentPath: "/root/child", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	lease, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "child", TurnID: "turn", OwnerID: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	request := sessiontree.PrepareSubAgentCloseRequest{
		CloseOperationID: "close-1",
		ParentThreadID:   "parent",
		TargetThreadID:   "child",
		Reason:           "test",
		Now:              now.Add(time.Minute),
	}
	before, err := second.Entries(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.PrepareSubAgentClose(ctx, request); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("PrepareSubAgentClose with remote lease err = %v, want ErrThreadAuthorityBusy", err)
	}
	meta, err := second.Thread(ctx, "child")
	if err != nil || meta.Lifecycle == sessiontree.ThreadLifecycleClosed {
		t.Fatalf("child after rejected close meta=%#v err=%v", meta, err)
	}
	afterRejected, err := second.Entries(ctx, "child")
	if err != nil || len(afterRejected) != len(before) {
		t.Fatalf("journal changed after rejected close before=%d after=%d err=%v", len(before), len(afterRejected), err)
	}
	request.TargetLease = &lease
	prepared, err := second.PrepareSubAgentClose(ctx, request)
	if err != nil || prepared.Replayed || prepared.Operation.State != sessiontree.SubAgentClosePrepared {
		t.Fatalf("PrepareSubAgentClose result=%#v err=%v", prepared, err)
	}
	meta, err = first.Thread(ctx, "child")
	if err != nil || !meta.IsClosing() || meta.CloseOperationID != "close-1" {
		t.Fatalf("cross-connection prepared meta=%#v err=%v", meta, err)
	}
	if err := first.ReleaseTurnLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	finished, err := second.FinishSubAgentClose(ctx, sessiontree.FinishSubAgentCloseRequest{
		CloseOperationID: "close-1", ParentThreadID: "parent", TargetThreadID: "child", Reason: "test", Now: now.Add(2 * time.Minute),
	})
	if err != nil || finished.Replayed || len(finished.Threads) != 1 || len(finished.Entries) != 1 {
		t.Fatalf("FinishSubAgentClose result=%#v err=%v", finished, err)
	}
	meta = finished.Threads[0]
	entry := finished.Entries[0]
	if meta.Lifecycle != sessiontree.ThreadLifecycleClosed || meta.CloseOperationID != "" || entry.ID == "" || meta.LeafID != entry.ID {
		t.Fatalf("closed meta=%#v entry=%#v", meta, entry)
	}
	reopened, err := first.Thread(ctx, "child")
	if err != nil || reopened.Lifecycle != sessiontree.ThreadLifecycleClosed || reopened.LeafID != entry.ID {
		t.Fatalf("cross-connection closed meta=%#v err=%v", reopened, err)
	}
	if _, err := first.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: "child", TurnID: "later", OwnerID: "later"}); !errors.Is(err, sessiontree.ErrThreadClosed) {
		t.Fatalf("AcquireTurnLease on closed child err = %v, want ErrThreadClosed", err)
	}
	if _, err := sessiontree.AppendMessage(ctx, first, "child", "later", session.Message{Role: session.User, Content: "later"}); !errors.Is(err, sessiontree.ErrThreadClosed) {
		t.Fatalf("AppendMessage on closed child err = %v, want ErrThreadClosed", err)
	}
}

func TestSQLiteStoreThreadPublicationRollsBackWithInitialEntry(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	child := sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "child", AgentPath: "/root/child"}
	badInitial := sessiontree.Entry{Type: sessiontree.EntryCustom, ParentID: "missing", Metadata: map[string]string{"kind": "input"}}
	if _, _, err := store.CreateThreadWithInitialEntry(ctx, child, badInitial); !errors.Is(err, sessiontree.ErrInvalidParent) {
		t.Fatalf("CreateThreadWithInitialEntry err = %v, want ErrInvalidParent", err)
	}
	if _, err := store.Thread(ctx, "child"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("child after rejected publication err = %v, want ErrThreadNotFound", err)
	}
	meta, initial, err := store.CreateThreadWithInitialEntry(ctx, child, sessiontree.Entry{Type: sessiontree.EntryCustom, Metadata: map[string]string{"kind": "input"}})
	if err != nil || meta.LeafID != initial.ID || initial.ID == "" {
		t.Fatalf("published child meta=%#v initial=%#v err=%v", meta, initial, err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, "parent", "turn-1", session.Message{Role: session.User, Content: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ForkWithInitialEntry(ctx, sessiontree.ForkOptions{SourceThreadID: "parent", NewThreadID: "fork"}, badInitial); !errors.Is(err, sessiontree.ErrInvalidParent) {
		t.Fatalf("ForkWithInitialEntry err = %v, want ErrInvalidParent", err)
	}
	if _, err := store.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("fork after rejected publication err = %v, want ErrThreadNotFound", err)
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
	if _, err := Open(dbPath); err == nil {
		t.Fatal("unsupported raw encoder unexpectedly opened")
	} else {
		var unsupported *storage.UnsupportedStoreSchemaError
		if !errors.As(err, &unsupported) {
			t.Fatalf("unsupported raw encoder open err = %v, want UnsupportedStoreSchemaError", err)
		}
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
	meta := sessiontree.ThreadMeta{ID: threadID, ParentThreadID: parentThreadID, CreatedAt: now}
	if parentThreadID != "" {
		meta.TaskName = threadID
		meta.AgentPath = "/root/" + threadID
	}
	if _, err := store.CreateThread(ctx, meta); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, threadID, "turn-1", session.Message{Role: session.User, Content: "hello"}); err != nil {
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
	seedSQLiteArtifactThroughEffect(t, ctx, store, threadID, "output")
}

func seedSQLiteArtifactThroughEffect(t *testing.T, ctx context.Context, store *Store, threadID, text string) artifact.Ref {
	t.Helper()
	now := time.Date(2026, 7, 13, 10, 5, 0, 0, time.UTC)
	turnID := "artifact-turn-" + threadID
	runID := "artifact-run-" + threadID
	callID := "artifact-call-" + threadID
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: "artifact-owner-" + threadID,
		RequestFingerprint: "artifact-admit-" + threadID, Input: session.Message{Role: session.User, Content: "preserve full output"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.PrepareEffectAttempt(ctx, sessiontree.PrepareEffectAttemptRequest{
		Lease: admitted.Lease, RequestFingerprint: "artifact-effect-" + threadID, Now: now,
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: threadID, TurnID: turnID, RunID: runID, ToolCallID: callID, ToolName: "read", ArgumentHash: "artifact-arguments-" + threadID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginEffectDispatch(ctx, sessiontree.BeginEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "artifact-effect-" + threadID,
		ObservedHeartbeat: admitted.Lease.Heartbeat, AuthorizationProofHash: "artifact-proof-" + threadID, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	finished, err := store.FinishEffectDispatch(ctx, sessiontree.FinishEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "artifact-effect-" + threadID,
		OutcomeFingerprint: "artifact-outcome-" + threadID, FullOutput: &artifact.FullOutput{Text: text}, Now: now,
		Result: sessiontree.Entry{ThreadID: threadID, TurnID: turnID, Type: sessiontree.EntryToolResult,
			Message: session.Message{Role: session.Tool, ToolCallID: callID, ToolName: "read", Content: "visible", ToolResult: &session.ToolResultView{Status: "success", Truncated: true}}},
	})
	if err != nil || finished.Artifact == nil {
		t.Fatalf("FinishEffectDispatch result=%#v err=%v", finished, err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: runID, TerminalEntryID: "artifact-terminal-" + threadID,
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "artifact-turn-outcome-" + threadID, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	return *finished.Artifact
}

func assertSQLiteThreadTreeDataCount(t *testing.T, ctx context.Context, store *Store, threadID string, want int) {
	t.Helper()
	for _, item := range []struct {
		table  string
		column string
	}{
		{table: "entries", column: "thread_id"},
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
		if item.table == "entries" && want == 1 {
			if got == 0 {
				t.Fatalf("%s rows for %q = %d, want at least one", item.table, threadID, got)
			}
			continue
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

func mustMarshalSQLiteForkOperationPlan(t *testing.T, plan storage.ForkOperationPlan) []byte {
	t.Helper()
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func emptySQLiteForkArtifactClosure(t *testing.T, sourceThreadID, destinationThreadID string) artifact.Closure {
	t.Helper()
	items := []artifact.ManifestItem{}
	fingerprint, err := artifact.ClosureFingerprint(sourceThreadID, destinationThreadID, items)
	if err != nil {
		t.Fatal(err)
	}
	return artifact.Closure{SourceThreadID: sourceThreadID, DestinationThreadID: destinationThreadID, Items: items, Fingerprint: fingerprint}
}
