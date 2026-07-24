package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteStoreMigratesQuiescentNonEmptySchemaVersion13(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v13-quiescent.db")
	entries := createQuiescentSchemaVersion13Store(t, path)

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open migrated v13 store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if version, err := store.SchemaVersion(context.Background()); err != nil || version != schemaVersion {
		t.Fatalf("migrated version=%q err=%v, want %q", version, err, schemaVersion)
	}
	pathEntries, err := store.Path(context.Background(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pathEntries) != len(entries) {
		t.Fatalf("migrated path length=%d, want %d", len(pathEntries), len(entries))
	}
	for index := range entries {
		if pathEntries[index].ID != entries[index].ID || pathEntries[index].Raw != entries[index].Raw || pathEntries[index].RawHash != entries[index].RawHash {
			t.Fatalf("migrated entry %d=%#v, want identity/raw from %#v", index, pathEntries[index], entries[index])
		}
	}
}

func TestSQLiteSchemaVersion13MigrationKeepsTitleAuthoritySeparateFromExecutionIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v13-title.db")
	createQuiescentSchemaVersion13Store(t, path)
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE threads SET
		title = 'Published title', title_status = 'ready', title_source = 'provider',
		title_updated_at = '2026-07-24T00:00:04Z'
		WHERE id = 'thread'`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta, err := store.Thread(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TitleGeneration != 1 || meta.TitleToken != "migrated-v15:thread" {
		t.Fatalf("migrated title authority generation/token=%d/%q", meta.TitleGeneration, meta.TitleToken)
	}
	var admissions int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM turn_admissions`).Scan(&admissions); err != nil {
		t.Fatal(err)
	}
	if admissions != 0 {
		t.Fatalf("title authority migration synthesized %d execution admissions", admissions)
	}
}

func TestSQLiteSchemaVersion13MigrationPreservesPublishedSubAgentStatus(t *testing.T) {
	tests := []struct {
		status   string
		terminal sessiontree.TurnMarkerStatus
		closed   bool
	}{
		{status: "idle"},
		{status: "completed", terminal: sessiontree.TurnCompleted},
		{status: "waiting", terminal: sessiontree.TurnWaiting},
		{status: "failed", terminal: sessiontree.TurnFailed},
		{status: "cancelled", terminal: sessiontree.TurnAborted},
		{status: "interrupted", terminal: sessiontree.TurnAborted},
		{status: "closed", terminal: sessiontree.TurnCompleted, closed: true},
	}
	for _, test := range tests {
		t.Run(test.status, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "v13-subagent.db")
			createQuiescentSchemaVersion13Store(t, path)
			addSchemaVersion13Child(t, path, test.status, test.terminal, test.closed)

			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			meta, err := store.Thread(context.Background(), "child")
			if err != nil {
				t.Fatal(err)
			}
			entries, err := store.Path(context.Background(), "child", "")
			if err != nil {
				t.Fatal(err)
			}
			if test.closed {
				if !meta.IsClosed() {
					t.Fatalf("migrated child lifecycle=%q, want closed", meta.Lifecycle)
				}
				return
			}
			if meta.IsClosed() {
				t.Fatalf("migrated child lifecycle=%q, want open", meta.Lifecycle)
			}
			if got := sessionlifecycle.Derive(entries, sessionlifecycle.PhaseIdle).Status(); got != test.status {
				t.Fatalf("migrated public journal status=%q, want %q", got, test.status)
			}
		})
	}
}

func TestSQLiteStoreMigratesPublishedNonEmptySchemasVersion3Through13(t *testing.T) {
	for version := 3; version <= 13; version++ {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("v%d.db", version))
			entries := createPublishedLegacyStore(t, path, version)
			store, err := Open(path)
			if err != nil {
				t.Fatalf("Open schema v%d: %v", version, err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if got, err := store.SchemaVersion(context.Background()); err != nil || got != schemaVersion {
				t.Fatalf("migrated schema=%q err=%v", got, err)
			}
			migrated, err := store.Path(context.Background(), "thread", "")
			if err != nil || len(migrated) != len(entries) {
				t.Fatalf("migrated path=%#v err=%v", migrated, err)
			}
			for index := range entries {
				if migrated[index].ID != entries[index].ID || migrated[index].Raw != entries[index].Raw || migrated[index].RawHash != entries[index].RawHash {
					t.Fatalf("entry %d identity/raw changed", index)
				}
			}
		})
	}
}

func TestPublishedLegacySchemaFixturesMatchTaggedSources(t *testing.T) {
	want := map[string]string{
		"3":  "c63a0cffe07bfeda3428ab303e54a6bc901bd3bc952b36d5cd150cc4893a428a",
		"4":  "b34db0bcce689a0d0e6a0fbab135226ead24be8a0bd41b1bd42e2ed2ba970601",
		"5":  "52b0d2ddf52b2a0adff6b45e8f25ae9f71f67004fdb63ba62f2d578ae09bfcf7",
		"6":  "b674e996d0c3368bd60f94f9fe0da194db1c10109627e8a056a367e9e5b14c1a",
		"7":  "d8ed27584d5e6dac9eaa35cc7c61ca00bd39536d9cfcf93865f0e8b7f44894c5",
		"8":  "219f70e0a393f93b357112757fc2b01d233d4309675b1cfbb6123cf629fed6e2",
		"9":  "94c48bed78158f2a478815db79e531d8c4a973cc729aa115f1f97e7bd4d82c2e",
		"10": "b20539109f05d0ea7f75ce6f2f52c4653ab9f3c680f8420c84030f89bb3f09f9",
		"11": schemaFingerprintVersion11,
		"12": schemaFingerprintVersion12,
		"13": schemaFingerprintVersion13,
	}
	for version, wantHash := range want {
		schema, ok := legacyPublishedSchemaSQL(version)
		if !ok || schema == "" {
			t.Fatalf("schema v%s fixture is missing", version)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256([]byte(schema))); got != wantHash {
			t.Fatalf("schema v%s fixture hash=%s, want tagged source hash %s", version, got, wantHash)
		}
	}
}

func TestSQLiteSchemaVersion13MigrationRejectsInFlightStateAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB)
	}{
		{
			name: "active lease",
			mutate: func(t *testing.T, db *sql.DB) {
				_, err := db.Exec(`INSERT INTO active_turn_leases(thread_id, turn_id, owner_id, created_at)
					VALUES('thread', 'turn', 'owner', '2026-07-24T00:00:00Z')`)
				if err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "prepared fork",
			mutate: func(t *testing.T, db *sql.DB) {
				_, err := db.Exec(`INSERT INTO fork_operations(
					operation_id, request_fingerprint, state, plan_json, created_at, updated_at
				) VALUES('fork', 'fingerprint', 'prepared', '{}', '2026-07-24T00:00:00Z', '2026-07-24T00:00:00Z')`)
				if err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unfinished turn",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`DELETE FROM entries WHERE id = 'terminal'`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE threads SET leaf_id = 'assistant' WHERE id = 'thread'`); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "root carries subagent status",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`UPDATE threads SET status = 'completed' WHERE id = 'thread'`); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "v13-in-flight.db")
			createQuiescentSchemaVersion13Store(t, path)
			db, err := sql.Open(driverName, path)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			_, err = Open(path)
			if err == nil || !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("Open error=%v, want authority corruption", err)
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
			if version != schemaVersion13 || fingerprint != schemaFingerprintVersion13 {
				t.Fatalf("failed migration metadata=%q/%q", version, fingerprint)
			}
			var threadCount int
			if err := db.QueryRow(`SELECT COUNT(*) FROM threads`).Scan(&threadCount); err != nil || threadCount != 1 {
				t.Fatalf("failed migration thread count=%d err=%v", threadCount, err)
			}
		})
	}
}

func TestSQLiteLegacySchemaMigrationRejectsActiveAuthorityAtomically(t *testing.T) {
	for version := 3; version <= 13; version++ {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("v%d-active.db", version))
			createPublishedLegacyStore(t, path, version)
			db, err := sql.Open(driverName, path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO active_turn_leases(thread_id, turn_id, owner_id, created_at)
				VALUES('thread', 'turn', 'owner', '2026-07-24T00:00:00Z')`); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := Open(path); err == nil || !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("Open schema v%d active authority error=%v", version, err)
			}
			assertLegacyMigrationRolledBack(t, path, version, "active_turn_leases")
		})
	}
}

func TestSQLiteLegacySchemaMigrationRejectsUnmappedToolArtifactsAtomically(t *testing.T) {
	for version := 5; version <= 13; version++ {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("v%d-artifact.db", version))
			createPublishedLegacyStore(t, path, version)
			db, err := sql.Open(driverName, path)
			if err != nil {
				t.Fatal(err)
			}
			hash := artifact.TextSHA256("body")
			if _, err := db.Exec(`INSERT INTO tool_output_artifacts(
				id, run_id, thread_id, turn_id, prompt_scope_id, step, call_id, tool_name,
				kind, mime, safe_label, url, size_bytes, sha256, text, created_at
			) VALUES('artifact', 'run', 'thread', 'turn', 'thread', 1, 'call', 'read',
				'text', 'text/plain', 'artifact', '', 4, ?, 'body', '2026-07-24T00:00:00Z')`, hash); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := Open(path); err == nil || !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("Open schema v%d unmapped artifact error=%v", version, err)
			}
			assertLegacyMigrationRolledBack(t, path, version, "tool_output_artifacts")
		})
	}
}

func TestSQLiteLegacySchemaMigrationPreservesUniquelyBoundArtifacts(t *testing.T) {
	for version := 5; version <= 13; version++ {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("v%d-artifact.db", version))
			createPublishedLegacyStore(t, path, version)
			ref := addSchemaVersion13Artifact(t, path, "")

			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			content, err := store.ReadArtifact(context.Background(), sessiontree.ArtifactReadRequest{
				ThreadID: "thread", ArtifactID: ref.ID,
			})
			if err != nil || content.Ref != ref || content.Text != "full durable output" {
				t.Fatalf("migrated artifact=%#v err=%v", content, err)
			}
		})
	}
}

func TestSQLiteLegacySchemaMigrationRejectsPublishedArtifactURLAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v13-artifact-url.db")
	createPublishedLegacyStore(t, path, 13)
	addSchemaVersion13Artifact(t, path, "/api/artifacts/published-artifact")

	_, err := Open(path)
	if err == nil || !errors.Is(err, sessiontree.ErrAuthorityCorrupt) || !strings.Contains(err.Error(), "legacy product URL") {
		t.Fatalf("Open legacy URL artifact error=%v", err)
	}
	assertLegacyMigrationRolledBack(t, path, 13, "tool_output_artifacts")
}

func assertLegacyMigrationRolledBack(t *testing.T, path string, version int, preservedTable string) {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var gotVersion string
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&gotVersion); err != nil {
		t.Fatal(err)
	}
	if gotVersion != fmt.Sprint(version) {
		t.Fatalf("failed migration schema version=%q, want %d", gotVersion, version)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + quoteSchemaName(preservedTable)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("failed migration changed %s count=%d", preservedTable, count)
	}
}

func TestSQLiteLegacySchemaMigrationFailureRollsBackToVersion3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v3-drift.db")
	createPublishedLegacyStore(t, path, 3)
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE unexpected_legacy_data(id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO unexpected_legacy_data(id) VALUES('preserve-me')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); err == nil {
		t.Fatal("Open drifted v3 store succeeded")
	}
	db, err = sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != "3" {
		t.Fatalf("failed migration schema version=%q, want 3", version)
	}
	if hasTitle, err := legacyColumnExists(context.Background(), db, "threads", "title"); err != nil || hasTitle {
		t.Fatalf("failed migration title column exists=%v err=%v", hasTitle, err)
	}
	var preserved, entries int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unexpected_legacy_data WHERE id = 'preserve-me'`).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM entries WHERE thread_id = 'thread'`).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if preserved != 1 || entries != 4 {
		t.Fatalf("failed migration changed data: preserved=%d entries=%d", preserved, entries)
	}
}

func createQuiescentSchemaVersion13Store(t *testing.T, path string) []sessiontree.Entry {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(legacyPublishedSchemaVersion13SQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`,
		schemaVersion13, rawEncoderVersion, schemaFingerprintVersion13); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at)
		VALUES('thread', 'terminal', ?, ?)`, formatTime(now), formatTime(now.Add(3*time.Second))); err != nil {
		t.Fatal(err)
	}
	input := []sessiontree.Entry{
		{ID: "started", ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run"}, CreatedAt: now},
		{ID: "user", ThreadID: "thread", ParentID: "started", TurnID: "turn", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "hello"}, CreatedAt: now.Add(time.Second)},
		{ID: "assistant", ThreadID: "thread", ParentID: "user", TurnID: "turn", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "world"}, CreatedAt: now.Add(2 * time.Second)},
		{ID: "terminal", ThreadID: "thread", ParentID: "assistant", TurnID: "turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, CreatedAt: now.Add(3 * time.Second)},
	}
	entries := make([]sessiontree.Entry, 0, len(input))
	for _, entry := range input {
		inserted, err := insertSchemaVersion15Entry(db, entry)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, inserted)
	}
	return entries
}

func createPublishedLegacyStore(t *testing.T, path string, version int) []sessiontree.Entry {
	t.Helper()
	if version == 13 {
		return createQuiescentSchemaVersion13Store(t, path)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema, ok := legacyPublishedSchemaSQL(fmt.Sprint(version))
	if !ok {
		t.Fatalf("schema v%d fixture is missing", version)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema v%d: %v", version, err)
	}
	if version <= 10 {
		if _, err := db.Exec(`INSERT INTO schema_meta(key, value) VALUES
			('schema_version', ?), ('raw_encoder_version', ?)`, fmt.Sprint(version), rawEncoderVersion); err != nil {
			t.Fatal(err)
		}
	} else {
		fingerprint := schemaFingerprintVersion11
		if version == 12 {
			fingerprint = schemaFingerprintVersion12
		}
		if _, err := db.Exec(`INSERT INTO schema_meta(key, value) VALUES
			('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`,
			fmt.Sprint(version), rawEncoderVersion, fingerprint); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at)
		VALUES('thread', 'terminal', ?, ?)`, formatTime(now), formatTime(now.Add(3*time.Second))); err != nil {
		t.Fatal(err)
	}
	input := []sessiontree.Entry{
		{ID: "started", ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run"}, CreatedAt: now},
		{ID: "user", ThreadID: "thread", ParentID: "started", TurnID: "turn", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "hello"}, CreatedAt: now.Add(time.Second)},
		{ID: "assistant", ThreadID: "thread", ParentID: "user", TurnID: "turn", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "world"}, CreatedAt: now.Add(2 * time.Second)},
		{ID: "terminal", ThreadID: "thread", ParentID: "assistant", TurnID: "turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, CreatedAt: now.Add(3 * time.Second)},
	}
	entries := make([]sessiontree.Entry, 0, len(input))
	for _, entry := range input {
		inserted, err := insertSchemaVersion15Entry(db, entry)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, inserted)
	}
	return entries
}

func addSchemaVersion13Child(t *testing.T, path, status string, terminal sessiontree.TurnMarkerStatus, closed bool) {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)
	closedValue := 0
	if closed {
		closedValue = 1
	}
	if _, err := db.Exec(`INSERT INTO threads(
		id, parent_thread_id, parent_turn_id, task_name, agent_path, fork_mode,
		closed, status, created_at, updated_at
	) VALUES('child', 'thread', 'turn', 'worker', '/root/worker', 'none', ?, ?, ?, ?)`,
		closedValue, status, formatTime(now), formatTime(now)); err != nil {
		t.Fatal(err)
	}
	if terminal == "" {
		return
	}
	started, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "child-started", ThreadID: "child", TurnID: "child-turn", Type: sessiontree.EntryTurnMarker,
		TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "child-run"}, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	user, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "child-user", ThreadID: "child", ParentID: started.ID, TurnID: "child-turn",
		Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "work"}, CreatedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	parentID := user.ID
	if terminal == sessiontree.TurnFailed || terminal == sessiontree.TurnAborted {
		failureMessage := "provider failed"
		if status == "cancelled" {
			failureMessage = "context canceled"
		} else if status == "interrupted" {
			failureMessage = "turn interrupted during previous process"
		}
		failure, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
			ID: "child-failure", ThreadID: "child", ParentID: parentID, TurnID: "child-turn",
			Type: sessiontree.EntryRunFailure, Error: failureMessage, CreatedAt: now.Add(2 * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
		parentID = failure.ID
	}
	metadata := map[string]string{"run_id": "child-run"}
	if status == "interrupted" {
		metadata["recoverable"] = "true"
	}
	terminalEntry, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "child-terminal", ThreadID: "child", ParentID: parentID, TurnID: "child-turn",
		Type: sessiontree.EntryTurnMarker, TurnStatus: terminal, Metadata: metadata, CreatedAt: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE threads SET leaf_id = ?, updated_at = ? WHERE id = 'child'`, terminalEntry.ID, formatTime(terminalEntry.CreatedAt)); err != nil {
		t.Fatal(err)
	}
}

func addSchemaVersion13Artifact(t *testing.T, path, legacyURL string) artifact.Ref {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE threads SET leaf_id = 'assistant' WHERE id = 'thread'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM entries WHERE id = 'terminal' AND thread_id = 'thread'`); err != nil {
		t.Fatal(err)
	}
	text := "full durable output"
	ref := artifact.Ref{
		ID: "published-artifact", SafeLabel: "read-output.log", Kind: "tool_output",
		MIME: "text/plain; charset=utf-8", SizeBytes: int64(len(text)), SHA256: artifact.TextSHA256(text),
	}
	now := time.Date(2026, 7, 24, 0, 0, 2, 500_000_000, time.UTC)
	result, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "artifact-result", ThreadID: "thread", ParentID: "assistant", TurnID: "turn",
		Type: sessiontree.EntryToolResult,
		Message: session.Message{
			Role: session.Tool, ToolCallID: "call", ToolName: "read",
			ToolResult: &session.ToolResultView{Status: "success", FullOutput: &ref},
		},
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if legacyURL != "" {
		var messageJSON, raw string
		if err := db.QueryRow(`SELECT message_json, raw FROM entries WHERE thread_id = 'thread' AND id = ?`, result.ID).Scan(&messageJSON, &raw); err != nil {
			t.Fatal(err)
		}
		needle := `"safe_label":"` + ref.SafeLabel + `"`
		replacement := needle + `,"url":"` + legacyURL + `"`
		messageJSON = strings.Replace(messageJSON, needle, replacement, 1)
		raw = strings.Replace(raw, needle, replacement, 1)
		if _, err := db.Exec(`UPDATE entries SET message_json = ?, raw = ?, raw_hash = ? WHERE thread_id = 'thread' AND id = ?`,
			messageJSON, raw, sessiontree.StableHash(raw), result.ID); err != nil {
			t.Fatal(err)
		}
	}
	terminalEntry, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "terminal", ThreadID: "thread", ParentID: result.ID, TurnID: "turn",
		Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted,
		CreatedAt: now.Add(500 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE threads SET leaf_id = ?, updated_at = ? WHERE id = 'thread'`, terminalEntry.ID, formatTime(terminalEntry.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tool_output_artifacts(
		id, run_id, thread_id, turn_id, prompt_scope_id, step, call_id, tool_name,
		kind, mime, safe_label, url, size_bytes, sha256, text, metadata_json, created_at
	) VALUES(?, 'run', 'thread', 'turn', 'thread', 1, 'call', 'read', ?, ?, ?, ?, ?, ?, ?, '{}', ?)`,
		ref.ID, ref.Kind, ref.MIME, ref.SafeLabel, legacyURL, ref.SizeBytes, ref.SHA256, text, formatTime(now)); err != nil {
		t.Fatal(err)
	}
	return ref
}
