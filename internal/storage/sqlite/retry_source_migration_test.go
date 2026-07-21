package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteMigratesSchemaVersion15RetrySource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry-source-v15.db")
	var sourceEntryID string
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		sourceEntryID = seedSchemaVersion15Retry(t, db, false)
	})
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	page, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 2})
	if err != nil || len(page.Turns) != 2 || page.Turns[1].RetrySource == nil ||
		page.Turns[1].RetrySource.TurnID != "turn-original" || page.Turns[1].RetrySource.EntryID != sourceEntryID {
		t.Fatalf("migrated retry page=%#v err=%v", page, err)
	}
	entries, err := store.Entries(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.ID == "retry-started" {
			if entry.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] != "turn-original" ||
				entry.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] != sourceEntryID || sessiontree.ValidateEntryIntegrity(entry) != nil {
				t.Fatalf("migrated retry started=%#v", entry)
			}
			request := sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-replay",
				RetrySourceTurnID: "turn-original", RetrySourceEntryID: sourceEntryID,
			}
			request.RequestFingerprint, err = sessiontree.TurnAdmissionRequestFingerprint(request)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := store.AdmitTurn(context.Background(), request)
			if err != nil || !replayed.Replayed || replayed.Terminal == nil || replayed.Terminal.Terminal.ID != "retry-terminal" {
				t.Fatalf("migrated retry replay=%#v err=%v", replayed, err)
			}
			return
		}
	}
	t.Fatal("migrated retry started entry is missing")
}

func TestSQLiteSchemaVersion15InvalidRetrySourceMigrationIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-retry-source-v15.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		seedSchemaVersion15Retry(t, db, true)
	})
	if _, err := Open(path); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Open invalid retry source err=%v, want ErrAuthorityCorrupt", err)
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
	var pathDepthColumns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name = 'path_depth'`).Scan(&pathDepthColumns); err != nil {
		t.Fatal(err)
	}
	var metadataJSON string
	if err := db.QueryRow(`SELECT metadata_json FROM entries WHERE thread_id = 'thread' AND id = 'retry-started'`).Scan(&metadataJSON); err != nil {
		t.Fatal(err)
	}
	var requestFingerprint string
	if err := db.QueryRow(`SELECT request_fingerprint FROM turn_admissions WHERE thread_id = 'thread' AND turn_id = 'turn-retry'`).Scan(&requestFingerprint); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion15 || pathDepthColumns != 0 || metadataJSON != `{"run_id":"run-retry"}` || requestFingerprint != "request-retry" {
		t.Fatalf("failed retry migration version=%q path_depth_columns=%d metadata=%q fingerprint=%q", version, pathDepthColumns, metadataJSON, requestFingerprint)
	}
}

func TestSQLiteSchemaVersion15RetryMigrationRejectsAdmissionRunDriftAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry-run-drift-v15.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		seedSchemaVersion15Retry(t, db, false)
		if _, err := db.Exec(`UPDATE turn_admissions SET run_id = 'run-drift'
			WHERE thread_id = 'thread' AND turn_id = 'turn-retry'`); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := Open(path); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Open retry admission run drift err=%v, want ErrAuthorityCorrupt", err)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, metadataJSON, admissionRunID, requestFingerprint string
	var pathDepthColumns int
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name = 'path_depth'`).Scan(&pathDepthColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT metadata_json FROM entries WHERE thread_id = 'thread' AND id = 'retry-started'`).Scan(&metadataJSON); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT run_id, request_fingerprint FROM turn_admissions
		WHERE thread_id = 'thread' AND turn_id = 'turn-retry'`).Scan(&admissionRunID, &requestFingerprint); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion15 || pathDepthColumns != 0 || metadataJSON != `{"run_id":"run-retry"}` ||
		admissionRunID != "run-drift" || requestFingerprint != "request-retry" {
		t.Fatalf("failed retry migration version=%q path_depth_columns=%d metadata=%q run=%q fingerprint=%q",
			version, pathDepthColumns, metadataJSON, admissionRunID, requestFingerprint)
	}
}

func TestSQLiteSchemaVersion15RetryMigrationRejectsSavePointOnAbandonedSiblingAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "abandoned-save-point-v15.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		seedSchemaVersion15RetryWithAbandonedSavePoint(t, db)
	})
	if _, err := Open(path); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Open abandoned save-point retry err=%v, want ErrAuthorityCorrupt", err)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, metadataJSON, requestFingerprint string
	var pathDepthColumns int
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('entries') WHERE name = 'path_depth'`).Scan(&pathDepthColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT metadata_json FROM entries WHERE thread_id = 'thread' AND id = 'retry-started'`).Scan(&metadataJSON); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT request_fingerprint FROM turn_admissions WHERE thread_id = 'thread' AND turn_id = 'turn-retry'`).Scan(&requestFingerprint); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion15 || pathDepthColumns != 0 || metadataJSON != `{"run_id":"run-retry"}` || requestFingerprint != "request-retry" {
		t.Fatalf("failed retry migration version=%q path_depth_columns=%d metadata=%q fingerprint=%q", version, pathDepthColumns, metadataJSON, requestFingerprint)
	}
}

func seedSchemaVersion15RetryWithAbandonedSavePoint(t *testing.T, db *sql.DB) {
	t.Helper()
	now := time.Date(2026, 7, 21, 4, 30, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES('thread', ?, ?)`, formatTime(now), formatTime(now)); err != nil {
		t.Fatal(err)
	}
	started, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "original-started", ThreadID: "thread", TurnID: "turn-original", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run-original"}, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	user, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "original-user", ThreadID: "thread", ParentID: started.ID, TurnID: "turn-original", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect"}, CreatedAt: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	source, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "source-assistant", ThreadID: "thread", ParentID: user.ID, TurnID: "turn-original", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "partial"}, CreatedAt: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "abandoned-save-point", ThreadID: "thread", ParentID: source.ID, TurnID: "turn-original", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint, CreatedAt: now.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	boundary, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "active-boundary", ThreadID: "thread", ParentID: source.ID, TurnID: "turn-original", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnAborted, Metadata: map[string]string{"authority_kind": "branch_boundary", "reason": "retry"}, CreatedAt: now.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	retryStarted, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "retry-started", ThreadID: "thread", ParentID: boundary.ID, TurnID: "turn-retry", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run-retry"}, CreatedAt: now.Add(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "retry-terminal", ThreadID: "thread", ParentID: retryStarted.ID, TurnID: "turn-retry", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, CreatedAt: now.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE threads SET leaf_id = ?, updated_at = ? WHERE id = 'thread'`, terminal.ID, formatTime(terminal.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO turn_admissions(
		thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
		acquired_at, renewed_at, expires_at, boundary_terminal_id, turn_started_id, user_message_id, base_leaf_id
	) VALUES('thread', 'turn-retry', 'run-retry', 'request-retry', 'owner-retry', 1, 0, ?, ?, ?, ?, ?, NULL, ?)`,
		formatTime(now), formatTime(now), formatTime(now.Add(time.Minute)), boundary.ID, retryStarted.ID, source.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO turn_finishes(thread_id, turn_id, run_id, generation, outcome_fingerprint, terminal_entry_id, finished_at)
		VALUES('thread', 'turn-retry', 'run-retry', 1, 'outcome-retry', ?, ?)`, terminal.ID, formatTime(terminal.CreatedAt)); err != nil {
		t.Fatal(err)
	}
}

func seedSchemaVersion15Retry(t *testing.T, db *sql.DB, invalidSource bool) string {
	t.Helper()
	now := time.Date(2026, 7, 21, 4, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES('thread', ?, ?)`, formatTime(now), formatTime(now)); err != nil {
		t.Fatal(err)
	}
	originalStarted, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "original-started", ThreadID: "thread", TurnID: "turn-original", Type: sessiontree.EntryTurnMarker,
		TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run-original"}, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	originalUser, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "original-user", ThreadID: "thread", ParentID: originalStarted.ID, TurnID: "turn-original", Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "original question"}, CreatedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	boundary, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "original-boundary", ThreadID: "thread", ParentID: originalUser.ID, TurnID: "turn-original", Type: sessiontree.EntryTurnMarker,
		TurnStatus: sessiontree.TurnAborted, Metadata: map[string]string{"authority_kind": "branch_boundary", "reason": "retry"}, CreatedAt: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	retryStarted, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "retry-started", ThreadID: "thread", ParentID: boundary.ID, TurnID: "turn-retry", Type: sessiontree.EntryTurnMarker,
		TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run-retry"}, CreatedAt: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	retryAssistant, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "retry-assistant", ThreadID: "thread", ParentID: retryStarted.ID, TurnID: "turn-retry", Type: sessiontree.EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "retry answer"}, CreatedAt: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	retryTerminal, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "retry-terminal", ThreadID: "thread", ParentID: retryAssistant.ID, TurnID: "turn-retry", Type: sessiontree.EntryTurnMarker,
		TurnStatus: sessiontree.TurnCompleted, CreatedAt: now.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE threads SET leaf_id = ?, updated_at = ? WHERE id = 'thread'`, retryTerminal.ID, formatTime(retryTerminal.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	sourceEntryID := originalUser.ID
	if invalidSource {
		sourceEntryID = "missing-source"
	}
	if _, err := db.Exec(`INSERT INTO turn_admissions(
		thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
		acquired_at, renewed_at, expires_at, boundary_terminal_id, turn_started_id, user_message_id, base_leaf_id
	) VALUES('thread', 'turn-retry', 'run-retry', 'request-retry', 'owner-retry', 1, 0, ?, ?, ?, ?, ?, NULL, ?)`,
		formatTime(now), formatTime(now), formatTime(now.Add(time.Minute)), boundary.ID, retryStarted.ID, sourceEntryID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO turn_finishes(
		thread_id, turn_id, run_id, generation, outcome_fingerprint, terminal_entry_id, finished_at
	) VALUES('thread', 'turn-retry', 'run-retry', 1, 'outcome-retry', ?, ?)`, retryTerminal.ID, formatTime(retryTerminal.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	return originalUser.ID
}
