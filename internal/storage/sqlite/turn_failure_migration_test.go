package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteMigratesSchemaVersion15TypedTurnFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "typed-failure-v15.db")
	wants := []struct {
		threadID         string
		status           sessiontree.TurnMarkerStatus
		message          string
		metadata         map[string]string
		failureCode      string
		wantStatus       sessiontree.TurnMarkerStatus
		wantLegacyStatus string
		wantRawRewrite   bool
		unknownEffect    bool
	}{
		{threadID: "cancelled-text-only", status: sessiontree.TurnAborted, message: "context canceled", failureCode: sessiontree.TurnFailureLegacyUnclassified, wantStatus: sessiontree.TurnFailed, wantLegacyStatus: string(sessiontree.TurnAborted), wantRawRewrite: true},
		{threadID: "interrupted-text-only", status: sessiontree.TurnAborted, message: sessiontree.InterruptedTurnFailureMessage, failureCode: sessiontree.TurnFailureLegacyUnclassified, wantStatus: sessiontree.TurnFailed, wantLegacyStatus: string(sessiontree.TurnAborted), wantRawRewrite: true},
		{threadID: "effect-text-only", status: sessiontree.TurnFailed, message: sessiontree.InterruptedTurnEffectOutcomeUnknownMessage, failureCode: sessiontree.TurnFailureLegacyUnclassified, wantStatus: sessiontree.TurnFailed, wantRawRewrite: true},
		{threadID: "arbitrary", status: sessiontree.TurnFailed, message: "upstream exploded in an unrecognized way", failureCode: sessiontree.TurnFailureLegacyUnclassified, wantStatus: sessiontree.TurnFailed, wantRawRewrite: true},
		{threadID: "diagnostic-only", status: sessiontree.TurnFailed, message: "read failed", metadata: map[string]string{"diagnostic": "snapshot_error"}, failureCode: sessiontree.TurnFailureLegacyUnclassified, wantStatus: sessiontree.TurnFailed, wantRawRewrite: true},
		{threadID: "explicit-provider", status: sessiontree.TurnFailed, message: sessiontree.InterruptedTurnEffectOutcomeUnknownMessage, metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider}, failureCode: sessiontree.TurnFailureProvider, wantStatus: sessiontree.TurnFailed},
		{threadID: "explicit-cancelled", status: sessiontree.TurnAborted, message: "anything", metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureCancelled}, failureCode: sessiontree.TurnFailureCancelled, wantStatus: sessiontree.TurnAborted},
		{threadID: "unknown-effect", status: sessiontree.TurnFailed, message: "arbitrary effect failure", failureCode: sessiontree.TurnFailureEffectOutcomeUnknown, wantStatus: sessiontree.TurnFailed, wantRawRewrite: true, unknownEffect: true},
		{threadID: "branch-boundary", status: sessiontree.TurnAborted, metadata: map[string]string{
			"authority_kind": "branch_boundary", "reason": "fork",
		}, failureCode: sessiontree.TurnFailureInterrupted, wantStatus: sessiontree.TurnAborted, wantRawRewrite: true},
	}
	oldRaw := map[string]string{}
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		for _, want := range wants {
			terminal := seedSchemaVersion15Terminal(t, db, want.threadID, want.status, want.message, want.metadata)
			oldRaw[want.threadID] = terminal.Raw
			if want.unknownEffect {
				seedSchemaVersion15UnknownEffect(t, db, want.threadID, terminal.TurnID)
			}
		}
	})

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatalf("reopen migrated failures: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, want := range wants {
		entry, err := store.Entry(context.Background(), want.threadID, "terminal")
		if err != nil {
			t.Fatal(err)
		}
		if entry.Metadata[sessiontree.TurnFailureCodeMetadataKey] != want.failureCode {
			t.Fatalf("thread %q failure code=%q, want %q", want.threadID, entry.Metadata[sessiontree.TurnFailureCodeMetadataKey], want.failureCode)
		}
		if entry.TurnStatus != want.wantStatus {
			t.Fatalf("thread %q status=%q, want %q", want.threadID, entry.TurnStatus, want.wantStatus)
		}
		if entry.Metadata[schemaVersion15LegacyTurnStatusMetadataKey] != want.wantLegacyStatus {
			t.Fatalf("thread %q legacy status=%q, want %q", want.threadID, entry.Metadata[schemaVersion15LegacyTurnStatusMetadataKey], want.wantLegacyStatus)
		}
		if want.threadID == "branch-boundary" && entry.Metadata["failure_reason"] != sessiontree.BranchBoundaryTurnFailureMessage {
			t.Fatalf("branch boundary failure reason=%q", entry.Metadata["failure_reason"])
		}
		if want.wantRawRewrite && entry.Raw == oldRaw[want.threadID] {
			t.Fatalf("thread %q terminal raw was not rewritten", want.threadID)
		}
		if !want.wantRawRewrite && entry.Raw != oldRaw[want.threadID] {
			t.Fatalf("thread %q explicit valid failure raw changed", want.threadID)
		}
		if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
			t.Fatalf("thread %q migrated terminal integrity: %v", want.threadID, err)
		}
	}
}

func TestSQLiteSchemaVersion15InvalidTurnFailureMigrationIsAtomic(t *testing.T) {
	for _, test := range []struct {
		name          string
		status        sessiontree.TurnMarkerStatus
		message       string
		metadata      map[string]string
		unknownEffect bool
	}{
		{name: "invalid code", status: sessiontree.TurnFailed, message: "upstream exploded", metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: "invented"}},
		{name: "status mismatch", status: sessiontree.TurnAborted, message: "cancelled", metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider}},
		{name: "structured authority conflict", status: sessiontree.TurnFailed, message: sessiontree.InterruptedTurnEffectOutcomeUnknownMessage, metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider}, unknownEffect: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ambiguous-failure-v15.db")
			var before sessiontree.Entry
			createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
				before = seedSchemaVersion15Terminal(t, db, "thread", test.status, test.message, test.metadata)
				if test.unknownEffect {
					seedSchemaVersion15UnknownEffect(t, db, "thread", before.TurnID)
				}
			})
			if _, err := Open(path); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("Open ambiguous v15 err=%v, want ErrAuthorityCorrupt", err)
			}
			db, err := sql.Open(driverName, path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var version, fingerprint, raw, rawHash, metadataJSON string
			if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_fingerprint'`).Scan(&fingerprint); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT raw, raw_hash, metadata_json FROM entries WHERE thread_id = 'thread' AND id = 'terminal'`).Scan(&raw, &rawHash, &metadataJSON); err != nil {
				t.Fatal(err)
			}
			if version != schemaVersion15 || fingerprint != schemaFingerprintVersion15 || raw != before.Raw || rawHash != before.RawHash {
				t.Fatalf("failed migration was not atomic: version=%q fingerprint=%q raw=%q hash=%q metadata=%q", version, fingerprint, raw, rawHash, metadataJSON)
			}
			var addedColumns int
			if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('threads') WHERE name IN ('title_generation', 'title_token')`).Scan(&addedColumns); err != nil {
				t.Fatal(err)
			}
			if addedColumns != 0 {
				t.Fatalf("failed migration left %d title authority columns", addedColumns)
			}
		})
	}
}

func seedSchemaVersion15UnknownEffect(t *testing.T, db *sql.DB, threadID, turnID string) {
	t.Helper()
	now := time.Date(2026, 7, 20, 12, 30, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO effect_attempts(
		effect_attempt_id, thread_id, turn_id, run_id, tool_call_id, tool_name, argument_hash,
		request_fingerprint, state, terminal_fingerprint, owner_id, generation, created_at, updated_at
	) VALUES(?, ?, ?, ?, ?, 'inspect', 'args', 'request', 'unknown', 'outcome', 'owner', 1, ?, ?)`,
		"effect-"+threadID, threadID, turnID, "run-"+threadID, "call-"+threadID, formatTime(now), formatTime(now)); err != nil {
		t.Fatal(err)
	}
}

func seedSchemaVersion15Terminal(t *testing.T, db *sql.DB, threadID string, status sessiontree.TurnMarkerStatus, failureMessage string, metadata map[string]string) sessiontree.Entry {
	t.Helper()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES(?, ?, ?)`, threadID, formatTime(now), formatTime(now)); err != nil {
		t.Fatal(err)
	}
	started, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "started", ThreadID: threadID, TurnID: "turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
		Metadata: map[string]string{"run_id": "run-" + threadID}, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	user, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "user", ThreadID: threadID, ParentID: started.ID, TurnID: "turn", Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "work"}, CreatedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	parentID := user.ID
	if failureMessage != "" {
		failure, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
			ID: "failure", ThreadID: threadID, ParentID: parentID, TurnID: "turn", Type: sessiontree.EntryRunFailure,
			Error: failureMessage, CreatedAt: now.Add(2 * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
		parentID = failure.ID
	}
	terminal, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
		ID: "terminal", ThreadID: threadID, ParentID: parentID, TurnID: "turn", Type: sessiontree.EntryTurnMarker,
		TurnStatus: status, Metadata: metadata, CreatedAt: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE threads SET leaf_id = ?, updated_at = ? WHERE id = ?`, terminal.ID, formatTime(terminal.CreatedAt), threadID); err != nil {
		t.Fatal(err)
	}
	return terminal
}

func insertSchemaVersion15Entry(db *sql.DB, entry sessiontree.Entry) (sessiontree.Entry, error) {
	entry = sessiontree.PrepareEntry(entry)
	messageJSON, err := json.Marshal(entry.Message)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	metadataJSON, err := json.Marshal(entry.Metadata)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	var ordinal int64
	if err := db.QueryRow(`SELECT COALESCE(MAX(ordinal), 0) + 1 FROM entries WHERE thread_id = ?`, entry.ThreadID).Scan(&ordinal); err != nil {
		return sessiontree.Entry{}, err
	}
	_, err = db.Exec(`INSERT INTO entries(
		thread_id, id, ordinal, parent_id, type, turn_id, created_at, message_json,
		raw, raw_hash, raw_encoder_version, turn_status, error, metadata_json
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ThreadID, entry.ID, ordinal, entry.ParentID, string(entry.Type), entry.TurnID, formatTime(entry.CreatedAt),
		string(messageJSON), entry.Raw, entry.RawHash, 1, string(entry.TurnStatus), entry.Error, string(metadataJSON))
	return entry, err
}
