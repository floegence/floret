package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

// This fixture is intentionally built against the v14 schema. It proves that
// an interrupted dispatch is recovered through the public authority path after
// the store has migrated and been reopened; no migration code is allowed to
// manufacture a terminal marker by itself.
func TestSQLiteMigratesV14InterruptedDispatchAndRecoversUnknownEffect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v14-interrupted-dispatch.db")
	startedAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	recoveryAt := startedAt.Add(2 * time.Hour)
	createSchemaVersion14StoreForTest(t, path, func(db *sql.DB) {
		seedV14InterruptedDispatch(t, db, startedAt)
	})

	clock := func() time.Time { return recoveryAt }
	store, err := Open(path, WithAuthorityClock(clock))
	if err != nil {
		t.Fatalf("migrate v14 store: %v", err)
	}
	version, err := store.SchemaVersion(context.Background())
	if err != nil || version != schemaVersion {
		t.Fatalf("migrated schema version=%q err=%v, want %q", version, err, schemaVersion)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path, WithAuthorityClock(clock))
	if err != nil {
		t.Fatalf("reopen migrated v14 store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	lease := sessiontree.TurnLease{
		ThreadID: "thread", Purpose: sessiontree.TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner",
		Generation: 1, Heartbeat: 0, AcquiredAt: startedAt, RenewedAt: startedAt,
		ExpiresAt: startedAt.Add(30 * time.Second),
	}
	recovered, err := store.RecoverInterruptedTurn(context.Background(), sessiontree.RecoverInterruptedTurnRequest{
		ExpectedLease: lease, Now: recoveryAt,
	})
	if err != nil {
		t.Fatalf("recover migrated interrupted turn: %v", err)
	}
	if recovered.Replayed || recovered.Status != sessiontree.TurnFailed || recovered.Failure == nil ||
		recovered.Terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] != sessiontree.TurnFailureEffectOutcomeUnknown {
		t.Fatalf("recovery result=%#v, want non-replay effect_outcome_unknown failure", recovered)
	}
	if recovered.Failure.Error != sessiontree.InterruptedTurnEffectOutcomeUnknownMessage {
		t.Fatalf("recovery failure=%q, want %q", recovered.Failure.Error, sessiontree.InterruptedTurnEffectOutcomeUnknownMessage)
	}

	var state, terminalFingerprint string
	if err := store.db.QueryRow(`SELECT state, terminal_fingerprint FROM effect_attempts WHERE effect_attempt_id = 'effect-1'`).Scan(&state, &terminalFingerprint); err != nil {
		t.Fatal(err)
	}
	if state != string(sessiontree.EffectAttemptUnknown) || terminalFingerprint == "" {
		t.Fatalf("effect state=%q fingerprint=%q, want unknown with fingerprint", state, terminalFingerprint)
	}
	replayed, err := store.RecoverInterruptedTurn(context.Background(), sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: lease})
	if err != nil || !replayed.Replayed || replayed.Terminal.ID != recovered.Terminal.ID {
		t.Fatalf("recovery replay=%#v err=%v", replayed, err)
	}
}

func seedV14InterruptedDispatch(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, lease_generation, created_at, updated_at) VALUES('thread', 'call', 1, ?, ?)`, formatTime(now), formatTime(now)); err != nil {
		t.Fatal(err)
	}
	started := sessiontree.Entry{
		ThreadID: "thread", ID: "started", Type: sessiontree.EntryTurnMarker, TurnID: "turn",
		TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run"}, CreatedAt: now,
	}
	user := sessiontree.Entry{
		ThreadID: "thread", ID: "user", ParentID: "started", Type: sessiontree.EntryUserMessage, TurnID: "turn",
		Message: session.Message{Role: session.User, Content: "work"}, CreatedAt: now.Add(time.Second),
	}
	call := sessiontree.Entry{
		ThreadID: "thread", ID: "call", ParentID: "user", Type: sessiontree.EntryToolCall, TurnID: "turn",
		Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "inspect"}, CreatedAt: now.Add(2 * time.Second),
	}
	for _, entry := range []sessiontree.Entry{started, user, call} {
		if _, err := insertSchemaVersion15Entry(db, entry); err != nil {
			t.Fatal(err)
		}
	}
	lease := sessiontree.TurnLease{
		ThreadID: "thread", Purpose: sessiontree.TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner",
		Generation: 1, Heartbeat: 0, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(30 * time.Second),
	}
	if _, err := db.Exec(`INSERT INTO turn_admissions(
		thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
		acquired_at, renewed_at, expires_at, turn_started_id, user_message_id, base_leaf_id
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"thread", "turn", "run", "admission", lease.OwnerID, lease.Generation, lease.Heartbeat,
		formatTime(lease.AcquiredAt), formatTime(lease.RenewedAt), formatTime(lease.ExpiresAt), "started", "user", "user"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO active_turn_leases(
		thread_id, purpose, turn_id, mutation_id, mutation_kind, owner_id, generation, heartbeat,
		acquired_at, renewed_at, expires_at
	) VALUES(?, 'turn', ?, '', '', ?, ?, ?, ?, ?, ?)`,
		"thread", "turn", lease.OwnerID, lease.Generation, lease.Heartbeat,
		formatTime(lease.AcquiredAt), formatTime(lease.RenewedAt), formatTime(lease.ExpiresAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO effect_attempts(
		effect_attempt_id, thread_id, turn_id, run_id, tool_call_id, tool_name, argument_hash,
		request_fingerprint, state, owner_id, generation, created_at, updated_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'dispatching', ?, ?, ?, ?)`,
		"effect-1", "thread", "turn", "run", "call", "inspect", "args", "request", lease.OwnerID,
		lease.Generation, formatTime(now.Add(2*time.Second)), formatTime(now.Add(2*time.Second))); err != nil {
		t.Fatal(err)
	}
}
