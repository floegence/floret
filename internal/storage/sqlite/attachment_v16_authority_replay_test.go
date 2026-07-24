package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteV16LegacyAttachmentAuthorityReplaysRemainIdempotent(t *testing.T) {
	now := time.Date(2026, 7, 24, 15, 0, 0, 0, time.UTC)
	legacyMessage := sqliteLegacyOversizedAttachmentMessage()

	t.Run("subagent publication", func(t *testing.T) {
		store := newSQLiteAttachmentReplayStore(t)
		ctx := context.Background()
		if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		request := sessiontree.PublishSubAgentRequest{
			PublicationID: "publication", RequestFingerprint: "placeholder", ParentThreadID: "parent",
			ChildMeta: sessiontree.ThreadMeta{
				ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "/root/child",
				CreatedAt: now, UpdatedAt: now,
			},
			Message: session.Message{Role: session.User, Content: "placeholder"}, Now: now,
		}
		published, err := store.PublishSubAgent(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		request.Message = session.CloneMessage(legacyMessage)
		request.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "publication", request.Message)
		rewriteSQLiteLegacySubAgentInput(t, store, published.Input.SubAgentInputID, request.RequestFingerprint, request.Message,
			`UPDATE subagent_publications SET request_fingerprint = ? WHERE publication_id = ?`, request.PublicationID)
		store = reopenSQLiteAttachmentReplayStore(t, store)

		before := sqliteAuthorityTableCounts(t, store, "subagent_inputs", "subagent_publications", "threads")
		replayed, err := store.PublishSubAgent(ctx, request)
		if err != nil || !replayed.Replayed || replayed.Input.SubAgentInputID != published.Input.SubAgentInputID || len(replayed.Input.Message.Attachments) != 1 {
			t.Fatalf("legacy publication replay=%#v err=%v", replayed, err)
		}
		after := sqliteAuthorityTableCounts(t, store, "subagent_inputs", "subagent_publications", "threads")
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("legacy publication replay mutated authority: before=%v after=%v", before, after)
		}
		newRequest := request
		newRequest.PublicationID = "publication-new"
		newRequest.ChildMeta.ID = "child-new"
		newRequest.ChildMeta.AgentPath = "/root/child-new"
		newRequest.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "publication-new", newRequest.Message)
		if _, err := store.PublishSubAgent(ctx, newRequest); err == nil {
			t.Fatal("new oversized legacy publication unexpectedly succeeded")
		}
		if got := sqliteAuthorityTableCounts(t, store, "subagent_inputs", "subagent_publications", "threads"); !reflect.DeepEqual(got, before) {
			t.Fatalf("rejected publication mutated authority: before=%v after=%v", before, got)
		}
	})

	t.Run("subagent input", func(t *testing.T) {
		store := newSQLiteAttachmentReplayStore(t)
		ctx := context.Background()
		if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{
			ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "/root/child", CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		request := sessiontree.PublishSubAgentInputRequest{
			InputRequestID: "input", RequestFingerprint: "placeholder", ParentThreadID: "parent", ChildThreadID: "child",
			Message: session.Message{Role: session.User, Content: "placeholder"}, Now: now,
		}
		input, replayed, err := store.PublishSubAgentInput(ctx, request)
		if err != nil || replayed {
			t.Fatalf("seed subagent input=%#v replayed=%v err=%v", input, replayed, err)
		}
		request.Message = session.CloneMessage(legacyMessage)
		request.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "input", request.Message)
		rewriteSQLiteLegacySubAgentInput(t, store, input.SubAgentInputID, request.RequestFingerprint, request.Message,
			`UPDATE subagent_input_requests SET request_fingerprint = ? WHERE input_request_id = ?`, request.InputRequestID)
		store = reopenSQLiteAttachmentReplayStore(t, store)

		before := sqliteAuthorityTableCounts(t, store, "subagent_inputs", "subagent_input_requests")
		got, replayed, err := store.PublishSubAgentInput(ctx, request)
		if err != nil || !replayed || got.SubAgentInputID != input.SubAgentInputID || len(got.Message.Attachments) != 1 {
			t.Fatalf("legacy subagent input=%#v replayed=%v err=%v", got, replayed, err)
		}
		if after := sqliteAuthorityTableCounts(t, store, "subagent_inputs", "subagent_input_requests"); !reflect.DeepEqual(after, before) {
			t.Fatalf("legacy subagent input replay mutated authority: before=%v after=%v", before, after)
		}
		newRequest := request
		newRequest.InputRequestID = "input-new"
		newRequest.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "input-new", newRequest.Message)
		if _, _, err := store.PublishSubAgentInput(ctx, newRequest); err == nil {
			t.Fatal("new oversized legacy subagent input unexpectedly succeeded")
		}
		if after := sqliteAuthorityTableCounts(t, store, "subagent_inputs", "subagent_input_requests"); !reflect.DeepEqual(after, before) {
			t.Fatalf("rejected subagent input mutated authority: before=%v after=%v", before, after)
		}
	})

	t.Run("pending tool completion", func(t *testing.T) {
		store := newSQLiteAttachmentReplayStore(t)
		ctx := context.Background()
		request := pendingToolCompletionRequest(seedPendingToolRecovery(t, store, now), now)
		result, err := store.AdmitPendingToolCompletion(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		request.Input = session.CloneMessage(legacyMessage)
		request.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "pending-completion", request.Input)
		rewriteSQLiteLegacyPendingCompletion(t, store, result.Admission.UserMessage, request)
		store = reopenSQLiteAttachmentReplayStore(t, store)

		before := sqliteAuthorityTableCounts(t, store, "entries", "turn_admissions", "pending_tool_completions", "active_turn_leases")
		replayed, err := store.AdmitPendingToolCompletion(ctx, request)
		if err != nil || !replayed.Replayed || replayed.Admission.UserMessage.ID != result.Admission.UserMessage.ID || len(replayed.Admission.UserMessage.Message.Attachments) != 1 {
			t.Fatalf("legacy pending completion replay=%#v err=%v", replayed, err)
		}
		if after := sqliteAuthorityTableCounts(t, store, "entries", "turn_admissions", "pending_tool_completions", "active_turn_leases"); !reflect.DeepEqual(after, before) {
			t.Fatalf("legacy pending completion replay mutated authority: before=%v after=%v", before, after)
		}
		newRequest := request
		newRequest.CompletionRequestID = "completion-new"
		newRequest.ContinuationTurnID = "turn-new"
		newRequest.ContinuationRunID = "run-new"
		newRequest.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "pending-completion-new", newRequest.Input)
		if _, err := store.AdmitPendingToolCompletion(ctx, newRequest); err == nil {
			t.Fatal("new oversized legacy pending completion unexpectedly succeeded")
		}
		if after := sqliteAuthorityTableCounts(t, store, "entries", "turn_admissions", "pending_tool_completions", "active_turn_leases"); !reflect.DeepEqual(after, before) {
			t.Fatalf("rejected pending completion mutated authority: before=%v after=%v", before, after)
		}
	})

	t.Run("subagent pending tool completion", func(t *testing.T) {
		store := newSQLiteAttachmentReplayStore(t)
		ctx := context.Background()
		request := seedSubAgentPendingToolCompletion(t, store, now)
		result, err := store.PublishSubAgentPendingToolCompletion(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		request.Message = session.CloneMessage(legacyMessage)
		request.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "subagent-pending-completion", request.Message)
		rewriteSQLiteLegacySubAgentInput(t, store, result.Input.SubAgentInputID, request.RequestFingerprint, request.Message,
			`UPDATE subagent_pending_tool_completions SET request_fingerprint = ? WHERE input_request_id = ?`, request.InputRequestID)
		store = reopenSQLiteAttachmentReplayStore(t, store)

		before := sqliteAuthorityTableCounts(t, store, "entries", "subagent_inputs", "subagent_pending_tool_completions")
		replayed, err := store.PublishSubAgentPendingToolCompletion(ctx, request)
		if err != nil || !replayed.Replayed || replayed.Input.SubAgentInputID != result.Input.SubAgentInputID || len(replayed.Input.Message.Attachments) != 1 {
			t.Fatalf("legacy subagent pending completion replay=%#v err=%v", replayed, err)
		}
		if after := sqliteAuthorityTableCounts(t, store, "entries", "subagent_inputs", "subagent_pending_tool_completions"); !reflect.DeepEqual(after, before) {
			t.Fatalf("legacy subagent pending completion replay mutated authority: before=%v after=%v", before, after)
		}
		newRequest := request
		newRequest.InputRequestID = "child-completion-new"
		newRequest.RequestFingerprint = sqliteLegacyMessageFingerprint(t, "subagent-pending-completion-new", newRequest.Message)
		if _, err := store.PublishSubAgentPendingToolCompletion(ctx, newRequest); err == nil {
			t.Fatal("new oversized legacy subagent pending completion unexpectedly succeeded")
		}
		if after := sqliteAuthorityTableCounts(t, store, "entries", "subagent_inputs", "subagent_pending_tool_completions"); !reflect.DeepEqual(after, before) {
			t.Fatalf("rejected subagent pending completion mutated authority: before=%v after=%v", before, after)
		}
	})
}

func newSQLiteAttachmentReplayStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "legacy-attachment-authority-v16.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func reopenSQLiteAttachmentReplayStore(t *testing.T, store *Store) *Store {
	t.Helper()
	path := store.path
	now := store.now
	leasePolicy := store.leasePolicy
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, WithAuthorityClock(now), WithLeasePolicy(leasePolicy))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func sqliteLegacyOversizedAttachmentMessage() session.Message {
	return session.Message{Role: session.User, Attachments: []session.MessageAttachment{{
		ResourceRef: strings.Repeat("r", session.MaxMessageAttachmentResourceBytes+1),
		Name:        strings.Repeat("n", session.MaxMessageAttachmentNameRunes+1),
		MIMEType:    strings.Repeat("m", session.MaxMessageAttachmentMIMETypeBytes+1),
		SizeBytes:   session.MaxMessageAttachmentSizeBytes + 1,
	}}}
}

func sqliteLegacyMessageFingerprint(t *testing.T, scope string, message session.Message) string {
	t.Helper()
	raw, err := json.Marshal(struct {
		Scope   string          `json:"scope"`
		Message session.Message `json:"message"`
	}{Scope: scope, Message: message})
	if err != nil {
		t.Fatal(err)
	}
	return sessiontree.StableHash(string(raw))
}

func rewriteSQLiteLegacySubAgentInput(t *testing.T, store *Store, inputID, fingerprint string, message session.Message, ledgerUpdate string, ledgerID string) {
	t.Helper()
	messageJSON, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := tx.Exec(`UPDATE subagent_inputs SET message_json = ?, request_fingerprint = ? WHERE subagent_input_id = ?`, string(messageJSON), fingerprint, inputID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	} else if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		_ = tx.Rollback()
		t.Fatalf("legacy subagent input update affected %d rows: %v", affected, err)
	}
	if result, err := tx.Exec(ledgerUpdate, fingerprint, ledgerID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	} else if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		_ = tx.Rollback()
		t.Fatalf("legacy subagent ledger update affected %d rows: %v", affected, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func rewriteSQLiteLegacyPendingCompletion(t *testing.T, store *Store, user sessiontree.Entry, request sessiontree.AdmitPendingToolCompletionRequest) {
	t.Helper()
	user.Message = session.CloneMessage(request.Input)
	user = sessiontree.PrepareEntry(user)
	messageJSON, err := json.Marshal(user.Message)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	updates := []struct {
		query string
		args  []any
	}{
		{`UPDATE entries SET message_json = ?, raw = ?, raw_hash = ? WHERE thread_id = ? AND id = ?`, []any{string(messageJSON), user.Raw, user.RawHash, user.ThreadID, user.ID}},
		{`UPDATE pending_tool_completions SET request_fingerprint = ? WHERE completion_request_id = ?`, []any{request.RequestFingerprint, request.CompletionRequestID}},
		{`UPDATE turn_admissions SET request_fingerprint = ? WHERE thread_id = ? AND turn_id = ?`, []any{request.RequestFingerprint, request.Target.ThreadID, request.ContinuationTurnID}},
	}
	for _, update := range updates {
		result, err := tx.Exec(update.query, update.args...)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			_ = tx.Rollback()
			t.Fatalf("legacy pending completion update affected %d rows: %v", affected, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func sqliteAuthorityTableCounts(t *testing.T, store *Store, tables ...string) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(tables))
	for _, table := range tables {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	return counts
}
