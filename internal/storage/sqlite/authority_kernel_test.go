package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteRootAuthorityPersistsTombstoneAndRequestIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "authority.db")
	now := time.Date(2026, 7, 19, 6, 0, 0, 0, time.UTC)
	store, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	req := sessiontree.CreateRootRequest{
		ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1",
		Meta: sessiontree.ThreadMeta{ID: "root"},
	}
	if created, err := store.CreateRoot(ctx, req); err != nil || created.Replayed {
		t.Fatalf("created=%#v err=%v", created, err)
	}
	newIntent := req
	newIntent.CreateIntentID = "create-root-live-conflict"
	if _, err := store.CreateRoot(ctx, newIntent); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("new intent for live thread err=%v, want ErrRequestConflict", err)
	}
	if _, err := store.DeleteRootTree(ctx, "root"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	tombstone, err := reopened.ThreadTombstone(ctx, "root")
	if err != nil || tombstone.CreateIntentID != "create-root" || !tombstone.DeletedAt.Equal(now) {
		t.Fatalf("tombstone=%#v err=%v", tombstone, err)
	}
	if _, err := reopened.CreateRoot(ctx, req); !errors.Is(err, sessiontree.ErrThreadDeleted) {
		t.Fatalf("create replay after reopen err=%v, want ErrThreadDeleted", err)
	}
	newIntent.CreateIntentID = "create-root-after-delete"
	if _, err := reopened.CreateRoot(ctx, newIntent); !errors.Is(err, sessiontree.ErrThreadDeleted) {
		t.Fatalf("new create intent after delete err=%v, want ErrThreadDeleted", err)
	}
	deleted, err := reopened.DeleteRootTree(ctx, "root")
	if err != nil || !deleted.Replayed || len(deleted.ThreadIDs) != 1 || deleted.ThreadIDs[0] != "root" {
		t.Fatalf("delete replay=%#v err=%v", deleted, err)
	}
}

func TestSQLiteAuthorityInspectionRejectsLeaseLedgerAndLifecycleCorruption(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 20, 11, 30, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "inspection-corruption.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, meta := range []sessiontree.ThreadMeta{
		{ID: "root"},
		{ID: "other"},
		{ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "/root/child"},
	} {
		if _, err := store.CreateThread(ctx, meta); err != nil {
			t.Fatal(err)
		}
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "child", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.InspectSubAgentThreadAuthority(ctx, "other", "child"); !errors.Is(err, sessiontree.ErrSubAgentNotFound) {
		t.Fatalf("foreign child err=%v, want ErrSubAgentNotFound", err)
	}
	if _, err := store.InspectSubAgentThreadAuthority(ctx, "root", "missing"); !errors.Is(err, sessiontree.ErrSubAgentNotFound) {
		t.Fatalf("missing child err=%v, want ErrSubAgentNotFound", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE threads SET lease_generation = ? WHERE id = 'child'`, admitted.Lease.Generation+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InspectThreadAuthority(ctx, "child"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("lease generation mismatch err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE threads SET lease_generation = ?, lifecycle = 'closed' WHERE id = 'child'`, admitted.Lease.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InspectThreadAuthority(ctx, "child"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("closed child with lease err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLiteAuthorityInspectionRemainsReadableDuringWriterReservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inspection-read-concurrency.db")
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := writer.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	reserved := make(chan struct{})
	release := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writer.withImmediate(ctx, func(sqlRunner) error {
			close(reserved)
			<-release
			return nil
		})
	}()
	<-reserved
	readDone := make(chan error, 1)
	go func() {
		_, err := reader.InspectThreadAuthority(ctx, "thread")
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("authority inspection waited for an unrelated writer reservation")
	}
	close(release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteRootCreateReplayRejectsNonCanonicalLiveShape(t *testing.T) {
	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "closed root", query: `UPDATE threads SET lifecycle = 'closed' WHERE id = 'root'`},
		{name: "child ownership", query: `UPDATE threads SET parent_thread_id = 'parent', parent_turn_id = 'parent-turn', task_name = 'child', agent_path = 'child' WHERE id = 'root'`},
		{name: "fork provenance", query: `UPDATE threads SET forked_from_thread_id = 'source', forked_from_entry_id = 'source-entry' WHERE id = 'root'`},
		{name: "malformed fork operation", query: `UPDATE threads SET fork_operation_id = 'operation' WHERE id = 'root'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := Open(filepath.Join(t.TempDir(), "root-replay.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
				t.Fatal(err)
			}
			req := sessiontree.CreateRootRequest{
				ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1", Meta: sessiontree.ThreadMeta{ID: "root"},
			}
			if _, err := store.CreateRoot(ctx, req); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(ctx, test.query); err != nil {
				t.Fatal(err)
			}

			if _, err := store.CreateRoot(ctx, req); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("CreateRoot replay err=%v, want ErrRequestConflict", err)
			}
		})
	}
}

func TestSQLiteRootDeleteRejectsCreateLedgerReadFailureWithoutMutation(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "missing-create-provenance.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateRoot(ctx, sessiontree.CreateRootRequest{
		ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1", Meta: sessiontree.ThreadMeta{ID: "root"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `DROP TABLE root_create_intents`); err != nil {
		t.Fatal(err)
	}

	if _, err := store.DeleteRootTree(ctx, "root"); err == nil {
		t.Fatal("DeleteRootTree succeeded after create ledger read failure")
	}
	if got := sqliteTableRowCount(t, store, "threads"); got != 1 {
		t.Fatalf("thread rows after rejected delete = %d, want 1", got)
	}
	if got := sqliteTableRowCount(t, store, "thread_tombstones"); got != 0 {
		t.Fatalf("tombstones after rejected delete = %d, want 0", got)
	}
}

func TestSQLiteRootDeleteClearsQueryableAuthorityAndRetainsRequestIdentity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 6, 30, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "delete-ledgers.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateRoot(ctx, sessiontree.CreateRootRequest{
		ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1", Meta: sessiontree.ThreadMeta{ID: "root"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "root", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "child",
	}); err != nil {
		t.Fatal(err)
	}
	rootEntry, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "root", Type: sessiontree.EntryCustom, Message: session.Message{Role: session.Assistant, Content: "root"},
	}, sessiontree.AppendOptions{ID: "root-entry", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	childEntry, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "child", Type: sessiontree.EntryCustom, Message: session.Message{Role: session.Assistant, Content: "child"},
	}, sessiontree.AppendOptions{ID: "child-entry", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	timestamp := formatTime(now)
	if err := store.withImmediate(ctx, func(tx sqlRunner) error {
		statements := []struct {
			query string
			args  []any
		}{
			{`UPDATE threads SET lease_generation = 7 WHERE id = 'root'`, nil},
			{`UPDATE threads SET lifecycle = 'closed' WHERE id = 'child'`, nil},
			{`INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json)
				VALUES('host.settings', 'root', ?, ?, '{"model":"host-owned"}')`, []any{timestamp, timestamp}},
			{`INSERT INTO agent_todo_states(thread_id, version, items_json, updated_at) VALUES('root', 1, '[]', ?)`, []any{timestamp}},
			{`INSERT INTO provider_states(thread_id, leaf_entry_id, compatibility_key, state_json, created_by_run_id, created_by_turn_id, updated_at)
				VALUES('root', ?, 'compatibility', '{"kind":"fake","id":"state"}', 'run', 'turn', ?)`, []any{rootEntry.ID, timestamp}},
			{`INSERT INTO turn_admissions(thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
				acquired_at, renewed_at, expires_at, turn_started_id, user_message_id, base_leaf_id)
				VALUES('root', 'turn', 'run', 'admit', 'owner', 1, 0, ?, ?, ?, ?, ?, '')`, []any{timestamp, timestamp, formatTime(now.Add(time.Hour)), rootEntry.ID, rootEntry.ID}},
			{`INSERT INTO turn_finishes(thread_id, turn_id, run_id, generation, outcome_fingerprint, terminal_entry_id, finished_at)
				VALUES('root', 'turn', 'run', 1, 'finish', ?, ?)`, []any{rootEntry.ID, timestamp}},
			{`INSERT INTO effect_attempts(effect_attempt_id, thread_id, turn_id, run_id, tool_call_id, tool_name, argument_hash,
				request_fingerprint, state, terminal_fingerprint, result_entry_id, owner_id, generation, created_at, updated_at)
				VALUES('effect', 'root', 'turn', 'run', 'call', 'tool', 'arguments', 'effect-request', 'completed', 'effect-outcome', ?, 'owner', 1, ?, ?)`, []any{rootEntry.ID, timestamp, timestamp}},
			{`INSERT INTO pending_tool_completions(completion_request_id, request_fingerprint, thread_id, target_turn_id,
				target_run_id, target_tool_call_id, target_tool_name, target_handle, target_effect_attempt_id, settlement_fingerprint, settlement_entry_id,
				continuation_turn_id, continuation_run_id, turn_started_id, user_message_id, base_leaf_id, created_at)
				VALUES('root-completion', 'completion', 'root', 'turn', 'run', 'call', 'tool', 'tool:root', '', 'settlement', ?,
				'continuation-turn', 'continuation-run', ?, ?, ?, ?)`, []any{rootEntry.ID, rootEntry.ID, rootEntry.ID, rootEntry.ID, timestamp}},
			{`INSERT INTO compaction_operations(request_id, thread_id, request_fingerprint, source, source_leaf_id, active_path_hash,
				summary_schema_version, prompt_identity, request_payload_hash, state, error_code, error_message, outcome_fingerprint,
				finished_owner_id, finished_generation, created_at, updated_at, finished_at)
				VALUES('compaction', 'root', 'compaction-request', 'host', ?, 'path', '1', 'prompt', 'payload', 'failed',
				'provider_error', 'failed', 'compaction-outcome', 'owner', 1, ?, ?, ?)`, []any{rootEntry.ID, timestamp, timestamp, timestamp}},
			{`INSERT INTO subagent_inputs(subagent_input_id, parent_thread_id, child_thread_id, request_kind, request_id,
				request_fingerprint, sequence, state, message_json, cancelled_at, created_at)
				VALUES('input-row', 'root', 'child', 'publication', 'publication-retained', 'publication-fingerprint', 1,
				'cancelled', '{"role":"user","content":"queued"}', ?, ?)`, []any{timestamp, timestamp}},
			{`INSERT INTO subagent_publications(publication_id, parent_thread_id, child_thread_id, request_fingerprint, subagent_input_id, created_at)
				VALUES('publication-retained', 'root', 'child', 'publication-fingerprint', 'input-row', ?)`, []any{timestamp}},
			{`INSERT INTO subagent_input_requests(input_request_id, parent_thread_id, child_thread_id, request_fingerprint, subagent_input_id, created_at)
				VALUES('input-retained', 'root', 'child', 'input-fingerprint', 'input-row', ?)`, []any{timestamp}},
			{`INSERT INTO subagent_pending_tool_completions(input_request_id, request_fingerprint, settlement_fingerprint,
				parent_thread_id, child_thread_id, target_turn_id, target_run_id, target_tool_call_id, target_tool_name,
				target_handle, target_effect_attempt_id, settlement_entry_id, subagent_input_id, created_at)
				VALUES('completion-retained', 'completion-fingerprint', 'settlement-fingerprint', 'root', 'child',
				'child-turn', 'child-run', 'child-call', 'tool', 'tool:child', '', ?, 'input-row', ?)`, []any{childEntry.ID, timestamp}},
			{`INSERT INTO subagent_close_operations(close_operation_id, parent_thread_id, target_thread_id, reason,
				intent_fingerprint, request_fingerprint, state, nodes_json, result_entry_ids_json, prepared_at, finished_at)
				VALUES('close', 'root', 'child', 'done', 'close-intent', 'close-request', 'completed',
				'[{"thread_id":"child","was_open":true}]', '["child-entry"]', ?, ?)`, []any{timestamp, timestamp}},
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := store.DeleteRootTree(ctx, "root"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{
		"threads", "entries", "agent_todo_states", "provider_states", "active_turn_leases", "turn_admissions", "turn_finishes",
		"effect_attempts", "pending_tool_completions", "compaction_operations", "subagent_inputs", "subagent_close_operations",
	} {
		if got := sqliteTableRowCount(t, store, table); got != 0 {
			t.Fatalf("%s rows after root delete = %d, want 0", table, got)
		}
	}
	for table, want := range map[string]int{
		"root_create_intents": 1, "thread_tombstones": 2, "subagent_publications": 1,
		"subagent_input_requests": 1, "subagent_pending_tool_completions": 1, "metadata_records": 1,
	} {
		if got := sqliteTableRowCount(t, store, table); got != want {
			t.Fatalf("%s retained rows after root delete = %d, want %d", table, got, want)
		}
	}

	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "other-root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "other-child", ParentThreadID: "other-root", ParentTurnID: "other-turn", TaskName: "other", AgentPath: "other",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishSubAgent(ctx, sessiontree.PublishSubAgentRequest{
		PublicationID: "publication-retained", RequestFingerprint: "new-publication", ParentThreadID: "other-root",
		ChildMeta: sessiontree.ThreadMeta{ID: "new-child", ParentThreadID: "other-root"}, Message: session.Message{Role: session.User, Content: "start"},
	}); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("reused publication identity err=%v, want ErrRequestConflict", err)
	}
	if _, _, err := store.PublishSubAgentInput(ctx, sessiontree.PublishSubAgentInputRequest{
		InputRequestID: "input-retained", RequestFingerprint: "new-input", ParentThreadID: "other-root", ChildThreadID: "other-child",
		Message: session.Message{Role: session.User, Content: "continue"},
	}); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("reused input identity err=%v, want ErrRequestConflict", err)
	}
	target := sessiontree.PendingToolSettlementTarget{
		ThreadID: "other-child", TurnID: "other-turn", RunID: "other-run", ToolCallID: "other-call", ToolName: "tool", Handle: "tool:other",
	}
	if _, err := store.PublishSubAgentPendingToolCompletion(ctx, sessiontree.PublishSubAgentPendingToolCompletionRequest{
		InputRequestID: "completion-retained", RequestFingerprint: "new-completion", SettlementFingerprint: "new-settlement",
		ParentThreadID: "other-root", ChildThreadID: "other-child", Target: target,
		Settlement: sessiontree.Entry{ThreadID: "other-child", TurnID: "other-turn", Type: sessiontree.EntryCustom,
			Message:  session.Message{Role: session.Tool, ToolCallID: "other-call", ToolName: "tool", Content: "done"},
			Metadata: map[string]string{sessiontree.PendingToolSettlementKindKey: sessiontree.PendingToolSettlementKind, sessiontree.PendingToolSettlementFingerprintKey: "new-settlement"}},
		Message: session.Message{Role: session.User, Content: "continue"}, Now: now,
	}); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("reused pending completion identity err=%v, want ErrRequestConflict", err)
	}
}

func sqliteTableRowCount(t *testing.T, store *Store, table string) int {
	t.Helper()
	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+quoteSchemaName(table)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
