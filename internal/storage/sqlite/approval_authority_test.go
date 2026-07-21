package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

type approvalAuthorityTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	DeleteThread(context.Context, string) error
	AdmitTurn(context.Context, sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error)
	Entries(context.Context, string) ([]sessiontree.Entry, error)
	ActiveTurnLease(context.Context, string) (sessiontree.TurnLease, bool, error)
	FinishTurn(context.Context, sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error)
	Append(context.Context, sessiontree.Entry, sessiontree.AppendOptions) (sessiontree.Entry, error)
	ReleaseTurnLease(context.Context, sessiontree.TurnLease) error
	PrepareApprovalBatch(context.Context, sessiontree.PrepareApprovalBatchRequest) (sessiontree.PrepareApprovalBatchResult, error)
	ReadApprovalQueue(context.Context, string) (sessiontree.ApprovalQueue, error)
	Approval(context.Context, string) (sessiontree.ApprovalRecord, error)
	WaitApprovalDecision(context.Context, string) (sessiontree.WaitApprovalDecisionResult, error)
	ResolveApproval(context.Context, sessiontree.ResolveApprovalRequest) (sessiontree.ResolveApprovalResult, error)
	CommitApprovalDispatch(context.Context, sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error)
	FinalizeApproval(context.Context, sessiontree.FinalizeApprovalRequest) (sessiontree.FinalizeApprovalResult, error)
	CancelApprovalBatch(context.Context, sessiontree.CancelApprovalBatchRequest) (sessiontree.CancelApprovalBatchResult, error)
}

func TestSQLiteFinalizeApprovalRejectsAuthorityTamperingWithoutMutation(t *testing.T) {
	now := time.Date(2026, 7, 21, 13, 0, 0, 0, time.UTC)
	type authoritySnapshot struct {
		approval [][]string
		effect   [][]string
		queue    [][]string
		decision [][]string
		thread   [][]string
		entries  [][]string
	}
	setup := func(t *testing.T, terminal bool) (*Store, sessiontree.FinalizeApprovalRequest, func() authoritySnapshot) {
		t.Helper()
		store, err := Open(filepath.Join(t.TempDir(), "finalize-tamper.db"), WithAuthorityClock(func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		ctx := context.Background()
		lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
		prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
		if err != nil {
			t.Fatal(err)
		}
		record := prepared.Approvals[0]
		submitted, err := store.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
			DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
			ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: record.Revision,
			Decision: sessiontree.ApprovalDecisionApprove, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		req := sessiontree.FinalizeApprovalRequest{
			ResolutionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
			ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
			State: sessiontree.ApprovalFailed, Reason: sessiontree.ApprovalReasonAuthorizationUnavailable,
			FinalizedEntry: approvalFinalizedTestEntry("decision", record.Identity(), sessiontree.ApprovalFailed, sessiontree.ApprovalReasonAuthorizationUnavailable),
			Now:            now,
		}
		if terminal {
			if _, err := store.FinalizeApproval(ctx, req); err != nil {
				t.Fatal(err)
			}
		}
		snapshot := func() authoritySnapshot {
			t.Helper()
			return authoritySnapshot{
				approval: sqliteAuthorityRows(t, store.db, `SELECT * FROM approval_requests WHERE approval_id = ?`, record.ApprovalID),
				effect:   sqliteAuthorityRows(t, store.db, `SELECT * FROM effect_attempts WHERE effect_attempt_id = ?`, record.EffectAttemptID),
				queue:    sqliteAuthorityRows(t, store.db, `SELECT * FROM approval_queues WHERE root_thread_id = ?`, record.RootThreadID),
				decision: sqliteAuthorityRows(t, store.db, `SELECT * FROM approval_decisions WHERE decision_id = ?`, "decision"),
				thread:   sqliteAuthorityRows(t, store.db, `SELECT * FROM threads WHERE id = ?`, record.ThreadID),
				entries:  sqliteAuthorityRows(t, store.db, `SELECT * FROM entries WHERE thread_id = ? ORDER BY ordinal`, record.ThreadID),
			}
		}
		return store, req, snapshot
	}
	tests := []struct {
		name     string
		terminal bool
		mutate   func(context.Context, *Store, sessiontree.FinalizeApprovalRequest) error
	}{
		{name: "first/record identity", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_requests SET tool_name = 'tampered' WHERE approval_id = ?`, req.ExpectedCurrent.ApprovalID)
			return err
		}},
		{name: "first/record revision", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_requests SET revision = revision + 1 WHERE approval_id = ?`, req.ExpectedCurrent.ApprovalID)
			return err
		}},
		{name: "first/effect invocation", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET tool_name = 'tampered' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "first/effect state", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET state = 'cancelled', terminal_fingerprint = 'tampered' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "first/effect request fingerprint", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET request_fingerprint = '' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "first/effect rejection code", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET state = 'rejected', rejection_code = 'tampered', terminal_fingerprint = 'tampered' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "first/effect updated at", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET updated_at = ? WHERE effect_attempt_id = ?`, now.Add(time.Second).Format(time.RFC3339Nano), req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "first/effect owner", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET owner_id = '' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "first/queue revision", mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_queues SET revision = 0 WHERE root_thread_id = 'thread'`)
			return err
		}},
		{name: "first/queue current", mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_queues SET current_approval_id = '' WHERE root_thread_id = 'thread'`)
			return err
		}},
		{name: "first/receipt state", mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET receipt_state = 'failed' WHERE decision_id = 'decision'`)
			return err
		}},
		{name: "first/receipt reason", mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET reason = 'tampered' WHERE decision_id = 'decision'`)
			return err
		}},
		{name: "first/receipt revisions", mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET queue_revision = queue_revision + 1, approval_revision = approval_revision + 1 WHERE decision_id = 'decision'`)
			return err
		}},
		{name: "first/receipt submitted at", mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET submitted_at = ? WHERE decision_id = 'decision'`, now.Add(time.Second).Format(time.RFC3339Nano))
			return err
		}},
		{name: "first/receipt resolved at", mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET resolved_at = ? WHERE decision_id = 'decision'`, now.Format(time.RFC3339Nano))
			return err
		}},
		{name: "first/requested entry", mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE entries SET raw_hash = ? WHERE thread_id = 'thread' AND id = ?`,
				sessiontree.StableHash("tampered"), sessiontree.ApprovalRequestedEntryID(req.ExpectedCurrent.ApprovalID))
			return err
		}},
		{name: "replay/record identity", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_requests SET tool_name = 'tampered' WHERE approval_id = ?`, req.ExpectedCurrent.ApprovalID)
			return err
		}},
		{name: "replay/record revision", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_requests SET revision = revision + 1 WHERE approval_id = ?`, req.ExpectedCurrent.ApprovalID)
			return err
		}},
		{name: "replay/effect invocation", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET tool_name = 'tampered' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "replay/effect state", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET state = 'cancelled', rejection_code = '' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "replay/effect request fingerprint", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET request_fingerprint = '' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "replay/effect terminal fingerprint", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET terminal_fingerprint = ? WHERE effect_attempt_id = ?`,
				sessiontree.StableHash("tampered"), req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "replay/effect rejection code", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET rejection_code = 'tampered' WHERE effect_attempt_id = ?`, req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "replay/effect updated at", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET updated_at = ? WHERE effect_attempt_id = ?`, now.Add(time.Second).Format(time.RFC3339Nano), req.ExpectedCurrent.EffectAttemptID)
			return err
		}},
		{name: "replay/queue revision", terminal: true, mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_queues SET revision = 0 WHERE root_thread_id = 'thread'`)
			return err
		}},
		{name: "replay/queue current", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_queues SET current_approval_id = ? WHERE root_thread_id = 'thread'`, req.ExpectedCurrent.ApprovalID)
			return err
		}},
		{name: "replay/receipt state", terminal: true, mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET receipt_state = 'cancelled' WHERE decision_id = 'decision'`)
			return err
		}},
		{name: "replay/receipt reason", terminal: true, mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET reason = 'tampered' WHERE decision_id = 'decision'`)
			return err
		}},
		{name: "replay/receipt revisions", terminal: true, mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET queue_revision = queue_revision + 1, approval_revision = approval_revision + 1 WHERE decision_id = 'decision'`)
			return err
		}},
		{name: "replay/receipt submitted at", terminal: true, mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET submitted_at = ? WHERE decision_id = 'decision'`, now.Add(time.Second).Format(time.RFC3339Nano))
			return err
		}},
		{name: "replay/receipt resolved at", terminal: true, mutate: func(ctx context.Context, store *Store, _ sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE approval_decisions SET resolved_at = ? WHERE decision_id = 'decision'`, now.Add(time.Second).Format(time.RFC3339Nano))
			return err
		}},
		{name: "replay/finalized entry", terminal: true, mutate: func(ctx context.Context, store *Store, req sessiontree.FinalizeApprovalRequest) error {
			_, err := store.db.ExecContext(ctx, `UPDATE entries SET raw_hash = ? WHERE thread_id = 'thread' AND id = ?`,
				sessiontree.StableHash("tampered"), req.FinalizedEntry.ID)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, req, snapshot := setup(t, test.terminal)
			ctx := context.Background()
			if err := test.mutate(ctx, store, req); err != nil {
				t.Fatal(err)
			}
			before := snapshot()
			if _, err := store.FinalizeApproval(ctx, req); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("FinalizeApproval err=%v, want ErrAuthorityCorrupt", err)
			}
			if after := snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("failed finalization mutated authority:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func sqliteAuthorityRows(t *testing.T, db *sql.DB, query string, args ...any) [][]string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]string
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		serialized := make([]string, len(values))
		for index, value := range values {
			switch value := value.(type) {
			case []byte:
				serialized[index] = "bytes:" + string(value)
			default:
				serialized[index] = fmt.Sprintf("%T:%v", value, value)
			}
		}
		result = append(result, serialized)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFinishTurnRejectsVisibleApprovalAcrossAuthorities(t *testing.T) {
	now := time.Date(2026, 7, 21, 11, 10, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now, "run"))
			if err != nil {
				t.Fatal(err)
			}
			beforeEntries, err := repo.Entries(context.Background(), "thread")
			if err != nil {
				t.Fatal(err)
			}

			_, err = repo.FinishTurn(context.Background(), sessiontree.FinishTurnRequest{
				Lease: lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
				OutcomeFingerprint: "finish", Now: now,
			})
			if !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("FinishTurn err=%v, want ErrRequestConflict", err)
			}
			afterEntries, err := repo.Entries(context.Background(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			if len(afterEntries) != len(beforeEntries) {
				t.Fatalf("rejected finish appended entries: before=%d after=%d", len(beforeEntries), len(afterEntries))
			}
			for index := range beforeEntries {
				if afterEntries[index].ID != beforeEntries[index].ID || afterEntries[index].RawHash != beforeEntries[index].RawHash {
					t.Fatalf("rejected finish changed entry %d: before=%#v after=%#v", index, beforeEntries[index], afterEntries[index])
				}
			}
			active, ok, err := repo.ActiveTurnLease(context.Background(), "thread")
			if err != nil || !ok || !sessiontree.SameTurnLease(active, lease) {
				t.Fatalf("rejected finish lease=%#v ok=%v err=%v", active, ok, err)
			}
			queue, err := repo.ReadApprovalQueue(context.Background(), "thread")
			if err != nil || len(queue.Items) != 1 || queue.Items[0].ApprovalID != prepared.Approvals[0].ApprovalID ||
				queue.Items[0].State != sessiontree.ApprovalRequested {
				t.Fatalf("rejected finish queue=%#v err=%v", queue, err)
			}
			approval, err := repo.Approval(context.Background(), prepared.Approvals[0].ApprovalID)
			if err != nil || approval.State != sessiontree.ApprovalRequested {
				t.Fatalf("rejected finish approval=%#v err=%v", approval, err)
			}
		})
	}
}

func TestSQLiteFinishTurnReplayRejectsVisibleApprovalCorruption(t *testing.T) {
	now := time.Date(2026, 7, 21, 11, 30, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "finish-replay-approval.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now, "run"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CancelApprovalBatch(ctx, sessiontree.CancelApprovalBatchRequest{
		Lease: lease, RunID: "run", CancellationFingerprint: "cancel", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	finishRequest := sessiontree.FinishTurnRequest{
		Lease: lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}
	if _, err := store.FinishTurn(ctx, finishRequest); err != nil {
		t.Fatal(err)
	}
	approvalID := prepared.Approvals[0].ApprovalID
	if _, err := store.db.ExecContext(ctx, `UPDATE approval_requests
		SET state = 'requested', revision = 1, decision_id = '', reason = '', authorization_proof_hash = '', resolved_at = ''
		WHERE approval_id = ?`, approvalID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE approval_queues
		SET current_approval_id = ? WHERE root_thread_id = ?`, approvalID, prepared.Queue.RootThreadID); err != nil {
		t.Fatal(err)
	}
	beforeEntries, err := store.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.FinishTurn(ctx, finishRequest); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("FinishTurn replay err=%v, want ErrAuthorityCorrupt", err)
	}
	afterEntries, err := store.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(afterEntries) != len(beforeEntries) {
		t.Fatalf("rejected finish replay appended entries: before=%d after=%d", len(beforeEntries), len(afterEntries))
	}
	var state, current string
	if err := store.db.QueryRowContext(ctx, `SELECT state FROM approval_requests WHERE approval_id = ?`, approvalID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT current_approval_id FROM approval_queues WHERE root_thread_id = ?`, prepared.Queue.RootThreadID).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if state != "requested" || current != approvalID {
		t.Fatalf("rejected finish replay changed corruption proof: state=%q current=%q", state, current)
	}
}

func TestWaitApprovalDecisionReadsCommittedDecision(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 55, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name+"/decision-before-waiter", func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			resolved, err := repo.ResolveApproval(context.Background(), sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
				ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			waited, err := repo.WaitApprovalDecision(context.Background(), record.ApprovalID)
			if err != nil || waited.Approval.State != sessiontree.ApprovalDecisionSubmitted ||
				waited.Approval.DecisionID != resolved.Approval.DecisionID || waited.Queue.Revision < resolved.Queue.Revision {
				t.Fatalf("waited=%#v err=%v", waited, err)
			}
		})

		t.Run(name+"/context-cancel", func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := repo.WaitApprovalDecision(ctx, prepared.Approvals[0].ApprovalID); !errors.Is(err, context.Canceled) {
				t.Fatalf("WaitApprovalDecision err=%v, want context.Canceled", err)
			}
		})
	}
}

func TestSQLiteWaitApprovalDecisionNotifiesSubscribedWaiter(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 56, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "approval-wait.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.db.SetMaxOpenConns(1)
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	done := make(chan error, 1)
	go func() {
		waited, err := store.WaitApprovalDecision(ctx, record.ApprovalID)
		if err == nil && waited.Approval.State != sessiontree.ApprovalDecisionSubmitted {
			err = sessiontree.ErrAuthorityCorrupt
		}
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		store.approvalSignalMu.Lock()
		_, subscribed := store.approvalSignals[record.ApprovalID]
		store.approvalSignalMu.Unlock()
		if subscribed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("approval waiter did not subscribe")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := store.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
		ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("approval waiter was not notified")
	}
}

func TestSQLiteFinalizeApprovalNotifiesSubscribedWaiter(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 57, 30, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "approval-wait-finalize.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.db.SetMaxOpenConns(1)
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	done := waitSQLiteApprovalAsync(store, record.ApprovalID)
	waitForSQLiteApprovalSubscription(t, store, record.ApprovalID)
	if _, err := store.FinalizeApproval(ctx, sessiontree.FinalizeApprovalRequest{
		ResolutionID: "timeout", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: record.Revision,
		State: sessiontree.ApprovalTimedOut, Reason: sessiontree.ApprovalReasonTimedOut, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	waited := receiveSQLiteApprovalWait(t, done)
	if waited.err != nil || waited.result.Approval.State != sessiontree.ApprovalTimedOut ||
		waited.result.Receipt.Decision != sessiontree.ApprovalDecisionApprove || waited.result.Receipt.Reason != sessiontree.ApprovalReasonTimedOut {
		t.Fatalf("waited=%#v err=%v", waited.result, waited.err)
	}
}

func TestSQLiteWaitApprovalDecisionObservesCrossStoreCommits(t *testing.T) {
	t.Run("resolve", func(t *testing.T) {
		now := time.Date(2026, 7, 21, 8, 58, 0, 0, time.UTC)
		path := filepath.Join(t.TempDir(), "approval-cross-resolve.db")
		writer, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		ctx := context.Background()
		lease := seedApprovalTurn(t, writer, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
		prepared, err := writer.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
		if err != nil {
			t.Fatal(err)
		}
		observer, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		defer observer.Close()
		done := waitSQLiteApprovalAsync(observer, prepared.Approvals[0].ApprovalID)
		waitForSQLiteApprovalSubscription(t, observer, prepared.Approvals[0].ApprovalID)
		record := prepared.Approvals[0]
		if _, err := writer.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
			DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
			ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
			ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		waited := receiveSQLiteApprovalWait(t, done)
		if waited.err != nil || waited.result.Approval.State != sessiontree.ApprovalDecisionSubmitted ||
			waited.result.Receipt.Decision != sessiontree.ApprovalDecisionApprove {
			t.Fatalf("waited=%#v err=%v", waited.result, waited.err)
		}
	})

	t.Run("finalize", func(t *testing.T) {
		now := time.Date(2026, 7, 21, 8, 59, 0, 0, time.UTC)
		path := filepath.Join(t.TempDir(), "approval-cross-finalize.db")
		writer, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		ctx := context.Background()
		lease := seedApprovalTurn(t, writer, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
		prepared, err := writer.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
		if err != nil {
			t.Fatal(err)
		}
		observer, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		defer observer.Close()
		record := prepared.Approvals[0]
		done := waitSQLiteApprovalAsync(observer, record.ApprovalID)
		waitForSQLiteApprovalSubscription(t, observer, record.ApprovalID)
		if _, err := writer.FinalizeApproval(ctx, sessiontree.FinalizeApprovalRequest{
			ResolutionID: "timeout", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
			ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: record.Revision,
			State: sessiontree.ApprovalTimedOut, Reason: sessiontree.ApprovalReasonTimedOut, Now: now.Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		waited := receiveSQLiteApprovalWait(t, done)
		if waited.err != nil || waited.result.Approval.State != sessiontree.ApprovalTimedOut ||
			waited.result.Receipt.Decision != sessiontree.ApprovalDecisionApprove || waited.result.Receipt.Reason != sessiontree.ApprovalReasonTimedOut {
			t.Fatalf("waited=%#v err=%v", waited.result, waited.err)
		}
	})

	t.Run("recovery", func(t *testing.T) {
		initial := time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)
		var clock atomic.Int64
		clock.Store(initial.UnixNano())
		now := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
		policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
		path := filepath.Join(t.TempDir(), "approval-cross-recovery.db")
		writer, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(now))
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		ctx := context.Background()
		lease := seedApprovalTurn(t, writer, initial, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
		prepared, err := writer.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, initial))
		if err != nil {
			t.Fatal(err)
		}
		observer, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(now))
		if err != nil {
			t.Fatal(err)
		}
		defer observer.Close()
		done := waitSQLiteApprovalAsync(observer, prepared.Approvals[0].ApprovalID)
		waitForSQLiteApprovalSubscription(t, observer, prepared.Approvals[0].ApprovalID)
		clock.Store(lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond).UnixNano())
		if _, err := writer.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: lease, Now: now()}); err != nil {
			t.Fatal(err)
		}
		waited := receiveSQLiteApprovalWait(t, done)
		if waited.err != nil || waited.result.Approval.State != sessiontree.ApprovalCancelled ||
			waited.result.Receipt.Decision != sessiontree.ApprovalDecisionApprove || waited.result.Receipt.Reason != sessiontree.ApprovalReasonCancelled {
			t.Fatalf("waited=%#v err=%v", waited.result, waited.err)
		}
	})
}

func TestSQLiteWaitApprovalDecisionExitsWhenStoreCloses(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 1, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "approval-wait-close.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	done := waitSQLiteApprovalAsync(store, prepared.Approvals[0].ApprovalID)
	waitForSQLiteApprovalSubscription(t, store, prepared.Approvals[0].ApprovalID)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	waited := receiveSQLiteApprovalWait(t, done)
	if waited.err == nil {
		t.Fatalf("waited=%#v, want closed store error", waited.result)
	}
}

type sqliteApprovalWaitOutcome struct {
	result sessiontree.WaitApprovalDecisionResult
	err    error
}

func waitSQLiteApprovalAsync(store *Store, approvalID string) <-chan sqliteApprovalWaitOutcome {
	done := make(chan sqliteApprovalWaitOutcome, 1)
	go func() {
		result, err := store.WaitApprovalDecision(context.Background(), approvalID)
		done <- sqliteApprovalWaitOutcome{result: result, err: err}
	}()
	return done
}

func waitForSQLiteApprovalSubscription(t *testing.T, store *Store, approvalID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		store.approvalSignalMu.Lock()
		_, subscribed := store.approvalSignals[approvalID]
		store.approvalSignalMu.Unlock()
		if subscribed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("approval waiter did not subscribe")
		}
		time.Sleep(time.Millisecond)
	}
}

func receiveSQLiteApprovalWait(t *testing.T, done <-chan sqliteApprovalWaitOutcome) sqliteApprovalWaitOutcome {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("approval waiter did not observe canonical state change")
		return sqliteApprovalWaitOutcome{}
	}
}

func TestSQLiteWaitApprovalDecisionReadsDecisionAfterReopen(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 57, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "approval-wait-reopen.db")
	store, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	if _, err := store.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
		ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, WithAuthorityClock(func() time.Time { return now.Add(2 * time.Second) }))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	waited, err := reopened.WaitApprovalDecision(ctx, record.ApprovalID)
	if err != nil || waited.Approval.State != sessiontree.ApprovalDecisionSubmitted || waited.Approval.DecisionID != "decision" {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
}

func TestSQLitePrepareApprovalBatchRollsBackRequestedEntryInsertFailure(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "approval-requested-rollback.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	req := approvalPrepare(lease, "call-1", 0, 2, now)
	second := approvalPrepare(lease, "call-2", 1, 2, now).Items[0]
	req.Items = append(req.Items, second)
	if _, err := store.db.Exec(`CREATE TRIGGER fail_second_approval_requested_entry
		BEFORE INSERT ON entries
		WHEN NEW.id = '` + second.RequestedEntry.ID + `'
		BEGIN
			SELECT RAISE(ABORT, 'injected requested entry insertion failure');
		END`); err != nil {
		t.Fatal(err)
	}

	var beforeEntries int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM entries WHERE thread_id = 'thread'`).Scan(&beforeEntries); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareApprovalBatch(ctx, req); err == nil || !strings.Contains(err.Error(), "injected requested entry insertion failure") {
		t.Fatalf("PrepareApprovalBatch err=%v, want injected insertion failure", err)
	}
	for table, key := range map[string]string{
		"approval_requests": "thread_id", "effect_attempts": "thread_id", "approval_queues": "root_thread_id",
	} {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table + ` WHERE ` + key + ` = 'thread'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows=%d, want rollback", table, count)
		}
	}
	var afterEntries int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM entries WHERE thread_id = 'thread'`).Scan(&afterEntries); err != nil {
		t.Fatal(err)
	}
	if afterEntries != beforeEntries {
		t.Fatalf("entries before=%d after=%d", beforeEntries, afterEntries)
	}
	queue, err := store.ReadApprovalQueue(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if queue.Generation != 0 || queue.Revision != 0 || queue.CurrentApprovalID != "" || len(queue.Items) != 0 {
		t.Fatalf("queue mutated: %#v", queue)
	}
}

func TestApprovalDispatchEntryFailureLeavesDecisionSubmitted(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 5, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			ctx := context.Background()
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			current := prepared.Approvals[0]
			submitted, err := repo.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: current.Identity(),
				ExpectedApprovalRevision: current.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			approvedEntry := approvalApprovedTestEntry("decision", current.Identity())
			if _, err := repo.Append(sessiontree.ContextWithTurnLease(ctx, lease), approvedEntry, sessiontree.AppendOptions{ID: approvedEntry.ID, Now: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CommitApprovalDispatch(ctx, sessiontree.CommitApprovalDispatchRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
				EffectAttemptID: current.EffectAttemptID, Lease: lease, AuthorizationProofHash: "proof",
				ApprovedEntry: approvedEntry, Now: now,
			}); err == nil {
				t.Fatal("commit succeeded despite approved entry conflict")
			}
			stored, err := repo.Approval(ctx, current.ApprovalID)
			if err != nil || stored.State != sessiontree.ApprovalDecisionSubmitted || stored.Revision != submitted.Approval.Revision {
				t.Fatalf("stored approval=%#v err=%v", stored, err)
			}
			queue, err := repo.ReadApprovalQueue(ctx, "thread")
			if err != nil || queue.CurrentApprovalID != current.ApprovalID || queue.Revision != submitted.Queue.Revision {
				t.Fatalf("queue=%#v err=%v", queue, err)
			}
		})
	}
}

func TestApprovalDispatchReplayRejectsRequestIdentityDrift(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 10, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			ctx := context.Background()
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			current := prepared.Approvals[0]
			submitted, err := repo.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: current.Identity(),
				ExpectedApprovalRevision: current.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			request := sessiontree.CommitApprovalDispatchRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
				EffectAttemptID: current.EffectAttemptID, Lease: lease, AuthorizationProofHash: "proof",
				ApprovedEntry: approvalApprovedTestEntry("decision", current.Identity()), Now: now.Add(2 * time.Second),
			}
			if _, err := repo.CommitApprovalDispatch(ctx, request); err != nil {
				t.Fatal(err)
			}
			for _, testCase := range []struct {
				name    string
				wantErr error
				mutate  func(*sessiontree.CommitApprovalDispatchRequest)
			}{
				{name: "approval revision", wantErr: sessiontree.ErrRequestConflict, mutate: func(req *sessiontree.CommitApprovalDispatchRequest) {
					req.ExpectedApprovalRevision++
				}},
				{name: "effect attempt", wantErr: sessiontree.ErrRequestConflict, mutate: func(req *sessiontree.CommitApprovalDispatchRequest) {
					req.EffectAttemptID = "different-effect"
				}},
				{name: "lease owner", wantErr: sessiontree.ErrStaleAuthority, mutate: func(req *sessiontree.CommitApprovalDispatchRequest) {
					req.Lease.OwnerID = "different-owner"
				}},
				{name: "lease generation", wantErr: sessiontree.ErrStaleAuthority, mutate: func(req *sessiontree.CommitApprovalDispatchRequest) {
					req.Lease.Generation++
				}},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					drifted := request
					testCase.mutate(&drifted)
					if _, err := repo.CommitApprovalDispatch(ctx, drifted); !errors.Is(err, testCase.wantErr) {
						t.Fatalf("CommitApprovalDispatch err=%v, want %v", err, testCase.wantErr)
					}
				})
			}
		})
	}
}

func TestSQLiteApprovalDispatchReplayRejectsCorruptAuthority(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 15, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*testing.T, *Store, string, string)
	}{
		{name: "approval state", mutate: func(t *testing.T, store *Store, approvalID, _ string) {
			if _, err := store.db.Exec(`UPDATE approval_requests SET state = 'failed', reason = ?, authorization_proof_hash = '' WHERE approval_id = ?`, sessiontree.ApprovalReasonAuthorizationUnavailable, approvalID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "approval revision", mutate: func(t *testing.T, store *Store, approvalID, _ string) {
			if _, err := store.db.Exec(`UPDATE approval_requests SET revision = revision + 1 WHERE approval_id = ?`, approvalID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "effect state", mutate: func(t *testing.T, store *Store, _, effectID string) {
			if _, err := store.db.Exec(`UPDATE effect_attempts SET state = 'cancelled', terminal_fingerprint = 'corrupt' WHERE effect_attempt_id = ?`, effectID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "receipt proof", mutate: func(t *testing.T, store *Store, _, _ string) {
			if _, err := store.db.Exec(`UPDATE approval_decisions SET authorization_proof_hash = 'different-proof' WHERE decision_id = 'decision'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "receipt revision", mutate: func(t *testing.T, store *Store, _, _ string) {
			if _, err := store.db.Exec(`UPDATE approval_decisions SET approval_revision = approval_revision - 1 WHERE decision_id = 'decision'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "queue revision", mutate: func(t *testing.T, store *Store, _, _ string) {
			if _, err := store.db.Exec(`UPDATE approval_queues SET revision = revision - 1 WHERE root_thread_id = 'thread'`); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "approval-replay-corruption.db"), WithAuthorityClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx := context.Background()
			lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			current := prepared.Approvals[0]
			submitted, err := store.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: current.Identity(),
				ExpectedApprovalRevision: current.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			request := sessiontree.CommitApprovalDispatchRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
				EffectAttemptID: current.EffectAttemptID, Lease: lease, AuthorizationProofHash: "proof",
				ApprovedEntry: approvalApprovedTestEntry("decision", current.Identity()), Now: now.Add(2 * time.Second),
			}
			if _, err := store.CommitApprovalDispatch(ctx, request); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(t, store, current.ApprovalID, current.EffectAttemptID)
			if _, err := store.CommitApprovalDispatch(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("CommitApprovalDispatch err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

func TestSQLiteResolveApprovalReplayRejectsCorruptAuthority(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 12, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*testing.T, *Store, sessiontree.ApprovalRecord)
	}{
		{name: "receipt reason", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_decisions SET reason = ? WHERE decision_id = ?`, sessiontree.ApprovalReasonPolicyDenied, record.DecisionID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "record reason", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_requests SET reason = ? WHERE approval_id = ?`, sessiontree.ApprovalReasonPolicyDenied, record.ApprovalID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "effect rejection", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE effect_attempts SET rejection_code = ? WHERE effect_attempt_id = ?`, sessiontree.ApprovalReasonPolicyDenied, record.EffectAttemptID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "effect owner", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE effect_attempts SET owner_id = '' WHERE effect_attempt_id = ?`, record.EffectAttemptID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "queue revision", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_queues SET revision = 0 WHERE root_thread_id = ?`, record.RootThreadID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "terminal retained as current", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_queues SET current_approval_id = ? WHERE root_thread_id = ?`, record.ApprovalID, record.RootThreadID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rejected entry", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`DELETE FROM entries WHERE thread_id = ? AND id = ?`, record.ThreadID, sessiontree.ApprovalRejectedEntryID(record.DecisionID, record.ApprovalID)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "approval-resolve-corruption.db"), WithAuthorityClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx := context.Background()
			lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			request := sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: record.Revision,
				Decision: sessiontree.ApprovalDecisionReject, RejectedEntry: approvalRejectedTestEntry("decision", record.Identity()), Now: now.Add(time.Second),
			}
			resolved, err := store.ResolveApproval(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(t, store, resolved.Approval)
			if _, err := store.ResolveApproval(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("ResolveApproval err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

type effectPreparationTestRepo interface {
	PrepareEffectAttempt(context.Context, sessiontree.PrepareEffectAttemptRequest) (sessiontree.PrepareEffectAttemptResult, error)
}

func TestPrepareApprovalBatchIsAtomicAndModelOrdered(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 30, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name+"/ordered-replay", func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			req := approvalPrepare(lease, "call-late", 2, 3, now)
			req.Items = append(req.Items, approvalPrepare(lease, "call-first", 0, 3, now).Items[0])
			prepared, err := repo.PrepareApprovalBatch(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if prepared.Replayed || len(prepared.Effects) != 2 || len(prepared.Approvals) != 2 || len(prepared.Queue.Items) != 2 {
				t.Fatalf("prepared batch = %#v", prepared)
			}
			for index, wantBatchIndex := range []int{0, 2} {
				approval := prepared.Approvals[index]
				if approval.BatchIndex != wantBatchIndex || approval.QueueSequence != int64(index+1) ||
					approval.ApprovalID != approval.EffectAttemptID || approval.ApprovalID != prepared.Effects[index].EffectAttemptID {
					t.Fatalf("prepared item %d = approval %#v effect %#v", index, approval, prepared.Effects[index])
				}
			}
			replayed, err := repo.PrepareApprovalBatch(context.Background(), req)
			if err != nil || !replayed.Replayed || len(replayed.Approvals) != 2 || replayed.Queue.Revision != prepared.Queue.Revision {
				t.Fatalf("batch replay = %#v err=%v", replayed, err)
			}
			conflict := req
			conflict.Items = append([]sessiontree.ApprovalPreflightItem(nil), req.Items...)
			conflict.Items[0].ApprovalRequestFingerprint = "different"
			if _, err := repo.PrepareApprovalBatch(context.Background(), conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("batch replay conflict err=%v", err)
			}
			queue, err := repo.ReadApprovalQueue(context.Background(), "thread")
			if err != nil || queue.Revision != prepared.Queue.Revision || len(queue.Items) != 2 {
				t.Fatalf("replay conflict mutated queue = %#v err=%v", queue, err)
			}
		})

		t.Run(name+"/conflict-zero-mutation", func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			effectRepo := repo.(effectPreparationTestRepo)
			if _, err := effectRepo.PrepareEffectAttempt(context.Background(), sessiontree.PrepareEffectAttemptRequest{
				Lease: lease, RequestFingerprint: "persisted-effect", Now: now,
				Invocation: sessiontree.EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call-conflict", ToolName: "write_file", ArgumentHash: "args-call-conflict"},
			}); err != nil {
				t.Fatal(err)
			}
			req := approvalPrepare(lease, "call-new", 0, 2, now)
			conflicting := approvalPrepare(lease, "call-conflict", 1, 2, now).Items[0]
			conflicting.EffectRequestFingerprint = "different-effect"
			req.Items = append(req.Items, conflicting)
			if _, err := repo.PrepareApprovalBatch(context.Background(), req); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("atomic batch conflict err=%v", err)
			}
			queue, err := repo.ReadApprovalQueue(context.Background(), "thread")
			if err != nil || queue.Generation != 0 || queue.Revision != 0 || len(queue.Items) != 0 {
				t.Fatalf("failed batch mutated approvals = %#v err=%v", queue, err)
			}
			newEffect, err := effectRepo.PrepareEffectAttempt(context.Background(), sessiontree.PrepareEffectAttemptRequest{
				Lease: lease, RequestFingerprint: "effect-call-new", Now: now,
				Invocation: sessiontree.EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call-new", ToolName: "write_file", ArgumentHash: "args-call-new"},
			})
			if err != nil || newEffect.Replayed {
				t.Fatalf("failed batch persisted first effect: %#v err=%v", newEffect, err)
			}
		})
	}
}

func TestApprovalDecisionContinuationIgnoresConcurrentRootAndChildTailEnqueue(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 45, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		for _, terminal := range []string{"dispatch", "finalize"} {
			t.Run(name+"/"+terminal, func(t *testing.T) {
				repo := makeRepo(t)
				if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: "root", CreatedAt: now, UpdatedAt: now}); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{
					ID: "child", ParentThreadID: "root", ParentTurnID: "turn-root", TaskName: "child", AgentPath: "root/child",
					CreatedAt: now, UpdatedAt: now,
				}); err != nil {
					t.Fatal(err)
				}
				rootLease := admitApprovalTurn(t, repo, now, "root", "turn-root", "run")
				childLease := admitApprovalTurn(t, repo, now, "child", "turn-child", "run-child")
				prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(rootLease, "call-current", 0, 3, now))
				if err != nil {
					t.Fatal(err)
				}
				current := prepared.Queue.Items[0]
				submitted, err := repo.ResolveApproval(context.Background(), sessiontree.ResolveApprovalRequest{
					DecisionID: "decision", ExpectedRootThreadID: "root", ExpectedGeneration: 1, ExpectedRevision: prepared.Queue.Revision,
					ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: current.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				start := make(chan struct{})
				errs := make(chan error, 2)
				go func() {
					<-start
					_, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(rootLease, "call-root-tail", 1, 3, now))
					errs <- err
				}()
				go func() {
					<-start
					_, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(childLease, "call-child-tail", 2, 3, now, "run-child"))
					errs <- err
				}()
				close(start)
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
				withTail, err := repo.ReadApprovalQueue(context.Background(), "root")
				if err != nil || withTail.CurrentApprovalID != current.ApprovalID || withTail.Revision != submitted.Queue.Revision+2 || len(withTail.Items) != 3 {
					t.Fatalf("tail queue = %#v err=%v", withTail, err)
				}
				if terminal == "dispatch" {
					result, err := repo.CommitApprovalDispatch(context.Background(), sessiontree.CommitApprovalDispatchRequest{
						DecisionID: "decision", ExpectedRootThreadID: "root", ExpectedGeneration: 1,
						ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
						EffectAttemptID: current.EffectAttemptID, Lease: rootLease, AuthorizationProofHash: "proof",
						ApprovedEntry: approvalApprovedTestEntry("decision", current.Identity()), Now: now,
					})
					if err != nil || result.Approval.State != sessiontree.ApprovalApproved || len(result.Queue.Items) != 2 {
						t.Fatalf("dispatch after tail = %#v err=%v", result, err)
					}
				} else {
					result, err := repo.FinalizeApproval(context.Background(), sessiontree.FinalizeApprovalRequest{
						ResolutionID: "decision", ExpectedRootThreadID: "root", ExpectedGeneration: 1,
						ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
						State: sessiontree.ApprovalFailed, Reason: sessiontree.ApprovalReasonAuthorizationUnavailable,
						FinalizedEntry: approvalFinalizedTestEntry("decision", current.Identity(), sessiontree.ApprovalFailed, sessiontree.ApprovalReasonAuthorizationUnavailable), Now: now,
					})
					if err != nil || result.Approval.State != sessiontree.ApprovalFailed || len(result.Queue.Items) != 2 {
						t.Fatalf("finalize after tail = %#v err=%v", result, err)
					}
				}
			})
		}
	}
}

func TestCancelApprovalBatchAtomicallyCancelsExactRun(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 55, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			req := approvalPrepare(lease, "call-1", 0, 2, now)
			req.Items = append(req.Items, approvalPrepare(lease, "call-2", 1, 2, now).Items[0])
			prepared, err := repo.PrepareApprovalBatch(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call-other", 0, 1, now, "other-run"))
			if err != nil {
				t.Fatal(err)
			}
			cancelReq := sessiontree.CancelApprovalBatchRequest{
				Lease: lease, RunID: "run", CancellationFingerprint: "caller-cancelled", Now: now.Add(time.Second),
			}
			cancelled, err := repo.CancelApprovalBatch(context.Background(), cancelReq)
			if err != nil {
				t.Fatal(err)
			}
			if cancelled.Replayed || len(cancelled.Approvals) != 2 || len(cancelled.Effects) != 2 || cancelled.Queue.Revision != 3 ||
				cancelled.Queue.CurrentApprovalID != tail.Approvals[0].ApprovalID || len(cancelled.Queue.Items) != 1 {
				t.Fatalf("cancelled batch = %#v", cancelled)
			}
			for index := range cancelled.Approvals {
				if cancelled.Approvals[index].State != sessiontree.ApprovalCancelled || cancelled.Effects[index].State != sessiontree.EffectAttemptCancelled ||
					cancelled.Effects[index].TerminalFingerprint != sessiontree.ApprovalBatchCancellationFingerprint(cancelReq.CancellationFingerprint) {
					t.Fatalf("cancelled item %d = approval %#v effect %#v", index, cancelled.Approvals[index], cancelled.Effects[index])
				}
			}
			replayed, err := repo.CancelApprovalBatch(context.Background(), cancelReq)
			if err != nil || !replayed.Replayed || len(replayed.Approvals) != 2 || replayed.Queue.Revision != cancelled.Queue.Revision {
				t.Fatalf("cancel replay = %#v err=%v", replayed, err)
			}
			conflict := cancelReq
			conflict.CancellationFingerprint = "different"
			if _, err := repo.CancelApprovalBatch(context.Background(), conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("cancel replay conflict err=%v", err)
			}
			for _, approval := range prepared.Approvals {
				stored, err := repo.Approval(context.Background(), approval.ApprovalID)
				if err != nil || stored.State != sessiontree.ApprovalCancelled {
					t.Fatalf("stored cancellation = %#v err=%v", stored, err)
				}
			}
		})
	}
}

func TestCancelApprovalBatchEntryFailureRollsBackAuthority(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 56, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			ctx := context.Background()
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			fingerprint := "cancel-entry-failure"
			entry := approvalCancelledTestEntry(fingerprint, prepared.Approvals[0].Identity())
			if _, err := repo.Append(sessiontree.ContextWithTurnLease(ctx, lease), entry, sessiontree.AppendOptions{ID: entry.ID, Now: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CancelApprovalBatch(ctx, sessiontree.CancelApprovalBatchRequest{
				Lease: lease, RunID: "run", CancellationFingerprint: fingerprint,
				CancellationEntries: []sessiontree.ApprovalCancellationEntry{{ApprovalID: prepared.Approvals[0].ApprovalID, Entry: entry}}, Now: now,
			}); err == nil {
				t.Fatal("cancellation succeeded despite entry conflict")
			}
			stored, err := repo.Approval(ctx, prepared.Approvals[0].ApprovalID)
			if err != nil || stored.State != sessiontree.ApprovalRequested || stored.Revision != prepared.Approvals[0].Revision {
				t.Fatalf("approval after rollback=%#v err=%v", stored, err)
			}
			queue, err := repo.ReadApprovalQueue(ctx, "thread")
			if err != nil || queue.CurrentApprovalID != prepared.Approvals[0].ApprovalID || queue.Revision != prepared.Queue.Revision {
				t.Fatalf("queue after rollback=%#v err=%v", queue, err)
			}
		})
	}
}

func TestCancelApprovalBatchIgnoresCandidateForConcurrentlyRejectedApproval(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 57, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			ctx := context.Background()
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			request := approvalPrepare(lease, "call-1", 0, 2, now)
			request.Items = append(request.Items, approvalPrepare(lease, "call-2", 1, 2, now).Items[0])
			prepared, err := repo.PrepareApprovalBatch(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			first := prepared.Approvals[0]
			if _, err := repo.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
				DecisionID: "reject-first", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: first.Identity(),
				ExpectedApprovalRevision: first.Revision, Decision: sessiontree.ApprovalDecisionReject,
				RejectedEntry: approvalRejectedTestEntry("reject-first", first.Identity()), Now: now,
			}); err != nil {
				t.Fatal(err)
			}
			fingerprint := "cancel-after-reject"
			cancelled, err := repo.CancelApprovalBatch(ctx, sessiontree.CancelApprovalBatchRequest{
				Lease: lease, RunID: "run", CancellationFingerprint: fingerprint,
				CancellationEntries: []sessiontree.ApprovalCancellationEntry{{ApprovalID: first.ApprovalID, Entry: approvalCancelledTestEntry(fingerprint, first.Identity())}},
				Now:                 now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(cancelled.Approvals) != 1 || cancelled.Approvals[0].ApprovalID != prepared.Approvals[1].ApprovalID ||
				len(cancelled.CancellationEntries) != 0 || len(cancelled.Queue.Items) != 0 {
				t.Fatalf("cancelled=%#v", cancelled)
			}
			storedFirst, err := repo.Approval(ctx, first.ApprovalID)
			if err != nil || storedFirst.State != sessiontree.ApprovalRejected {
				t.Fatalf("first approval=%#v err=%v", storedFirst, err)
			}
		})
	}
}

func TestCancelApprovalBatchAndProofCommitHaveSingleWinner(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 58, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			current := prepared.Queue.Items[0]
			submitted, err := repo.ResolveApproval(context.Background(), sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: 1, ExpectedRevision: 1,
				ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: current.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			commitResults := make(chan sessiontree.CommitApprovalDispatchResult, 1)
			commitErrors := make(chan error, 1)
			cancelResults := make(chan sessiontree.CancelApprovalBatchResult, 1)
			cancelErrors := make(chan error, 1)
			go func() {
				<-start
				result, err := repo.CommitApprovalDispatch(context.Background(), sessiontree.CommitApprovalDispatchRequest{
					DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: 1,
					ExpectedCurrent: current.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
					EffectAttemptID: current.EffectAttemptID, Lease: lease, AuthorizationProofHash: "proof",
					ApprovedEntry: approvalApprovedTestEntry("decision", current.Identity()), Now: now,
				})
				commitResults <- result
				commitErrors <- err
			}()
			go func() {
				<-start
				result, err := repo.CancelApprovalBatch(context.Background(), sessiontree.CancelApprovalBatchRequest{
					Lease: lease, RunID: "run", CancellationFingerprint: "caller-cancelled", Now: now,
				})
				cancelResults <- result
				cancelErrors <- err
			}()
			close(start)
			committed, commitErr := <-commitResults, <-commitErrors
			cancelled, cancelErr := <-cancelResults, <-cancelErrors
			if (commitErr == nil) == (cancelErr == nil) {
				t.Fatalf("commit=%#v err=%v cancel=%#v err=%v, want one winner", committed, commitErr, cancelled, cancelErr)
			}
			stored, err := repo.Approval(context.Background(), current.ApprovalID)
			if err != nil {
				t.Fatal(err)
			}
			if commitErr == nil {
				if stored.State != sessiontree.ApprovalApproved || !errors.Is(cancelErr, sessiontree.ErrRequestConflict) {
					t.Fatalf("commit winner stored=%#v cancel err=%v", stored, cancelErr)
				}
			} else if stored.State != sessiontree.ApprovalCancelled || !errors.Is(commitErr, sessiontree.ErrStaleAuthority) {
				t.Fatalf("cancel winner stored=%#v commit err=%v", stored, commitErr)
			}
		})
	}
}

func approvalAuthorityTestRepos() map[string]func(*testing.T) approvalAuthorityTestRepo {
	return map[string]func(*testing.T) approvalAuthorityTestRepo{
		"memory": func(t *testing.T) approvalAuthorityTestRepo { return sessiontree.NewMemoryRepo() },
		"sqlite": func(t *testing.T) approvalAuthorityTestRepo {
			store, err := Open(filepath.Join(t.TempDir(), "approval.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		},
	}
}

func TestApprovalFinalizationMatchesMemoryAndSQLite(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 30, 0, 0, time.UTC)
	cases := []struct {
		name        string
		state       sessiontree.ApprovalState
		reason      string
		effectState sessiontree.EffectAttemptState
	}{
		{name: "policy denied", state: sessiontree.ApprovalRejected, reason: sessiontree.ApprovalReasonPolicyDenied, effectState: sessiontree.EffectAttemptRejected},
		{name: "authorization unavailable", state: sessiontree.ApprovalFailed, reason: sessiontree.ApprovalReasonAuthorizationUnavailable, effectState: sessiontree.EffectAttemptRejected},
		{name: "timed out", state: sessiontree.ApprovalTimedOut, reason: sessiontree.ApprovalReasonTimedOut, effectState: sessiontree.EffectAttemptCancelled},
		{name: "cancelled", state: sessiontree.ApprovalCancelled, reason: sessiontree.ApprovalReasonCancelled, effectState: sessiontree.EffectAttemptCancelled},
	}
	for _, backend := range []string{"memory", "sqlite"} {
		for index, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				var repo approvalAuthorityTestRepo
				if backend == "memory" {
					memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(sessiontree.DefaultLeasePolicy, func() time.Time { return now })
					if err != nil {
						t.Fatal(err)
					}
					repo = memory
				} else {
					store, err := Open(filepath.Join(t.TempDir(), "finalize.db"), WithAuthorityClock(func() time.Time { return now }))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					repo = store
				}
				threadID := "thread-" + string(rune('a'+index))
				lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: threadID}, "turn", "run")
				enqueued, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
				if err != nil {
					t.Fatal(err)
				}
				item := enqueued.Queue.Items[0]
				req := sessiontree.FinalizeApprovalRequest{
					ResolutionID: "resolution", ExpectedRootThreadID: threadID, ExpectedGeneration: 1,
					ExpectedCurrent: item.Identity(), ExpectedApprovalRevision: item.Revision, State: tc.state, Reason: tc.reason, Now: now,
				}
				if tc.effectState == sessiontree.EffectAttemptRejected {
					req.FinalizedEntry = approvalFinalizedTestEntry(req.ResolutionID, item.Identity(), tc.state, tc.reason)
				}
				finalized, err := repo.FinalizeApproval(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				if finalized.Replayed || finalized.Approval.State != tc.state || finalized.Approval.Reason != tc.reason ||
					finalized.Effect.State != tc.effectState || finalized.Receipt.Decision != sessiontree.ApprovalDecisionApprove ||
					finalized.Queue.Revision != 2 || finalized.Queue.CurrentApprovalID != "" || len(finalized.Queue.Items) != 0 {
					t.Fatalf("finalized = %#v", finalized)
				}
				if tc.effectState == sessiontree.EffectAttemptRejected {
					entries, err := repo.Entries(context.Background(), threadID)
					if err != nil {
						t.Fatal(err)
					}
					count := 0
					for _, entry := range entries {
						if entry.ID == finalized.FinalizedEntry.ID {
							count++
						}
					}
					if count != 1 || finalized.FinalizedEntry.ID != sessiontree.ApprovalFinalizationEntryID(req.ResolutionID, item.ApprovalID) {
						t.Fatalf("finalized entry count=%d result=%#v", count, finalized.FinalizedEntry)
					}
				}
				if tc.effectState == sessiontree.EffectAttemptRejected && finalized.Effect.RejectionCode != tc.reason {
					t.Fatalf("effect rejection = %#v, want %q", finalized.Effect, tc.reason)
				}
				replayed, err := repo.FinalizeApproval(context.Background(), req)
				if err != nil || !replayed.Replayed || replayed.Approval.State != tc.state {
					t.Fatalf("finalization replay = %#v err=%v", replayed, err)
				}
				if err := sessiontree.ValidateFinalizeApprovalResultAuthority(req, finalized); err != nil {
					t.Fatalf("first finalization authority: %v", err)
				}
				replayed.Replayed = false
				if !reflect.DeepEqual(replayed, finalized) {
					t.Fatalf("first/replay mismatch:\nfirst=%#v\nreplay=%#v", finalized, replayed)
				}
				conflict := req
				conflict.Reason = "different"
				if _, err := repo.FinalizeApproval(context.Background(), conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
					t.Fatalf("finalization conflict err=%v", err)
				}
			})
		}
	}
}

func TestSQLiteFinalizeApprovalCanonicalEntrySurvivesReopen(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 45, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "finalize-entry-reopen.db")
	store, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	item := prepared.Queue.Items[0]
	req := sessiontree.FinalizeApprovalRequest{
		ResolutionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedCurrent: item.Identity(), ExpectedApprovalRevision: item.Revision,
		State: sessiontree.ApprovalFailed, Reason: sessiontree.ApprovalReasonAuthorizationUnavailable,
		FinalizedEntry: approvalFinalizedTestEntry("decision", item.Identity(), sessiontree.ApprovalFailed, sessiontree.ApprovalReasonAuthorizationUnavailable), Now: now.Add(time.Second),
	}
	finalized, err := store.FinalizeApproval(ctx, req)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if finalized.Replayed {
		t.Fatalf("first finalization unexpectedly replayed: %#v", finalized)
	}
	if finalized.FinalizedEntry.ID == "" {
		t.Fatalf("missing finalized entry: %#v", finalized)
	}
	entries, err := store.Entries(ctx, "thread")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if countEntryID(entries, finalized.FinalizedEntry.ID) != 1 {
		_ = store.Close()
		t.Fatalf("finalized entry count before reopen = %d", countEntryID(entries, finalized.FinalizedEntry.ID))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, WithAuthorityClock(func() time.Time { return now.Add(2 * time.Second) }))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err = reopened.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if countEntryID(entries, finalized.FinalizedEntry.ID) != 1 {
		t.Fatalf("finalized entry count after reopen = %d", countEntryID(entries, finalized.FinalizedEntry.ID))
	}
	replayed, err := reopened.FinalizeApproval(ctx, req)
	if err != nil || !replayed.Replayed || replayed.FinalizedEntry.ID != finalized.FinalizedEntry.ID {
		t.Fatalf("finalization replay=%#v err=%v", replayed, err)
	}
}

func countEntryID(entries []sessiontree.Entry, id string) int {
	count := 0
	for _, entry := range entries {
		if entry.ID == id {
			count++
		}
	}
	return count
}

func TestResolveApprovalReplayReturnsPostApprovalPolicyRejection(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 35, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			request := sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
				ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
			}
			submitted, err := repo.ResolveApproval(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.FinalizeApproval(context.Background(), sessiontree.FinalizeApprovalRequest{
				ResolutionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
				State: sessiontree.ApprovalRejected, Reason: sessiontree.ApprovalReasonPolicyDenied,
				FinalizedEntry: approvalFinalizedTestEntry("decision", record.Identity(), sessiontree.ApprovalRejected, sessiontree.ApprovalReasonPolicyDenied), Now: now.Add(2 * time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			replayed, err := repo.ResolveApproval(context.Background(), request)
			if err != nil || !replayed.Replayed || replayed.Receipt.Decision != sessiontree.ApprovalDecisionApprove ||
				replayed.Receipt.State != sessiontree.ApprovalRejected || replayed.Receipt.Reason != sessiontree.ApprovalReasonPolicyDenied ||
				replayed.Approval.State != sessiontree.ApprovalRejected || replayed.Effect.State != sessiontree.EffectAttemptRejected ||
				replayed.Effect.RejectionCode != sessiontree.ApprovalReasonPolicyDenied || len(replayed.Queue.Items) != 0 {
				t.Fatalf("policy rejection replay=%#v err=%v", replayed, err)
			}
		})
	}
}

func TestWaitApprovalDecisionPreservesPostApprovalPolicyRejection(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 36, 0, 0, time.UTC)
	for name, makeRepo := range approvalAuthorityTestRepos() {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			submitted, err := repo.ResolveApproval(context.Background(), sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
				ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.FinalizeApproval(context.Background(), sessiontree.FinalizeApprovalRequest{
				ResolutionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
				State: sessiontree.ApprovalRejected, Reason: sessiontree.ApprovalReasonPolicyDenied,
				FinalizedEntry: approvalFinalizedTestEntry("decision", record.Identity(), sessiontree.ApprovalRejected, sessiontree.ApprovalReasonPolicyDenied), Now: now.Add(2 * time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			waited, err := repo.WaitApprovalDecision(context.Background(), record.ApprovalID)
			if err != nil || waited.Approval.State != sessiontree.ApprovalRejected || waited.Receipt.Decision != sessiontree.ApprovalDecisionApprove ||
				waited.Receipt.State != sessiontree.ApprovalRejected || waited.Receipt.Reason != sessiontree.ApprovalReasonPolicyDenied {
				t.Fatalf("waited=%#v err=%v", waited, err)
			}
		})
	}
}

func TestSQLiteCrossStoreWaitFirstReadsPostApprovalPolicyRejection(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 37, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "approval-cross-policy.db")
	writer, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ctx := context.Background()
	lease := seedApprovalTurn(t, writer, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := writer.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	submitted, err := writer.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
		ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.FinalizeApproval(ctx, sessiontree.FinalizeApprovalRequest{
		ResolutionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
		State: sessiontree.ApprovalRejected, Reason: sessiontree.ApprovalReasonPolicyDenied,
		FinalizedEntry: approvalFinalizedTestEntry("decision", record.Identity(), sessiontree.ApprovalRejected, sessiontree.ApprovalReasonPolicyDenied), Now: now.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	observer, err := Open(path, WithAuthorityClock(func() time.Time { return now.Add(3 * time.Second) }))
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	waited, err := observer.WaitApprovalDecision(ctx, record.ApprovalID)
	if err != nil || waited.Receipt.Decision != sessiontree.ApprovalDecisionApprove ||
		waited.Receipt.State != sessiontree.ApprovalRejected || waited.Receipt.Reason != sessiontree.ApprovalReasonPolicyDenied {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
}

func TestSQLiteWaitApprovalDecisionRejectsMissingPostDecisionReceipt(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 38, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "approval-missing-receipt.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	if _, err := store.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
		ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM approval_decisions WHERE decision_id = 'decision'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WaitApprovalDecision(ctx, record.ApprovalID); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("WaitApprovalDecision err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLiteApprovalDecisionSnapshotDoesNotMixCrossStoreCommit(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 39, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "approval-snapshot-boundary.db")
	writer, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ctx := context.Background()
	lease := seedApprovalTurn(t, writer, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := writer.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	submitted, err := writer.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
		ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	observer, err := Open(path, WithAuthorityClock(func() time.Time { return now.Add(2 * time.Second) }))
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	tx, err := observer.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	reachedQueueRead := make(chan struct{})
	releaseQueueRead := make(chan struct{})
	runner := &approvalSnapshotBarrierRunner{sqlRunner: tx, reached: reachedQueueRead, release: releaseQueueRead}
	type snapshotOutcome struct {
		result  sessiontree.WaitApprovalDecisionResult
		pending bool
		err     error
	}
	done := make(chan snapshotOutcome, 1)
	go func() {
		result, pending, err := readSQLiteApprovalDecisionSnapshot(ctx, runner, record.ApprovalID, now.Add(2*time.Second))
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		done <- snapshotOutcome{result: result, pending: pending, err: err}
	}()
	select {
	case <-reachedQueueRead:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not reach queue read boundary")
	}
	if _, err := writer.FinalizeApproval(ctx, sessiontree.FinalizeApprovalRequest{
		ResolutionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
		State: sessiontree.ApprovalRejected, Reason: sessiontree.ApprovalReasonPolicyDenied,
		FinalizedEntry: approvalFinalizedTestEntry("decision", record.Identity(), sessiontree.ApprovalRejected, sessiontree.ApprovalReasonPolicyDenied), Now: now.Add(3 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	close(releaseQueueRead)
	select {
	case snapshot := <-done:
		if snapshot.err != nil || snapshot.pending || snapshot.result.Approval.State != sessiontree.ApprovalDecisionSubmitted ||
			snapshot.result.Receipt.State != sessiontree.ApprovalDecisionSubmitted || snapshot.result.Receipt.Decision != sessiontree.ApprovalDecisionApprove {
			t.Fatalf("snapshot=%#v pending=%v err=%v", snapshot.result, snapshot.pending, snapshot.err)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot read did not finish")
	}
	waited, err := observer.WaitApprovalDecision(ctx, record.ApprovalID)
	if err != nil || waited.Receipt.State != sessiontree.ApprovalRejected ||
		waited.Receipt.Decision != sessiontree.ApprovalDecisionApprove || waited.Receipt.Reason != sessiontree.ApprovalReasonPolicyDenied {
		t.Fatalf("latest wait=%#v err=%v", waited, err)
	}
}

type approvalSnapshotBarrierRunner struct {
	sqlRunner
	once    sync.Once
	reached chan struct{}
	release <-chan struct{}
}

func (r *approvalSnapshotBarrierRunner) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if strings.Contains(query, "FROM approval_queues") {
		r.once.Do(func() {
			close(r.reached)
			<-r.release
		})
	}
	return r.sqlRunner.QueryRowContext(ctx, query, args...)
}

func TestApprovalAuthorityMatchesMemoryAndSQLite(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "approval.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for name, repo := range map[string]approvalAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			testApprovalRootQueueAndDecisionReceipts(t, repo, now)
		})
	}
}

func TestApprovalChildDeleteMatchesMemoryAndSQLite(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 45, 0, 0, time.UTC)
	for name, makeRepo := range map[string]func(t *testing.T) approvalAuthorityTestRepo{
		"memory": func(t *testing.T) approvalAuthorityTestRepo { return sessiontree.NewMemoryRepo() },
		"sqlite": func(t *testing.T) approvalAuthorityTestRepo {
			store, err := Open(filepath.Join(t.TempDir(), "approval-lifecycle.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		},
	} {
		t.Run(name+"/child-delete", func(t *testing.T) {
			repo := makeRepo(t)
			if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: "root", CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			child := sessiontree.ThreadMeta{ID: "child", ParentThreadID: "root", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "root/child"}
			lease := seedApprovalTurn(t, repo, now, child, "turn", "run")
			prepared, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.ReleaseTurnLease(context.Background(), lease); err != nil {
				t.Fatal(err)
			}
			if err := repo.DeleteThread(context.Background(), "child"); err != nil {
				t.Fatal(err)
			}
			queue, err := repo.ReadApprovalQueue(context.Background(), "root")
			if err != nil || queue.Generation != 1 || queue.Revision != 2 || queue.CurrentApprovalID != "" || len(queue.Items) != 0 {
				t.Fatalf("child delete queue = %#v err=%v", queue, err)
			}
			if _, err := repo.Approval(context.Background(), prepared.Approvals[0].ApprovalID); !errors.Is(err, sessiontree.ErrApprovalNotFound) {
				t.Fatalf("child delete approval err=%v", err)
			}
		})
	}
}

func TestApprovalDispatchAndCancellationHaveSingleAtomicWinner(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 50, 0, 0, time.UTC)
	for name, makeRepo := range map[string]func(t *testing.T) approvalAuthorityTestRepo{
		"memory": func(t *testing.T) approvalAuthorityTestRepo { return sessiontree.NewMemoryRepo() },
		"sqlite": func(t *testing.T) approvalAuthorityTestRepo {
			store, err := Open(filepath.Join(t.TempDir(), "approval-race.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		},
	} {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			enqueued, err := repo.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
			if err != nil {
				t.Fatal(err)
			}
			item := enqueued.Queue.Items[0]
			decisionID := "decision"
			submitted, err := repo.ResolveApproval(context.Background(), sessiontree.ResolveApprovalRequest{
				DecisionID: decisionID, ExpectedRootThreadID: "thread", ExpectedGeneration: 1, ExpectedRevision: 1,
				ExpectedCurrent: item.Identity(), ExpectedApprovalRevision: item.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			commitResult := make(chan sessiontree.CommitApprovalDispatchResult, 1)
			commitErr := make(chan error, 1)
			cancelResult := make(chan sessiontree.FinalizeApprovalResult, 1)
			cancelErr := make(chan error, 1)
			go func() {
				<-start
				result, err := repo.CommitApprovalDispatch(context.Background(), sessiontree.CommitApprovalDispatchRequest{
					DecisionID: decisionID, ExpectedRootThreadID: "thread", ExpectedGeneration: 1,
					ExpectedCurrent: item.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
					EffectAttemptID: submitted.Approval.EffectAttemptID, Lease: lease, AuthorizationProofHash: "proof",
					ApprovedEntry: approvalApprovedTestEntry(decisionID, item.Identity()), Now: now.Add(time.Second),
				})
				commitResult <- result
				commitErr <- err
			}()
			go func() {
				<-start
				result, err := repo.FinalizeApproval(context.Background(), sessiontree.FinalizeApprovalRequest{
					ResolutionID: decisionID, ExpectedRootThreadID: "thread", ExpectedGeneration: 1,
					ExpectedCurrent: item.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
					State: sessiontree.ApprovalCancelled, Reason: sessiontree.ApprovalReasonCancelled, Now: now.Add(time.Second),
				})
				cancelResult <- result
				cancelErr <- err
			}()
			close(start)
			committed, commitFailure := <-commitResult, <-commitErr
			cancelled, cancelFailure := <-cancelResult, <-cancelErr
			if (commitFailure == nil) == (cancelFailure == nil) {
				t.Fatalf("commit err=%v result=%#v cancel err=%v result=%#v, want exactly one winner", commitFailure, committed, cancelFailure, cancelled)
			}
			stored, err := repo.Approval(context.Background(), item.ApprovalID)
			if err != nil {
				t.Fatal(err)
			}
			if commitFailure == nil {
				if stored.State != sessiontree.ApprovalApproved || committed.Effect.State != sessiontree.EffectAttemptDispatching || !errors.Is(cancelFailure, sessiontree.ErrRequestConflict) {
					t.Fatalf("dispatch winner stored=%#v commit=%#v cancel err=%v", stored, committed, cancelFailure)
				}
			} else if stored.State != sessiontree.ApprovalCancelled || cancelled.Effect.State != sessiontree.EffectAttemptCancelled || !errors.Is(commitFailure, sessiontree.ErrStaleAuthority) {
				t.Fatalf("cancel winner stored=%#v cancel=%#v commit err=%v", stored, cancelled, commitFailure)
			}
		})
	}
}

func testApprovalRootQueueAndDecisionReceipts(t *testing.T, repo approvalAuthorityTestRepo, now time.Time) {
	t.Helper()
	ctx := context.Background()
	rootLease := seedApprovalTurn(t, repo, now, sessiontree.ThreadMeta{ID: "root"}, "turn-root", "run-root")
	childMeta := sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "root", ParentTurnID: "turn-root", TaskName: "child", AgentPath: "root/child",
	}
	childLease := seedApprovalTurn(t, repo, now, childMeta, "turn-child", "run-child")

	rootEnqueue, err := repo.PrepareApprovalBatch(ctx, approvalPrepare(rootLease, "call-root", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	rootApprovalID := rootEnqueue.Approvals[0].ApprovalID
	if rootEnqueue.Replayed || rootEnqueue.Queue.RootThreadID != "root" || rootEnqueue.Queue.Generation != 1 || rootEnqueue.Queue.Revision != 1 || rootEnqueue.Queue.CurrentApprovalID != rootApprovalID {
		t.Fatalf("root enqueue = %#v", rootEnqueue)
	}
	childEnqueue, err := repo.PrepareApprovalBatch(ctx, approvalPrepare(childLease, "call-child", 0, 1, now.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	queue := childEnqueue.Queue
	childApprovalID := childEnqueue.Approvals[0].ApprovalID
	if queue.Generation != 1 || queue.Revision != 2 || queue.CurrentApprovalID != rootApprovalID || len(queue.Items) != 2 {
		t.Fatalf("root queue = %#v", queue)
	}
	if queue.Items[0].ApprovalID != rootApprovalID || queue.Items[0].QueueSequence != 1 || queue.Items[0].ParentThreadID != "" ||
		queue.Items[1].ApprovalID != childApprovalID || queue.Items[1].QueueSequence != 2 || queue.Items[1].ParentThreadID != "root" {
		t.Fatalf("ordered items = %#v", queue.Items)
	}

	rootItem := queue.Items[0]
	reject := sessiontree.ResolveApprovalRequest{
		DecisionID: "decision-reject-root", ExpectedRootThreadID: "root", ExpectedGeneration: 1, ExpectedRevision: 2,
		ExpectedCurrent: rootItem.Identity(), ExpectedApprovalRevision: rootItem.Revision, Decision: sessiontree.ApprovalDecisionReject,
		RejectedEntry: approvalRejectedTestEntry("decision-reject-root", rootItem.Identity()), Now: now.Add(2 * time.Second),
	}
	rejected, err := repo.ResolveApproval(ctx, reject)
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Replayed || rejected.Receipt.State != sessiontree.ApprovalRejected || rejected.Receipt.Reason != sessiontree.ApprovalReasonUserRejected ||
		rejected.Queue.Revision != 3 || rejected.Queue.CurrentApprovalID != childApprovalID || len(rejected.Queue.Items) != 1 ||
		rejected.Effect.State != sessiontree.EffectAttemptRejected || rejected.Effect.RejectionCode != sessiontree.ApprovalReasonUserRejected {
		t.Fatalf("rejected = %#v", rejected)
	}
	replayedReject, err := repo.ResolveApproval(ctx, reject)
	if err != nil || !replayedReject.Replayed || replayedReject.Receipt.State != sessiontree.ApprovalRejected {
		t.Fatalf("reject replay = %#v err=%v", replayedReject, err)
	}
	conflict := reject
	conflict.Decision = sessiontree.ApprovalDecisionApprove
	conflict.RejectedEntry = sessiontree.Entry{}
	if _, err := repo.ResolveApproval(ctx, conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("decision identity conflict err=%v", err)
	}
	stale := reject
	stale.DecisionID = "decision-stale"
	stale.RejectedEntry = approvalRejectedTestEntry(stale.DecisionID, rootItem.Identity())
	if _, err := repo.ResolveApproval(ctx, stale); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("stale queue CAS err=%v", err)
	}
	afterConflict, err := repo.ReadApprovalQueue(ctx, "root")
	if err != nil || afterConflict.Revision != 3 || afterConflict.CurrentApprovalID != childApprovalID || len(afterConflict.Items) != 1 {
		t.Fatalf("conflict mutated queue = %#v err=%v", afterConflict, err)
	}

	childItem := afterConflict.Items[0]
	approve := sessiontree.ResolveApprovalRequest{
		DecisionID: "decision-approve-child", ExpectedRootThreadID: "root", ExpectedGeneration: 1, ExpectedRevision: 3,
		ExpectedCurrent: childItem.Identity(), ExpectedApprovalRevision: childItem.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(3 * time.Second),
	}
	submitted, err := repo.ResolveApproval(ctx, approve)
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Receipt.State != sessiontree.ApprovalDecisionSubmitted || submitted.Queue.CurrentApprovalID != childApprovalID || submitted.Queue.Revision != 4 || len(submitted.Queue.Items) != 1 {
		t.Fatalf("submitted = %#v", submitted)
	}
	replayedSubmitted, err := repo.ResolveApproval(ctx, approve)
	if err != nil || !replayedSubmitted.Replayed || replayedSubmitted.Receipt.State != sessiontree.ApprovalDecisionSubmitted {
		t.Fatalf("submitted replay = %#v err=%v", replayedSubmitted, err)
	}

	approved, err := repo.CommitApprovalDispatch(ctx, sessiontree.CommitApprovalDispatchRequest{
		DecisionID: approve.DecisionID, ExpectedRootThreadID: "root", ExpectedGeneration: 1,
		ExpectedCurrent: childItem.Identity(), ExpectedApprovalRevision: childItem.Revision + 1,
		EffectAttemptID: childItem.EffectAttemptID, Lease: childLease, AuthorizationProofHash: "proof-hash",
		ApprovedEntry: approvalApprovedTestEntry(approve.DecisionID, childItem.Identity()), Now: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if approved.Replayed || approved.Receipt.State != sessiontree.ApprovalApproved || approved.Receipt.AuthorizationProofHash != "proof-hash" ||
		approved.Effect.State != sessiontree.EffectAttemptDispatching || approved.Queue.Revision != 5 || approved.Queue.CurrentApprovalID != "" || len(approved.Queue.Items) != 0 ||
		approved.ApprovedEntry.ID != sessiontree.ApprovalDispatchEntryID(approve.DecisionID, childItem.ApprovalID) ||
		approved.ApprovedEntry.ThreadID != "child" || approved.ApprovedEntry.TurnID != "turn-child" || approved.ApprovedEntry.ParentID == "" {
		t.Fatalf("approved dispatch = %#v", approved)
	}
	replayedDispatch, err := repo.CommitApprovalDispatch(ctx, sessiontree.CommitApprovalDispatchRequest{
		DecisionID: approve.DecisionID, ExpectedRootThreadID: "root", ExpectedGeneration: 1,
		ExpectedCurrent: childItem.Identity(), ExpectedApprovalRevision: childItem.Revision + 1,
		EffectAttemptID: childItem.EffectAttemptID, Lease: childLease, AuthorizationProofHash: "proof-hash",
		ApprovedEntry: approvalApprovedTestEntry(approve.DecisionID, childItem.Identity()), Now: now.Add(4 * time.Second),
	})
	if err != nil || !replayedDispatch.Replayed || replayedDispatch.Receipt.State != sessiontree.ApprovalApproved ||
		replayedDispatch.ApprovedEntry.ID != approved.ApprovedEntry.ID || replayedDispatch.ApprovedEntry.RawHash != approved.ApprovedEntry.RawHash {
		t.Fatalf("dispatch replay = %#v err=%v", replayedDispatch, err)
	}
	stored, err := repo.Approval(ctx, childApprovalID)
	if err != nil || stored.State != sessiontree.ApprovalApproved || stored.AuthorizationProofHash != "proof-hash" {
		t.Fatalf("stored approval = %#v err=%v", stored, err)
	}
}

func TestSQLiteApprovalAuthorityPersistsAcrossReopen(t *testing.T) {
	now := time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "approval-reopen.db")
	store, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	lease := seedApprovalTurn(t, store, now, sessiontree.ThreadMeta{ID: "source"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(context.Background(), approvalPrepare(lease, "call", 0, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	approvalID := prepared.Approvals[0].ApprovalID
	if err := store.ReleaseTurnLease(context.Background(), lease); err != nil {
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
	queue, err := reopened.ReadApprovalQueue(context.Background(), "source")
	if err != nil || queue.Generation != 1 || queue.Revision != 1 || queue.CurrentApprovalID != approvalID || len(queue.Items) != 1 {
		t.Fatalf("reopened queue = %#v err=%v", queue, err)
	}
	if err := reopened.DeleteThread(context.Background(), "source"); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ReadApprovalQueue(context.Background(), "source"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("deleted queue err=%v, want ErrThreadNotFound", err)
	}
	if _, err := reopened.Approval(context.Background(), approvalID); !errors.Is(err, sessiontree.ErrApprovalNotFound) {
		t.Fatalf("deleted approval err=%v, want ErrApprovalNotFound", err)
	}
}

func TestSQLiteApprovalQueueUsesCanonicalOrderIndex(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "approval-plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN `+approvalSelectSQL+`
		WHERE root_thread_id = ? AND state IN ('requested', 'decision_submitted')
		ORDER BY queue_sequence, approval_id`, "root")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var detail string
	for rows.Next() {
		var id, parent, unused int
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "approval_requests_root_queue_idx") {
			return
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("approval queue query plan did not use canonical order index: %s", detail)
}

func TestSQLiteMigratesSchemaVersion15ApprovalAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "approval-v15.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		if _, err := db.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES('root', ?, ?)`, formatTime(time.Now()), formatTime(time.Now())); err != nil {
			t.Fatal(err)
		}
	})
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Thread(context.Background(), "root"); err != nil {
		t.Fatalf("migrated thread: %v", err)
	}
	for _, object := range []string{"approval_queues", "approval_requests", "approval_decisions", "approval_requests_root_queue_idx"} {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, object).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("migrated approval authority object %q count=%d", object, count)
		}
	}
	queue, err := store.ReadApprovalQueue(context.Background(), "root")
	if err != nil || queue.RootThreadID != "root" || queue.Generation != 0 || queue.Revision != 0 || len(queue.Items) != 0 {
		t.Fatalf("migrated empty approval queue = %#v err=%v", queue, err)
	}
}

func seedApprovalTurn(t *testing.T, repo approvalAuthorityTestRepo, now time.Time, meta sessiontree.ThreadMeta, turnID, runID string) sessiontree.TurnLease {
	t.Helper()
	ctx := context.Background()
	meta.CreatedAt = now
	meta.UpdatedAt = now
	if _, err := repo.CreateThread(ctx, meta); err != nil {
		t.Fatal(err)
	}
	return admitApprovalTurn(t, repo, now, meta.ID, turnID, runID)
}

func admitApprovalTurn(t *testing.T, repo approvalAuthorityTestRepo, now time.Time, threadID, turnID, runID string) sessiontree.TurnLease {
	t.Helper()
	admitted, err := repo.AdmitTurn(context.Background(), sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: "owner-" + threadID,
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit-" + turnID, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return admitted.Lease
}

func approvalPrepare(lease sessiontree.TurnLease, callID string, batchIndex, batchSize int, now time.Time, runIDs ...string) sessiontree.PrepareApprovalBatchRequest {
	runID := "run"
	if len(runIDs) != 0 {
		runID = runIDs[0]
	}
	item := sessiontree.ApprovalPreflightItem{
		EffectRequestFingerprint: "effect-" + callID, ApprovalRequestFingerprint: "approval-" + callID,
		Invocation: sessiontree.EffectInvocationIdentity{ThreadID: lease.ThreadID, TurnID: lease.TurnID, RunID: runID, ToolCallID: callID, ToolName: "write_file", ArgumentHash: "args-" + callID},
		ToolKind:   "local", Step: 1, BatchIndex: batchIndex, BatchSize: batchSize,
		Resources: []sessiontree.ApprovalResource{{Kind: "file", Value: "notes.md"}}, Effects: []string{"write"},
		Labels: map[string]string{"surface": "test"}, HostContext: map[string]string{"target": "local"}, Destructive: true,
	}
	item.EffectAttemptID = sessiontree.ApprovalEffectAttemptID(item.Invocation)
	item.RequestedEntry = sessiontree.Entry{
		ID: sessiontree.ApprovalRequestedEntryID(item.EffectAttemptID), ThreadID: lease.ThreadID, TurnID: lease.TurnID,
		Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "requested"},
	}
	return sessiontree.PrepareApprovalBatchRequest{Lease: lease, Now: now, Items: []sessiontree.ApprovalPreflightItem{item}}
}

func approvalApprovedTestEntry(decisionID string, identity sessiontree.ApprovalIdentity) sessiontree.Entry {
	return sessiontree.Entry{
		ID: sessiontree.ApprovalDispatchEntryID(decisionID, identity.ApprovalID), ThreadID: identity.ThreadID,
		TurnID: identity.TurnID, Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "approved"},
	}
}

func approvalRejectedTestEntry(decisionID string, identity sessiontree.ApprovalIdentity) sessiontree.Entry {
	return sessiontree.Entry{
		ID: sessiontree.ApprovalRejectedEntryID(decisionID, identity.ApprovalID), ThreadID: identity.ThreadID,
		TurnID: identity.TurnID, Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "rejected"},
	}
}

func approvalFinalizedTestEntry(resolutionID string, identity sessiontree.ApprovalIdentity, state sessiontree.ApprovalState, reason string) sessiontree.Entry {
	return sessiontree.Entry{
		ID: sessiontree.ApprovalFinalizationEntryID(resolutionID, identity.ApprovalID), ThreadID: identity.ThreadID,
		TurnID: identity.TurnID, Type: sessiontree.EntryCustom,
		Metadata: map[string]string{"approval_state": string(state), "approval_reason": reason},
	}
}

func approvalCancelledTestEntry(fingerprint string, identity sessiontree.ApprovalIdentity) sessiontree.Entry {
	return sessiontree.Entry{
		ID: sessiontree.ApprovalCancellationEntryID(fingerprint, identity.ApprovalID), ThreadID: identity.ThreadID,
		TurnID: identity.TurnID, Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "canceled"},
	}
}
