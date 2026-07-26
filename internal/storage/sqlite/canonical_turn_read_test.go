package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteReadCanonicalTurnPersistsAuthorityAndActivePath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}

	original, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-original", RunID: "run-original", OwnerID: "owner-original",
		RequestFingerprint: "request-original", Input: session.Message{Role: session.User, Content: "original question"},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalFinish, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: original.Lease, RunID: "run-original", TerminalEntryID: "original-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "outcome-original",
	})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-retry",
		RequestFingerprint: "request-retry", RetrySourceTurnID: "turn-original", RetrySourceEntryID: original.UserMessage.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: retry.Lease, RunID: "run-retry", TerminalEntryID: "retry-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "outcome-retry",
	}); err != nil {
		t.Fatal(err)
	}
	assertSQLiteCanonicalTurnRead(t, store, "turn-original", "run-original", "")
	assertSQLiteCanonicalTurnRead(t, store, "turn-retry", "run-retry", "turn-original")
	if _, err := store.db.ExecContext(ctx, `UPDATE turn_admissions SET base_leaf_id = 'wrong-source' WHERE thread_id = 'thread' AND turn_id = 'turn-retry'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCanonicalTurn(ctx, "thread", "turn-retry"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("retry admission mismatch err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE turn_admissions SET base_leaf_id = ? WHERE thread_id = 'thread' AND turn_id = 'turn-retry'`, original.UserMessage.ID); err != nil {
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
	assertSQLiteCanonicalTurnRead(t, store, "turn-retry", "run-retry", "turn-original")

	if err := store.MoveLeaf(ctx, "thread", originalFinish.Terminal.ID); err != nil {
		t.Fatal(err)
	}
	branch, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-branch", RunID: "run-branch", OwnerID: "owner-branch",
		RequestFingerprint: "request-branch", Input: session.Message{Role: session.User, Content: "branch question"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: branch.Lease, RunID: "run-branch", TerminalEntryID: "branch-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "outcome-branch",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCanonicalTurn(ctx, "thread", "turn-retry"); !errors.Is(err, sessiontree.ErrCanonicalTurnNotFound) {
		t.Fatalf("abandoned retry err=%v, want ErrCanonicalTurnNotFound", err)
	}
	assertSQLiteCanonicalTurnRead(t, store, "turn-branch", "run-branch", "")

	if _, err := store.db.ExecContext(ctx, `DELETE FROM turn_admissions WHERE thread_id = 'thread' AND turn_id = 'turn-branch'`); err != nil {
		t.Fatal(err)
	}
	if read, err := store.ReadCanonicalTurn(ctx, "thread", "turn-branch"); err != nil || read.Turn.TurnID != "turn-branch" {
		t.Fatalf("normal journal read without execution ledger=%#v err=%v", read, err)
	}
}

func TestSQLiteReadCanonicalTurnDistinguishesMarkerOnlyAndBrokenJournal(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, threadID := range []string{"marker", "broken", "duplicate-terminal"} {
		if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, store, "marker", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCanonicalTurn(ctx, "marker", "turn"); !errors.Is(err, sessiontree.ErrCanonicalTurnNotFound) {
		t.Fatalf("marker-only err=%v, want ErrCanonicalTurnNotFound", err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, "broken", "turn", session.Message{Role: session.User, Content: "input"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCanonicalTurn(ctx, "broken", "turn"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("broken journal err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, store, "duplicate-terminal", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store, "duplicate-terminal", "turn", session.Message{Role: session.User, Content: "input"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := sessiontree.AppendTurnMarker(ctx, store, "duplicate-terminal", "turn", sessiontree.TurnCompleted, map[string]string{"run_id": "run"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ReadCanonicalTurn(ctx, "duplicate-terminal", "turn"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("duplicate terminal exact err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "duplicate-terminal", Tail: 1}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("duplicate terminal list err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLiteReadCanonicalTurnRejectsNonLatestRetrySourceOutsideAncestorPath(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	original, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{ThreadID: "thread", TurnID: "original", RunID: "original-run", OwnerID: "original-owner", RequestFingerprint: "original-request", Input: session.Message{Role: session.User, Content: "original"}})
	if err != nil {
		t.Fatal(err)
	}
	originalFinish, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{Lease: original.Lease, RunID: "original-run", TerminalEntryID: "original-terminal", Status: sessiontree.TurnCompleted, OutcomeFingerprint: "original-outcome"})
	if err != nil {
		t.Fatal(err)
	}
	offPath, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{ThreadID: "thread", TurnID: "off-path", RunID: "off-path-run", OwnerID: "off-path-owner", RequestFingerprint: "off-path-request", Input: session.Message{Role: session.User, Content: "off path"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{Lease: offPath.Lease, RunID: "off-path-run", TerminalEntryID: "off-path-terminal", Status: sessiontree.TurnCompleted, OutcomeFingerprint: "off-path-outcome"}); err != nil {
		t.Fatal(err)
	}
	if err := store.MoveLeaf(ctx, "thread", originalFinish.Terminal.ID); err != nil {
		t.Fatal(err)
	}
	retry, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{ThreadID: "thread", TurnID: "retry", RunID: "retry-run", OwnerID: "retry-owner", RequestFingerprint: "retry-request", RetrySourceTurnID: "original", RetrySourceEntryID: original.UserMessage.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{Lease: retry.Lease, RunID: "retry-run", TerminalEntryID: "retry-terminal", Status: sessiontree.TurnCompleted, OutcomeFingerprint: "retry-outcome"}); err != nil {
		t.Fatal(err)
	}
	newer, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{ThreadID: "thread", TurnID: "newer", RunID: "newer-run", OwnerID: "newer-owner", RequestFingerprint: "newer-request", Input: session.Message{Role: session.User, Content: "newer"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{Lease: newer.Lease, RunID: "newer-run", TerminalEntryID: "newer-terminal", Status: sessiontree.TurnCompleted, OutcomeFingerprint: "newer-outcome"}); err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{sessiontree.RetrySourceTurnIDMetadataKey: "off-path", sessiontree.RetrySourceEntryIDMetadataKey: offPath.UserMessage.ID, "run_id": "retry-run"}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := loadEntry(ctx, store.db, "thread", retry.TurnStarted.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.Metadata = metadata
	entry.Raw = sessiontree.RawForEntry(entry)
	entry.RawHash = sessiontree.StableHash(entry.Raw)
	if _, err := store.db.ExecContext(ctx, `UPDATE entries SET metadata_json = ?, raw = ?, raw_hash = ? WHERE thread_id = ? AND id = ?`, string(metadataJSON), entry.Raw, entry.RawHash, "thread", retry.TurnStarted.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE turn_admissions SET base_leaf_id = ? WHERE thread_id = ? AND turn_id = ?`, offPath.UserMessage.ID, "thread", "retry"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCanonicalTurn(ctx, "thread", "retry"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("non-latest retry with non-ancestor source err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLiteReadCanonicalTurnRejectsCorruptActiveLeafPath(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store)
	}{
		{name: "empty leaf", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.ExecContext(ctx, `UPDATE threads SET leaf_id = '' WHERE id = 'thread'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing leaf", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.ExecContext(ctx, `UPDATE threads SET leaf_id = 'missing-leaf' WHERE id = 'thread'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "broken parent chain", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.ExecContext(ctx, `UPDATE entries SET parent_id = 'missing-parent' WHERE thread_id = 'thread' AND id = 'terminal'`); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "floret.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request", Input: session.Message{Role: session.User, Content: "input"}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted, OutcomeFingerprint: "outcome"}); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, store)
			_, err = store.ReadCanonicalTurn(ctx, "thread", "turn")
			if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) || errors.Is(err, sessiontree.ErrCanonicalTurnNotFound) {
				t.Fatalf("corrupt active leaf path err=%v, want ErrAuthorityCorrupt only", err)
			}
		})
	}
}

func assertSQLiteCanonicalTurnRead(t *testing.T, store *Store, turnID, runID, retryTurnID string) {
	t.Helper()
	read, err := store.ReadCanonicalTurn(context.Background(), "thread", turnID)
	if err != nil {
		t.Fatal(err)
	}
	if read.Turn.TurnID != turnID || read.Turn.RunID != runID || read.ThroughOrdinal < read.Turn.StartedOrdinal || read.LatestTurn.TurnID == "" {
		t.Fatalf("canonical read=%#v", read)
	}
	if retryTurnID == "" {
		if read.Turn.RetrySource != nil {
			t.Fatalf("unexpected retry source=%#v", read.Turn.RetrySource)
		}
	} else if read.Turn.RetrySource == nil || read.Turn.RetrySource.TurnID != retryTurnID {
		t.Fatalf("retry source=%#v, want turn %q", read.Turn.RetrySource, retryTurnID)
	}
}
