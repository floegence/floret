package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteReferenceOnlyRetryAdmissionAndProjectionHaveZeroMutation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 21, 8, 30, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "reference-only-retry.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	original, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request", Now: now,
		Input: session.Message{Role: session.User, References: []session.MessageReference{{
			ReferenceID: "reference-1", Kind: session.MessageReferenceText, Label: "selected text", Text: "selection",
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: original.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnFailed,
		Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider}, FailureMessage: "provider failed",
		OutcomeFingerprint: "outcome", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil || len(page.Turns) != 1 || page.HasRetryTarget {
		t.Fatalf("reference-only canonical page = %#v, err = %v", page, err)
	}

	beforeEntries, err := store.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	beforeMeta, err := store.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	var beforeGeneration int64
	if err := store.db.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = 'thread'`).Scan(&beforeGeneration); err != nil {
		t.Fatal(err)
	}
	var beforeAdmissions, beforeFinishes int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM turn_admissions`).Scan(&beforeAdmissions); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM turn_finishes`).Scan(&beforeFinishes); err != nil {
		t.Fatal(err)
	}

	_, err = store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "retry", RunID: "retry-run", OwnerID: "retry-owner", RequestFingerprint: "retry-request",
		RetrySourceTurnID: "turn", RetrySourceEntryID: original.UserMessage.ID, Now: now,
	})
	if !errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
		t.Fatalf("reference-only retry admission error = %v, want ErrInvalidThreadAuthority", err)
	}
	afterEntries, entriesErr := store.Entries(ctx, "thread")
	afterMeta, metaErr := store.Thread(ctx, "thread")
	var afterGeneration int64
	generationErr := store.db.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = 'thread'`).Scan(&afterGeneration)
	var afterAdmissions, afterFinishes, activeLeases int
	admissionsErr := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM turn_admissions`).Scan(&afterAdmissions)
	finishesErr := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM turn_finishes`).Scan(&afterFinishes)
	leasesErr := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_turn_leases`).Scan(&activeLeases)
	if entriesErr != nil || metaErr != nil || generationErr != nil || admissionsErr != nil || finishesErr != nil || leasesErr != nil ||
		!reflect.DeepEqual(afterEntries, beforeEntries) || !reflect.DeepEqual(afterMeta, beforeMeta) || afterGeneration != beforeGeneration ||
		afterAdmissions != beforeAdmissions || afterFinishes != beforeFinishes || activeLeases != 0 {
		t.Fatalf("rejected retry mutated SQLite authority: entries_equal=%v meta_equal=%v generation=%d/%d admissions=%d/%d finishes=%d/%d leases=%d errors=%v/%v/%v/%v/%v/%v",
			reflect.DeepEqual(afterEntries, beforeEntries), reflect.DeepEqual(afterMeta, beforeMeta), afterGeneration, beforeGeneration,
			afterAdmissions, beforeAdmissions, afterFinishes, beforeFinishes, activeLeases,
			entriesErr, metaErr, generationErr, admissionsErr, finishesErr, leasesErr)
	}
}
