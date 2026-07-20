package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

type turnAuthorityTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	Thread(context.Context, string) (sessiontree.ThreadMeta, error)
	Entries(context.Context, string) ([]sessiontree.Entry, error)
	Append(context.Context, sessiontree.Entry, sessiontree.AppendOptions) (sessiontree.Entry, error)
	ActiveTurnLease(context.Context, string) (sessiontree.TurnLease, bool, error)
	AdmitTurn(context.Context, sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error)
	ReadTurnAdmission(context.Context, string, string, string) (sessiontree.AdmitTurnResult, bool, error)
	CanonicalTurnEntries(context.Context, string, string, string) ([]sessiontree.Entry, bool, error)
	FinishTurn(context.Context, sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error)
	RecoverInterruptedTurn(context.Context, sessiontree.RecoverInterruptedTurnRequest) (sessiontree.RecoverInterruptedTurnResult, error)
	ProviderState(context.Context, string) (sessiontree.ProviderStateRecord, error)
	Fork(context.Context, sessiontree.ForkOptions) (sessiontree.ThreadMeta, error)
}

func TestStartedTurnIdentityIsUniqueAcrossJournalWriters(t *testing.T) {
	now := time.Date(2026, 7, 20, 16, 0, 0, 0, time.UTC)
	memory := sessiontree.NewMemoryRepo()
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "unique-started-turn.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for name, repo := range map[string]turnAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(ctx, sessiontree.Entry{
				ThreadID: "thread", TurnID: "historical-turn", Type: sessiontree.EntryTurnMarker,
				TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "historical-run"},
			}, sessiontree.AppendOptions{ID: "historical-started", Now: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(ctx, sessiontree.Entry{
				ThreadID: "thread", TurnID: "historical-turn", Type: sessiontree.EntryTurnMarker,
				TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "different-run"},
			}, sessiontree.AppendOptions{ID: "duplicate-started", Now: now}); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("duplicate generic started err=%v, want ErrRequestConflict", err)
			}
			if _, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "historical-turn", RunID: "new-run", OwnerID: "owner",
				RequestFingerprint: "new-request", Input: session.Message{Role: session.User, Content: "reuse"}, Now: now,
			}); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("historical turn admission err=%v, want ErrRequestConflict", err)
			}
			entries, err := repo.Entries(ctx, "thread")
			if err != nil || len(entries) != 1 || entries[0].ID != "historical-started" {
				t.Fatalf("journal after rejected reuse = %#v err=%v", entries, err)
			}
			if lease, active, err := repo.ActiveTurnLease(ctx, "thread"); err != nil || active {
				t.Fatalf("rejected reuse lease=%#v active=%v err=%v", lease, active, err)
			}
		})
	}
}

func TestForkRejectsCanonicalTurnAndRunIdentityCollisions(t *testing.T) {
	now := time.Date(2026, 7, 20, 17, 0, 0, 0, time.UTC)
	for _, collision := range []struct {
		name      string
		turnIDMap map[string]string
		runIDMap  map[string]string
	}{
		{
			name:      "turn id",
			turnIDMap: map[string]string{"turn-a": "fork-turn", "turn-b": "fork-turn"},
			runIDMap:  map[string]string{"run-a": "fork-run-a", "run-b": "fork-run-b"},
		},
		{
			name:      "run id",
			turnIDMap: map[string]string{"turn-a": "fork-turn-a", "turn-b": "fork-turn-b"},
			runIDMap:  map[string]string{"run-a": "fork-run", "run-b": "fork-run"},
		},
	} {
		t.Run(collision.name, func(t *testing.T) {
			memory := sessiontree.NewMemoryRepo()
			sqliteStore, err := Open(filepath.Join(t.TempDir(), "fork-identity-collision.db"), WithAuthorityClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqliteStore.Close() })

			for backend, repo := range map[string]turnAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
				t.Run(backend, func(t *testing.T) {
					ctx := context.Background()
					if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
						t.Fatal(err)
					}
					parentID := ""
					for _, identity := range []struct{ turnID, runID string }{{"turn-a", "run-a"}, {"turn-b", "run-b"}} {
						started, err := repo.Append(ctx, sessiontree.Entry{
							ThreadID: "source", ParentID: parentID, TurnID: identity.turnID,
							Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
							Metadata: map[string]string{"run_id": identity.runID},
						}, sessiontree.AppendOptions{Now: now})
						if err != nil {
							t.Fatal(err)
						}
						terminal, err := repo.Append(ctx, sessiontree.Entry{
							ThreadID: "source", ParentID: started.ID, TurnID: identity.turnID,
							Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted,
						}, sessiontree.AppendOptions{Now: now})
						if err != nil {
							t.Fatal(err)
						}
						parentID = terminal.ID
					}
					if _, err := repo.Fork(ctx, sessiontree.ForkOptions{
						SourceThreadID: "source", NewThreadID: "fork", TurnIDMap: collision.turnIDMap, RunIDMap: collision.runIDMap, Now: now,
					}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
						t.Fatalf("fork collision err=%v, want ErrAuthorityCorrupt", err)
					}
					if _, err := repo.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
						t.Fatalf("rejected fork remained visible: %v", err)
					}
				})
			}
		})
	}
}

func TestSchemaVersion13ExcludesTurnAuthorityLedgers(t *testing.T) {
	for _, table := range []string{"turn_admissions", "turn_finishes"} {
		if strings.Contains(schemaVersion13SQL, table) {
			t.Fatalf("canonical schema v13 unexpectedly contains %q", table)
		}
	}
}

func TestSQLiteTurnAuthorityMatchesMemoryAdmissionAndFinish(t *testing.T) {
	fixed := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "turn-authority.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for name, repo := range map[string]turnAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: fixed, UpdatedAt: fixed}); err != nil {
				t.Fatal(err)
			}
			admitRequest := sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
				Input: session.Message{Role: session.User, Content: "hello"}, RequestFingerprint: "admit-fingerprint", Now: fixed,
			}
			admitted, err := repo.AdmitTurn(ctx, admitRequest)
			if err != nil {
				t.Fatal(err)
			}
			if admitted.Replayed || admitted.BaseLeafID != "" || admitted.Lease.Generation != 1 || admitted.Lease.OwnerID != "owner-1" ||
				!admitted.Lease.AcquiredAt.Equal(fixed) || !admitted.Lease.ExpiresAt.Equal(fixed.Add(policy.TTL)) {
				t.Fatalf("admission = %#v", admitted)
			}
			if admitted.TurnStarted.Type != sessiontree.EntryTurnMarker || admitted.TurnStarted.TurnStatus != sessiontree.TurnStarted ||
				admitted.TurnStarted.Metadata["run_id"] != "run-1" || admitted.UserMessage.Type != sessiontree.EntryUserMessage ||
				admitted.UserMessage.Message.Role != session.User || admitted.UserMessage.Message.Content != "hello" ||
				admitted.UserMessage.ParentID != admitted.TurnStarted.ID {
				t.Fatalf("canonical admission entries = started %#v user %#v", admitted.TurnStarted, admitted.UserMessage)
			}
			canonical, found, err := repo.CanonicalTurnEntries(ctx, "thread", "turn-1", "run-1")
			if err != nil || !found || len(canonical) != 2 || canonical[0].ID != admitted.TurnStarted.ID || canonical[1].ID != admitted.UserMessage.ID {
				t.Fatalf("canonical turn entries = %#v found=%v err=%v", canonical, found, err)
			}
			replayed, err := repo.AdmitTurn(ctx, admitRequest)
			if err != nil || !replayed.Replayed || !sessiontree.SameTurnLease(replayed.Lease, admitted.Lease) {
				t.Fatalf("admission replay = %#v err=%v", replayed, err)
			}
			conflict := admitRequest
			conflict.RequestFingerprint = "changed"
			if _, err := repo.AdmitTurn(ctx, conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("changed admission replay err=%v, want conflict", err)
			}
			entries, err := repo.Entries(ctx, "thread")
			if err != nil || len(entries) != 2 {
				t.Fatalf("entries after admission conflict = %#v err=%v", entries, err)
			}

			finishRequest := sessiontree.FinishTurnRequest{
				Lease: admitted.Lease, RunID: "run-1", TerminalEntryID: "terminal-1", Status: sessiontree.TurnFailed,
				Metadata: map[string]string{"reason": "provider"}, FailureMessage: "provider failed",
				OutcomeFingerprint: "finish-fingerprint", Now: fixed.Add(time.Second),
				ProviderState: &sessiontree.ProviderStateRecord{
					ThreadID: "thread", LeafEntryID: "terminal-1", CompatibilityKey: "provider:model",
					State: provider.State{Kind: "responses", ID: "state-1"}, CreatedByRunID: "run-1", CreatedByTurnID: "turn-1", UpdatedAt: fixed.Add(time.Second),
				},
			}
			finished, err := repo.FinishTurn(ctx, finishRequest)
			if err != nil {
				t.Fatal(err)
			}
			if finished.Replayed || finished.Failure == nil || finished.Failure.Type != sessiontree.EntryRunFailure || finished.Failure.Error != "provider failed" ||
				finished.Terminal.ID != "terminal-1" || finished.Terminal.Type != sessiontree.EntryTurnMarker || finished.Terminal.TurnStatus != sessiontree.TurnFailed ||
				finished.Terminal.ParentID != finished.Failure.ID || finished.Terminal.Metadata["reason"] != "provider" {
				t.Fatalf("finish = %#v", finished)
			}
			canonical, found, err = repo.CanonicalTurnEntries(ctx, "thread", "turn-1", "run-1")
			if err != nil || !found || len(canonical) != 4 || canonical[2].ID != finished.Failure.ID || canonical[3].ID != finished.Terminal.ID {
				t.Fatalf("finished canonical turn entries = %#v found=%v err=%v", canonical, found, err)
			}
			if lease, active, err := repo.ActiveTurnLease(ctx, "thread"); err != nil || active {
				t.Fatalf("active lease after finish = %#v active=%v err=%v", lease, active, err)
			}
			providerState, err := repo.ProviderState(ctx, "thread")
			if err != nil || providerState.LeafEntryID != "terminal-1" || providerState.State.ID != "state-1" {
				t.Fatalf("provider state after finish = %#v err=%v", providerState, err)
			}
			finishReplay, err := repo.FinishTurn(ctx, finishRequest)
			if err != nil || !finishReplay.Replayed || finishReplay.Failure == nil || finishReplay.Terminal.ID != finished.Terminal.ID {
				t.Fatalf("finish replay = %#v err=%v", finishReplay, err)
			}
			finishConflict := finishRequest
			finishConflict.OutcomeFingerprint = "changed"
			if _, err := repo.FinishTurn(ctx, finishConflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("changed finish replay err=%v, want conflict", err)
			}
			entries, err = repo.Entries(ctx, "thread")
			if err != nil || len(entries) != 4 {
				t.Fatalf("entries after finish replay/conflict = %#v err=%v", entries, err)
			}
			meta, err := repo.Thread(ctx, "thread")
			if err != nil || meta.LeafID != "terminal-1" {
				t.Fatalf("thread after finish = %#v err=%v", meta, err)
			}
		})
	}
}

func TestTurnAdmissionReplayUsesExactFinishedBranchWhileRetryIsActive(t *testing.T) {
	now := time.Date(2026, 7, 20, 11, 0, 0, 0, time.UTC)
	memory := sessiontree.NewMemoryRepo()
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "exact-replay.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for name, repo := range map[string]turnAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			originalRequest := sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn-a", RunID: "run-a", OwnerID: "owner-a", RequestFingerprint: "fingerprint-a",
				Input: session.Message{Role: session.User, Content: "original"}, Now: now,
			}
			original, err := repo.AdmitTurn(ctx, originalRequest)
			if err != nil {
				t.Fatal(err)
			}
			finished, err := repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
				Lease: original.Lease, RunID: "run-a", TerminalEntryID: "terminal-a", Status: sessiontree.TurnCompleted,
				OutcomeFingerprint: "outcome-a", Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn-b", RunID: "run-b", OwnerID: "owner-b", RequestFingerprint: "fingerprint-b",
				RetryLeafID: original.UserMessage.ID, Now: now,
			}); err != nil {
				t.Fatal(err)
			}
			before, err := repo.Entries(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := repo.AdmitTurn(ctx, originalRequest)
			if err != nil {
				t.Fatal(err)
			}
			if !replayed.Replayed || replayed.Terminal == nil || replayed.Terminal.Terminal.ID != finished.Terminal.ID || replayed.Terminal.Terminal.TurnStatus != sessiontree.TurnCompleted {
				t.Fatalf("exact replay=%#v, want original terminal %#v", replayed, finished.Terminal)
			}
			after, err := repo.Entries(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("exact replay mutated journal: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestTurnAdmissionReadAcceptsExactInterruptedRecoveryFinish(t *testing.T) {
	initial := time.Date(2026, 7, 20, 14, 0, 0, 0, time.UTC)
	now := initial
	policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "recovered-replay.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for name, repo := range map[string]turnAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			now = initial
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: initial, UpdatedAt: initial}); err != nil {
				t.Fatal(err)
			}
			request := sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "run"}, RequestFingerprint: "request", Now: initial,
			}
			admitted, err := repo.AdmitTurn(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			now = initial.Add(policy.TTL + policy.ClockSkewAllowance + time.Second)
			recovered, err := repo.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Status != sessiontree.TurnAborted || recovered.Generation != admitted.Lease.Generation+1 {
				t.Fatalf("recovery = %#v", recovered)
			}
			read, found, err := repo.ReadTurnAdmission(ctx, "thread", "turn", "run")
			if err != nil || !found || !read.Replayed || read.Terminal == nil ||
				read.Terminal.Terminal.ID != recovered.Terminal.ID || read.Terminal.Terminal.TurnStatus != sessiontree.TurnAborted {
				t.Fatalf("recovered exact read = %#v found=%v err=%v", read, found, err)
			}
			entries, err := repo.Entries(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			terminalCount := 0
			for _, entry := range entries {
				if entry.TurnID == "turn" && entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnAborted {
					terminalCount++
				}
			}
			if terminalCount != 1 {
				t.Fatalf("recovered terminal markers = %d entries=%#v", terminalCount, entries)
			}
		})
	}
}

func TestSQLiteAdmitTurnRejectsMalformedReferencesWithoutMutation(t *testing.T) {
	now := time.Date(2026, 7, 20, 9, 30, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "invalid-reference.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	_, err = store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "fingerprint",
		Input: session.Message{Role: session.User, Content: "inspect", References: []session.MessageReference{{
			ReferenceID: "ref-1", Kind: session.MessageReferenceText, Label: "missing text",
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "message reference") {
		t.Fatalf("AdmitTurn error = %v, want malformed message reference", err)
	}
	assertSQLiteTurnAuthorityCounts(t, store, 0, 0, 0, 0)
}

func TestSQLiteCanonicalTurnEntriesRejectsReferenceRawDrift(t *testing.T) {
	now := time.Date(2026, 7, 20, 9, 45, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "reference-raw-drift.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request",
		Input: session.Message{Role: session.User, Content: "inspect", References: []session.MessageReference{{
			ReferenceID: "context:0", Kind: session.MessageReferenceText, Label: "quote", Text: "original",
		}}}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE entries
		SET message_json = json_set(message_json, '$.References[0].text', 'changed without raw update')
		WHERE thread_id = ? AND id = ?`, "thread", admitted.UserMessage.ID); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.CanonicalTurnEntries(ctx, "thread", "turn", "run"); !found || !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("reference raw drift found=%v err=%v", found, err)
	}
}

func TestSQLiteAppendRestrictsReferencesToValidUserMessagesWithoutMutation(t *testing.T) {
	now := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	valid := []session.MessageReference{{ReferenceID: "ref-1", Kind: session.MessageReferenceText, Label: "quote", Text: "selected"}}
	tests := map[string]sessiontree.Entry{
		"malformed user reference": {
			ThreadID: "thread", Type: sessiontree.EntryUserMessage,
			Message: session.Message{Role: session.User, Content: "inspect", References: []session.MessageReference{{
				ReferenceID: "ref-1", Kind: session.MessageReferenceText, Label: "missing text",
			}}},
		},
		"assistant reference": {
			ThreadID: "thread", Type: sessiontree.EntryAssistantMessage,
			Message: session.Message{Role: session.Assistant, Content: "answer", References: valid},
		},
	}

	for name, entry := range tests {
		t.Run(name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "invalid-append.db"), WithAuthorityClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			ctx := context.Background()
			if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, entry, sessiontree.AppendOptions{Now: now}); err == nil || !strings.Contains(err.Error(), "message reference") {
				t.Fatalf("Append error = %v, want message reference rejection", err)
			}
			assertSQLiteTurnAuthorityCounts(t, store, 0, 0, 0, 0)
			meta, err := store.Thread(ctx, "thread")
			if err != nil || meta.LeafID != "" {
				t.Fatalf("thread after rejected append = %#v err=%v", meta, err)
			}
		})
	}
}

func TestSQLiteAdmitTurnMissingRetryTargetHasZeroSideEffects(t *testing.T) {
	fixed := time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "missing-retry.db"), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: fixed, UpdatedAt: fixed}); err != nil {
		t.Fatal(err)
	}
	_, err = store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
		RetryLeafID:        "missing",
		RequestFingerprint: "retry-fingerprint",
	})
	if !errors.Is(err, sessiontree.ErrEntryNotFound) {
		t.Fatalf("AdmitTurn err=%v, want ErrEntryNotFound", err)
	}
	assertSQLiteTurnAuthorityCounts(t, store, 0, 0, 0, 0)
	meta, err := store.Thread(ctx, "thread")
	if err != nil || meta.LeafID != "" {
		t.Fatalf("thread after rejected retry = %#v err=%v", meta, err)
	}
	var generation int64
	if err := store.db.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = 'thread'`).Scan(&generation); err != nil || generation != 0 {
		t.Fatalf("lease generation after rejected retry = %d err=%v", generation, err)
	}
}

func TestSQLiteAdmitTurnClaimedThreadHasZeroSideEffects(t *testing.T) {
	fixed := time.Date(2026, 7, 19, 13, 30, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "claimed-admission.db"), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: fixed, UpdatedAt: fixed}); err != nil {
		t.Fatal(err)
	}
	plan := mustMarshalSQLiteForkOperationPlan(t, storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "fork-operation", RequestFingerprint: "fork-fingerprint", PreparedAt: fixed,
		Root: storage.ForkOperationPlanNode{NodeID: "root", SourceThreadID: "thread", DestinationThreadID: "destination", ArtifactClosure: emptySQLiteForkArtifactClosure(t, "thread", "destination")},
	})
	if _, created, err := store.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        "fork-operation",
		RequestFingerprint: "fork-fingerprint",
		SourceThreadIDs:    []string{"thread"},
		AuthorityThreadIDs: []string{"thread", "destination"},
		State:              storage.ForkOperationPrepared,
		Plan:               plan,
		CreatedAt:          fixed,
		UpdatedAt:          fixed,
	}); err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	_, err = store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
		Input: session.Message{Role: session.User, Content: "blocked"}, RequestFingerprint: "admit-fingerprint",
	})
	if !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("AdmitTurn err=%v, want ErrThreadAuthorityBusy", err)
	}
	assertSQLiteTurnAuthorityCounts(t, store, 0, 0, 0, 0)
	var generation int64
	if err := store.db.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = 'thread'`).Scan(&generation); err != nil || generation != 0 {
		t.Fatalf("lease generation after claimed admission = %d err=%v", generation, err)
	}
}

func TestSQLiteFinishTurnRejectsExpiredProofWithoutMutation(t *testing.T) {
	current := time.Date(2026, 7, 19, 14, 0, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: 9 * time.Second, RenewInterval: 3 * time.Second, ClockSkewAllowance: time.Second}
	store, err := Open(filepath.Join(t.TempDir(), "expired-finish.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return current }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: current, UpdatedAt: current}); err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit-fingerprint",
	})
	if err != nil {
		t.Fatal(err)
	}
	current = current.Add(policy.TTL + time.Nanosecond)
	_, err = store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-1", TerminalEntryID: "terminal-1",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "finish-fingerprint",
	})
	if !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("FinishTurn err=%v, want ErrStaleAuthority", err)
	}
	assertSQLiteTurnAuthorityCounts(t, store, 2, 1, 1, 0)
	lease, active, err := store.ActiveTurnLease(ctx, "thread")
	if err != nil || !active || !sessiontree.SameTurnLease(lease, admitted.Lease) {
		t.Fatalf("lease after rejected finish = %#v active=%v err=%v", lease, active, err)
	}
}

func TestSQLiteFinishTurnCommitsProviderStateAtomically(t *testing.T) {
	fixed := time.Date(2026, 7, 19, 14, 30, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "provider-state-finish.db"), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: fixed, UpdatedAt: fixed}); err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit-fingerprint",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-1", TerminalEntryID: "terminal-1", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish-fingerprint", Now: fixed,
		ProviderState: &sessiontree.ProviderStateRecord{
			ThreadID: "thread", LeafEntryID: "terminal-1", CompatibilityKey: "provider:model",
			State: provider.State{Kind: "responses", ID: "state-1"}, CreatedByRunID: "run-1", CreatedByTurnID: "turn-1", UpdatedAt: fixed,
		},
	}
	if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER fail_provider_state_insert
		BEFORE INSERT ON provider_states BEGIN SELECT RAISE(ABORT, 'injected provider state failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, request); err == nil || !strings.Contains(err.Error(), "injected provider state failure") {
		t.Fatalf("FinishTurn error = %v", err)
	}
	assertSQLiteTurnAuthorityCounts(t, store, 2, 1, 1, 0)
	var providerStates int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_states`).Scan(&providerStates); err != nil || providerStates != 0 {
		t.Fatalf("provider states after rollback = %d err=%v", providerStates, err)
	}
	if _, err := store.db.ExecContext(ctx, `DROP TRIGGER fail_provider_state_insert`); err != nil {
		t.Fatal(err)
	}
	finished, err := store.FinishTurn(ctx, request)
	if err != nil || finished.Replayed || finished.Terminal.ID != "terminal-1" {
		t.Fatalf("FinishTurn result=%#v err=%v", finished, err)
	}
	state, err := store.ProviderState(ctx, "thread")
	if err != nil || state.LeafEntryID != "terminal-1" || state.State.ID != "state-1" {
		t.Fatalf("provider state=%#v err=%v", state, err)
	}
	assertSQLiteTurnAuthorityCounts(t, store, 3, 0, 1, 1)
}

func TestSQLiteDualOpenersAdmitOnlyOneTurnOwner(t *testing.T) {
	fixed := time.Date(2026, 7, 19, 15, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "dual-admit.db")
	first, err := Open(path, WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := Open(path, WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	ctx := context.Background()
	if _, err := first.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: fixed, UpdatedAt: fixed}); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		owner string
		turn  string
		err   error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for index, store := range []*Store{first, second} {
		index, store := index, store
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			owner := fmt.Sprintf("owner-%d", index+1)
			turnID := fmt.Sprintf("turn-%d", index+1)
			_, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: turnID, RunID: "run-" + turnID, OwnerID: owner,
				Input: session.Message{Role: session.User, Content: owner}, RequestFingerprint: "fingerprint-" + owner,
			})
			outcomes <- outcome{owner: owner, turn: turnID, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(outcomes)
	successes := 0
	busy := 0
	winnerOwner := ""
	winnerTurn := ""
	for outcome := range outcomes {
		switch {
		case outcome.err == nil:
			successes++
			winnerOwner = outcome.owner
			winnerTurn = outcome.turn
		case errors.Is(outcome.err, sessiontree.ErrActiveTurn):
			busy++
		default:
			t.Fatalf("unexpected admission outcome: %#v", outcome)
		}
	}
	if successes != 1 || busy != 1 {
		t.Fatalf("admission outcomes: successes=%d busy=%d", successes, busy)
	}
	lease, active, err := first.ActiveTurnLease(ctx, "thread")
	if err != nil || !active || lease.OwnerID != winnerOwner || lease.TurnID != winnerTurn {
		t.Fatalf("durable winner lease = %#v active=%v err=%v, want owner=%q turn=%q", lease, active, err, winnerOwner, winnerTurn)
	}
	assertSQLiteTurnAuthorityCounts(t, first, 2, 1, 1, 0)
}

func assertSQLiteTurnAuthorityCounts(t *testing.T, store *Store, entries, leases, admissions, finishes int) {
	t.Helper()
	ctx := context.Background()
	for table, want := range map[string]int{
		"entries": entries, "active_turn_leases": leases, "turn_admissions": admissions, "turn_finishes": finishes,
	} {
		var got int
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if got != want {
			t.Fatalf("%s count = %d, want %d", table, got, want)
		}
	}
}
