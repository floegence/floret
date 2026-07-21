package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteStartedRunIdentityIsScopedByThread(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "global-run.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, threadID := range []string{"first", "second"} {
		if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID}); err != nil {
			t.Fatal(err)
		}
	}
	for _, threadID := range []string{"first", "second"} {
		if _, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
			ThreadID: threadID, TurnID: "turn", RunID: "run-shared", OwnerID: "owner-" + threadID,
			RequestFingerprint: "request-" + threadID, Input: session.Message{Role: session.User, Content: threadID},
		}); err != nil {
			t.Fatalf("admit thread %q: %v", threadID, err)
		}
		page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: threadID, Tail: 1})
		if err != nil || len(page.Turns) != 1 || page.Turns[0].RunID != "run-shared" ||
			page.Turns[0].Entries[1].Entry.Message.Content != threadID {
			t.Fatalf("thread %q page=%#v err=%v", threadID, page, err)
		}
	}
}

func TestStartedRunIdentityConcurrentAdmissionsAreIndependentAcrossThreads(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) turnAuthorityTestRepo
	}{
		{name: "memory", repo: func(*testing.T) turnAuthorityTestRepo { return sessiontree.NewMemoryRepo() }},
		{name: "sqlite", repo: func(t *testing.T) turnAuthorityTestRepo {
			store, err := Open(filepath.Join(t.TempDir(), "concurrent-global-run.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := tc.repo(t)
			for _, threadID := range []string{"first", "second"} {
				if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID}); err != nil {
					t.Fatal(err)
				}
			}
			start := make(chan struct{})
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			for _, threadID := range []string{"first", "second"} {
				threadID := threadID
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
						ThreadID: threadID, TurnID: "turn-" + threadID, RunID: "run-shared", OwnerID: "owner-" + threadID,
						RequestFingerprint: "request-" + threadID, Input: session.Message{Role: session.User, Content: threadID},
					})
					errs <- err
				}()
			}
			close(start)
			wg.Wait()
			close(errs)
			successes := 0
			for err := range errs {
				if err == nil {
					successes++
				} else {
					t.Fatalf("concurrent admission err=%v", err)
				}
			}
			if successes != 2 {
				t.Fatalf("successes=%d, want two", successes)
			}
			totalEntries, activeLeases := 0, 0
			for _, threadID := range []string{"first", "second"} {
				entries, err := repo.Entries(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				totalEntries += len(entries)
				if _, active, err := repo.ActiveTurnLease(ctx, threadID); err != nil {
					t.Fatal(err)
				} else if active {
					activeLeases++
				}
			}
			if totalEntries != 4 || activeLeases != 2 {
				t.Fatalf("entries=%d active_leases=%d, want 4 and 2", totalEntries, activeLeases)
			}
		})
	}
}

func TestStartedRunIdentityNormalizationMatchesBackends(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) turnAuthorityTestRepo
	}{
		{name: "memory", repo: func(*testing.T) turnAuthorityTestRepo { return sessiontree.NewMemoryRepo() }},
		{name: "sqlite", repo: func(t *testing.T) turnAuthorityTestRepo {
			store, err := Open(filepath.Join(t.TempDir(), "normalized-global-run.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := tc.repo(t)
			for _, threadID := range []string{"first", "second"} {
				if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := repo.Append(ctx, sessiontree.Entry{
				ThreadID: "first", TurnID: "turn-first", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
				Metadata: map[string]string{"run_id": "  run-shared  "},
			}, sessiontree.AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			entries, err := repo.Entries(ctx, "first")
			if err != nil || len(entries) != 1 || entries[0].Metadata["run_id"] != "run-shared" {
				t.Fatalf("normalized entries=%#v err=%v", entries, err)
			}
			if _, err := repo.Append(ctx, sessiontree.Entry{
				ThreadID: "second", TurnID: "turn-second", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
				Metadata: map[string]string{"run_id": "run-shared"},
			}, sessiontree.AppendOptions{}); err != nil {
				t.Fatalf("cross-thread normalized run identity: %v", err)
			}
		})
	}
}

func TestTurnAdmissionNormalizesStartedRunIdentityAndReplayAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) turnAuthorityTestRepo
	}{
		{name: "memory", repo: func(*testing.T) turnAuthorityTestRepo { return sessiontree.NewMemoryRepo() }},
		{name: "sqlite", repo: func(t *testing.T) turnAuthorityTestRepo {
			store, err := Open(filepath.Join(t.TempDir(), "normalized-admission.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := tc.repo(t)
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			request := sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "\t run \u00a0", OwnerID: "owner",
				RequestFingerprint: "request", Input: session.Message{Role: session.User, Content: "input"},
			}
			admitted, err := repo.AdmitTurn(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if admitted.TurnStarted.Metadata["run_id"] != "run" {
				t.Fatalf("started run=%q, want canonical run", admitted.TurnStarted.Metadata["run_id"])
			}
			replayed, found, err := repo.ReadTurnAdmission(ctx, "thread", "turn", "run")
			if err != nil || !found || !replayed.Replayed || replayed.TurnStarted.ID != admitted.TurnStarted.ID {
				t.Fatalf("canonical replay=%#v found=%v err=%v", replayed, found, err)
			}
		})
	}
}

func TestSQLiteForkPreservesExplicitRunIdentityAcrossThreads(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "fork-global-run.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "source-turn", "source-run", "source")
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); err != nil {
		t.Fatalf("fork preserving run identity: %v", err)
	}
	entries, found, err := store.CanonicalTurnEntries(ctx, "fork", "source-turn", "source-run")
	if err != nil || !found || len(entries) == 0 {
		t.Fatalf("fork canonical entries=%#v found=%v err=%v", entries, found, err)
	}
}

func TestSQLiteForkRejectsReferenceRawDriftWithoutDestinationMutation(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "fork-reference-raw-drift.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request",
		Input: session.Message{
			Role: session.User, Content: "inspect", References: []session.MessageReference{{
				ReferenceID: "context:0", Kind: session.MessageReferenceText, Label: "Selected text", Text: "original",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE entries
		SET message_json = json_set(message_json, '$.References[0].text', 'changed without raw update')
		WHERE thread_id = ? AND id = ?`, "thread", admitted.UserMessage.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("corrupt fork destination error = %v, want ErrThreadNotFound", err)
	}
}

func TestSQLiteForkRewritesRetrySourceAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fork-retry-source.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "source", TurnID: "turn-original", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
		Metadata: map[string]string{"run_id": "run-original"},
	}, sessiontree.AppendOptions{ID: "source-started"}); err != nil {
		t.Fatal(err)
	}
	user, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "source", TurnID: "turn-original", Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "original"},
	}, sessiontree.AppendOptions{ID: "source-user"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "source", TurnID: "turn-original", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted,
	}, sessiontree.AppendOptions{ID: "source-terminal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "source", TurnID: "turn-retry", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
		Metadata: map[string]string{
			"run_id": "run-retry", sessiontree.RetrySourceTurnIDMetadataKey: "turn-original", sessiontree.RetrySourceEntryIDMetadataKey: user.ID,
		},
	}, sessiontree.AppendOptions{ID: "retry-started"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "source", TurnID: "turn-retry", Type: sessiontree.EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "retried"},
	}, sessiontree.AppendOptions{ID: "retry-assistant"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "source", TurnID: "turn-retry", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted,
	}, sessiontree.AppendOptions{ID: "retry-terminal"}); err != nil {
		t.Fatal(err)
	}
	insertSQLiteRetryAdmissionFact(t, store.db, "source", "turn-retry", "run-retry", "retry-started", user.ID, time.Now().UTC())
	if _, err := store.db.ExecContext(ctx, `INSERT INTO turn_finishes(
		thread_id, turn_id, run_id, generation, outcome_fingerprint, terminal_entry_id, finished_at
	) VALUES('source', 'turn-retry', 'run-retry', 1, 'outcome-retry', 'retry-terminal', ?)`, formatTime(time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: "source", NewThreadID: "fork",
		TurnIDMap: map[string]string{"turn-original": "fork-original", "turn-retry": "fork-retry"},
		RunIDMap:  map[string]string{"run-original": "fork-run-original", "run-retry": "fork-run-retry"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 2})
	if err != nil || len(page.Turns) != 2 || page.Turns[1].RetrySource == nil || page.Turns[1].RetrySource.TurnID != "fork-original" {
		t.Fatalf("fork retry page=%#v err=%v", page, err)
	}
	forkUserEntryID := ""
	for _, item := range page.Turns[0].Entries {
		if item.Entry.Type == sessiontree.EntryUserMessage {
			forkUserEntryID = item.Entry.ID
		}
	}
	if forkUserEntryID == "" || page.Turns[1].RetrySource.EntryID != forkUserEntryID || page.Turns[1].RetrySource.EntryID == user.ID {
		t.Fatalf("fork retry source=%#v original_user=%q fork_user=%q", page.Turns[1].RetrySource, user.ID, forkUserEntryID)
	}
	var admissionRunID, admissionStartedID, admissionBaseLeafID string
	if err := store.db.QueryRowContext(ctx, `SELECT run_id, turn_started_id, base_leaf_id FROM turn_admissions
		WHERE thread_id = 'fork' AND turn_id = 'fork-retry'`).Scan(&admissionRunID, &admissionStartedID, &admissionBaseLeafID); err != nil {
		t.Fatal(err)
	}
	retryStartedID := page.Turns[1].StartedEntryID
	if admissionRunID != "fork-run-retry" || admissionStartedID != retryStartedID || admissionBaseLeafID != forkUserEntryID {
		t.Fatalf("fork retry admission run=%q started=%q base=%q, want run=%q started=%q base=%q",
			admissionRunID, admissionStartedID, admissionBaseLeafID, "fork-run-retry", retryStartedID, forkUserEntryID)
	}
	var finishRunID, finishTerminalID string
	if err := store.db.QueryRowContext(ctx, `SELECT run_id, terminal_entry_id FROM turn_finishes
		WHERE thread_id = 'fork' AND turn_id = 'fork-retry'`).Scan(&finishRunID, &finishTerminalID); err != nil {
		t.Fatal(err)
	}
	retryTerminalID := ""
	for _, item := range page.Turns[1].Entries {
		if item.Entry.Type == sessiontree.EntryTurnMarker && item.Entry.TurnStatus == sessiontree.TurnCompleted {
			retryTerminalID = item.Entry.ID
		}
	}
	if finishRunID != "fork-run-retry" || retryTerminalID == "" || finishTerminalID != retryTerminalID {
		t.Fatalf("fork retry finish run=%q terminal=%q, want run=%q terminal=%q", finishRunID, finishTerminalID, "fork-run-retry", retryTerminalID)
	}
}

func TestSQLiteForkRejectsRetryAdmissionRunDriftWithoutDestinationMutation(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"fork", "fork-with-initial-entry"} {
		t.Run(mode, func(t *testing.T) {
			store := buildSQLiteRetrySourceChainFixture(t, true)
			if _, err := store.db.ExecContext(ctx, `UPDATE turn_admissions SET run_id = 'run-drift'
				WHERE thread_id = 'thread' AND turn_id = 'retry-two'`); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mode {
			case "fork":
				_, err = store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"})
			case "fork-with-initial-entry":
				_, _, err = store.ForkWithInitialEntry(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}, sessiontree.Entry{Type: sessiontree.EntryCustom})
			}
			if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("%s error = %v, want ErrAuthorityCorrupt", mode, err)
			}
			assertSQLiteForkDestinationEmpty(t, store, "fork")
		})
	}
}

func TestSQLiteForkRejectsRewriteThatAddsRetryAuthorityWithoutSourceAdmission(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "fork-added-retry-authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	sourceUserID := appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-source", "run-source", "source")
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-rewritten", "run-rewritten", "rewritten")
	_, err = store.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: "thread", NewThreadID: "fork",
		RewriteEntry: func(entry sessiontree.Entry, _ sessiontree.ForkEntryIdentity) (sessiontree.Entry, error) {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-rewritten" {
				entry.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] = "turn-source"
				entry.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] = sourceUserID
			}
			return entry, nil
		},
	})
	if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
	}
	assertSQLiteForkDestinationEmpty(t, store, "fork")
}

func TestSQLiteForkRejectsRetryAuthorityIdentityOverlays(t *testing.T) {
	ctx := context.Background()
	for _, overlay := range []struct {
		name   string
		mutate func(sessiontree.Entry) sessiontree.Entry
	}{
		{name: "started-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.ID = "started-id-overlay"
			}
			return entry
		}},
		{name: "started-thread-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.ThreadID = "started-thread-overlay"
			}
			return entry
		}},
		{name: "started-parent-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.ParentID = "started-parent-overlay"
			}
			return entry
		}},
		{name: "started-turn-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.TurnID = "started-turn-overlay"
			}
			return entry
		}},
		{name: "started-type", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.Type = sessiontree.EntryCustom
			}
			return entry
		}},
		{name: "started-turn-status", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.TurnStatus = sessiontree.TurnCompleted
			}
			return entry
		}},
		{name: "started-run-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.Metadata["run_id"] = "run-overlay"
			}
			return entry
		}},
		{name: "started-retry-source-turn-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] = "started-source-turn-overlay"
			}
			return entry
		}},
		{name: "started-retry-source-entry-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.TurnID == "turn-retry" {
				entry.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] = "started-source-entry-overlay"
			}
			return entry
		}},
		{name: "target-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.ID = "target-id-overlay"
			}
			return entry
		}},
		{name: "target-thread-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.ThreadID = "target-thread-overlay"
			}
			return entry
		}},
		{name: "target-parent-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.ParentID = "target-parent-overlay"
			}
			return entry
		}},
		{name: "target-turn-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.TurnID = "turn-overlay"
			}
			return entry
		}},
		{name: "target-type", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Type = sessiontree.EntryCustom
			}
			return entry
		}},
		{name: "target-turn-status", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.TurnStatus = sessiontree.TurnCompleted
			}
			return entry
		}},
		{name: "promote-custom-to-started", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryCustom && entry.Metadata["kind"] == "ordinary" {
				entry.Type = sessiontree.EntryTurnMarker
				entry.TurnStatus = sessiontree.TurnStarted
				entry.TurnID = "turn-injected"
				entry.Metadata["run_id"] = "run-injected"
			}
			return entry
		}},
		{name: "target-run-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Metadata["run_id"] = "target-run-overlay"
			}
			return entry
		}},
		{name: "target-retry-source-turn-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] = "turn-overlay"
			}
			return entry
		}},
		{name: "target-retry-source-entry-id", mutate: func(entry sessiontree.Entry) sessiontree.Entry {
			if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] = "entry-overlay"
			}
			return entry
		}},
	} {
		t.Run(overlay.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "fork-retry-overlay.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			seedSQLiteAdmittedRetryForkSource(t, ctx, store)
			_, err = store.Fork(ctx, sessiontree.ForkOptions{
				SourceThreadID: "source", NewThreadID: "fork",
				RunIDMap: map[string]string{"run-retry": "run-retry-mapped", "target-run": "target-run-mapped"},
				RewriteEntry: func(entry sessiontree.Entry, _ sessiontree.ForkEntryIdentity) (sessiontree.Entry, error) {
					return overlay.mutate(entry), nil
				},
			})
			if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
			}
			assertSQLiteForkDestinationEmpty(t, store, "fork")
		})
	}
}

func TestSQLiteForkUsesImmutableIdentityMapSnapshots(t *testing.T) {
	ctx := context.Background()
	for _, mutation := range []string{"callback-identity", "captured-maps"} {
		t.Run(mutation, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "fork-map-snapshot.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			seedSQLiteAdmittedRetryForkSource(t, ctx, store)
			turnIDs := map[string]string{"turn-original": "turn-original-mapped", "turn-retry": "turn-retry-mapped"}
			runIDs := map[string]string{"run-original": "run-original-mapped", "run-retry": "run-retry-mapped", "target-run": "target-run-mapped"}
			callbackMapDrift := false
			capturedMutated := false
			_, err = store.Fork(ctx, sessiontree.ForkOptions{
				SourceThreadID: "source", NewThreadID: "fork", TurnIDMap: turnIDs, RunIDMap: runIDs,
				RewriteEntry: func(entry sessiontree.Entry, identity sessiontree.ForkEntryIdentity) (sessiontree.Entry, error) {
					switch mutation {
					case "callback-identity":
						if identity.TurnIDMap["turn-retry"] != "turn-retry-mapped" || identity.RunIDMap["run-retry"] != "run-retry-mapped" {
							callbackMapDrift = true
						}
						identity.TurnIDMap["turn-retry"] = "turn-identity-overlay"
						identity.RunIDMap["run-retry"] = "run-identity-overlay"
					case "captured-maps":
						if !capturedMutated {
							turnIDs["turn-retry"] = "turn-captured-overlay"
							runIDs["run-retry"] = "run-captured-overlay"
							capturedMutated = true
						}
					}
					return entry, nil
				},
			})
			if err != nil || callbackMapDrift {
				t.Fatalf("Fork error=%v callback_map_drift=%t", err, callbackMapDrift)
			}
			page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 2})
			if err != nil || len(page.Turns) != 2 || page.Turns[0].TurnID != "turn-original-mapped" || page.Turns[0].RunID != "run-original-mapped" ||
				page.Turns[1].TurnID != "turn-retry-mapped" || page.Turns[1].RunID != "run-retry-mapped" {
				t.Fatalf("fork page=%#v err=%v", page, err)
			}
		})
	}
}

func TestSQLiteSnapshotForkNodesSeparatesSharedIdentityMaps(t *testing.T) {
	turnIDs := map[string]string{"turn": "turn-mapped"}
	runIDs := map[string]string{"run": "run-mapped"}
	snapshot := snapshotForkNodes([]sessiontree.ForkOptions{
		{NewThreadID: "fork-a", TurnIDMap: turnIDs, RunIDMap: runIDs},
		{NewThreadID: "fork-b", TurnIDMap: turnIDs, RunIDMap: runIDs},
	})
	turnIDs["turn"] = "turn-input-overlay"
	runIDs["run"] = "run-input-overlay"
	snapshot[0].TurnIDMap["turn"] = "turn-node-overlay"
	snapshot[0].RunIDMap["run"] = "run-node-overlay"
	if snapshot[1].TurnIDMap["turn"] != "turn-mapped" || snapshot[1].RunIDMap["run"] != "run-mapped" {
		t.Fatalf("snapshot nodes share identity maps: %#v", snapshot)
	}
}

func TestSQLiteForkCopiesMappedRetryOfRetryChainAndAdmissions(t *testing.T) {
	ctx := context.Background()
	store := buildSQLiteSettledRetrySourceChainFixture(t)
	_, err := store.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: "thread", NewThreadID: "fork",
		TurnIDMap: map[string]string{
			"source-turn": "source-turn-mapped", "retry-one": "retry-one-mapped", "retry-two": "retry-two-mapped",
		},
		RunIDMap: map[string]string{
			"source-run": "source-run-mapped", "retry-one-run": "retry-one-run-mapped", "retry-two-run": "retry-two-run-mapped",
		},
	})
	if err != nil {
		t.Fatalf("Fork retry chain: %v", err)
	}
	page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 3})
	if err != nil {
		entries, entriesErr := store.Entries(ctx, "fork")
		t.Fatalf("ListCanonicalTurns error=%v entries_error=%v entries=%#v", err, entriesErr, entries)
	}
	ids := assertSQLiteMappedRetryOfRetryFork(t, page)
	first, found, err := loadSQLiteTurnAdmission(ctx, store.db, "fork", "retry-one-mapped")
	if err != nil || !found {
		t.Fatalf("first mapped admission=%#v found=%t err=%v", first, found, err)
	}
	second, found, err := loadSQLiteTurnAdmission(ctx, store.db, "fork", "retry-two-mapped")
	if err != nil || !found {
		t.Fatalf("second mapped admission=%#v found=%t err=%v", second, found, err)
	}
	if first.RunID != "retry-one-run-mapped" || first.TurnStartedID != ids.retryOneStarted || first.BaseLeafID != ids.sourceUser ||
		second.RunID != "retry-two-run-mapped" || second.TurnStartedID != ids.retryTwoStarted || second.BaseLeafID != ids.retryOneAssistant {
		t.Fatalf("mapped retry admissions first=%#v second=%#v ids=%#v", first, second, ids)
	}
	firstFinish, found, err := loadSQLiteTurnFinish(ctx, store.db, "fork", "retry-one-mapped")
	if err != nil || !found {
		t.Fatalf("first mapped finish=%#v found=%t err=%v", firstFinish, found, err)
	}
	secondFinish, found, err := loadSQLiteTurnFinish(ctx, store.db, "fork", "retry-two-mapped")
	if err != nil || !found {
		t.Fatalf("second mapped finish=%#v found=%t err=%v", secondFinish, found, err)
	}
	if firstFinish.RunID != "retry-one-run-mapped" || firstFinish.FailureEntryID != ids.retryOneFailure || firstFinish.TerminalEntryID != ids.retryOneTerminal ||
		secondFinish.RunID != "retry-two-run-mapped" || secondFinish.FailureEntryID != "" || secondFinish.TerminalEntryID != ids.retryTwoTerminal {
		t.Fatalf("mapped retry finishes first=%#v second=%#v ids=%#v", firstFinish, secondFinish, ids)
	}
	var admissionCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM turn_admissions WHERE thread_id = 'fork'`).Scan(&admissionCount); err != nil {
		t.Fatal(err)
	}
	if admissionCount != 2 {
		t.Fatalf("mapped retry admission count=%d, want 2", admissionCount)
	}
}

func TestSQLiteForkRejectsRetryChainWithoutIntermediateTerminal(t *testing.T) {
	ctx := context.Background()
	store := buildSQLiteRetrySourceChainFixture(t, true)
	_, err := store.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: "thread", NewThreadID: "fork",
		TurnIDMap: map[string]string{"source-turn": "source-turn-mapped", "retry-one": "retry-one-mapped", "retry-two": "retry-two-mapped"},
		RunIDMap:  map[string]string{"source-run": "source-run-mapped", "retry-one-run": "retry-one-run-mapped", "retry-two-run": "retry-two-run-mapped"},
	})
	if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Fork error=%v, want ErrAuthorityCorrupt", err)
	}
	assertSQLiteForkDestinationEmpty(t, store, "fork")
}

type sqliteMappedRetryForkIDs struct {
	sourceUser        string
	retryOneStarted   string
	retryOneAssistant string
	retryOneFailure   string
	retryOneTerminal  string
	retryTwoStarted   string
	retryTwoTerminal  string
}

func assertSQLiteMappedRetryOfRetryFork(tb testing.TB, page sessiontree.CanonicalTurnsPage) sqliteMappedRetryForkIDs {
	tb.Helper()
	if len(page.Turns) != 3 || page.Turns[0].TurnID != "source-turn-mapped" || page.Turns[0].RunID != "source-run-mapped" ||
		page.Turns[1].TurnID != "retry-one-mapped" || page.Turns[1].RunID != "retry-one-run-mapped" ||
		page.Turns[2].TurnID != "retry-two-mapped" || page.Turns[2].RunID != "retry-two-run-mapped" {
		tb.Fatalf("mapped retry chain page=%#v", page)
	}
	findEntry := func(turn sessiontree.CanonicalTurn, entryType sessiontree.EntryType) string {
		for _, item := range turn.Entries {
			if item.Entry.Type == entryType {
				return item.Entry.ID
			}
		}
		return ""
	}
	findTerminal := func(turn sessiontree.CanonicalTurn) string {
		for _, item := range turn.Entries {
			entry := item.Entry
			if entry.Type == sessiontree.EntryTurnMarker && (entry.TurnStatus == sessiontree.TurnCompleted || entry.TurnStatus == sessiontree.TurnWaiting ||
				entry.TurnStatus == sessiontree.TurnFailed || entry.TurnStatus == sessiontree.TurnAborted) {
				return entry.ID
			}
		}
		return ""
	}
	ids := sqliteMappedRetryForkIDs{
		sourceUser:        findEntry(page.Turns[0], sessiontree.EntryUserMessage),
		retryOneStarted:   page.Turns[1].StartedEntryID,
		retryOneAssistant: findEntry(page.Turns[1], sessiontree.EntryAssistantMessage),
		retryOneFailure:   findEntry(page.Turns[1], sessiontree.EntryRunFailure),
		retryOneTerminal:  findTerminal(page.Turns[1]),
		retryTwoStarted:   page.Turns[2].StartedEntryID,
		retryTwoTerminal:  findTerminal(page.Turns[2]),
	}
	firstSource := page.Turns[1].RetrySource
	secondSource := page.Turns[2].RetrySource
	if ids.sourceUser == "" || ids.retryOneStarted == "" || ids.retryOneAssistant == "" || ids.retryOneFailure == "" || ids.retryOneTerminal == "" ||
		ids.retryTwoStarted == "" || ids.retryTwoTerminal == "" ||
		firstSource == nil || firstSource.TurnID != "source-turn-mapped" || firstSource.EntryID != ids.sourceUser ||
		secondSource == nil || secondSource.TurnID != "retry-one-mapped" || secondSource.EntryID != ids.retryOneAssistant {
		tb.Fatalf("mapped retry sources first=%#v second=%#v ids=%#v", firstSource, secondSource, ids)
	}
	return ids
}

func TestSQLiteForkRejectsRewriteThatRemovesStartedRunIdentity(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "fork-missing-run.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "source-turn", "source-run", "source")
	_, err = store.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: "thread",
		NewThreadID:    "rewritten",
		RunIDMap:       map[string]string{"source-run": "rewritten-run"},
		RewriteEntry: func(entry sessiontree.Entry, _ sessiontree.ForkEntryIdentity) (sessiontree.Entry, error) {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
				delete(entry.Metadata, "run_id")
			}
			return entry, nil
		},
	})
	if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("fork rewrite without started run err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.Thread(ctx, "rewritten"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("failed fork destination err=%v, want ErrThreadNotFound", err)
	}
}

func TestSQLiteVersion15MigrationAcceptsCrossThreadSharedRunIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v15-duplicate-global-run.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		seedSchemaVersion15StartedRun(t, db, "first", "turn-first", "run-shared", 1)
		seedSchemaVersion15StartedRun(t, db, "second", "turn-second", "run-shared", 1)
	})

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version, fingerprint string
	if err := store.db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_fingerprint'`).Scan(&fingerprint); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion || fingerprint != schemaFingerprintVersion16 {
		t.Fatalf("migration metadata version=%q fingerprint=%q", version, fingerprint)
	}
	for _, threadID := range []string{"first", "second"} {
		page, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: threadID, Tail: 1})
		if err != nil || len(page.Turns) != 0 {
			t.Fatalf("migrated thread %q page=%#v err=%v", threadID, page, err)
		}
		entries, err := store.Entries(context.Background(), threadID)
		if err != nil || len(entries) != 1 || entries[0].Metadata["run_id"] != "run-shared" {
			t.Fatalf("migrated thread %q entries=%#v err=%v", threadID, entries, err)
		}
	}
}

func TestSQLiteVersion15MigrationRejectsNonCanonicalStartedRunAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v15-noncanonical-global-run.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		seedSchemaVersion15StartedRun(t, db, "thread", "turn", "\trun\u00a0", 1)
	})

	if _, err := Open(path); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Open noncanonical v15 run err=%v, want ErrAuthorityCorrupt", err)
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
	var titleColumnCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('threads') WHERE name = 'title_generation'`).Scan(&titleColumnCount); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion15 || titleColumnCount != 0 {
		t.Fatalf("failed migration version=%q title_column=%d", version, titleColumnCount)
	}
}

func TestSQLiteVersion15MigrationDoesNotCreateGlobalStartedRunIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v15-global-run.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		seedSchemaVersion15StartedRun(t, db, "first", "turn-first", "run-first", 1)
		seedSchemaVersion15StartedRun(t, db, "second", "turn-second", "run-second", 1)
	})
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = 'entries_started_run_unique_idx'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("global started run index count=%d, want zero", count)
	}
}

func TestSQLiteVersion15MigrationBackfillsEntryPathDepth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v15-path-depth.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		now := time.Date(2026, 7, 21, 1, 0, 0, 0, time.UTC)
		if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at) VALUES('thread', 'user', ?, ?)`,
			formatTime(now), formatTime(now)); err != nil {
			t.Fatal(err)
		}
		if _, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
			ID: "started", ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryTurnMarker,
			TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run"}, CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := insertSchemaVersion15Entry(db, sessiontree.Entry{
			ID: "user", ThreadID: "thread", ParentID: "started", TurnID: "turn", Type: sessiontree.EntryUserMessage,
			Message: session.Message{Role: session.User, Content: "input"}, CreatedAt: now.Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	})
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.Entries(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].PathDepth != 1 || entries[1].PathDepth != 2 {
		t.Fatalf("migrated path depths = %#v", entries)
	}
}

func TestSQLiteVersion15MigrationRejectsBrokenEntryGraphAtomically(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(*testing.T, *sql.DB)
	}{
		{name: "missing parent", seed: func(t *testing.T, db *sql.DB) {
			now := time.Date(2026, 7, 21, 1, 0, 0, 0, time.UTC)
			if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at) VALUES('thread', 'orphan', ?, ?)`,
				formatTime(now), formatTime(now)); err != nil {
				t.Fatal(err)
			}
			if _, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "root", ThreadID: "thread", Type: sessiontree.EntryCustom, CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := insertSchemaVersion15Entry(db, sessiontree.Entry{ID: "orphan", ThreadID: "thread", ParentID: "missing", Type: sessiontree.EntryCustom, CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "cycle", seed: func(t *testing.T, db *sql.DB) {
			now := time.Date(2026, 7, 21, 1, 0, 0, 0, time.UTC)
			if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at) VALUES('thread', 'cycle-b', ?, ?)`,
				formatTime(now), formatTime(now)); err != nil {
				t.Fatal(err)
			}
			for _, entry := range []sessiontree.Entry{
				{ID: "root", ThreadID: "thread", Type: sessiontree.EntryCustom, CreatedAt: now},
				{ID: "cycle-a", ThreadID: "thread", ParentID: "cycle-b", Type: sessiontree.EntryCustom, CreatedAt: now},
				{ID: "cycle-b", ThreadID: "thread", ParentID: "cycle-a", Type: sessiontree.EntryCustom, CreatedAt: now},
			} {
				if _, err := insertSchemaVersion15Entry(db, entry); err != nil {
					t.Fatal(err)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "broken-v15.db")
			createSchemaVersion15TitleStore(t, path, func(db *sql.DB) { tc.seed(t, db) })
			if _, err := Open(path); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("Open broken v15 err=%v, want ErrAuthorityCorrupt", err)
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
			if version != schemaVersion15 || pathDepthColumns != 0 {
				t.Fatalf("failed migration version=%q path_depth_columns=%d", version, pathDepthColumns)
			}
		})
	}
}

func TestSQLiteVersion15PathDepthMigrationScalesLinearly(t *testing.T) {
	shortDuration := migrateLargeSchemaVersion15Journal(t, 1_000)
	longDuration := migrateLargeSchemaVersion15Journal(t, 100_000)
	if !raceDetectorEnabled && longDuration > 20*time.Second {
		t.Fatalf("100k entry path depth migration took %s", longDuration)
	}
	baseline := shortDuration
	if baseline < time.Millisecond {
		baseline = time.Millisecond
	}
	if longDuration > baseline*250 {
		t.Fatalf("path depth migration scaled superlinearly: 1k=%s 100k=%s", shortDuration, longDuration)
	}
}

func migrateLargeSchemaVersion15Journal(t *testing.T, entries int) time.Duration {
	t.Helper()
	path := filepath.Join(t.TempDir(), fmt.Sprintf("path-depth-%d.db", entries))
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		now := time.Date(2026, 7, 21, 5, 0, 0, 0, time.UTC)
		leafID := fmt.Sprintf("entry-%06d", entries)
		if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at) VALUES('thread', ?, ?, ?)`,
			leafID, formatTime(now), formatTime(now)); err != nil {
			t.Fatal(err)
		}
		prototype := sessiontree.PrepareEntry(sessiontree.Entry{Type: sessiontree.EntryCustom, CreatedAt: now})
		messageJSON, err := json.Marshal(prototype.Message)
		if err != nil {
			t.Fatal(err)
		}
		metadataJSON, err := json.Marshal(prototype.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(`WITH digits(d) AS (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)),
			numbers(n) AS (
				SELECT 1 + a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d
				FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e
			)
			INSERT INTO entries(
				thread_id, id, ordinal, parent_id, type, created_at, message_json,
				raw, raw_hash, raw_encoder_version, metadata_json
			)
			SELECT 'thread', printf('entry-%06d', n), n,
				CASE WHEN n = 1 THEN '' ELSE printf('entry-%06d', n - 1) END,
				?, ?, ?, ?, ?, 1, ?
			FROM numbers WHERE n <= ? ORDER BY n`, string(sessiontree.EntryCustom), formatTime(now), string(messageJSON),
			prototype.Raw, prototype.RawHash, string(metadataJSON), entries)
		if err != nil {
			t.Fatal(err)
		}
	})
	started := time.Now()
	store, err := Open(path)
	duration := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var depth int64
	if err := store.db.QueryRow(`SELECT path_depth FROM entries WHERE thread_id = 'thread' AND id = ?`, fmt.Sprintf("entry-%06d", entries)).Scan(&depth); err != nil {
		t.Fatal(err)
	}
	if depth != int64(entries) {
		t.Fatalf("migrated leaf depth=%d, want %d", depth, entries)
	}
	return duration
}

func TestSQLiteStartedTurnLookupUsesThreadTurnUniqueIndex(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "started-turn-query-plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN SELECT COUNT(*) FROM entries
		WHERE thread_id = ? AND turn_id = ? AND type = ? AND turn_status = ?`,
		"thread", "turn", string(sessiontree.EntryTurnMarker), string(sessiontree.TurnStarted))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "entries_started_turn_unique_idx") || strings.Contains(plan, "SCAN entries") {
		t.Fatalf("started turn query plan = %q", plan)
	}
}

func TestSQLiteListCanonicalTurnsPagesOnlyActivePath(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "canonical-page.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-1", "run-1", "one")
	secondLeaf := appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-2", "run-2", "two")
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-abandoned", "run-abandoned", "abandoned")
	if err := store.MoveLeaf(ctx, "thread", secondLeaf); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-3", "run-3", "three")

	page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 2 || page.Turns[0].TurnID != "turn-2" || page.Turns[1].TurnID != "turn-3" || !page.HasMore {
		t.Fatalf("tail page = %#v", page)
	}
	if page.LatestTurnID != "turn-3" || page.ThroughOrdinal != 6 || !page.HasRetryTarget {
		t.Fatalf("page authority = %#v", page)
	}
	if page.Turns[0].StartedOrdinal != 3 || page.Turns[1].StartedOrdinal != 5 || len(page.Turns[1].Entries) != 2 {
		t.Fatalf("page ordinals = %#v", page.Turns)
	}
	if page.BeforeCursor == nil {
		t.Fatal("tail page did not return a before cursor")
	}
	before, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{
		ThreadID: "thread", BeforeCursor: page.BeforeCursor, Limit: 1,
	})
	if err != nil || len(before.Turns) != 1 || before.Turns[0].TurnID != "turn-1" || before.HasMore {
		t.Fatalf("before page=%#v err=%v", before, err)
	}
	cursor := page.SinceCursor
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-4", "run-4", "four")
	since, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{
		ThreadID: "thread", SinceCursor: &cursor, Limit: 1,
	})
	if err != nil || len(since.Turns) != 1 || since.Turns[0].TurnID != "turn-4" || since.HasMore {
		t.Fatalf("since page=%#v err=%v", since, err)
	}
}

func TestSQLiteListCanonicalTurnsRejectsSinceCursorAfterBranch(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "canonical-since-branch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	firstLeaf := appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-1", "run-1", "one")
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-2", "run-2", "two")
	page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	cursor := page.SinceCursor
	if err := store.MoveLeaf(ctx, "thread", firstLeaf); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-branch", "run-branch", "branch")
	_, err = store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{
		ThreadID: "thread", SinceCursor: &cursor, Limit: 1,
	})
	if !errors.Is(err, sessiontree.ErrStaleCanonicalTurnCursor) {
		t.Fatalf("since branch err=%v, want ErrStaleCanonicalTurnCursor", err)
	}
}

func TestSQLiteListCanonicalTurnsRejectsEmptySinceCursor(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "canonical-empty-since.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{
		ThreadID: "thread", SinceCursor: &sessiontree.CanonicalTurnSinceCursor{}, Limit: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "since cursor requires entry identity") {
		t.Fatalf("empty since cursor err=%v", err)
	}
}

func TestSQLiteListCanonicalTurnsSinceIncludesFactsAddedToAnchorTurn(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "canonical-in-flight-since.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn", "run", "inspect")
	initial, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	cursor := initial.SinceCursor
	appended := []sessiontree.Entry{
		{ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "working"}},
		{ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "inspect"}},
		{ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "inspect", ToolResult: &session.ToolResultView{Status: "success"}}},
		{ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted},
	}
	for index, entry := range appended {
		stored, err := store.Append(ctx, entry, sessiontree.AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		page, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &cursor, Limit: 1})
		if err != nil || len(page.Turns) != 1 || page.Turns[0].TurnID != "turn" {
			t.Fatalf("increment %d page=%#v err=%v", index, page, err)
		}
		if page.SinceCursor.EntryID != stored.ID || len(page.Turns[0].Entries) != 3+index {
			t.Fatalf("increment %d page=%#v, want cursor=%q entries=%d", index, page, stored.ID, 3+index)
		}
		cursor = page.SinceCursor
	}
	empty, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &cursor, Limit: 1})
	if err != nil || len(empty.Turns) != 0 || empty.SinceCursor != cursor {
		t.Fatalf("settled incremental page=%#v err=%v", empty, err)
	}
}

func TestSQLiteListCanonicalTurnsRejectsParentCycle(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "canonical-cycle.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn", "run", "input")
	if _, err := store.db.Exec(`UPDATE entries SET parent_id = 'thread-entry-2' WHERE thread_id = 'thread' AND id = 'thread-entry-1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, sessiontree.ErrInvalidParent) {
		t.Fatalf("cycle page err=%v, want ErrInvalidParent", err)
	}
}

func TestSQLiteListCanonicalTurnsRejectsMissingStartedRunOutsidePage(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "canonical-missing-run.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-1", "run-1", "one")
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn-2", "run-2", "two")
	if _, err := store.db.Exec(`UPDATE entries SET metadata_json = '{}' WHERE thread_id = 'thread' AND turn_id = 'turn-1' AND turn_status = 'started'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("missing old run page err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLiteCanonicalTurnPageUsesConstantQueryCount(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "canonical-query-count.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 250; index++ {
		appendSQLiteCanonicalTurnFixture(t, ctx, store,
			fmt.Sprintf("turn-%03d", index), fmt.Sprintf("run-%03d", index), fmt.Sprintf("input-%03d", index))
	}
	counter := &canonicalTurnQueryCounter{runner: store.db}
	page, err := listSQLiteCanonicalTurnsWithRunner(ctx, counter, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 2 || counter.queries != 2 {
		t.Fatalf("page turns=%d queries=%d, want 2 turns and 2 queries", len(page.Turns), counter.queries)
	}
}

func TestSQLiteCanonicalTurnBeforePagesUseLinearQueryCount(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "canonical-linear-page-count.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 100; index++ {
		appendSQLiteCanonicalTurnFixture(t, ctx, store,
			fmt.Sprintf("turn-%03d", index), fmt.Sprintf("run-%03d", index), fmt.Sprintf("input-%03d", index))
	}
	counter := &canonicalTurnQueryCounter{runner: store.db}
	page, err := listSQLiteCanonicalTurnsWithRunner(ctx, counter, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 10})
	if err != nil {
		t.Fatal(err)
	}
	turns := len(page.Turns)
	for page.BeforeCursor != nil {
		page, err = listSQLiteCanonicalTurnsWithRunner(ctx, counter, sessiontree.ListCanonicalTurnsOptions{
			ThreadID: "thread", BeforeCursor: page.BeforeCursor, Limit: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		turns += len(page.Turns)
	}
	if turns != 100 || counter.queries != 29 {
		t.Fatalf("paged turns=%d queries=%d, want 100 turns and 29 queries", turns, counter.queries)
	}
}

func TestSQLiteCanonicalTurnAncestorQueryPlanIsBounded(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "canonical-query-plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN `+sqliteAncestorChunkQuery(), "thread", "leaf", 64)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	plan := strings.Join(details, "\n")
	for _, forbidden := range []string{"SCAN ENTRIES", "USE TEMP B-TREE", "COUNT(", "WINDOW"} {
		if strings.Contains(strings.ToUpper(plan), forbidden) {
			t.Fatalf("ancestor query plan contains %q: %s", forbidden, plan)
		}
	}
	if !strings.Contains(plan, "sqlite_autoindex_entries_1") {
		t.Fatalf("ancestor query plan does not use entry identity index: %s", plan)
	}
}

func TestSQLiteCanonicalTurnPageAllocationsDoNotScaleWithThreadLength(t *testing.T) {
	shortStore, shortSince := buildSQLiteCanonicalReferenceFixture(t, 1_000)
	longStore, longSince := buildSQLiteCanonicalReferenceFixture(t, 100_000)
	for _, mode := range []string{"tail", "before", "since"} {
		shortAllocs := sqliteCanonicalPageAllocs(t, shortStore, shortSince, mode)
		longAllocs := sqliteCanonicalPageAllocs(t, longStore, longSince, mode)
		if longAllocs > shortAllocs*1.10 {
			t.Fatalf("%s allocations scaled with thread length: 1k=%f 100k=%f", mode, shortAllocs, longAllocs)
		}
	}
}

func TestSQLiteRetryEligibilityIsBoundedByExactSourceIdentity(t *testing.T) {
	shortStore := buildSQLiteDeepRetryFixture(t, 1_000, true)
	longStore := buildSQLiteDeepRetryFixture(t, 100_000, true)
	shortAllocs, shortQueries := sqliteDeepRetryPageCost(t, shortStore)
	longAllocs, longQueries := sqliteDeepRetryPageCost(t, longStore)
	if shortQueries != longQueries || longQueries > 40 {
		t.Fatalf("retry eligibility queries changed with source depth: 1k=%d 100k=%d", shortQueries, longQueries)
	}
	if longAllocs > shortAllocs*1.10 {
		t.Fatalf("retry eligibility allocations scaled with source depth: 1k=%f 100k=%f", shortAllocs, longAllocs)
	}
}

func TestSQLiteRetryEligibilityQueriesScaleLinearlyWithSourceChain(t *testing.T) {
	queries := make(map[int]int)
	allocations := make(map[int]float64)
	for _, chainLength := range []int{1, 8, 32} {
		store := buildSQLiteDeepRetryChainFixture(t, 1_000, chainLength, true)
		allocations[chainLength], queries[chainLength] = sqliteDeepRetryPageCost(t, store)
	}
	perHop := queries[8] - queries[1]
	if perHop <= 0 || perHop%7 != 0 || queries[32]-queries[1] != (perHop/7)*31 {
		t.Fatalf("retry chain queries are not linear: one=%d eight=%d thirty-two=%d", queries[1], queries[8], queries[32])
	}
	if perHop/7 > 4 {
		t.Fatalf("retry chain uses %d indexed queries per hop, want at most 4", perHop/7)
	}
	shortPerHop := (allocations[8] - allocations[1]) / 7
	longPerHop := (allocations[32] - allocations[1]) / 31
	if shortPerHop <= 0 || longPerHop < shortPerHop*0.75 || longPerHop > shortPerHop*1.25 {
		t.Fatalf("retry chain allocations are not linear: one=%f eight=%f thirty-two=%f", allocations[1], allocations[8], allocations[32])
	}
}

func TestSQLiteRetryEligibilityFollowsExactRetrySourceChain(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable-%t", durable), func(t *testing.T) {
			store := buildSQLiteRetrySourceChainFixture(t, durable)
			page, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
			if err != nil || page.HasRetryTarget != durable {
				t.Fatalf("retry chain page=%#v err=%v, want eligible=%t", page, err, durable)
			}
		})
	}
}

func TestSQLiteRetryEligibilityRejectsCrossBranchSource(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "cross-branch-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedCrossBranchRetrySource(t, context.Background(), store)
	insertSQLiteRetryAdmissionFact(t, store.db, "thread", "retry-turn", "retry-run", "retry-started", "off-user", time.Now().UTC())

	if _, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.Fork(context.Background(), sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
	}
	assertSQLiteForkDestinationEmpty(t, store, "fork")
}

func TestSQLiteRetryEligibilityRejectsAdmissionBaseLeafDrift(t *testing.T) {
	store := buildSQLiteRetrySourceChainFixture(t, true)
	if _, err := store.db.Exec(`UPDATE turn_admissions SET base_leaf_id = 'source-user' WHERE thread_id = 'thread' AND turn_id = 'retry-two'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLiteRetryEligibilityRejectsAbandonedSavePointSibling(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "abandoned-save-point-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry, parentID string) {
		t.Helper()
		if _, err := store.Append(ctx, entry, sessiontree.AppendOptions{ID: entry.ID, ParentID: parentID}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "source-started", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "source-run"}}, "")
	appendEntry(sessiontree.Entry{ID: "source-user", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect"}}, "source-started")
	appendEntry(sessiontree.Entry{ID: "source-assistant", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "partial"}}, "source-user")
	appendEntry(sessiontree.Entry{ID: "abandoned-save-point", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint}, "source-assistant")
	appendEntry(sessiontree.Entry{ID: "active-save-point", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint}, "source-assistant")
	appendEntry(sessiontree.Entry{
		ID: "retry-started", ThreadID: "thread", TurnID: "retry-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
		Metadata: map[string]string{"run_id": "retry-run", sessiontree.RetrySourceTurnIDMetadataKey: "source-turn", sessiontree.RetrySourceEntryIDMetadataKey: "source-assistant"},
	}, "active-save-point")
	appendEntry(sessiontree.Entry{ID: "retry-assistant", ThreadID: "thread", TurnID: "retry-turn", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "retried"}}, "retry-started")
	appendEntry(sessiontree.Entry{ID: "retry-terminal", ThreadID: "thread", TurnID: "retry-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted}, "retry-assistant")
	insertSQLiteRetryAdmissionFact(t, store.db, "thread", "retry-turn", "retry-run", "retry-started", "source-assistant", time.Now().UTC())

	if _, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
	}
	assertSQLiteForkDestinationEmpty(t, store, "fork")
}

func TestSQLiteRetryEligibilityRejectsExplicitSourceCycle(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "retry-source-cycle.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry, parentID string) {
		t.Helper()
		if _, err := store.Append(ctx, entry, sessiontree.AppendOptions{ID: entry.ID, ParentID: parentID}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "root", ThreadID: "thread", Type: sessiontree.EntryCustom}, "")
	appendEntry(sessiontree.Entry{ID: "retry-a-started", ThreadID: "thread", TurnID: "retry-a", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "retry-a-run", sessiontree.RetrySourceTurnIDMetadataKey: "retry-b", sessiontree.RetrySourceEntryIDMetadataKey: "retry-b-assistant"}}, "root")
	appendEntry(sessiontree.Entry{ID: "retry-a-assistant", ThreadID: "thread", TurnID: "retry-a", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "a"}}, "retry-a-started")
	appendEntry(sessiontree.Entry{ID: "retry-a-save-point", ThreadID: "thread", TurnID: "retry-a", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint}, "retry-a-assistant")
	appendEntry(sessiontree.Entry{ID: "retry-b-started", ThreadID: "thread", TurnID: "retry-b", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "retry-b-run", sessiontree.RetrySourceTurnIDMetadataKey: "retry-a", sessiontree.RetrySourceEntryIDMetadataKey: "retry-a-assistant"}}, "retry-a-save-point")
	appendEntry(sessiontree.Entry{ID: "retry-b-assistant", ThreadID: "thread", TurnID: "retry-b", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "b"}}, "retry-b-started")
	appendEntry(sessiontree.Entry{ID: "retry-b-save-point", ThreadID: "thread", TurnID: "retry-b", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint}, "retry-b-assistant")
	now := time.Now().UTC()
	insertSQLiteRetryAdmissionFact(t, store.db, "thread", "retry-a", "retry-a-run", "retry-a-started", "retry-b-assistant", now)
	insertSQLiteRetryAdmissionFact(t, store.db, "thread", "retry-b", "retry-b-run", "retry-b-started", "retry-a-assistant", now)

	if _, err := store.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
	}
	assertSQLiteForkDestinationEmpty(t, store, "fork")
}

func seedCrossBranchRetrySource(tb testing.TB, ctx context.Context, store *Store) {
	tb.Helper()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry, parentID string) {
		tb.Helper()
		if _, err := store.Append(ctx, entry, sessiontree.AppendOptions{ID: entry.ID, ParentID: parentID}); err != nil {
			tb.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "root", ThreadID: "thread", Type: sessiontree.EntryCustom}, "")
	appendEntry(sessiontree.Entry{ID: "off-started", ThreadID: "thread", TurnID: "off-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "off-run"}}, "root")
	appendEntry(sessiontree.Entry{ID: "off-user", ThreadID: "thread", TurnID: "off-turn", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "off-branch input"}}, "off-started")
	appendEntry(sessiontree.Entry{ID: "off-terminal", ThreadID: "thread", TurnID: "off-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted}, "off-user")
	appendEntry(sessiontree.Entry{ID: "active-started", ThreadID: "thread", TurnID: "active-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "active-run"}}, "root")
	appendEntry(sessiontree.Entry{ID: "active-user", ThreadID: "thread", TurnID: "active-turn", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "active input"}}, "active-started")
	appendEntry(sessiontree.Entry{ID: "active-terminal", ThreadID: "thread", TurnID: "active-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted}, "active-user")
	appendEntry(sessiontree.Entry{
		ID: "retry-started", ThreadID: "thread", TurnID: "retry-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
		Metadata: map[string]string{"run_id": "retry-run", sessiontree.RetrySourceTurnIDMetadataKey: "off-turn", sessiontree.RetrySourceEntryIDMetadataKey: "off-user"},
	}, "active-terminal")
	appendEntry(sessiontree.Entry{ID: "retry-assistant", ThreadID: "thread", TurnID: "retry-turn", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "retried"}}, "retry-started")
	appendEntry(sessiontree.Entry{ID: "retry-terminal", ThreadID: "thread", TurnID: "retry-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted}, "retry-assistant")
}

func TestSQLiteRetryEligibilityQueryPlansUseCanonicalIndexes(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "retry-eligibility-plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, test := range []struct {
		name  string
		query string
		args  []any
		index string
	}{
		{name: "admission", query: sqliteTurnAdmissionQuery(), args: []any{"thread", "turn"}, index: "sqlite_autoindex_turn_admissions_1"},
		{name: "source turn", query: sqliteRetrySourceTurnQuery(), args: []any{"thread", "turn"}, index: "entries_turn_ordinal_idx"},
		{name: "source ancestor", query: sqliteRetrySourceAncestorQuery(), args: []any{"thread", "descendant", "thread", 10, "source", 10}, index: "sqlite_autoindex_entries_1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, err := store.db.Query(`EXPLAIN QUERY PLAN `+test.query, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var details []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				details = append(details, detail)
			}
			plan := strings.Join(details, "\n")
			if !strings.Contains(plan, test.index) || strings.Contains(strings.ToUpper(plan), "SCAN ENTRIES") {
				t.Fatalf("query plan=%q, want index %q", plan, test.index)
			}
		})
	}
}

func BenchmarkSQLiteRetryEligibilityBySourceDepth(b *testing.B) {
	for _, depth := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("depth-%d", depth), func(b *testing.B) {
			store := buildSQLiteDeepRetryFixture(b, depth, true)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				page, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
				if err != nil || !page.HasRetryTarget {
					b.Fatalf("retry page=%#v err=%v", page, err)
				}
			}
		})
	}
}

func sqliteDeepRetryPageCost(t *testing.T, store *Store) (float64, int) {
	t.Helper()
	allocs := testing.AllocsPerRun(5, func() {
		page, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
		if err != nil || !page.HasRetryTarget {
			panic(fmt.Sprintf("retry page=%#v err=%v", page, err))
		}
	})
	counter := &canonicalTurnQueryCounter{runner: store.db}
	page, err := listSQLiteCanonicalTurnsWithRunner(context.Background(), counter, sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil || !page.HasRetryTarget {
		t.Fatalf("counted retry page=%#v err=%v", page, err)
	}
	return allocs, counter.queries
}

func buildSQLiteDeepRetryFixture(tb testing.TB, depth int, durable bool) *Store {
	return buildSQLiteDeepRetryChainFixture(tb, depth, 8, durable)
}

func buildSQLiteDeepRetryChainFixture(tb testing.TB, depth, chainLength int, durable bool) *Store {
	tb.Helper()
	if chainLength <= 0 {
		tb.Fatal("retry chain length must be positive")
	}
	store, err := Open(filepath.Join(tb.TempDir(), "deep-retry.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	tx, err := store.db.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 9, 30, 0, 0, time.UTC)
	if _, err := tx.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES('thread', ?, ?)`, formatTime(now), formatTime(now)); err != nil {
		_ = tx.Rollback()
		tb.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO entries(
		thread_id, id, ordinal, parent_id, path_depth, type, turn_id, created_at,
		message_json, raw, raw_hash, raw_encoder_version, turn_status, metadata_json
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		tb.Fatal(err)
	}
	defer stmt.Close()
	parentID := ""
	ordinal := int64(0)
	insert := func(entry sessiontree.Entry) {
		ordinal++
		entry.PathDepth = ordinal
		messageJSON, marshalErr := json.Marshal(entry.Message)
		if marshalErr != nil {
			_ = tx.Rollback()
			tb.Fatal(marshalErr)
		}
		metadataJSON, marshalErr := json.Marshal(entry.Metadata)
		if marshalErr != nil {
			_ = tx.Rollback()
			tb.Fatal(marshalErr)
		}
		if _, execErr := stmt.Exec(entry.ThreadID, entry.ID, ordinal, entry.ParentID, entry.PathDepth, string(entry.Type), entry.TurnID,
			formatTime(entry.CreatedAt), string(messageJSON), entry.Raw, entry.RawHash, string(entry.TurnStatus), string(metadataJSON)); execErr != nil {
			_ = tx.Rollback()
			tb.Fatal(execErr)
		}
		parentID = entry.ID
	}
	for index := 0; index < depth; index++ {
		insert(sessiontree.PrepareEntry(sessiontree.Entry{
			ID: fmt.Sprintf("prefix-%06d", index), ThreadID: "thread", ParentID: parentID, Type: sessiontree.EntryCustom, CreatedAt: now,
		}))
	}
	content := ""
	if durable {
		content = "durable input"
	}
	sourceStarted := sessiontree.PrepareEntry(sessiontree.Entry{
		ID: "source-started", ThreadID: "thread", ParentID: parentID, TurnID: "source-turn", Type: sessiontree.EntryTurnMarker,
		TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "source-run"}, CreatedAt: now,
	})
	sourceUser := sessiontree.PrepareEntry(sessiontree.Entry{
		ID: "source-user", ThreadID: "thread", ParentID: sourceStarted.ID, TurnID: "source-turn", Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: content, References: []session.MessageReference{{ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "quote", Text: "selection"}}}, CreatedAt: now,
	})
	insert(sourceStarted)
	insert(sourceUser)
	sourceTerminal := sessiontree.PrepareEntry(sessiontree.Entry{ID: "source-terminal", ThreadID: "thread", ParentID: sourceUser.ID, TurnID: "source-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, CreatedAt: now})
	insert(sourceTerminal)
	sourceTurnID := "source-turn"
	sourceEntryID := sourceUser.ID
	for index := 0; index < chainLength; index++ {
		turnID := fmt.Sprintf("retry-%03d", index)
		started := sessiontree.PrepareEntry(sessiontree.Entry{
			ID: turnID + "-started", ThreadID: "thread", ParentID: parentID, TurnID: turnID, Type: sessiontree.EntryTurnMarker,
			TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{
				"run_id": turnID + "-run", sessiontree.RetrySourceTurnIDMetadataKey: sourceTurnID, sessiontree.RetrySourceEntryIDMetadataKey: sourceEntryID,
			}, CreatedAt: now,
		})
		assistant := sessiontree.PrepareEntry(sessiontree.Entry{
			ID: turnID + "-assistant", ThreadID: "thread", ParentID: started.ID, TurnID: turnID, Type: sessiontree.EntryAssistantMessage,
			Message: session.Message{Role: session.Assistant, Content: "retried"}, CreatedAt: now,
		})
		status := sessiontree.TurnSavePoint
		terminalID := turnID + "-save-point"
		if index == chainLength-1 {
			status = sessiontree.TurnCompleted
			terminalID = turnID + "-terminal"
		}
		terminal := sessiontree.PrepareEntry(sessiontree.Entry{ID: terminalID, ThreadID: "thread", ParentID: assistant.ID, TurnID: turnID, Type: sessiontree.EntryTurnMarker, TurnStatus: status, CreatedAt: now})
		insert(started)
		insert(assistant)
		insert(terminal)
		insertSQLiteRetryAdmissionFact(tb, tx, "thread", turnID, turnID+"-run", started.ID, sourceEntryID, now)
		sourceTurnID = turnID
		sourceEntryID = assistant.ID
	}
	if _, err := tx.Exec(`UPDATE threads SET leaf_id = ?, updated_at = ? WHERE id = 'thread'`, parentID, formatTime(now)); err != nil {
		_ = tx.Rollback()
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	return store
}

func buildSQLiteSettledRetrySourceChainFixture(tb testing.TB) *Store {
	tb.Helper()
	ctx := context.Background()
	store, err := Open(filepath.Join(tb.TempDir(), "settled-retry-chain.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	admit := func(req sessiontree.AdmitTurnRequest) sessiontree.AdmitTurnResult {
		tb.Helper()
		fingerprint, err := sessiontree.TurnAdmissionRequestFingerprint(req)
		if err != nil {
			tb.Fatal(err)
		}
		req.RequestFingerprint = fingerprint
		result, err := store.AdmitTurn(ctx, req)
		if err != nil {
			tb.Fatal(err)
		}
		return result
	}
	original := admit(sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "source-turn", RunID: "source-run", OwnerID: "source-owner",
		Input: session.Message{Role: session.User, Content: "durable input", References: []session.MessageReference{{
			ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "quote", Text: "selection",
		}}},
	})
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: original.Lease, RunID: "source-run", TerminalEntryID: "source-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "source-outcome",
	}); err != nil {
		tb.Fatal(err)
	}
	first := admit(sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "retry-one", RunID: "retry-one-run", OwnerID: "retry-one-owner",
		RetrySourceTurnID: "source-turn", RetrySourceEntryID: original.UserMessage.ID,
	})
	firstAssistant, err := store.Append(sessiontree.ContextWithTurnLease(ctx, first.Lease), sessiontree.Entry{
		ThreadID: "thread", TurnID: "retry-one", Type: sessiontree.EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "partial"},
	}, sessiontree.AppendOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := store.Append(sessiontree.ContextWithTurnLease(ctx, first.Lease), sessiontree.Entry{
		ThreadID: "thread", TurnID: "retry-one", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint,
	}, sessiontree.AppendOptions{}); err != nil {
		tb.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: first.Lease, RunID: "retry-one-run", TerminalEntryID: "retry-one-terminal", Status: sessiontree.TurnFailed,
		FailureMessage: "retry failed", Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider},
		OutcomeFingerprint: "retry-one-outcome",
	}); err != nil {
		tb.Fatal(err)
	}
	second := admit(sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "retry-two", RunID: "retry-two-run", OwnerID: "retry-two-owner",
		RetrySourceTurnID: "retry-one", RetrySourceEntryID: firstAssistant.ID,
	})
	if _, err := store.Append(sessiontree.ContextWithTurnLease(ctx, second.Lease), sessiontree.Entry{
		ThreadID: "thread", TurnID: "retry-two", Type: sessiontree.EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "done"},
	}, sessiontree.AppendOptions{}); err != nil {
		tb.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: second.Lease, RunID: "retry-two-run", TerminalEntryID: "retry-two-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "retry-two-outcome",
	}); err != nil {
		tb.Fatal(err)
	}
	return store
}

func buildSQLiteRetrySourceChainFixture(tb testing.TB, durable bool) *Store {
	tb.Helper()
	store, err := Open(filepath.Join(tb.TempDir(), "retry-chain.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	content := ""
	if durable {
		content = "durable input"
	}
	entries := []sessiontree.Entry{
		{ID: "source-started", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "source-run"}},
		{
			ID: "source-user", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryUserMessage,
			Message: session.Message{Role: session.User, Content: content, References: []session.MessageReference{{
				ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "quote", Text: "selection",
			}}},
		},
		{ID: "source-terminal", ThreadID: "thread", TurnID: "source-turn", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted},
		{ID: "retry-one-started", ThreadID: "thread", TurnID: "retry-one", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "retry-one-run", sessiontree.RetrySourceTurnIDMetadataKey: "source-turn", sessiontree.RetrySourceEntryIDMetadataKey: "source-user"}},
		{ID: "retry-one-assistant", ThreadID: "thread", TurnID: "retry-one", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "partial"}},
		{ID: "retry-one-save-point", ThreadID: "thread", TurnID: "retry-one", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint},
		{ID: "retry-two-started", ThreadID: "thread", TurnID: "retry-two", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "retry-two-run", sessiontree.RetrySourceTurnIDMetadataKey: "retry-one", sessiontree.RetrySourceEntryIDMetadataKey: "retry-one-assistant"}},
		{ID: "retry-two-assistant", ThreadID: "thread", TurnID: "retry-two", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "done"}},
		{ID: "retry-two-terminal", ThreadID: "thread", TurnID: "retry-two", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted},
	}
	for _, entry := range entries {
		if _, err := store.Append(ctx, entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			tb.Fatal(err)
		}
	}
	now := time.Now().UTC()
	insertSQLiteRetryAdmissionFact(tb, store.db, "thread", "retry-one", "retry-one-run", "retry-one-started", "source-user", now)
	insertSQLiteRetryAdmissionFact(tb, store.db, "thread", "retry-two", "retry-two-run", "retry-two-started", "retry-one-assistant", now)
	return store
}

type sqliteTestExecer interface {
	Exec(string, ...any) (sql.Result, error)
}

func assertSQLiteForkDestinationEmpty(tb testing.TB, store *Store, threadID string) {
	tb.Helper()
	if _, err := store.Thread(context.Background(), threadID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		tb.Fatalf("rejected fork destination error = %v, want ErrThreadNotFound", err)
	}
	for _, target := range []struct{ table, column string }{
		{table: "threads", column: "id"},
		{table: "entries", column: "thread_id"},
		{table: "turn_admissions", column: "thread_id"},
		{table: "turn_finishes", column: "thread_id"},
	} {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM `+target.table+` WHERE `+target.column+` = ?`, threadID).Scan(&count); err != nil {
			tb.Fatal(err)
		}
		if count != 0 {
			tb.Fatalf("rejected fork left %d rows in %s for destination %q", count, target.table, threadID)
		}
	}
}

func seedSQLiteAdmittedRetryForkSource(tb testing.TB, ctx context.Context, store *Store) {
	tb.Helper()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		tb.Fatal(err)
	}
	entries := []sessiontree.Entry{
		{ID: "source-started", ThreadID: "source", TurnID: "turn-original", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": "run-original"}},
		{ID: "source-user", ThreadID: "source", TurnID: "turn-original", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "original"}, Metadata: map[string]string{
			"run_id": "target-run", sessiontree.RetrySourceTurnIDMetadataKey: "turn-original", sessiontree.RetrySourceEntryIDMetadataKey: "source-started",
		}},
		{ID: "source-terminal", ThreadID: "source", TurnID: "turn-original", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted},
		{ID: "source-custom", ThreadID: "source", Type: sessiontree.EntryCustom, Metadata: map[string]string{"kind": "ordinary"}},
	}
	for _, entry := range entries {
		if _, err := store.Append(ctx, entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			tb.Fatal(err)
		}
	}
	request := sessiontree.AdmitTurnRequest{
		ThreadID: "source", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-retry",
		RetrySourceTurnID: "turn-original", RetrySourceEntryID: "source-user",
	}
	var err error
	request.RequestFingerprint, err = sessiontree.TurnAdmissionRequestFingerprint(request)
	if err != nil {
		tb.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, request)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-retry", TerminalEntryID: "retry-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "outcome-retry",
	}); err != nil {
		tb.Fatal(err)
	}
}

func insertSQLiteRetryAdmissionFact(tb testing.TB, db sqliteTestExecer, threadID, turnID, runID, startedEntryID, sourceEntryID string, now time.Time) {
	tb.Helper()
	if _, err := db.Exec(`INSERT INTO turn_admissions(
		thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
		acquired_at, renewed_at, expires_at, turn_started_id, user_message_id, base_leaf_id
	) VALUES(?, ?, ?, ?, 'historical', 1, 0, ?, ?, ?, ?, NULL, ?)`,
		threadID, turnID, runID, "historical-"+turnID,
		formatTime(now), formatTime(now), formatTime(now.Add(time.Minute)), startedEntryID, sourceEntryID); err != nil {
		tb.Fatal(err)
	}
}

func BenchmarkThreadTurnsWithReferences(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("sqlite/%d", size), func(b *testing.B) {
			store, since := buildSQLiteCanonicalReferenceFixture(b, size)
			tail, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20})
			if err != nil || tail.BeforeCursor == nil {
				b.Fatalf("prepare cursors page=%#v err=%v", tail, err)
			}
			queries := map[string]sessiontree.ListCanonicalTurnsOptions{
				"tail":   {ThreadID: "thread", Tail: 20},
				"before": {ThreadID: "thread", BeforeCursor: tail.BeforeCursor, Limit: 20},
				"since":  {ThreadID: "thread", SinceCursor: &since, Limit: 20},
			}
			for _, mode := range []string{"tail", "before", "since"} {
				opts := queries[mode]
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					for index := 0; index < b.N; index++ {
						if _, err := store.ListCanonicalTurns(context.Background(), opts); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkThreadTurnsWithReferences200x10(b *testing.B) {
	b.Run("sqlite/200x10", func(b *testing.B) {
		store, since := buildSQLiteCanonicalReferenceFixtureWithReferences(b, 200, sqliteCanonicalBenchmarkReferencesTen())
		tail, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20})
		if err != nil || tail.BeforeCursor == nil {
			b.Fatalf("prepare cursors page=%#v err=%v", tail, err)
		}
		queries := map[string]sessiontree.ListCanonicalTurnsOptions{
			"tail":   {ThreadID: "thread", Tail: 20},
			"before": {ThreadID: "thread", BeforeCursor: tail.BeforeCursor, Limit: 20},
			"since":  {ThreadID: "thread", SinceCursor: &since, Limit: 20},
		}
		for mode, opts := range queries {
			b.Run(mode, func(b *testing.B) {
				b.ReportAllocs()
				for index := 0; index < b.N; index++ {
					if _, err := store.ListCanonicalTurns(context.Background(), opts); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
}

func sqliteCanonicalPageAllocs(t *testing.T, store *Store, since sessiontree.CanonicalTurnSinceCursor, mode string) float64 {
	t.Helper()
	tail, err := store.ListCanonicalTurns(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20})
	if err != nil || tail.BeforeCursor == nil {
		t.Fatalf("prepare %s cursor page=%#v err=%v", mode, tail, err)
	}
	opts := sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20}
	switch mode {
	case "before":
		opts = sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", BeforeCursor: tail.BeforeCursor, Limit: 20}
	case "since":
		opts = sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &since, Limit: 20}
	}
	return testing.AllocsPerRun(5, func() {
		if _, err := store.ListCanonicalTurns(context.Background(), opts); err != nil {
			panic(err)
		}
	})
}

func buildSQLiteCanonicalReferenceFixture(tb testing.TB, turns int) (*Store, sessiontree.CanonicalTurnSinceCursor) {
	return buildSQLiteCanonicalReferenceFixtureWithReferences(tb, turns, sqliteCanonicalBenchmarkReferences())
}

func buildSQLiteCanonicalReferenceFixtureWithReferences(tb testing.TB, turns int, references []session.MessageReference) (*Store, sessiontree.CanonicalTurnSinceCursor) {
	tb.Helper()
	store, err := Open(filepath.Join(tb.TempDir(), "canonical-references.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	tx, err := store.db.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 2, 0, 0, 0, time.UTC)
	if _, err := tx.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES('thread', ?, ?)`, formatTime(now), formatTime(now)); err != nil {
		_ = tx.Rollback()
		tb.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO entries(
		thread_id, id, ordinal, parent_id, path_depth, type, turn_id, created_at,
		message_json, raw, raw_hash, raw_encoder_version, turn_status, metadata_json
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		tb.Fatal(err)
	}
	defer stmt.Close()
	parentID := ""
	ordinal := int64(0)
	since := sessiontree.CanonicalTurnSinceCursor{}
	for index := 0; index < turns; index++ {
		turnID := fmt.Sprintf("turn-%06d", index)
		started := sessiontree.PrepareEntry(sessiontree.Entry{
			ID: fmt.Sprintf("entry-%06d-started", index), ThreadID: "thread", ParentID: parentID,
			TurnID: turnID, Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
			Metadata: map[string]string{"run_id": fmt.Sprintf("run-%06d", index)}, CreatedAt: now,
		})
		user := sessiontree.PrepareEntry(sessiontree.Entry{
			ID: fmt.Sprintf("entry-%06d-user", index), ThreadID: "thread", ParentID: started.ID,
			TurnID: turnID, Type: sessiontree.EntryUserMessage, CreatedAt: now,
			Message: session.Message{Role: session.User, Content: "inspect references", References: references},
		})
		for _, entry := range []sessiontree.Entry{started, user} {
			ordinal++
			entry.PathDepth = ordinal
			messageJSON, marshalErr := json.Marshal(entry.Message)
			if marshalErr != nil {
				_ = tx.Rollback()
				tb.Fatal(marshalErr)
			}
			metadataJSON, marshalErr := json.Marshal(entry.Metadata)
			if marshalErr != nil {
				_ = tx.Rollback()
				tb.Fatal(marshalErr)
			}
			if _, execErr := stmt.Exec(entry.ThreadID, entry.ID, ordinal, entry.ParentID, entry.PathDepth,
				string(entry.Type), entry.TurnID, formatTime(entry.CreatedAt), string(messageJSON),
				entry.Raw, entry.RawHash, string(entry.TurnStatus), string(metadataJSON)); execErr != nil {
				_ = tx.Rollback()
				tb.Fatal(execErr)
			}
		}
		parentID = user.ID
		if index == turns-21 {
			since.EntryID = user.ID
		}
	}
	if _, err := tx.Exec(`UPDATE threads SET leaf_id = ?, updated_at = ? WHERE id = 'thread'`, parentID, formatTime(now)); err != nil {
		_ = tx.Rollback()
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	return store, since
}

func sqliteCanonicalBenchmarkReferences() []session.MessageReference {
	return []session.MessageReference{
		{ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "Selected text", Text: "the exact referenced text"},
		{ReferenceID: "file", Kind: session.MessageReferenceFile, Label: "config.go", ResourceRef: "file:///workspace/config.go", Text: "package config"},
	}
}

func sqliteCanonicalBenchmarkReferencesTen() []session.MessageReference {
	references := sqliteCanonicalBenchmarkReferences()
	for index := len(references); index < 10; index++ {
		references = append(references, session.MessageReference{
			ReferenceID: fmt.Sprintf("reference-%d", index), Kind: session.MessageReferenceText,
			Label: fmt.Sprintf("Reference %d", index), Text: fmt.Sprintf("selected text %d", index),
		})
	}
	return references
}

type canonicalTurnQueryCounter struct {
	runner  sqlRunner
	queries int
}

func (c *canonicalTurnQueryCounter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.runner.ExecContext(ctx, query, args...)
}

func (c *canonicalTurnQueryCounter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	c.queries++
	return c.runner.QueryContext(ctx, query, args...)
}

func (c *canonicalTurnQueryCounter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	c.queries++
	return c.runner.QueryRowContext(ctx, query, args...)
}

func seedSchemaVersion15StartedRun(t *testing.T, db *sql.DB, threadID, turnID, runID string, ordinal int64) {
	t.Helper()
	entryID := threadID + "-started"
	created := time.Date(2026, 7, 21, 0, 0, int(ordinal), 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO threads(id, leaf_id, created_at, updated_at) VALUES(?, ?, ?, ?)`, threadID, entryID, formatTime(created), formatTime(created)); err != nil {
		t.Fatal(err)
	}
	entry := sessiontree.PrepareEntry(sessiontree.Entry{
		ThreadID: threadID, ID: entryID, Type: sessiontree.EntryTurnMarker, TurnID: turnID,
		TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": runID}, CreatedAt: created,
	})
	if _, err := insertSchemaVersion15Entry(db, entry); err != nil {
		t.Fatal(err)
	}
}

type sqliteCanonicalTurnTest interface {
	Helper()
	Fatal(...any)
}

func appendSQLiteCanonicalTurnFixture(t sqliteCanonicalTurnTest, ctx context.Context, store *Store, turnID, runID, input string) string {
	t.Helper()
	_, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "thread", TurnID: turnID, Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted,
		Metadata: map[string]string{"run_id": runID},
	}, sessiontree.AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	user, err := store.Append(ctx, sessiontree.Entry{
		ThreadID: "thread", TurnID: turnID, Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: input},
	}, sessiontree.AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return user.ID
}
