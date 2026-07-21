package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
)

func TestStartedRunIdentityIsScopedByThreadAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) Repo
	}{
		{name: "memory", repo: func(*testing.T) Repo { return NewMemoryRepo() }},
		{name: "file", repo: func(t *testing.T) Repo { return NewFileRepo(filepath.Join(t.TempDir(), "threads")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := tc.repo(t)
			for _, threadID := range []string{"first", "second"} {
				if _, err := repo.CreateThread(ctx, ThreadMeta{ID: threadID}); err != nil {
					t.Fatal(err)
				}
			}
			appendCanonicalTurnFixture(t, ctx, repo, "first", "turn", "run-shared", "first input")
			appendCanonicalTurnFixture(t, ctx, repo, "second", "turn", "run-shared", "second input")
			for _, want := range []struct{ threadID, input string }{{"first", "first input"}, {"second", "second input"}} {
				page, err := repo.(CanonicalTurnPageRepo).ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: want.threadID, Tail: 1})
				if err != nil || len(page.Turns) != 1 || page.Turns[0].TurnID != "turn" || page.Turns[0].RunID != "run-shared" ||
					len(page.Turns[0].Entries) != 2 || page.Turns[0].Entries[1].Entry.Message.Content != want.input {
					t.Fatalf("thread %q page=%#v err=%v", want.threadID, page, err)
				}
			}
		})
	}
}

func TestMemoryTurnAdmissionReplayUsesExplicitThreadTurnRunIdentity(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	for _, threadID := range []string{"first", "second"} {
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: threadID}); err != nil {
			t.Fatal(err)
		}
	}
	for _, threadID := range []string{"first", "second"} {
		if _, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
			ThreadID: threadID, TurnID: "turn", RunID: "run-shared", OwnerID: "owner-" + threadID,
			RequestFingerprint: "request-" + threadID, Input: session.Message{Role: session.User, Content: threadID},
		}); err != nil {
			t.Fatalf("admit thread %q: %v", threadID, err)
		}
		replayed, found, err := repo.ReadTurnAdmission(ctx, threadID, "turn", "run-shared")
		if err != nil || !found || !replayed.Replayed || replayed.UserMessage.Message.Content != threadID {
			t.Fatalf("replay thread %q result=%#v found=%v err=%v", threadID, replayed, found, err)
		}
	}
}

func TestFileRepoReopensCrossThreadSharedRunIdentity(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	repo := NewFileRepo(root)
	for _, threadID := range []string{"first", "second"} {
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: threadID}); err != nil {
			t.Fatal(err)
		}
	}
	appendCanonicalTurnFixture(t, ctx, repo, "first", "turn", "run-shared", "first")
	appendCanonicalTurnFixture(t, ctx, repo, "second", "turn", "run-shared", "second")
	reopened := NewFileRepo(root)
	for _, threadID := range []string{"first", "second"} {
		page, err := reopened.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: threadID, Tail: 1})
		if err != nil || len(page.Turns) != 1 || page.Turns[0].RunID != "run-shared" {
			t.Fatalf("reopen thread %q page=%#v err=%v", threadID, page, err)
		}
	}
}

func TestForkPreservesExplicitRunIdentityAcrossThreads(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) Repo
	}{
		{name: "memory", repo: func(*testing.T) Repo { return NewMemoryRepo() }},
		{name: "file", repo: func(t *testing.T) Repo { return NewFileRepo(filepath.Join(t.TempDir(), "threads")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := tc.repo(t)
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
				t.Fatal(err)
			}
			appendCanonicalTurnFixture(t, ctx, repo, "source", "source-turn", "source-run", "source")
			if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"}); err != nil {
				t.Fatalf("fork preserving run identity: %v", err)
			}
			if entries, found, err := repo.(CanonicalTurnRepo).CanonicalTurnEntries(ctx, "fork", "source-turn", "source-run"); err != nil || !found || len(entries) == 0 {
				t.Fatalf("fork canonical entries=%#v found=%v err=%v", entries, found, err)
			}
		})
	}
}

func TestForkRewritesRetrySourceAcrossMemoryAndFileReopen(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) (Repo, func() CanonicalTurnPageRepo)
	}{
		{name: "memory", repo: func(*testing.T) (Repo, func() CanonicalTurnPageRepo) {
			repo := NewMemoryRepo()
			return repo, func() CanonicalTurnPageRepo { return repo }
		}},
		{name: "file", repo: func(t *testing.T) (Repo, func() CanonicalTurnPageRepo) {
			root := filepath.Join(t.TempDir(), "threads")
			return NewFileRepo(root), func() CanonicalTurnPageRepo { return NewFileRepo(root) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, reopened := tc.repo(t)
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(ctx, Entry{
				ThreadID: "source", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnStarted,
				Metadata: map[string]string{"run_id": "run-original"},
			}, AppendOptions{ID: "source-started"}); err != nil {
				t.Fatal(err)
			}
			user, err := repo.Append(ctx, Entry{
				ThreadID: "source", TurnID: "turn-original", Type: EntryUserMessage,
				Message: session.Message{Role: session.User, Content: "original"},
			}, AppendOptions{ID: "source-user"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(ctx, Entry{
				ThreadID: "source", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnCompleted,
			}, AppendOptions{ID: "source-terminal"}); err != nil {
				t.Fatal(err)
			}
			if memory, ok := repo.(*MemoryRepo); ok {
				request := AdmitTurnRequest{
					ThreadID: "source", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-retry",
					RetrySourceTurnID: "turn-original", RetrySourceEntryID: user.ID,
				}
				request.RequestFingerprint, err = TurnAdmissionRequestFingerprint(request)
				if err != nil {
					t.Fatal(err)
				}
				admitted, err := memory.AdmitTurn(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := memory.FinishTurn(ctx, FinishTurnRequest{
					Lease: admitted.Lease, RunID: "run-retry", TerminalEntryID: "retry-terminal", Status: TurnCompleted,
					OutcomeFingerprint: "outcome-retry",
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := repo.Append(ctx, Entry{
					ThreadID: "source", TurnID: "turn-retry", Type: EntryTurnMarker, TurnStatus: TurnStarted,
					Metadata: map[string]string{
						"run_id": "run-retry", RetrySourceTurnIDMetadataKey: "turn-original", RetrySourceEntryIDMetadataKey: user.ID,
					},
				}, AppendOptions{ID: "retry-started"}); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.Append(ctx, Entry{
					ThreadID: "source", TurnID: "turn-retry", Type: EntryAssistantMessage,
					Message: session.Message{Role: session.Assistant, Content: "retried"},
				}, AppendOptions{ID: "retry-assistant"}); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.Append(ctx, Entry{
					ThreadID: "source", TurnID: "turn-retry", Type: EntryTurnMarker, TurnStatus: TurnCompleted,
				}, AppendOptions{ID: "retry-terminal"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := repo.Fork(ctx, ForkOptions{
				SourceThreadID: "source", NewThreadID: "fork",
				TurnIDMap: map[string]string{"turn-original": "fork-original", "turn-retry": "fork-retry"},
				RunIDMap:  map[string]string{"run-original": "fork-run-original", "run-retry": "fork-run-retry"},
			}); err != nil {
				t.Fatal(err)
			}
			page, err := reopened().ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 2})
			if err != nil || len(page.Turns) != 2 || page.Turns[0].TurnID != "fork-original" || page.Turns[1].TurnID != "fork-retry" ||
				page.Turns[1].RetrySource == nil || page.Turns[1].RetrySource.TurnID != "fork-original" {
				t.Fatalf("fork retry page=%#v err=%v", page, err)
			}
			forkUserEntryID := ""
			for _, item := range page.Turns[0].Entries {
				if item.Entry.Type == EntryUserMessage {
					forkUserEntryID = item.Entry.ID
				}
			}
			if forkUserEntryID == "" || page.Turns[1].RetrySource.EntryID != forkUserEntryID || page.Turns[1].RetrySource.EntryID == user.ID {
				t.Fatalf("fork retry source=%#v original_user=%q fork_user=%q", page.Turns[1].RetrySource, user.ID, forkUserEntryID)
			}
		})
	}
}

func TestMemoryForkRejectsRetryAdmissionRunDriftWithoutDestinationMutation(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"fork", "fork-with-initial-entry"} {
		t.Run(mode, func(t *testing.T) {
			repo := NewMemoryRepo()
			seedMemoryAdmittedRetryForkSource(t, ctx, repo)
			repo.mu.Lock()
			admission := repo.turnAdmissions[turnAdmissionKey("source", "turn-retry")]
			admission.RunID = "run-drift"
			repo.turnAdmissions[turnAdmissionKey("source", "turn-retry")] = admission
			seqBefore := repo.seq
			repo.mu.Unlock()

			var err error
			switch mode {
			case "fork":
				_, err = repo.Fork(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"})
			case "fork-with-initial-entry":
				_, _, err = repo.ForkWithInitialEntry(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"}, Entry{Type: EntryCustom})
			}
			if !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("%s error = %v, want ErrAuthorityCorrupt", mode, err)
			}
			repo.mu.Lock()
			_, threadExists := repo.threads["fork"]
			entryCount := len(repo.entries["fork"])
			_, admissionExists := repo.turnAdmissions[turnAdmissionKey("fork", "turn-retry")]
			_, finishExists := repo.turnFinishes[turnAdmissionKey("fork", "turn-retry")]
			seqAfter := repo.seq
			repo.mu.Unlock()
			if threadExists || entryCount != 0 || admissionExists || finishExists || seqAfter != seqBefore {
				t.Fatalf("rejected %s mutated destination: thread=%t entries=%d admission=%t finish=%t seq=%d want=%d",
					mode, threadExists, entryCount, admissionExists, finishExists, seqAfter, seqBefore)
			}
		})
	}
}

func TestMemoryForkWithInitialEntryRollbackClearsRetryAuthority(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	seedMemoryAdmittedRetryForkSource(t, ctx, repo)
	_, _, err := repo.ForkWithInitialEntry(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"}, Entry{
		Type: EntryCustom, ParentID: "missing",
	})
	if !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("ForkWithInitialEntry error = %v, want ErrInvalidParent", err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	_, threadExists := repo.threads["fork"]
	_, admissionExists := repo.turnAdmissions[turnAdmissionKey("fork", "turn-retry")]
	_, finishExists := repo.turnFinishes[turnAdmissionKey("fork", "turn-retry")]
	if threadExists || len(repo.entries["fork"]) != 0 || admissionExists || finishExists {
		t.Fatalf("failed publication retained fork authority: thread=%t entries=%d admission=%t finish=%t",
			threadExists, len(repo.entries["fork"]), admissionExists, finishExists)
	}
}

func TestMemoryForkRejectsRewriteThatChangesRetrySourceAuthority(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	seedMemoryAdmittedRetryForkSource(t, ctx, repo)
	_, err := repo.Fork(ctx, ForkOptions{
		SourceThreadID: "source", NewThreadID: "fork",
		RewriteEntry: func(entry Entry, _ ForkEntryIdentity) (Entry, error) {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				delete(entry.Metadata, RetrySourceTurnIDMetadataKey)
				delete(entry.Metadata, RetrySourceEntryIDMetadataKey)
			}
			return entry, nil
		},
	})
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if _, exists := repo.threads["fork"]; exists || len(repo.entries["fork"]) != 0 {
		t.Fatalf("rejected retry rewrite published destination: thread=%t entries=%d", exists, len(repo.entries["fork"]))
	}
}

func TestForkRejectsRetryAuthorityIdentityOverlaysAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, overlay := range []struct {
		name   string
		mutate func(Entry) Entry
	}{
		{name: "started-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.ID = "started-id-overlay"
			}
			return entry
		}},
		{name: "started-thread-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.ThreadID = "started-thread-overlay"
			}
			return entry
		}},
		{name: "started-parent-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.ParentID = "started-parent-overlay"
			}
			return entry
		}},
		{name: "started-turn-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.TurnID = "started-turn-overlay"
			}
			return entry
		}},
		{name: "started-type", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.Type = EntryCustom
			}
			return entry
		}},
		{name: "started-turn-status", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.TurnStatus = TurnCompleted
			}
			return entry
		}},
		{name: "started-run-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.Metadata["run_id"] = "run-overlay"
			}
			return entry
		}},
		{name: "started-retry-source-turn-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.Metadata[RetrySourceTurnIDMetadataKey] = "started-source-turn-overlay"
			}
			return entry
		}},
		{name: "started-retry-source-entry-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == "turn-retry" {
				entry.Metadata[RetrySourceEntryIDMetadataKey] = "started-source-entry-overlay"
			}
			return entry
		}},
		{name: "target-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.ID = "target-id-overlay"
			}
			return entry
		}},
		{name: "target-thread-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.ThreadID = "target-thread-overlay"
			}
			return entry
		}},
		{name: "target-parent-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.ParentID = "target-parent-overlay"
			}
			return entry
		}},
		{name: "target-turn-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.TurnID = "turn-overlay"
			}
			return entry
		}},
		{name: "target-type", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Type = EntryCustom
			}
			return entry
		}},
		{name: "target-turn-status", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.TurnStatus = TurnCompleted
			}
			return entry
		}},
		{name: "promote-custom-to-started", mutate: func(entry Entry) Entry {
			if entry.Type == EntryCustom && entry.Metadata["kind"] == "ordinary" {
				entry.Type = EntryTurnMarker
				entry.TurnStatus = TurnStarted
				entry.TurnID = "turn-injected"
				entry.Metadata["run_id"] = "run-injected"
			}
			return entry
		}},
		{name: "target-run-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Metadata["run_id"] = "target-run-overlay"
			}
			return entry
		}},
		{name: "target-retry-source-turn-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Metadata[RetrySourceTurnIDMetadataKey] = "turn-overlay"
			}
			return entry
		}},
		{name: "target-retry-source-entry-id", mutate: func(entry Entry) Entry {
			if entry.Type == EntryUserMessage && entry.TurnID == "turn-original" {
				entry.Metadata[RetrySourceEntryIDMetadataKey] = "entry-overlay"
			}
			return entry
		}},
	} {
		for _, backend := range []string{"memory", "file"} {
			t.Run(backend+"/"+overlay.name, func(t *testing.T) {
				var repo Repo
				var root string
				if backend == "memory" {
					memory := NewMemoryRepo()
					seedMemoryAdmittedRetryForkSource(t, ctx, memory)
					repo = memory
				} else {
					root = filepath.Join(t.TempDir(), "threads")
					fileRepo := NewFileRepo(root)
					seedFileRetryForkSource(t, ctx, fileRepo)
					repo = fileRepo
				}
				_, err := repo.Fork(ctx, ForkOptions{
					SourceThreadID: "source", NewThreadID: "fork",
					RunIDMap: map[string]string{"run-retry": "run-retry-mapped", "target-run": "target-run-mapped"},
					RewriteEntry: func(entry Entry, _ ForkEntryIdentity) (Entry, error) {
						return overlay.mutate(entry), nil
					},
				})
				if !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
				}
				if _, err := repo.Thread(ctx, "fork"); !errors.Is(err, ErrThreadNotFound) {
					t.Fatalf("rejected fork destination error = %v, want ErrThreadNotFound", err)
				}
				if backend == "file" {
					if _, err := os.Stat(filepath.Join(root, safePath("fork"))); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("rejected fork destination stat error = %v, want not exist", err)
					}
				} else {
					memory := repo.(*MemoryRepo)
					memory.mu.Lock()
					entryCount := len(memory.entries["fork"])
					_, admissionExists := memory.turnAdmissions[turnAdmissionKey("fork", "turn-retry")]
					memory.mu.Unlock()
					if entryCount != 0 || admissionExists {
						t.Fatalf("rejected fork retained entries=%d admission=%t", entryCount, admissionExists)
					}
				}
			})
		}
	}
}

func TestForkUsesImmutableIdentityMapSnapshotsAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, mutation := range []string{"callback-identity", "captured-maps"} {
		for _, backend := range []string{"memory", "file"} {
			t.Run(backend+"/"+mutation, func(t *testing.T) {
				var repo Repo
				if backend == "memory" {
					memory := NewMemoryRepo()
					seedMemoryAdmittedRetryForkSource(t, ctx, memory)
					repo = memory
				} else {
					fileRepo := NewFileRepo(filepath.Join(t.TempDir(), "threads"))
					seedFileRetryForkSource(t, ctx, fileRepo)
					repo = fileRepo
				}
				turnIDs := map[string]string{"turn-original": "turn-original-mapped", "turn-retry": "turn-retry-mapped"}
				runIDs := map[string]string{"run-original": "run-original-mapped", "run-retry": "run-retry-mapped", "target-run": "target-run-mapped"}
				callbackMapDrift := false
				capturedMutated := false
				_, err := repo.Fork(ctx, ForkOptions{
					SourceThreadID: "source", NewThreadID: "fork", TurnIDMap: turnIDs, RunIDMap: runIDs,
					RewriteEntry: func(entry Entry, identity ForkEntryIdentity) (Entry, error) {
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
				page, err := repo.(CanonicalTurnPageRepo).ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 2})
				if err != nil || len(page.Turns) != 2 || page.Turns[0].TurnID != "turn-original-mapped" || page.Turns[0].RunID != "run-original-mapped" ||
					page.Turns[1].TurnID != "turn-retry-mapped" || page.Turns[1].RunID != "run-retry-mapped" {
					t.Fatalf("fork page=%#v err=%v", page, err)
				}
			})
		}
	}
}

func TestSnapshotForkNodesSeparatesSharedIdentityMaps(t *testing.T) {
	turnIDs := map[string]string{"turn": "turn-mapped"}
	runIDs := map[string]string{"run": "run-mapped"}
	snapshot := snapshotForkNodes([]ForkOptions{
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

func TestMemoryCommitForkBatchRollbackClearsRetryAuthority(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	seedMemoryAdmittedRetryForkSource(t, ctx, repo)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source-b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{ThreadID: "source-b", Type: EntryCustom, Metadata: map[string]string{"kind": "source-b"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	closureFor := func(sourceThreadID, destinationThreadID string) artifact.Closure {
		t.Helper()
		path, err := repo.Path(ctx, sourceThreadID, "")
		if err != nil {
			t.Fatal(err)
		}
		entryIDs := make([]string, len(path))
		for index, entry := range path {
			entryIDs[index] = entry.ID
		}
		closure, err := repo.ArtifactClosure(ctx, ArtifactClosureRequest{
			SourceThreadID: sourceThreadID, DestinationThreadID: destinationThreadID, EntryIDs: entryIDs,
		})
		if err != nil {
			t.Fatal(err)
		}
		return closure
	}
	injected := errors.New("second fork failed")
	nodes := []ForkOptions{
		{OperationID: "operation", OperationNodeID: "root", SourceThreadID: "source", NewThreadID: "fork-a", ArtifactClosure: closureFor("source", "fork-a")},
		{OperationID: "operation", OperationNodeID: "second", SourceThreadID: "source-b", NewThreadID: "fork-b", ArtifactClosure: closureFor("source-b", "fork-b"),
			RewriteEntry: func(Entry, ForkEntryIdentity) (Entry, error) { return Entry{}, injected }},
	}
	repo.mu.Lock()
	for _, threadID := range []string{"source", "source-b", "fork-a", "fork-b"} {
		repo.authorityClaims[threadID] = "operation"
	}
	repo.mu.Unlock()
	if _, err := repo.CommitForkBatch(ctx, "operation", nodes, func() error { return nil }); !errors.Is(err, injected) {
		t.Fatalf("CommitForkBatch error = %v, want injected failure", err)
	}
	repo.mu.Lock()
	_, admissionExists := repo.turnAdmissions[turnAdmissionKey("fork-a", "turn-retry")]
	_, finishExists := repo.turnFinishes[turnAdmissionKey("fork-a", "turn-retry")]
	entryCount := len(repo.entries["fork-a"])
	_, threadExists := repo.threads["fork-a"]
	for threadID, owner := range repo.authorityClaims {
		if owner == "operation" {
			delete(repo.authorityClaims, threadID)
		}
	}
	repo.mu.Unlock()
	if threadExists || entryCount != 0 || admissionExists || finishExists {
		t.Fatalf("batch rollback retained thread=%t entries=%d admission=%t finish=%t", threadExists, entryCount, admissionExists, finishExists)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "fork-a"}); err != nil {
		t.Fatal(err)
	}
	request := AdmitTurnRequest{
		ThreadID: "fork-a", TurnID: "turn-retry", RunID: "fresh-run", OwnerID: "fresh-owner",
		RequestFingerprint: "fresh-request", Input: session.Message{Role: session.User, Content: "fresh"},
	}
	if _, err := repo.AdmitTurn(ctx, request); err != nil {
		t.Fatalf("reused destination was polluted: %v", err)
	}
}

func TestForkCopiesMappedRetryOfRetryChainAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			var repo Repo
			var read func() CanonicalTurnPageRepo
			if backend == "memory" {
				memory := buildMemorySettledRetrySourceChainFixture(t)
				repo = memory
				read = func() CanonicalTurnPageRepo { return memory }
			} else {
				source := buildMemorySettledRetrySourceChainFixture(t)
				entries, err := source.Entries(ctx, "thread")
				if err != nil {
					t.Fatal(err)
				}
				root := filepath.Join(t.TempDir(), "threads")
				file := NewFileRepo(root)
				if _, err := file.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if _, err := file.Append(ctx, entry, AppendOptions{ID: entry.ID, ParentID: entry.ParentID}); err != nil {
						t.Fatal(err)
					}
				}
				repo = file
				read = func() CanonicalTurnPageRepo { return NewFileRepo(root) }
			}
			_, err := repo.Fork(ctx, ForkOptions{
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
			page, err := read().ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 3})
			if err != nil {
				t.Fatal(err)
			}
			ids := assertMappedRetryOfRetryFork(t, page)
			if memory, ok := repo.(*MemoryRepo); ok {
				memory.mu.Lock()
				first := memory.turnAdmissions[turnAdmissionKey("fork", "retry-one-mapped")]
				second := memory.turnAdmissions[turnAdmissionKey("fork", "retry-two-mapped")]
				memory.mu.Unlock()
				if first.RunID != "retry-one-run-mapped" || first.TurnStartedID != ids.retryOneStarted || first.BaseLeafID != ids.sourceUser ||
					second.RunID != "retry-two-run-mapped" || second.TurnStartedID != ids.retryTwoStarted || second.BaseLeafID != ids.retryOneAssistant {
					t.Fatalf("mapped retry admissions first=%#v second=%#v ids=%#v", first, second, ids)
				}
			}
		})
	}
}

func TestForkRejectsRetryChainWithoutIntermediateTerminalAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			var repo Repo
			if backend == "memory" {
				repo = buildMemoryRetrySourceChainFixture(t, true)
			} else {
				source := buildMemoryRetrySourceChainFixture(t, true)
				entries, err := source.Entries(ctx, "thread")
				if err != nil {
					t.Fatal(err)
				}
				file := NewFileRepo(filepath.Join(t.TempDir(), "threads"))
				if _, err := file.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if _, err := file.Append(ctx, entry, AppendOptions{ID: entry.ID, ParentID: entry.ParentID}); err != nil {
						t.Fatal(err)
					}
				}
				repo = file
			}
			_, err := repo.Fork(ctx, ForkOptions{
				SourceThreadID: "thread", NewThreadID: "fork",
				TurnIDMap: map[string]string{"source-turn": "source-turn-mapped", "retry-one": "retry-one-mapped", "retry-two": "retry-two-mapped"},
				RunIDMap:  map[string]string{"source-run": "source-run-mapped", "retry-one-run": "retry-one-run-mapped", "retry-two-run": "retry-two-run-mapped"},
			})
			if !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("Fork error=%v, want ErrAuthorityCorrupt", err)
			}
			if _, err := repo.Thread(ctx, "fork"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("rejected fork destination error=%v, want ErrThreadNotFound", err)
			}
			if memory, ok := repo.(*MemoryRepo); ok {
				memory.mu.Lock()
				entryCount := len(memory.entries["fork"])
				admissionCount := 0
				for _, admission := range memory.turnAdmissions {
					if admission.ThreadID == "fork" {
						admissionCount++
					}
				}
				memory.mu.Unlock()
				if entryCount != 0 || admissionCount != 0 {
					t.Fatalf("rejected fork retained entries=%d admissions=%d", entryCount, admissionCount)
				}
			}
			if file, ok := repo.(*FileRepo); ok {
				if _, err := os.Stat(filepath.Join(file.root, safePath("fork"))); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("rejected fork destination stat error=%v, want not exist", err)
				}
			}
		})
	}
}

type mappedRetryForkIDs struct {
	sourceUser        string
	retryOneStarted   string
	retryOneAssistant string
	retryTwoStarted   string
}

func assertMappedRetryOfRetryFork(tb testing.TB, page CanonicalTurnsPage) mappedRetryForkIDs {
	tb.Helper()
	if len(page.Turns) != 3 || page.Turns[0].TurnID != "source-turn-mapped" || page.Turns[0].RunID != "source-run-mapped" ||
		page.Turns[1].TurnID != "retry-one-mapped" || page.Turns[1].RunID != "retry-one-run-mapped" ||
		page.Turns[2].TurnID != "retry-two-mapped" || page.Turns[2].RunID != "retry-two-run-mapped" {
		tb.Fatalf("mapped retry chain page=%#v", page)
	}
	findEntry := func(turn CanonicalTurn, entryType EntryType) string {
		for _, item := range turn.Entries {
			if item.Entry.Type == entryType {
				return item.Entry.ID
			}
		}
		return ""
	}
	ids := mappedRetryForkIDs{
		sourceUser:        findEntry(page.Turns[0], EntryUserMessage),
		retryOneStarted:   page.Turns[1].StartedEntryID,
		retryOneAssistant: findEntry(page.Turns[1], EntryAssistantMessage),
		retryTwoStarted:   page.Turns[2].StartedEntryID,
	}
	firstSource := page.Turns[1].RetrySource
	secondSource := page.Turns[2].RetrySource
	if ids.sourceUser == "" || ids.retryOneStarted == "" || ids.retryOneAssistant == "" || ids.retryTwoStarted == "" ||
		firstSource == nil || firstSource.TurnID != "source-turn-mapped" || firstSource.EntryID != ids.sourceUser ||
		secondSource == nil || secondSource.TurnID != "retry-one-mapped" || secondSource.EntryID != ids.retryOneAssistant {
		tb.Fatalf("mapped retry sources first=%#v second=%#v ids=%#v", firstSource, secondSource, ids)
	}
	return ids
}

func TestForkRejectsInvalidRetrySourceWithoutDestinationMutationAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, corruption := range []struct {
		name string
		seed func(testing.TB, context.Context, Repo)
	}{
		{name: "cross-branch", seed: seedCrossBranchRetrySource},
		{name: "abandoned-save-point", seed: seedRetrySourceWithAbandonedSavePoint},
	} {
		for _, backend := range []string{"memory", "file"} {
			t.Run(backend+"/"+corruption.name, func(t *testing.T) {
				var repo Repo
				var root string
				if backend == "memory" {
					repo = NewMemoryRepo()
				} else {
					root = filepath.Join(t.TempDir(), "threads")
					repo = NewFileRepo(root)
				}
				corruption.seed(t, ctx, repo)
				if memory, ok := repo.(*MemoryRepo); ok {
					memory.mu.Lock()
					// Corrupt fixtures bypass AdmitTurn by design; install the matching
					// stored fact so this test reaches structural source validation.
					if err := memory.rebuildRetryAdmissionFactsLocked("thread", memory.entries["thread"]); err != nil {
						memory.mu.Unlock()
						t.Fatal(err)
					}
					memory.mu.Unlock()
				}

				if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
				}
				if _, err := repo.Thread(ctx, "fork"); !errors.Is(err, ErrThreadNotFound) {
					t.Fatalf("rejected fork destination error = %v, want ErrThreadNotFound", err)
				}
				if backend == "file" {
					if _, err := os.Stat(filepath.Join(root, safePath("fork"))); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("rejected fork destination stat error = %v, want not exist", err)
					}
				}
			})
		}
	}
}

func seedMemoryAdmittedRetryForkSource(tb testing.TB, ctx context.Context, repo *MemoryRepo) Entry {
	tb.Helper()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
		tb.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "source", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": "run-original"},
	}, AppendOptions{ID: "source-started"}); err != nil {
		tb.Fatal(err)
	}
	user, err := repo.Append(ctx, Entry{
		ThreadID: "source", TurnID: "turn-original", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "original"}, Metadata: map[string]string{
			"run_id": "target-run", RetrySourceTurnIDMetadataKey: "turn-original", RetrySourceEntryIDMetadataKey: "source-started",
		},
	}, AppendOptions{ID: "source-user"})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "source", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnCompleted,
	}, AppendOptions{ID: "source-terminal"}); err != nil {
		tb.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ID: "source-custom", ThreadID: "source", Type: EntryCustom, Metadata: map[string]string{"kind": "ordinary"},
	}, AppendOptions{ID: "source-custom"}); err != nil {
		tb.Fatal(err)
	}
	request := AdmitTurnRequest{
		ThreadID: "source", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-retry",
		RetrySourceTurnID: "turn-original", RetrySourceEntryID: user.ID,
	}
	request.RequestFingerprint, err = TurnAdmissionRequestFingerprint(request)
	if err != nil {
		tb.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, request)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-retry", TerminalEntryID: "retry-terminal", Status: TurnCompleted,
		OutcomeFingerprint: "outcome-retry",
	}); err != nil {
		tb.Fatal(err)
	}
	return user
}

func seedFileRetryForkSource(tb testing.TB, ctx context.Context, repo *FileRepo) {
	tb.Helper()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
		tb.Fatal(err)
	}
	entries := []Entry{
		{ID: "source-started", ThreadID: "source", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "run-original"}},
		{ID: "source-user", ThreadID: "source", TurnID: "turn-original", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "original"}, Metadata: map[string]string{
			"run_id": "target-run", RetrySourceTurnIDMetadataKey: "turn-original", RetrySourceEntryIDMetadataKey: "source-started",
		}},
		{ID: "source-terminal", ThreadID: "source", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnCompleted},
		{ID: "source-custom", ThreadID: "source", Type: EntryCustom, Metadata: map[string]string{"kind": "ordinary"}},
		{ID: "retry-started", ThreadID: "source", TurnID: "turn-retry", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{
			"run_id": "run-retry", RetrySourceTurnIDMetadataKey: "turn-original", RetrySourceEntryIDMetadataKey: "source-user",
		}},
		{ID: "retry-assistant", ThreadID: "source", TurnID: "turn-retry", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "retried"}},
		{ID: "retry-terminal", ThreadID: "source", TurnID: "turn-retry", Type: EntryTurnMarker, TurnStatus: TurnCompleted},
	}
	for _, entry := range entries {
		if _, err := repo.Append(ctx, entry, AppendOptions{ID: entry.ID}); err != nil {
			tb.Fatal(err)
		}
	}
}

func TestListCanonicalTurnsPagesOnlyActivePath(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-1", "run-1", "one")
	secondLeaf := appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-2", "run-2", "two")
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-abandoned", "run-abandoned", "abandoned")
	if err := repo.MoveLeaf(ctx, "thread", secondLeaf); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-3", "run-3", "three")

	page, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 2 || page.Turns[0].TurnID != "turn-2" || page.Turns[1].TurnID != "turn-3" || !page.HasMore {
		t.Fatalf("tail page = %#v", page)
	}
	if page.LatestTurnID != "turn-3" || page.ThroughOrdinal != 6 || !page.HasRetryTarget {
		t.Fatalf("page authority = %#v", page)
	}
	if page.Turns[0].StartedOrdinal != 3 || page.Turns[1].StartedOrdinal != 5 {
		t.Fatalf("started ordinals = %#v", page.Turns)
	}
	if len(page.Turns[0].Entries) != 2 || page.Turns[0].Entries[1].Ordinal != 4 || page.Turns[0].Entries[1].Entry.Message.Content != "two" {
		t.Fatalf("turn entries = %#v", page.Turns[0].Entries)
	}

	if page.BeforeCursor == nil || page.BeforeCursor.EntryID != page.Turns[0].Entries[0].Entry.ID {
		t.Fatalf("tail before cursor = %#v", page.BeforeCursor)
	}
	if page.SinceCursor.EntryID != page.Turns[1].Entries[len(page.Turns[1].Entries)-1].Entry.ID {
		t.Fatalf("tail since cursor = %#v", page.SinceCursor)
	}

	before, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", BeforeCursor: page.BeforeCursor, Limit: 1})
	if err != nil || len(before.Turns) != 1 || before.Turns[0].TurnID != "turn-1" || before.HasMore {
		t.Fatalf("before page=%#v err=%v", before, err)
	}
	if before.ThroughOrdinal != 2 || before.BeforeCursor != nil {
		t.Fatalf("before page cursors=%#v", before)
	}

	thirdLeaf := page.SinceCursor
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-4", "run-4", "four")
	since, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &thirdLeaf, Limit: 1})
	if err != nil || len(since.Turns) != 1 || since.Turns[0].TurnID != "turn-4" || since.HasMore {
		t.Fatalf("since page=%#v err=%v", since, err)
	}
	if since.SinceCursor.EntryID != since.Turns[0].Entries[len(since.Turns[0].Entries)-1].Entry.ID {
		t.Fatalf("since cursor=%#v", since.SinceCursor)
	}
}

func TestListCanonicalTurnsBeforeCursorKeepsHistoricalBranch(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-1", "run-1", "one")
	secondLeaf := appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-2", "run-2", "two")
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-3", "run-3", "three")

	tail, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	if tail.BeforeCursor == nil {
		t.Fatal("tail page did not return a before cursor")
	}
	cursor := *tail.BeforeCursor
	if err := repo.MoveLeaf(ctx, "thread", secondLeaf); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-branch", "run-branch", "branch")

	page, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", BeforeCursor: &cursor, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 1 || page.Turns[0].TurnID != "turn-1" || page.ThroughOrdinal != 2 {
		t.Fatalf("historical page = %#v", page)
	}
}

func TestListCanonicalTurnsRejectsSinceCursorAfterBranch(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	firstLeaf := appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-1", "run-1", "one")
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-2", "run-2", "two")
	tail, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	cursor := tail.SinceCursor
	if err := repo.MoveLeaf(ctx, "thread", firstLeaf); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-branch", "run-branch", "branch")

	_, err = repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &cursor, Limit: 1})
	if !errors.Is(err, ErrStaleCanonicalTurnCursor) {
		t.Fatalf("since branch err=%v, want ErrStaleCanonicalTurnCursor", err)
	}
}

func TestListCanonicalTurnsRejectsEmptySinceCursor(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{
		ThreadID: "thread", SinceCursor: &CanonicalTurnSinceCursor{}, Limit: 1,
	}); err == nil || !strings.Contains(err.Error(), "since cursor requires entry identity") {
		t.Fatalf("empty since cursor err=%v", err)
	}
}

func TestMemoryListCanonicalTurnsSinceIncludesFactsAddedToAnchorTurn(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn", "run", "inspect")
	initial, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	cursor := initial.SinceCursor

	appended := []Entry{
		{ThreadID: "thread", TurnID: "turn", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "working"}},
		{ThreadID: "thread", TurnID: "turn", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "inspect"}},
		{ThreadID: "thread", TurnID: "turn", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "inspect", ToolResult: &session.ToolResultView{Status: "success"}}},
		{ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnCompleted},
	}
	for index, entry := range appended {
		stored, err := repo.Append(ctx, entry, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		page, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &cursor, Limit: 1})
		if err != nil || len(page.Turns) != 1 || page.Turns[0].TurnID != "turn" {
			t.Fatalf("increment %d page=%#v err=%v", index, page, err)
		}
		if got := page.SinceCursor.EntryID; got != stored.ID {
			t.Fatalf("increment %d since cursor=%q, want %q", index, got, stored.ID)
		}
		if got := len(page.Turns[0].Entries); got != 3+index {
			t.Fatalf("increment %d canonical entries=%d, want %d: %#v", index, got, 3+index, page.Turns[0].Entries)
		}
		cursor = page.SinceCursor
	}
	empty, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &cursor, Limit: 1})
	if err != nil || len(empty.Turns) != 0 || empty.SinceCursor != cursor {
		t.Fatalf("settled incremental page=%#v err=%v", empty, err)
	}
}

func TestFileRepoListCanonicalTurnsSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-1", "run-1", "one")
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn-2", "run-2", "two")
	page, err := NewFileRepo(root).ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil || len(page.Turns) != 1 || page.Turns[0].TurnID != "turn-2" || !page.HasMore || page.ThroughOrdinal != 4 {
		t.Fatalf("reopened page=%#v err=%v", page, err)
	}
	if page.Turns[0].Entries[0].Entry.PathDepth != 3 || page.Turns[0].Entries[1].Entry.PathDepth != 4 {
		t.Fatalf("reopened path depths=%#v", page.Turns[0].Entries)
	}
}

func TestFileRepoMigratesLegacyPathDepthsDurablyBeforePagePathAndFork(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	threadDir := filepath.Join(root, safePath("thread"))
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 3, 0, 0, 0, time.UTC)
	entries := []Entry{
		PrepareEntry(Entry{
			ID: "started", ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker,
			TurnStatus: TurnStarted, CreatedAt: now, Metadata: map[string]string{"run_id": "run"},
		}),
		PrepareEntry(Entry{
			ID: "user", ThreadID: "thread", ParentID: "started", TurnID: "turn", Type: EntryUserMessage,
			CreatedAt: now.Add(time.Second), Message: session.Message{Role: session.User, Content: "legacy input"},
		}),
		PrepareEntry(Entry{
			ID: "terminal", ThreadID: "thread", ParentID: "user", TurnID: "turn", Type: EntryTurnMarker,
			TurnStatus: TurnCompleted, CreatedAt: now.Add(2 * time.Second),
		}),
	}
	meta := ThreadMeta{ID: "thread", LeafID: "terminal", CreatedAt: now, UpdatedAt: now.Add(2 * time.Second)}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "thread.json"), metaJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(threadDir, "entries.jsonl")
	if err := writeLegacyEntriesWithoutPathDepthForTest(journalPath, entries); err != nil {
		t.Fatal(err)
	}
	legacyJournal, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(legacyJournal), "path_depth") {
		t.Fatalf("legacy journal unexpectedly contains path_depth:\n%s", legacyJournal)
	}

	repo := NewFileRepo(root)
	page, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil || len(page.Turns) != 1 || len(page.Turns[0].Entries) != 3 {
		t.Fatalf("migrated page=%#v err=%v", page, err)
	}
	path, err := repo.Path(ctx, "thread", "")
	if err != nil || len(path) != 3 {
		t.Fatalf("migrated path=%#v err=%v", path, err)
	}
	for index, entry := range path {
		if entry.PathDepth != int64(index+1) {
			t.Fatalf("migrated path[%d].PathDepth=%d, want %d", index, entry.PathDepth, index+1)
		}
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); err != nil {
		t.Fatalf("fork migrated journal: %v", err)
	}
	migratedJournal, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(migratedJournal), "\"path_depth\"") != len(entries) {
		t.Fatalf("migrated journal does not persist every path depth:\n%s", migratedJournal)
	}

	reopened := NewFileRepo(root)
	reopenedPage, err := reopened.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil || len(reopenedPage.Turns) != 1 {
		t.Fatalf("second reopen page=%#v err=%v", reopenedPage, err)
	}
	forkPath, err := reopened.Path(ctx, "fork", "")
	if err != nil || len(forkPath) != 3 {
		t.Fatalf("second reopen fork path=%#v err=%v", forkPath, err)
	}
	afterSecondReopen, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(afterSecondReopen) != string(migratedJournal) {
		t.Fatal("second reopen rewrote the already migrated journal")
	}
}

func TestFileRepoDoesNotRewriteBrokenAllZeroLegacyPathDepthJournal(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	threadDir := filepath.Join(root, safePath("thread"))
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 4, 0, 0, 0, time.UTC)
	meta := ThreadMeta{ID: "thread", LeafID: "orphan", CreatedAt: now, UpdatedAt: now}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "thread.json"), metaJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(threadDir, "entries.jsonl")
	orphan := PrepareEntry(Entry{
		ID: "orphan", ThreadID: "thread", ParentID: "missing-parent", TurnID: "turn", Type: EntryTurnMarker,
		TurnStatus: TurnStarted, CreatedAt: now, Metadata: map[string]string{"run_id": "run"},
	})
	if err := writeLegacyEntriesWithoutPathDepthForTest(journalPath, []Entry{orphan}); err != nil {
		t.Fatal(err)
	}
	assertPathUnchanged := preservePathForTest(t, journalPath)

	repo := NewFileRepo(root)
	if _, err := repo.Thread(ctx, "thread"); err != nil {
		t.Fatalf("broken journal must not hide thread metadata: %v", err)
	}
	if _, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("broken legacy page error=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := repo.Path(ctx, "thread", ""); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("broken legacy path error=%v, want ErrInvalidParent", err)
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("broken legacy fork error=%v, want ErrInvalidParent", err)
	}
	if _, err := os.Stat(filepath.Join(root, safePath("fork"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("broken legacy fork destination stat error=%v, want not exist", err)
	}
	assertPathUnchanged()
}

func TestFileRepoDoesNotRewriteMixedPathDepthJournal(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	threadDir := filepath.Join(root, safePath("thread"))
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 4, 30, 0, 0, time.UTC)
	meta := ThreadMeta{ID: "thread", LeafID: "user", CreatedAt: now, UpdatedAt: now.Add(time.Second)}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "thread.json"), metaJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		PrepareEntry(Entry{
			ID: "started", ThreadID: "thread", PathDepth: 1, TurnID: "turn", Type: EntryTurnMarker,
			TurnStatus: TurnStarted, CreatedAt: now, Metadata: map[string]string{"run_id": "run"},
		}),
		PrepareEntry(Entry{
			ID: "user", ThreadID: "thread", ParentID: "started", TurnID: "turn", Type: EntryUserMessage,
			CreatedAt: now.Add(time.Second), Message: session.Message{Role: session.User, Content: "mixed"},
		}),
	}
	journalPath := filepath.Join(threadDir, "entries.jsonl")
	if err := rewriteEntriesForTest(journalPath, entries); err != nil {
		t.Fatal(err)
	}
	assertPathUnchanged := preservePathForTest(t, journalPath)

	repo := NewFileRepo(root)
	if _, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("mixed page error=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := repo.Path(ctx, "thread", ""); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("mixed path error=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("mixed fork error=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := os.Stat(filepath.Join(root, safePath("fork"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mixed fork destination stat error=%v, want not exist", err)
	}
	assertPathUnchanged()
}

func TestFileRepoRejectsMixedPathDepthWithMissingParentWithoutRewrite(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	threadDir := filepath.Join(root, safePath("thread"))
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 4, 45, 0, 0, time.UTC)
	meta := ThreadMeta{ID: "thread", LeafID: "orphan", CreatedAt: now, UpdatedAt: now}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "thread.json"), metaJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(threadDir, "entries.jsonl")
	entries := []Entry{
		PrepareEntry(Entry{ID: "root", ThreadID: "thread", PathDepth: 1, Type: EntryCustom, CreatedAt: now}),
		PrepareEntry(Entry{ID: "orphan", ThreadID: "thread", ParentID: "missing", Type: EntryCustom, CreatedAt: now.Add(time.Second)}),
	}
	if err := rewriteEntriesForTest(journalPath, entries); err != nil {
		t.Fatal(err)
	}
	assertPathUnchanged := preservePathForTest(t, journalPath)

	if _, err := NewFileRepo(root).Thread(ctx, "thread"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("mixed broken journal err=%v, want ErrAuthorityCorrupt", err)
	}
	assertPathUnchanged()
}

func TestFileRepoDefersLegacyPathDepthRewriteUntilAllAuthorityLoads(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	threadDir := filepath.Join(root, safePath("thread"))
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 5, 0, 0, 0, time.UTC)
	meta := ThreadMeta{ID: "thread", LeafID: "user", CreatedAt: now, UpdatedAt: now.Add(time.Second)}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "thread.json"), metaJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		PrepareEntry(Entry{ID: "started", ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "run"}, CreatedAt: now}),
		PrepareEntry(Entry{ID: "user", ThreadID: "thread", ParentID: "started", TurnID: "turn", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect"}, CreatedAt: now.Add(time.Second)}),
	}
	journalPath := filepath.Join(threadDir, "entries.jsonl")
	if err := writeLegacyEntriesWithoutPathDepthForTest(journalPath, entries); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "agent_todos.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertPathUnchanged := preservePathForTest(t, journalPath)

	if _, err := NewFileRepo(root).Thread(ctx, "thread"); err == nil || !strings.Contains(err.Error(), "decode agent todo state") {
		t.Fatalf("invalid staged authority err=%v", err)
	}
	assertPathUnchanged()
}

func TestFileRepoListCanonicalTurnsRejectsReferenceRawDrift(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendCanonicalTurnFixture(t, ctx, repo, "thread", "turn", "run", "inspect")
	journalPath := filepath.Join(root, safePath("thread"), "entries.jsonl")
	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for index, line := range lines {
		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Type != EntryUserMessage {
			continue
		}
		entry.Message.References = []session.MessageReference{{
			ReferenceID: "context:0", Kind: session.MessageReferenceText, Label: "quote", Text: "changed without raw update",
		}}
		encoded, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		lines[index] = string(encoded)
	}
	if err := os.WriteFile(journalPath, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileRepo(root).ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("reference raw drift page err=%v", err)
	}
}

func TestFileRepoRetrySourceSurvivesReopenWithoutDuplicatingUser(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "threads")
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": "run-original"},
	}, AppendOptions{ID: "original-started"}); err != nil {
		t.Fatal(err)
	}
	user, err := repo.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn-original", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "original question"},
	}, AppendOptions{ID: "original-user"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn-original", Type: EntryTurnMarker, TurnStatus: TurnFailed,
	}, AppendOptions{ID: "original-terminal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn-retry", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{
			"run_id": "run-retry", RetrySourceTurnIDMetadataKey: "turn-original", RetrySourceEntryIDMetadataKey: user.ID,
		},
	}, AppendOptions{ID: "retry-started"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn-retry", Type: EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "retry answer"},
	}, AppendOptions{ID: "retry-assistant"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn-retry", Type: EntryTurnMarker, TurnStatus: TurnCompleted,
	}, AppendOptions{ID: "retry-terminal"}); err != nil {
		t.Fatal(err)
	}

	page, err := NewFileRepo(root).ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 2})
	if err != nil || len(page.Turns) != 2 || page.Turns[1].RetrySource == nil ||
		page.Turns[1].RetrySource.TurnID != "turn-original" || page.Turns[1].RetrySource.EntryID != user.ID {
		t.Fatalf("reopened retry page=%#v err=%v", page, err)
	}
	for _, item := range page.Turns[1].Entries {
		if item.Entry.Type == EntryUserMessage {
			t.Fatalf("reopened retry duplicated user entry: %#v", page.Turns[1].Entries)
		}
	}
}

func TestCanonicalTurnPagesRejectReferenceRawDriftAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, repoKind := range []string{"memory", "file"} {
		for _, mode := range []string{"tail", "before", "since"} {
			t.Run(repoKind+"/"+mode, func(t *testing.T) {
				var repo Repo
				var root string
				if repoKind == "memory" {
					repo = NewMemoryRepo()
				} else {
					root = filepath.Join(t.TempDir(), "threads")
					repo = NewFileRepo(root)
				}
				if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
					t.Fatal(err)
				}
				firstUserID := appendCanonicalReferenceTurnFixture(t, ctx, repo, "thread", "turn-1", "run-1", "first")
				firstPage, err := repo.(CanonicalTurnPageRepo).ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
				if err != nil {
					t.Fatal(err)
				}
				secondUserID := appendCanonicalReferenceTurnFixture(t, ctx, repo, "thread", "turn-2", "run-2", "second")

				var opts ListCanonicalTurnsOptions
				corruptEntryID := secondUserID
				switch mode {
				case "tail":
					opts = ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}
				case "before":
					tail, err := repo.(CanonicalTurnPageRepo).ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
					if err != nil || tail.BeforeCursor == nil {
						t.Fatalf("tail page=%#v err=%v", tail, err)
					}
					opts = ListCanonicalTurnsOptions{ThreadID: "thread", BeforeCursor: tail.BeforeCursor, Limit: 1}
					corruptEntryID = firstUserID
				case "since":
					cursor := firstPage.SinceCursor
					opts = ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &cursor, Limit: 1}
				}

				if memory, ok := repo.(*MemoryRepo); ok {
					corruptMemoryReferenceRawDrift(t, memory, "thread", corruptEntryID)
				} else {
					corruptFileReferenceRawDrift(t, root, "thread", corruptEntryID)
					repo = NewFileRepo(root)
				}
				if _, err := repo.(CanonicalTurnPageRepo).ListCanonicalTurns(ctx, opts); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
				}
			})
		}
	}
}

func TestForkRejectsReferenceRawDriftWithoutDestinationMutationAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, repoKind := range []string{"memory", "file"} {
		t.Run(repoKind, func(t *testing.T) {
			var repo Repo
			var root string
			if repoKind == "memory" {
				repo = NewMemoryRepo()
			} else {
				root = filepath.Join(t.TempDir(), "threads")
				repo = NewFileRepo(root)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
				t.Fatal(err)
			}
			userID := appendCanonicalReferenceTurnFixture(t, ctx, repo, "source", "turn", "run", "source")
			if memory, ok := repo.(*MemoryRepo); ok {
				corruptMemoryReferenceRawDrift(t, memory, "source", userID)
			} else {
				corruptFileReferenceRawDrift(t, root, "source", userID)
				repo = NewFileRepo(root)
			}

			if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
			}
			if repoKind == "memory" {
				memory := repo.(*MemoryRepo)
				memory.mu.Lock()
				_, threadExists := memory.threads["fork"]
				entries := len(memory.entries["fork"])
				memory.mu.Unlock()
				if threadExists || entries != 0 {
					t.Fatalf("corrupt fork mutated destination: thread_exists=%v entries=%d", threadExists, entries)
				}
			} else if _, err := os.Stat(filepath.Join(root, safePath("fork"))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("corrupt fork destination stat error = %v, want not exist", err)
			}
		})
	}
}

func TestMemoryCanonicalTurnPageAllocationsDoNotScaleWithThreadLength(t *testing.T) {
	shortRepo, shortSince := buildMemoryCanonicalReferenceFixture(t, 1_000)
	longRepo, longSince := buildMemoryCanonicalReferenceFixture(t, 100_000)
	for _, mode := range []string{"tail", "before", "since"} {
		shortAllocs := memoryCanonicalPageAllocs(t, shortRepo, shortSince, mode)
		longAllocs := memoryCanonicalPageAllocs(t, longRepo, longSince, mode)
		if longAllocs > shortAllocs*1.10 {
			t.Fatalf("%s allocations scaled with thread length: 1k=%f 100k=%f", mode, shortAllocs, longAllocs)
		}
	}
}

func TestMemoryRetryEligibilityAllocationsDoNotScaleWithSourceDepth(t *testing.T) {
	shortRepo := buildMemoryDeepRetryFixture(t, 1_000, true)
	longRepo := buildMemoryDeepRetryFixture(t, 100_000, true)
	shortAllocs := memoryDeepRetryPageAllocs(t, shortRepo)
	longAllocs := memoryDeepRetryPageAllocs(t, longRepo)
	if longAllocs > shortAllocs*1.10 {
		t.Fatalf("retry eligibility allocations scaled with source depth: 1k=%f 100k=%f", shortAllocs, longAllocs)
	}
}

func TestMemoryRetryEligibilityAllocationsScaleLinearlyWithSourceChain(t *testing.T) {
	allocations := make(map[int]float64)
	for _, chainLength := range []int{1, 8, 32} {
		allocations[chainLength] = memoryDeepRetryPageAllocs(t, buildMemoryDeepRetryChainFixture(t, 1_000, chainLength, true))
	}
	assertRetryChainAllocationsLinear(t, allocations)
}

func assertRetryChainAllocationsLinear(t *testing.T, allocations map[int]float64) {
	t.Helper()
	shortPerHop := (allocations[8] - allocations[1]) / 7
	longPerHop := (allocations[32] - allocations[1]) / 31
	if shortPerHop <= 0 || longPerHop < shortPerHop*0.75 || longPerHop > shortPerHop*1.25 {
		t.Fatalf("retry chain allocations are not linear: one=%f eight=%f thirty-two=%f", allocations[1], allocations[8], allocations[32])
	}
}

func TestMemoryRetryEligibilityFollowsExactRetrySourceChain(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable-%t", durable), func(t *testing.T) {
			repo := buildMemoryRetrySourceChainFixture(t, durable)
			page, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
			if err != nil || page.HasRetryTarget != durable {
				t.Fatalf("retry chain page=%#v err=%v, want eligible=%t", page, err, durable)
			}
		})
	}
}

func TestRetryEligibilityRejectsCrossBranchSourceAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) (Repo, func() CanonicalTurnPageRepo)
	}{
		{name: "memory", repo: func(*testing.T) (Repo, func() CanonicalTurnPageRepo) {
			repo := NewMemoryRepo()
			return repo, func() CanonicalTurnPageRepo { return repo }
		}},
		{name: "file", repo: func(t *testing.T) (Repo, func() CanonicalTurnPageRepo) {
			root := filepath.Join(t.TempDir(), "threads")
			return NewFileRepo(root), func() CanonicalTurnPageRepo { return NewFileRepo(root) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, reopened := tc.repo(t)
			seedCrossBranchRetrySource(t, ctx, repo)
			if memory, ok := repo.(*MemoryRepo); ok {
				memory.mu.Lock()
				if err := memory.rebuildRetryAdmissionFactsLocked("thread", memory.entries["thread"]); err != nil {
					memory.mu.Unlock()
					t.Fatal(err)
				}
				memory.mu.Unlock()
			}
			assertRetrySourceForkCorruptionRejected(t, ctx, reopened())
		})
	}
}

func TestMemoryRetryEligibilityRejectsAdmissionBaseLeafDrift(t *testing.T) {
	repo := buildMemoryRetrySourceChainFixture(t, true)
	repo.mu.Lock()
	admission := repo.turnAdmissions[turnAdmissionKey("thread", "retry-two")]
	admission.BaseLeafID = "source-user"
	repo.turnAdmissions[turnAdmissionKey("thread", "retry-two")] = admission
	repo.mu.Unlock()

	if _, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
	}
}

func TestRetryEligibilityRejectsAbandonedSavePointSiblingAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) (Repo, func() CanonicalTurnPageRepo)
	}{
		{name: "memory", repo: func(*testing.T) (Repo, func() CanonicalTurnPageRepo) {
			repo := NewMemoryRepo()
			return repo, func() CanonicalTurnPageRepo { return repo }
		}},
		{name: "file", repo: func(t *testing.T) (Repo, func() CanonicalTurnPageRepo) {
			root := filepath.Join(t.TempDir(), "threads")
			return NewFileRepo(root), func() CanonicalTurnPageRepo { return NewFileRepo(root) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, reopened := tc.repo(t)
			seedRetrySourceWithAbandonedSavePoint(t, ctx, repo)
			if memory, ok := repo.(*MemoryRepo); ok {
				memory.mu.Lock()
				if err := memory.rebuildRetryAdmissionFactsLocked("thread", memory.entries["thread"]); err != nil {
					memory.mu.Unlock()
					t.Fatal(err)
				}
				memory.mu.Unlock()
			}
			if _, err := reopened().ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

func TestRetryEligibilityRejectsExplicitSourceCycleAcrossMemoryAndFile(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo func(*testing.T) (Repo, func() CanonicalTurnPageRepo)
	}{
		{name: "memory", repo: func(*testing.T) (Repo, func() CanonicalTurnPageRepo) {
			repo := NewMemoryRepo()
			return repo, func() CanonicalTurnPageRepo { return repo }
		}},
		{name: "file", repo: func(t *testing.T) (Repo, func() CanonicalTurnPageRepo) {
			root := filepath.Join(t.TempDir(), "threads")
			return NewFileRepo(root), func() CanonicalTurnPageRepo { return NewFileRepo(root) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, reopened := tc.repo(t)
			seedRetrySourceCycle(t, ctx, repo)
			if memory, ok := repo.(*MemoryRepo); ok {
				memory.mu.Lock()
				if err := memory.rebuildRetryAdmissionFactsLocked("thread", memory.entries["thread"]); err != nil {
					memory.mu.Unlock()
					t.Fatal(err)
				}
				memory.mu.Unlock()
			}
			assertRetrySourceForkCorruptionRejected(t, ctx, reopened())
		})
	}
}

func assertRetrySourceForkCorruptionRejected(tb testing.TB, ctx context.Context, authority CanonicalTurnPageRepo) {
	tb.Helper()
	if _, err := authority.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, ErrAuthorityCorrupt) {
		tb.Fatalf("ListCanonicalTurns error = %v, want ErrAuthorityCorrupt", err)
	}
	forker := authority.(Repo)
	if _, err := forker.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); !errors.Is(err, ErrAuthorityCorrupt) {
		tb.Fatalf("Fork error = %v, want ErrAuthorityCorrupt", err)
	}
	if _, err := forker.Thread(ctx, "fork"); !errors.Is(err, ErrThreadNotFound) {
		tb.Fatalf("rejected fork destination error = %v, want ErrThreadNotFound", err)
	}
	if memory, ok := forker.(*MemoryRepo); ok {
		memory.mu.Lock()
		entryCount := len(memory.entries["fork"])
		admissionCount := 0
		for _, admission := range memory.turnAdmissions {
			if admission.ThreadID == "fork" {
				admissionCount++
			}
		}
		memory.mu.Unlock()
		if entryCount != 0 || admissionCount != 0 {
			tb.Fatalf("rejected fork retained entries=%d admissions=%d", entryCount, admissionCount)
		}
	}
	if file, ok := forker.(*FileRepo); ok {
		if _, err := os.Stat(filepath.Join(file.root, safePath("fork"))); !errors.Is(err, os.ErrNotExist) {
			tb.Fatalf("rejected fork destination stat error = %v, want not exist", err)
		}
	}
}

func seedRetrySourceCycle(tb testing.TB, ctx context.Context, repo Repo) {
	tb.Helper()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	appendEntry := func(entry Entry, parentID string) {
		tb.Helper()
		if _, err := repo.Append(ctx, entry, AppendOptions{ID: entry.ID, ParentID: parentID}); err != nil {
			tb.Fatal(err)
		}
	}
	appendEntry(Entry{ID: "root", ThreadID: "thread", Type: EntryCustom}, "")
	appendEntry(Entry{ID: "retry-a-started", ThreadID: "thread", TurnID: "retry-a", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "retry-a-run", RetrySourceTurnIDMetadataKey: "retry-b", RetrySourceEntryIDMetadataKey: "retry-b-assistant"}}, "root")
	appendEntry(Entry{ID: "retry-a-assistant", ThreadID: "thread", TurnID: "retry-a", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "a"}}, "retry-a-started")
	appendEntry(Entry{ID: "retry-a-save-point", ThreadID: "thread", TurnID: "retry-a", Type: EntryTurnMarker, TurnStatus: TurnSavePoint}, "retry-a-assistant")
	appendEntry(Entry{ID: "retry-b-started", ThreadID: "thread", TurnID: "retry-b", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "retry-b-run", RetrySourceTurnIDMetadataKey: "retry-a", RetrySourceEntryIDMetadataKey: "retry-a-assistant"}}, "retry-a-save-point")
	appendEntry(Entry{ID: "retry-b-assistant", ThreadID: "thread", TurnID: "retry-b", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "b"}}, "retry-b-started")
	appendEntry(Entry{ID: "retry-b-save-point", ThreadID: "thread", TurnID: "retry-b", Type: EntryTurnMarker, TurnStatus: TurnSavePoint}, "retry-b-assistant")
}

func seedRetrySourceWithAbandonedSavePoint(tb testing.TB, ctx context.Context, repo Repo) {
	tb.Helper()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	appendEntry := func(entry Entry, parentID string) {
		tb.Helper()
		if _, err := repo.Append(ctx, entry, AppendOptions{ID: entry.ID, ParentID: parentID}); err != nil {
			tb.Fatal(err)
		}
	}
	appendEntry(Entry{ID: "source-started", ThreadID: "thread", TurnID: "source-turn", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "source-run"}}, "")
	appendEntry(Entry{ID: "source-user", ThreadID: "thread", TurnID: "source-turn", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect"}}, "source-started")
	appendEntry(Entry{ID: "source-assistant", ThreadID: "thread", TurnID: "source-turn", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "partial"}}, "source-user")
	appendEntry(Entry{ID: "abandoned-save-point", ThreadID: "thread", TurnID: "source-turn", Type: EntryTurnMarker, TurnStatus: TurnSavePoint}, "source-assistant")
	appendEntry(Entry{ID: "active-save-point", ThreadID: "thread", TurnID: "source-turn", Type: EntryTurnMarker, TurnStatus: TurnSavePoint}, "source-assistant")
	appendEntry(Entry{
		ID: "retry-started", ThreadID: "thread", TurnID: "retry-turn", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": "retry-run", RetrySourceTurnIDMetadataKey: "source-turn", RetrySourceEntryIDMetadataKey: "source-assistant"},
	}, "active-save-point")
	appendEntry(Entry{ID: "retry-assistant", ThreadID: "thread", TurnID: "retry-turn", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "retried"}}, "retry-started")
	appendEntry(Entry{ID: "retry-terminal", ThreadID: "thread", TurnID: "retry-turn", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, "retry-assistant")
}

func seedCrossBranchRetrySource(tb testing.TB, ctx context.Context, repo Repo) {
	tb.Helper()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	appendEntry := func(entry Entry, parentID string) {
		tb.Helper()
		if _, err := repo.Append(ctx, entry, AppendOptions{ID: entry.ID, ParentID: parentID}); err != nil {
			tb.Fatal(err)
		}
	}
	appendEntry(Entry{ID: "root", ThreadID: "thread", Type: EntryCustom}, "")
	appendEntry(Entry{ID: "off-started", ThreadID: "thread", TurnID: "off-turn", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "off-run"}}, "root")
	appendEntry(Entry{ID: "off-user", ThreadID: "thread", TurnID: "off-turn", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "off-branch input"}}, "off-started")
	appendEntry(Entry{ID: "off-terminal", ThreadID: "thread", TurnID: "off-turn", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, "off-user")
	appendEntry(Entry{ID: "active-started", ThreadID: "thread", TurnID: "active-turn", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "active-run"}}, "root")
	appendEntry(Entry{ID: "active-user", ThreadID: "thread", TurnID: "active-turn", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "active input"}}, "active-started")
	appendEntry(Entry{ID: "active-terminal", ThreadID: "thread", TurnID: "active-turn", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, "active-user")
	appendEntry(Entry{
		ID: "retry-started", ThreadID: "thread", TurnID: "retry-turn", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": "retry-run", RetrySourceTurnIDMetadataKey: "off-turn", RetrySourceEntryIDMetadataKey: "off-user"},
	}, "active-terminal")
	appendEntry(Entry{ID: "retry-assistant", ThreadID: "thread", TurnID: "retry-turn", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "retried"}}, "retry-started")
	appendEntry(Entry{ID: "retry-terminal", ThreadID: "thread", TurnID: "retry-turn", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, "retry-assistant")
}

func BenchmarkMemoryRetryEligibilityBySourceDepth(b *testing.B) {
	for _, depth := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("depth-%d", depth), func(b *testing.B) {
			repo := buildMemoryDeepRetryFixture(b, depth, true)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				page, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
				if err != nil || !page.HasRetryTarget {
					b.Fatalf("retry page=%#v err=%v", page, err)
				}
			}
		})
	}
}

func memoryDeepRetryPageAllocs(t *testing.T, repo *MemoryRepo) float64 {
	t.Helper()
	return testing.AllocsPerRun(10, func() {
		page, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
		if err != nil || !page.HasRetryTarget {
			panic(fmt.Sprintf("retry page=%#v err=%v", page, err))
		}
	})
}

func buildMemoryDeepRetryFixture(tb testing.TB, depth int, durable bool) *MemoryRepo {
	return buildMemoryDeepRetryChainFixture(tb, depth, 8, durable)
}

func buildMemoryDeepRetryChainFixture(tb testing.TB, depth, chainLength int, durable bool) *MemoryRepo {
	tb.Helper()
	if chainLength <= 0 {
		tb.Fatal("retry chain length must be positive")
	}
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(context.Background(), ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	now := time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)
	parentID := ""
	for index := 0; index < depth; index++ {
		entry := PrepareEntry(Entry{
			ID: fmt.Sprintf("prefix-%06d", index), ThreadID: "thread", ParentID: parentID, Type: EntryCustom, CreatedAt: now,
		})
		repo.appendIndexedEntriesLocked("thread", entry)
		parentID = entry.ID
	}
	content := ""
	if durable {
		content = "durable input"
	}
	sourceStarted := PrepareEntry(Entry{
		ID: "source-started", ThreadID: "thread", ParentID: parentID, TurnID: "source-turn", Type: EntryTurnMarker,
		TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "source-run"}, CreatedAt: now,
	})
	sourceUser := PrepareEntry(Entry{
		ID: "source-user", ThreadID: "thread", ParentID: sourceStarted.ID, TurnID: "source-turn", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: content, References: []session.MessageReference{{ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "quote", Text: "selection"}}}, CreatedAt: now,
	})
	sourceTerminal := PrepareEntry(Entry{
		ID: "source-terminal", ThreadID: "thread", ParentID: sourceUser.ID, TurnID: "source-turn", Type: EntryTurnMarker,
		TurnStatus: TurnCompleted, CreatedAt: now,
	})
	repo.appendIndexedEntriesLocked("thread", sourceStarted, sourceUser, sourceTerminal)
	parentID = sourceTerminal.ID
	sourceTurnID := "source-turn"
	sourceEntryID := sourceUser.ID
	for index := 0; index < chainLength; index++ {
		turnID := fmt.Sprintf("retry-%03d", index)
		started := PrepareEntry(Entry{
			ID: turnID + "-started", ThreadID: "thread", ParentID: parentID, TurnID: turnID, Type: EntryTurnMarker,
			TurnStatus: TurnStarted, Metadata: map[string]string{
				"run_id": turnID + "-run", RetrySourceTurnIDMetadataKey: sourceTurnID, RetrySourceEntryIDMetadataKey: sourceEntryID,
			}, CreatedAt: now,
		})
		assistant := PrepareEntry(Entry{
			ID: turnID + "-assistant", ThreadID: "thread", ParentID: started.ID, TurnID: turnID, Type: EntryAssistantMessage,
			Message: session.Message{Role: session.Assistant, Content: "retried"}, CreatedAt: now,
		})
		status := TurnSavePoint
		terminalID := turnID + "-save-point"
		if index == chainLength-1 {
			status = TurnCompleted
			terminalID = turnID + "-terminal"
		}
		terminal := PrepareEntry(Entry{
			ID: terminalID, ThreadID: "thread", ParentID: assistant.ID, TurnID: turnID, Type: EntryTurnMarker,
			TurnStatus: status, CreatedAt: now,
		})
		repo.appendIndexedEntriesLocked("thread", started, assistant, terminal)
		parentID = terminal.ID
		sourceTurnID = turnID
		sourceEntryID = assistant.ID
	}
	if err := repo.rebuildRetryAdmissionFactsLocked("thread", repo.entries["thread"]); err != nil {
		tb.Fatal(err)
	}
	meta := repo.threads["thread"]
	meta.LeafID = parentID
	repo.threads["thread"] = meta
	return repo
}

func buildMemorySettledRetrySourceChainFixture(tb testing.TB) *MemoryRepo {
	tb.Helper()
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	admit := func(req AdmitTurnRequest) AdmitTurnResult {
		tb.Helper()
		fingerprint, err := TurnAdmissionRequestFingerprint(req)
		if err != nil {
			tb.Fatal(err)
		}
		req.RequestFingerprint = fingerprint
		result, err := repo.AdmitTurn(ctx, req)
		if err != nil {
			tb.Fatal(err)
		}
		return result
	}
	original := admit(AdmitTurnRequest{
		ThreadID: "thread", TurnID: "source-turn", RunID: "source-run", OwnerID: "source-owner",
		Input: session.Message{Role: session.User, Content: "durable input", References: []session.MessageReference{{
			ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "quote", Text: "selection",
		}}},
	})
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: original.Lease, RunID: "source-run", TerminalEntryID: "source-terminal", Status: TurnCompleted,
		OutcomeFingerprint: "source-outcome",
	}); err != nil {
		tb.Fatal(err)
	}
	first := admit(AdmitTurnRequest{
		ThreadID: "thread", TurnID: "retry-one", RunID: "retry-one-run", OwnerID: "retry-one-owner",
		RetrySourceTurnID: "source-turn", RetrySourceEntryID: original.UserMessage.ID,
	})
	firstAssistant, err := repo.Append(ContextWithTurnLease(ctx, first.Lease), Entry{
		ThreadID: "thread", TurnID: "retry-one", Type: EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "partial"},
	}, AppendOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := repo.Append(ContextWithTurnLease(ctx, first.Lease), Entry{
		ThreadID: "thread", TurnID: "retry-one", Type: EntryTurnMarker, TurnStatus: TurnSavePoint,
	}, AppendOptions{}); err != nil {
		tb.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: first.Lease, RunID: "retry-one-run", TerminalEntryID: "retry-one-terminal", Status: TurnFailed,
		FailureMessage: "retry failed", Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureProvider},
		OutcomeFingerprint: "retry-one-outcome",
	}); err != nil {
		tb.Fatal(err)
	}
	second := admit(AdmitTurnRequest{
		ThreadID: "thread", TurnID: "retry-two", RunID: "retry-two-run", OwnerID: "retry-two-owner",
		RetrySourceTurnID: "retry-one", RetrySourceEntryID: firstAssistant.ID,
	})
	if _, err := repo.Append(ContextWithTurnLease(ctx, second.Lease), Entry{
		ThreadID: "thread", TurnID: "retry-two", Type: EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "done"},
	}, AppendOptions{}); err != nil {
		tb.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: second.Lease, RunID: "retry-two-run", TerminalEntryID: "retry-two-terminal", Status: TurnCompleted,
		OutcomeFingerprint: "retry-two-outcome",
	}); err != nil {
		tb.Fatal(err)
	}
	return repo
}

func buildMemoryRetrySourceChainFixture(tb testing.TB, durable bool) *MemoryRepo {
	tb.Helper()
	repo := NewMemoryRepo()
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	content := ""
	if durable {
		content = "durable input"
	}
	entries := []Entry{
		{ID: "source-started", ThreadID: "thread", TurnID: "source-turn", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "source-run"}},
		{
			ID: "source-user", ThreadID: "thread", TurnID: "source-turn", Type: EntryUserMessage,
			Message: session.Message{Role: session.User, Content: content, References: []session.MessageReference{{
				ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "quote", Text: "selection",
			}}},
		},
		{ID: "source-terminal", ThreadID: "thread", TurnID: "source-turn", Type: EntryTurnMarker, TurnStatus: TurnCompleted},
		{ID: "retry-one-started", ThreadID: "thread", TurnID: "retry-one", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "retry-one-run", RetrySourceTurnIDMetadataKey: "source-turn", RetrySourceEntryIDMetadataKey: "source-user"}},
		{ID: "retry-one-assistant", ThreadID: "thread", TurnID: "retry-one", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "partial"}},
		{ID: "retry-one-save-point", ThreadID: "thread", TurnID: "retry-one", Type: EntryTurnMarker, TurnStatus: TurnSavePoint},
		{ID: "retry-two-started", ThreadID: "thread", TurnID: "retry-two", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "retry-two-run", RetrySourceTurnIDMetadataKey: "retry-one", RetrySourceEntryIDMetadataKey: "retry-one-assistant"}},
		{ID: "retry-two-assistant", ThreadID: "thread", TurnID: "retry-two", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "done"}},
		{ID: "retry-two-terminal", ThreadID: "thread", TurnID: "retry-two", Type: EntryTurnMarker, TurnStatus: TurnCompleted},
	}
	for _, entry := range entries {
		if _, err := repo.Append(ctx, entry, AppendOptions{ID: entry.ID}); err != nil {
			tb.Fatal(err)
		}
	}
	repo.mu.Lock()
	if err := repo.rebuildRetryAdmissionFactsLocked("thread", repo.entries["thread"]); err != nil {
		repo.mu.Unlock()
		tb.Fatal(err)
	}
	repo.mu.Unlock()
	return repo
}

func BenchmarkThreadTurnsWithReferences(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("memory/%d", size), func(b *testing.B) {
			repo, since := buildMemoryCanonicalReferenceFixture(b, size)
			tail, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20})
			if err != nil || tail.BeforeCursor == nil {
				b.Fatalf("prepare cursors page=%#v err=%v", tail, err)
			}
			queries := map[string]ListCanonicalTurnsOptions{
				"tail":   {ThreadID: "thread", Tail: 20},
				"before": {ThreadID: "thread", BeforeCursor: tail.BeforeCursor, Limit: 20},
				"since":  {ThreadID: "thread", SinceCursor: &since, Limit: 20},
			}
			for _, mode := range []string{"tail", "before", "since"} {
				opts := queries[mode]
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					for index := 0; index < b.N; index++ {
						if _, err := repo.ListCanonicalTurns(context.Background(), opts); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkThreadTurnsWithReferences200x10(b *testing.B) {
	b.Run("memory/200x10", func(b *testing.B) {
		repo, since := buildMemoryCanonicalReferenceFixtureWithReferences(b, 200, canonicalBenchmarkReferencesTen())
		tail, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20})
		if err != nil || tail.BeforeCursor == nil {
			b.Fatalf("prepare cursors page=%#v err=%v", tail, err)
		}
		queries := map[string]ListCanonicalTurnsOptions{
			"tail":   {ThreadID: "thread", Tail: 20},
			"before": {ThreadID: "thread", BeforeCursor: tail.BeforeCursor, Limit: 20},
			"since":  {ThreadID: "thread", SinceCursor: &since, Limit: 20},
		}
		for mode, opts := range queries {
			b.Run(mode, func(b *testing.B) {
				b.ReportAllocs()
				for index := 0; index < b.N; index++ {
					if _, err := repo.ListCanonicalTurns(context.Background(), opts); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
}

func memoryCanonicalPageAllocs(t *testing.T, repo *MemoryRepo, since CanonicalTurnSinceCursor, mode string) float64 {
	t.Helper()
	tail, err := repo.ListCanonicalTurns(context.Background(), ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20})
	if err != nil || tail.BeforeCursor == nil {
		t.Fatalf("prepare %s cursor page=%#v err=%v", mode, tail, err)
	}
	opts := ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 20}
	switch mode {
	case "before":
		opts = ListCanonicalTurnsOptions{ThreadID: "thread", BeforeCursor: tail.BeforeCursor, Limit: 20}
	case "since":
		opts = ListCanonicalTurnsOptions{ThreadID: "thread", SinceCursor: &since, Limit: 20}
	}
	return testing.AllocsPerRun(20, func() {
		if _, err := repo.ListCanonicalTurns(context.Background(), opts); err != nil {
			panic(err)
		}
	})
}

func buildMemoryCanonicalReferenceFixture(tb testing.TB, turns int) (*MemoryRepo, CanonicalTurnSinceCursor) {
	return buildMemoryCanonicalReferenceFixtureWithReferences(tb, turns, canonicalBenchmarkReferences())
}

func buildMemoryCanonicalReferenceFixtureWithReferences(tb testing.TB, turns int, references []session.MessageReference) (*MemoryRepo, CanonicalTurnSinceCursor) {
	tb.Helper()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(context.Background(), ThreadMeta{ID: "thread"}); err != nil {
		tb.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 2, 0, 0, 0, time.UTC)
	parentID := ""
	since := CanonicalTurnSinceCursor{}
	repo.mu.Lock()
	for index := 0; index < turns; index++ {
		turnID := fmt.Sprintf("turn-%06d", index)
		started := PrepareEntry(Entry{
			ID: fmt.Sprintf("entry-%06d-started", index), ThreadID: "thread", ParentID: parentID,
			TurnID: turnID, Type: EntryTurnMarker, TurnStatus: TurnStarted,
			Metadata: map[string]string{"run_id": fmt.Sprintf("run-%06d", index)}, CreatedAt: now,
		})
		user := PrepareEntry(Entry{
			ID: fmt.Sprintf("entry-%06d-user", index), ThreadID: "thread", ParentID: started.ID,
			TurnID: turnID, Type: EntryUserMessage, CreatedAt: now,
			Message: session.Message{Role: session.User, Content: "inspect references", References: references},
		})
		repo.appendIndexedEntriesLocked("thread", started, user)
		parentID = user.ID
		if index == turns-21 {
			since.EntryID = user.ID
		}
	}
	meta := repo.threads["thread"]
	meta.LeafID = parentID
	meta.UpdatedAt = now
	repo.threads["thread"] = meta
	repo.mu.Unlock()
	return repo, since
}

func canonicalBenchmarkReferencesTen() []session.MessageReference {
	references := canonicalBenchmarkReferences()
	for index := len(references); index < 10; index++ {
		references = append(references, session.MessageReference{
			ReferenceID: fmt.Sprintf("reference-%d", index), Kind: session.MessageReferenceText,
			Label: fmt.Sprintf("Reference %d", index), Text: fmt.Sprintf("selected text %d", index),
		})
	}
	return references
}

func canonicalBenchmarkReferences() []session.MessageReference {
	return []session.MessageReference{
		{ReferenceID: "quote", Kind: session.MessageReferenceText, Label: "Selected text", Text: "the exact referenced text"},
		{ReferenceID: "file", Kind: session.MessageReferenceFile, Label: "config.go", ResourceRef: "file:///workspace/config.go", Text: "package config"},
	}
}

func appendCanonicalTurnFixture(t *testing.T, ctx context.Context, repo Repo, threadID, turnID, runID, input string) string {
	t.Helper()
	_, err := repo.Append(ctx, Entry{
		ThreadID: threadID, TurnID: turnID, Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": runID}, CreatedAt: time.Now().UTC(),
	}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	user, err := repo.Append(ctx, Entry{
		ThreadID: threadID, TurnID: turnID, Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: input}, CreatedAt: time.Now().UTC(),
	}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return user.ID
}

func appendCanonicalReferenceTurnFixture(t *testing.T, ctx context.Context, repo Repo, threadID, turnID, runID, input string) string {
	t.Helper()
	if _, err := repo.Append(ctx, Entry{
		ThreadID: threadID, TurnID: turnID, Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": runID}, CreatedAt: time.Now().UTC(),
	}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	user, err := repo.Append(ctx, Entry{
		ThreadID: threadID, TurnID: turnID, Type: EntryUserMessage,
		Message: session.Message{
			Role: session.User, Content: input, References: []session.MessageReference{{
				ReferenceID: "context:0", Kind: session.MessageReferenceText, Label: "Selected text", Text: input + " reference",
			}},
		},
		CreatedAt: time.Now().UTC(),
	}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return user.ID
}

func corruptMemoryReferenceRawDrift(t *testing.T, repo *MemoryRepo, threadID, entryID string) {
	t.Helper()
	repo.mu.Lock()
	defer repo.mu.Unlock()
	for index := range repo.entries[threadID] {
		entry := &repo.entries[threadID][index]
		if entry.ID == entryID {
			entry.Message.References[0].Text = "changed without raw update"
			return
		}
	}
	t.Fatalf("entry %q not found", entryID)
}

func corruptFileReferenceRawDrift(t *testing.T, root, threadID, entryID string) {
	t.Helper()
	path := filepath.Join(root, safePath(threadID), "entries.jsonl")
	entries, err := readEntries(path)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for index := range entries {
		if entries[index].ID == entryID {
			entries[index].Message.References[0].Text = "changed without raw update"
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("entry %q not found", entryID)
	}
	if err := rewriteEntriesForTest(path, entries); err != nil {
		t.Fatal(err)
	}
}

func rewriteEntriesForTest(path string, entries []Entry) error {
	data := make([]byte, 0)
	for _, entry := range entries {
		raw, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		data = append(data, raw...)
		data = append(data, '\n')
	}
	return os.WriteFile(path, data, 0o600)
}

func writeLegacyEntriesWithoutPathDepthForTest(path string, entries []Entry) error {
	data := make([]byte, 0)
	for _, entry := range entries {
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		var legacy map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &legacy); err != nil {
			return err
		}
		delete(legacy, "path_depth")
		encoded, err = json.Marshal(legacy)
		if err != nil {
			return err
		}
		data = append(data, encoded...)
		data = append(data, '\n')
	}
	return os.WriteFile(path, data, 0o600)
}

func preservePathForTest(t *testing.T, path string) func() {
	t.Helper()
	fixedTime := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, fixedTime, fixedTime); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("journal bytes changed without a valid migration:\n got: %s\nwant: %s", got, want)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.ModTime().Equal(fixedTime) {
			t.Fatalf("journal mtime=%s, want unchanged %s", info.ModTime(), fixedTime)
		}
	}
}
