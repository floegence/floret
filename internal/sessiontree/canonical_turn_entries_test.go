package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryCanonicalTurnEntriesTracksExactTurnAndCleansDelete(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 20, 15, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		RequestFingerprint: "request", Input: session.Message{Role: session.User, Content: "inspect"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	assistant, err := repo.Append(ContextWithTurnLease(ctx, admitted.Lease), Entry{
		ThreadID: "thread", TurnID: "turn", Type: EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "first"},
	}, AppendOptions{Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}

	entries, found, err := repo.CanonicalTurnEntries(ctx, "thread", "turn", "run")
	if err != nil || !found {
		t.Fatalf("CanonicalTurnEntries found=%v err=%v", found, err)
	}
	if len(entries) != 3 || entries[0].ID != admitted.TurnStarted.ID || entries[1].ID != admitted.UserMessage.ID || entries[2].ID != assistant.ID {
		t.Fatalf("canonical entries = %#v", entries)
	}
	if _, _, err := repo.CanonicalTurnEntries(ctx, "thread", "turn", "different-run"); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("run mismatch err=%v, want ErrRequestConflict", err)
	}

	if err := repo.ReleaseTurnLease(ctx, admitted.Lease); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if _, exists := repo.turnEntryOrdinals["thread"]; exists {
		t.Fatalf("turn entry index survived thread delete: %#v", repo.turnEntryOrdinals["thread"])
	}
}

func TestMemoryCanonicalTurnEntriesFailsClosedOnCorruptIndex(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		RequestFingerprint: "request", Input: session.Message{Role: session.User, Content: "inspect"},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.mu.Lock()
	repo.turnEntryOrdinals["thread"]["turn"] = []int{1, 0}
	repo.mu.Unlock()
	if _, found, err := repo.CanonicalTurnEntries(ctx, "thread", "turn", "run"); !found || !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt ordinal read found=%v err=%v", found, err)
	}

	repo.mu.Lock()
	repo.turnEntryOrdinals["thread"]["turn"] = []int{0, len(repo.entries["thread"])}
	repo.mu.Unlock()
	if _, found, err := repo.CanonicalTurnEntries(ctx, "thread", "turn", "run"); !found || !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("dangling ordinal read found=%v err=%v", found, err)
	}

	repo.mu.Lock()
	repo.turnEntryOrdinals["thread"]["turn"] = []int{0}
	repo.mu.Unlock()
	if _, found, err := repo.CanonicalTurnEntries(ctx, "thread", "turn", "run"); !found || !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("missing ordinal read found=%v err=%v", found, err)
	}

	_ = admitted
}

func TestFileRepoCanonicalTurnEntriesSurviveReopenAndDeleteReuse(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": "run"},
	}, AppendOptions{ID: "started"}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendMessage(ctx, repo, "thread", "turn", session.Message{Role: session.User, Content: "persisted"}); err != nil {
		t.Fatal(err)
	}

	reopened := NewFileRepo(root)
	entries, found, err := reopened.CanonicalTurnEntries(ctx, "thread", "turn", "run")
	if err != nil || !found || len(entries) != 2 || entries[0].ID != "started" || entries[1].Message.Content != "persisted" {
		t.Fatalf("reopened canonical entries=%#v found=%v err=%v", entries, found, err)
	}
	if err := reopened.DeleteThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": "replacement-run"},
	}, AppendOptions{ID: "replacement-started"}); err != nil {
		t.Fatalf("append after delete reuse: %v", err)
	}
	entries, found, err = reopened.CanonicalTurnEntries(ctx, "thread", "turn", "replacement-run")
	if err != nil || !found || len(entries) != 1 || entries[0].ID != "replacement-started" {
		t.Fatalf("replacement canonical entries=%#v found=%v err=%v", entries, found, err)
	}
}

func TestFileRepoCanonicalTurnIndexRollsBackFailedFork(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{
		ThreadID: "source", TurnID: "source-turn", Type: EntryTurnMarker, TurnStatus: TurnStarted,
		Metadata: map[string]string{"run_id": "source-run"},
	}, AppendOptions{ID: "source-started"}); err != nil {
		t.Fatal(err)
	}
	destinationDir := filepath.Join(root, safePath("fork"))
	if err := os.MkdirAll(destinationDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destinationDir, "occupied"), []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"}); err == nil {
		t.Fatal("fork succeeded despite occupied destination")
	}
	if _, exists := repo.mem.turnEntryOrdinals["fork"]; exists {
		t.Fatalf("failed fork retained canonical index: %#v", repo.mem.turnEntryOrdinals["fork"])
	}
	if err := os.RemoveAll(destinationDir); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	entries, found, err := NewFileRepo(root).CanonicalTurnEntries(ctx, "fork", "source-turn", "source-run")
	if err != nil || !found || len(entries) == 0 {
		t.Fatalf("retried fork canonical entries=%#v found=%v err=%v", entries, found, err)
	}
}

func TestFileRepoReopenRejectsCorruptCanonicalTurnJournal(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []Entry{
		{ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "run-a"}},
		{ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "run-b"}},
	} {
		entry = PrepareEntry(entry)
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, safePath("thread"), "entries.jsonl")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewFileRepo(root).Thread(ctx, "thread"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt file repo reopen err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryCanonicalTurnEntriesRejectsReferenceRawDrift(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request",
		Input: session.Message{Role: session.User, Content: "inspect", References: []session.MessageReference{{
			ReferenceID: "context:0", Kind: session.MessageReferenceText, Label: "quote", Text: "original",
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	for index := range repo.entries["thread"] {
		if repo.entries["thread"][index].ID == admitted.UserMessage.ID {
			repo.entries["thread"][index].Message.References[0].Text = "changed without raw update"
		}
	}
	repo.mu.Unlock()
	if _, found, err := repo.CanonicalTurnEntries(ctx, "thread", "turn", "run"); !found || !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("reference raw drift found=%v err=%v", found, err)
	}
}
