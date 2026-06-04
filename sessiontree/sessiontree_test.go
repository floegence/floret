package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/compaction"
	"github.com/floegence/floret/session"
)

func TestMemoryRepoAppendUpdatesLeafAndBuildContextFiltersEntries(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	meta, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != "" {
		t.Fatalf("new thread leaf = %q", meta.LeafID)
	}
	user, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{ThreadID: "thread", Type: EntryModelChange, Provider: "fake", Model: "fake-model"}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	assistant, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Assistant, Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	meta, err = repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != assistant.ID {
		t.Fatalf("leaf = %q, want %q", meta.LeafID, assistant.ID)
	}
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	contextMessages := BuildContext(path, ContextOptions{})
	if len(contextMessages) != 2 || contextMessages[0].Content != "hello" || contextMessages[1].Content != "hi" {
		t.Fatalf("context should include only provider-visible messages: %#v", contextMessages)
	}
	if user.Raw == "" || user.RawHash == "" {
		t.Fatalf("entry raw ledger was not created: %#v", user)
	}
}

func TestFileRepoReadsLongJSONLEntries(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "start"}); err != nil {
		t.Fatal(err)
	}
	longResult := strings.Repeat("weather payload ", 12_000)
	tool, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Tool, Content: longResult, ToolCallID: "fetch-1", ToolName: "web_fetch"})
	if err != nil {
		t.Fatal(err)
	}

	restored := NewFileRepo(root)
	entries, err := restored.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[1].Message.Content != longResult {
		t.Fatalf("restored entries = %#v", entries)
	}
	path, err := restored.Path(ctx, "thread", tool.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(path) != 2 || path[1].Message.Content != longResult {
		t.Fatalf("path = %#v", path)
	}
	if _, err := AppendMessage(ctx, restored, "thread", "turn-2", session.Message{Role: session.User, Content: "continue"}); err != nil {
		t.Fatal(err)
	}
}

func TestFileRepoSkipsMalformedThreadWithoutHidingValidThreads(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "good"}); err != nil {
		t.Fatal(err)
	}
	longResult := strings.Repeat("weather payload ", 12_000)
	if _, err := AppendMessage(ctx, repo, "good", "turn-1", session.Message{Role: session.Tool, Content: longResult, ToolCallID: "fetch-1", ToolName: "web_fetch"}); err != nil {
		t.Fatal(err)
	}
	badDir := filepath.Join(root, "bad")
	if err := os.MkdirAll(badDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "thread.json"), []byte(`{"id":"bad","created_at":"2026-06-03T00:00:00Z","updated_at":"2026-06-03T00:00:00Z","leaf_id":"bad-entry"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "entries.jsonl"), []byte(`{"id":"bad-entry"`), 0o600); err != nil {
		t.Fatal(err)
	}

	restored := NewFileRepo(root)
	entries, err := restored.Entries(ctx, "good")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Message.Content != longResult {
		t.Fatalf("good entries = %#v", entries)
	}
	if _, err := restored.Thread(ctx, "bad"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("bad thread err = %v, want ErrThreadNotFound", err)
	}
}

func TestMoveLeafPreservesOldBranch(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	root, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "root"})
	branchA, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Assistant, Content: "a"})
	if err := repo.MoveLeaf(ctx, "thread", root.ID); err != nil {
		t.Fatal(err)
	}
	branchB, _ := AppendMessage(ctx, repo, "thread", "turn-2", session.Message{Role: session.Assistant, Content: "b"})
	entries, err := repo.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("move should not delete old branch: %#v", entries)
	}
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if path[len(path)-1].ID != branchB.ID || pathContains(path, branchA.ID) {
		t.Fatalf("active path should point to branch B only: %#v", path)
	}
	oldBranchPath, err := repo.Path(ctx, "thread", branchA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldBranchPath[len(oldBranchPath)-1].ID != branchA.ID {
		t.Fatalf("old branch should remain readable by explicit leaf: %#v", oldBranchPath)
	}
}

func TestForkAtAndBeforeUserMessage(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	root, _ := AppendMessage(ctx, repo, "source", "turn-1", session.Message{Role: session.User, Content: "one"})
	second, _ := AppendMessage(ctx, repo, "source", "turn-2", session.Message{Role: session.User, Content: "two"})
	at, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", EntryID: second.ID, NewThreadID: "fork-at"})
	if err != nil {
		t.Fatal(err)
	}
	before, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", EntryID: second.ID, Position: ForkBefore, NewThreadID: "fork-before"})
	if err != nil {
		t.Fatal(err)
	}
	atPath, _ := repo.Path(ctx, at.ID, "")
	beforePath, _ := repo.Path(ctx, before.ID, "")
	if got := BuildContext(atPath, ContextOptions{}); len(got) != 2 || got[1].Content != "two" {
		t.Fatalf("fork at should include target user: %#v", got)
	}
	if got := BuildContext(beforePath, ContextOptions{}); len(got) != 1 || got[0].Content != "one" {
		t.Fatalf("fork before should stop at parent: %#v", got)
	}
	if before.ForkedFromEntryID != root.ID {
		t.Fatalf("fork metadata should point to effective source leaf: %#v", before)
	}
}

func TestCompactionContextReplacesHeadAndKeepsTail(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	_, _ = AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "old"})
	kept, _ := AppendMessage(ctx, repo, "thread", "turn-2", session.Message{Role: session.User, Content: "kept"})
	if _, err := repo.Append(ctx, Entry{ThreadID: "thread", Type: EntryCompaction, Summary: "summary", FirstKeptEntryID: kept.ID}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	tail, _ := AppendMessage(ctx, repo, "thread", "turn-3", session.Message{Role: session.User, Content: "tail"})
	path, err := repo.Path(ctx, "thread", tail.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := BuildContext(path, ContextOptions{})
	if len(got) != 3 || got[0].Content != "summary" || got[0].Kind != session.MessageKindCompactionSummary || got[1].Content != "kept" || got[2].Content != "tail" {
		t.Fatalf("compaction should replace earlier head and keep suffix: %#v", got)
	}
	entries, _ := repo.Entries(ctx, "thread")
	if len(entries) != 4 {
		t.Fatalf("compaction should not delete old entries: %#v", entries)
	}
}

func TestMultipleCompactionsUseOnlyLastBoundary(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	old, _ := AppendMessage(ctx, repo, "thread", "t1", session.Message{Role: session.User, Content: "old"})
	kept1, _ := AppendMessage(ctx, repo, "thread", "t2", session.Message{Role: session.User, Content: "kept1"})
	c1, err := AppendCompaction(ctx, repo, "thread", "t2", compaction.Result{
		CompactionID:            "c1",
		FirstKeptEntryID:        kept1.ID,
		CompactedThroughEntryID: old.ID,
		Summary:                 "S1",
		SummarySchemaVersion:    compaction.SummarySchemaVersion,
		Trigger:                 compaction.TriggerPreRequest,
		Reason:                  compaction.ReasonThreshold,
		Phase:                   compaction.PhaseInstall,
	})
	if err != nil {
		t.Fatal(err)
	}
	kept2, _ := AppendMessage(ctx, repo, "thread", "t3", session.Message{Role: session.User, Content: "kept2"})
	_, err = AppendCompaction(ctx, repo, "thread", "t3", compaction.Result{
		CompactionID:            "c2",
		PreviousCompactionID:    "c1",
		FirstKeptEntryID:        kept2.ID,
		CompactedThroughEntryID: c1.ID,
		Summary:                 "S2",
		SummarySchemaVersion:    compaction.SummarySchemaVersion,
		Trigger:                 compaction.TriggerPostResponse,
		Reason:                  compaction.ReasonFollowUpPressure,
		Phase:                   compaction.PhaseInstall,
		Details:                 map[string]string{"compaction_generation": "2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	got := BuildContext(path, ContextOptions{})
	if len(got) != 2 || got[0].Content != "S2" || got[1].Content != "kept2" {
		t.Fatalf("context should use only last compaction boundary: %#v", got)
	}
	if pathContains(path, old.ID) && len(got) > 0 && got[0].Content == "S1" {
		t.Fatalf("old summary should not stack into active context: %#v", got)
	}
}

func TestForkRewritesCompactionReferences(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	old, _ := AppendMessage(ctx, repo, "source", "t1", session.Message{Role: session.User, Content: "old"})
	kept, _ := AppendMessage(ctx, repo, "source", "t2", session.Message{Role: session.User, Content: "kept"})
	compacted, err := AppendCompaction(ctx, repo, "source", "t2", compaction.Result{
		CompactionID:            "c1",
		FirstKeptEntryID:        kept.ID,
		CompactedThroughEntryID: old.ID,
		Summary:                 "summary",
		SummarySchemaVersion:    compaction.SummarySchemaVersion,
		Trigger:                 compaction.TriggerPreRequest,
		Reason:                  compaction.ReasonThreshold,
		Phase:                   compaction.PhaseInstall,
	})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", EntryID: compacted.ID, NewThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, fork.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	var comp Entry
	for _, entry := range path {
		if entry.Type == EntryCompaction {
			comp = entry
			break
		}
	}
	if comp.FirstKeptEntryID == kept.ID || comp.CompactedThroughEntryID == old.ID {
		t.Fatalf("fork should rewrite compaction entry refs: %#v", comp)
	}
	if comp.PreviousCompactionID != "" {
		t.Fatalf("fork should not rewrite stable previous compaction id: %#v", comp)
	}
	got := BuildContext(path, ContextOptions{})
	if len(got) != 2 || got[0].Content != "summary" || got[1].Content != "kept" {
		t.Fatalf("forked context should retain rewritten tail: %#v", got)
	}
}

func TestBuildContextConvertsSignalToolCallsToProviderSafeAssistantText(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	user, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "hello"})
	ask, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "ask", ToolName: "ask_user", ToolArgs: `{"question":"more?"}`})
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	got := BuildContext(path, ContextOptions{})
	if len(got) != 2 {
		t.Fatalf("context = %#v", got)
	}
	if got[0].EntryID != user.ID || got[0].ParentEntryID != user.ParentID {
		t.Fatalf("user entry refs missing: %#v", got[0])
	}
	if got[1].Role != session.Assistant || got[1].Content != "Agent requested user input: more?" || got[1].ToolName != "" || got[1].EntryID != ask.ID {
		t.Fatalf("signal tool call should be provider-safe assistant text with entry ref: %#v", got[1])
	}
}

func TestBuildContextAttachesEntryRefsToMessages(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "first"})
	second, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Assistant, Content: "second"})
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	got := BuildContext(path, ContextOptions{})
	if got[0].EntryID != first.ID || got[1].EntryID != second.ID || got[1].ParentEntryID != first.ID {
		t.Fatalf("entry refs missing from context: %#v", got)
	}
}

func TestFileRepoPersistsThreadLeafEntriesAndFork(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	user, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "persist me"})
	if err != nil {
		t.Fatal(err)
	}
	reloaded := NewFileRepo(root)
	meta, err := reloaded.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != user.ID {
		t.Fatalf("reloaded leaf = %q, want %q", meta.LeafID, user.ID)
	}
	path, err := reloaded.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := BuildContext(path, ContextOptions{}); len(got) != 1 || got[0].Content != "persist me" {
		t.Fatalf("reloaded context = %#v", got)
	}
	fork, err := reloaded.Fork(ctx, ForkOptions{SourceThreadID: "thread", EntryID: user.ID, NewThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if fork.ID != "fork" || fork.LeafID == "" {
		t.Fatalf("fork meta = %#v", fork)
	}
	reloadedAgain := NewFileRepo(root)
	forkMeta, err := reloadedAgain.Thread(ctx, "fork")
	if err != nil {
		t.Fatal(err)
	}
	if forkMeta.ForkedFromThreadID != "thread" || forkMeta.ForkedFromEntryID != user.ID {
		t.Fatalf("reloaded fork metadata = %#v", forkMeta)
	}
	forkPath, err := reloadedAgain.Path(ctx, "fork", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := BuildContext(forkPath, ContextOptions{}); len(got) != 1 || got[0].Content != "persist me" {
		t.Fatalf("reloaded fork context = %#v", got)
	}
}

func TestFileRepoAppendAfterReloadDoesNotReuseEntryID(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	reloaded := NewFileRepo(root)
	second, err := AppendMessage(ctx, reloaded, "thread", "turn-2", session.Message{Role: session.User, Content: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || second.ParentID == second.ID {
		t.Fatalf("reloaded append reused an entry id: first=%#v second=%#v", first, second)
	}
	path, err := reloaded.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := BuildContext(path, ContextOptions{}); len(got) != 2 || got[0].Content != "first" || got[1].Content != "second" {
		t.Fatalf("path after reload append = %#v", got)
	}
}

func TestFileRepoRepairsStaleThreadLeafFromJournal(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	meta, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := AppendTurnMarker(ctx, repo, "thread", "turn-1", TurnCompleted, nil)
	if err != nil {
		t.Fatal(err)
	}
	meta.LeafID = first.ID
	meta.UpdatedAt = first.CreatedAt
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "thread", "thread.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	restored := NewFileRepo(root)
	repaired, err := restored.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if repaired.LeafID != second.ID || !repaired.UpdatedAt.Equal(second.CreatedAt) {
		t.Fatalf("repaired meta = %#v, want leaf %q", repaired, second.ID)
	}
	path, err := restored.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	if !pathContains(path, second.ID) {
		t.Fatalf("path should include repaired terminal marker: %#v", path)
	}
}

func TestFileRepoRepairsMissingThreadLeafWhenJournalIsReachable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	meta, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "first"}); err != nil {
		t.Fatal(err)
	}
	terminal, err := AppendTurnMarker(ctx, repo, "thread", "turn-1", TurnAborted, nil)
	if err != nil {
		t.Fatal(err)
	}
	meta.LeafID = "missing-leaf"
	meta.UpdatedAt = meta.CreatedAt
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "thread", "thread.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	restored := NewFileRepo(root)
	repaired, err := restored.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if repaired.LeafID != terminal.ID || !repaired.UpdatedAt.Equal(terminal.CreatedAt) {
		t.Fatalf("repaired meta = %#v, want leaf %q", repaired, terminal.ID)
	}
}

func TestFileRepoDoesNotRepairMissingThreadLeafFromBrokenJournal(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	threadDir := filepath.Join(root, "thread")
	if err := os.MkdirAll(threadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	meta := ThreadMeta{ID: "thread", LeafID: "missing-leaf", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "thread.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	broken := Entry{ID: "orphan", ThreadID: "thread", ParentID: "missing-parent", Type: EntryTurnMarker, TurnID: "turn-1", TurnStatus: TurnCompleted, CreatedAt: meta.UpdatedAt.Add(time.Second)}
	entryData, err := json.Marshal(broken)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(threadDir, "entries.jsonl"), append(entryData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	restored := NewFileRepo(root)
	reloaded, err := restored.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.LeafID != "missing-leaf" {
		t.Fatalf("broken journal should not repair leaf: %#v", reloaded)
	}
	if _, err := restored.Path(ctx, "thread", ""); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("path err = %v, want ErrEntryNotFound", err)
	}
}

func TestInvalidParentIsRejected(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	_, err := repo.Append(ctx, Entry{ThreadID: "thread", Type: EntryCustom}, AppendOptions{ParentID: "missing"})
	if !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("err = %v, want invalid parent", err)
	}
}

func pathContains(path []Entry, id string) bool {
	for _, entry := range path {
		if entry.ID == id {
			return true
		}
	}
	return false
}
