package sessiontree

import (
	"context"
	"errors"
	"testing"

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
	if len(got) != 3 || got[0].Content != "summary" || got[1].Content != "kept" || got[2].Content != "tail" {
		t.Fatalf("compaction should replace earlier head and keep suffix: %#v", got)
	}
	entries, _ := repo.Entries(ctx, "thread")
	if len(entries) != 4 {
		t.Fatalf("compaction should not delete old entries: %#v", entries)
	}
}

func TestBuildContextConvertsSignalToolCallsToProviderSafeAssistantText(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	user, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "hello"})
	ask, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "ask", ToolName: "ask_user", ToolArgs: "more?"})
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
