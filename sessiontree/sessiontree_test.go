package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/artifact"
	"github.com/floegence/floret/session/compaction"
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

func TestMemoryRepoThreadTitleMetadataDoesNotChangeUpdatedAt(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	created := time.Date(2026, 6, 5, 10, 0, 0, 0, time.UTC)
	meta, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: created})
	if err != nil {
		t.Fatal(err)
	}
	meta.Title = "Plan release checks"
	meta.TitleStatus = ThreadTitleReady
	meta.TitleSource = ThreadTitleSourceProvider
	meta.TitleUpdatedAt = created.Add(time.Minute)
	if err := repo.UpdateThread(ctx, meta); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != meta.Title || got.TitleStatus != ThreadTitleReady || got.TitleSource != ThreadTitleSourceProvider || !got.TitleUpdatedAt.Equal(meta.TitleUpdatedAt) {
		t.Fatalf("title metadata = %#v", got)
	}
	if !got.UpdatedAt.Equal(created) {
		t.Fatalf("title update should not reorder thread updated_at: %#v", got)
	}
}

func TestListThreadsOrdersByCreatedAtWithStableCursor(t *testing.T) {
	ctx := context.Background()
	for name, repo := range map[string]Repo{
		"memory": NewMemoryRepo(),
		"file":   NewFileRepo(t.TempDir()),
	} {
		t.Run(name, func(t *testing.T) {
			older := time.Date(2026, 6, 5, 9, 0, 0, 0, time.UTC)
			newer := older.Add(time.Hour)
			sameNewest := newer.Add(time.Hour)
			for _, meta := range []ThreadMeta{
				{ID: "older", CreatedAt: older},
				{ID: "beta", CreatedAt: sameNewest},
				{ID: "alpha", CreatedAt: sameNewest},
				{ID: "newer", CreatedAt: newer},
				{ID: "archived", CreatedAt: sameNewest.Add(time.Hour), Archived: true},
			} {
				if _, err := repo.CreateThread(ctx, meta); err != nil {
					t.Fatalf("CreateThread(%s): %v", meta.ID, err)
				}
			}
			updatedOlder, err := repo.Thread(ctx, "older")
			if err != nil {
				t.Fatal(err)
			}
			updatedOlder.UpdatedAt = sameNewest.Add(24 * time.Hour)
			if err := repo.UpdateThread(ctx, updatedOlder); err != nil {
				t.Fatal(err)
			}

			firstPage, err := ListThreads(ctx, repo, ListThreadsOptions{Limit: 2})
			if err != nil {
				t.Fatal(err)
			}
			if got := threadIDs(firstPage); !slices.Equal(got, []string{"alpha", "beta"}) {
				t.Fatalf("first page ids=%v, want stable created_at order", got)
			}
			secondPage, err := ListThreads(ctx, repo, ListThreadsOptions{
				AfterCreatedAt: firstPage[len(firstPage)-1].CreatedAt,
				AfterID:        firstPage[len(firstPage)-1].ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := threadIDs(secondPage); !slices.Equal(got, []string{"newer", "older"}) {
				t.Fatalf("second page ids=%v, want cursor after created_at/id", got)
			}
			withArchived, err := ListThreads(ctx, repo, ListThreadsOptions{IncludeArchived: true, Limit: 1})
			if err != nil {
				t.Fatal(err)
			}
			if got := threadIDs(withArchived); !slices.Equal(got, []string{"archived"}) {
				t.Fatalf("archived ids=%v, want archived included at created_at position", got)
			}
		})
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

func TestRepoRejectsDuplicateThreadAndForkIDs(t *testing.T) {
	ctx := context.Background()
	for _, repo := range []Repo{NewMemoryRepo(), NewFileRepo(t.TempDir())} {
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source"}); !errors.Is(err, ErrThreadExists) {
			t.Fatalf("%T duplicate create err = %v, want ErrThreadExists", repo, err)
		}
		if _, err := AppendMessage(ctx, repo, "source", "turn-1", session.Message{Role: session.User, Content: "seed"}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "existing"}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "existing"}); !errors.Is(err, ErrThreadExists) {
			t.Fatalf("%T duplicate fork err = %v, want ErrThreadExists", repo, err)
		}
		entries, err := repo.Entries(ctx, "existing")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("%T duplicate fork polluted existing entries: %#v", repo, entries)
		}
	}
}

func TestFileRepoThreadIDsDoNotShareEncodedDirectoryOrLease(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := NewFileRepo(root)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "a/b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "a_b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendMessage(ctx, repo, "a/b", "turn-a", session.Message{Role: session.User, Content: "slash"}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendMessage(ctx, repo, "a_b", "turn-b", session.Message{Role: session.User, Content: "underscore"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AcquireTurnLease(ctx, TurnLease{ThreadID: "a/b", TurnID: "turn-a", OwnerID: "owner-a", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AcquireTurnLease(ctx, TurnLease{ThreadID: "a_b", TurnID: "turn-b", OwnerID: "owner-b", CreatedAt: time.Now()}); err != nil {
		t.Fatalf("distinct thread IDs should not share lease file: %v", err)
	}
	reloaded := NewFileRepo(root)
	slash, err := reloaded.Entries(ctx, "a/b")
	if err != nil {
		t.Fatal(err)
	}
	underscore, err := reloaded.Entries(ctx, "a_b")
	if err != nil {
		t.Fatal(err)
	}
	if len(slash) != 1 || slash[0].Message.Content != "slash" || len(underscore) != 1 || underscore[0].Message.Content != "underscore" {
		t.Fatalf("distinct thread journals polluted: slash=%#v underscore=%#v", slash, underscore)
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
	if len(got) != 3 || got[0].Role != session.User || got[0].Kind != session.MessageKindCompactionSummary || !strings.Contains(got[0].Content, "<compaction_summary") || got[1].Content != "kept" || got[2].Content != "tail" {
		t.Fatalf("compaction should replace earlier head and keep suffix: %#v", got)
	}
	entries, _ := repo.Entries(ctx, "thread")
	if len(entries) != 4 {
		t.Fatalf("compaction should not delete old entries: %#v", entries)
	}
}

func TestCompactionContextEmbedsKeptUsersInCheckpointAndDedupesTail(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	oldUser, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "old user"})
	_, _ = AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Assistant, Content: "old assistant"})
	latestUser, _ := AppendMessage(ctx, repo, "thread", "turn-2", session.Message{Role: session.User, Content: "latest user"})
	if _, err := repo.Append(ctx, Entry{
		ThreadID:         "thread",
		Type:             EntryCompaction,
		Summary:          "summary",
		FirstKeptEntryID: latestUser.ID,
		KeptUserEntryIDs: []string{oldUser.ID, latestUser.ID},
	}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	tail, _ := AppendMessage(ctx, repo, "thread", "turn-2", session.Message{Role: session.Assistant, Content: "tail assistant"})
	path, err := repo.Path(ctx, "thread", tail.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := BuildContext(path, ContextOptions{})
	contents := messageContents(got)
	want := []string{got[0].Content, "latest user", "tail assistant"}
	if !slices.Equal(contents, want) {
		t.Fatalf("context contents = %#v, want %#v; full=%#v", contents, want, got)
	}
	if got[0].Role != session.User || got[0].Kind != session.MessageKindCompactionSummary {
		t.Fatalf("context should start with user checkpoint: %#v", got)
	}
	if !strings.Contains(got[0].Content, `"entry_id": "`+oldUser.ID+`"`) || !strings.Contains(got[0].Content, "old user") {
		t.Fatalf("tail-external kept user should be embedded in checkpoint: %#v", got[0])
	}
	if strings.Contains(got[0].Content, `"entry_id": "`+latestUser.ID+`"`) {
		t.Fatalf("tail user should not be duplicated inside checkpoint: %#v", got[0])
	}
}

func TestCompactionContextDoesNotInferTailWhenKeptUsersAreExplicit(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	user, _ := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "single user"})
	if _, err := repo.Append(ctx, Entry{
		ThreadID:         "thread",
		Type:             EntryCompaction,
		Summary:          "summary",
		KeptUserEntryIDs: []string{user.ID},
	}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	got := BuildContext(path, ContextOptions{})
	contents := messageContents(got)
	want := []string{got[0].Content}
	if !slices.Equal(contents, want) {
		t.Fatalf("context contents = %#v, want %#v; full=%#v", contents, want, got)
	}
	if len(got) != 1 || got[0].Role != session.User || got[0].Kind != session.MessageKindCompactionSummary {
		t.Fatalf("explicit kept users without tail should produce one checkpoint: %#v", got)
	}
	if !strings.Contains(got[0].Content, `"entry_id": "`+user.ID+`"`) || !strings.Contains(got[0].Content, "single user") {
		t.Fatalf("explicit kept user should be embedded in checkpoint: %#v", got[0])
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
		KeptUserEntryIDs:        []string{old.ID, kept1.ID},
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
	kept3, _ := AppendMessage(ctx, repo, "thread", "t3", session.Message{Role: session.User, Content: "kept3"})
	_, err = AppendCompaction(ctx, repo, "thread", "t3", compaction.Result{
		CompactionID:            "c2",
		PreviousCompactionID:    "c1",
		FirstKeptEntryID:        kept3.ID,
		KeptUserEntryIDs:        []string{kept2.ID, kept3.ID},
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
	if len(got) != 2 || !strings.Contains(got[0].Content, "S2") || got[0].Kind != session.MessageKindCompactionSummary || got[1].Content != "kept3" {
		t.Fatalf("context should use only last compaction boundary: %#v", got)
	}
	if summary := compaction.ExtractCheckpointSummary(got[0].Content); summary != "S2" {
		t.Fatalf("context should use only latest compaction summary, got %q", summary)
	}
	if strings.Count(got[0].Content, "<preserved_user_inputs>") != 1 || strings.Count(got[0].Content, "<compaction_summary") != 1 {
		t.Fatalf("latest checkpoint should have one preserved block and one summary envelope: %q", got[0].Content)
	}
	if !strings.Contains(got[0].Content, "The retained tail follows") || strings.Contains(got[0].Content, "No retained tail follows this checkpoint.") {
		t.Fatalf("durable checkpoint should describe the retained tail it is followed by: %q", got[0].Content)
	}
	preserved := preservedUsersFromCheckpoint(t, got[0].Content)
	if len(preserved) != 1 || preserved[0].EntryID != kept2.ID || preserved[0].Content != "kept2" {
		t.Fatalf("latest checkpoint should preserve only tail-external kept users: %#v", preserved)
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
		KeptUserEntryIDs:        []string{old.ID},
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
	if len(comp.KeptUserEntryIDs) > 0 && comp.KeptUserEntryIDs[0] == old.ID {
		t.Fatalf("fork should rewrite kept user refs: %#v", comp)
	}
	if comp.PreviousCompactionID != "" {
		t.Fatalf("fork should not rewrite stable previous compaction id: %#v", comp)
	}
	got := BuildContext(path, ContextOptions{})
	if len(got) != 2 || !strings.Contains(got[0].Content, "old") || !strings.Contains(got[0].Content, "summary") || got[1].Content != "kept" {
		t.Fatalf("forked context should retain rewritten tail: %#v", got)
	}
}

func messageContents(messages []session.Message) []string {
	out := make([]string, len(messages))
	for i, msg := range messages {
		out[i] = msg.Content
	}
	return out
}

type checkpointPreservedUser struct {
	EntryID string `json:"entry_id"`
	Content string `json:"content"`
}

func preservedUsersFromCheckpoint(t *testing.T, content string) []checkpointPreservedUser {
	t.Helper()
	start := strings.Index(content, "<preserved_user_inputs>")
	if start < 0 {
		return nil
	}
	start += len("<preserved_user_inputs>")
	end := strings.Index(content[start:], "</preserved_user_inputs>")
	if end < 0 {
		t.Fatalf("checkpoint preserved block is not closed: %q", content)
	}
	block := content[start : start+end]
	jsonStart := strings.Index(block, "[")
	jsonEnd := strings.LastIndex(block, "]")
	if jsonStart < 0 || jsonEnd < jsonStart {
		t.Fatalf("checkpoint preserved block has no JSON array: %q", block)
	}
	var users []checkpointPreservedUser
	if err := json.Unmarshal([]byte(block[jsonStart:jsonEnd+1]), &users); err != nil {
		t.Fatalf("decode checkpoint preserved users: %v\n%s", err, block[jsonStart:jsonEnd+1])
	}
	return users
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

func TestBuildContextProjectionKeepsMessagesCanonicalAndExposesArtifactSegments(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	ref := artifact.Ref{ID: "artifact-1", SafeLabel: "tool-output.log", URL: "/artifacts/tool-output.log", Kind: artifact.DefaultKind, MIME: artifact.DefaultMIME, SizeBytes: 128, SHA256: "abc123"}
	if _, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "inspect"}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.Tool, Content: "visible", ToolCallID: "call-1", ToolName: "read", ToolResult: &session.ToolResultView{Truncated: true, OriginalBytes: 128, VisibleBytes: 7, Strategy: "tail", ContentSHA256: "abc123", FullOutput: &ref}}); err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	projection := BuildContextProjection(path, ContextProjectionOptions{Purpose: ProjectionTestUI})
	canonical := BuildContext(path, ContextOptions{})
	if len(projection.Messages) != len(canonical) || projection.Messages[1].Content != canonical[1].Content {
		t.Fatalf("projection messages should match BuildContext: projection=%#v canonical=%#v", projection.Messages, canonical)
	}
	if len(projection.Segments) != len(projection.Messages) ||
		len(projection.Segments[1].ArtifactRefs) != 1 ||
		projection.Segments[1].ArtifactRefs[0].ID != ref.ID ||
		projection.Segments[1].UIPreview != "visible" {
		t.Fatalf("projection segments missing artifact metadata: %#v", projection.Segments)
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
	meta, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	updated := meta.UpdatedAt
	meta.Title = "Persist title metadata"
	meta.TitleStatus = ThreadTitleReady
	meta.TitleSource = ThreadTitleSourceProvider
	meta.TitleUpdatedAt = updated.Add(time.Minute)
	if err := repo.UpdateThread(ctx, meta); err != nil {
		t.Fatal(err)
	}
	reloaded := NewFileRepo(root)
	meta, err = reloaded.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != user.ID {
		t.Fatalf("reloaded leaf = %q, want %q", meta.LeafID, user.ID)
	}
	if meta.Title != "Persist title metadata" || meta.TitleStatus != ThreadTitleReady || meta.TitleSource != ThreadTitleSourceProvider || !meta.UpdatedAt.Equal(updated) {
		t.Fatalf("reloaded title metadata = %#v", meta)
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

func TestMemoryAndFileRepoDeleteThread(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo Repo
	}{
		{name: "memory", repo: NewMemoryRepo()},
		{name: "file", repo: NewFileRepo(t.TempDir())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := AppendMessage(ctx, tc.repo, "thread", "turn-1", session.Message{Role: session.User, Content: "hello"}); err != nil {
				t.Fatal(err)
			}
			if err := tc.repo.DeleteThread(ctx, "thread"); err != nil {
				t.Fatal(err)
			}
			if _, err := tc.repo.Thread(ctx, "thread"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("thread err = %v, want ErrThreadNotFound", err)
			}
			if _, err := tc.repo.Entries(ctx, "thread"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("entries err = %v, want ErrThreadNotFound", err)
			}
			if err := tc.repo.DeleteThread(ctx, "thread"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("second delete err = %v, want ErrThreadNotFound", err)
			}
		})
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
	if err := os.WriteFile(filepath.Join(root, safePath("thread"), "thread.json"), data, 0o600); err != nil {
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
	if err := os.WriteFile(filepath.Join(root, safePath("thread"), "thread.json"), data, 0o600); err != nil {
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
	threadDir := filepath.Join(root, safePath("thread"))
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

func threadIDs(threads []ThreadMeta) []string {
	out := make([]string, 0, len(threads))
	for _, thread := range threads {
		out = append(out, thread.ID)
	}
	return out
}
