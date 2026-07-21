package agentharness

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestThreadEntryOrdinalUsesExactCanonicalPathDepth(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	root, err := repo.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MoveLeaf(ctx, "thread", root.ID); err != nil {
		t.Fatal(err)
	}
	active, err := repo.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}

	counting := &entryLookupCountingRepo{JournalRepo: repo}
	harness := &AgentHarness{options: Options{Repo: counting}}
	if got := harness.threadEntryOrdinal(active); got != 2 {
		t.Fatalf("ordinal = %d, want canonical path depth 2", got)
	}
	if counting.entryCalls != 1 || counting.entriesCalls != 0 {
		t.Fatalf("lookup calls = Entry:%d Entries:%d, want Entry:1 Entries:0", counting.entryCalls, counting.entriesCalls)
	}
}

func TestCommittedEffectResultUsesExactCanonicalEntry(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	message := session.Message{
		Role: session.Tool, Content: "committed", ToolCallID: "call-1", ToolName: "effect",
	}
	entry, err := repo.Append(ctx, sessiontree.Entry{
		ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryToolResult, Message: message,
	}, sessiontree.AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}

	counting := &entryLookupCountingRepo{JournalRepo: repo}
	projection := &turnProjection{
		ctx: ctx, turnID: "turn",
		thread: &Thread{id: "thread", harness: &AgentHarness{options: Options{Repo: counting}}},
	}
	committed, err := projection.committedEffectResult(message, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("canonical effect result was not recognized")
	}
	if counting.entryCalls != 1 || counting.threadCalls != 0 || counting.pathCalls != 0 {
		t.Fatalf("lookup calls = Entry:%d Thread:%d Path:%d, want Entry:1 Thread:0 Path:0", counting.entryCalls, counting.threadCalls, counting.pathCalls)
	}

	counting.reset()
	committed, err = projection.committedEffectResult(message, "")
	if err != nil || committed {
		t.Fatalf("empty canonical entry identity = (%t, %v), want (false, nil)", committed, err)
	}
	if counting.entryCalls != 0 || counting.threadCalls != 0 || counting.pathCalls != 0 {
		t.Fatalf("empty identity performed lookups: %#v", counting)
	}

	counting.reset()
	tampered := session.CloneMessage(message)
	tampered.Content = "different"
	if _, err := projection.committedEffectResult(tampered, entry.ID); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("signature mismatch error = %v, want ErrAuthorityCorrupt", err)
	}
	if counting.entryCalls != 1 || counting.threadCalls != 0 || counting.pathCalls != 0 {
		t.Fatalf("mismatch lookup calls = Entry:%d Thread:%d Path:%d, want Entry:1 Thread:0 Path:0", counting.entryCalls, counting.threadCalls, counting.pathCalls)
	}

	counting.reset()
	if _, err := projection.committedEffectResult(message, "missing"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("missing canonical entry error = %v, want ErrAuthorityCorrupt", err)
	}
}

type entryLookupCountingRepo struct {
	sessiontree.JournalRepo
	entryCalls   int
	entriesCalls int
	threadCalls  int
	pathCalls    int
}

func (r *entryLookupCountingRepo) Entry(ctx context.Context, threadID, entryID string) (sessiontree.Entry, error) {
	r.entryCalls++
	return r.JournalRepo.Entry(ctx, threadID, entryID)
}

func (r *entryLookupCountingRepo) Entries(ctx context.Context, threadID string) ([]sessiontree.Entry, error) {
	r.entriesCalls++
	return r.JournalRepo.Entries(ctx, threadID)
}

func (r *entryLookupCountingRepo) Thread(ctx context.Context, threadID string) (sessiontree.ThreadMeta, error) {
	r.threadCalls++
	return r.JournalRepo.Thread(ctx, threadID)
}

func (r *entryLookupCountingRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	r.pathCalls++
	return r.JournalRepo.Path(ctx, threadID, leafID)
}

func (r *entryLookupCountingRepo) reset() {
	r.entryCalls = 0
	r.entriesCalls = 0
	r.threadCalls = 0
	r.pathCalls = 0
}
