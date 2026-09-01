package sessiontree

import (
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func TestForkRemovesEffectAuthorityAndPreservesConversationPath(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "request-fingerprint", InputRequestFingerprint: "input-fingerprint",
		Input: session.Message{Role: session.User, Content: "run an effect"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AppendRuntimeFacts(ctx, "thread", []Entry{{
		ID: "tool-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "shell", ToolArgs: `{}`}, CreatedAt: now,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
		Invocation: EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "shell", ArgumentHash: StableHash(`{}`),
		},
		RequestFingerprint: "effect-fingerprint", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CancelTurn(ctx, testCancelTurnRequest(now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}

	first, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork-one", Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	assertForkWithoutEffectAuthority(t, repo.domainMemory, first)
	second, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "fork-one", NewThreadID: "fork-two", Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	assertForkWithoutEffectAuthority(t, repo.domainMemory, second)

	reopened, err := NewBackendRepo(ctx, backend, func() time.Time { return now.Add(4 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	for _, threadID := range []string{"fork-one", "fork-two"} {
		meta, err := reopened.Thread(ctx, threadID)
		if err != nil {
			t.Fatal(err)
		}
		assertForkWithoutEffectAuthority(t, reopened.domainMemory, meta)
	}
}

func assertForkWithoutEffectAuthority(t *testing.T, repo *MemoryRepo, meta ThreadMeta) {
	t.Helper()
	path, err := repo.Path(t.Context(), meta.ID, meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	for index, entry := range path {
		if entry.Type == EntryEffectAttempt {
			t.Fatalf("fork %q copied effect authority entry %q", meta.ID, entry.ID)
		}
		if entry.ThreadID != meta.ID {
			t.Fatalf("fork entry %q thread_id=%q", entry.ID, entry.ThreadID)
		}
		if entry.PathDepth != int64(index+1) {
			t.Fatalf("fork entry %q depth=%d, want %d", entry.ID, entry.PathDepth, index+1)
		}
		wantParent := ""
		if index > 0 {
			wantParent = path[index-1].ID
		}
		if entry.ParentID != wantParent {
			t.Fatalf("fork entry %q parent=%q, want %q", entry.ID, entry.ParentID, wantParent)
		}
	}
}
