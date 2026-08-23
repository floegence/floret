package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v4/internal/session"
)

func TestEffectAttemptDecodesPublishedV3PascalShape(t *testing.T) {
	var attempt EffectAttempt
	if err := json.Unmarshal([]byte(`{"EffectAttemptID":"effect-1","Invocation":{"ThreadID":"thread","TurnID":"turn","RunID":"run","ToolCallID":"call","ToolName":"write","ArgumentHash":"hash"},"RequestFingerprint":"request","State":"unknown","OwnerID":"owner","Generation":3}`), &attempt); err != nil {
		t.Fatal(err)
	}
	if attempt.EffectAttemptID != "effect-1" || attempt.Invocation.ToolCallID != "call" || attempt.OwnerID != "owner" || attempt.Generation != 3 {
		t.Fatalf("decoded published v3 attempt=%#v", attempt)
	}
}

func TestFinishTurnAllowsSettledRetryChild(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 1, 2, 3, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	accepted, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "turn", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "write"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := EffectAttemptRepo(repo)
	sourceInvocation := EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "write", ArgumentHash: StableHash(`{"value":"x"}`)}
	source, err := authority.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{Invocation: sourceInvocation, RequestFingerprint: "effect", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{EffectAttemptID: source.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof", Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.MarkEffectUnknown(ctx, MarkEffectUnknownRequest{EffectAttemptID: source.Attempt.EffectAttemptID, RequestFingerprint: "effect", OutcomeFingerprint: "unknown", Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ClaimEffectRetry(ctx, ClaimEffectRetryRequest{EffectAttemptID: source.Attempt.EffectAttemptID, ToolCallID: "call", RequestKey: "retry", RequestFingerprint: "retry-fingerprint", Now: now}); err != nil {
		t.Fatal(err)
	}
	childInvocation := sourceInvocation
	childInvocation.SourceEffectAttemptID = source.Attempt.EffectAttemptID
	child, err := authority.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{Invocation: childInvocation, RequestFingerprint: "retry-effect", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{EffectAttemptID: child.Attempt.EffectAttemptID, RequestFingerprint: "retry-effect", AuthorizationProofHash: "proof", Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.FinishEffectDispatch(ctx, FinishEffectDispatchRequest{
		EffectAttemptID: child.Attempt.EffectAttemptID, RequestFingerprint: "retry-effect", OutcomeFingerprint: "done", Now: now,
		Result: Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "write", ToolResult: &session.ToolResultView{Status: "success"}}},
	}); err != nil {
		t.Fatal(err)
	}
	finished, err := repo.FinishTurn(ctx, FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted, OutcomeFingerprint: "terminal", Now: now})
	if err != nil {
		t.Fatalf("FinishTurn rejected settled retry child: %v", err)
	}
	if finished.Terminal.ID != "terminal" || accepted.TurnStarted.ID == "" {
		t.Fatalf("finish=%#v accepted=%#v", finished, accepted)
	}
}

func TestClaimEffectRetryConsumesUnknownSourceExactlyOnce(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 1, 2, 3, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "turn", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "write"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	invocation := EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "write", ArgumentHash: StableHash(`{"path":"x"}`)}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{Invocation: invocation, RequestFingerprint: "effect", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof", Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkEffectUnknown(ctx, MarkEffectUnknownRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", OutcomeFingerprint: "unknown", Now: now}); err != nil {
		t.Fatal(err)
	}
	requests := []ClaimEffectRetryRequest{
		{EffectAttemptID: prepared.Attempt.EffectAttemptID, ToolCallID: "call", RequestKey: "retry-a", RequestFingerprint: "retry-a-fingerprint", Now: now.Add(time.Second)},
		{EffectAttemptID: prepared.Attempt.EffectAttemptID, ToolCallID: "call", RequestKey: "retry-b", RequestFingerprint: "retry-b-fingerprint", Now: now.Add(time.Second)},
	}
	results := make([]ClaimEffectRetryResult, 2)
	errs := make([]error, 2)
	var wait sync.WaitGroup
	for index := range requests {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			results[index], errs[index] = repo.ClaimEffectRetry(ctx, requests[index])
		}(index)
	}
	wait.Wait()
	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("claim error=%v, want conflict for loser", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims=%d, want exactly one", successes)
	}
	winner := requests[0]
	for index := range errs {
		if errs[index] == nil {
			winner = requests[index]
		}
	}
	replayed, err := repo.ClaimEffectRetry(ctx, winner)
	if err != nil || !replayed.Replayed || replayed.Attempt.State != EffectAttemptRetrying {
		t.Fatalf("same claim replay=%#v err=%v", replayed, err)
	}
	if _, err := repo.EffectAttempt(ctx, "thread", prepared.Attempt.EffectAttemptID); err != nil {
		t.Fatal(err)
	}
}
