package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v6/internal/provider"
	"github.com/floegence/floret/v6/internal/session"
)

func TestFailUnknownEffectTurnAtomicallyTerminatesEveryPendingEffect(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "turn", InputRequestFingerprint: "input",
		Input: session.Message{Role: session.User, Content: "run tools"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	for _, callID := range []string{"call-a", "call-b"} {
		if _, err := repo.AppendRuntimeFacts(ctx, "thread", []Entry{{
			ID: "tool-call:" + callID, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall,
			Message: session.Message{Role: session.Assistant, ToolCallID: callID, ToolName: "subagents", ToolArgs: `{}`},
		}}); err != nil {
			t.Fatal(err)
		}
		prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
			Invocation: EffectInvocationIdentity{
				ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: callID, ToolName: "subagents", ArgumentHash: StableHash(`{}`),
			},
			RequestFingerprint: "effect:" + callID, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{
			EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect:" + callID,
			AuthorizationProofHash: "proof", Now: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.PutProviderState(ctx, ProviderStateRecord{
		ThreadID: "thread", LeafEntryID: "provider-leaf", CompatibilityKey: "provider",
		State: provider.State{Kind: "opaque", ID: "state"}, CreatedByRunID: "run", CreatedByTurnID: "turn", UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	failed, err := repo.FailUnknownEffectTurn(ctx, FailUnknownEffectTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if failed.Terminal.TurnStatus != TurnFailed || failed.Terminal.Metadata[TurnFailureCodeMetadataKey] != TurnFailureEffectOutcomeUnknown {
		t.Fatalf("terminal=%#v", failed.Terminal)
	}
	if failed.Failure.Error != EffectOutcomeUnknownFailureMessage || len(failed.ToolResults) != 2 {
		t.Fatalf("failure=%#v tool_results=%d", failed.Failure, len(failed.ToolResults))
	}
	for _, result := range failed.ToolResults {
		if result.Message.ToolResult == nil || result.Message.ToolResult.Status != "error" {
			t.Fatalf("tool result=%#v", result.Message.ToolResult)
		}
	}
	if _, err := repo.ProviderState(ctx, "thread"); !errors.Is(err, ErrProviderStateNotFound) {
		t.Fatalf("provider state error=%v, want not found", err)
	}
	replayed, err := repo.FailUnknownEffectTurn(ctx, FailUnknownEffectTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Now: now.Add(2 * time.Second)})
	if err != nil || !replayed.Replayed {
		t.Fatalf("replay=%#v err=%v", replayed, err)
	}
	if _, err := repo.AppendRuntimeFacts(ctx, "thread", []Entry{{
		ID: "late", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnSavePoint,
	}}); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("late write error=%v, want stale authority", err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "next", RunID: "next-run", LogicalRequestID: "next",
		RequestFingerprint: "next", InputRequestFingerprint: "next-input",
		Input: session.Message{Role: session.User, Content: "continue"}, Now: now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("next turn: %v", err)
	}
}
