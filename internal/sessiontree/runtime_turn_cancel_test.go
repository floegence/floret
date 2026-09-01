package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func TestCancelTurnAndEffectCompletionSettleOneTerminal(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		t.Run(fmt.Sprintf("iteration-%02d", iteration), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 8, 23, 9, 0, 0, iteration, time.UTC)
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send",
				RequestFingerprint: "send", InputRequestFingerprint: "input",
				Input: session.Message{Role: session.User, Content: "run"}, Now: now,
			}); err != nil {
				t.Fatal(err)
			}
			call := Entry{
				ID: "tool-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall,
				Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "shell", ToolArgs: `{}`},
			}
			if _, err := repo.AppendRuntimeFacts(ctx, "thread", []Entry{call}); err != nil {
				t.Fatal(err)
			}
			prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
				Invocation:         EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "shell", ArgumentHash: StableHash(`{}`)},
				RequestFingerprint: "effect", Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof", Now: now}); err != nil {
				t.Fatal(err)
			}

			start := make(chan struct{})
			var cancelErr, finishErr error
			var wait sync.WaitGroup
			wait.Add(2)
			go func() {
				defer wait.Done()
				<-start
				_, cancelErr = repo.CancelTurn(ctx, testCancelTurnRequest(now))
			}()
			go func() {
				defer wait.Done()
				<-start
				_, finishErr = repo.FinishEffectDispatch(ctx, FinishEffectDispatchRequest{
					EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", OutcomeFingerprint: "done", Now: now,
					Result: Entry{
						ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult,
						Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "shell", ToolResult: &session.ToolResultView{Status: "success"}},
					},
				})
			}()
			close(start)
			wait.Wait()
			if cancelErr != nil {
				t.Fatalf("CancelTurn race error=%v", cancelErr)
			}
			if finishErr != nil && !errors.Is(finishErr, ErrStaleAuthority) {
				t.Fatalf("FinishEffectDispatch race error=%v", finishErr)
			}
			if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
				ThreadID: "thread", TurnID: "next", RunID: "next-run", LogicalRequestID: "next",
				RequestFingerprint: "next", InputRequestFingerprint: "next-input",
				Input: session.Message{Role: session.User, Content: "continue"}, Now: now.Add(time.Second),
			}); err != nil {
				t.Fatalf("next turn after race: %v", err)
			}
		})
	}
}

func TestCancelTurnFailsWhenEffectOutcomeIsUnknown(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send",
		RequestFingerprint: "send-fingerprint", InputRequestFingerprint: "input-fingerprint",
		Input: session.Message{Role: session.User, Content: "run"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	toolCall := Entry{
		ID: "tool-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "shell", ToolArgs: `{"command":"sleep 20"}`},
	}
	interaction := Entry{
		ID: "interaction-requested:approval", ThreadID: "thread", TurnID: "turn", RunID: "run",
		Type: EntryInteractionAsked, RequestKey: "approval", RequestFingerprint: "approval-fingerprint", Payload: json.RawMessage(`{"id":"approval"}`),
	}
	if _, err := repo.AppendRuntimeFacts(ctx, "thread", []Entry{toolCall, interaction}); err != nil {
		t.Fatal(err)
	}
	invocation := EffectInvocationIdentity{
		ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "shell",
		ArgumentHash: StableHash(toolCall.Message.ToolArgs),
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{Invocation: invocation, RequestFingerprint: "effect", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{
		EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof", Now: now,
	}); err != nil {
		t.Fatal(err)
	}

	request := testCancelTurnRequest(now)
	cancelled, err := repo.CancelTurn(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Terminal.TurnStatus != TurnFailed ||
		cancelled.Terminal.Metadata[TurnFailureCodeMetadataKey] != TurnFailureEffectOutcomeUnknown ||
		len(cancelled.InteractionResolutions) != 1 || len(cancelled.ToolResults) != 1 {
		t.Fatalf("cancel result=%#v", cancelled)
	}
	if got := cancelled.ToolResults[0].Message.ToolResult.Status; got != "error" {
		t.Fatalf("tool status=%q, want error", got)
	}
	attempt, err := repo.EffectAttempt(ctx, "thread", prepared.Attempt.EffectAttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.State != EffectAttemptUnknown {
		t.Fatalf("effect state=%q, want unknown fact preserved", attempt.State)
	}
	if _, err := repo.FinishEffectDispatch(ctx, FinishEffectDispatchRequest{
		EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: "effect", OutcomeFingerprint: "late", Now: now,
		Result: Entry{
			ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult,
			Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "shell", ToolResult: &session.ToolResultView{Status: "success"}},
		},
	}); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("late effect result error=%v, want stale authority", err)
	}
	if _, err := repo.AppendRuntimeFacts(ctx, "thread", []Entry{{
		ID: "late-save", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnSavePoint,
	}}); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("late write error=%v, want stale authority", err)
	}
	replayed, err := repo.CancelTurn(ctx, request)
	if err != nil || !replayed.Replayed {
		t.Fatalf("cancel replay=%#v err=%v", replayed, err)
	}
	canonical, found, err := repo.CanonicalTurnEntries(ctx, "thread", "turn", "run")
	if err != nil || !found || len(canonical) == 0 {
		t.Fatalf("canonical failed turn found=%v entries=%d err=%v", found, len(canonical), err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn-2", RunID: "run-2", LogicalRequestID: "send-2",
		RequestFingerprint: "send-2", InputRequestFingerprint: "input-2",
		Input: session.Message{Role: session.User, Content: "continue"}, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("next turn after cancel: %v", err)
	}
}

func TestCancelTurnRollsBackPartialSettlement(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send",
		RequestFingerprint: "send", InputRequestFingerprint: "input",
		Input: session.Message{Role: session.User, Content: "run"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	toolCall := Entry{
		ID: "tool-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "shell", ToolArgs: `{}`},
	}
	conflict := Entry{ID: "tool-effect-unknown:turn:call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryCustom}
	if _, err := repo.AppendRuntimeFacts(ctx, "thread", []Entry{toolCall, conflict}); err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
		Invocation:         EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "shell", ArgumentHash: StableHash(`{}`)},
		RequestFingerprint: "effect", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof", Now: now}); err != nil {
		t.Fatal(err)
	}
	before, _ := repo.Entries(ctx, "thread")
	if _, err := repo.CancelTurn(ctx, testCancelTurnRequest(now)); err == nil {
		t.Fatal("CancelTurn succeeded despite conflicting tool closure")
	}
	after, _ := repo.Entries(ctx, "thread")
	if len(after) != len(before) {
		t.Fatalf("entries after rollback=%d, want %d", len(after), len(before))
	}
	attempt, err := repo.EffectAttempt(ctx, "thread", prepared.Attempt.EffectAttemptID)
	if err != nil || attempt.State != EffectAttemptDispatching {
		t.Fatalf("effect after rollback=%#v err=%v", attempt, err)
	}
}

func testCancelTurnRequest(now time.Time) CancelTurnRequest {
	resolution := json.RawMessage(`{"accepted":false,"outcome":"cancelled"}`)
	return CancelTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", CancelEntryID: "cancel", TerminalEntryID: "terminal",
		RequestKey: "stop", RequestFingerprint: "stop-fingerprint", OutcomeFingerprint: "cancelled-fingerprint",
		InteractionResolutionPayload: resolution,
		Metadata:                     map[string]string{TurnFailureCodeMetadataKey: TurnFailureCancelled}, ClearProviderState: true, Now: now,
	}
}
