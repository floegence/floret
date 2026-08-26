package agentharness

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v5/internal/engine"
	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/sessiontree"
)

func TestValidateCommittedEffectFinalizationConsumesCanonicalMessage(t *testing.T) {
	prepared := sessiontree.EffectAttempt{
		EffectAttemptID: "effect-1",
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: "thread-1", TurnID: "turn-1", RunID: "run-1", ToolCallID: "call-1", ToolName: "shell",
		},
	}
	request := engine.EffectResultFinalizationRequest{
		RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", ToolCallID: "call-1",
		Message: session.Message{Role: session.Tool, Content: "pre-commit projection", ToolCallID: "call-1", ToolName: "shell", ToolResult: &session.ToolResultView{Status: "success"}},
	}
	entry := sessiontree.Entry{
		ID: "result-1", ThreadID: "thread-1", TurnID: "turn-1", Type: sessiontree.EntryToolResult,
		Message:  session.Message{Role: session.Tool, Content: "canonical result", ToolCallID: "call-1", ToolName: "shell", ToolResult: &session.ToolResultView{Status: "success"}},
		Metadata: map[string]string{sessiontree.PendingToolEffectAttemptIDKey: "effect-1"},
	}
	entry.Raw = sessiontree.RawForEntry(entry)
	entry.RawHash = sessiontree.StableHash(entry.Raw)
	finishedAttempt := prepared
	finishedAttempt.ResultEntryID = entry.ID
	finishedAttempt.State = sessiontree.EffectAttemptCompleted

	got, err := validateCommittedEffectFinalization(request, prepared, sessiontree.FinishEffectDispatchResult{Attempt: finishedAttempt, Result: entry})
	if err != nil {
		t.Fatal(err)
	}
	if got.Message.Content != "canonical result" || got.CanonicalEntryID != entry.ID {
		t.Fatalf("finalization=%#v, want canonical entry", got)
	}

	corrupt := entry
	corrupt.Message.ToolCallID = "other-call"
	if _, err := validateCommittedEffectFinalization(request, prepared, sessiontree.FinishEffectDispatchResult{Attempt: finishedAttempt, Result: corrupt}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("identity error=%v, want ErrAuthorityCorrupt", err)
	}
}
