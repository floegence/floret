package sessiontree

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestPromotedAdmissionBaseAgreesAcrossEntryPoints(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	facts, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{{ID: "queued", ThreadID: "thread", Type: EntryQueueAdded, Payload: json.RawMessage(`{"id":"queue"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	req := AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send-fp", InputRequestFingerprint: "input-fp", Input: session.Message{Role: session.User, Content: "hello"}, PromotedQueueID: "queue", PromotionRequestKey: "promote", PromotionRequestFingerprint: "promote-fp"}
	accepted, err := repo.AcceptTurn(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := repo.AcceptTurn(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	read, found, err := repo.ReadAcceptedTurn(t.Context(), "thread", "turn", "run")
	if err != nil || !found {
		t.Fatalf("read=%v %v", found, err)
	}
	for _, result := range []AcceptTurnResult{accepted, replay, read} {
		if result.BaseLeafID != facts[0].ID {
			t.Fatalf("base=%q", result.BaseLeafID)
		}
	}
	for _, mutate := range []func(*Entry){func(e *Entry) { e.Payload = json.RawMessage(`"different"`) }, func(e *Entry) { e.ParentID = "missing" }, func(e *Entry) { e.ThreadID = "other" }} {
		original := repo.entries["thread"][1]
		broken := cloneEntry(original)
		mutate(&broken)
		broken.Raw = rawForEntry(broken)
		broken.RawHash = stableHash(broken.Raw)
		repo.entries["thread"][1] = broken
		if _, _, err = repo.ReadAcceptedTurn(t.Context(), "thread", "turn", "run"); err == nil {
			t.Fatal("accepted corrupt promotion")
		}
		repo.entries["thread"][1] = original
	}
	req.RetrySourceTurnID = "conflict"
	if _, err = repo.AcceptTurn(t.Context(), req); err == nil {
		t.Fatal("accepted conflicting replay identity")
	}
}

func TestRetryTerminalRetainsUncertainResultWithoutSavepoint(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "uncertain-thread"}); err != nil {
			t.Fatal(err)
		}
		original := AcceptTurnRequest{ThreadID: "uncertain-thread", TurnID: "original", RunID: "original-run", LogicalRequestID: "send", RequestFingerprint: "send-fp", InputRequestFingerprint: "input-fp", Input: session.Message{Role: session.User, Content: "perform once"}}
		if _, err := repo.AcceptTurn(t.Context(), original); err != nil {
			t.Fatal(err)
		}
		appendMessage := func(message session.Message) {
			t.Helper()
			if _, err := AppendRunMessage(t.Context(), repo, "uncertain-thread", "original", "original-run", message); err != nil {
				t.Fatal(err)
			}
		}
		appendMessage(session.Message{Role: session.Assistant, ToolCallID: "effect", ToolName: "click", ToolArgs: "{}"})
		if incomplete {
			appendMessage(session.Message{Role: session.Assistant, ToolCallID: "other", ToolName: "read", ToolArgs: "{}"})
		}
		appendMessage(session.Message{Role: session.Tool, ToolCallID: "effect", ToolName: "click", Content: "unconfirmed", ToolResult: &session.ToolResultView{Status: "error", EffectOutcome: tools.EffectOutcomeUncertain}})
		finished, err := repo.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "uncertain-thread", TurnID: "original", RunID: "original-run", TerminalEntryID: "terminal", OutcomeFingerprint: "finish-fp", Status: TurnFailed, FailureMessage: "savepoint failed", Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureStorage}})
		if err != nil {
			t.Fatal(err)
		}
		retry := AcceptTurnRequest{ThreadID: "uncertain-thread", TurnID: "retry", RunID: "retry-run", LogicalRequestID: "retry", RequestFingerprint: "retry-fp", RetrySourceTurnID: "original", RetrySourceEntryID: finished.Terminal.ID}
		admitted, err := repo.AcceptTurn(t.Context(), retry)
		if incomplete {
			if err == nil {
				t.Fatal("accepted incomplete exchange")
			}
			continue
		}
		if err != nil || admitted.BaseLeafID != "terminal" {
			t.Fatalf("admission=%+v err=%v", admitted, err)
		}
		if _, found, err := repo.ReadAcceptedTurn(t.Context(), "uncertain-thread", "retry", "retry-run"); err != nil || !found {
			t.Fatalf("readback=%v %v", found, err)
		}
		source, err := repo.ReadCanonicalTurn(t.Context(), "uncertain-thread", "original")
		if err != nil {
			t.Fatal(err)
		}
		entries := make([]Entry, len(source.Turn.Entries))
		for i, item := range source.Turn.Entries {
			entries[i] = item.Entry
		}
		if _, _, _, _, err := ValidateCanonicalRetrySourceTurn(entries, "uncertain-thread", CanonicalTurnRetrySource{TurnID: "original", EntryID: "terminal"}); err != nil {
			t.Fatal(err)
		}
	}
}
