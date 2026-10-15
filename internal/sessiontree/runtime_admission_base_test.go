package sessiontree

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
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
