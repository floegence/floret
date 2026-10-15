package sessiontree

import (
	"encoding/json"
	"github.com/floegence/floret/v7/internal/session"
	"testing"
	"time"
)

func TestSteeringProjectionForkAndRetry(t *testing.T) {
	repo := newMemoryRepo(time.Now)
	_, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "start", RequestFingerprint: "start", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "original"}})
	if err != nil {
		t.Fatal(err)
	}
	request := Entry{ID: "guide", Type: EntrySteeringRequested, ThreadID: "thread", TurnID: "turn", RunID: "run", RequestKey: "guide", RequestFingerprint: "guide", Message: session.Message{Role: session.User, Content: "new direction"}}
	_, err = repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{request})
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := repo.Thread(t.Context(), "thread")
	path, _ := repo.Path(t.Context(), "thread", meta.LeafID)
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatal("pending steering entered model history")
	}
	payload, _ := json.Marshal(SteeringResolution{SteeringID: "guide"})
	_, err = repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{{ID: "apply", Type: EntrySteeringApplied, ThreadID: "thread", TurnID: "turn", RunID: "run", Payload: payload}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "done", OutcomeFingerprint: "done", Status: TurnCompleted})
	if err != nil {
		t.Fatal(err)
	}
	meta, _ = repo.Thread(t.Context(), "thread")
	path, _ = repo.Path(t.Context(), "thread", meta.LeafID)
	history, err = BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[1].Content != "new direction" {
		t.Fatalf("history=%#v", history)
	}
	fork, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	forkPath, _ := repo.Path(t.Context(), fork.ID, fork.LeafID)
	history, err = BuildContextChecked(forkPath, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[1].Content != "new direction" {
		t.Fatalf("fork history=%#v", history)
	}
	prefix, _ := repo.Path(t.Context(), "thread", accepted.UserMessage.ID)
	retryPath := RetainAppliedSteering(prefix, path, "turn")
	history, err = BuildContextChecked(retryPath, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[1].Content != "new direction" {
		t.Fatal("retry dropped applied steering")
	}
}

func TestSteeringProjectionRejectsMalformedResolution(t *testing.T) {
	request := Entry{ID: "guide", Type: EntrySteeringRequested, TurnID: "turn", RunID: "run", RequestKey: "guide", RequestFingerprint: "guide", Message: session.Message{Role: session.User, Content: "instruction"}}
	for _, payload := range []string{`{"steering_id":"missing"}`, `{"steering_id":"guide","unknown":true}`, `{"steering_id":"guide"} {}`, `{"steering_id":"guide","reason":"stopped"}`} {
		_, err := projectSteering([]Entry{request, {Type: EntrySteeringApplied, TurnID: "turn", RunID: "run", Payload: json.RawMessage(payload)}})
		if err == nil {
			t.Fatalf("accepted malformed resolution: %s", payload)
		}
	}
}
