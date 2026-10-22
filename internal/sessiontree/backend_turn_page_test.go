package sessiontree

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func TestV32CanonicalTurnPagesMatchMemoryAndRetry(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for index := range 8 {
		turn := fmt.Sprintf("turn-%d", index)
		request := AcceptTurnRequest{ThreadID: "thread", TurnID: turn, RunID: turn + "-run", LogicalRequestID: turn, RequestFingerprint: turn, InputRequestFingerprint: turn, Input: session.Message{Role: session.User, Content: "input " + turn}}
		if index == 7 {
			request.Input = session.Message{}
			request.InputRequestFingerprint = ""
			request.RetrySourceTurnID = "turn-6"
			request.RetrySourceEntryID = "user:turn-6"
		}
		if _, err := memory.AcceptTurn(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: turn, RunID: request.RunID, TerminalEntryID: turn + "-done", OutcomeFingerprint: turn, Status: TurnCompleted}); err != nil {
			t.Fatal(err)
		}
	}
	_, repo := openV31ReviewFixture(t, memory)
	for _, options := range []ListCanonicalTurnsOptions{
		{ThreadID: "thread", Tail: 3},
		{ThreadID: "thread", BeforeCursor: &CanonicalTurnBeforeCursor{EntryID: "turn-started:turn-5"}, Limit: 2},
		{ThreadID: "thread", SinceCursor: &CanonicalTurnSinceCursor{EntryID: "turn-2-done"}, Limit: 2},
		{ThreadID: "thread", SinceCursor: &CanonicalTurnSinceCursor{EntryID: "user:turn-6"}, Limit: 3},
	} {
		want, err := memory.ListCanonicalTurns(t.Context(), options)
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.ListCanonicalTurns(t.Context(), options)
		if err != nil {
			t.Fatal(err)
		}
		wantJSON, _ := json.Marshal(want)
		gotJSON, _ := json.Marshal(got)
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("page mismatch for %+v\nwant %s\ngot %s", options, wantJSON, gotJSON)
		}
	}
	for _, turn := range []string{"turn-1", "turn-7"} {
		want, err := memory.ReadCanonicalTurn(t.Context(), "thread", turn)
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.ReadCanonicalTurn(t.Context(), "thread", turn)
		if err != nil {
			t.Fatal(err)
		}
		wantJSON, _ := json.Marshal(want)
		gotJSON, _ := json.Marshal(got)
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("read mismatch for %s", turn)
		}
	}
	if repo.threadMemory != nil {
		t.Fatal("turn pages loaded a MemoryRepo")
	}
}
