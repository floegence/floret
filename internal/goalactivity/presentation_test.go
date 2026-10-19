package goalactivity

import (
	"encoding/json"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
	"testing"
)

func TestUnconfirmedReviewAndMutationsDoNotClaimSuccess(t *testing.T) {
	for _, status := range []string{"running", "success", "error", "canceled"} {
		activity := Presentation(nil, "turn", "run", "call", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Claimed"}`, status, "")
		goal := activity.Payload.(tools.StructuredActivityPayload).Goal
		if goal.Outcome != "" || goal.Evidence != "" {
			t.Fatal("unconfirmed arguments became review evidence")
		}
	}
	for _, op := range []string{"set", "delete"} {
		raw, _ := json.Marshal(map[string]any{"operation": op, "expected_version": 1, "text": "Requested"})
		goal := Presentation(nil, "turn", "run", "call", string(raw), "success", "").Payload.(tools.StructuredActivityPayload).Goal
		if goal.Applied {
			t.Fatal("unconfirmed mutation claimed success")
		}
	}
}

func TestReviewDoesNotBorrowAnotherExecutionOrVersion(t *testing.T) {
	raw, _ := json.Marshal(sessiontree.GoalReview{Version: 2, Outcome: "completed", Evidence: "Another version"})
	path := []sessiontree.Entry{{ID: "goal-review:" + sessiontree.StableHash("thread\x00run\x00call")[:32], ThreadID: "thread", TurnID: "other-turn", RunID: "run", Type: sessiontree.EntryGoalReview, Payload: raw}}
	args := `{"operation":"review","expected_version":1}`
	goal := Presentation(path, "turn", "run", "call", args, "success", "").Payload.(tools.StructuredActivityPayload).Goal
	if goal.Outcome != "" {
		t.Fatal("borrowed unrelated review")
	}
}
