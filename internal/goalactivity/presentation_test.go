package goalactivity

import (
	"encoding/json"
	"github.com/floegence/floret/v7/internal/session"
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
	args := `{"operation":"review","expected_version":1,"outcome":"continue","evidence":"Waiting for a decision"}`
	goal := Presentation(path, "turn", "run", "call", args, "success", "").Payload.(tools.StructuredActivityPayload).Goal
	if goal.Outcome != "" {
		t.Fatal("borrowed unrelated review")
	}
}

func TestHistoricalForkReviewUsesInvocationSpan(t *testing.T) {
	review, _ := json.Marshal(sessiontree.GoalReview{Version: 1, Outcome: "continue", Evidence: "Waiting for a decision"})
	path := []sessiontree.Entry{
		{ID: "fork-call", ThreadID: "fork", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{ToolName: "goal", ToolCallID: "call"}},
		{ID: "fork-review", ThreadID: "fork", TurnID: "turn", RunID: "run", Type: sessiontree.EntryGoalReview, Payload: review},
		{ID: "fork-result", ThreadID: "fork", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{ToolName: "goal", ToolCallID: "call"}},
	}
	args := `{"operation":"review","expected_version":1,"outcome":"continue","evidence":"Waiting for a decision"}`
	goal := Presentation(path, "turn", "run", "call", args, "success", "").Payload.(tools.StructuredActivityPayload).Goal
	if goal.Outcome != "continue" || goal.Evidence != "Waiting for a decision" {
		t.Fatal("fork lost confirmed review")
	}
	goal = Presentation(path[1:], "turn", "run", "call", args, "success", "").Payload.(tools.StructuredActivityPayload).Goal
	if goal.Outcome != "" {
		t.Fatal("unmatched invocation inherited review")
	}
}

func TestBatchedReviewsMatchExactConfirmedEvidence(t *testing.T) {
	makeReview := func(outcome, evidence string) []byte {
		raw, _ := json.Marshal(sessiontree.GoalReview{Version: 1, Outcome: outcome, Evidence: evidence})
		return raw
	}
	path := []sessiontree.Entry{
		{Type: sessiontree.EntryToolCall, Message: session.Message{ToolName: "goal", ToolCallID: "first"}},
		{Type: sessiontree.EntryToolCall, Message: session.Message{ToolName: "goal", ToolCallID: "second"}},
		{Type: sessiontree.EntryGoalReview, Payload: makeReview("blocked", "Needs access")},
		{Type: sessiontree.EntryGoalReview, Payload: makeReview("continue", "Needs verification")},
		{Type: sessiontree.EntryToolResult, Message: session.Message{ToolName: "goal", ToolCallID: "first"}},
		{Type: sessiontree.EntryToolResult, Message: session.Message{ToolName: "goal", ToolCallID: "second"}},
	}
	for i := range path {
		path[i].TurnID = "turn"
		path[i].RunID = "run"
	}
	for _, test := range []struct{ call, outcome, evidence string }{{"first", "continue", "Needs verification"}, {"second", "blocked", "Needs access"}} {
		args, _ := json.Marshal(map[string]any{"operation": "review", "expected_version": 1, "outcome": test.outcome, "evidence": test.evidence})
		goal := Presentation(path, "turn", "run", test.call, string(args), "success", "").Payload.(tools.StructuredActivityPayload).Goal
		if goal.Outcome != test.outcome || goal.Evidence != test.evidence {
			t.Fatal("review inherited another invocation's result")
		}
	}
}
