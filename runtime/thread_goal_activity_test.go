package runtime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestGoalActivityRestoresHistoricalReviewWithoutChangingFacts(t *testing.T) {
	for _, outcome := range []string{"continue", "blocked", "completed"} {
		t.Run(outcome, func(t *testing.T) {
			text := strings.Repeat("Original goal. ", 1000)
			change, _ := json.Marshal(sessiontree.GoalChange{State: session.GoalState{Version: 1, Text: text, Status: "active", Actor: "user"}, Operation: "set"})
			args, _ := json.Marshal(map[string]any{"operation": "review", "expected_version": 1, "outcome": outcome, "evidence": "Waiting for the user's decision."})
			review, _ := json.Marshal(sessiontree.GoalReview{Version: 1, Outcome: outcome, Evidence: "Waiting for the user's decision."})
			entries := []sessiontree.Entry{
				{ID: "goal", Type: sessiontree.EntryGoal, Payload: change},
				{ID: "call", Type: sessiontree.EntryToolCall, Message: session.Message{ToolName: "goal", ToolCallID: "review", ToolArgs: string(args)}},
				{ID: "goal-review:" + sessiontree.StableHash("thread\x00run\x00review")[:32], Type: sessiontree.EntryGoalReview, Payload: review},
				{ID: "result", Type: sessiontree.EntryToolResult, Message: session.Message{ToolName: "goal", ToolCallID: "review", Content: "Goal review recorded.", ToolResult: &session.ToolResultView{Status: "success"}}},
			}
			for i := range entries {
				entries[i].ThreadID = "thread"
				entries[i].TurnID = "turn"
				entries[i].RunID = "run"
				entries[i].CreatedAt = time.Now()
			}
			before, _ := json.Marshal(entries)
			items, _, err := threadRuntimeItemsFromEntries(entries)
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(items)
			var rows []map[string]any
			if err := json.Unmarshal(encoded, &rows); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range rows {
				if row["kind"] != "tool" {
					continue
				}
				activity := row["activity"].(map[string]any)
				payload := activity["presentation"].(map[string]any)["payload"].(map[string]any)
				goal, ok := payload["goal"].(map[string]any)
				if !ok {
					t.Fatal("missing goal presentation")
				}
				if goal["text"] != text || goal["outcome"] != outcome || goal["evidence"] != "Waiting for the user's decision." {
					t.Fatalf("incorrect goal presentation: %+v", goal)
				}
				found = true
			}
			if !found {
				t.Fatal("missing tool")
			}
			after, _ := json.Marshal(entries)
			if string(before) != string(after) {
				t.Fatal("read changed canonical facts")
			}
		})
	}
}
