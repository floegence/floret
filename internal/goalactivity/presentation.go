// Package goalactivity projects canonical Goal facts into tool display data.
package goalactivity

import (
	"encoding/json"
	"strings"

	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

// Presentation uses the invocation's journal position and confirmed review,
// never a later objective or an outcome inferred from tool success.
func Presentation(path []sessiontree.Entry, turnID, runID, callID, args, status, failure string) *tools.ActivityPresentation {
	var input struct {
		Operation string `json:"operation"`
		Version   int64  `json:"expected_version"`
		Text      string `json:"text"`
		Outcome   string `json:"outcome"`
		Evidence  string `json:"evidence"`
	}
	if json.Unmarshal([]byte(args), &input) != nil {
		return nil
	}
	if input.Operation != "review" && input.Operation != "set" && input.Operation != "delete" {
		return nil
	}
	goal := &tools.GoalActivity{Operation: input.Operation, Version: input.Version}
	for _, entry := range path {
		if entry.Type == sessiontree.EntryGoal {
			var change sessiontree.GoalChange
			if json.Unmarshal(entry.Payload, &change) == nil && change.State.Version == input.Version {
				goal.Text = change.State.Text
			}
		}
	}
	if input.Operation == "set" {
		goal.Text = input.Text
	}
	if status == "success" && input.Operation != "review" {
		for _, entry := range path {
			if entry.Type == sessiontree.EntryGoal && entry.TurnID == turnID && entry.RunID == runID && entry.RequestKey == "model:"+runID+":"+callID {
				goal.Applied = true
			}
		}
	}
	if status == "success" && input.Operation == "review" {
		// Forks re-key journal entries. Match the review in the confirmed
		// invocation span rather than deriving an entry ID from thread identity.
		invocationOpen := false
		var recorded *sessiontree.GoalReview
		for _, entry := range path {
			if entry.TurnID != turnID || entry.RunID != runID {
				continue
			}
			if entry.Type == sessiontree.EntryToolCall && entry.Message.ToolName == "goal" && entry.Message.ToolCallID == callID {
				invocationOpen = true
			}
			if invocationOpen && entry.Type == sessiontree.EntryGoalReview {
				var review sessiontree.GoalReview
				if json.Unmarshal(entry.Payload, &review) == nil && review.Version == input.Version && review.Outcome == input.Outcome && review.Evidence == input.Evidence {
					recorded = &review
				}
			}
			if invocationOpen && entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == callID {
				break
			}
		}
		if recorded != nil {
			goal.Outcome = recorded.Outcome
			goal.Evidence = recorded.Evidence
		}
	}
	payload := tools.StructuredActivityPayload{Status: status, Operation: input.Operation, Goal: goal}
	if strings.TrimSpace(failure) != "" {
		payload.Error = &tools.ActivityError{Message: failure}
	}
	return event.Sanitize(event.Event{Activity: &tools.ActivityPresentation{Label: "Goal", Renderer: tools.ActivityRendererStructured, Payload: payload}}).Activity
}
