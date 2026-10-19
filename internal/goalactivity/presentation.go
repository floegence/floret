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
		threadID := ""
		if len(path) > 0 {
			threadID = path[len(path)-1].ThreadID
		}
		id := "goal-review:" + sessiontree.StableHash(threadID + "\x00" + runID + "\x00" + callID)[:32]
		for _, entry := range path {
			if entry.ID != id || entry.Type != sessiontree.EntryGoalReview || entry.TurnID != turnID || entry.RunID != runID {
				continue
			}
			var review sessiontree.GoalReview
			if json.Unmarshal(entry.Payload, &review) == nil && review.Version == input.Version {
				goal.Outcome = review.Outcome
				goal.Evidence = review.Evidence
			}
		}
	}
	payload := tools.StructuredActivityPayload{Status: status, Operation: input.Operation, Goal: goal}
	if strings.TrimSpace(failure) != "" {
		payload.Error = &tools.ActivityError{Message: failure}
	}
	return event.Sanitize(event.Event{Activity: &tools.ActivityPresentation{Label: "Goal", Renderer: tools.ActivityRendererStructured, Payload: payload}}).Activity
}
