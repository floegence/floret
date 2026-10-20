// Package goalactivity projects canonical Goal facts into tool display data.
package goalactivity

import (
	"encoding/json"
	"strings"

	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

type invocationIdentity struct{ turn, run, call string }
type mutationIdentity struct{ turn, run, key string }
type reviewIdentity struct {
	version           int64
	outcome, evidence string
}

// Projector observes canonical Goal facts once. It never infers confirmation
// from arguments, activity status or another invocation.
type Projector struct {
	versions  map[int64]string
	mutations map[mutationIdentity]bool
	reviews   map[invocationIdentity]map[reviewIdentity]sessiontree.GoalReview
	open      map[invocationIdentity]bool
	closed    map[invocationIdentity]bool
}

func (projector *Projector) Observe(entry sessiontree.Entry) {
	switch entry.Type {
	case sessiontree.EntryGoal:
		var change sessiontree.GoalChange
		if json.Unmarshal(entry.Payload, &change) != nil {
			return
		}
		if projector.versions == nil {
			projector.versions = map[int64]string{}
			projector.mutations = map[mutationIdentity]bool{}
		}
		projector.versions[change.State.Version] = change.State.Text
		projector.mutations[mutationIdentity{entry.TurnID, entry.RunID, entry.RequestKey}] = true
	case sessiontree.EntryToolCall:
		if entry.Message.ToolName != "goal" {
			return
		}
		if projector.open == nil {
			projector.open = map[invocationIdentity]bool{}
			projector.reviews = map[invocationIdentity]map[reviewIdentity]sessiontree.GoalReview{}
			projector.closed = map[invocationIdentity]bool{}
		}
		key := invocationIdentity{entry.TurnID, entry.RunID, entry.Message.ToolCallID}
		if !projector.closed[key] {
			projector.open[key] = true
		}
	case sessiontree.EntryToolResult:
		key := invocationIdentity{entry.TurnID, entry.RunID, entry.Message.ToolCallID}
		if projector.open[key] {
			projector.closed[key] = true
			delete(projector.open, key)
		}
	case sessiontree.EntryGoalReview:
		var review sessiontree.GoalReview
		if json.Unmarshal(entry.Payload, &review) != nil {
			return
		}
		for key := range projector.open {
			if key.turn != entry.TurnID || key.run != entry.RunID {
				continue
			}
			if projector.reviews[key] == nil {
				projector.reviews[key] = map[reviewIdentity]sessiontree.GoalReview{}
			}
			projector.reviews[key][reviewIdentity{review.Version, review.Outcome, review.Evidence}] = review
		}
	}
}

// Presentation uses the invocation's confirmed canonical facts, never a later
// objective or an outcome inferred from tool success.
func (projector *Projector) Presentation(turnID, runID, callID, args, status, failure string) *tools.ActivityPresentation {
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
	goal := &tools.GoalActivity{Operation: input.Operation, Version: input.Version, Text: projector.versions[input.Version]}
	if input.Operation == "set" {
		goal.Text = input.Text
	}
	if status == "success" && input.Operation != "review" {
		goal.Applied = projector.mutations[mutationIdentity{turnID, runID, "model:" + runID + ":" + callID}]
	}
	if status == "success" && input.Operation == "review" {
		if review, found := projector.reviews[invocationIdentity{turnID, runID, callID}][reviewIdentity{input.Version, input.Outcome, input.Evidence}]; found {
			goal.Outcome, goal.Evidence = review.Outcome, review.Evidence
		}
	}
	payload := tools.StructuredActivityPayload{Status: status, Operation: input.Operation, Goal: goal}
	if strings.TrimSpace(failure) != "" {
		payload.Error = &tools.ActivityError{Message: failure}
	}
	return event.Sanitize(event.Event{Activity: &tools.ActivityPresentation{Label: "Goal", Renderer: tools.ActivityRendererStructured, Payload: payload}}).Activity
}

// Presentation is the explicit full-path adapter used by harness callers.
// Streaming presentation advances the same Projector.
func Presentation(path []sessiontree.Entry, turnID, runID, callID, args, status, failure string) *tools.ActivityPresentation {
	projector := Projector{}
	for _, entry := range path {
		projector.Observe(entry)
	}
	return projector.Presentation(turnID, runID, callID, args, status, failure)
}
