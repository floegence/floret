package tools

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGoalActivityPreservesExactTextAndDetachedCopies(t *testing.T) {
	text := strings.Repeat("Long objective. ", 1000)
	activity := &ActivityPresentation{Renderer: ActivityRendererStructured, Payload: StructuredActivityPayload{Goal: &GoalActivity{Operation: "review", Version: 1, Text: text, Outcome: "continue", Evidence: "Needs a decision"}}}
	if err := activity.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(activity)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ActivityPresentation
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Payload.(StructuredActivityPayload).Goal.Text != text {
		t.Fatal("objective truncated")
	}
	clone := CloneActivityPresentation(activity)
	clone.Payload.(StructuredActivityPayload).Goal.Text = "other"
	if activity.Payload.(StructuredActivityPayload).Goal.Text != text {
		t.Fatal("clone shares goal")
	}
	merged := MergeActivityPresentations(activity, &ActivityPresentation{Renderer: ActivityRendererStructured, Payload: StructuredActivityPayload{Status: "success"}})
	if merged.Payload.(StructuredActivityPayload).Goal.Text != text {
		t.Fatal("status update lost goal")
	}
}
