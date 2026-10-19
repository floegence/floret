package event

import (
	"github.com/floegence/floret/v7/tools"
	"strings"
	"testing"
)

func TestGoalActivitySanitizationKeepsFullDetachedObjective(t *testing.T) {
	text := strings.Repeat("Original goal. ", 1000)
	original := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Goal: &tools.GoalActivity{Operation: "review", Version: 1, Text: text, Outcome: "continue", Evidence: "Needs a decision"}}}
	clean := NormalizeActivityPresentation(original)
	if clean == nil {
		t.Fatal("lost Goal activity")
	}
	goal := clean.Payload.(tools.StructuredActivityPayload).Goal
	if goal.Text != text {
		t.Fatal("objective truncated")
	}
	goal.Text = "mutated"
	if original.Payload.(tools.StructuredActivityPayload).Goal.Text != text {
		t.Fatal("sanitizer shares Goal")
	}
}
