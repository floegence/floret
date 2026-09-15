package compaction

import (
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

func TestOverflowRetainsLatestInteractionAndProtectedAnchors(t *testing.T) {
	frame := session.MessageAttachment{ResourceRef: "frame://latest", MIMEType: "image/png", SizeBytes: 4 * 1024 * 1024}
	history := []session.Message{
		{Role: session.User, Content: "Complete the visual task", EntryID: "user"},
		{Role: session.Assistant, ToolCallID: "old", ToolName: "observe", EntryID: "old-call"},
		{Role: session.Tool, ToolCallID: "old", Content: strings.Repeat("Earlier application context. ", 1500), EntryID: "old-result"},
		{Role: session.User, Content: "Continue observing", EntryID: "anchor"},
		{Role: session.Assistant, ToolCallID: "a", ToolName: "observe", EntryID: "a-call"},
		{Role: session.Assistant, ToolCallID: "b", ToolName: "observe", EntryID: "b-call"},
		{Role: session.Tool, ToolCallID: "a", Content: "Latest first result", EntryID: "a-result", ToolResult: &session.ToolResultView{Attachments: []session.MessageAttachment{frame}}},
		{Role: session.Tool, ToolCallID: "b", Content: "Latest second result", EntryID: "b-result", ToolResult: &session.ToolResultView{Attachments: []session.MessageAttachment{frame}}},
	}
	for _, tc := range []struct {
		name   string
		suffix []session.Message
		anchor string
		start  int
		noCut  bool
	}{
		{name: "complete_batch", start: 4},
		{name: "trailing_assistant", start: 4, suffix: []session.Message{{Role: session.Assistant, Content: "Now continue", EntryID: "response"}}},
		{name: "new_user", start: 8, suffix: []session.Message{{Role: session.User, Content: "Next task", EntryID: "next"}}},
		{name: "protected_observation", anchor: "anchor", start: 3},
		{name: "protected_initial_user", anchor: "user", noCut: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := append(session.CloneMessages(history), tc.suffix...)
			original := session.CloneMessages(input)
			prep, err := Prepare(t.Context(), Request{History: input, Trigger: TriggerOverflow, Reason: ReasonProviderOverflow, SupplementalAnchorEntryID: tc.anchor,
				Policy: contextpolicy.Policy{ContextWindowTokens: 950000}}, ExtractiveSummaryGenerator{})
			if tc.noCut {
				if err != ErrNoCutPoint {
					t.Fatalf("protected initial context must fail closed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(prep.RetainedTail, input[tc.start:]) {
				t.Fatalf("wrong retained interaction: start=%q want=%q", prep.Result.FirstKeptEntryID, input[tc.start].EntryID)
			}
			if !reflect.DeepEqual(input, original) {
				t.Fatal("compaction changed canonical history or image descriptors")
			}
			if prep.Result.Details["retained_tail_strategy"] != "latest_interaction" {
				t.Fatal("overflow tail decision is not observable")
			}
			if !strings.Contains(prep.ActiveMessages[0].Content, "Complete the visual task") {
				t.Fatal("lost original user goal")
			}
		})
	}
}
