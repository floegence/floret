package compaction

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

type minimalExchangeSummary struct{ calls int }

func (s *minimalExchangeSummary) GenerateSummary(context.Context, Preparation) (string, error) {
	s.calls++
	return "replacement summary", nil
}

func TestMinimalCompleteExchangeRecompaction(t *testing.T) {
	pair := []session.Message{
		{Role: session.Assistant, ToolCallID: "call", ToolName: "observe", Content: "tool_call", ToolArgs: "{}", EntryID: "call-entry"},
		{Role: session.Tool, ToolCallID: "call", ToolName: "observe", Content: "completed", EntryID: "result-entry"},
	}
	previous := BuildCheckpointMessage("previous summary", nil, pair)
	previous.CompactionID = "previous"
	previous.CompactionGeneration = 5
	previous.CompactionWindowID = "previous"
	previous.ModelContextSnapshot = &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "environment", Title: "Environment", Text: "test"}}}
	input := append([]session.Message{previous}, pair...)
	initial, _, err := session.ProjectProviderHistory(input, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = session.ValidateToolHistory(initial); err != nil {
		t.Fatal(err)
	}
	summary := &minimalExchangeSummary{}
	_, err = Prepare(t.Context(), Request{History: input, Trigger: TriggerOverflow, Reason: ReasonProviderOverflow, PreviousCompactionID: "previous", PreviousGeneration: 5, PreviousWindowID: "previous", PreviousSummary: "previous summary", Policy: contextpolicy.Policy{ContextWindowTokens: 950000}}, summary)
	if !errors.Is(err, ErrNoCutPoint) || summary.calls != 0 {
		t.Fatalf("want no cut before summary generation, got error=%v calls=%d", err, summary.calls)
	}
}

func TestMinimalVisualExchangeRecompactionPreservesWholePair(t *testing.T) {
	pair := []session.Message{
		{Role: session.Assistant, ToolCallID: "call", ToolName: "observe", ToolArgs: "{}", EntryID: "call-entry"},
		{Role: session.Tool, ToolCallID: "call", ToolName: "observe", Content: "completed", EntryID: "result-entry", Attachments: []session.MessageAttachment{{ResourceRef: "image:frame", Name: "frame", MIMEType: "image/png", SizeBytes: 6 << 20}}},
	}
	previous := BuildCheckpointMessage("previous summary", nil, pair)
	previous.CompactionID = "previous"
	previous.CompactionGeneration = 5
	previous.CompactionWindowID = "previous"
	summary := &minimalExchangeSummary{}
	prep, err := Prepare(t.Context(), Request{History: append([]session.Message{previous}, pair...), Trigger: TriggerOverflow, Reason: ReasonProviderOverflow, PreviousCompactionID: "previous", PreviousGeneration: 5, PreviousWindowID: "previous", PreviousSummary: "previous summary", Policy: contextpolicy.Policy{ContextWindowTokens: 950000}}, summary)
	if err != nil {
		t.Fatal(err)
	}
	if summary.calls != 1 || len(prep.CompactedHead) != 2 || len(prep.RetainedTail) != 0 || prep.Result.CompactedThroughEntryID != "result-entry" {
		t.Fatalf("incomplete visual exchange: %+v", prep)
	}
	projected, _, err := session.ProjectProviderHistory(prep.ActiveMessages, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = session.ValidateToolHistory(projected); err != nil {
		t.Fatal(err)
	}
}

func TestVisualExchangeWithSupplementalAnchorCannotCompact(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "protected", EntryID: "anchor"},
		{Role: session.Assistant, ToolCallID: "call", ToolName: "observe", ToolArgs: "{}", EntryID: "call"},
		{Role: session.Tool, ToolCallID: "call", ToolName: "observe", EntryID: "result", Attachments: []session.MessageAttachment{{ResourceRef: "image:frame", MIMEType: "image/png", SizeBytes: 1024}}},
	}
	summary := &minimalExchangeSummary{}
	_, err := Prepare(t.Context(), Request{History: history, Trigger: TriggerOverflow, SupplementalAnchorEntryID: "anchor", Policy: contextpolicy.Policy{ContextWindowTokens: 950000}}, summary)
	if !errors.Is(err, ErrNoCutPoint) || summary.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, summary.calls)
	}
}

func TestMediaRecoverySummarizesOlderUserImagesButKeepsCurrentImages(t *testing.T) {
	history := []session.Message{
		{Role: session.User, EntryID: "older", Content: "old frame", Attachments: []session.MessageAttachment{{ResourceRef: "old", MIMEType: "image/png", SizeBytes: 100}}},
		{Role: session.Assistant, EntryID: "answer", Content: "reviewed"},
		{Role: session.User, EntryID: "current", Content: "current frame", Attachments: []session.MessageAttachment{{ResourceRef: "current", MIMEType: "image/png", SizeBytes: 100}}},
	}
	for _, reason := range []Reason{"image_count", "image_bytes", "request_bytes"} {
		prep, err := Prepare(t.Context(), Request{History: history, Trigger: TriggerOverflow, Reason: reason, Policy: contextpolicy.Policy{ContextWindowTokens: 950000}}, &minimalExchangeSummary{})
		if err != nil {
			t.Fatal(err)
		}
		var refs []string
		for _, m := range prep.ActiveMessages {
			for _, a := range m.Attachments {
				refs = append(refs, a.ResourceRef)
			}
		}
		if len(refs) != 1 || refs[0] != "current" {
			t.Fatalf("retained media: %v", refs)
		}
		if len(prep.CompactedHead[0].Attachments) != 1 || prep.CompactedHead[0].Attachments[0].ResourceRef != "old" {
			t.Fatal("historical image omitted from summary")
		}
		if history[0].Attachments[0].ResourceRef != "old" {
			t.Fatal("mutated canonical source")
		}
	}
}
