package compaction

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"testing"
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
