package compaction

import (
	"context"
	"strings"
	"testing"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/session"
)

func TestPrepareProducesStableCutpointAndPreservesToolPair(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "old goal", EntryID: "e1"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "call-1", ToolName: "read", ToolArgs: "A", EntryID: "e2"},
		{Role: session.Tool, Content: strings.Repeat("large result ", 200), ToolCallID: "call-1", ToolName: "read", EntryID: "e3"},
		{Role: session.User, Content: "continue", EntryID: "e4"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 400, ReservedOutputTokens: 40, ReservedSummaryTokens: 40, RecentTailTokens: 30, MicrocompactToolTokens: 20},
		Trigger: TriggerPreRequest,
		Reason:  ReasonThreshold,
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.FirstKeptEntryID == "" || prep.Result.CompactedThroughEntryID == "" {
		t.Fatalf("result missing stable boundary ids: %#v", prep.Result)
	}
	for _, msg := range prep.ActiveMessages {
		if msg.Role == session.Tool && msg.ToolCallID == "call-1" {
			if !hasAssistantCall(prep.ActiveMessages, "call-1") {
				t.Fatalf("tool result retained without assistant tool call: %#v", prep.ActiveMessages)
			}
		}
		if msg.Kind == session.MessageKindMicrocompactMarker && !strings.Contains(msg.Content, "large tool result compacted") {
			t.Fatalf("microcompact marker lost recovery metadata: %#v", msg)
		}
	}
	if prep.Result.SummarySchemaVersion != SummarySchemaVersion || prep.Result.TokensBefore <= prep.Result.TokensAfterEstimate {
		t.Fatalf("summary/token contract not satisfied: %#v", prep.Result)
	}
}

func TestPrepareUpdatesPreviousSummaryWithoutStackingOldSummaryMessages(t *testing.T) {
	history := []session.Message{
		{Role: session.Assistant, Content: "S1 old summary", Kind: session.MessageKindCompactionSummary, CompactionID: "c1"},
		{Role: session.User, Content: "new work", EntryID: "e2"},
		{Role: session.Assistant, Content: "new decision", EntryID: "e3"},
		{Role: session.User, Content: "tail", EntryID: "e4"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200, ReservedOutputTokens: 20, ReservedSummaryTokens: 40, RecentTailTokens: 12},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, msg := range prep.ActiveMessages {
		if msg.Kind == session.MessageKindCompactionSummary {
			count++
		}
	}
	if count != 1 || prep.Result.PreviousCompactionID != "c1" || !strings.Contains(prep.Result.Summary, "Previous Summary") {
		t.Fatalf("previous summary should be updated into one replacement summary: %#v", prep)
	}
}

func hasAssistantCall(messages []session.Message, id string) bool {
	for _, msg := range messages {
		if msg.Role == session.Assistant && msg.ToolCallID == id {
			return true
		}
	}
	return false
}
