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
	previous := BuildCheckpointMessage("S1 old summary", []session.Message{{Role: session.User, Content: "original user preserved in checkpoint", EntryID: "e1"}}, nil)
	previous.CompactionID = "c1"
	history := []session.Message{
		previous,
		{Role: session.User, Content: "new work", EntryID: "e2"},
		{Role: session.Assistant, Content: "new decision", EntryID: "e3"},
		{Role: session.User, Content: "tail", EntryID: "e4"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 1200, ReservedOutputTokens: 80, ReservedSummaryTokens: 120, RecentTailTokens: 12},
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

func TestPrepareKeepsLatestUserAndRecentUsersWithinBudget(t *testing.T) {
	large := strings.Repeat("x", int(contextpolicy.DefaultRecentUserTokens*4))
	history := []session.Message{
		{Role: session.User, Content: "oldest", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: large, EntryID: "u2"},
		{Role: session.Assistant, Content: "a2", EntryID: "a2"},
		{Role: session.User, Content: "latest", EntryID: "u3"},
		{Role: session.Assistant, Content: "tail", EntryID: "a3"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 8},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u3"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
	if len(prep.ActiveMessages) == 0 || prep.ActiveMessages[0].Role != session.User || prep.ActiveMessages[0].Kind != session.MessageKindCompactionSummary {
		t.Fatalf("active context should start with user checkpoint: %#v", prep.ActiveMessages)
	}
	if !strings.Contains(prep.ActiveMessages[0].Content, `"entry_id": "u3"`) || !strings.Contains(prep.ActiveMessages[0].Content, "latest") {
		t.Fatalf("latest user should be preserved inside checkpoint: %#v", prep.ActiveMessages[0])
	}
	if countEntryID(prep.ActiveMessages, "u3") != 0 {
		t.Fatalf("tail-external kept user should not appear as standalone message: %#v", prep.ActiveMessages)
	}
}

func TestPrepareKeepsRecentUsersInOrderWithinBudget(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "u1", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: "u2", EntryID: "u2"},
		{Role: session.Assistant, Content: "a2", EntryID: "a2"},
		{Role: session.User, Content: "u3", EntryID: "u3"},
		{Role: session.Assistant, Content: "tail", EntryID: "a3"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 8},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u1,u2,u3"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
}

func TestPrepareKeepsOversizedLatestUserAsFloor(t *testing.T) {
	large := strings.Repeat("x", int(contextpolicy.DefaultRecentUserTokens*4))
	history := []session.Message{
		{Role: session.User, Content: "old", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: large, EntryID: "u2"},
		{Role: session.Assistant, Content: "tail", EntryID: "a2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 8},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u2"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
}

func TestPrepareDeduplicatesKeptUsersAlreadyInTail(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "old", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: "latest", EntryID: "u2"},
		{Role: session.Assistant, Content: "tail", EntryID: "a2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 20},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u1,u2"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
	if countEntryID(prep.ActiveMessages, "u2") != 1 {
		t.Fatalf("latest user should appear once with tail position winning: %#v", prep.ActiveMessages)
	}
	if strings.Contains(prep.ActiveMessages[0].Content, `"entry_id": "u2"`) {
		t.Fatalf("tail user should not be duplicated inside checkpoint: %#v", prep.ActiveMessages[0])
	}
	if !strings.Contains(prep.ActiveMessages[0].Content, `"entry_id": "u1"`) {
		t.Fatalf("tail-external user should be preserved inside checkpoint: %#v", prep.ActiveMessages[0])
	}
}

func TestPrepareExtractsPurePreviousSummaryFromCheckpoint(t *testing.T) {
	previous := BuildCheckpointMessage("S1 old summary", []session.Message{{Role: session.User, Content: "old user", EntryID: "u1"}}, nil)
	previous.CompactionID = "c1"
	history := []session.Message{
		previous,
		{Role: session.User, Content: "new work", EntryID: "u2"},
		{Role: session.Assistant, Content: "new decision", EntryID: "a2"},
		{Role: session.User, Content: "tail", EntryID: "u3"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 1200, ReservedOutputTokens: 80, ReservedSummaryTokens: 120, RecentTailTokens: 12},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.PreviousCompactionID != "c1" {
		t.Fatalf("previous compaction id should be carried forward: %#v", prep.Result)
	}
	if strings.Contains(prep.Result.Summary, "preserved_user_inputs") || !strings.Contains(prep.Result.Summary, "S1 old summary") {
		t.Fatalf("summary should merge pure previous summary without checkpoint wrapper: %q", prep.Result.Summary)
	}
}

func TestPrepareShrinksTailWithoutLeavingOrphanToolResult(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "old", EntryID: "u1"},
		{Role: session.Assistant, Content: "call", ToolCallID: "call-1", ToolName: "read", ToolArgs: "{}", EntryID: "tc1"},
		{Role: session.Tool, Content: strings.Repeat("result ", 80), ToolCallID: "call-1", ToolName: "read", EntryID: "tr1"},
		{Role: session.User, Content: "latest", EntryID: "u2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 500, ReservedOutputTokens: 80, ReservedSummaryTokens: 80, RecentTailTokens: 120, MicrocompactToolTokens: 1000},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.Details["tail_shrunk_before_summary"] != "true" {
		t.Fatalf("tail should shrink before checkpoint: %#v", prep.Result.Details)
	}
	for _, msg := range prep.ActiveMessages {
		if msg.Role == session.Tool && msg.ToolCallID == "call-1" && !hasAssistantCall(prep.ActiveMessages, "call-1") {
			t.Fatalf("active context retained orphan tool result: %#v", prep.ActiveMessages)
		}
	}
	if prep.Result.FirstKeptEntryID != "u2" {
		t.Fatalf("tail should restart at latest user after dropping orphaned tool result, got %q", prep.Result.FirstKeptEntryID)
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

func countEntryID(messages []session.Message, id string) int {
	var count int
	for _, msg := range messages {
		if msg.EntryID == id {
			count++
		}
	}
	return count
}
