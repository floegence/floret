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
		{Role: session.User, Content: "original user kept before summary", EntryID: "e1"},
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
	if len(prep.ActiveMessages) == 0 || prep.ActiveMessages[0].EntryID != "u3" {
		t.Fatalf("latest user should be kept before summary when outside tail: %#v", prep.ActiveMessages)
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
	countLatest := 0
	for _, msg := range prep.ActiveMessages {
		if msg.EntryID == "u2" {
			countLatest++
		}
	}
	if countLatest != 1 {
		t.Fatalf("latest user should appear once with tail position winning: %#v", prep.ActiveMessages)
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
		Policy:  contextpolicy.Policy{ContextWindowTokens: 180, ReservedOutputTokens: 20, ReservedSummaryTokens: 20, RecentTailTokens: 120, MicrocompactToolTokens: 1000},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.Details["tail_shrunk_before_summary"] != "true" {
		t.Fatalf("tail should shrink before summary: %#v", prep.Result.Details)
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
