package runtime

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func TestCanonicalIntermediateResponsesAndOrdinals(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		boundary  sessiontree.Entry
		segmented bool
		want      int
	}{
		{"local", sessiontree.Entry{Type: sessiontree.EntryToolCall, Message: session.Message{ToolCallID: "call", ToolName: "inspect"}}, true, 2},
		{"hosted", sessiontree.Entry{Type: sessiontree.EntryCustom, ThreadID: "thread", Metadata: map[string]string{"kind": sessiontree.HostedToolEntryKind, "type": string(observation.EventTypeHostedToolCall)}, Message: session.Message{ToolCallID: "hosted", ToolName: "search"}}, true, 2},
		{"semantic", sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint, Metadata: map[string]string{"reason": "context_continue", "continuation_reason": "hook"}}, true, 2},
		{"truncated", sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint, Metadata: map[string]string{"continuation_reason": "provider_truncated"}}, true, 0},
		{"legacy", sessiontree.Entry{Type: sessiontree.EntryToolCall, Message: session.Message{ToolCallID: "call", ToolName: "inspect"}}, false, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			for _, status := range []sessiontree.TurnMarkerStatus{sessiontree.TurnFailed, sessiontree.TurnAborted, sessiontree.TurnCompleted} {
				start := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
				entries := []sessiontree.Entry{
					{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{}},
					{ID: "user", Type: sessiontree.EntryUserMessage, Message: session.Message{Content: "request"}},
					{ID: "prose-a", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Content: "first", Reasoning: "reason"}},
					{ID: "prose-b", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Content: "continued"}},
					scenario.boundary,
					{ID: "tail", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Content: "partial answer"}},
					{ID: "end", Type: sessiontree.EntryTurnMarker, TurnStatus: status},
				}
				if scenario.segmented {
					entries[0].Metadata[sessiontree.ResponseSegmentsMetadataKey] = "1"
				}
				for i := range entries {
					entries[i].TurnID = "turn"
					entries[i].RunID = "run"
					entries[i].CreatedAt = start.Add(-time.Duration(i) * time.Second)
				}
				before, _ := json.Marshal(entries)
				items, _, err := threadRuntimeItemsFromEntries(entries)
				if err != nil {
					t.Fatal(err)
				}
				turns, err := threadRuntimeTurnsFromEntries(entries, items)
				if err != nil {
					t.Fatal(err)
				}
				if len(turns) != 1 || len(turns[0].IntermediateItemIDs) != scenario.want {
					t.Fatalf("status=%s turns=%+v", status, turns)
				}
				for _, id := range turns[0].IntermediateItemIDs {
					for _, item := range items {
						if item.ID == id && item.Text == "partial answer" {
							t.Fatal("trailing partial classified as intermediate")
						}
					}
				}
				for i, item := range items {
					if item.CanonicalOrdinal == 0 || i > 0 && item.CanonicalOrdinal < items[i-1].CanonicalOrdinal {
						t.Fatalf("invalid canonical positions: %+v", items)
					}
				}
				after, _ := json.Marshal(entries)
				if string(before) != string(after) {
					t.Fatal("read rewrote entries")
				}
				clone := cloneThreadTurns(turns)
				if !reflect.DeepEqual(clone, turns) {
					t.Fatal("clone changed facts")
				}
				if len(clone[0].IntermediateItemIDs) > 0 {
					clone[0].IntermediateItemIDs[0] = "mutated"
					if turns[0].IntermediateItemIDs[0] == "mutated" {
						t.Fatal("clone shares intermediate references")
					}
				}
			}
		})
	}
}

func TestCanonicalToolResultRetainsCallPosition(t *testing.T) {
	entries := []sessiontree.Entry{
		{Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run", Message: session.Message{ToolCallID: "call", ToolName: "inspect"}},
		{Type: sessiontree.EntryCustom},
		{Type: sessiontree.EntryToolResult, TurnID: "turn", RunID: "run", Message: session.Message{ToolCallID: "call", ToolName: "inspect"}},
	}
	items, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil || len(items) != 1 || items[0].CanonicalOrdinal != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}
