package runtime

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func BenchmarkCanonicalSharedContentProjection(b *testing.B) {
	for _, count := range []int{1096, 10960} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			entries := make([]sessiontree.Entry, count)
			for i := range entries {
				entries[i] = sessiontree.Entry{ID: fmt.Sprint(i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, CreatedAt: time.Unix(1, 0), Message: session.Message{Role: session.Assistant, ContentSourceID: "source", Content: strings.Repeat("中a", 32)}}
			}
			b.ReportAllocs()
			b.SetBytes(int64(count * 128))
			b.ResetTimer()
			for range b.N {
				items, _, err := threadRuntimeItemsFromEntries(entries)
				if err != nil || len(items) != 1 || len(items[0].Text) != count*128 {
					b.Fatalf("items=%d err=%v", len(items), err)
				}
			}
		})
	}
}

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
					if entries[i].Message.Content != "" && entries[i].Type == sessiontree.EntryAssistantMessage {
						entries[i].Message.ContentSourceID = "assistant:" + entries[i].ID
					}
					if entries[i].Message.Reasoning != "" {
						entries[i].Message.ReasoningSourceID = "thinking:" + entries[i].ID
					}
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

func TestCanonicalSharedContentSourceSurvivesNewlyVisibleCorrectionReasoning(t *testing.T) {
	at := time.Now()
	entries := []sessiontree.Entry{
		{ID: "first", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, CreatedAt: at, Message: session.Message{Role: session.Assistant, Content: "first", ContentSourceID: "assistant:turn:1"}},
		{ID: "invalid", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, CreatedAt: at, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindToolValidationError, ToolName: "read", ToolCallID: "invalid", Reasoning: "correction", ReasoningSourceID: "thinking:entry:invalid"}},
		{ID: "second", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, CreatedAt: at, Message: session.Message{Role: session.Assistant, Content: "second", ContentSourceID: "assistant:turn:1"}},
	}
	items, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil || len(items) != 2 || items[0].ID != "assistant:turn:1" || items[0].Text != "firstsecond" || len(items[0].sourceEntryIDs) != 2 || items[1].ID != "thinking:entry:invalid" {
		t.Fatalf("shared historical source split: %+v %v", items, err)
	}
}
