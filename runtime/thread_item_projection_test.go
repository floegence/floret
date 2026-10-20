package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/tools"
)

func incrementalProjectionFixture(t testing.TB) []sessiontree.Entry {
	t.Helper()
	encode := func(value any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	input := &tools.InputRequest{Summary: "Complete verification", Questions: []tools.InputQuestion{{ID: "q", Prompt: "Continue?", Kind: "write"}}}
	entries := []sessiontree.Entry{
		{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted},
		{ID: "user", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "request"}},
		{ID: "goal", Type: sessiontree.EntryGoal, Payload: encode(sessiontree.GoalChange{State: session.GoalState{Version: 1, Text: "Verify canonical facts", Status: "active", Actor: "user"}, Operation: "set"})},
		{ID: "text-a", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "中文🌸", ContentSourceID: "answer", Reasoning: "reason", ReasoningSourceID: "thinking"}},
		{ID: "text-b", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "\r\né", ContentSourceID: "answer", Reasoning: "reason", ReasoningSourceID: "thinking"}},
		{ID: "steer:first", Type: sessiontree.EntrySteeringRequested, Message: session.Message{Role: session.User, Content: "Verify twice"}},
		{ID: "approval", Type: sessiontree.EntryInteractionAsked, Payload: encode(ThreadInteraction{ID: "approval", TurnID: "turn", RunID: "run", Kind: ThreadInteractionApproval, ToolCallID: "work"})},
		{ID: "interaction-resolved:approval", Type: sessiontree.EntryInteractionDone, Payload: encode(InteractionResolution{Accepted: true, Approved: func() *bool { v := true; return &v }()})},
		{ID: "work-call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "work"}},
		{ID: "review-call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "goal", ToolCallID: "review", ToolArgs: string(encode(map[string]any{"operation": "review", "expected_version": 1, "outcome": "continue", "evidence": "Verify"}))}},
		{ID: "review", Type: sessiontree.EntryGoalReview, Payload: encode(sessiontree.GoalReview{Version: 1, Outcome: "continue", Evidence: "Verify"})},
		{ID: "review-result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "goal", ToolCallID: "review", ToolResult: &session.ToolResultView{Status: "success"}}},
		{ID: "work-result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", Content: "Observed", ToolResult: &session.ToolResultView{Status: "success", InputRequired: input}}},
		{ID: "later-call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "later"}},
		{ID: "later-result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "later", ToolResult: &session.ToolResultView{Status: "error", InputRequired: input}}},
		{ID: "waiting", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnWaiting},
		{ID: "interaction-resolved:" + sessiontree.ToolInputInteractionID("work-result"), Type: sessiontree.EntryInteractionDone, Payload: encode(InteractionResolution{Accepted: true, Input: map[string]string{"q": "yes"}})},
		{ID: "hosted", Type: sessiontree.EntryCustom, Metadata: map[string]string{"kind": sessiontree.HostedToolEntryKind, "type": string(observation.EventTypeHostedToolCall)}, Message: session.Message{ToolCallID: "search", ToolName: "web_search"}},
		{ID: "terminal", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, Metadata: map[string]string{sessiontree.GoalCompletedMetadataKey: "1"}},
	}
	for index := range entries {
		entries[index].ThreadID, entries[index].TurnID, entries[index].RunID = "thread", "turn", "run"
		entries[index].CreatedAt = time.Unix(100+int64(index), 0).UTC()
	}
	return entries
}

func TestThreadItemProjectionMatchesEveryPrefixAndChunkBoundary(t *testing.T) {
	entries := incrementalProjectionFixture(t)
	before, _ := json.Marshal(entries)
	// This fixed digest records the original full-path implementation's complete
	// facts; chunk comparisons alone could let two changed reducers agree wrongly.
	wantItems, wantInteractions, err := threadRuntimeItemsFromEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(struct {
		Items        []ThreadItem
		Interactions []ThreadInteraction
	}{wantItems, wantInteractions})
	const historicalSHA256 = "1dc5d80086c8103d70a817ba97d8c01d1309565d0847af8b9536ece191ba8b0c"
	if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != historicalSHA256 {
		t.Fatalf("historical projection digest: %s", got)
	}
	for size := 1; size <= len(entries); size++ {
		projection := newThreadItemProjection()
		for start := 0; start < len(entries); start += size {
			end := min(len(entries), start+size)
			if err := projection.append(entries[start:end], nil); err != nil {
				t.Fatal(err)
			}
			items, interactions := projection.snapshot()
			expectedItems, expectedInteractions, err := threadRuntimeItemsFromEntries(entries[:end])
			if err != nil || !reflect.DeepEqual(items, expectedItems) || !reflect.DeepEqual(interactions, expectedInteractions) {
				t.Fatalf("chunk=%d prefix=%d differs: %v", size, end, err)
			}
		}
	}
	after, _ := json.Marshal(entries)
	if string(before) != string(after) {
		t.Fatal("projection mutated canonical facts")
	}
}

func BenchmarkIncrementalFixedHistoryAppend(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			projection := newThreadItemProjection()
			entries := make([]sessiontree.Entry, count)
			for i := range entries {
				entries[i] = sessiontree.Entry{ID: fmt.Sprint(i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "historical"}}
			}
			if err := projection.append(entries, nil); err != nil {
				b.Fatal(err)
			}
			// Reuse an exact invocation so each measurement changes one item without
			// including intentional presentation growth or explicit full snapshots.
			call := sessiontree.Entry{ID: "call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "work"}}
			if err := projection.append([]sessiontree.Entry{call}, nil); err != nil {
				b.Fatal(err)
			}
			result := sessiontree.Entry{ID: "result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", ToolResult: &session.ToolResultView{Status: "success"}}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := projection.append([]sessiontree.Entry{result}, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
