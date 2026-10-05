package runtime

import (
	"context"
	"reflect"
	"strings"
	"testing"

	internalprovider "github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/provider"
)

func TestReasoningHistoryDefaultAndEstimate(t *testing.T) {
	req := internalprovider.Request{Messages: []session.Message{
		{Role: session.User, Content: "old"},
		{Role: session.Assistant, Content: "answer", Reasoning: strings.Repeat("old thought ", 1000)},
		{Role: session.User, Content: "new"},
	}}
	for _, policy := range []provider.ReasoningHistoryPolicy{"", provider.ReasoningHistoryAll} {
		adapter := modelGatewayProvider{reasoningHistory: policy}
		got, err := adapter.requestMessages(req)
		if err != nil || !reflect.DeepEqual(got, req.Messages) {
			t.Fatalf("default changed history: %v", err)
		}
	}
	adapter := modelGatewayProvider{reasoningHistory: provider.ReasoningHistoryCurrentUser}
	filtered, err := adapter.requestMessages(req)
	if err != nil {
		t.Fatal(err)
	}
	want, err := internalprovider.GenericRequestEstimate(internalprovider.Request{Messages: filtered})
	if err != nil {
		t.Fatal(err)
	}
	got, err := adapter.EstimateTokens(context.Background(), req)
	if err != nil || got != want {
		t.Fatalf("estimate=%+v want=%+v err=%v", got, want, err)
	}
	full, err := internalprovider.GenericRequestEstimate(req)
	if err != nil || got.EstimatedInputTokens >= full.EstimatedInputTokens {
		t.Fatalf("discarded reasoning was counted: %+v full=%+v err=%v", got, full, err)
	}
	// A low-level continuation without a new user boundary must retain thought.
	req.Messages = req.Messages[1:2]
	filtered, err = adapter.requestMessages(req)
	if err != nil || !reflect.DeepEqual(filtered, req.Messages) {
		t.Fatalf("unanchored continuation lost reasoning: %v", err)
	}
}

func TestReasoningHistoryCurrentUserPreservesConversationAndContinuation(t *testing.T) {
	history := []session.Message{
		{Role: session.System, Content: "Current tools are available."},
		{Role: session.User, Content: "Old task"},
		{Role: session.Assistant, Reasoning: "OLD private thought"},
		{Role: session.Assistant, Content: "Old answer", Reasoning: "OLD unavailable tools"},
		{Role: session.Assistant, ToolCallID: "old", ToolName: "read", ToolArgs: `{}`, Reasoning: "OLD tool thinking"},
		{Role: session.Tool, ToolCallID: "old", ToolName: "read", Content: "old result"},
		{Role: session.User, Content: "New task", EntryID: "current"},
		{Role: session.Assistant, Reasoning: "CURRENT truncated reasoning"},
		{Role: session.Assistant, ToolCallID: "current", ToolName: "read", ToolArgs: `{}`, Reasoning: "CURRENT tool thinking"},
		{Role: session.Tool, ToolCallID: "current", ToolName: "read", Content: "current result"},
	}
	before := session.CloneMessages(history)
	for _, ephemeralAt := range []int{-1, 6, 9} {
		req := internalprovider.Request{Messages: history}
		if ephemeralAt >= 0 {
			req.EphemeralUser = &internalprovider.EphemeralUserMessage{HistoryInsertAt: ephemeralAt, Message: session.Message{Role: session.User, Content: "Ephemeral answer"}}
		}
		adapter := modelGatewayProvider{reasoningHistory: provider.ReasoningHistoryCurrentUser}
		got, err := adapter.modelRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		var thoughts, text string
		var calls, results int
		for _, message := range got.Messages {
			thoughts += message.Reasoning
			text += message.Text
			for _, call := range message.ToolCalls {
				thoughts += call.Reasoning
				calls++
			}
			if message.ToolResult != nil {
				results++
			}
			if err := message.validateStored(); err != nil {
				t.Fatal(err)
			}
		}
		if strings.Contains(thoughts, "OLD") || !strings.Contains(thoughts, "CURRENT truncated reasoning") || !strings.Contains(thoughts, "CURRENT tool thinking") {
			t.Fatalf("ephemeral=%d reasoning=%q", ephemeralAt, thoughts)
		}
		if !strings.Contains(text, "Old answer") || calls != 2 || results != 2 {
			t.Fatalf("lost canonical content: %+v", got.Messages)
		}
		if !reflect.DeepEqual(history, before) {
			t.Fatal("canonical history was mutated")
		}
	}
}
