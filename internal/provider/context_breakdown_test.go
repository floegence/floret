package provider

import (
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestRequestBreakdownCoversProviderHistoryWithoutCountingDiscardedToolOutput(t *testing.T) {
	request := Request{Messages: []session.Message{
		{Role: session.System, Content: "System instruction"},
		{Role: session.User, Content: "User text and rendered reference"},
		{Role: session.User, Kind: session.MessageKindModelContext, Content: "Project rules and skills"},
		{Role: session.User, Kind: session.MessageKindCompactionSummary, Content: "Earlier conversation summary"},
		{Role: session.Assistant, Content: "Assistant reply", Reasoning: "Retained reasoning"},
		{Role: session.Assistant, ToolCallID: "read", ToolName: "read", ToolArgs: `{"path":"a.go"}`},
		{Role: session.Tool, ToolCallID: "read", ToolName: "read", Content: "Visible output", ToolResult: &session.ToolResultView{OriginalBytes: 900000, VisibleBytes: 14, Truncated: true}},
	}, Tools: []tools.ToolDefinition{{Name: "read", Description: "Read a file"}}}
	got, err := EstimateContextBreakdown(request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Source == "" || got.SystemTokens <= 0 || got.UserTokens <= 0 || got.AssistantTokens <= 0 || got.ToolCallTokens <= 0 || got.ToolResultTokens <= 0 || got.ModelContextTokens <= 0 || got.CompactionTokens <= 0 || got.ReasoningTokens <= 0 || got.ToolDefinitionTokens <= 0 {
		t.Fatalf("missing category: %+v", got)
	}
	request.Messages[6].ToolResult.OriginalBytes = 1000000000
	same, err := EstimateContextBreakdown(request)
	if err != nil || same != got {
		t.Fatalf("non-model output metadata changed breakdown: %+v, %v", same, err)
	}
	request.Messages[6].Content = strings.Repeat("large visible output ", 2000)
	larger, err := EstimateContextBreakdown(request)
	if err != nil || larger.ToolResultTokens <= got.ToolResultTokens {
		t.Fatalf("visible tool output not counted: %+v, %v", larger, err)
	}
	larger.ToolResultTokens = got.ToolResultTokens
	if larger != got {
		t.Fatalf("tool output changed unrelated categories: %+v", larger)
	}
}

func TestRequestBreakdownDoesNotPersistSupplementalContextStatistics(t *testing.T) {
	request := Request{Messages: []session.Message{{Role: session.User, Content: "durable"}}, EphemeralUser: &EphemeralUserMessage{HistoryInsertAt: 1, Message: session.Message{Role: session.User, Content: "secret"}}}
	got, err := EstimateContextBreakdown(request)
	if err != nil || got.Source != "" {
		t.Fatalf("ephemeral request breakdown=%+v err=%v", got, err)
	}
}
