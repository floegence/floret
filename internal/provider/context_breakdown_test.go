package provider

import (
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
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
	got, err := testBreakdown(request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Source == "" || got.SystemTokens <= 0 || got.UserTokens <= 0 || got.AssistantTokens <= 0 || got.ToolCallTokens <= 0 || got.ToolResultTokens <= 0 || got.ModelContextTokens <= 0 || got.CompactionTokens <= 0 || got.ReasoningTokens <= 0 || got.ToolDefinitionTokens <= 0 {
		t.Fatalf("missing category: %+v", got)
	}
	request.Messages[6].ToolResult.OriginalBytes = 1000000000
	same, err := testBreakdown(request)
	if err != nil || !reflect.DeepEqual(same, got) {
		t.Fatalf("non-model output metadata changed breakdown: %+v, %v", same, err)
	}
	request.Messages[6].Content = strings.Repeat("large visible output ", 2000)
	larger, err := testBreakdown(request)
	if err != nil || larger.ToolResultTokens <= got.ToolResultTokens {
		t.Fatalf("visible tool output not counted: %+v, %v", larger, err)
	}
	larger.ToolResultTokens = got.ToolResultTokens
	if !reflect.DeepEqual(larger, got) {
		t.Fatalf("tool output changed unrelated categories: %+v", larger)
	}
}

func TestRequestBreakdownDoesNotPersistSupplementalContextStatistics(t *testing.T) {
	request := Request{Messages: []session.Message{{Role: session.User, Content: "durable"}}, EphemeralUser: &EphemeralUserMessage{HistoryInsertAt: 1, Message: session.Message{Role: session.User, Content: "secret"}}}
	got, err := testBreakdown(request)
	if err != nil || got.Source != "" {
		t.Fatalf("ephemeral request breakdown=%+v err=%v", got, err)
	}
}

func TestBreakdownAttributesFrozenToolsAndRetainedContextUpdates(t *testing.T) {
	snapshot := func(skill string) *session.ModelContextSnapshot {
		return &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "skills", Title: "Available skills", Text: skill}, {Key: "repository_rules", Title: "Rule files", Text: "AGENTS instructions"}}}
	}
	history, _, err := session.ProjectProviderHistory([]session.Message{
		{Role: session.User, Content: "work", ModelContextSnapshot: snapshot("old skill catalog")},
		{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: snapshot("new skill catalog with more content")},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Messages: history, Tools: []tools.ToolDefinition{
		{Name: "terminal.exec", Description: "execute", Annotations: map[string]any{"source": "builtin", "namespace": "terminal"}},
		{Name: "opaque_name", Description: "external", Annotations: map[string]any{"source": "mcp", "namespace": "docs"}},
	}, HostedTools: []HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}
	got, err := testBreakdown(request)
	if err != nil {
		t.Fatal(err)
	}
	sums := map[string]int64{}
	origins := map[string]string{}
	skill := int64(0)
	for _, item := range got.Details.Items {
		sums[item.Category] += item.Tokens
		origins[item.Key] = item.Source
		if item.Key == "skills" {
			skill = item.Tokens
		}
	}
	if sums["tool_definition"] != got.ToolDefinitionTokens || sums["model_context"] != got.ModelContextTokens || origins["opaque_name"] != "mcp" || origins["terminal.exec"] != "builtin" || origins["web_search"] != "provider" || skill <= 0 {
		t.Fatalf("bad attribution: %+v", got.Details)
	}
	latest, _, _ := session.ProjectProviderHistory([]session.Message{{Role: session.User, Content: "work", ModelContextSnapshot: snapshot("new skill catalog with more content")}}, "")
	request.Messages = latest
	current, err := testBreakdown(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range current.Details.Items {
		if item.Key == "skills" && item.Tokens >= skill {
			t.Fatal("historical skill update was not counted")
		}
	}
}

func testBreakdown(req Request) (contextpolicy.ContextBreakdown, error) {
	out, err := GenericRequestEstimate(req)
	return out.Breakdown, err
}
