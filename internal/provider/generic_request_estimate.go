package provider

import (
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/internal/requestestimate"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"strings"
)

// Generic gateways expose typed semantic input but no final provider rendering.
// This conversion is a projection only; all counting belongs to requestestimate.
func estimateGenericInput(req Request) (TokenEstimate, error) {
	messages, err := MessagesWithEphemeralUser(req.Messages, req.EphemeralUser)
	if err != nil {
		return TokenEstimate{}, err
	}
	var rendered []any
	var attrs []requestestimate.Attribution
	reasoning := ""
	for _, m := range messages {
		if m.Role != session.Assistant {
			reasoning = ""
		}
		item := map[string]any{"role": string(m.Role), "content": m.Content}
		if m.Role == session.Tool {
			item["role"] = "tool"
			item["tool_call_id"] = m.ToolCallID
		}
		if m.Role == session.Assistant && m.ToolCallID != "" {
			item["content"] = ""
			item["tool_calls"] = []any{map[string]any{"type": "function", "id": m.ToolCallID, "function": map[string]any{"name": m.ToolName, "arguments": m.ToolArgs}}}
		}
		if m.Role == session.Assistant && m.Reasoning != "" {
			if strings.HasPrefix(m.Reasoning, reasoning) {
				item["reasoning_content"] = strings.TrimPrefix(m.Reasoning, reasoning)
				reasoning = m.Reasoning
			} else {
				item["reasoning_content"] = m.Reasoning
				reasoning += m.Reasoning
			}
		}
		if m.Kind == session.MessageKindModelContext || m.Kind == session.MessageKindCompactionSummary {
			category := "model_context"
			if m.Kind == session.MessageKindCompactionSummary {
				category = "compaction"
			}
			a := requestestimate.Attribution{Path: fmt.Sprintf("/messages/%d/content", len(rendered)), Category: category}
			offset := 0
			for _, p := range m.ModelContextParts {
				a.Parts = append(a.Parts, requestestimate.Part{Start: offset, End: offset + len(p.Text), Key: p.Key})
				offset += len(p.Text)
			}
			attrs = append(attrs, a)
		}
		rendered = append(rendered, item)
	}
	var definitions []any
	for _, tool := range req.Tools {
		source, _ := tool.Annotations["source"].(string)
		namespace, _ := tool.Annotations["namespace"].(string)
		attrs = append(attrs, requestestimate.Attribution{Path: fmt.Sprintf("/tools/%d", len(definitions)), Category: "tool_definition", Key: tool.Name, Source: source, Namespace: namespace})
		definitions = append(definitions, map[string]any{"type": "function", "function": map[string]any{"name": tool.Name, "description": tool.Description, "parameters": tool.InputSchema}})
	}
	for _, tool := range req.HostedTools {
		attrs = append(attrs, requestestimate.Attribution{Path: fmt.Sprintf("/tools/%d", len(definitions)), Category: "tool_definition", Key: tool.Name, Source: "provider"})
		definitions = append(definitions, map[string]any{"type": tool.Type, "name": tool.Name, "description": tool.Description, "parameters": tool.Parameters})
	}
	raw, err := json.Marshal(map[string]any{"messages": rendered, "tools": definitions})
	if err != nil {
		return TokenEstimate{}, err
	}
	model := req.Model
	if model == "" {
		model = "unknown"
	}
	out, err := requestestimate.CountAttributed(req.Provider, model, "input_json", raw, attrs)
	if err != nil {
		return TokenEstimate{}, err
	}
	if req.EphemeralUser != nil {
		out.Breakdown = contextpolicy.ContextBreakdown{}
	}
	return TokenEstimate{Breakdown: out.Breakdown, AttachmentTokens: out.Media, PrefixTokens: out.Prefix, MessageTokens: out.Messages, ToolDefinitionTokens: out.Tools, EstimatedInputTokens: out.Prefix + out.Messages + out.Tools, Source: out.Source, Method: TokenEstimateGenericPayload, Confidence: EstimateApproximate}, nil
}
