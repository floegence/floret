package provider

import (
	"context"
	"encoding/json"

	"github.com/floegence/floret/v7/internal/requestestimate"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

// ContextBreakdownEstimator applies the adapter's actual history filtering
// before collecting semantic input weights. It never changes budget estimates.
type ContextBreakdownEstimator interface {
	EstimateContextBreakdown(context.Context, Request) (contextpolicy.ContextBreakdown, error)
}

// EstimateContextBreakdown counts only provider-visible fields of the frozen
// projection using offline BPE estimates. Media comes from prepared input,
// never attachment descriptors or original artifact byte sizes.
func EstimateContextBreakdown(req Request) (contextpolicy.ContextBreakdown, error) {
	if req.EphemeralUser != nil {
		return contextpolicy.ContextBreakdown{}, nil
	}
	out := contextpolicy.ContextBreakdown{Source: "semantic_input_bpe_v2"}
	addDetail := func(category, key, source, namespace string, tokens int64) {
		if out.Details == nil {
			out.Details = &contextpolicy.ContextBreakdownDetails{}
		}
		for i := range out.Details.Items {
			item := &out.Details.Items[i]
			if item.Category == category && item.Key == key && item.Source == source && item.Namespace == namespace {
				item.Tokens += tokens
				return
			}
		}
		out.Details.Items = append(out.Details.Items, contextpolicy.ContextTokenDetail{Category: category, Key: key, Source: source, Namespace: namespace, Tokens: tokens})
	}
	count := func(value any) (int64, error) {
		raw, err := json.Marshal(value)
		if err != nil {
			return 0, err
		}
		return requestestimate.CountText(req.Provider, req.Model, string(raw))
	}
	for _, message := range req.Messages {
		var target *int64
		switch {
		case message.Role == session.System:
			target = &out.SystemTokens
		case message.Kind == session.MessageKindModelContext:
			target = &out.ModelContextTokens
		case message.Kind == session.MessageKindCompactionSummary:
			target = &out.CompactionTokens
		case message.Role == session.Tool:
			target = &out.ToolResultTokens
		case message.Role == session.Assistant && message.ToolCallID != "":
			target = &out.ToolCallTokens
		case message.Role == session.Assistant:
			target = &out.AssistantTokens
		default:
			target = &out.UserTokens
		}
		// Internal journal IDs, Activity, full-output references and display
		// metadata do not consume model input. Tool call text is a marker.
		item := struct {
			Role   session.Role `json:"role"`
			Text   string       `json:"text,omitempty"`
			CallID string       `json:"call_id,omitempty"`
			Name   string       `json:"name,omitempty"`
			Args   string       `json:"arguments,omitempty"`
		}{Role: message.Role, Text: message.Content}
		if message.ToolCallID != "" {
			item.CallID, item.Name = message.ToolCallID, message.ToolName
		}
		if message.Role == session.Assistant && message.ToolCallID != "" {
			item.Text, item.Args = "", message.ToolArgs
		}
		n, err := count(item)
		if err != nil {
			return contextpolicy.ContextBreakdown{}, err
		}
		if message.Kind == session.MessageKindModelContext && len(message.ModelContextParts) > 0 {
			n = 0
			framing, err := count(struct {
				Role session.Role `json:"role"`
			}{message.Role})
			if err != nil {
				return contextpolicy.ContextBreakdown{}, err
			}
			addDetail("model_context", "", "", "", framing)
			n += framing
			for _, part := range message.ModelContextParts {
				weight, err := requestestimate.CountText(req.Provider, req.Model, part.Text)
				if err != nil {
					return contextpolicy.ContextBreakdown{}, err
				}
				addDetail("model_context", part.Key, "", "", weight)
				n += weight
			}
		} else if message.Kind == session.MessageKindModelContext {
			addDetail("model_context", "", "", "", n)
		}
		*target += n
		if message.Role == session.Assistant && message.Reasoning != "" {
			n, err := count(message.Reasoning)
			if err != nil {
				return contextpolicy.ContextBreakdown{}, err
			}
			out.ReasoningTokens += n
		}
	}
	for _, tool := range req.Tools {
		n, err := count(tool)
		if err != nil {
			return contextpolicy.ContextBreakdown{}, err
		}
		source, _ := tool.Annotations["source"].(string)
		namespace, _ := tool.Annotations["namespace"].(string)
		addDetail("tool_definition", tool.Name, source, namespace, n)
		out.ToolDefinitionTokens += n
	}
	for _, tool := range req.HostedTools {
		n, err := count(tool)
		if err != nil {
			return contextpolicy.ContextBreakdown{}, err
		}
		addDetail("tool_definition", tool.Name, "provider", "", n)
		out.ToolDefinitionTokens += n
	}
	return out, nil
}
