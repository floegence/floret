package adapters

import (
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/requestestimate"
	"github.com/floegence/floret/v7/internal/session"
)

func estimateRenderedRequest(req provider.Request, format string, body any) (provider.TokenEstimate, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	model := req.Model
	if model == "" {
		model = "unknown"
	}
	attrs := []requestestimate.Attribution{}
	// The adapter already owns protocol projection; map ordered canonical user
	// messages onto its rendered user messages after all assistant coalescing.
	var bodyMap struct {
		System   json.RawMessage
		Messages []struct {
			Role    string
			Content json.RawMessage
		}
	}
	if err := json.Unmarshal(raw, &bodyMap); err != nil {
		return provider.TokenEstimate{}, err
	}

	addMessage := func(path string, canonical session.Message) {
		if canonical.Kind != session.MessageKindModelContext && canonical.Kind != session.MessageKindCompactionSummary {
			return
		}
		category := "model_context"
		if canonical.Kind == session.MessageKindCompactionSummary {
			category = "compaction"
		}
		a := requestestimate.Attribution{Path: path, Category: category}
		offset := 0
		for _, p := range canonical.ModelContextParts {
			a.Parts = append(a.Parts, requestestimate.Part{Start: offset, End: offset + len(p.Text), Key: p.Key})
			offset += len(p.Text)
		}
		attrs = append(attrs, a)
	}
	canonical, err := provider.MessagesWithEphemeralUser(req.Messages, req.EphemeralUser)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	for _, role := range []session.Role{session.System, session.User} {
		var originals []session.Message
		for _, m := range canonical {
			if m.Role == role {
				originals = append(originals, m)
			}
		}
		if role == session.System && format == "anthropic_messages" {
			for i, m := range originals {
				path := "/system"
				if len(bodyMap.System) > 0 && bodyMap.System[0] == '[' {
					path = fmt.Sprintf("/system/%d/text", i)
				}
				addMessage(path, m)
			}
			continue
		}
		u := 0
		for i, m := range bodyMap.Messages {
			if m.Role != string(role) {
				continue
			}
			path := fmt.Sprintf("/messages/%d/content", i)
			if format == "anthropic_messages" && len(m.Content) > 0 && m.Content[0] == '[' {
				var blocks []struct{ Type string }
				_ = json.Unmarshal(m.Content, &blocks)
				if len(blocks) > 0 && blocks[0].Type == "tool_result" {
					continue
				}
				path += "/0/text"
			}
			if u >= len(originals) {
				break
			}
			addMessage(path, originals[u])
			u++
		}
		if u != len(originals) {
			attrs = append(attrs, requestestimate.Attribution{Path: "/missing_canonical_attribution"})
		}
	}

	for i, t := range req.Tools {
		source, _ := t.Annotations["source"].(string)
		ns, _ := t.Annotations["namespace"].(string)
		attrs = append(attrs, requestestimate.Attribution{Path: fmt.Sprintf("/tools/%d", i), Category: "tool_definition", Key: t.Name, Source: source, Namespace: ns})
	}
	out, err := requestestimate.CountAttributed(req.Provider, model, format, raw, attrs)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	return provider.TokenEstimate{Breakdown: out.Breakdown, AttachmentTokens: out.Media, PrefixTokens: out.Prefix, MessageTokens: out.Messages, ToolDefinitionTokens: out.Tools, EstimatedInputTokens: out.Prefix + out.Messages + out.Tools, Source: out.Source, Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative, Coverage: provider.TokenEstimateCoverageComplete}, nil
}
