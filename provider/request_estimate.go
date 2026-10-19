package provider

import (
	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/internal/requestestimate"
)

// RequestFormat identifies the rendered input protocol, independently of model
// identity. An OpenAI-compatible transport does not imply an OpenAI tokenizer.
type RequestFormat string

const (
	RequestFormatOpenAIChat      RequestFormat = "openai_chat"
	RequestFormatOpenAIResponses RequestFormat = "openai_responses"
	RequestFormatAnthropic       RequestFormat = "anthropic_messages"
	// RequestFormatJSON is for opaque host transports. Payload must contain only
	// complete model input, excluding routing, credentials and runtime metadata.
	// It uses the generic estimate method, without claiming server wire fidelity.
	RequestFormatJSON RequestFormat = "input_json"
)

// RenderedRequest is the immutable JSON input of one provider request. The
// caller sends the same prepared input after estimation. Payload is not retained.
type RenderedRequest struct {
	// Attribution refers only to decoded values in Payload using JSON Pointer paths.
	Attribution []RenderedContentAttribution
	Provider    string
	Model       string
	Format      RequestFormat
	Payload     []byte
}

// EstimateRenderedRequest counts model input with a supported official vocabulary
// or an explicitly identified proxy estimate. Native usage remains authoritative.
// No network, resource resolution, or provider call occurs. Image transport data
// is budgeted separately; opaque files retain a conservative transport-byte budget.
// Encrypted/redacted reasoning remains diagnostic with partial attribution; its
// length is not converted to decrypted tokens. Hidden-input overflow uses the
// Engine's bounded compaction recovery.
func EstimateRenderedRequest(request RenderedRequest) (TokenEstimate, error) {
	estimate, err := requestestimate.CountAttributed(request.Provider, request.Model, string(request.Format), request.Payload, internalAttribution(request.Attribution))
	if err != nil {
		return TokenEstimate{}, err
	}
	return TokenEstimate{Characters: estimate.Characters.Clone(), Breakdown: config.ContextBreakdown(estimate.Breakdown), AttachmentTokens: estimate.Media, PrefixTokens: estimate.Prefix, MessageTokens: estimate.Messages, ToolDefinitionTokens: estimate.Tools,
		EstimatedInputTokens: estimate.Prefix + estimate.Messages + estimate.Tools, Source: estimate.Source, Method: estimate.Method, Confidence: "approximate", Coverage: "complete_request"}, nil
}

// MessageAttribution identifies canonical model-context or compaction text.
// Parts partition Text by UTF-8 byte offsets and contain no prompt content.
type MessageAttribution struct {
	Category string                   `json:"category"`
	Parts    []ContentAttributionPart `json:"parts,omitempty"`
}

// ContentAttributionPart identifies one contiguous decoded-text span.
type ContentAttributionPart struct {
	// Category optionally identifies a span within merged protocol instructions.
	Category string `json:"category,omitempty"`
	Start    int    `json:"start"`
	End      int    `json:"end"`
	Key      string `json:"key,omitempty"`
}

// RenderedContentAttribution assigns a frozen payload value to its actual origin.
// Parts apply to strings and cover their complete UTF-8 text. Tool identifiers
// are observation metadata and must not be added to the model-visible payload.
type RenderedContentAttribution struct {
	Path      string
	Category  string
	Key       string
	Source    string
	Namespace string
	Parts     []ContentAttributionPart
}

func internalAttribution(values []RenderedContentAttribution) []requestestimate.Attribution {
	out := make([]requestestimate.Attribution, len(values))
	for i, v := range values {
		out[i] = requestestimate.Attribution{Path: v.Path, Category: v.Category, Key: v.Key, Source: v.Source, Namespace: v.Namespace}
		for _, p := range v.Parts {
			out[i].Parts = append(out[i].Parts, requestestimate.Part{Start: p.Start, End: p.End, Key: p.Key, Category: p.Category})
		}
	}
	return out
}

func cloneTokenEstimate(in TokenEstimate) TokenEstimate {
	in.Characters = in.Characters.Clone()
	if in.Breakdown.Details != nil {
		details := *in.Breakdown.Details
		details.Items = append([]config.ContextTokenDetail(nil), details.Items...)
		in.Breakdown.Details = &details
	}
	return in
}
