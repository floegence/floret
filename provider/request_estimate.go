package provider

import "github.com/floegence/floret/v7/internal/requestestimate"

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
	Provider string
	Model    string
	Format   RequestFormat
	Payload  []byte
}

// EstimateRenderedRequest counts model input with a supported official vocabulary
// or an explicitly identified proxy estimate. Native usage remains authoritative.
// No network, resource resolution, or provider call occurs. Image transport data
// is budgeted separately; opaque files retain a conservative transport-byte budget.
func EstimateRenderedRequest(request RenderedRequest) (TokenEstimate, error) {
	estimate, err := requestestimate.Count(request.Provider, request.Model, string(request.Format), request.Payload)
	if err != nil {
		return TokenEstimate{}, err
	}
	return TokenEstimate{PrefixTokens: estimate.Prefix, MessageTokens: estimate.Messages, ToolDefinitionTokens: estimate.Tools,
		EstimatedInputTokens: estimate.Prefix + estimate.Messages + estimate.Tools, Source: estimate.Source, Method: estimate.Method, Confidence: "conservative", Coverage: "complete_request"}, nil
}
