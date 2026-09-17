package adapters

import (
	"encoding/json"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/requestestimate"
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
	out, err := requestestimate.Count(req.Provider, model, format, raw)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	return provider.TokenEstimate{PrefixTokens: out.Prefix, MessageTokens: out.Messages, ToolDefinitionTokens: out.Tools, EstimatedInputTokens: out.Prefix + out.Messages + out.Tools, Source: out.Source, Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative, Coverage: provider.TokenEstimateCoverageComplete}, nil
}
