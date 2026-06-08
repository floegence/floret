package adapters

import (
	"encoding/json"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session/contextpolicy"
)

func estimateRenderedParts(source string, prefix, history, tools any) (provider.TokenEstimate, error) {
	prefixTokens, err := estimateRenderedValue(prefix)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	historyTokens, err := estimateRenderedValue(history)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	toolTokens, err := estimateRenderedValue(tools)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	return provider.TokenEstimate{
		PrefixTokens:  prefixTokens,
		HistoryTokens: historyTokens,
		ToolTokens:    toolTokens,
		InputTokens:   prefixTokens + historyTokens + toolTokens,
		Source:        source,
		Confidence:    provider.EstimateConservative,
	}, nil
}

func estimateRenderedValue(value any) (int64, error) {
	if isEmptyRenderedValue(value) {
		return 0, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	return contextpolicy.EstimateText(string(raw)), nil
}

func isEmptyRenderedValue(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return v == ""
	case []chatMessage:
		return len(v) == 0
	case []chatTool:
		return len(v) == 0
	case []anthropicMessage:
		return len(v) == 0
	case []anthropicTool:
		return len(v) == 0
	case []anthropicContentBlock:
		return len(v) == 0
	default:
		return false
	}
}
