package observation

import "github.com/floegence/floret/v7/config"

// ContextAccounting reconciles one request sample without reallocating category tokens.
// Categories are estimates even when the reference is provider measured.
type ContextAccounting struct {
	// Characters is validated visible input from the same confirmed request.
	Characters          *config.ContextCharacterBreakdown `json:"characters,omitempty"`
	CharactersAvailable bool                              `json:"characters_available"`
	CharacterLimitation string                            `json:"character_limitation,omitempty"`
	Available           bool                              `json:"available"`
	InputTokens         int64                             `json:"input_tokens"`
	Measured            bool                              `json:"measured"`
	// Deprecated: token attribution is a budget diagnostic, not a content distribution.
	BreakdownAvailable bool   `json:"breakdown_available"`
	UnattributedTokens int64  `json:"unattributed_tokens"`
	Limitation         string `json:"limitation,omitempty"`
}

// ReconcileContextStatus is the sole display arithmetic for a frozen request.
// A validation error preserves a trustworthy native total in the returned result.
// Missing attribution and legacy mixed-unit data are ordinary unavailable states.
func ReconcileContextStatus(s ContextStatus) (ContextAccounting, error) {
	result := ContextAccounting{}
	if s.Phase == ContextPhaseProviderUsage && s.Usage.Available && s.Usage.HasInputUsage() && s.Usage.Source == "native" && s.Usage.WindowInputTokens >= 0 && s.Usage.InputTokens >= 0 && s.Usage.CacheReadTokens >= 0 && s.Usage.CacheWriteTokens >= 0 {
		result.Available, result.Measured = true, true
		result.InputTokens = s.Usage.Normalized().WindowInputTokens
	}
	if err := s.Validate(); err != nil {
		result.Limitation = "invalid_breakdown"
		result.CharacterLimitation = "invalid_attribution"
		return result, err
	}
	if s.PayloadFingerprint == "" || s.RequestID == "" || s.Provider == "" || s.Model == "" {
		result.CharacterLimitation = "request_identity_unavailable"
	} else if s.RequestEstimate.Characters == nil {
		result.CharacterLimitation = "characters_unavailable"
	} else if result.Measured {
		result.CharactersAvailable = true
		result.Characters = s.RequestEstimate.Characters.Clone()
	} else {
		result.CharacterLimitation = "input_measurement_unavailable"
	}
	b := s.RequestEstimate.Breakdown
	if b.Source != "prepared_content_v4" || b.Quality != "token_estimate" {
		result.Limitation = "unavailable_attribution"
		return result, nil
	}
	if s.PayloadFingerprint == "" || s.RequestID == "" || s.Provider == "" || s.Model == "" {
		result.Limitation = "request_identity_unavailable"
		return result, nil
	}
	total := b.FramingTokens + b.SystemTokens + b.UserTokens + b.AssistantTokens + b.ToolDefinitionTokens + b.ToolCallTokens + b.ToolResultTokens + b.ModelContextTokens + b.CompactionTokens + b.ReasoningTokens + b.AttachmentTokens
	if !result.Available {
		if b.AttributionCoverage != "complete" {
			result.Limitation = b.Limitation
			return result, nil
		}
		result.Available, result.InputTokens = true, total
	}
	if total > result.InputTokens {
		result.Limitation = "categories_exceed_total"
		return result, nil
	}
	result.BreakdownAvailable = true
	result.UnattributedTokens = result.InputTokens - total
	result.Limitation = b.Limitation
	return result, nil
}
