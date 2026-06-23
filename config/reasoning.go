package config

import "strings"

// ReasoningLevel is a provider-neutral model reasoning intent. Provider
// adapters translate it into the model-specific wire shape advertised by the
// selected model capability.
type ReasoningLevel string

const (
	ReasoningLevelDefault ReasoningLevel = "default"
	ReasoningLevelOff     ReasoningLevel = "off"
	ReasoningLevelMinimal ReasoningLevel = "minimal"
	ReasoningLevelLow     ReasoningLevel = "low"
	ReasoningLevelMedium  ReasoningLevel = "medium"
	ReasoningLevelHigh    ReasoningLevel = "high"
	ReasoningLevelXHigh   ReasoningLevel = "xhigh"
	ReasoningLevelMax     ReasoningLevel = "max"
)

type ReasoningSelection struct {
	Level        ReasoningLevel `json:"level,omitempty"`
	BudgetTokens int64          `json:"budget_tokens,omitempty"`
}

type ReasoningBudget struct {
	MinTokens int64 `json:"min_tokens,omitempty"`
	MaxTokens int64 `json:"max_tokens,omitempty"`
}

type ReasoningCapability struct {
	Kind              string           `json:"kind"`
	SupportedLevels   []ReasoningLevel `json:"supported_levels,omitempty"`
	DefaultLevel      ReasoningLevel   `json:"default_level,omitempty"`
	DisableSupported  bool             `json:"disable_supported,omitempty"`
	DefaultEnabled    *bool            `json:"default_enabled,omitempty"`
	Budget            ReasoningBudget  `json:"budget,omitempty"`
	DynamicModelValue bool             `json:"dynamic_model_value,omitempty"`
}

func (s ReasoningSelection) IsZero() bool {
	return strings.TrimSpace(string(s.Level)) == "" && s.BudgetTokens == 0
}

func NormalizeReasoningSelection(s ReasoningSelection) ReasoningSelection {
	s.Level = ReasoningLevel(strings.TrimSpace(strings.ToLower(string(s.Level))))
	if s.Level == "none" {
		s.Level = ReasoningLevelOff
	}
	if s.BudgetTokens < 0 {
		s.BudgetTokens = 0
	}
	return s
}

func ValidateReasoningLevel(level ReasoningLevel) bool {
	switch NormalizeReasoningSelection(ReasoningSelection{Level: level}).Level {
	case "", ReasoningLevelDefault, ReasoningLevelOff, ReasoningLevelMinimal, ReasoningLevelLow, ReasoningLevelMedium, ReasoningLevelHigh, ReasoningLevelXHigh, ReasoningLevelMax:
		return true
	default:
		return false
	}
}
