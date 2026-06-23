package provider

import (
	"fmt"
	"slices"
	"strings"
)

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

const (
	ReasoningKindNone         = "none"
	ReasoningKindEffort       = "effort"
	ReasoningKindToggle       = "toggle"
	ReasoningKindBudget       = "budget"
	ReasoningKindToggleBudget = "toggle_budget"
	ReasoningKindEffortBudget = "effort_budget"
	ReasoningKindAlwaysOn     = "always_on"
	ReasoningKindDynamic      = "provider_dynamic"
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
	Kind                      string           `json:"kind,omitempty"`
	SupportedLevels           []ReasoningLevel `json:"supported_levels,omitempty"`
	DefaultLevel              ReasoningLevel   `json:"default_level,omitempty"`
	DisableSupported          bool             `json:"disable_supported,omitempty"`
	DefaultEnabled            *bool            `json:"default_enabled,omitempty"`
	Budget                    ReasoningBudget  `json:"budget,omitempty"`
	DynamicProviderMetadata   bool             `json:"dynamic_provider_metadata,omitempty"`
	WireShape                 string           `json:"wire_shape,omitempty"`
	DisableShape              string           `json:"disable_shape,omitempty"`
	BudgetShape               string           `json:"budget_shape,omitempty"`
	ResponseReasoningFields   []string         `json:"response_reasoning_fields,omitempty"`
	HistoryReplayRequirements []string         `json:"history_replay_requirements,omitempty"`
	SourceURLs                []string         `json:"source_urls,omitempty"`
	SourceCheckedAt           string           `json:"source_checked_at,omitempty"`
	Fixture                   string           `json:"fixture,omitempty"`
}

func (s ReasoningSelection) IsZero() bool {
	return strings.TrimSpace(string(s.Level)) == "" && s.BudgetTokens == 0
}

func NormalizeReasoningSelection(s ReasoningSelection) ReasoningSelection {
	s.Level = NormalizeReasoningLevel(s.Level)
	if s.BudgetTokens < 0 {
		s.BudgetTokens = 0
	}
	return s
}

func NormalizeReasoningLevel(level ReasoningLevel) ReasoningLevel {
	value := ReasoningLevel(strings.TrimSpace(strings.ToLower(string(level))))
	if value == "none" {
		return ReasoningLevelOff
	}
	return value
}

func (c ReasoningCapability) SupportsLevel(level ReasoningLevel) bool {
	level = NormalizeReasoningLevel(level)
	if level == "" || level == ReasoningLevelDefault {
		return true
	}
	if level == ReasoningLevelOff {
		if c.DisableSupported {
			return true
		}
		return slices.Contains(c.SupportedLevels, ReasoningLevelOff)
	}
	return slices.Contains(c.SupportedLevels, level)
}

func (c ReasoningCapability) SupportsBudget() bool {
	switch c.Kind {
	case ReasoningKindBudget, ReasoningKindToggleBudget, ReasoningKindEffortBudget:
		return true
	}
	return strings.TrimSpace(c.BudgetShape) != "" || c.Budget.MinTokens > 0 || c.Budget.MaxTokens > 0
}

func (c ReasoningCapability) ValidateSelection(selection ReasoningSelection) error {
	selection = NormalizeReasoningSelection(selection)
	if selection.IsZero() {
		return nil
	}
	if c.Kind == "" || c.Kind == ReasoningKindNone {
		return fmt.Errorf("model does not support reasoning selection")
	}
	if selection.Level != ReasoningLevelDefault && !c.SupportsLevel(selection.Level) {
		return fmt.Errorf("reasoning level %q is unsupported by model capability", selection.Level)
	}
	if selection.BudgetTokens > 0 {
		if !c.SupportsBudget() {
			return fmt.Errorf("reasoning budget is unsupported by model capability")
		}
		if c.Budget.MinTokens > 0 && selection.BudgetTokens < c.Budget.MinTokens {
			return fmt.Errorf("reasoning budget %d is below model minimum %d", selection.BudgetTokens, c.Budget.MinTokens)
		}
		if c.Budget.MaxTokens > 0 && selection.BudgetTokens > c.Budget.MaxTokens {
			return fmt.Errorf("reasoning budget %d exceeds model maximum %d", selection.BudgetTokens, c.Budget.MaxTokens)
		}
	}
	return nil
}

func ShortRequestReasoningSelection(capability ReasoningCapability) ReasoningSelection {
	for _, level := range []ReasoningLevel{ReasoningLevelOff, ReasoningLevelMinimal, ReasoningLevelLow} {
		if capability.SupportsLevel(level) {
			return ReasoningSelection{Level: level}
		}
	}
	return ReasoningSelection{}
}
