package config

import (
	"fmt"
	"slices"
	"strings"
)

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
	Kind              string           `json:"kind"`
	SupportedLevels   []ReasoningLevel `json:"supported_levels,omitempty"`
	DefaultLevel      ReasoningLevel   `json:"default_level,omitempty"`
	DisableSupported  bool             `json:"disable_supported,omitempty"`
	DefaultEnabled    *bool            `json:"default_enabled,omitempty"`
	Budget            ReasoningBudget  `json:"budget,omitempty"`
	DynamicModelValue bool             `json:"dynamic_model_value,omitempty"`
}

func (c ReasoningCapability) Normalize() ReasoningCapability {
	c.Kind = strings.TrimSpace(strings.ToLower(c.Kind))
	c.DefaultLevel = NormalizeReasoningSelection(ReasoningSelection{Level: c.DefaultLevel}).Level
	levels := make([]ReasoningLevel, 0, len(c.SupportedLevels))
	for _, level := range c.SupportedLevels {
		level = NormalizeReasoningSelection(ReasoningSelection{Level: level}).Level
		if level == "" || slices.Contains(levels, level) {
			continue
		}
		levels = append(levels, level)
	}
	c.SupportedLevels = levels
	return c
}

func (c ReasoningCapability) IsZero() bool {
	c = c.Normalize()
	return c.Kind == "" && len(c.SupportedLevels) == 0 && c.DefaultLevel == "" && !c.DisableSupported && c.DefaultEnabled == nil && c.Budget == (ReasoningBudget{}) && !c.DynamicModelValue
}

func (c ReasoningCapability) SupportsLevel(level ReasoningLevel) bool {
	c = c.Normalize()
	level = NormalizeReasoningSelection(ReasoningSelection{Level: level}).Level
	if level == "" || level == ReasoningLevelDefault {
		return true
	}
	if level == ReasoningLevelOff && c.DisableSupported {
		return true
	}
	return slices.Contains(c.SupportedLevels, level)
}

func (c ReasoningCapability) Validate() error {
	c = c.Normalize()
	if c.IsZero() {
		return nil
	}
	if c.Kind == "" {
		return fmt.Errorf("reasoning capability kind is required")
	}
	switch c.Kind {
	case ReasoningKindNone, ReasoningKindEffort, ReasoningKindToggle, ReasoningKindBudget, ReasoningKindToggleBudget, ReasoningKindEffortBudget, ReasoningKindAlwaysOn, ReasoningKindDynamic:
	default:
		return fmt.Errorf("unsupported reasoning capability kind %q", c.Kind)
	}
	for _, level := range c.SupportedLevels {
		if !ValidateReasoningLevel(level) || level == ReasoningLevelDefault {
			return fmt.Errorf("unsupported reasoning level %q", level)
		}
	}
	if c.DefaultLevel != "" && !c.SupportsLevel(c.DefaultLevel) {
		return fmt.Errorf("default reasoning level %q is unsupported", c.DefaultLevel)
	}
	if c.Budget.MinTokens < 0 || c.Budget.MaxTokens < 0 {
		return fmt.Errorf("reasoning budget cannot be negative")
	}
	if c.Budget.MinTokens > 0 && c.Budget.MaxTokens > 0 && c.Budget.MinTokens > c.Budget.MaxTokens {
		return fmt.Errorf("reasoning budget minimum exceeds maximum")
	}
	if c.Kind == ReasoningKindNone {
		if len(c.SupportedLevels) > 0 || c.DefaultLevel != "" || c.DisableSupported || c.DefaultEnabled != nil || c.Budget != (ReasoningBudget{}) || c.DynamicModelValue {
			return fmt.Errorf("reasoning capability kind none cannot advertise reasoning behavior")
		}
		return nil
	}
	return nil
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
