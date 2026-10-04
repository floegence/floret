package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/floegence/floret/v7/config"
)

// ParseOllamaReasoningCapability resolves the thinking object from /api/show.
// Metadata is authoritative, independent of model names. Missing metadata and
// unknown named levels retain provider-managed behavior without invented controls.
// Default remains an omitted request parameter, not a frozen server default.
func ParseOllamaReasoningCapability(raw json.RawMessage, thinkingCapable bool) (config.ReasoningCapability, error) {
	none := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	managed := config.ReasoningCapability{Kind: config.ReasoningKindDynamic, DynamicModelValue: true}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		if thinkingCapable {
			return managed, nil
		}
		return none, nil
	}
	var metadata struct {
		Values  []json.RawMessage `json:"values"`
		Default json.RawMessage   `json:"default"`
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return none, fmt.Errorf("invalid Ollama thinking metadata: %w", err)
	}
	if len(metadata.Values) == 0 {
		return none, fmt.Errorf("Ollama thinking metadata has no values")
	}
	cap := config.ReasoningCapability{Kind: config.ReasoningKindEffort, DefaultLevel: config.ReasoningLevelDefault, DynamicModelValue: true}
	hasTrue, hasNamed, hasUnknown, defaultFound := false, false, false, len(metadata.Default) == 0
	for _, rawValue := range metadata.Values {
		value := bytes.TrimSpace(rawValue)
		if bytes.Equal(value, bytes.TrimSpace(metadata.Default)) {
			defaultFound = true
		}
		switch string(value) {
		case "false":
			cap.DisableSupported = true
		case "true":
			hasTrue = true
		default:
			var name string
			if err := json.Unmarshal(value, &name); err != nil || name == "" {
				return none, fmt.Errorf("Ollama thinking values must be booleans or nonempty names")
			}
			hasNamed = true
			level := config.ReasoningLevel(name)
			if !slices.Contains([]config.ReasoningLevel{config.ReasoningLevelMinimal, config.ReasoningLevelLow, config.ReasoningLevelMedium, config.ReasoningLevelHigh, config.ReasoningLevelXHigh, config.ReasoningLevelMax}, level) {
				hasUnknown = true
				continue
			}
			if !slices.Contains(cap.SupportedLevels, level) {
				cap.SupportedLevels = append(cap.SupportedLevels, level)
			}
		}
	}
	if !defaultFound {
		return none, fmt.Errorf("Ollama thinking default is absent from supported values")
	}
	if !hasNamed && !hasTrue {
		return none, nil
	}
	if !hasNamed && hasTrue {
		if !cap.DisableSupported {
			cap.Kind = config.ReasoningKindAlwaysOn
		} else {
			cap.Kind = config.ReasoningKindToggle
			cap.SupportedLevels = []config.ReasoningLevel{config.ReasoningLevelOn}
		}
	} else if len(cap.SupportedLevels) == 0 && hasUnknown {
		cap.Kind = config.ReasoningKindDynamic
	}
	// A mixed boolean/named declaration does not imply an effort name for true.
	// Only exact named values and an explicitly advertised false are controllable.
	return cap, nil
}

// OllamaReasoningEffort maps verified model intent to the OpenAI-compatible
// reasoning_effort field. An empty result must be omitted. Boolean On uses
// Ollama's documented medium compatibility alias; named levels stay exact.
func OllamaReasoningEffort(capability config.ReasoningCapability, selection config.ReasoningSelection) (string, error) {
	capability = capability.Normalize()
	selection = config.NormalizeReasoningSelection(selection)
	if err := capability.Validate(); err != nil {
		return "", err
	}
	if selection.BudgetTokens != 0 {
		return "", fmt.Errorf("Ollama thinking does not support token budgets")
	}
	if selection.Level == "" || selection.Level == config.ReasoningLevelDefault {
		return "", nil
	}
	if capability.Kind == config.ReasoningKindNone || !capability.SupportsLevel(selection.Level) {
		return "", fmt.Errorf("Ollama reasoning level %q is not declared by the model", selection.Level)
	}
	switch selection.Level {
	case config.ReasoningLevelOff:
		return "none", nil
	case config.ReasoningLevelOn:
		if capability.Kind != config.ReasoningKindToggle {
			return "", fmt.Errorf("Ollama explicit on requires boolean thinking metadata")
		}
		return "medium", nil
	default:
		return string(selection.Level), nil
	}
}
