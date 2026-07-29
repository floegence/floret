package config

import (
	"errors"
	"strings"
)

// AgentConfig contains only the provider-neutral persona and model-behavior
// policy of an Agent. Transport credentials and provider selection do not
// belong in this contract.
type AgentConfig struct {
	Profile      AgentProfile       `json:"profile"`
	SystemPrompt string             `json:"system_prompt"`
	Context      ContextPolicy      `json:"context"`
	Reasoning    ReasoningSelection `json:"reasoning,omitempty"`
}

// Validate verifies that persona and prompt identity are explicit and that
// the provider-neutral policies are internally coherent.
func (configuration AgentConfig) Validate() error {
	if strings.TrimSpace(configuration.Profile.ID) == "" || strings.TrimSpace(configuration.Profile.Name) == "" {
		return errors.New("agent profile requires id and name")
	}
	if strings.TrimSpace(configuration.SystemPrompt) == "" {
		return errors.New("agent system prompt is required")
	}
	if configuration.Context.ContextWindowTokens <= 0 {
		return errors.New("agent context window must be positive")
	}
	if !ValidateReasoningLevel(configuration.Reasoning.Level) {
		return errors.New("agent reasoning level is invalid")
	}
	if configuration.Reasoning.BudgetTokens < 0 {
		return errors.New("agent reasoning budget cannot be negative")
	}
	return nil
}
