package config_test

import (
	"testing"

	"github.com/floegence/floret/v2/config"
)

func TestAgentConfigRequiresExplicitPersonaAndPrompt(t *testing.T) {
	valid := config.AgentConfig{
		Profile:      config.AgentProfile{ID: "reviewer", Name: "Code reviewer"},
		SystemPrompt: "Review code with precise, actionable findings.",
		Context:      config.ContextPolicy{ContextWindowTokens: 32_000},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid AgentConfig: %v", err)
	}
	for name, candidate := range map[string]config.AgentConfig{
		"profile": {SystemPrompt: valid.SystemPrompt, Context: valid.Context},
		"prompt":  {Profile: valid.Profile, Context: valid.Context},
	} {
		t.Run(name, func(t *testing.T) {
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid AgentConfig passed validation")
			}
		})
	}
}
