package config

import (
	"strings"
	"testing"
)

func TestReasoningCapabilityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		capability ReasoningCapability
		wantError  string
	}{
		{name: "zero", capability: ReasoningCapability{}},
		{name: "none", capability: ReasoningCapability{Kind: ReasoningKindNone}},
		{name: "effort", capability: ReasoningCapability{Kind: ReasoningKindEffort, SupportedLevels: []ReasoningLevel{ReasoningLevelLow, ReasoningLevelHigh}, DefaultLevel: ReasoningLevelHigh}},
		{name: "unknown kind", capability: ReasoningCapability{Kind: "typo"}, wantError: "unsupported reasoning capability kind"},
		{name: "negative minimum", capability: ReasoningCapability{Kind: ReasoningKindBudget, Budget: ReasoningBudget{MinTokens: -1}}, wantError: "cannot be negative"},
		{name: "negative maximum", capability: ReasoningCapability{Kind: ReasoningKindBudget, Budget: ReasoningBudget{MaxTokens: -1}}, wantError: "cannot be negative"},
		{name: "inverted budget", capability: ReasoningCapability{Kind: ReasoningKindBudget, Budget: ReasoningBudget{MinTokens: 20, MaxTokens: 10}}, wantError: "minimum exceeds maximum"},
		{name: "none with behavior", capability: ReasoningCapability{Kind: ReasoningKindNone, DisableSupported: true}, wantError: "cannot advertise reasoning behavior"},
		{name: "unsupported default", capability: ReasoningCapability{Kind: ReasoningKindEffort, SupportedLevels: []ReasoningLevel{ReasoningLevelLow}, DefaultLevel: ReasoningLevelHigh}, wantError: "default reasoning level"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.capability.Validate()
			if tt.wantError == "" && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if tt.wantError != "" && (err == nil || !strings.Contains(err.Error(), tt.wantError)) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestReasoningCapabilityNormalizePreservesInvalidBudgetForValidation(t *testing.T) {
	t.Parallel()

	capability := ReasoningCapability{Kind: " BUDGET ", Budget: ReasoningBudget{MinTokens: -1}}.Normalize()
	if capability.Kind != ReasoningKindBudget || capability.Budget.MinTokens != -1 {
		t.Fatalf("Normalize() = %#v, want normalized kind and preserved invalid budget", capability)
	}
}
