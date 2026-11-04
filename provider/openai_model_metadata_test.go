package provider_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
)

func TestParseOpenAIModelMetadata(t *testing.T) {
	raw := json.RawMessage(`{"id":"auto/agent","display_name":"Agent","context_window":922000,"context_length":922000,"max_input_tokens":922000,"max_output_tokens":128000,"modalities":{"input":["text","image"]},"tool_call":true,"supported_parameters":["tools","reasoning"],"reasoning":true,"reasoning_levels":["low","xhigh","max","ultra"],"supported_reasoning_levels":[{"effort":"low"},{"effort":"xhigh"},{"effort":"max"},{"effort":"ultra"}]}`)
	m, err := provider.ParseOpenAIModelMetadata(raw)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "auto/agent" || m.DisplayName != "Agent" || m.ContextWindow != 922000 || m.MaxInputTokens != 922000 || m.MaxOutputTokens != 128000 || !slices.Equal(m.InputModalities, []string{"text", "image"}) || m.SupportsTools == nil || !*m.SupportsTools {
		t.Fatalf("metadata = %+v", m)
	}
	if !m.Reasoning.SupportsLevel(config.ReasoningLevelUltra) || m.Reasoning.DisableSupported || m.Reasoning.DefaultLevel != "" {
		t.Fatalf("reasoning = %+v", m.Reasoning)
	}
	if err := m.Reasoning.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestParseOpenAIModelMetadataPreservesUnknownAndExplicitAbsence(t *testing.T) {
	m, err := provider.ParseOpenAIModelMetadata(json.RawMessage(`{"id":"plain-model"}`))
	if err != nil || m.SupportsTools != nil || m.InputModalities != nil || !m.Reasoning.IsZero() || m.ContextWindow != 0 {
		t.Fatalf("unknown metadata = %+v, %v", m, err)
	}
	m, err = provider.ParseOpenAIModelMetadata(json.RawMessage(`{"id":"text-model","tool_call":false,"reasoning":false,"modalities":{"input":["text"]}}`))
	if err != nil || m.SupportsTools == nil || *m.SupportsTools || m.Reasoning.Kind != config.ReasoningKindNone {
		t.Fatalf("explicit absence = %+v, %v", m, err)
	}
	m, err = provider.ParseOpenAIModelMetadata(json.RawMessage(`{"id":"reasoner","reasoning":true}`))
	if err != nil || !m.Reasoning.IsZero() {
		t.Fatalf("unqualified reasoning invented controls: %+v, %v", m, err)
	}
}

func TestParseOpenAIModelMetadataRejectsConflictingOrInvalidFacts(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"id":"a","context_window":-1}`, `{"id":"a","context_window":100,"context_length":200}`, `{"id":"a","tool_call":false,"supported_parameters":["tools"]}`, `{"id":"a","reasoning":false,"reasoning_levels":["high"]}`, `{"id":"a","reasoning_levels":["invented"]}`, `{"id":"a","reasoning_levels":["high"],"supported_reasoning_levels":[{"effort":"low"}]}`, `{"id":"a","max_output_tokens":"100"}`} {
		t.Run(raw, func(t *testing.T) {
			if _, err := provider.ParseOpenAIModelMetadata(json.RawMessage(raw)); err == nil {
				t.Fatal("invalid facts accepted")
			}
		})
	}
}

func TestParseOpenAIModelMetadataKeepsInputAndContextLimitsIndependent(t *testing.T) {
	m, err := provider.ParseOpenAIModelMetadata(json.RawMessage(`{"id":"agent","context_window":1050000,"max_input_tokens":922000,"max_output_tokens":128000}`))
	if err != nil || m.ContextWindow != 1050000 || m.MaxInputTokens != 922000 || m.MaxOutputTokens != 128000 {
		t.Fatalf("independent limits = %+v, %v", m, err)
	}
}
