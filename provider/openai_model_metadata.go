package provider

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/config"
)

// OpenAIModelMetadata contains optional serving facts from an OpenAI-compatible
// model inventory. Zero limits and nil collections/pointers mean unknown.
// ContextWindow and MaxInputTokens remain independent; neither includes an
// invented output allowance. Hosts own discovery, selection and overrides.
type OpenAIModelMetadata struct {
	ID              string
	DisplayName     string
	ContextWindow   int64
	MaxInputTokens  int64
	MaxOutputTokens int64
	InputModalities []string
	SupportsTools   *bool
	Reasoning       config.ReasoningCapability
}

// ParseOpenAIModelMetadata reads one model object, including explicit gateway
// extensions for serving limits, input modalities, tools and reasoning efforts.
// ID-only inventories remain valid. Missing facts do not imply unsupported
// capabilities. Contradictory declarations fail instead of choosing a source.
// Effort controls never follow from a model name or a reasoning boolean alone.
func ParseOpenAIModelMetadata(raw json.RawMessage) (OpenAIModelMetadata, error) {
	var in struct {
		ID            string `json:"id"`
		DisplayName   string `json:"display_name"`
		ContextWindow *int64 `json:"context_window"`
		ContextLength *int64 `json:"context_length"`
		MaxInput      *int64 `json:"max_input_tokens"`
		MaxOutput     *int64 `json:"max_output_tokens"`
		Modalities    *struct {
			Input []string `json:"input"`
		} `json:"modalities"`
		Tools           *bool                   `json:"tool_call"`
		Parameters      []string                `json:"supported_parameters"`
		Reasoning       *bool                   `json:"reasoning"`
		Levels          []config.ReasoningLevel `json:"reasoning_levels"`
		SupportedLevels []struct {
			Effort config.ReasoningLevel `json:"effort"`
		} `json:"supported_reasoning_levels"`
	}
	fail := func(reason string) (OpenAIModelMetadata, error) {
		return OpenAIModelMetadata{}, fmt.Errorf("invalid OpenAI model metadata: %s", reason)
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return fail("malformed model object")
	}
	if strings.TrimSpace(in.ID) == "" || strings.TrimSpace(in.ID) != in.ID || strings.ContainsAny(in.ID, "\x00\r\n") {
		return fail("invalid model identity")
	}
	out := OpenAIModelMetadata{ID: in.ID, DisplayName: in.DisplayName, SupportsTools: in.Tools}
	for _, limit := range []*int64{in.ContextWindow, in.ContextLength, in.MaxInput, in.MaxOutput} {
		if limit != nil && *limit < 0 {
			return fail("negative token limit")
		}
	}
	if in.ContextWindow != nil {
		out.ContextWindow = *in.ContextWindow
	}
	if in.ContextLength != nil {
		if out.ContextWindow > 0 && *in.ContextLength > 0 && out.ContextWindow != *in.ContextLength {
			return fail("conflicting context limits")
		}
		if out.ContextWindow == 0 {
			out.ContextWindow = *in.ContextLength
		}
	}
	if in.MaxInput != nil {
		out.MaxInputTokens = *in.MaxInput
	}
	if in.MaxOutput != nil {
		out.MaxOutputTokens = *in.MaxOutput
	}
	if in.Modalities != nil && in.Modalities.Input != nil {
		out.InputModalities = []string{}
		for _, modality := range in.Modalities.Input {
			if modality == "" || strings.TrimSpace(modality) != modality || slices.Contains(out.InputModalities, modality) {
				return fail("invalid input modalities")
			}
			out.InputModalities = append(out.InputModalities, modality)
		}
	}
	if in.Parameters != nil {
		tools := slices.Contains(in.Parameters, "tools")
		if in.Tools != nil && *in.Tools != tools {
			return fail("conflicting tool capability")
		}
		out.SupportsTools = &tools
	}
	levels := slices.Clone(in.Levels)
	if in.SupportedLevels != nil {
		declared := make([]config.ReasoningLevel, 0, len(in.SupportedLevels))
		for _, item := range in.SupportedLevels {
			declared = append(declared, item.Effort)
		}
		if in.Levels != nil {
			a, b := slices.Clone(levels), slices.Clone(declared)
			slices.Sort(a)
			slices.Sort(b)
			if !slices.Equal(a, b) {
				return fail("conflicting reasoning efforts")
			}
		}
		levels = declared
	}
	if in.Reasoning != nil && !*in.Reasoning {
		if len(levels) > 0 {
			return fail("conflicting reasoning capability")
		}
		out.Reasoning.Kind = config.ReasoningKindNone
	} else if len(levels) > 0 {
		out.Reasoning.Kind = config.ReasoningKindEffort
		for _, level := range levels {
			if string(level) != strings.ToLower(strings.TrimSpace(string(level))) || !config.ValidateReasoningLevel(level) || level == "" || level == config.ReasoningLevelDefault || level == config.ReasoningLevelOn || slices.Contains(out.Reasoning.SupportedLevels, level) {
				return fail("invalid reasoning effort")
			}
			out.Reasoning.SupportedLevels = append(out.Reasoning.SupportedLevels, level)
			if level == config.ReasoningLevelOff {
				out.Reasoning.DisableSupported = true
			}
		}
	}
	return out, nil
}
