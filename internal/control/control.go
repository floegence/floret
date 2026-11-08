package control

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/tools"
)

const (
	AskUserTool = "ask_user"
)

type SignalKind string

const (
	SignalNone    SignalKind = ""
	SignalAskUser SignalKind = "ask_user"
)

type Signal struct {
	Kind    SignalKind
	Prompt  string
	Output  string
	Payload map[string]any
}

func ToolDefinitions() []tools.ToolDefinition {
	return []tools.ToolDefinition{{
		Name:        AskUserTool,
		Title:       "Ask user",
		Description: "Ask and wait when execution needs user input or guided questions are requested. Ordinary discussion and optional feedback may use prose.",
		InputSchema: askUserInputSchema(),
		Strict:      true,
		Annotations: map[string]any{
			"kind":      "control",
			"source":    "floret",
			"namespace": "control",
		},
	}}
}

func IsControlTool(name string) bool {
	name = strings.TrimSpace(name)
	return name == AskUserTool
}

func Project(call provider.ToolCall) (Signal, bool, error) {
	switch strings.TrimSpace(call.Name) {
	case AskUserTool:
		payload, err := ValidateAskUserToolArgs(askUserInputSchema(), call.Args)
		if err != nil {
			return Signal{}, true, fmt.Errorf("%s", tools.InvalidArgumentsText(AskUserTool, err))
		}
		question := firstNonEmptyString(controlString(payload["question"]), firstQuestionText(payload["questions"]))
		if question == "" {
			return Signal{}, true, fmt.Errorf("question or questions[0].question is required")
		}
		return Signal{Kind: SignalAskUser, Prompt: question, Payload: cloneControlMap(payload)}, true, nil
	default:
		return Signal{}, false, nil
	}
}

func AskUser(calls []provider.ToolCall) (Signal, bool) {
	for _, call := range calls {
		if signal, ok, err := Project(call); ok && err == nil && signal.Kind == SignalAskUser {
			return signal, true
		}
	}
	return Signal{}, false
}

func askUserInputSchema() map[string]any {
	choiceItem := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"choice_id":   map[string]any{"type": "string", "minLength": 1, "maxLength": 64},
			"label":       map[string]any{"type": "string", "minLength": 1, "maxLength": 200},
			"description": map[string]any{"type": "string", "maxLength": 240},
		},
		"required":             []string{"choice_id", "label"},
		"additionalProperties": false,
	}
	questionItem := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":                map[string]any{"type": "string", "minLength": 1, "maxLength": 80},
			"header":            map[string]any{"type": "string", "minLength": 1, "maxLength": 120},
			"question":          map[string]any{"type": "string", "minLength": 1, "maxLength": 400},
			"is_secret":         map[string]any{"type": "boolean"},
			"response_mode":     map[string]any{"type": "string", "enum": []string{"write", "select_or_write"}, "description": "Use write for a direct text answer. Use select_or_write for fixed choices plus a separate custom-text answer."},
			"write_label":       map[string]any{"type": "string", "maxLength": 200, "description": "Label for the direct or custom-text answer."},
			"write_placeholder": map[string]any{"type": "string", "maxLength": 160},
			"choices": map[string]any{
				"type": "array", "maxItems": 4, "items": choiceItem,
			},
		},
		"required":             []string{"id", "header", "question", "is_secret", "response_mode"},
		"additionalProperties": false,
	}
	return tools.StrictObject(map[string]any{
		"questions": map[string]any{
			"type": "array", "minItems": 1, "maxItems": maxAskUserQuestions, "items": questionItem,
		},
		"evidence_refs": map[string]any{
			"type": "array", "maxItems": 12, "items": map[string]any{"type": "string", "maxLength": 120},
			"description": "Relevant existing tool-call IDs, or []. Never invent IDs; references do not grant permission.",
		},
	}, []string{"questions", "evidence_refs"})
}

// ValidateAskUserToolArgs accepts the compact model contract and the released v7
// fields retained by pending interactions and frozen Turn tool surfaces. These
// deprecated fields remain validated and preserved through v7; only a future
// major release may remove their admission. They are not advertised to models.
func ValidateAskUserToolArgs(definition map[string]any, args string) (map[string]any, error) {
	schema, err := tools.NormalizeInputSchema(definition)
	if err != nil {
		return nil, err
	}
	properties := schema["properties"].(map[string]any)
	// Extend only the canonical question shape. Custom controls keep their own
	// schema, and frozen released definitions retain their original constraints.
	questions, _ := properties["questions"].(map[string]any)
	questionItem, _ := questions["items"].(map[string]any)
	question, _ := questionItem["properties"].(map[string]any)
	choices, _ := question["choices"].(map[string]any)
	choiceItem, _ := choices["items"].(map[string]any)
	choice, _ := choiceItem["properties"].(map[string]any)
	if question != nil && choice != nil && question["response_mode"] != nil && properties["evidence_refs"] != nil {
		add := func(properties map[string]any, key string, value map[string]any) {
			if _, present := properties[key]; !present {
				properties[key] = value
			}
		}
		add(properties, "reason_code", tools.Enum("user_decision_required", "permission_blocked", "missing_external_input", "conflicting_constraints", "safety_confirmation"))
		add(properties, "required_from_user", map[string]any{"type": "array", "minItems": 1, "maxItems": 8, "items": map[string]any{"type": "string", "maxLength": 200}})
		add(question, "choices_exhaustive", map[string]any{"type": "boolean"})
		add(choice, "kind", tools.Enum("select"))
	}
	payload, err := tools.Validate(schema, []byte(strings.TrimSpace(args)))
	if err == nil {
		return payload, nil
	}
	// A frozen definition may already accept select. Only normalize after its
	// original contract rejects the call, preserving released admission.
	if normalized := NormalizeAskUserToolArgs(args); normalized != args {
		return tools.Validate(schema, []byte(strings.TrimSpace(normalized)))
	}
	return nil, err
}

const maxAskUserQuestions = 5

// NormalizeAskUserToolArgs canonicalizes the retired ask_user "select" mode.
// Invalid JSON is left untouched so the strict schema validator reports it.
func NormalizeAskUserToolArgs(args string) string {
	var payload map[string]any
	if err := json.Unmarshal([]byte(args), &payload); err != nil || payload == nil {
		return args
	}
	normalizedPayload, changed := NormalizeAskUserPayload(payload)
	if !changed {
		return args
	}
	normalized, err := json.Marshal(normalizedPayload)
	if err != nil {
		return args
	}
	return string(normalized)
}

// NormalizeAskUserPayload returns a detached projection for legacy ask_user
// questions without modifying the source payload. The schema bounds traversal.
func NormalizeAskUserPayload(payload map[string]any) (map[string]any, bool) {
	questions, ok := payload["questions"].([]any)
	if !ok || len(questions) > maxAskUserQuestions {
		return payload, false
	}
	var normalizedQuestions []any
	for index, raw := range questions {
		question, ok := raw.(map[string]any)
		if !ok || question["response_mode"] != "select" {
			continue
		}
		if normalizedQuestions == nil {
			normalizedQuestions = append([]any(nil), questions...)
		}
		normalizedQuestion := make(map[string]any, len(question)+1)
		for key, value := range question {
			normalizedQuestion[key] = value
		}
		normalizedQuestion["response_mode"] = "select_or_write"
		normalizedQuestion["choices_exhaustive"] = false
		normalizedQuestions[index] = normalizedQuestion
	}
	if normalizedQuestions == nil {
		return payload, false
	}
	normalizedPayload := make(map[string]any, len(payload))
	for key, value := range payload {
		normalizedPayload[key] = value
	}
	normalizedPayload["questions"] = normalizedQuestions
	return normalizedPayload, true
}

func controlString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func firstQuestionText(value any) string {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return ""
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		return ""
	}
	return controlString(item["question"])
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func cloneControlMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneControlAny(value)
	}
	return out
}

func cloneControlAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneControlMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneControlAny(item)
		}
		return out
	default:
		return value
	}
}
