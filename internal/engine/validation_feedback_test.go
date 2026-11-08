package engine

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/control"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestValidationFeedbackExplainsExecutionAndCorrectionOwnership(t *testing.T) {
	message := validationFeedbackMessage(provider.ToolCall{ID: "invalid", Name: "write"}, errors.New("missing path"))
	if message.Kind != session.MessageKindToolValidationError || message.ToolCallID != "invalid" {
		t.Fatalf("validation identity lost: %+v", message)
	}
	for _, want := range []string{"not executed", "Rewrite the input to match the schema", "runtime limits correction attempts"} {
		if !strings.Contains(message.Content, want) {
			t.Errorf("feedback missing %q: %s", want, message.Content)
		}
	}
}

func TestInputValidationPreservesFrozenSchemaAndRejectsInvalidReleasedFields(t *testing.T) {
	const compact = `{"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Which route?","response_mode":"write","is_secret":false}]}`
	for _, name := range []string{"ask_user", "user_ask"} {
		definitions := control.ToolDefinitions()
		definitions[0].Name = name
		before, _ := json.Marshal(definitions)
		for _, args := range []string{compact, strings.Replace(compact, `"evidence_refs":[]`, `"reason_code":"missing_external_input","required_from_user":["route"],"evidence_refs":[]`, 1)} {
			if err := validateModelToolArguments(definitions, provider.ToolCall{Name: name, Args: args}, name); err != nil {
				t.Fatal(err)
			}
		}
		after, _ := json.Marshal(definitions)
		if string(before) != string(after) {
			t.Fatal("compatibility validation mutated advertised schema")
		}
		invalid := strings.Replace(compact, `"evidence_refs":[]`, `"reason_code":false,"evidence_refs":[]`, 1)
		if err := validateModelToolArguments(definitions, provider.ToolCall{Name: name, Args: invalid}, name); err == nil {
			t.Fatal("invalid released field bypassed correction")
		}
	}
	// A host-defined control named ask_user must keep its declared constraints.
	custom := []tools.ToolDefinition{{Name: "ask_user", InputSchema: tools.StrictObject(map[string]any{"custom": tools.String("")}, []string{"custom"})}}
	if err := validateModelToolArguments(custom, provider.ToolCall{Name: "ask_user", Args: compact}, ""); err == nil {
		t.Fatal("custom control schema was replaced")
	}
	if err := validateModelToolArguments(custom, provider.ToolCall{Name: "ask_user", Args: `{"custom":"value"}`}, ""); err != nil {
		t.Fatal(err)
	}
	frozen := control.ToolDefinitions()
	frozen[0].InputSchema["properties"].(map[string]any)["reason_code"] = tools.Enum("missing_external_input")
	frozen[0].InputSchema["required"] = []string{"questions", "evidence_refs", "reason_code"}
	if err := validateModelToolArguments(frozen, provider.ToolCall{Name: "ask_user", Args: compact}); err == nil {
		t.Fatal("frozen required field was weakened")
	}
}
