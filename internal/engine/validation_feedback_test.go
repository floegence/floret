package engine

import (
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
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
