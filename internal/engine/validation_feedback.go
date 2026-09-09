package engine

import (
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

// Only provider arguments are correctable. Activity and control projectors run
// after this boundary; their failures are engine contracts, not model feedback.
func validateModelToolArguments(definitions []tools.ToolDefinition, call provider.ToolCall) error {
	for _, definition := range definitions {
		if definition.Name == call.Name {
			_, err := tools.Validate(definition.InputSchema, []byte(call.Args))
			return err
		}
	}
	return nil // Unavailable ordinary tools retain the tool runtime's error result.
}

func validationFeedbackMessage(call provider.ToolCall, err error) session.Message {
	return session.Message{
		Role: session.Tool, Kind: session.MessageKindToolValidationError,
		Content:    "ERROR: " + tools.InvalidArgumentsText(call.Name, err),
		ToolCallID: call.ID, ToolName: call.Name,
		ToolResult: &session.ToolResultView{Status: "error"},
	}
}
