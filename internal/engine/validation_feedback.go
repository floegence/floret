package engine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/internal/control"
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
			if err != nil && control.IsControlTool(strings.TrimSpace(call.Name)) {
				if normalized := control.NormalizeAskUserToolArgs(call.Args); normalized != call.Args {
					_, err = tools.Validate(definition.InputSchema, []byte(normalized))
				}
			}
			return err
		}
	}
	return nil // Unavailable ordinary tools retain the tool runtime's error result.
}

func validationFeedbackMessage(call provider.ToolCall, err error) session.Message {
	return session.Message{
		Role: session.Tool, Kind: session.MessageKindToolValidationError,
		Content:    "ERROR: Tool call not executed. " + tools.InvalidArgumentsText(call.Name, err) + " The runtime limits correction attempts for consecutive invalid tool batches. Do not repeat other calls whose results have already been received.",
		ToolCallID: call.ID, ToolName: call.Name,
		ToolResult: &session.ToolResultView{Status: "error"},
	}
}

// validationCorrectionEpisode belongs to one Run. Only a fully settled,
// validation-clean tool batch ends an episode; provider continuations do not.
type validationCorrectionEpisode struct{ invalidBatches int }

const validationCorrectionLimit = 2
const toolArgumentCorrectionExhausted = "tool argument correction exhausted"
const controlArgumentCorrectionExhaustedPrefix = "control argument correction exhausted: "

func (c *validationCorrectionEpisode) observeInvalidBatch(errorCount int) map[string]any {
	if errorCount == 0 {
		return nil
	}
	c.invalidBatches++
	return map[string]any{
		"validation_correction_attempt": c.invalidBatches,
		"validation_correction_limit":   validationCorrectionLimit,
		"validation_error_count":        errorCount,
	}
}

// Settle only after every result is committed. A valid sibling cannot close
// an invalid batch, and exhaustion cannot discard a confirmed sibling result.
func (c *validationCorrectionEpisode) settleBatch(validationErrors map[string]error, first provider.ToolCall, controlOnly bool) error {
	if len(validationErrors) == 0 {
		c.invalidBatches = 0
		return nil
	}
	if c.invalidBatches <= validationCorrectionLimit {
		return nil
	}
	if controlOnly {
		return withFailureOrigin(fmt.Errorf("%s%s", controlArgumentCorrectionExhaustedPrefix, tools.InvalidArgumentsText(first.Name, validationErrors[first.ID])), FailureOriginControl)
	}
	return withFailureOrigin(errors.New(toolArgumentCorrectionExhausted), FailureOriginToolDispatch)
}
