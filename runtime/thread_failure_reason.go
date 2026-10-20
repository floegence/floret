package runtime

import (
	"strings"

	"github.com/floegence/floret/v7/internal/engine"
)

func classifyThreadTurnFailureReason(code ThreadTurnFailureCode, message string) ThreadTurnFailureReason {
	if code == ThreadTurnFailureProvider && message == engine.ErrDuplicateToolCallID.Error() {
		return ThreadTurnFailureDuplicateToolCallID
	}
	// These are the exact canonical forms emitted by released Engines, including
	// v7.69.1. Classification is a read projection, never a journal rewrite.
	if code == ThreadTurnFailureToolDispatch && message == "tool argument correction exhausted" {
		return ThreadTurnFailureToolArgumentCorrectionExhausted
	}
	if code == ThreadTurnFailureControlError {
		const prefix = "control argument correction exhausted: Tool "
		const suffix = ". Rewrite the input to match the schema."
		if body, ok := strings.CutPrefix(message, prefix); ok && strings.HasSuffix(body, suffix) {
			name, reason, found := strings.Cut(strings.TrimSuffix(body, suffix), " was called with invalid arguments: ")
			if found && name != "" && !strings.ContainsAny(name, " \t\r\n") && strings.TrimSpace(reason) != "" {
				return ThreadTurnFailureToolArgumentCorrectionExhausted
			}
		}
	}

	if code != ThreadTurnFailureEngineContract {
		return ""
	}
	switch message {
	case engine.ErrDuplicateTools.Error():
		return ThreadTurnFailureRepeatedToolCalls
	case engine.ErrNoProgress.Error():
		return ThreadTurnFailureNoProgress
	case engine.ErrDuplicateToolCallID.Error():
		return ThreadTurnFailureDuplicateToolCallID
	case engine.ErrStopHookLoop.Error():
		return ThreadTurnFailureStopHookLoop
	default:
		return ""
	}
}
