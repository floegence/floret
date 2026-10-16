package runtime

import "github.com/floegence/floret/v7/internal/engine"

func classifyThreadTurnFailureReason(code ThreadTurnFailureCode, message string) ThreadTurnFailureReason {
	if code == ThreadTurnFailureProvider && message == engine.ErrDuplicateToolCallID.Error() {
		return ThreadTurnFailureDuplicateToolCallID
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
