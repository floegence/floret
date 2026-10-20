package runtime

import "testing"

func TestToolArgumentCorrectionFailureReasonExactReleasedForms(t *testing.T) {
	control := "control argument correction exhausted: Tool ask_user was called with invalid arguments: missing question. Rewrite the input to match the schema."
	for _, tc := range []struct {
		code       ThreadTurnFailureCode
		message    string
		recognized bool
	}{
		{ThreadTurnFailureToolDispatch, "tool argument correction exhausted", true},
		{ThreadTurnFailureControlError, control, true},
		{ThreadTurnFailureProvider, "tool argument correction exhausted", false},
		{ThreadTurnFailureEngineContract, "tool argument correction exhausted", false},
		{ThreadTurnFailureToolDispatch, "upstream: tool argument correction exhausted", false},
		{ThreadTurnFailureToolDispatch, "tool argument correction exhausted: details", false},
		{ThreadTurnFailureControlError, "control argument correction exhausted", false},
		{ThreadTurnFailureControlError, "control argument correction exhausted: Tool  was called with invalid arguments: missing question. Rewrite the input to match the schema.", false},
		{ThreadTurnFailureControlError, "control argument correction exhausted: Tool ask user was called with invalid arguments: missing question. Rewrite the input to match the schema.", false},
		{ThreadTurnFailureControlError, "control argument correction exhausted: Tool ask_user was called with invalid arguments: . Rewrite the input to match the schema.", false},
		{ThreadTurnFailureControlError, control + " extra", false},
	} {
		original := &ThreadTurnFailure{Code: tc.code, Message: tc.message}
		projected := cloneThreadTurnFailure(original)
		want := ThreadTurnFailureReason("")
		if tc.recognized {
			want = ThreadTurnFailureToolArgumentCorrectionExhausted
		}
		if projected.Reason != want || original.Reason != "" {
			t.Fatalf("%s %q: %+v", tc.code, tc.message, projected)
		}
		if err := projected.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
