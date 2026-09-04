package controlstate

import (
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestClassifyAskUserControlState(t *testing.T) {
	validPayload := map[string]any{
		"questions":   []any{map[string]any{"id": "city", "header": "City", "question": "Which city?", "is_secret": false, "response_mode": "write"}},
		"reason_code": "missing_external_input", "required_from_user": []any{"city"}, "evidence_refs": []any{},
	}
	tests := []struct {
		name   string
		signal *session.ControlSignalView
		want   Classification
	}{
		{name: "valid waiting input", signal: &session.ControlSignalView{Name: "ask_user", CallID: "ask", Disposition: "waiting", Payload: validPayload}, want: WaitingInput},
		{name: "missing questions", signal: &session.ControlSignalView{Name: "ask_user", Disposition: "waiting"}, want: Invalid},
		{name: "empty questions", signal: &session.ControlSignalView{Name: "ask_user", Disposition: "waiting", Payload: map[string]any{"questions": []any{}}}, want: Invalid},
		{name: "historical waiting error", signal: &session.ControlSignalView{Name: "ask_user", Disposition: "waiting", ErrorCode: session.ControlSignalErrorCodeControlError}, want: Failed},
		{name: "terminal failure", signal: &session.ControlSignalView{Name: "ask_user", Disposition: FailedDisposition, ErrorCode: session.ControlSignalErrorCodeControlError}, want: Failed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Classify(test.signal); got != test.want {
				t.Fatalf("classification=%d, want %d", got, test.want)
			}
		})
	}
}
