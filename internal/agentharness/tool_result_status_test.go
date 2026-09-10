package agentharness

import (
	"testing"

	"github.com/floegence/floret/v7/internal/event"
)

func TestToolResultEventPreservesConfirmedCancellation(t *testing.T) {
	for _, test := range []struct{ name, status, err, want string }{
		{"confirmed cancellation", "canceled", "", "canceled"},
		{"real error during stop", "canceled", "command failed", "error"},
		{"error status", "error", "", "error"},
		{"success", "success", "", "success"},
	} {
		t.Run(test.name, func(t *testing.T) {
			view := toolResultViewFromEvent(event.Event{Err: test.err, Metadata: map[string]any{"tool_result_status": test.status}})
			if view == nil || view.Status != test.want {
				t.Fatalf("result view = %#v, want %s", view, test.want)
			}
		})
	}
}
