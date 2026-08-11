package agentharness

import (
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessiontree"
)

func TestSubAgentDetailObservationUsesCanonicalControlDisposition(t *testing.T) {
	t.Parallel()
	for _, disposition := range []string{"waiting", "terminal", "continue"} {
		detail := SubAgentDetailEvent{
			Ordinal: 1, ThreadID: "thread-control", TurnID: "turn-control",
			Kind:    SubAgentDetailEventToolCall,
			Message: &SubAgentDetailMessage{Kind: string(session.MessageKindControlSignal)},
			ToolCall: &SubAgentDetailToolCall{
				ID: "call-control", Name: "control",
				ControlSignal: &SubAgentDetailControlSignal{Disposition: disposition},
			},
		}
		observed, ok := subAgentDetailObservationEvent(detail, sessiontree.Entry{CreatedAt: time.UnixMilli(1_786_449_220_000).UTC()}, subAgentDetailActivityContext{})
		if !ok || observed.Metadata == nil || observed.Metadata["control_disposition"] != disposition {
			t.Fatalf("disposition %q projected as %#v, ok=%t", disposition, observed.Metadata, ok)
		}
	}
}
