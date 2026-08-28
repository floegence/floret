package activityview

import (
	"testing"

	"github.com/floegence/floret/v5/tools"
)

func TestWithTerminalStatusPreservesSubAgentOperation(t *testing.T) {
	t.Parallel()

	got := WithTerminalStatus(&tools.ActivityPresentation{
		Renderer: tools.ActivityRendererSubAgentOperation,
		Payload: tools.SubAgentOperationActivityPayload{
			Action:  tools.SubAgentOperationWait,
			Status:  "running",
			Targets: []tools.SubAgentOperationTarget{{ThreadID: "thread-1", TaskName: "research"}},
		},
	}, "error", "wait failed")
	payload, ok := got.Payload.(tools.SubAgentOperationActivityPayload)
	if !ok || got.Renderer != tools.ActivityRendererSubAgentOperation || payload.Action != tools.SubAgentOperationWait || payload.Status != "error" || payload.Error == nil || payload.Error.Message != "wait failed" {
		t.Fatalf("terminal activity = %#v", got)
	}
}
