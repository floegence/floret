package tools_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v6/identity"
	"github.com/floegence/floret/v6/tools"
)

func TestSubAgentOperationActivityRoundTripMergeAndFinalize(t *testing.T) {
	t.Parallel()

	initial := &tools.ActivityPresentation{
		Renderer: tools.ActivityRendererSubAgentOperation,
		Payload: tools.SubAgentOperationActivityPayload{
			Action: tools.SubAgentOperationWait, Status: "running", RequestedCount: 3,
			Targets: []tools.SubAgentOperationTarget{
				{ThreadID: identity.ThreadID("thread-big-tech"), TaskName: "Big Tech AI News"},
				{ThreadID: identity.ThreadID("thread-models"), TaskName: "AI Models and Products"},
				{ThreadID: identity.ThreadID("thread-policy"), TaskName: "AI Policy and Business"},
			},
		},
	}
	terminal := &tools.ActivityPresentation{
		Renderer: tools.ActivityRendererSubAgentOperation,
		Payload: tools.SubAgentOperationActivityPayload{
			Action: tools.SubAgentOperationWait, Status: "success", RequestedCount: 3, CompletedCount: 2,
			Targets: []tools.SubAgentOperationTarget{
				{ThreadID: identity.ThreadID("thread-big-tech"), TaskName: "Big Tech AI News", Status: "completed"},
				{ThreadID: identity.ThreadID("thread-models"), TaskName: "AI Models and Products", Status: "completed"},
				{ThreadID: identity.ThreadID("thread-policy"), TaskName: "AI Policy and Business", Status: "running"},
			},
		},
	}

	merged := tools.MergeActivityPresentations(initial, terminal)
	payload, ok := merged.Payload.(tools.SubAgentOperationActivityPayload)
	if !ok || payload.Action != tools.SubAgentOperationWait || payload.Status != "success" || payload.RequestedCount != 3 || payload.CompletedCount != 2 || len(payload.Targets) != 3 {
		t.Fatalf("merged payload = %#v", merged.Payload)
	}

	encoded, err := json.Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	var decoded tools.ActivityPresentation
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(merged, &decoded) {
		t.Fatalf("round trip:\nwant %#v\ngot  %#v", merged, &decoded)
	}

	clone := tools.CloneActivityPresentation(merged)
	clonePayload := clone.Payload.(tools.SubAgentOperationActivityPayload)
	clonePayload.Targets[0].TaskName = "changed"
	clonePayload.Error = &tools.ActivityError{Message: "changed"}
	if merged.Payload.(tools.SubAgentOperationActivityPayload).Targets[0].TaskName != "Big Tech AI News" {
		t.Fatal("clone shares subagent operation targets")
	}

	finalized := tools.FinalizeActivityPresentation(merged, "completed")
	if status, ok := tools.ActivityStatus(finalized); !ok || status != "completed" {
		t.Fatalf("final status = %q, %t", status, ok)
	}
}

func TestSubAgentOperationActivityAcceptsEveryAction(t *testing.T) {
	t.Parallel()

	actions := []tools.SubAgentOperationAction{
		tools.SubAgentOperationSpawn,
		tools.SubAgentOperationWait,
		tools.SubAgentOperationList,
		tools.SubAgentOperationInspect,
		tools.SubAgentOperationSendInput,
		tools.SubAgentOperationClose,
		tools.SubAgentOperationCloseAll,
	}
	for _, action := range actions {
		action := action
		t.Run(string(action), func(t *testing.T) {
			t.Parallel()
			payload := tools.SubAgentOperationActivityPayload{Action: action, Status: "running"}
			if action == tools.SubAgentOperationSpawn {
				payload.Targets = []tools.SubAgentOperationTarget{{TaskName: "research", TaskDescription: "Find current sources"}}
			}
			if _, err := json.Marshal(tools.ActivityPresentation{Renderer: tools.ActivityRendererSubAgentOperation, Payload: payload}); err != nil {
				t.Fatalf("action %q: %v", action, err)
			}
		})
	}
}

func TestSubAgentOperationActivityRejectsInvalidPayloads(t *testing.T) {
	t.Parallel()

	tooManyTargets := make([]tools.SubAgentOperationTarget, 201)
	for i := range tooManyTargets {
		tooManyTargets[i].TaskName = "task"
	}
	tests := []struct {
		name    string
		payload tools.SubAgentOperationActivityPayload
		want    string
	}{
		{name: "unknown action", payload: tools.SubAgentOperationActivityPayload{Action: "unknown"}, want: "action is unsupported"},
		{name: "empty target", payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationWait, Targets: []tools.SubAgentOperationTarget{{}}}, want: "requires thread or task name"},
		{name: "duplicate thread", payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationWait, Targets: []tools.SubAgentOperationTarget{{ThreadID: "thread-1"}, {ThreadID: "thread-1"}}}, want: "duplicated"},
		{name: "negative count", payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationWait, RequestedCount: -1}, want: "non-negative"},
		{name: "impossible outcome", payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationWait, RequestedCount: 1, CompletedCount: 1, MissingCount: 1}, want: "exceeds requested count"},
		{name: "invalid timeout", payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationClose, TimedOut: true}, want: "may time out"},
		{name: "too many targets", payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationList, Targets: tooManyTargets}, want: "too many"},
		{name: "long task name", payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationSpawn, Targets: []tools.SubAgentOperationTarget{{TaskName: strings.Repeat("x", 8_001)}}}, want: "size limit"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := json.Marshal(tools.ActivityPresentation{Renderer: tools.ActivityRendererSubAgentOperation, Payload: test.payload})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Marshal() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSubAgentOperationActivityStrictDecodeRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	var presentation tools.ActivityPresentation
	err := json.Unmarshal([]byte(`{"renderer":"subagent_operation","payload":{"action":"wait","legacy_items":[]}}`), &presentation)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("Unmarshal() error = %v, want unknown field", err)
	}
}
