package engine_test

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
)

func TestDiagnosticSinkPreservesCanonicalToolNavigation(t *testing.T) {
	target := tools.ActivityTargetRef{Kind: "file", Label: "report.go", Path: "/private/workspace/report.go", URI: "file:///private/workspace/report.go", Line: 12}
	for _, renderer := range []tools.ActivityRenderer{tools.ActivityRendererFile, tools.ActivityRendererStructured, tools.ActivityRendererPatch} {
		t.Run(string(renderer), func(t *testing.T) {
			activity := &tools.ActivityPresentation{Renderer: renderer, TargetRefs: []tools.ActivityTargetRef{target}}
			switch renderer {
			case tools.ActivityRendererFile:
				activity.Payload = tools.FileActivityPayload{Operation: "write", Additions: 518, Truncated: true}
			case tools.ActivityRendererStructured:
				activity.Payload = tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Title: "Input", Target: &target}}, Rows: []tools.StructuredActivityRow{{Title: "Result", Target: &target}}}
			case tools.ActivityRendererPatch:
				mutations := tools.FileMutationActivityPayloads{{Target: &target, Additions: 518, Truncated: true}}
				activity.Payload = tools.PatchActivityPayload{Mutations: &mutations}
			}
			original := tools.CloneActivityPresentation(activity)
			gateway := harness.NewScriptedProvider(
				harness.Step(harness.Tool("write-1", "write", "{}"), harness.DoneReason("tool_calls")),
				harness.Step(harness.Text("done"), harness.Done()),
			)
			registry := tools.NewRegistry()
			mustRegister(t, registry, tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
				Activity: func(tools.Invocation[any]) (*tools.ActivityPresentation, error) { return activity, nil },
			}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
				return tools.Result{Text: "written", Activity: activity}, nil
			}))
			var trace bytes.Buffer
			executor := newTestEngine(gateway, &event.Recorder{})
			executor.Sink = event.NewTraceWriter(&trace)
			executor.Tools = registry
			if result := executor.Run(t.Context(), "write"); result.Status != engine.Completed {
				t.Fatalf("execution failed: %+v", result)
			}
			messages, err := executor.Store.Transcript("run")
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, message := range messages {
				if message.Role == session.Tool && message.ToolCallID == "write-1" {
					found = true
					if !reflect.DeepEqual(message.Activity, original) {
						t.Fatalf("diagnostic sink changed canonical navigation: %+v", message.Activity)
					}
				}
			}
			if !found {
				t.Fatal("missing canonical tool result")
			}
			if strings.Contains(trace.String(), "/private/workspace") {
				t.Fatalf("diagnostic trace leaked path: %s", &trace)
			}
			if !reflect.DeepEqual(activity, original) {
				t.Fatal("execution mutated tool-owned presentation")
			}
		})
	}
}
