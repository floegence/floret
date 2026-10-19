package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestActivityNavigationSurvivesExecutionHistoryForkAndRestart(t *testing.T) {
	for _, renderer := range []tools.ActivityRenderer{tools.ActivityRendererFile, tools.ActivityRendererStructured, tools.ActivityRendererPatch} {
		t.Run(string(renderer), func(t *testing.T) {
			target := tools.ActivityTargetRef{Kind: "file", Label: "report.go", Path: "/private/workspace/report.go", Line: 12, ResourceRef: "host-resource:report"}
			presentation := func() *tools.ActivityPresentation {
				activity := &tools.ActivityPresentation{Renderer: renderer, TargetRefs: []tools.ActivityTargetRef{target}}
				switch renderer {
				case tools.ActivityRendererFile:
					activity.Payload = tools.FileActivityPayload{Operation: "write", Additions: 518, Truncated: true}
				case tools.ActivityRendererStructured:
					activity.Payload = tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Title: "Input", Target: &target}}, Rows: []tools.StructuredActivityRow{{Title: "Result", Target: &target}}}
				case tools.ActivityRendererPatch:
					mutations := tools.FileMutationActivityPayloads{{Target: &target, DisplayName: "report.go", Additions: 518, Truncated: true}}
					activity.Payload = tools.PatchActivityPayload{Mutations: &mutations}
				}
				return activity
			}
			checkActivity := func(activity *tools.ActivityPresentation) {
				t.Helper()
				if activity == nil {
					t.Fatal("missing navigation activity")
				}
				if !reflect.DeepEqual(activity.TargetRefs, []tools.ActivityTargetRef{target}) {
					t.Fatalf("navigation targets changed: %+v", activity.TargetRefs)
				}
				switch payload := activity.Payload.(type) {
				case tools.StructuredActivityPayload:
					if len(payload.Inputs) != 1 || len(payload.Rows) != 1 || !reflect.DeepEqual(payload.Inputs[0].Target, &target) || !reflect.DeepEqual(payload.Rows[0].Target, &target) {
						t.Fatalf("row targets changed: %+v", payload)
					}
				case tools.PatchActivityPayload:
					if payload.Mutations == nil || len(*payload.Mutations) != 1 || !reflect.DeepEqual((*payload.Mutations)[0].Target, &target) || (*payload.Mutations)[0].Additions != 518 || !(*payload.Mutations)[0].Truncated {
						t.Fatalf("patch target changed: %+v", payload)
					}
				case tools.FileActivityPayload:
					if payload.Additions != 518 || !payload.Truncated {
						t.Fatalf("file preview changed: %+v", payload)
					}
				default:
					t.Fatalf("unexpected payload %T", payload)
				}
			}
			checkItems := func(items []ThreadItem) {
				t.Helper()
				for _, item := range items {
					if item.Activity != nil && item.Activity.ToolID == "write-1" {
						checkActivity(item.Activity.Presentation)
						return
					}
				}
				t.Fatal("missing tool activity")
			}
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			recorder := &runtimeEventRecorder{}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "navigation", StateCompatibilityKey: "navigation:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write-1", Name: "write", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
			)
			tool := tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
				Activity: func(tools.Invocation[any]) (*tools.ActivityPresentation, error) { return presentation(), nil },
			}, nil, nil, func(ctx context.Context, invocation tools.Invocation[map[string]any]) (tools.Result, error) {
				invocation.UpdateActivity(tools.ActivityUpdate{Activity: presentation()})
				select {
				case <-release:
				case <-ctx.Done():
					return tools.Result{}, ctx.Err()
				}
				return tools.Result{Text: "written", Activity: presentation()}, nil
			})
			agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEventSink(recorder), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			database := filepath.Join(t.TempDir(), "navigation.sqlite")
			open := func() (*Host, ThreadService) {
				t.Helper()
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(database)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, service
			}
			host, service := open()
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "write"}}); err != nil {
				t.Fatal(err)
			}
			live := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
				for _, item := range view.Items {
					if item.Activity != nil && item.Activity.ToolID == "write-1" && item.Activity.Presentation != nil {
						return true
					}
				}
				return false
			})
			checkItems(live.Items)
			close(release)
			completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
			if *completed.LastOutcome != TurnOutcomeCompleted {
				t.Fatalf("turn failed: %+v", completed.Failure)
			}
			checkItems(completed.Items)
			foundCall, foundUpdate, foundResult, foundTimeline := false, false, false, false
			for _, event := range recorder.snapshot() {
				if event.ToolID == "write-1" && event.Activity != nil {
					checkActivity(event.Activity)
					foundCall = foundCall || event.Type == observation.EventTypeToolCall
					foundUpdate = foundUpdate || event.Type == observation.EventTypeToolActivityUpdated
					foundResult = foundResult || event.Type == observation.EventTypeToolResult
				}
				if event.ActivityTimeline != nil {
					for _, item := range event.ActivityTimeline.Items {
						if item.ToolID == "write-1" && item.Presentation != nil {
							checkActivity(item.Presentation)
							foundTimeline = true
						}
					}
				}
			}
			if !foundCall || !foundUpdate || !foundResult || !foundTimeline {
				t.Fatalf("missing live observations: call=%v update=%v result=%v timeline=%v", foundCall, foundUpdate, foundResult, foundTimeline)
			}
			entries, err := host.store.repo.Path(t.Context(), created.ThreadID.String(), "")
			if err != nil {
				t.Fatal(err)
			}
			canonicalActivities := 0
			for _, entry := range entries {
				if entry.Message.Activity != nil {
					checkActivity(entry.Message.Activity)
					canonicalActivities++
				}
			}
			if canonicalActivities < 3 {
				t.Fatalf("missing canonical dispatch, update, or result: %d", canonicalActivities)
			}
			canonical, err := json.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			checkRead := func(threadID identity.ThreadID) {
				t.Helper()
				view, err := service.View(t.Context(), threadID)
				if err != nil {
					t.Fatal(err)
				}
				checkItems(view.Items)
				history, err := service.History(t.Context(), threadID, "", 100)
				if err != nil {
					t.Fatal(err)
				}
				checkItems(history.Items)
			}
			checkRead(created.ThreadID)
			forked, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
			if err != nil {
				t.Fatal(err)
			}
			checkRead(forked.ThreadID)
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
			checkRead(created.ThreadID)
			checkRead(forked.ThreadID)
			restored, err := host.store.repo.Path(t.Context(), created.ThreadID.String(), "")
			if err != nil {
				t.Fatal(err)
			}
			after, err := json.Marshal(restored)
			if err != nil {
				t.Fatal(err)
			}
			if string(canonical) != string(after) {
				t.Fatal("readback or restart rewrote canonical journal")
			}
		})
	}
}
