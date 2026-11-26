package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"sync"
	"testing"
	"time"
)

type failFirstWorkStop struct {
	*internalstorage.BackendKernel
	once sync.Once
	err  error
}

func (r *failFirstWorkStop) StopHostWork(ctx context.Context, thread string, stop sessiontree.HostWorkStop) error {
	var err error
	r.once.Do(func() { err = r.err })
	if err != nil {
		return err
	}
	return r.BackendKernel.StopHostWork(ctx, thread, stop)
}

func TestHostWorkCompletionAndRestartUnknown(t *testing.T) {
	for _, mode := range []string{"unknown", "completed", "stopped", "subtree_stopped", "queue_stop_failure", "suspended", "overlapping", "overlapping_stop", "completion_before_dispatch_failure"} {
		complete := mode != "unknown"
		t.Run(mode, func(t *testing.T) {
			var completionGate chan struct{}
			var sourceGate chan struct{}
			if mode == "queue_stop_failure" {
				sourceGate = make(chan struct{})
			}
			if mode == "overlapping" || mode == "overlapping_stop" {
				completionGate = make(chan struct{})
			}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "host-work", StateCompatibilityKey: "host-work"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "shell-1", Name: "shell", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{BlockUntil: sourceGate, Events: []provider.Event{{Type: provider.EventDelta, Text: "The command is running."}, {Type: provider.EventDone, Reason: "stop"}}},
				florettest.Step{BlockUntil: completionGate, Events: []provider.Event{{Type: provider.EventDelta, Text: "Observed completion."}, {Type: provider.EventDone, Reason: "stop"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Retained result."}, {Type: provider.EventDone, Reason: "stop"}}})
			var service ThreadService
			var binding BindHostWorkInput
			tool := tools.Define[map[string]any](tools.Definition{Name: "shell", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
				binding.WorkID = "process-1"
				if err := service.(ThreadHostWorkService).BindHostWork(ctx, binding); err != nil {
					return tools.Result{}, err
				}
				if mode == "completion_before_dispatch_failure" {
					if err := service.(ThreadHostWorkService).CompleteHostWork(ctx, CompleteHostWorkInput{ThreadID: binding.ThreadID, WorkID: binding.WorkID, Notice: "finished", Result: []byte(`{"exit_code":0}`)}); err != nil {
						return tools.Result{}, err
					}
					return tools.Result{DispatchErr: context.DeadlineExceeded}, nil
				}
				return tools.Result{Text: "running process-1", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "running", Summary: "command running"}}}, nil
			})
			agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				binding = BindHostWorkInput{EffectAttemptID: req.EffectAttemptID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID}
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/state.db"
			writeFailure := errors.New("host-work cancellation write failed")
			open := func() *Host {
				h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				if mode == "queue_stop_failure" {
					h.store.repo = &failFirstWorkStop{BackendKernel: h.store.repo.(*internalstorage.BackendKernel), err: writeFailure}
				}
				service, err = h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return h
			}
			host := open()
			defer func() { host.Shutdown(context.Background()) }()
			in := CreateThreadInput{RequestKey: "create"}
			if mode == "subtree_stopped" {
				root, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "idle-root"})
				if err != nil {
					t.Fatal(err)
				}
				in.ParentThreadID, in.TaskName, in.HostProfileRef = root.ThreadID, "background", "test"
			}
			created, err := service.Create(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "start", Input: UserInput{Text: "run command"}}); err != nil {
				t.Fatal(err)
			}
			wait := func(want int) {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					v, e := service.View(t.Context(), created.ThreadID)
					if e != nil {
						t.Fatal(e)
					}
					if v.Failure != nil {
						t.Fatalf("failed before expected request: %+v", v.Failure)
					}
					if v.Activity == ThreadActivityIdle && len(gateway.Requests()) >= want {
						if v.Failure != nil {
							t.Fatalf("failed: %+v", v.Failure)
						}
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatalf("not settled: requests=%d", len(gateway.Requests()))
			}
			if mode == "completion_before_dispatch_failure" {
				v := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.Failure != nil })
				for range 2 {
					facts, err := service.(ThreadTurnFailureReader).ReadTurnFailure(t.Context(), threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{created.ThreadID}}, ThreadTurnFailureInput{ThreadID: created.ThreadID, TurnID: v.TurnID, RunID: v.RunID})
					if err != nil || facts == nil || facts.Code != ThreadTurnFailureEffectOutcomeUnknown || facts.UnconfirmedEffects == nil || len(facts.UnconfirmedEffects.CompletedWork) != 1 || string(facts.UnconfirmedEffects.CompletedWork[0].Result) != `{"exit_code":0}` || len(gateway.Requests()) != 1 {
						t.Fatalf("evidence=%+v err=%v", facts, err)
					}
					if err := host.Shutdown(t.Context()); err != nil {
						t.Fatal(err)
					}
					host = open()
				}
				return
			}
			if mode == "queue_stop_failure" {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if err := gateway.WaitForRequests(ctx, 2); err != nil {
					t.Fatal(err)
				}
				if _, err := service.Send(ctx, SendInput{ThreadID: created.ThreadID, RequestKey: "queued", Input: UserInput{Text: "queue input"}}); err != nil {
					t.Fatal(err)
				}
				host.idMu.Lock()
				pause := &pausedPromotionIDs{IDSource: host.idSource, entered: make(chan struct{}), resume: make(chan struct{})}
				host.idSource = pause
				host.idMu.Unlock()
				defer close(pause.resume)
				close(sourceGate)
				select {
				case <-pause.entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				controller := service.(ThreadSubtreeStopper)
				frozen, err := controller.SubtreeExecutions(ctx, created.ThreadID)
				if err != nil || len(frozen.Targets) != 1 {
					t.Fatalf("combined grant=%+v %v", frozen, err)
				}
				out, err := controller.StopSubtree(ctx, frozen)
				if !errors.Is(err, writeFailure) || len(out.Suppressed)+len(out.Admitted)+len(out.AlreadyEnded) != 0 {
					t.Fatalf("after failure=%+v %v", out, err)
				}
				actor := service.(*threadRuntimeService).runtime(created.ThreadID)
				if err := actor.apply(ctx, func() error {
					if actor.state.autoPromotion == nil {
						return errors.New("queue grant lost on failed write")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				out, err = controller.StopSubtree(ctx, frozen)
				if err != nil || len(out.Suppressed) != 1 || out.Suppressed[0] != frozen.Targets[0] || len(out.Admitted)+len(out.AlreadyEnded) != 0 {
					t.Fatalf("frozen retry=%+v %v", out, err)
				}
				return
			}
			wait(2)
			var frozen StopSubtreeInput
			if mode == "subtree_stopped" {
				stopper := service.(ThreadSubtreeStopper)
				snapshot, err := stopper.SubtreeExecutions(t.Context(), in.ParentThreadID)
				if err != nil {
					t.Fatal(err)
				}
				frozen = snapshot
				out, err := stopper.StopSubtree(t.Context(), snapshot)
				if err != nil || len(out.Admitted) != 0 || len(out.Suppressed) != 1 || out.Suppressed[0].ThreadID != created.ThreadID {
					t.Fatalf("continuation suppression=%+v err=%v", out, err)
				}
			}
			if mode == "stopped" {
				if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop-background"}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "overlapping" || mode == "overlapping_stop" {
				v, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "overlap", Input: UserInput{Text: "continue while the command runs"}})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if err := gateway.WaitForRequests(ctx, 3); err != nil {
					t.Fatal(err)
				}
				if mode == "overlapping_stop" {
					stopper := service.(ThreadSubtreeStopper)
					frozen, err = stopper.SubtreeExecutions(ctx, created.ThreadID)
					if err != nil || len(frozen.Targets) != 2 {
						t.Fatalf("overlapping inventory=%+v %v", frozen, err)
					}
					out, err := stopper.StopSubtree(ctx, frozen)
					if err != nil || len(out.Suppressed) != 1 || out.Suppressed[0].RunID != binding.RunID || len(out.Admitted) != 1 || out.Admitted[0].RunID != v.RunID {
						t.Fatalf("overlapping stop=%+v %v", out, err)
					}
					settled := waitThreadView(t, service, v.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
					if settled.LastOutcome == nil || *settled.LastOutcome != TurnOutcomeCancelled {
						t.Fatalf("settled outcome=%v", settled.LastOutcome)
					}
				}
			}
			if complete {
				output := "sealed UTF-8 output 中文"
				in := CompleteHostWorkInput{ThreadID: created.ThreadID, WorkID: "process-1", Notice: "process-1 completed: 7319", Notify: mode != "suspended" && mode != "overlapping", Output: &output}
				if err := service.(ThreadHostWorkService).CompleteHostWork(t.Context(), in); err != nil {
					t.Fatal(err)
				}
				if mode == "overlapping" {
					close(completionGate)
					wait(3)
				} else if mode == "overlapping_stop" {
					close(completionGate)
					service.(*threadRuntimeService).continueHostWork(created.ThreadID)
					if len(gateway.Requests()) != 3 {
						t.Fatal("stopped old source started another execution")
					}
				} else if mode != "stopped" && mode != "subtree_stopped" && mode != "suspended" {
					wait(3)
				} else {
					time.Sleep(30 * time.Millisecond)
					if len(gateway.Requests()) != 2 {
						t.Fatal("stopped background work woke provider")
					}
				}
				if err := service.(ThreadHostWorkService).CompleteHostWork(t.Context(), in); err != nil {
					t.Fatal(err)
				}
			}
			if err := host.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			host = open()
			if mode == "overlapping_stop" {
				replayed, err := service.(ThreadSubtreeStopper).StopSubtree(t.Context(), frozen)
				if err != nil || len(replayed.AlreadyEnded) != 2 || len(replayed.Admitted)+len(replayed.Suppressed) != 0 {
					t.Fatalf("overlap after restart=%+v %v", replayed, err)
				}
			}
			works, err := service.(ThreadHostWorkService).ListHostWork(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			want := "unknown"
			if complete {
				want = "completed"
			}
			if len(works) != 1 || works[0].State != want {
				t.Fatalf("work=%+v", works)
			}
			if !complete {
				v, err := service.View(t.Context(), created.ThreadID)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, item := range v.Items {
					if item.Activity != nil {
						status, _ := tools.ActivityStatus(item.Activity.Presentation)
						if status == "unknown" {
							found = true
							scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{created.ThreadID}}
							span, err := service.(ThreadItemRangeReader).ItemRange(t.Context(), scope, ThreadItemRangeInput{ThreadID: created.ThreadID, AnchorID: item.ID, Kinds: []ThreadItemKind{ThreadItemTool}})
							if err != nil || span.Activity.Unknown != 1 || span.Facts&ThreadItemRangeUnknownActivity == 0 {
								t.Fatalf("unknown range=%+v err=%v", span, err)
							}
							for _, fields := range []bool{false, true} {
								sub, err := service.(ThreadWindowSource).SubscribeWindowUpdates(t.Context(), scope)
								if err != nil {
									t.Fatal(err)
								}
								if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: created.ThreadID, ItemIDs: []string{item.ID}, ActivityFields: fields}); err != nil {
									t.Fatal(err)
								}
								for {
									update := nextUpdate(t, sub)
									if update.Window == nil {
										continue
									}
									items := update.Window.Value.Items
									if len(items) != 1 || items[0].Activity.Status != observation.ActivityStatusUnknown || !items[0].Activity.NeedsAttention {
										t.Fatalf("window fields=%t items=%+v", fields, items)
									}
									break
								}
								sub.Close()
							}

							timeline := observation.ActivityTimeline{SchemaVersion: observation.ActivityTimelineSchemaVersion, Items: []observation.ActivityItem{*item.Activity}}
							timeline.Summary = observation.RebuildActivitySummary(timeline)
							if err := observation.ValidateActivityTimeline(timeline); err != nil {
								t.Fatalf("public recovered activity: %v", err)
							}
							if item.Activity.Status != observation.ActivityStatus("unknown") || !item.Activity.NeedsAttention || timeline.Summary.Status != observation.ActivityStatus("unknown") {
								t.Fatalf("unknown public activity=%+v summary=%+v", item.Activity, timeline.Summary)
							}
						}
					}
				}
				if !found {
					t.Fatalf("recovered original activity did not retain unknown status: %+v", v.Items)
				}
			}
			if complete {
				out, err := service.(ThreadHostWorkService).ReadHostWorkOutput(t.Context(), created.ThreadID, "process-1")
				if mode == "subtree_stopped" {
					if !errors.Is(err, ErrSubAgentParentRequired) || out != "" {
						t.Fatalf("unscoped artifact read=%q %v", out, err)
					}
					bindings, readErr := host.store.repo.(sessiontree.HostWorkRepo).HostWorks(t.Context(), created.ThreadID.String())
					if readErr != nil || len(bindings) != 1 || bindings[0].Output == nil {
						t.Fatalf("bindings=%+v %v", bindings, readErr)
					}
					artifact, readErr := host.store.repo.(sessiontree.ArtifactAuthorityRepo).ReadArtifact(t.Context(), sessiontree.ArtifactReadRequest{ThreadID: created.ThreadID.String(), ParentThreadID: in.ParentThreadID.String(), ArtifactID: bindings[0].Output.ID})
					out, err = artifact.Text, readErr
					replay, stopErr := service.(ThreadSubtreeStopper).StopSubtree(t.Context(), frozen)
					if stopErr != nil || len(replay.AlreadyEnded) != 1 || len(replay.Suppressed)+len(replay.Admitted) != 0 {
						t.Fatalf("restart replay=%+v %v", replay, stopErr)
					}
				}
				if err != nil || out != "sealed UTF-8 output 中文" {
					t.Fatalf("output=%q err=%v", out, err)
				}
			}
			count := len(gateway.Requests())
			time.Sleep(30 * time.Millisecond)
			if len(gateway.Requests()) != count {
				t.Fatal("restart caused duplicate or unknown wake")
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "next", Input: UserInput{Text: "report current state"}}); err != nil {
				t.Fatal(err)
			}
			wait(count + 1)
			requests := gateway.Requests()
			needle := "7319"
			if !complete {
				needle = "outcome and any unsealed output are unknown"
			}
			seen := 0
			for _, m := range requests[len(requests)-1].Messages {
				if strings.Contains(m.Text, needle) {
					seen++
				}
			}
			if seen != 1 {
				t.Fatalf("notice count=%d", seen)
			}
		})
	}
}
