package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestHostedContextCalibrationAcrossTurnsAndRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, large := range []bool{false, true} {
			t.Run(fmt.Sprintf("restart_%t_large_%t", restart, large), func(t *testing.T) {
				release := make(chan struct{})
				previous, next := int64(280862), int64(291336)
				if large {
					previous, next = 550000, 600000
				}
				usage := func(tokens int64) provider.Event {
					return provider.Event{Type: provider.EventUsage, Usage: provider.Usage{InputTokens: tokens, WindowInputTokens: tokens, Available: true, Source: "native"}}
				}
				gateway := &calibrationGateway{ScriptedGateway: florettest.NewScriptedGateway(
					provider.Identity{Provider: "test", Model: "calibration", StateCompatibilityKey: "calibration:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentExpanded},
					florettest.Step{Events: []provider.Event{usage(64000), {Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "read", Name: "read", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
					florettest.Step{Events: []provider.Event{usage(71872), {Type: provider.EventDelta, Text: "read complete"}, {Type: provider.EventDone, Reason: "stop"}}},
					florettest.Step{BlockUntil: release, Events: []provider.Event{usage(74312), {Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
				), estimates: []int64{previous - 10000, previous, next}}
				recorder := &calibrationRecorder{}
				read := tools.Define[map[string]any](tools.Definition{Name: "read", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
					return tools.Result{Text: "recorded tool output"}, nil
				})
				agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 950000, MaxOutputTokens: 384000}}, gateway, WithAgentTools(read), WithAgentEventSink(recorder), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, r EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
					return effect(ctx, EffectAuthorizationProof{EffectAttemptID: r.EffectAttemptID, RequestFingerprint: r.RequestFingerprint, ThreadID: r.ThreadID, TurnID: r.TurnID, RunID: r.RunID, ToolCallID: r.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
				})))
				if err != nil {
					t.Fatal(err)
				}
				path := t.TempDir() + "/context.sqlite"
				open := func() (*Host, ThreadService) {
					h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
					if err != nil {
						t.Fatal(err)
					}
					s, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
					if err != nil {
						t.Fatal(err)
					}
					return h, s
				}
				host, service := open()
				defer func() { _ = host.Shutdown(context.Background()) }()
				created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
				if err != nil {
					t.Fatal(err)
				}
				send := func(key string) {
					started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: key}, RequestKey: RequestKey(key)})
					if err != nil {
						t.Fatal(err)
					}
					if key == "second" {
						ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
						defer cancel()
						if err := gateway.WaitForRequests(ctx, 3); err != nil {
							t.Fatal(err)
						}
						snapshot, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
						if err != nil {
							t.Fatal(err)
						}
						if snapshot.ContextUsage == nil || snapshot.ContextUsage.Confirmed == nil || snapshot.ContextUsage.Estimate == nil {
							t.Fatalf("missing split usage: %+v", snapshot)
						}
						if snapshot.ContextUsage.Confirmed.Usage.WindowInputTokens != 71872 || snapshot.ContextUsage.Estimate.ContextPressure.ProjectedInputTokens != 71872+next-previous {
							t.Fatalf("wrong split usage: %+v", snapshot.ContextUsage)
						}
						recorder.mu.Lock()
						var live *ThreadContextUsage
						for _, ev := range recorder.events {
							if ev.Type == observation.EventTypeProviderRequest {
								live = ev.ContextUsage
							}
						}
						recorder.mu.Unlock()
						liveJSON, _ := json.Marshal(live)
						canonicalJSON, _ := json.Marshal(snapshot.ContextUsage)
						if !bytes.Equal(liveJSON, canonicalJSON) {
							t.Fatalf("live=%s canonical=%s", liveJSON, canonicalJSON)
						}
						close(release)
					}
					view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
						return v.TurnID == started.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
					})
					if view.Failure != nil {
						t.Fatalf("turn failed: %+v", view.Failure)
					}
				}
				send("first")
				if restart {
					if err := host.Shutdown(t.Context()); err != nil {
						t.Fatal(err)
					}
					host, service = open()
				}
				send("second")
				snapshot, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.ContextUsage == nil || snapshot.ContextUsage.Confirmed == nil || snapshot.ContextUsage.Confirmed.Usage.WindowInputTokens != 74312 || snapshot.ContextUsage.Estimate != nil {
					t.Fatalf("completed usage=%+v", snapshot.ContextUsage)
				}
				recorder.mu.Lock()
				defer recorder.mu.Unlock()
				var requests []Event
				for _, ev := range recorder.events {
					if ev.Type == observation.EventTypeContextCompact {
						t.Fatal("unexpected compaction")
					}
					if ev.Type == observation.EventTypeProviderRequest {
						requests = append(requests, ev)
					}
				}
				if len(requests) != 3 {
					t.Fatalf("requests=%d", len(requests))
				}
				pressure := requests[2].ContextStatus.ContextPressure
				if pressure.Source != config.PressureSourceUsageAnchoredDelta || pressure.ProjectedInputTokens != 71872+next-previous {
					t.Fatalf("hosted next-turn projection=%+v; want anchored %d", pressure, 71872+next-previous)
				}
			})
		}
	}
}

type calibrationRecorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *calibrationRecorder) EmitEvent(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

type calibrationGateway struct {
	*florettest.ScriptedGateway
	estimates []int64
}

func (g *calibrationGateway) Prepare(_ context.Context, r provider.Request) (provider.PreparedRequest, error) {
	index := len(g.Requests())
	if index >= len(g.estimates) {
		return nil, fmt.Errorf("unexpected preparation %d", index)
	}
	return &calibrationPrepared{gateway: g, request: r, tokens: g.estimates[index]}, nil
}

type calibrationPrepared struct {
	gateway *calibrationGateway
	request provider.Request
	tokens  int64
}

func (p *calibrationPrepared) Stream(ctx context.Context) (<-chan provider.Event, error) {
	return p.gateway.Stream(ctx, p.request)
}
func (p *calibrationPrepared) TokenEstimate() provider.TokenEstimate {
	return provider.TokenEstimate{MessageTokens: p.tokens, EstimatedInputTokens: p.tokens, Source: "calibration", Method: string(config.EstimateMethodProviderRenderedPayload), Confidence: "conservative", Coverage: "complete_request"}
}
func (p *calibrationPrepared) RenderedPayloadFingerprint() string {
	return fmt.Sprintf("calibration:%d", p.tokens)
}
func (p *calibrationPrepared) Close() error { return nil }
