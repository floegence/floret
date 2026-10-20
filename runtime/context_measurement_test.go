package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestMeasuredFrozenPrefixSurvivesPublicGatewayAndRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, responseEnd := range []bool{false, true} {
			t.Run(fmt.Sprintf("restart_%t_response_%t", restart, responseEnd), func(t *testing.T) {
				recorder := &calibrationRecorder{}
				gateway := &measurementGateway{ScriptedGateway: florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "measurement", StateCompatibilityKey: "account:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentExpanded}), responseEnd: responseEnd}
				agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 525000, MaxOutputTokens: 1000}}, gateway, WithAgentEventSink(recorder))
				if err != nil {
					t.Fatal(err)
				}
				path := t.TempDir() + "/measurement.sqlite"
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
				for i, key := range []string{"first", "second"} {
					if i == 1 && restart {
						if err := host.Shutdown(t.Context()); err != nil {
							t.Fatal(err)
						}
						host, service = open()
					}
					started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: key}, RequestKey: RequestKey(key)})
					if err != nil {
						t.Fatal(err)
					}
					view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
						return v.TurnID == started.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
					})
					if view.Failure != nil {
						t.Fatalf("turn failed: %+v", view.Failure)
					}
				}
				recorder.mu.Lock()
				defer recorder.mu.Unlock()
				var requests []Event
				for _, ev := range recorder.events {
					if ev.Type == observation.EventTypeContextCompact {
						t.Fatal("qualified measurement compacted below threshold")
					}
					if ev.Type == observation.EventTypeProviderRequest {
						requests = append(requests, ev)
					}
				}
				if len(requests) != 2 {
					t.Fatalf("requests=%d", len(requests))
				}
				pressure := requests[1].ContextStatus.ContextPressure
				if pressure.Source != config.PressureSourceUsageAnchoredDelta || pressure.ProjectedInputTokens < 1000 || pressure.ProjectedInputTokens >= 10000 {
					t.Fatalf("measurement was not restored: %+v", pressure)
				}
			})
		}
	}
}

type measurementGateway struct {
	*florettest.ScriptedGateway
	responseEnd bool
}

func (g *measurementGateway) Prepare(_ context.Context, r provider.Request) (provider.PreparedRequest, error) {
	raw, _ := json.Marshal(map[string]any{"model": "measurement", "messages": testCalibrationMessages(r.Messages)})
	tokens := int64(464496)
	if len(r.Messages) > 2 {
		tokens = 474100
	}
	return &measurementPrepared{gateway: g, request: r, raw: raw, tokens: tokens}, nil
}

type measurementPrepared struct {
	gateway *measurementGateway
	request provider.Request
	raw     []byte
	tokens  int64
}

func (p *measurementPrepared) TokenEstimate() provider.TokenEstimate {
	return provider.TokenEstimate{EstimatedInputTokens: p.tokens, MessageTokens: p.tokens, Source: "measurement:v1", Method: string(config.EstimateMethodProviderRenderedPayload), Confidence: "conservative", Coverage: "complete_request"}
}
func (p *measurementPrepared) RenderedPayloadFingerprint() string { return string(p.raw) }
func (p *measurementPrepared) Close() error                       { return nil }
func (p *measurementPrepared) rendered() provider.RenderedRequest {
	return provider.RenderedRequest{Provider: "test", Model: "measurement", Format: provider.RequestFormatOpenAIChat, Payload: p.raw}
}
func (p *measurementPrepared) InputMeasurementPrefix() provider.MeasurementPrefix {
	v, _ := provider.RenderedMeasurementPrefix(p.rendered(), "account:v1", "input")
	return v
}
func (p *measurementPrepared) MeasurementSuffix(v provider.MeasurementPrefix) ([]byte, bool) {
	return provider.RenderedMeasurementSuffix(p.rendered(), "account:v1", v)
}
func (p *measurementPrepared) Stream(context.Context) (<-chan provider.Event, error) {
	evidence := &provider.RequestMeasurement{AttemptID: p.request.AttemptID, CompleteInput: true, Input: p.InputMeasurementPrefix(), ResponseID: "response"}
	if p.gateway.responseEnd {
		var body map[string]any
		json.Unmarshal(p.raw, &body)
		body["messages"] = append(body["messages"].([]any), map[string]any{"role": "assistant", "content": "answer"})
		raw, _ := json.Marshal(body)
		rendered := p.rendered()
		rendered.Payload = raw
		evidence.ResponseEnd, _ = provider.RenderedMeasurementPrefix(rendered, "account:v1", "response_end")
	}
	out := make(chan provider.Event, 3)
	out <- provider.Event{Type: provider.EventUsage, Usage: provider.Usage{Source: "native", Available: true, InputTokensReported: true, InputTokenScope: "provider_execution", InputTokens: 1000, OutputTokensReported: true, OutputTokens: 200, OutputIncludesReasoning: true, Measurement: evidence}}
	out <- provider.Event{Type: provider.EventDelta, Text: "answer"}
	out <- provider.Event{Type: provider.EventDone, Reason: "stop", ResponseID: "response"}
	close(out)
	return out, nil
}
