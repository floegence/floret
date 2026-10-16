package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
)

type outputBudgetGateway struct {
	*florettest.ScriptedGateway
	tokens               int64
	preparations, refits int
}

func (g *outputBudgetGateway) Prepare(ctx context.Context, req provider.Request) (provider.PreparedRequest, error) {
	g.preparations++
	return &outputBudgetPrepared{gateway: g, request: req}, ctx.Err()
}

type outputBudgetPrepared struct {
	gateway *outputBudgetGateway
	request provider.Request
	closed  bool
}

func (p *outputBudgetPrepared) Stream(ctx context.Context) (<-chan provider.Event, error) {
	if p.closed {
		return nil, fmt.Errorf("request already consumed")
	}
	p.closed = true
	return p.gateway.Stream(ctx, p.request)
}
func (p *outputBudgetPrepared) TokenEstimate() provider.TokenEstimate {
	return provider.TokenEstimate{EstimatedInputTokens: p.gateway.tokens, MessageTokens: p.gateway.tokens, Source: "budget_fixture", Method: "provider_rendered_payload_estimate", Confidence: "conservative", Coverage: "complete_request"}
}
func (p *outputBudgetPrepared) RenderedPayloadFingerprint() string {
	return fmt.Sprintf("budget:%d:%d", p.gateway.tokens, p.request.MaxOutputTokens)
}
func (p *outputBudgetPrepared) Close() error { p.closed = true; return nil }
func (p *outputBudgetPrepared) WithMaxOutputTokens(ctx context.Context, limit int64) (provider.PreparedRequest, error) {
	if p.closed {
		return nil, fmt.Errorf("refit closed request")
	}
	p.gateway.refits++
	req := p.request
	req.MaxOutputTokens = limit
	return &outputBudgetPrepared{gateway: p.gateway, request: req}, ctx.Err()
}
func (p *outputBudgetPrepared) PrepareRetry(ctx context.Context, next provider.Request) (provider.PreparedRequest, error) {
	if next.MaxOutputTokens != p.request.MaxOutputTokens {
		return nil, fmt.Errorf("retry changed frozen output ceiling")
	}
	req := p.request
	req.AttemptID = next.AttemptID
	req.AttemptEpoch = next.AttemptEpoch
	return &outputBudgetPrepared{gateway: p.gateway, request: req}, ctx.Err()
}

func TestRequestOutputBudgetFreezesOnceAndSurvivesRetry(t *testing.T) {
	g := &outputBudgetGateway{tokens: 68400, ScriptedGateway: florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "budget", StateCompatibilityKey: "budget:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported, OutputControl: provider.OutputControlClient}, florettest.Step{ReturnError: &provider.ProviderHTTPError{StatusCode: 500}}, florettest.Step{Events: []provider.Event{{Type: provider.EventUsage, Usage: provider.Usage{InputTokens: 69000, WindowInputTokens: 69000, Available: true}}, {Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})}
	retry := DefaultProviderRetryPolicy()
	retry.BaseDelay = time.Microsecond
	retry.MaxDelay = time.Microsecond
	retry.Jitter = 0
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "budget", Name: "Budget"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216}}, g, WithAgentProviderRetryPolicy(retry))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "write"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || g.preparations != 1 || g.refits != 1 {
		t.Fatalf("failure=%+v prepare=%d refit=%d", view.Failure, g.preparations, g.refits)
	}
	reqs := g.Requests()
	if len(reqs) != 2 || reqs[0].MaxOutputTokens != 393216 || reqs[1].MaxOutputTokens != 393216 {
		t.Fatalf("requests=%+v", reqs)
	}
	snapshot, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	pressure := snapshot.ContextUsage.Confirmed.ContextPressure
	if pressure.RequestOutputTokens != 393216 || pressure.SafetyMarginTokens != 4096 || pressure.OutputControl != "client" || pressure.ThresholdTokens != 855000 {
		t.Fatalf("pressure=%+v", pressure)
	}
}

func TestRequestOutputBudgetRejectsUnsupportedAndExplicitServerLimits(t *testing.T) {
	for _, control := range []provider.OutputControl{"", provider.OutputControlServer} {
		t.Run(string(control), func(t *testing.T) {
			g := &outputBudgetGateway{tokens: 10, ScriptedGateway: florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "budget", StateCompatibilityKey: "budget:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported, OutputControl: control})}
			agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "budget", Name: "Budget"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216, MaxOutputTokens: 100000}}, g)
			if err != nil {
				t.Fatal(err)
			}
			_, service := testThreadServiceWithAgent(t, agent)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
				t.Fatal(err)
			}
			view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if view.Failure == nil || view.Failure.Code != ThreadTurnFailureContextBudgetInvalid || len(g.Requests()) != 0 {
				t.Fatalf("view=%+v requests=%d", view, len(g.Requests()))
			}
		})
	}
}

func TestRequestOutputBudgetModesAndBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		input, explicit, want int64
		control               provider.OutputControl
		fail                  bool
	}{
		{name: "server managed", input: 68400, control: provider.OutputControlServer},
		{name: "soft threshold with no savings", input: 855000, want: 90904, control: provider.OutputControlClient},
		{name: "explicit unchanged", input: 68400, explicit: 200000, want: 200000, control: provider.OutputControlClient},
		{name: "hard overflow", input: 950000, control: provider.OutputControlClient, fail: true},
		{name: "explicit exceeds model", input: 10, explicit: 400000, control: provider.OutputControlClient, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &outputBudgetGateway{tokens: tc.input, ScriptedGateway: florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "budget", StateCompatibilityKey: "budget:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported, OutputControl: tc.control}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})}
			agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "budget", Name: "Budget"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216, MaxOutputTokens: tc.explicit}}, g)
			if err != nil {
				if tc.fail {
					return
				}
				t.Fatal(err)
			}
			_, service := testThreadServiceWithAgent(t, agent)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "write"}})
			if err != nil {
				t.Fatal(err)
			}
			view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			requests := g.Requests()
			if tc.input == 855000 {
				if len(requests) != 2 {
					t.Fatalf("soft no-savings requests=%d", len(requests))
				}
				requests = requests[1:]
			}
			if tc.fail {
				if view.Failure == nil || len(requests) != 0 {
					t.Fatalf("failure=%+v requests=%d", view.Failure, len(requests))
				}
				return
			}
			if view.Failure != nil || len(requests) != 1 || requests[0].MaxOutputTokens != tc.want {
				t.Fatalf("failure=%+v requests=%+v", view.Failure, requests)
			}
			if tc.control == provider.OutputControlServer && g.refits != 0 {
				t.Fatalf("server refits=%d", g.refits)
			}
		})
	}
}

func TestAutomaticOutputBudgetPreservesBoundedLengthContinuation(t *testing.T) {
	for _, finish := range []bool{false, true} {
		t.Run(fmt.Sprint(finish), func(t *testing.T) {
			steps := []florettest.Step{{Events: []provider.Event{{Type: provider.EventDelta, Text: "first "}, {Type: provider.EventTruncated, Reason: "length"}}}, {Events: []provider.Event{{Type: provider.EventDelta, Text: "second "}, {Type: provider.EventTruncated, Reason: "length"}}}, {Events: []provider.Event{{Type: provider.EventDelta, Text: "third"}, {Type: provider.EventTruncated, Reason: "length"}}}}
			if finish {
				steps[2].Events[1] = provider.Event{Type: provider.EventDone, Reason: "stop"}
			}
			g := &outputBudgetGateway{tokens: 100, ScriptedGateway: florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "budget", StateCompatibilityKey: "budget:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported, OutputControl: provider.OutputControlClient}, steps...)}
			agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "budget", Name: "Budget"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216}}, g)
			if err != nil {
				t.Fatal(err)
			}
			_, service := testThreadServiceWithAgent(t, agent)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "write"}})
			if err != nil {
				t.Fatal(err)
			}
			view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if (view.Failure == nil) != finish || len(g.Requests()) != 3 {
				t.Fatalf("failure=%+v requests=%d", view.Failure, len(g.Requests()))
			}
		})
	}
}
