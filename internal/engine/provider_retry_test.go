package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/providerretry"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	"io"
	"testing"
	"time"
)

type recoveryProvider struct {
	requests       []provider.Request
	gateHeld       *bool
	failCount      int
	attemptsClosed int
	failure        error
}

func (p *recoveryProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.requests = append(p.requests, req)
	out := make(chan provider.StreamEvent, 4)
	if len(p.requests) <= p.failCount {
		out <- provider.StreamEvent{Type: provider.Delta, Text: "discarded draft"}
		failure := p.failure
		if failure == nil {
			failure = providerretry.WithAdvice(errors.New("upstream gateway error"), providerretry.Advice{Category: providerretry.Transient})
		}
		out <- provider.StreamEvent{Type: provider.Error, Err: failure}
	} else {
		out <- provider.StreamEvent{Type: provider.Delta, Text: "recovered"}
		out <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	}
	close(out)
	return out, nil
}
func fastRecoveryPolicy() providerretry.Policy {
	p := providerretry.DefaultPolicy()
	p.BaseDelay = time.Microsecond
	p.MaxDelay = time.Microsecond
	p.Jitter = 0
	return p
}
func TestProviderRecoveryKeepsFrozenRequestIdentityAndReleasesAdmission(t *testing.T) {
	p := &recoveryProvider{failCount: 2, failure: io.EOF}
	rec := &event.Recorder{}
	e := newTestEngine(p, rec)
	e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
	gateHeld := false
	admissions := 0
	e.Options.ProviderRequestGate = func(context.Context) (func(), error) {
		if gateHeld {
			t.Fatal("previous admission leaked")
		}
		gateHeld = true
		admissions++
		return func() { gateHeld = false }, nil
	}
	result := e.Run(t.Context(), "work")
	if result.Status != engine.Completed || result.Output != "recovered" || len(p.requests) != 3 || admissions != 3 || gateHeld {
		t.Fatalf("result=%#v attempts=%d admissions=%d", result, len(p.requests), admissions)
	}
	first := p.requests[0]
	for i, req := range p.requests {
		if req.Step != first.Step || req.RunID != first.RunID || req.TurnID != first.TurnID || req.ThreadID != first.ThreadID || req.LogicalRequestID != first.LogicalRequestID || req.PromptScopeID != first.PromptScopeID || req.AttemptEpoch != i+1 || req.AttemptID == "" {
			t.Fatalf("identity drift: %#v", req)
		}
		requestJSON, _ := json.Marshal(req.Messages)
		firstJSON, _ := json.Marshal(first.Messages)
		toolsJSON, _ := json.Marshal(req.Tools)
		firstToolsJSON, _ := json.Marshal(first.Tools)
		if !bytes.Equal(requestJSON, firstJSON) || !bytes.Equal(toolsJSON, firstToolsJSON) {
			t.Fatal("model input drift")
		}
	}
	records, err := e.Prompt.ProviderRequests(t.Context(), first.PromptScopeID)
	if err != nil || len(records) != 3 {
		t.Fatalf("records=%v err=%v", records, err)
	}
	segments, _ := e.Prompt.Segments(t.Context(), first.PromptScopeID, first.Provider, first.Model)
	seen := map[string]bool{}
	for _, segment := range segments {
		if seen[segment.ID] {
			t.Fatal("retry re-committed staged segments")
		}
		seen[segment.ID] = true
	}
	retries := 0
	for _, ev := range rec.Events {
		if ev.Type == event.ProviderRetry {
			retries++
			if ev.Retry == nil || ev.Retry.Retry != retries || ev.Retry.ReplacedAttemptEpoch != retries {
				t.Fatalf("retry=%#v", ev)
			}
		}
	}
	if retries != 2 {
		t.Fatalf("retries=%d", retries)
	}
}
func TestProviderRecoveryDoesNotReplayUnsafeHostedToolsOrStorage(t *testing.T) {
	for _, mode := range []string{"unsafe_hosted", "storage", "storage_eof", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			p := &recoveryProvider{failCount: 10}
			e := newTestEngine(p, &event.Recorder{})
			e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
			switch mode {
			case "unsafe_hosted":
				e.Options.HostedToolDefinitions = []provider.HostedToolDefinition{{Name: "search", Type: "web_search"}}
			case "storage":
				e.Prompt = &failingProviderRequestStore{Store: cache.NewMemoryStore(), err: providerretry.WithAdvice(errors.New("storage"), providerretry.Advice{Category: providerretry.Transient})}
			case "storage_eof":
				e.Prompt = &failingProviderRequestStore{Store: cache.NewMemoryStore(), err: io.EOF}
			case "disabled":
				e.Options.ProviderRetryPolicy.Disabled = true
			}
			got := e.Run(t.Context(), "work")
			if got.Status != engine.Failed || len(p.requests) > 1 {
				t.Fatalf("result=%#v calls=%d", got, len(p.requests))
			}
		})
	}
}
func TestProviderRecoveryStopDuringWait(t *testing.T) {
	p := &recoveryProvider{failCount: 10}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ProviderRetryPolicy = providerretry.DefaultPolicy()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	e.Sink = recoverySink(func(ev event.Event) {
		if ev.Type == event.ProviderRetry {
			cancel()
		}
	})
	got := e.Run(ctx, "work")
	if got.Status != engine.Cancelled || len(p.requests) != 1 {
		t.Fatalf("result=%#v calls=%d", got, len(p.requests))
	}
}

type recoverySink func(event.Event)

func (s recoverySink) Emit(ev event.Event) { s(ev) }

type replayRecoveryProvider struct {
	recoveryProvider
	handles          []*replayRecoveryHandle
	drift, failClose bool
}
type replayRecoveryHandle struct {
	owner        *replayRecoveryProvider
	req          provider.Request
	closed, used bool
	fingerprint  string
}

func (h *replayRecoveryHandle) InputMeasurementPrefix() requestmeasure.Prefix {
	return requestmeasure.Prefix{ExecutionIdentity: h.req.AttemptID, Format: "test:v1", Fingerprint: "frozen", ItemCount: 1, Boundary: "input"}
}
func (h *replayRecoveryHandle) MeasurementSuffix(requestmeasure.Prefix) ([]byte, bool) {
	return nil, false
}

func TestProviderRecoveryRefreshesAttemptMeasurementPrefix(t *testing.T) {
	p := &replayRecoveryProvider{recoveryProvider: recoveryProvider{failCount: 1}}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
	got := e.Run(t.Context(), "work")
	if got.Status != engine.Completed || len(p.requests) != 2 {
		t.Fatalf("result=%+v requests=%d", got, len(p.requests))
	}
	records, err := e.Prompt.ProviderRequests(t.Context(), p.requests[0].PromptScopeID)
	if err != nil || len(records) != 2 {
		t.Fatalf("records=%v err=%v", records, err)
	}
	for _, record := range records {
		if record.MeasurementPrefix.ExecutionIdentity != record.AttemptID {
			t.Fatalf("attempt %s reused measurement identity %s", record.AttemptID, record.MeasurementPrefix.ExecutionIdentity)
		}
	}
}

func (p *replayRecoveryProvider) PrepareRequest(_ context.Context, req provider.Request) (provider.PreparedRequest, error) {
	return p.prepare(req, "frozen"), nil
}
func (p *replayRecoveryProvider) prepare(req provider.Request, fingerprint string) *replayRecoveryHandle {
	h := &replayRecoveryHandle{owner: p, req: req, fingerprint: fingerprint}
	p.handles = append(p.handles, h)
	return h
}
func (h *replayRecoveryHandle) Stream(ctx context.Context) (<-chan provider.StreamEvent, error) {
	if h.used || h.closed {
		return nil, errors.New("single-use violation")
	}
	h.used = true
	return h.owner.recoveryProvider.Stream(ctx, h.req)
}
func (h *replayRecoveryHandle) TokenEstimate() provider.TokenEstimate { return preparedEstimate(10) }
func (h *replayRecoveryHandle) PayloadFingerprint() string            { return h.fingerprint }
func (h *replayRecoveryHandle) Close() error {
	h.closed = true
	if h.owner.failClose {
		return errors.New("close failed")
	}
	return nil
}
func (h *replayRecoveryHandle) PrepareRetry(_ context.Context, req provider.Request) (provider.PreparedRequest, error) {
	if !h.closed {
		return nil, errors.New("attempt not closed before replay")
	}
	fingerprint := h.fingerprint
	if h.owner.drift {
		fingerprint = "changed"
	}
	return h.owner.prepare(req, fingerprint), nil
}
func TestProviderRecoveryUsesFreshSingleUsePreparationsAndRejectsDrift(t *testing.T) {
	for _, mode := range []string{"recover", "drift", "close"} {
		t.Run(mode, func(t *testing.T) {
			p := &replayRecoveryProvider{recoveryProvider: recoveryProvider{failCount: 1}, drift: mode == "drift", failClose: mode == "close"}
			e := newTestEngine(p, &event.Recorder{})
			e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
			got := e.Run(t.Context(), "work")
			if mode == "recover" {
				if got.Status != engine.Completed || len(p.requests) != 2 || len(p.handles) != 2 {
					t.Fatalf("result=%+v requests=%d handles=%d", got, len(p.requests), len(p.handles))
				}
			} else if got.Status != engine.Failed || len(p.requests) != 1 {
				t.Fatalf("result=%+v requests=%d", got, len(p.requests))
			}
			for _, h := range p.handles {
				if !h.closed {
					t.Fatal("preparation leaked")
				}
			}
		})
	}
}

func TestProviderRecoveryPermitsDeclaredReadOnlyHostedRequests(t *testing.T) {
	p := &recoveryProvider{failCount: 1}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
	e.Options.HostedToolDefinitions = []provider.HostedToolDefinition{{Name: "search", Type: "web_search", ReplaySafe: true}}
	got := e.Run(t.Context(), "work")
	if got.Status != engine.Completed || len(p.requests) != 2 || !p.requests[1].HostedTools[0].ReplaySafe {
		t.Fatalf("result=%+v requests=%+v", got, p.requests)
	}
}

type usageRecoveryProvider struct{ calls int }

func (p *usageRecoveryProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	p.calls++
	events := make(chan provider.StreamEvent, 4)
	events <- provider.StreamEvent{Type: provider.UsageEvent, Usage: provider.Usage{Available: true, InputTokens: 10, OutputTokens: 2}}
	if p.calls == 1 {
		events <- provider.StreamEvent{Type: provider.Error, Err: providerretry.WithAdvice(errors.New("interrupted"), providerretry.Advice{Category: providerretry.Interrupted})}
	} else {
		events <- provider.StreamEvent{Type: provider.Delta, Text: "ok"}
		events <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	}
	close(events)
	return events, nil
}
func TestProviderRecoveryCountsKnownUsageOncePerPhysicalAttempt(t *testing.T) {
	p := &usageRecoveryProvider{}
	rec := &event.Recorder{}
	e := newTestEngine(p, rec)
	e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
	got := e.Run(t.Context(), "work")
	if got.Status != engine.Completed {
		t.Fatal(got.Err)
	}
	responses, err := e.Prompt.ProviderResponses(t.Context(), "run")
	if err != nil || len(responses) != 2 {
		t.Fatalf("responses=%+v err=%v", responses, err)
	}
	if responses[0].AttemptID == responses[1].AttemptID || responses[0].InputTokens != 10 || responses[1].InputTokens != 10 {
		t.Fatalf("attempt usage=%+v", responses)
	}
	totals := int64(0)
	for _, ev := range rec.Events {
		if ev.Type == event.ProviderUsage {
			if status, ok := engine.ProviderUsageContextStatusFromMetadata(ev.Metadata); ok {
				totals += status.Usage.InputTokens
			}
		}
	}
	if totals != 20 {
		t.Fatalf("canonical usage=%d", totals)
	}
}
