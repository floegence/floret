package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/contextpolicy"
)

func TestModelGatewayExpandedAttachmentsRequirePreparedRequests(t *testing.T) {
	capabilities := runtimeGatewayCapabilities()
	capabilities.AttachmentPayload = ModelGatewayAttachmentPayloadExpanded
	direct := runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
		return runtimeGatewayEvents("unused"), nil
	})
	if err := capabilities.validate(direct); err == nil {
		t.Fatal("expected expanded attachment gateway without preparer to fail")
	}
	prepared := &recordingPreparedModelGateway{}
	if err := capabilities.validate(prepared); err != nil {
		t.Fatalf("prepared expanded attachment gateway: %v", err)
	}
	capabilities.AttachmentPayload = "unknown"
	if err := capabilities.validate(prepared); err == nil {
		t.Fatal("expected unknown attachment payload mode to fail")
	}
}

func TestPreparedModelGatewayConsumesExactPreparedRequestAndRecordsFingerprint(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	gateway := &recordingPreparedModelGateway{}
	capabilities := runtimeGatewayCapabilities()
	capabilities.AttachmentPayload = ModelGatewayAttachmentPayloadExpanded
	host, err := newTestHost(t, providerHostOptions{
		Config:                   runtimeGatewayConfig("prepared gateway"),
		ModelGateway:             gateway,
		ModelGatewayIdentity:     runtimeGatewayIdentity("prepared-model"),
		ModelGatewayCapabilities: capabilities,
		Store:                    store,
		IDGenerator:              deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	stats := &MessageAttachmentTextStats{UnicodeCodePointCount: 11, LogicalLineCount: 2}
	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1",
		Input: TurnInput{Attachments: []MessageAttachment{{
			ResourceRef: "resource:v1:notes", Name: "notes.txt", MIMEType: "text/plain", SizeBytes: 12, TextStats: stats,
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "prepared response" {
		t.Fatalf("result = %#v", result)
	}
	gateway.mu.Lock()
	prepareCalls := gateway.prepareCalls
	directCalls := gateway.directCalls
	requests := append([]ModelRequest(nil), gateway.requests...)
	handles := append([]*recordingPreparedModelRequest(nil), gateway.handles...)
	gateway.mu.Unlock()
	if prepareCalls != 1 || directCalls != 0 || len(requests) != 1 || len(handles) != 1 {
		t.Fatalf("gateway calls prepare=%d direct=%d requests=%d handles=%d", prepareCalls, directCalls, len(requests), len(handles))
	}
	attachment := requests[0].Messages[len(requests[0].Messages)-1].Attachments[0]
	if attachment.ResourceRef != "resource:v1:notes" || attachment.TextStats == nil || attachment.TextStats.UnicodeCodePointCount != 11 || attachment.TextStats.LogicalLineCount != 2 {
		t.Fatalf("prepared request attachment = %#v", attachment)
	}
	handles[0].mu.Lock()
	streamCalls := handles[0].streamCalls
	closeCalls := handles[0].closeCalls
	handles[0].mu.Unlock()
	if streamCalls != 1 || closeCalls != 1 {
		t.Fatalf("prepared handle lifecycle stream=%d close=%d", streamCalls, closeCalls)
	}
	if err := handles[0].Close(); err != nil {
		t.Fatal(err)
	}
	handles[0].mu.Lock()
	closeCalls = handles[0].closeCalls
	handles[0].mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("prepared handle double close count = %d", closeCalls)
	}
	records, err := store.prompt.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ProviderPayloadHash != "sha256:prepared-payload-1" {
		t.Fatalf("provider request records = %#v", records)
	}
}

func TestDescriptorOnlyGatewayKeepsLegacyDirectStreamEvenWhenPreparerExists(t *testing.T) {
	gateway := &recordingPreparedModelGateway{}
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("descriptor gateway"), ModelGateway: gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("descriptor-model"), ModelGatewayCapabilities: runtimeGatewayCapabilities(),
		Store: NewMemoryStore(), IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(context.Background(), CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(context.Background(), RunTurnRequest{
		RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"},
	})
	if err != nil || result.Status != TurnStatusCompleted || result.Output != "direct response" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	gateway.mu.Lock()
	prepareCalls, directCalls := gateway.prepareCalls, gateway.directCalls
	gateway.mu.Unlock()
	if prepareCalls != 0 || directCalls != 1 {
		t.Fatalf("descriptor gateway calls prepare=%d direct=%d", prepareCalls, directCalls)
	}
}

func TestDescriptorOnlyGatewayAttachmentEstimateBoundsSerializedRequestBytes(t *testing.T) {
	adapter := modelGatewayProvider{
		gateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			return runtimeGatewayEvents("unused"), nil
		}),
		identity: runtimeGatewayIdentity("descriptor-estimate-model"),
	}
	tests := map[string]session.MessageAttachment{
		"ASCII": {ResourceRef: "resource:notes", Name: "notes.txt", MIMEType: "text/plain", SizeBytes: 12},
		"CJK":   {ResourceRef: "resource:中文", Name: "说明文档.txt", MIMEType: "text/plain", SizeBytes: 24},
		"emoji": {ResourceRef: "resource:emoji", Name: "notes-😀.txt", MIMEType: "text/plain", SizeBytes: 16},
		"maximum descriptor": {
			ResourceRef: strings.Repeat("r", session.MaxMessageAttachmentResourceBytes),
			Name:        strings.Repeat("n", session.MaxMessageAttachmentNameRunes),
			MIMEType:    strings.Repeat("m", session.MaxMessageAttachmentMIMETypeBytes),
			SizeBytes:   session.MaxMessageAttachmentSizeBytes,
		},
	}
	for name, attachment := range tests {
		t.Run(name, func(t *testing.T) {
			req := provider.Request{
				RunID: "run", ThreadID: "thread", TurnID: "turn", TraceID: "trace", PromptScopeID: "scope", Step: 2,
				Messages:        []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "inspect", Attachments: []session.MessageAttachment{attachment}}},
				Tools:           []provider.ToolDefinition{{Name: "read", Description: "read a resource", InputSchema: map[string]any{"type": "object"}}},
				HostedTools:     []provider.HostedToolDefinition{{Name: "search", Type: "web", Options: map[string]any{"region": "global"}}},
				MaxOutputTokens: 4096,
				PreviousState:   &provider.State{Kind: "response", ID: "state"},
				Labels:          provider.RequestLabels{Correlation: map[string]string{"request": "test"}, Host: map[string]string{"surface": "flower"}},
			}
			modelReq, err := adapter.modelRequest(req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(modelReq)
			if err != nil {
				t.Fatal(err)
			}
			estimate, err := adapter.EstimateTokens(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if estimate.EstimatedInputTokens < int64(len(raw)) || estimate.Source != "model_gateway_request_json_utf8_byte_upper_bound" ||
				estimate.Method != provider.TokenEstimateGenericPayload || estimate.Confidence != provider.EstimateConservative ||
				estimate.Coverage != provider.TokenEstimateCoverageComplete || estimate.MessageTokens <= 0 ||
				estimate.PrefixTokens+estimate.MessageTokens+estimate.ToolDefinitionTokens != estimate.EstimatedInputTokens {
				t.Fatalf("estimate=%#v serialized_bytes=%d", estimate, len(raw))
			}
		})
	}

	withoutAttachments := provider.Request{Messages: []session.Message{{Role: session.User, Content: "legacy text estimate"}}}
	want, err := provider.GenericRequestEstimate(withoutAttachments)
	if err != nil {
		t.Fatal(err)
	}
	got, err := adapter.EstimateTokens(context.Background(), withoutAttachments)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("descriptor-only request without attachments estimate=%#v want legacy=%#v", got, want)
	}
}

func TestDescriptorOnlyGatewayAttachmentEstimateIncreasesPressureAfterNativeUsageAnchor(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		requests = append(requests, req)
		events := make(chan ModelEvent, 3)
		events <- ModelEvent{Type: ModelEventUsage, Usage: ProviderUsage{
			InputTokens: 128, WindowInputTokens: 128, OutputTokens: 8, TotalTokens: 136, Available: true,
		}}
		events <- ModelEvent{Type: ModelEventDelta, Text: "response"}
		events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("descriptor anchor"), ModelGateway: gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("descriptor-anchor-model"), ModelGatewayCapabilities: runtimeGatewayCapabilities(),
		Store: store, IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{
			Text: "establish native anchor",
			Attachments: []MessageAttachment{{
				ResourceRef: "resource:anchor", Name: "anchor.txt", MIMEType: "text/plain", SizeBytes: 6,
			}},
		},
	}); err != nil || result.Status != TurnStatusCompleted {
		t.Fatalf("anchor result=%#v err=%v", result, err)
	}
	if result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID: "turn-2", ThreadID: "thread", TurnID: "turn-2",
		Input: TurnInput{Attachments: []MessageAttachment{{
			ResourceRef: strings.Repeat("r", MaxMessageAttachmentResourceRefBytes),
			Name:        strings.Repeat("n", MaxMessageAttachmentNameRunes),
			MIMEType:    "application/octet-stream", SizeBytes: MaxMessageAttachmentSizeBytes,
		}}},
	}); err != nil || result.Status != TurnStatusCompleted {
		t.Fatalf("attachment result=%#v err=%v", result, err)
	}
	records, err := store.prompt.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || len(requests) != 2 {
		t.Fatalf("records=%d requests=%d", len(records), len(requests))
	}
	second := records[1]
	raw, err := json.Marshal(requests[1])
	if err != nil {
		t.Fatal(err)
	}
	componentTotal := second.RequestEstimate.PrefixTokens + second.RequestEstimate.MessageTokens + second.RequestEstimate.ToolDefinitionTokens
	if second.RequestEstimate.EstimatedInputTokens < int64(len(raw)) || componentTotal != second.RequestEstimate.EstimatedInputTokens {
		t.Fatalf("attachment estimate=%#v serialized_bytes=%d", second.RequestEstimate, len(raw))
	}
	if second.ProjectedPressure.Source != contextpolicy.PressureSourceUsageAnchoredDelta ||
		second.ProjectedPressure.ProjectedInputTokens <= 128 ||
		second.ProjectedPressure.ProjectedInputTokens <= records[0].ProjectedPressure.ProjectedInputTokens {
		t.Fatalf("attachment pressure did not grow from native anchor: first=%#v second=%#v", records[0].ProjectedPressure, second.ProjectedPressure)
	}
}

func TestDescriptorOnlyGatewayAttachmentEstimateDrivesProjectedPressure(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var streamed ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		streamed = req
		return runtimeGatewayEvents("direct response"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("descriptor pressure"), ModelGateway: gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("descriptor-pressure-model"), ModelGatewayCapabilities: runtimeGatewayCapabilities(),
		Store: store, IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1",
		Input: TurnInput{Attachments: []MessageAttachment{{
			ResourceRef: "resource:v1:pressure", Name: "pressure.txt", MIMEType: "text/plain", SizeBytes: 8,
		}}},
	})
	if err != nil || result.Status != TurnStatusCompleted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	raw, err := json.Marshal(streamed)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.prompt.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("provider request records = %#v", records)
	}
	record := records[0]
	if record.RequestEstimate.EstimatedInputTokens < int64(len(raw)) ||
		record.RequestEstimate.Source != "model_gateway_request_json_utf8_byte_upper_bound" ||
		record.RequestEstimate.Method != provider.TokenEstimateGenericPayload ||
		record.RequestEstimate.Confidence != contextpolicy.EstimateConservative ||
		record.ProjectedPressure.ProjectedInputTokens != record.RequestEstimate.EstimatedInputTokens ||
		record.ProjectedPressure.EstimateMethod != provider.TokenEstimateGenericPayload ||
		record.ProjectedPressure.Confidence != contextpolicy.EstimateConservative {
		t.Fatalf("provider request record=%#v serialized_bytes=%d", record, len(raw))
	}
}

func TestPreparedModelGatewayHandleClosesWhenStoreCancelsTurn(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	gateway := newBlockingPreparedModelGateway()
	capabilities := runtimeGatewayCapabilities()
	capabilities.AttachmentPayload = ModelGatewayAttachmentPayloadExpanded
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("blocking prepared gateway"), ModelGateway: gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("blocking-model"), ModelGatewayCapabilities: capabilities,
		Store: store, IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, runErr := host.RunTurn(ctx, RunTurnRequest{
			RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"},
		})
		runDone <- runErr
	}()
	select {
	case <-gateway.started:
	case <-time.After(2 * time.Second):
		t.Fatal("prepared stream did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Store.Close did not cancel prepared stream")
	}
	select {
	case err := <-runDone:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("RunTurn err=%v, want cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled RunTurn did not return")
	}
	gateway.handle.mu.Lock()
	streamCalls, closeCalls := gateway.handle.streamCalls, gateway.handle.closeCalls
	gateway.handle.mu.Unlock()
	if streamCalls != 1 || closeCalls != 1 {
		t.Fatalf("Store.Close handle lifecycle stream=%d close=%d", streamCalls, closeCalls)
	}
}

func TestPreparedModelGatewayRejectsIncompleteEstimateAndClosesHandle(t *testing.T) {
	gateway := &recordingPreparedModelGateway{estimate: ModelRequestTokenEstimate{
		EstimatedInputTokens: 100,
		Source:               "incomplete",
		Method:               "provider_rendered_payload_estimate",
		Confidence:           "conservative",
	}}
	store := NewMemoryStore()
	capabilities := runtimeGatewayCapabilities()
	capabilities.AttachmentPayload = ModelGatewayAttachmentPayloadExpanded
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("prepared gateway"), ModelGateway: gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("prepared-model"), ModelGatewayCapabilities: capabilities,
		Store: store, IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(context.Background(), CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, runErr := host.RunTurn(context.Background(), RunTurnRequest{
		RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"},
	})
	if runErr == nil || result.Status != TurnStatusFailed {
		t.Fatalf("result=%#v err=%v", result, runErr)
	}
	gateway.mu.Lock()
	handles := append([]*recordingPreparedModelRequest(nil), gateway.handles...)
	gateway.mu.Unlock()
	if len(handles) != 1 {
		t.Fatalf("handles = %d", len(handles))
	}
	handles[0].mu.Lock()
	streamCalls, closeCalls := handles[0].streamCalls, handles[0].closeCalls
	handles[0].mu.Unlock()
	if streamCalls != 0 || closeCalls != 1 {
		t.Fatalf("invalid estimate handle lifecycle stream=%d close=%d", streamCalls, closeCalls)
	}
}

type recordingPreparedModelGateway struct {
	mu           sync.Mutex
	prepareCalls int
	directCalls  int
	requests     []ModelRequest
	handles      []*recordingPreparedModelRequest
	estimate     ModelRequestTokenEstimate
}

func (g *recordingPreparedModelGateway) StreamModel(context.Context, ModelRequest) (<-chan ModelEvent, error) {
	g.mu.Lock()
	g.directCalls++
	g.mu.Unlock()
	return runtimeGatewayEvents("direct response"), nil
}

func (g *recordingPreparedModelGateway) PrepareModelRequest(_ context.Context, req ModelRequest) (PreparedModelRequest, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prepareCalls++
	g.requests = append(g.requests, req)
	estimate := g.estimate
	if estimate.Source == "" {
		estimate = ModelRequestTokenEstimate{
			EstimatedInputTokens: 100,
			Source:               "prepared_gateway_test",
			Method:               "provider_rendered_payload_estimate",
			Confidence:           "conservative",
			Coverage:             ModelRequestTokenEstimateCoverageComplete,
		}
	}
	handle := &recordingPreparedModelRequest{
		estimate:    estimate,
		fingerprint: "sha256:prepared-payload-" + string(rune('0'+g.prepareCalls)),
	}
	g.handles = append(g.handles, handle)
	return handle, nil
}

type recordingPreparedModelRequest struct {
	mu          sync.Mutex
	estimate    ModelRequestTokenEstimate
	fingerprint string
	streamCalls int
	closeCalls  int
	closed      bool
}

func (p *recordingPreparedModelRequest) StreamModel(context.Context) (<-chan ModelEvent, error) {
	p.mu.Lock()
	p.streamCalls++
	p.mu.Unlock()
	return runtimeGatewayEvents("prepared response"), nil
}

func (p *recordingPreparedModelRequest) TokenEstimate() ModelRequestTokenEstimate {
	return p.estimate
}

func (p *recordingPreparedModelRequest) RenderedPayloadFingerprint() string {
	return p.fingerprint
}

func (p *recordingPreparedModelRequest) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.closeCalls++
	return nil
}

var _ ModelGateway = (*recordingPreparedModelGateway)(nil)
var _ ModelGatewayRequestPreparer = (*recordingPreparedModelGateway)(nil)
var _ PreparedModelRequest = (*recordingPreparedModelRequest)(nil)

type blockingPreparedModelGateway struct {
	started chan struct{}
	handle  *blockingPreparedModelRequest
}

func newBlockingPreparedModelGateway() *blockingPreparedModelGateway {
	return &blockingPreparedModelGateway{started: make(chan struct{}), handle: &blockingPreparedModelRequest{}}
}

func (g *blockingPreparedModelGateway) StreamModel(context.Context, ModelRequest) (<-chan ModelEvent, error) {
	return nil, errors.New("direct stream is unavailable")
}

func (g *blockingPreparedModelGateway) PrepareModelRequest(context.Context, ModelRequest) (PreparedModelRequest, error) {
	g.handle.started = g.started
	return g.handle, nil
}

type blockingPreparedModelRequest struct {
	mu          sync.Mutex
	started     chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	streamCalls int
	closeCalls  int
}

func (p *blockingPreparedModelRequest) StreamModel(ctx context.Context) (<-chan ModelEvent, error) {
	p.mu.Lock()
	p.streamCalls++
	p.mu.Unlock()
	p.startOnce.Do(func() { close(p.started) })
	events := make(chan ModelEvent)
	go func() {
		<-ctx.Done()
		close(events)
	}()
	return events, nil
}

func (p *blockingPreparedModelRequest) TokenEstimate() ModelRequestTokenEstimate {
	return ModelRequestTokenEstimate{
		EstimatedInputTokens: 100, Source: "blocking_prepared_test", Method: "provider_rendered_payload_estimate",
		Confidence: "conservative", Coverage: ModelRequestTokenEstimateCoverageComplete,
	}
}

func (p *blockingPreparedModelRequest) RenderedPayloadFingerprint() string {
	return "sha256:blocking-prepared"
}

func (p *blockingPreparedModelRequest) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closeCalls++
		p.mu.Unlock()
	})
	return nil
}
