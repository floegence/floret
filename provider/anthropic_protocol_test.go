package provider_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestAnthropicPublicCacheReachesFrozenWire(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"msg_test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"cache_read_input_tokens":12000,"cache_creation_input_tokens":1000,"output_tokens":11}}`)
	}))
	defer server.Close()
	gateway, err := provider.NewAnthropic(provider.AnthropicOptions{Provider: "custom-provider-id", Model: "claude", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "account", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
	if err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{true, false} {
		req := provider.Request{RunID: "run", PromptScopeID: "scope", MaxOutputTokens: 128, ReusePolicy: &provider.RequestReusePolicy{PromptCacheEnabled: enabled, PromptCacheNamespace: "ns", ProviderStateAllowed: true}, Messages: []provider.Message{{Role: provider.RoleSystem, Text: "stable system"}, {Role: provider.RoleUser, Text: "hello"}}, Tools: []tools.ToolDefinition{{Name: "tools.inspect", InputSchema: map[string]any{"type": "object"}}}}
		prepared, err := gateway.(provider.RequestPreparer).Prepare(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		stream, err := prepared.Stream(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for e := range stream {
			if e.Err != nil {
				t.Fatal(e.Err)
			}
		}
		var count func(any) int
		count = func(v any) int {
			n := 0
			switch x := v.(type) {
			case map[string]any:
				for k, c := range x {
					if k == "cache_control" {
						n++
					}
					n += count(c)
				}
			case []any:
				for _, c := range x {
					n += count(c)
				}
			}
			return n
		}
		want := 0
		if enabled {
			want = 3
		}
		if n := count(body); n != want {
			t.Fatalf("enabled=%v markers=%d want=%d body=%v", enabled, n, want, body)
		}
	}
}

func TestAnthropicNativeHistoryFrozenReplayAndMeasurement(t *testing.T) {
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, raw)
		w.Header().Set("Content-Type", "text/event-stream")
		events := []string{
			`{"type":"message_start","message":{"id":"msg_native","content":[],"usage":{"input_tokens":7,"cache_read_input_tokens":12000,"cache_creation_input_tokens":1000,"output_tokens":0}}}`,
			`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"consider"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"block"}}`,
			`{"type":"content_block_stop","index":0}`,
			`{"type":"content_block_start","index":1,"content_block":{"type":"text","text":"answer"}}`,
			`{"type":"content_block_stop","index":1}`,
			`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":11}}`,
			`{"type":"message_stop"}`,
		}
		for _, e := range events {
			fmt.Fprintf(w, "data: %s"+string([]byte{10, 10}), e)
		}
	}))
	defer server.Close()
	newGateway := func() provider.Gateway {
		g, e := provider.NewAnthropic(provider.AnthropicOptions{Provider: "custom", Model: "claude", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "account", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
		if e != nil {
			t.Fatal(e)
		}
		return g
	}
	req := provider.Request{RunID: "run", PromptScopeID: "scope", AttemptID: "attempt-a", MaxOutputTokens: 128, ReusePolicy: &provider.RequestReusePolicy{PromptCacheEnabled: true, PromptCacheNamespace: "ns", ProviderStateAllowed: true}, Messages: []provider.Message{{Role: provider.RoleUser, Text: "question"}}}
	prepared, e := newGateway().(provider.RequestPreparer).Prepare(context.Background(), req)
	if e != nil {
		t.Fatal(e)
	}
	prefix := prepared.(provider.PreparedRequestMeasurement).InputMeasurementPrefix()
	stream, e := prepared.Stream(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	var state *provider.State
	var usage provider.Usage
	var text, reasoning string
	for event := range stream {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		switch event.Type {
		case provider.EventDelta:
			text += event.Text
		case provider.EventReasoning:
			reasoning += event.Text
		case provider.EventUsage:
			usage = event.Usage
		case provider.EventDone:
			state = event.ResponseState
		}
	}
	if text != "answer" || reasoning != "consider" || state == nil {
		t.Fatalf("output=%s reasoning=%s state=%v", text, reasoning, state)
	}
	if usage.Measurement == nil || usage.Measurement.Input != prefix || usage.WindowInputTokens != 13007 || usage.OutputTokens != 11 {
		t.Fatalf("usage=%+v", usage)
	}
	if usage.Measurement.ResponseEnd.Fingerprint != "" {
		t.Fatal("summarized thinking cannot certify response-end input")
	}
	raw, _ := json.Marshal(state)
	state = new(provider.State)
	if e = json.Unmarshal(raw, state); e != nil {
		t.Fatal(e)
	}
	req.PreviousState = state
	req.ReusePolicy.PromptCacheEnabled = false
	req.Messages = append(req.Messages, provider.Message{Role: provider.RoleAssistant, Text: text, Reasoning: reasoning}, provider.Message{Role: provider.RoleUser, Text: "next"})
	next, e := newGateway().(provider.RequestPreparer).Prepare(context.Background(), req)
	if e != nil {
		t.Fatal(e)
	}
	if suffix, ok := next.(provider.PreparedRequestMeasurement).MeasurementSuffix(usage.Measurement.Input); !ok || !bytes.Contains(suffix, []byte("next")) {
		t.Fatalf("suffix=%s ok=%v", suffix, ok)
	}
	stream, e = next.Stream(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for event := range stream {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	if !bytes.Contains(bodies[1], []byte("signed-block")) || bytes.Contains(bodies[1], []byte("cache_control")) || bytes.Contains(bodies[1], []byte("floret_reasoning")) {
		t.Fatalf("native replay=%s", bodies[1])
	}
	if bytes.Index(bodies[1], []byte("signed-block")) > bytes.Index(bodies[1], []byte("answer")) {
		t.Fatal("native block order changed")
	}
	req.Messages[1].Text = "tampered"
	if _, e = newGateway().(provider.RequestPreparer).Prepare(context.Background(), req); e == nil {
		t.Fatal("accepted changed canonical output")
	}
}

func TestAnthropicInterruptedStreamNeverPublishesState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, `data: {"type":"message_start","message":{"id":"msg","content":[],"usage":{"input_tokens":9}}}

data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"partial"}}

`)
	}))
	defer server.Close()
	g, e := provider.NewAnthropic(provider.AnthropicOptions{Provider: "custom", Model: "claude", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "account", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
	if e != nil {
		t.Fatal(e)
	}
	stream, e := g.Stream(context.Background(), provider.Request{RunID: "run", PromptScopeID: "scope", MaxOutputTokens: 128, Messages: []provider.Message{{Role: provider.RoleUser, Text: "hello"}}})
	if e != nil {
		t.Fatal(e)
	}
	failed := false
	for event := range stream {
		if event.ResponseState != nil || event.Usage.Measurement != nil {
			t.Fatal("partial stream published state/evidence")
		}
		if event.Err != nil {
			failed = true
		}
	}
	if !failed {
		t.Fatal("missing terminal accepted")
	}
}

func TestAnthropicAttachmentsFreezeOnceAndKeepAttemptIdentity(t *testing.T) {
	var calls atomic.Int32
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, raw)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"msg","content":[{"type":"text","text":"answer"}],"stop_reason":"end_turn","usage":{"input_tokens":9,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"output_tokens":2}}`)
	}))
	defer server.Close()
	options := provider.AnthropicOptions{Provider: "custom", Model: "fixture", BaseURL: server.URL, APIKey: "key", StateCompatibilityKey: "account", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, ResolveAttachment: func(ctx context.Context, a provider.Attachment) (provider.AnthropicAttachment, error) {
		calls.Add(1)
		return provider.AnthropicAttachment{Kind: "document", MIMEType: "text/plain", Data: []byte("authorized document")}, nil
	}}
	gateway, err := provider.NewAnthropic(options)
	if err != nil {
		t.Fatal(err)
	}
	req := provider.Request{RunID: "run", PromptScopeID: "scope", AttemptID: "first", MaxOutputTokens: 128, ReusePolicy: &provider.RequestReusePolicy{ProviderStateAllowed: true}, Messages: []provider.Message{{Role: provider.RoleUser, Text: "inspect", Attachments: []provider.Attachment{{ResourceRef: "opaque", Name: "file.txt", MIMEType: "text/plain", SizeBytes: 19}}}}}
	prepared, err := gateway.(provider.RequestPreparer).Prepare(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	prefix := prepared.(provider.PreparedRequestMeasurement).InputMeasurementPrefix()
	refit, err := prepared.(provider.PreparedRequestOutputLimiter).WithMaxOutputTokens(t.Context(), 64)
	if err != nil {
		t.Fatal(err)
	}
	if refit.(provider.PreparedRequestMeasurement).InputMeasurementPrefix() != prefix || refit.RenderedPayloadFingerprint() == prepared.RenderedPayloadFingerprint() {
		t.Fatal("refit changed input or retained body fingerprint")
	}
	run := func(p provider.PreparedRequest, attempt string) *provider.State {
		stream, e := p.Stream(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		var state *provider.State
		measured := false
		for event := range stream {
			if event.Err != nil {
				t.Fatal(event.Err)
			}
			if event.Type == provider.EventUsage {
				measured = event.Usage.Measurement != nil && event.Usage.Measurement.AttemptID == attempt
			}
			if event.ResponseState != nil {
				state = event.ResponseState
			}
		}
		if !measured {
			t.Fatal("lost request-bound attempt")
		}
		return state
	}
	state := run(refit, "first")
	retry, err := refit.(provider.PreparedRequestReplay).PrepareRetry(t.Context(), provider.Request{AttemptID: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	run(retry, "retry")
	if calls.Load() != 1 || len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("replay changed bytes or reopened attachment")
	}
	raw, _ := json.Marshal(state)
	if bytes.Contains(raw, []byte("authorized document")) {
		t.Fatal("input attachment persisted in native output")
	}
	options.MaxRequestBytes = 32
	gateway, err = provider.NewAnthropic(options)
	if err != nil {
		t.Fatal(err)
	}
	_, err = gateway.(provider.RequestPreparer).Prepare(t.Context(), req)
	var size *provider.RequestByteLimitError
	if !errors.As(err, &size) || !errors.Is(err, provider.ErrContextOverflow) {
		t.Fatalf("byte limit classification=%v", err)
	}
}
