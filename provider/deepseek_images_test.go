package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func TestDeepSeekVisionPreparesImagesWithoutPersistingPayload(t *testing.T) {
	const dataURL = "data:image/png;base64,AQID"
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"vision-response\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"a picture\"}]}]}}\n\n")
	}))
	defer server.Close()
	calls := 0
	data := []byte{1, 2, 3}
	options := provider.DeepSeekOptions{Model: "deepseek-v4-flash-vision-exp", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "vision", ResolveAttachment: func(ctx context.Context, a provider.Attachment) ([]byte, error) {
		calls++
		if a.ResourceRef != "opaque-photo" || a.MIMEType != "image/png" {
			t.Errorf("unexpected descriptor: %+v", a)
		}
		return data, ctx.Err()
	}}
	gateway, err := provider.NewDeepSeek(options)
	if err != nil {
		t.Fatal(err)
	}
	if gateway.Capabilities().AttachmentPayload != provider.AttachmentExpanded {
		t.Fatal("image expansion is not advertised")
	}
	req := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "What is this?", Attachments: []provider.Attachment{{ResourceRef: "opaque-photo", Name: "photo.png", MIMEType: "image/png", SizeBytes: 3}}}}}
	prepared, err := gateway.(provider.RequestPreparer).Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	data = []byte{4, 5, 6}
	stream, err := prepared.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var state *provider.State
	for event := range stream {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.ResponseState != nil {
			state = event.ResponseState
		}
	}
	if calls != 1 || len(bodies) != 1 || !strings.Contains(bodies[0], dataURL) || strings.Contains(bodies[0], "image_reference") {
		t.Fatalf("unexpected image request: calls=%d bodies=%v", calls, bodies)
	}
	if prepared.TokenEstimate().EstimatedInputTokens < int64(len(bodies[0])) {
		t.Fatal("estimate excludes image payload")
	}
	raw, _ := json.Marshal(state)
	if state == nil || strings.Contains(string(raw), "base64") || !strings.Contains(string(raw), "opaque-photo") {
		t.Fatalf("image state must retain only reference and digest: %s", raw)
	}
	// Restart with the same model and authorized content, retaining the exact state.
	data = []byte{1, 2, 3}
	gateway, err = provider.NewDeepSeek(options)
	if err != nil {
		t.Fatal(err)
	}
	req.PreviousState = state
	req.Messages = append(req.Messages, provider.Message{Role: provider.RoleAssistant, Text: "a picture"}, provider.Message{Role: provider.RoleUser, Text: "More detail?"})
	collectDeepSeek(t, gateway, req)
	if calls != 2 || !strings.Contains(bodies[1], dataURL) {
		t.Fatal("restart did not resolve and replay the image")
	}
	data = []byte{4, 5, 6}
	if _, err = gateway.(provider.RequestPreparer).Prepare(context.Background(), req); err == nil {
		t.Fatal("changed image bytes reused old provider state")
	}
}

func TestDeepSeekImageCapabilityAndAuthorityFailures(t *testing.T) {
	denied := errors.New("authorization denied")
	for _, tc := range []struct {
		name, model, mime string
		resolver          bool
		cancel            bool
		resolveErr        error
	}{
		{name: "text model", model: "deepseek-v4-pro", mime: "image/png", resolver: true},
		{name: "missing resolver", model: "deepseek-v4-flash-vision-exp", mime: "image/png"},
		{name: "unsupported media", model: "deepseek-v4-flash-vision-exp", mime: "audio/wav", resolver: true},
		{name: "denied", model: "deepseek-v4-flash-vision-exp", mime: "image/png", resolver: true, resolveErr: denied},
		{name: "cancelled", model: "deepseek-v4-flash-vision-exp", mime: "image/png", resolver: true, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			options := provider.DeepSeekOptions{Model: tc.model, BaseURL: "https://api.deepseek.com", APIKey: "test", StateCompatibilityKey: "image"}
			if tc.resolver {
				options.ResolveAttachment = func(context.Context, provider.Attachment) ([]byte, error) { calls++; return []byte{1}, tc.resolveErr }
			}
			gateway, err := provider.NewDeepSeek(options)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			req := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Attachments: []provider.Attachment{{ResourceRef: "photo", Name: "photo", MIMEType: tc.mime}}}}}
			if _, err = gateway.(provider.RequestPreparer).Prepare(ctx, req); err == nil {
				t.Fatal("unsupported or unauthorized image accepted")
			}
			if tc.resolveErr != nil && !errors.Is(err, denied) {
				t.Fatalf("authorization error lost: %v", err)
			}
			if tc.resolveErr == nil && calls != 0 {
				t.Fatal("resolved bytes before validating capability or cancellation")
			}
		})
	}
}

func TestDeepSeekVisionToolResultImageRoundTrip(t *testing.T) {
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"computer-response\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}]}}\n\n")
	}))
	defer server.Close()
	data := []byte{9, 8, 7}
	resolverCalls := 0
	options := provider.DeepSeekOptions{
		Model: "deepseek-v4-flash-vision-exp", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "tool-image",
		ResolveAttachment: func(_ context.Context, attachment provider.Attachment) ([]byte, error) {
			resolverCalls++
			if attachment.ResourceRef != "screen-1" {
				t.Fatalf("unexpected tool attachment: %+v", attachment)
			}
			return data, nil
		},
	}
	gateway, err := provider.NewDeepSeek(options)
	if err != nil {
		t.Fatal(err)
	}
	request := provider.Request{
		RunID: "run", PromptScopeID: "scope",
		Messages: []provider.Message{
			{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{{ID: "call-1", Name: "computer.screenshot", Args: "{}"}}},
			{Role: provider.RoleTool, ToolResult: &provider.ToolResult{CallID: "call-1", ToolName: "computer.screenshot", Text: "screenshot captured", Attachments: []provider.Attachment{{ResourceRef: "screen-1", Name: "screen.png", MIMEType: "image/png", SizeBytes: int64(len(data))}}}},
		},
	}
	events := collectDeepSeek(t, gateway, request)
	var state *provider.State
	for _, event := range events {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.ResponseState != nil {
			state = event.ResponseState
		}
	}
	if state == nil || resolverCalls != 1 || len(bodies) != 1 {
		t.Fatalf("tool image was not resolved: resolver=%d bodies=%d state=%v", resolverCalls, len(bodies), state)
	}
	if !strings.Contains(bodies[0], "function_call_output") || !strings.Contains(bodies[0], "input_image") || strings.Contains(bodies[0], "image_reference") {
		t.Fatalf("tool result image was not expanded: %s", bodies[0])
	}
	stateBytes, _ := json.Marshal(state)
	if strings.Contains(string(stateBytes), "base64") || !strings.Contains(string(stateBytes), "screen-1") {
		t.Fatalf("tool image state must retain only the opaque reference: %s", stateBytes)
	}
	data = []byte{9, 8, 7}
	request.PreviousState = state
	request.Messages = append(request.Messages, provider.Message{Role: provider.RoleAssistant, Text: "done"}, provider.Message{Role: provider.RoleUser, Text: "continue"})
	collectDeepSeek(t, gateway, request)
	if resolverCalls != 2 || len(bodies) != 2 || !strings.Contains(bodies[1], "input_image") {
		t.Fatalf("tool image was not replayed: resolver=%d bodies=%d", resolverCalls, len(bodies))
	}
}
