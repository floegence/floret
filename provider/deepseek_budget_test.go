package provider_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestDeepSeekImageBudgetDoesNotCountTransportEncodingAsText(t *testing.T) {
	image := []byte(strings.Repeat("image-fixture", 250000))
	url := "data:image/png;base64," + base64.StdEncoding.EncodeToString(image)
	var sent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
		}
		sent = string(raw)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"budget\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer server.Close()
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash-vision-exp", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "budget", ResolveAttachment: func(context.Context, provider.Attachment) ([]byte, error) { return image, nil }})
	if err != nil {
		t.Fatal(err)
	}
	request := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "Describe the screen", Attachments: []provider.Attachment{{ResourceRef: "screen", MIMEType: "image/png", SizeBytes: int64(len(image))}}}}}
	prepared, err := gateway.(provider.RequestPreparer).Prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	estimate := prepared.TokenEstimate()
	if estimate.EstimatedInputTokens < 1024 || estimate.EstimatedInputTokens > 10000 {
		t.Fatalf("one image must use the documented vision budget, not encoded bytes: %+v", estimate)
	}
	stream, err := prepared.Stream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for event := range stream {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	if !strings.Contains(sent, url) {
		t.Fatal("budgeting changed the transmitted image")
	}
	if prepared.RenderedPayloadFingerprint() != fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(sent))) {
		t.Fatal("fingerprint does not describe the actual transmitted image request")
	}

	// Text that resembles an image payload is still text and must retain its cost.
	request.Messages[0].Attachments = nil
	request.Messages[0].Text = url
	literal, err := gateway.(provider.RequestPreparer).Prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer literal.Close()
	if literal.TokenEstimate().EstimatedInputTokens < int64(len(url)) {
		t.Fatal("literal text was incorrectly discounted")
	}
}

func TestDeepSeekImageBudgetIncludesToolImagesAndRestartedHistory(t *testing.T) {
	data := []byte(strings.Repeat("native-screen", 200000))
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprintf("images_%d", count), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Input []json.RawMessage `json:"input"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				images := 0
				for _, item := range body.Input {
					images += strings.Count(string(item), `"type":"input_image"`)
				}
				if images != count {
					t.Errorf("transmitted images = %d, want %d", images, count)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"budget-history\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}]}}\n\n")
			}))
			defer server.Close()
			options := provider.DeepSeekOptions{Model: "deepseek-v4-flash-vision-exp", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "history-budget", ResolveAttachment: func(context.Context, provider.Attachment) ([]byte, error) { return data, nil }}
			attachments := make([]provider.Attachment, count)
			for i := range attachments {
				attachments[i] = provider.Attachment{ResourceRef: fmt.Sprintf("screen-%d", i), MIMEType: "image/png", SizeBytes: int64(len(data))}
			}
			request := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{
				{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{{ID: "call", Name: "screenshot", Args: "{}"}}},
				{Role: provider.RoleTool, ToolResult: &provider.ToolResult{CallID: "call", ToolName: "screenshot", Text: "captured", Attachments: attachments}},
			}}
			for attempt := 0; attempt < 2; attempt++ {
				gateway, err := provider.NewDeepSeek(options)
				if err != nil {
					t.Fatal(err)
				}
				prepared, err := gateway.(provider.RequestPreparer).Prepare(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = prepared.Close() })
				estimate := prepared.TokenEstimate().EstimatedInputTokens
				if estimate < int64(count*1024) || estimate > int64(count*1024+10000) {
					t.Fatalf("attempt %d: invalid image estimate %d", attempt, estimate)
				}
				stream, err := prepared.Stream(t.Context())
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
				if state == nil {
					t.Fatal("missing continuation state")
				}
				raw, _ := json.Marshal(state)
				if strings.Contains(string(raw), "base64") {
					t.Fatal("image bytes entered durable state")
				}
				request.PreviousState = state
				request.Messages = append(request.Messages, provider.Message{Role: provider.RoleAssistant, Text: "done"}, provider.Message{Role: provider.RoleUser, Text: "continue"})
			}
		})
	}
}

func TestDeepSeekImageBudgetDoesNotDiscountToolSchemaOrArguments(t *testing.T) {
	url := "data:image/png;base64," + strings.Repeat("AAAA", 100000)
	part := map[string]any{"type": "input_image", "image_url": url}
	raw, _ := json.Marshal(part)
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash-vision-exp", BaseURL: "https://example.invalid", APIKey: "test", StateCompatibilityKey: "literal-budget"})
	if err != nil {
		t.Fatal(err)
	}
	request := provider.Request{RunID: "run", PromptScopeID: "scope",
		Messages: []provider.Message{
			{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{{ID: "call", Name: "inspect", Args: string(raw)}}},
			{Role: provider.RoleTool, ToolResult: &provider.ToolResult{CallID: "call", ToolName: "inspect", Text: "done"}},
		},
		Tools: []tools.ToolDefinition{{Name: "inspect", InputSchema: map[string]any{"type": "object", "examples": []any{part}}}},
	}
	prepared, err := gateway.(provider.RequestPreparer).Prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if prepared.TokenEstimate().EstimatedInputTokens < int64(2*len(url)) {
		t.Fatal("ordinary schema or argument content was discounted as visual input")
	}
}
