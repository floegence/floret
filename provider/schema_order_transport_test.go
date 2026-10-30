package provider_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestOfficialOrderedSchemaHTTPFrozenRetries(t *testing.T) {
	for _, kind := range []string{"chat", "anthropic", "deepseek"} {
		t.Run(kind, func(t *testing.T) {
			received := make(chan []byte, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				received <- raw
				w.Header().Set("Content-Type", "text/event-stream")
				switch kind {
				case "chat":
					fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`)
				case "anthropic":
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"id":"m","role":"assistant","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn"}`)
				case "deepseek":
					fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n")
				}
			}))
			defer server.Close()
			var gateway provider.Gateway
			var err error
			switch kind {
			case "chat":
				gateway, err = provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{Provider: "openai", Model: "gpt-4o", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
			case "anthropic":
				gateway, err = provider.NewAnthropic(provider.AnthropicOptions{Provider: "anthropic", Model: "claude-sonnet-4-5", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
			case "deepseek":
				gateway, err = provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test"})
			}
			if err != nil {
				t.Fatal(err)
			}
			schema := map[string]any{"type": "object", "properties": map[string]any{"content": tools.String("body"), "file_path": tools.String("path")}, "required": []string{"file_path", "content"}, "additionalProperties": false}
			req := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "write"}}, Tools: []tools.ToolDefinition{{Name: "write", InputSchema: schema}}, MaxOutputTokens: 1000}
			prepared, err := gateway.(provider.RequestPreparer).Prepare(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			fingerprint := prepared.RenderedPayloadFingerprint()
			schema["required"] = []string{"content", "file_path"}
			drain := func(p provider.PreparedRequest) []byte {
				t.Helper()
				events, err := p.Stream(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				for event := range events {
					if event.Err != nil {
						t.Fatal(event.Err)
					}
				}
				return <-received
			}
			first := drain(prepared)
			if !bytes.Contains(first, []byte(`"properties":{"file_path":`)) || fingerprint != fmt.Sprintf("sha256:%x", sha256.Sum256(first)) {
				t.Fatalf("wire not ordered/frozen: %s", first)
			}
			retryer := prepared.(interface {
				PrepareRetry(context.Context, provider.Request) (provider.PreparedRequest, error)
			})
			retry, err := retryer.PrepareRetry(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first, drain(retry)) {
				t.Fatal("network retry changed frozen bytes")
			}
			fresh, err := retryer.PrepareRetry(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			refit, err := fresh.(provider.PreparedRequestOutputLimiter).WithMaxOutputTokens(t.Context(), 500)
			if err != nil {
				t.Fatal(err)
			}
			refitted := drain(refit)
			var before, after map[string]json.RawMessage
			json.Unmarshal(first, &before)
			json.Unmarshal(refitted, &after)
			if !bytes.Equal(before["tools"], after["tools"]) || refit.RenderedPayloadFingerprint() == fingerprint {
				t.Fatal("refit changed tools or retained stale fingerprint")
			}
		})
	}
}
