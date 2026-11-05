package provider_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func TestDeepSeekContinuationPreservesAdjacentAssistantHistory(t *testing.T) {
	for _, prior := range []provider.Message{
		{Role: provider.RoleAssistant, Reasoning: "retained reasoning from an interrupted response"},
		{Role: provider.RoleAssistant, Text: "retained prose from an interrupted response"},
	} {
		t.Run(fmt.Sprintf("reasoning=%t", prior.Reasoning != ""), func(t *testing.T) {
			var bodies []json.RawMessage
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct{ Input json.RawMessage }
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				bodies = append(bodies, body.Input)
				w.Header().Set("Content-Type", "text/event-stream")
				output := `[{"type":"function_call","id":"item","call_id":"call","name":"inspect","arguments":"{}"}]`
				if len(bodies) > 1 {
					output = `[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]`
				}
				fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response\",\"status\":\"completed\",\"output\":%s}}\n\n", output)
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "fixture", StateCompatibilityKey: "fixture"})
			if err != nil {
				t.Fatal(err)
			}
			req := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "inspect once"}, prior}}
			for _, event := range collectDeepSeek(t, gateway, req) {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				if event.ResponseState != nil {
					req.PreviousState = event.ResponseState
				}
			}
			if req.PreviousState == nil {
				t.Fatal("missing continuation state")
			}
			// Persistence and RPC retain this public opaque envelope verbatim.
			encoded, err := json.Marshal(req.PreviousState)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &req.PreviousState); err != nil {
				t.Fatal(err)
			}
			req.Messages = append(req.Messages,
				provider.Message{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{{ID: "call", Name: "inspect", Args: "{}"}}},
				provider.Message{Role: provider.RoleTool, ToolResult: &provider.ToolResult{CallID: "call", ToolName: "inspect", Text: "observed once"}})
			for _, event := range collectDeepSeek(t, gateway, req) {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
			}
			if len(bodies) != 2 || !strings.Contains(string(bodies[1]), "retained") || strings.Count(string(bodies[1]), `"type":"function_call"`) != 1 || !strings.Contains(string(bodies[1]), "observed once") {
				t.Fatalf("continuation lost or duplicated history: %s", bodies)
			}
			req.Messages[1].Reasoning, req.Messages[1].Text = "changed", ""
			if _, err := gateway.Stream(t.Context(), req); err == nil {
				t.Fatal("accepted changed canonical history")
			}
		})
	}
}

func TestDeepSeekContinuationAcceptsExactBoundariesInsideRuntimeProjection(t *testing.T) {
	for _, tc := range []struct {
		name, oldReasoning, oldText, newReasoning, newText string
	}{
		{name: "reasoning and call", oldReasoning: "retained reasoning"},
		{name: "two reasoning fragments and call", oldReasoning: "retained 思考\n", newReasoning: "new \\\"thought\""},
		{name: "reasoning and text", oldReasoning: "retained reasoning", newReasoning: "new reasoning", newText: "checking"},
		{name: "two text fragments and call", oldText: "retained text\n", newText: "new text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bodies []json.RawMessage
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct{ Input json.RawMessage }
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				bodies = append(bodies, body.Input)
				w.Header().Set("Content-Type", "text/event-stream")
				var output []map[string]any
				if len(bodies) == 1 {
					if tc.newReasoning != "" {
						output = append(output, map[string]any{"type": "reasoning", "id": "reason", "content": []map[string]string{{"type": "reasoning_text", "text": tc.newReasoning}}})
					}
					if tc.newText != "" {
						output = append(output, map[string]any{"type": "message", "id": "message", "role": "assistant", "content": []map[string]string{{"type": "output_text", "text": tc.newText}}})
					}
					output = append(output, map[string]any{"type": "function_call", "id": "item", "call_id": "call", "name": "inspect", "arguments": "{}"})
				} else {
					output = []map[string]any{{"type": "message", "role": "assistant", "content": []map[string]string{{"type": "output_text", "text": "done"}}}}
				}
				raw, _ := json.Marshal(output)
				fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response\",\"status\":\"completed\",\"output\":%s}}\n\n", raw)
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "fixture", StateCompatibilityKey: "fixture"})
			if err != nil {
				t.Fatal(err)
			}
			req := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{
				{Role: provider.RoleUser, Text: "inspect once"},
				{Role: provider.RoleAssistant, Reasoning: tc.oldReasoning, Text: tc.oldText},
			}}
			for _, event := range collectDeepSeek(t, gateway, req) {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				if event.ResponseState != nil {
					req.PreviousState = event.ResponseState
				}
			}
			if req.PreviousState == nil {
				t.Fatal("missing continuation state")
			}
			// Runtime groups adjacent canonical assistant fragments into one message.
			req.Messages[1].Reasoning += tc.newReasoning
			req.Messages[1].Text += tc.newText
			req.Messages[1].ToolCalls = []provider.ToolCall{{ID: "call", Name: "inspect", Args: "{}"}}
			req.Messages = append(req.Messages, provider.Message{Role: provider.RoleTool, ToolResult: &provider.ToolResult{CallID: "call", ToolName: "inspect", Text: "observed once"}})
			for _, event := range collectDeepSeek(t, gateway, req) {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
			}
			if len(bodies) != 2 || strings.Count(string(bodies[1]), `"call_id":"call"`) != 2 || !strings.Contains(string(bodies[1]), "observed once") {
				t.Fatalf("continuation lost or duplicated history: %s", bodies)
			}
			if tc.oldReasoning != "" {
				req.Messages[1].Reasoning = "changed" + tc.newReasoning
			} else {
				req.Messages[1].Text = "changed" + tc.newText
			}
			if _, err := gateway.Stream(t.Context(), req); err == nil {
				t.Fatal("accepted changed bound history")
			}
		})
	}
}
