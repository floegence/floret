package provider_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
)

func TestDeepSeekResponsesReplaysRawSearchAndReasoning(t *testing.T) {
	var bodies []map[string]json.RawMessage
	output := `[{"type":"reasoning","id":"rs1","content":[{"type":"reasoning_text","text":"consider"}]},{"type":"web_search_call","id":"ws1","status":"completed","action":{"type":"search","query":"weather"},"opaque_results":"keep-exact"},{"type":"message","id":"m1","role":"assistant","content":[{"type":"output_text","text":"sunny","annotations":[{"type":"url_citation","url":"https://example.com/weather","title":"Weather"}]}]}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Errorf("path = %s", r.URL.Path)
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp1\",\"status\":\"completed\",\"output\":%s,\"usage\":{\"input_tokens\":100,\"input_tokens_details\":{\"cached_tokens\":40},\"output_tokens\":15,\"output_tokens_details\":{\"reasoning_tokens\":5},\"total_tokens\":115}}}\n\n", output)
	}))
	defer server.Close()
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "secret", StateCompatibilityKey: "deepseek:test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	req := provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "weather?"}}, Reasoning: config.ReasoningSelection{Level: config.ReasoningLevelOff}, HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}
	first := collectDeepSeek(t, gateway, req)
	var state *provider.State
	var text string
	var search, source bool
	for _, e := range first {
		if e.Err != nil {
			t.Fatal(e.Err)
		}
		if e.Type == provider.EventDelta {
			text += e.Text
		}
		if e.Type == provider.EventHostedToolCall {
			search = true
		}
		if len(e.Sources) > 0 {
			source = true
		}
		if e.ResponseState != nil {
			state = e.ResponseState
		}
		if e.Type == provider.EventUsage && (e.Usage.InputTokens != 60 || e.Usage.CacheReadTokens != 40 || e.Usage.OutputTokens != 10 || e.Usage.ReasoningTokens != 5) {
			t.Fatalf("usage = %+v", e.Usage)
		}
	}
	if state == nil || text != "sunny" || !search || !source {
		t.Fatalf("events = %+v", first)
	}
	req.PreviousState = state
	req.Messages = append(req.Messages, provider.Message{Role: provider.RoleAssistant, Text: "sunny", Reasoning: "consider"}, provider.Message{Role: provider.RoleUser, Text: "tomorrow?"})
	collectDeepSeek(t, gateway, req)
	for _, b := range bodies {
		for _, key := range []string{"messages", "enable_search", "previous_response_id", "store", "include", "prompt_cache_key"} {
			if _, ok := b[key]; ok {
				t.Errorf("unexpected %s", key)
			}
		}
		if string(b["reasoning"]) != `{"effort":"none"}` {
			t.Errorf("reasoning = %s", b["reasoning"])
		}
	}
	if !strings.Contains(string(bodies[1]["input"]), `"opaque_results":"keep-exact"`) || !strings.Contains(string(bodies[1]["input"]), `"reasoning_text"`) || !strings.Contains(string(bodies[1]["input"]), "tomorrow?") {
		t.Fatalf("replay = %s", bodies[1]["input"])
	}
	req.Messages[0].Text = "different history"
	if _, err := gateway.Stream(context.Background(), req); err == nil {
		t.Fatal("accepted mismatched opaque state")
	}
}

func collectDeepSeek(t *testing.T, g provider.Gateway, r provider.Request) []provider.Event {
	t.Helper()
	ch, err := g.Stream(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	var events []provider.Event
	for e := range ch {
		events = append(events, e)
	}
	return events
}

func TestDeepSeekStreamsToolsAndRequiresTerminal(t *testing.T) {
	start := `{"type":"response.output_item.added","item":{"type":"function_call","id":"item1","call_id":"call1","name":"inspect"}}`
	args := `{"type":"response.function_call_arguments.delta","item_id":"item1","delta":"{}"}`
	done := `{"type":"response.output_item.done","item":{"type":"function_call","id":"item1","call_id":"call1","name":"inspect","arguments":"{}"}}`
	terminal := `{"type":"response.completed","response":{"id":"resp","status":"completed","output":[{"type":"function_call","id":"item1","call_id":"call1","name":"inspect","arguments":"{}"}]}}`
	for _, tc := range []struct {
		name          string
		events        []string
		wantError     bool
		wantTruncated bool
	}{
		{"function", []string{start, args, done, terminal}, false, false},
		{"missing terminal", []string{start, args, done}, true, false},
		{"failed", []string{`{"type":"response.failed","response":{"id":"resp","status":"failed","error":{"message":"failed"}}}`}, true, false},
		{"malformed", []string{`{broken`}, true, false},
		{"unknown output", []string{`{"type":"response.completed","response":{"id":"resp","status":"completed","output":[{"type":"future_tool"}]}}`}, true, false},
		{"truncated", []string{`{"type":"response.incomplete","response":{"id":"resp","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"part"}]}]}}`}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for _, e := range tc.events {
					fmt.Fprintf(w, "data: %s\n\n", e)
				}
			}))
			defer server.Close()
			g, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash", BaseURL: server.URL, APIKey: "secret", StateCompatibilityKey: "test", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			events := collectDeepSeek(t, g, provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "inspect"}}})
			last := events[len(events)-1]
			if (last.Err != nil) != tc.wantError {
				t.Fatalf("events = %+v", events)
			}
			if tc.wantTruncated && last.Type != provider.EventTruncated {
				t.Fatalf("last = %+v", last)
			}
			if tc.name == "function" {
				var starts, ends, invocations int
				for _, e := range events {
					switch e.Type {
					case provider.EventToolCallStart:
						starts++
					case provider.EventToolCallEnd:
						ends++
					case provider.EventToolCalls:
						invocations += len(e.ToolCalls)
						if e.ToolCalls[0].ID != "call1" {
							t.Fatal("confused item and call identity")
						}
					}
				}
				if starts != 1 || ends != 1 || invocations != 1 || last.Reason != "tool_calls" {
					t.Fatalf("events = %+v", events)
				}
			}
		})
	}
}

func TestDeepSeekRejectsInvalidRequestsBeforeHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid request reached HTTP") }))
	defer server.Close()
	g, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "secret", StateCompatibilityKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	for _, messages := range [][]provider.Message{
		{{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{{ID: "call", Name: "inspect", Args: "{}"}}}},
		{{Role: provider.RoleTool, ToolResult: &provider.ToolResult{CallID: "orphan", ToolName: "inspect", Text: "done"}}},
	} {
		if _, err := g.Stream(context.Background(), provider.Request{RunID: "run", PromptScopeID: "scope", Messages: messages}); err == nil {
			t.Fatal("invalid tool history accepted")
		}
	}
}

func TestDeepSeekCancellationClosesStream(t *testing.T) {
	ready := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": ready\n\n")
		w.(http.Flusher).Flush()
		close(ready)
		<-r.Context().Done()
	}))
	defer server.Close()
	g, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "secret", StateCompatibilityKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := g.Stream(ctx, provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	<-ready
	cancel()
	for e := range stream {
		if e.Type == provider.EventDone {
			t.Fatal("cancelled stream reported success")
		}
	}
}

func TestDeepSeekHostedSearchFailureIsNotSuccess(t *testing.T) {
	for _, streamed := range []bool{false, true} {
		t.Run(fmt.Sprint(streamed), func(t *testing.T) {
			item := `{"type":"web_search_call","id":"search-1","status":"failed","error":{"code":"search_unavailable","message":"upstream search unavailable"},"action":{"type":"search","queries":["Go release"]}}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if streamed {
					fmt.Fprintf(w, "data: {\"type\":\"response.output_item.done\",\"item\":%s}\n\n", item)
				}
				fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response-1\",\"status\":\"completed\",\"output\":[%s,{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Search unavailable.\"}]}]}}\n\n", item)
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			events := collectDeepSeek(t, gateway, provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "Search."}}, HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}})
			calls, results, done := 0, 0, false
			for _, event := range events {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				switch event.Type {
				case provider.EventHostedToolCall:
					calls++
				case provider.EventHostedToolResult:
					results++
					if event.HostedResult == nil || event.HostedResult.Error == nil || event.HostedResult.Error.Code != "search_unavailable" || strings.Contains(event.HostedResult.Text, "completed") {
						t.Fatalf("failed search reported as success: %+v", event.HostedResult)
					}
				case provider.EventDone:
					done = true
				}
			}
			if calls != 1 || results != 1 || !done {
				t.Fatalf("calls=%d results=%d done=%v", calls, results, done)
			}
		})
	}
}
