package provider_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func TestDeepSeekSearchRetainsOperationAcrossStatusUpdates(t *testing.T) {
	for _, action := range []string{
		`{"type":"open_page","url":"https://example.com/weather"}`,
		`{"type":"find_in_page","url":"https://example.com/weather","pattern":"forecast"}`,
	} {
		t.Run(action, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, "data: {\"type\":\"response.web_search_call.in_progress\",\"item_id\":\"search\"}\n\n")
				fmt.Fprintf(w, "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"web_search_call\",\"id\":\"search\",\"action\":%s}}\n\n", action)
				fmt.Fprint(w, "data: {\"type\":\"response.web_search_call.searching\",\"item_id\":\"search\"}\n\n")
				item := `{"type":"web_search_call","id":"search","status":"completed"}`
				fmt.Fprintf(w, "data: {\"type\":\"response.output_item.done\",\"item\":%s}\n\n", item)
				fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response\",\"status\":\"completed\",\"output\":[%s]}}\n\n", item)
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			events := collectDeepSeek(t, gateway, provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "weather"}}})
			calls, results := 0, 0
			for _, event := range events {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				if event.Type == provider.EventHostedToolCall {
					calls++
				}
				if event.Type == provider.EventHostedToolResult {
					results++
					if !strings.Contains(event.HostedToolCall.Args, `"url":"https://example.com/weather"`) {
						t.Fatalf("target lost: %s", event.HostedToolCall.Args)
					}
					if strings.Contains(action, "pattern") && !strings.Contains(event.HostedToolCall.Args, `"pattern":"forecast"`) {
						t.Fatalf("pattern lost: %s", event.HostedToolCall.Args)
					}
				}
			}
			if calls != 1 || results != 1 {
				t.Fatalf("calls=%d results=%d", calls, results)
			}
		})
	}
}

func TestDeepSeekSearchResultAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, action string
		provided     bool
		count        int
	}{
		{"unavailable", `{"type":"search","query":"weather"}`, false, 0},
		{"empty", `{"type":"search","query":"weather","sources":[]}`, true, 0},
		{"results", `{"type":"search","queries":["weather","forecast"],"sources":[{"title":"Weather","url":"https://example.com","snippet":"Sunny"}]}`, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response\",\"status\":\"completed\",\"output\":[{\"type\":\"web_search_call\",\"id\":\"search\",\"status\":\"completed\",\"action\":%s}]}}\n\n", tc.action)
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range collectDeepSeek(t, gateway, provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "weather"}}}) {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				if event.Type != provider.EventHostedToolResult {
					continue
				}
				found = true
				result := event.HostedResult
				if result.ResultsProvided != tc.provided || len(result.Results) != tc.count {
					t.Fatalf("result=%+v", result)
				}
				if tc.count > 0 && result.Results[0].Snippet != "Sunny" {
					t.Fatalf("snippet lost: %+v", result)
				}
			}
			if !found {
				t.Fatal("missing result")
			}
		})
	}
}
