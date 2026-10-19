package websearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/testing/tooltest"
	"github.com/floegence/floret/v7/tools"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeepSeekSearchContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/proxy/v1/tools/search" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("request=%v", r)
		}
		var args map[string]any
		_ = json.NewDecoder(r.Body).Decode(&args)
		if args["query"] != "site:example.com 新闻" || args["count"] != float64(10) || args["need_content"] != false {
			t.Errorf("args=%v", args)
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"","url":"https://example.com","snippet":"UI snippet","summary":"Model summary","authority":0,"site_name":null,"date":null}]}`))
	}))
	defer server.Close()
	client, err := NewDeepSeekClient(ClientOptions{BaseURL: server.URL + "/proxy/v1", APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	tool := NewTool(client, ToolOptions{})
	result := tooltest.Run(t.Context(), tools.NewRegistry(tool), tools.ToolCall{ID: "search", Name: ToolName, Args: `{"query":"site:example.com 新闻"}`}, func(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
		return tooltest.PermissionDecisionAllow, nil
	})
	if result.IsError || !strings.Contains(result.Text, "Model summary") || strings.Contains(result.Text, "UI snippet") {
		t.Fatalf("result=%+v", result)
	}
	activity := result.Activity.Payload.(tools.WebSearchActivityPayload)
	if !activity.ResultsProvided || len(activity.Results) != 1 || activity.Results[0].Title != "" || activity.Results[0].Snippet != "UI snippet" {
		t.Fatalf("activity=%+v", activity)
	}
}
func TestDeepSeekValidationAndEmpty(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; _, _ = w.Write([]byte(`{"results":[]}`)) }))
	defer server.Close()
	client, _ := NewDeepSeekClient(ClientOptions{BaseURL: server.URL, APIKey: "key"})
	for _, req := range []Request{{Query: " "}, {Query: strings.Repeat("中", 129)}, {Query: "q", Count: 51}, {Query: "q", OriginalQuestion: strings.Repeat("中", 129)}, {Query: "q", Localization: "not-ip"}} {
		if _, err := client.Search(t.Context(), req); err == nil {
			t.Errorf("accepted %+v", req)
		}
	}
	if calls != 0 {
		t.Fatal("invalid request dispatched")
	}
	got, err := client.Search(t.Context(), Request{Query: strings.Repeat("中", 128), Count: 50})
	if err != nil || got.Results == nil || len(got.Results) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestDeepSeekErrors(t *testing.T) {
	for _, status := range []int{400, 401, 402, 403, 404, 408, 413, 429, 500, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Retry-After", "2")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"code":"blocked","message":"secret failed"}}`))
			}))
			defer server.Close()
			client, _ := NewDeepSeekClient(ClientOptions{BaseURL: server.URL, APIKey: "secret"})
			_, err := client.Search(t.Context(), Request{Query: "q"})
			var serviceErr *HTTPError
			if !errors.As(err, &serviceErr) || serviceErr.StatusCode != status || serviceErr.RetryAfter != "2" || strings.Contains(err.Error(), "secret") || calls != 1 {
				t.Fatalf("error=%v calls=%d", err, calls)
			}
		})
	}
}
func TestDeepSeekMalformedResponse(t *testing.T) {
	for _, body := range []string{`{}`, `{"results":null}`, `{"results":[]}{}`, strings.Repeat("x", (5<<20)+1)} {
		t.Run("invalid", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer server.Close()
			client, _ := NewDeepSeekClient(ClientOptions{BaseURL: server.URL, APIKey: "key"})
			if _, err := client.Search(t.Context(), Request{Query: "q"}); err == nil {
				t.Fatal("accepted malformed response")
			}
		})
	}
}

func TestBraveAndCancellationAndRedirect(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("X-Subscription-Token") != "key" || r.URL.Query().Get("country") != "CN" {
			t.Errorf("request=%v", r)
		}
		_, _ = w.Write([]byte(`{"web":{"results":[{"title":"T","url":"https://example.com","description":"S"}]}}`))
	}))
	defer server.Close()
	client, err := NewBraveClient(ClientOptions{BaseURL: server.URL, APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Search(t.Context(), Request{Query: "q", Count: 5, Country: "CN"})
	if err != nil || result.Results[0].Snippet != "S" {
		t.Fatalf("%+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = client.Search(ctx, Request{Query: "q", Country: "CN"})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancel=%v calls=%d", err, calls)
	}
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls++ }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	remote, _ := NewDeepSeekClient(ClientOptions{BaseURL: redirect.URL, APIKey: "key"})
	if _, err := remote.Search(t.Context(), Request{Query: "q"}); err == nil || targetCalls != 0 {
		t.Fatalf("redirect=%v calls=%d", err, targetCalls)
	}
}

func TestLiveCredentialIsResolvedForEachSearch(t *testing.T) {
	key := "first"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			t.Error("stale credential")
		}
		fmt.Fprint(w, `{"results":[]}`)
	}))
	defer server.Close()
	client, err := NewDeepSeekClient(ClientOptions{BaseURL: server.URL, ResolveAPIKey: func(context.Context) (string, error) { return key, nil }})
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []string{"first", "rotated"} {
		key = next
		if _, err = client.Search(context.Background(), Request{Query: "rotation"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeToolKeepsProviderCountAndBraveModelContent(t *testing.T) {
	for _, deepseek := range []bool{false, true} {
		t.Run(fmt.Sprint(deepseek), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if deepseek {
					fmt.Fprint(w, `{"results":[{"url":"https://example.com","snippet":"UI snippet","summary":"Model summary"}]}`)
				} else {
					fmt.Fprint(w, `{"web":{"results":[{"title":"Title","url":"https://example.com","description":"Brave model content"}]}}`)
				}
			}))
			defer server.Close()
			options := ClientOptions{BaseURL: server.URL, APIKey: "test"}
			client, err := NewBraveClient(options)
			count := 20
			if deepseek {
				client, err = NewDeepSeekClient(options)
				count = 50
			}
			if err != nil {
				t.Fatal(err)
			}
			tool := NewTool(client, ToolOptions{})
			properties := tool.Definition.InputSchema["properties"].(map[string]any)
			if properties["count"].(map[string]any)["maximum"] != count {
				t.Fatalf("count schema=%v", properties["count"])
			}
			result := tooltest.Run(t.Context(), tools.NewRegistry(tool), tools.ToolCall{ID: "search", Name: ToolName, Args: fmt.Sprintf(`{"query":"q","count":%d}`, count)}, func(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
				return tooltest.PermissionDecisionAllow, nil
			})
			if result.IsError {
				t.Fatal(result.Text)
			}
			if !deepseek && !strings.Contains(result.Text, "Brave model content") {
				t.Fatalf("Brave model snippet lost: %s", result.Text)
			}
		})
	}
}
