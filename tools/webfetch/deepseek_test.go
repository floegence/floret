package webfetch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeepSeekFetchActualFactsAndSecurity(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/prefix/v1/tools/fetch" || r.Header.Get("Authorization") != "Bearer key" {
			t.Errorf("request=%v", r)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["url"] != "https://example.com/docs" {
			t.Errorf("body=%v", body)
		}
		_, _ = w.Write([]byte(`{"title":"Guide","content":"# Authoritative page"}`))
	}))
	defer server.Close()
	client, err := NewDeepSeekClient(DeepSeekOptions{BaseURL: server.URL + "/prefix", APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	client.resolver = &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}, "private.example": {"127.0.0.1"}}}
	tool := deepSeekTool(client, Options{})
	result := runTool(t, tool, t.Context(), `{"url":"https://example.com/docs"}`)
	if result.IsError || !strings.Contains(result.Text, "# Authoritative page") {
		t.Fatalf("result=%+v", result)
	}
	for _, field := range []string{"final_url", "status_code", "content_type", "bytes_read"} {
		if _, ok := result.Structured[field]; ok {
			t.Fatalf("fabricated %s", field)
		}
	}
	for _, args := range []string{`{"url":"http://127.0.0.1"}`, `{"url":"http://private.example"}`, `{"url":"https://user:pass@example.com"}`, `{"url":"https://example.com/docs","format":"text"}`} {
		if result := runTool(t, tool, t.Context(), args); !result.IsError {
			t.Errorf("accepted %s", args)
		}
	}
	if calls != 1 {
		t.Fatalf("security failure dispatched: %d", calls)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.Fetch(ctx, "https://example.com/docs"); err == nil {
		t.Fatal("accepted canceled request")
	}
}
func TestDeepSeekFetchRejectsEmptyAndBrokenPage(t *testing.T) {
	for _, body := range []string{`{}`, `{"content":""}`, `{"content":null}`, `{"content":"page"}{}`} {
		t.Run("invalid", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer server.Close()
			client, _ := NewDeepSeekClient(DeepSeekOptions{BaseURL: server.URL, APIKey: "key"})
			client.resolver = &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
			if _, err := client.Fetch(t.Context(), "https://example.com"); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
}
