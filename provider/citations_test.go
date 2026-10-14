package provider_test

import (
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDeepSeekCitationPositionsAcrossUnicodeParts(t *testing.T) {
	output := `[{"type":"message","id":"m","role":"assistant","content":[{"type":"output_text","text":"你好😀","annotations":[{"type":"url_citation","title":"First","url":"https://example.com/a","start_index":0,"end_index":3}]},{"type":"output_text","text":"answer","annotations":[{"type":"url_citation","title":"Second","url":"https://example.com/a","start_index":0,"end_index":6},{"type":"url_citation","title":"Second","url":"https://example.com/a","start_index":0,"end_index":6},{"type":"url_citation","url":"https://example.com/b","start_index":99,"end_index":100},{"type":"url_citation","url":"file:///tmp/a"}]}]}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp\",\"status\":\"completed\",\"output\":%s}}\n\n", output)
	}))
	defer server.Close()
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDeepSeek(t, gateway, provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "search"}}, Reasoning: config.ReasoningSelection{Level: config.ReasoningLevelOff}})
	var sources []provider.Source
	for _, e := range events {
		if e.Err != nil {
			t.Fatal(e.Err)
		}
		sources = append(sources, e.Sources...)
	}
	if len(sources) != 3 || *sources[0].StartIndex != 0 || *sources[0].EndIndex != 3 || *sources[1].StartIndex != 3 || *sources[1].EndIndex != 9 || sources[2].StartIndex != nil {
		t.Fatalf("sources=%+v", sources)
	}
	raw, _ := json.Marshal(sources)
	if string(raw) == "" {
		t.Fatal("missing JSON")
	}
}
