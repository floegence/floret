package tools_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestWebSearchPresentationAcceptsURLOnlySources(t *testing.T) {
	for _, title := range []string{"", "Official documentation"} {
		activity := &tools.ActivityPresentation{Renderer: tools.ActivityRendererWebSearch, Payload: tools.WebSearchActivityPayload{
			Operation: "search", Query: "official documentation", ResultsProvided: true,
			Results: []tools.WebSearchActivityResult{{Title: title, URL: "https://example.com/docs", Snippet: "Provider supplied excerpt"}},
		}}
		if err := activity.Validate(); err != nil {
			t.Fatalf("URL source with title %q rejected: %v", title, err)
		}
		data, err := json.Marshal(activity)
		if err != nil {
			t.Fatal(err)
		}
		var restored tools.ActivityPresentation
		if err := json.Unmarshal(data, &restored); err != nil {
			t.Fatal(err)
		}
		if got := restored.Payload.(tools.WebSearchActivityPayload); got.Query != "official documentation" || got.Results[0].Title != title || got.Results[0].Snippet != "Provider supplied excerpt" {
			t.Fatalf("provider facts changed: %+v", got)
		}
	}
	invalid := &tools.ActivityPresentation{Renderer: tools.ActivityRendererWebSearch, Payload: tools.WebSearchActivityPayload{Results: []tools.WebSearchActivityResult{{Title: "Missing address"}}}}
	if err := invalid.Validate(); err == nil {
		t.Fatal("accepted a source without a URL")
	}
}

func TestWebSearchPresentationMergeAndRoundTrip(t *testing.T) {
	present := func(payload tools.WebSearchActivityPayload) *tools.ActivityPresentation {
		return &tools.ActivityPresentation{Renderer: tools.ActivityRendererWebSearch, Payload: payload}
	}
	first := present(tools.WebSearchActivityPayload{Operation: "find_in_page", URL: "https://example.com", Pattern: "forecast", Query: "weather", Status: "running", ResultsProvided: true, Results: []tools.WebSearchActivityResult{{Title: "Weather", URL: "https://example.com", Snippet: "Sunny"}}})
	merged := tools.MergeActivityPresentations(first, present(tools.WebSearchActivityPayload{Status: "success"}))
	data, err := json.Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	var restored tools.ActivityPresentation
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	p := restored.Payload.(tools.WebSearchActivityPayload)
	if p.Operation != "find_in_page" || p.URL != "https://example.com" || p.Pattern != "forecast" || p.Query != "weather" || p.Status != "success" || !p.ResultsProvided || p.Results[0].Snippet != "Sunny" {
		t.Fatalf("lost facts: %+v", p)
	}
	p.Results[0].Snippet = "changed"
	if first.Payload.(tools.WebSearchActivityPayload).Results[0].Snippet != "Sunny" {
		t.Fatal("shared result slice")
	}
	cleared := tools.MergeActivityPresentations(merged, present(tools.WebSearchActivityPayload{ResultsProvided: true}))
	if p := cleared.Payload.(tools.WebSearchActivityPayload); !p.ResultsProvided || len(p.Results) != 0 {
		t.Fatalf("explicit empty lost: %+v", p)
	}
	for _, payload := range []tools.WebSearchActivityPayload{{Operation: "unsupported"}, {Pattern: strings.Repeat("x", 8001)}} {
		if err := present(payload).Validate(); err == nil {
			t.Fatal("invalid presentation accepted")
		}
	}
}
