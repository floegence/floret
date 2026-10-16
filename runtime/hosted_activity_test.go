package runtime

import (
	"context"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/tools"
)

func TestHostedActivityProjectionPreservesFailureAndInterruptedSearch(t *testing.T) {
	for _, failed := range []bool{false, true} {
		call, err := sessiontree.NewHostedToolEntry(observation.Event{
			Type: observation.EventTypeHostedToolCall, ThreadID: "thread", TurnID: "turn", RunID: "run", ToolID: "search", ToolName: "web_search", ObservedAt: time.Now(),
			Activity: &tools.ActivityPresentation{Label: "Web search", Renderer: tools.ActivityRendererWebSearch, Payload: tools.WebSearchActivityPayload{Query: "Go", Status: "running"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		entries := []sessiontree.Entry{call}
		want := observation.ActivityStatusCanceled
		if failed {
			result, err := sessiontree.NewHostedToolEntry(observation.Event{Type: observation.EventTypeHostedToolResult, ThreadID: "thread", TurnID: "turn", RunID: "run", ToolID: "search", ToolName: "web_search", Metadata: map[string]any{"error_present": true}, ObservedAt: time.Now()})
			if err != nil {
				t.Fatal(err)
			}
			entries = append(entries, result)
			want = observation.ActivityStatusError
		}
		entries = append(entries, sessiontree.Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnAborted, CreatedAt: time.Now()})
		items, _, err := threadRuntimeItemsFromEntries(entries)
		if err != nil || len(items) != 1 || items[0].Activity.Kind != observation.ActivityKindHosted || items[0].Activity.Status != want {
			t.Fatalf("failed=%v items=%+v err=%v", failed, items, err)
		}
	}
}

func TestURLOnlyHostedSourcesSurviveCanonicalRestart(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "sources", StateCompatibilityKey: "sources:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{
		{Type: provider.EventHostedToolCall, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search", Args: `{"type":"search","queries":["site:example.com Runtime","Go Runtime"]}`}},
		{Type: provider.EventHostedToolResult, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}, HostedResult: &provider.HostedToolResult{ResultsProvided: true, Results: []provider.HostedToolResultItem{{URL: "https://example.com/runtime", Snippet: "Provider excerpt"}, {Title: "Go", URL: "https://go.dev"}}}},
		{Type: provider.EventDelta, Text: "Done"}, {Type: provider.EventDone, Reason: "stop"},
	}})
	agent, err := testAgent(gateway, WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"}))
	if err != nil {
		t.Fatal(err)
	}
	source := storage.SQLite(t.TempDir() + "/sources.sqlite")
	open := func() (*Host, ThreadService) {
		h, err := Open(t.Context(), Options{Storage: source})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
		service, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return h, service
	}
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "Search"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	check := func(view ThreadView) {
		t.Helper()
		for _, item := range view.Items {
			if item.Activity == nil || item.Activity.Kind != observation.ActivityKindHosted {
				continue
			}
			if item.Activity.Presentation == nil {
				t.Fatal("lost hosted presentation")
			}
			p := item.Activity.Presentation.Payload.(tools.WebSearchActivityPayload)
			if p.Operation != "search" || p.Query != `site:example.com Runtime
Go Runtime` || !p.ResultsProvided || len(p.Results) != 2 || p.Results[0].Title != "" || p.Results[0].URL != "https://example.com/runtime" || p.Results[0].Snippet != "Provider excerpt" || p.Results[1].Title != "Go" {
				t.Fatalf("provider facts changed: %+v", p)
			}
			return
		}
		t.Fatalf("no hosted activity: %+v", view.Items)
	}
	check(view)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	check(restored)
}
