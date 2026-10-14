package runtime

import (
	"context"
	"encoding/json"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/agentharness"
	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"strings"
	"testing"
)

func citationIndex(v int) *int { return &v }
func TestCanonicalCitationsSurviveHostedBoundaryContinuationForkAndRestart(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "sqlite"}[sqlite], func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "citations", StateCompatibilityKey: "citations:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{
					{Type: provider.EventDelta, Text: "你好😀"},
					{Type: provider.EventHostedToolCall, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}},
					{Type: provider.EventHostedToolResult, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}, HostedResult: &provider.HostedToolResult{}},
					{Type: provider.EventDelta, Text: "answer"},
					{Type: provider.EventSources, Sources: []provider.Source{
						{Title: "Before", URL: "https://example.com/before", StartIndex: citationIndex(0), EndIndex: citationIndex(3)},
						{Title: "After", URL: "https://example.com/after", StartIndex: citationIndex(3), EndIndex: citationIndex(9)},
						{Title: "Cross", URL: "https://example.com/cross", StartIndex: citationIndex(2), EndIndex: citationIndex(4)},
						{URL: "javascript:alert(1)"},
					}}, {Type: provider.EventTruncated, Reason: "length"},
				}}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "final"}, {Type: provider.EventSources, Sources: []provider.Source{{URL: "https://example.com/final", StartIndex: citationIndex(0), EndIndex: citationIndex(5)}}}, {Type: provider.EventDone, Reason: "stop"}}})
			agent, err := testAgent(gateway, WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"}))
			if err != nil {
				t.Fatal(err)
			}
			src := storage.Memory()
			if sqlite {
				src = storage.SQLite(t.TempDir() + "/citations.sqlite")
			}
			open := func() (*Host, ThreadService) {
				h, e := Open(t.Context(), Options{Storage: src})
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
				s, e := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if e != nil {
					t.Fatal(e)
				}
				return h, s
			}
			host, service := open()
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "search"}}); err != nil {
				t.Fatal(err)
			}
			view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			check := func(id identity.ThreadID, v ThreadView) {
				t.Helper()
				var texts []string
				var refs [][]provider.Source
				for _, item := range v.Items {
					if item.Kind == ThreadItemAssistant {
						texts = append(texts, item.Text)
						refs = append(refs, item.Sources)
					}
				}
				if strings.Join(texts, "|") != "你好😀|answer|final" || len(refs[0]) != 1 || len(refs[1]) != 2 || len(refs[2]) != 1 {
					t.Fatalf("items=%+v failure=%+v", v.Items, v.Failure)
				}
				if *refs[1][0].StartIndex != 0 || *refs[1][0].EndIndex != 6 || refs[1][1].StartIndex != nil {
					t.Fatalf("ranges=%+v", refs)
				}
				raw, _ := json.Marshal(v)
				if !strings.Contains(string(raw), `"start_index":0`) {
					t.Fatal("JSON lost index zero")
				}
				path, e := host.store.repo.Path(t.Context(), id.String(), "")
				if e != nil {
					t.Fatal(e)
				}
				for _, entry := range path {
					if e = sessiontree.ValidateEntryIntegrity(entry); e != nil {
						t.Fatal(e)
					}
				}
				details, e := agentharness.New(agentharness.Options{Repo: host.store.repo}).ListThreadDetailEvents(t.Context(), agentharness.ListThreadDetailEventsOptions{ThreadID: id.String(), IncludeRaw: true})
				if e != nil {
					t.Fatal(e)
				}
				count := 0
				for _, detail := range details.Events {
					if detail.Message != nil {
						count += len(detail.Message.Sources)
					}
				}
				if count != 4 {
					t.Fatalf("detail sources=%d", count)
				}
				*refs[0][0].StartIndex = 99
				next, e := service.View(t.Context(), id)
				if e != nil {
					t.Fatal(e)
				}
				for _, item := range next.Items {
					if item.Text == "你好😀" && *item.Sources[0].StartIndex != 0 {
						t.Fatal("source clone aliases cached view")
					}
				}
			}
			check(created.ThreadID, view)
			fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
			if err != nil {
				t.Fatal(err)
			}
			check(fork.ThreadID, fork)
			if sqlite {
				if err = host.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
				host, service = open()
				for _, id := range []identity.ThreadID{created.ThreadID, fork.ThreadID} {
					v, e := service.View(t.Context(), id)
					if e != nil {
						t.Fatal(e)
					}
					check(id, v)
				}
			}
		})
	}
}

func TestOldAssistantCanonicalBytesOmitSources(t *testing.T) {
	var entry sessiontree.Entry
	raw := `{"Role":"assistant","Content":"old","Attachments":null,"Reasoning":"","ToolCallID":"","ToolName":"","ToolArgs":"","EntryID":"","ParentEntryID":"","Kind":"","ToolResult":null,"CompactionID":"","CompactionGeneration":0,"CompactionWindowID":""}`
	if err := json.Unmarshal([]byte(raw), &entry.Message); err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(entry.Message)
	if string(got) != raw {
		t.Fatalf("old bytes changed: %s", got)
	}
	if len(citation.Clone(entry.Message.Sources)) != 0 {
		t.Fatal("old record invented sources")
	}
}
