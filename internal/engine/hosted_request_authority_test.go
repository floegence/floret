package engine

import (
	"github.com/floegence/floret/v7/internal/provider"
	"testing"
)

func TestHostedEventsUseFrozenRequestAuthority(t *testing.T) {
	search := []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}
	for _, kind := range []provider.EventType{provider.HostedToolCall, provider.HostedToolResult} {
		for _, tc := range []struct {
			name                  string
			configured, requested []provider.HostedToolDefinition
			wantError             bool
		}{
			{"request_only", nil, search, false},
			{"options_only", search, nil, true},
			{"matching", search, search, false},
			{"unrequested", search, []provider.HostedToolDefinition{{Name: "other_search", Type: "other_search"}}, true},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				stream := make(chan provider.StreamEvent, 4)
				if kind == provider.HostedToolResult {
					stream <- provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: provider.ToolCall{ID: "search-1", Name: "web_search", Args: "{}"}}
				}
				stream <- provider.StreamEvent{Type: kind, ToolCall: provider.ToolCall{ID: "search-1", Name: "web_search", Args: "{}"}}
				stream <- provider.StreamEvent{Type: provider.Delta, Text: "verified"}
				stream <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
				close(stream)
				_, err := (&Engine{}).consume(t.Context(), Options{HostedToolDefinitions: tc.configured}, 1, stream, provider.Request{HostedTools: tc.requested}, nil)
				if (err != nil) != tc.wantError {
					t.Fatalf("error=%v, wantError=%v", err, tc.wantError)
				}
			})
		}
	}
}
