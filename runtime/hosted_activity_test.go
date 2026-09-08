package runtime

import (
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
