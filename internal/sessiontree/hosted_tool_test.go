package sessiontree

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/tools"
)

func TestHostedToolEntryPreservesOnlySafeActivity(t *testing.T) {
	entry, err := NewHostedToolEntry(observation.Event{
		Type: observation.EventTypeHostedToolResult, ThreadID: "thread", TurnID: "turn", RunID: "run", ToolID: "search", ToolName: "web_search", ObservedAt: time.Now(),
		Result: "opaque provider receipt", Metadata: map[string]any{"error_present": true, "result_count": 1, "results": "private raw data"},
		Activity: &tools.ActivityPresentation{Label: "Web search", Renderer: tools.ActivityRendererWebSearch, Payload: tools.WebSearchActivityPayload{Query: "Go", Status: "error", Error: &tools.ActivityError{Message: "Web search failed"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry = PrepareEntry(entry)
	if err := ValidateEntryIntegrity(entry); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "opaque provider receipt") || strings.Contains(string(raw), "private raw data") || entry.Message.Role != "" {
		t.Fatalf("hosted activity leaked provider history: %s", raw)
	}
	var restored Entry
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	observed, err := HostedToolObservation(restored)
	if err != nil || observed.RunID != "run" || observed.TurnID != "turn" || observed.ThreadID != "thread" || observed.ToolKind != "hosted" || observed.Metadata["error_present"] != true {
		t.Fatalf("restored activity: %+v %v", observed, err)
	}
	if messages := messagesForEntries([]Entry{restored}); len(messages) != 0 {
		t.Fatalf("hosted activity entered model history: %+v", messages)
	}
	for _, mutation := range []func(*Entry){
		func(e *Entry) { e.RunID = "" },
		func(e *Entry) { e.Message.Role = "tool" },
		func(e *Entry) { e.Metadata["type"] = "tool_result" },
		func(e *Entry) { e.Metadata["error_present"] = "false" },
	} {
		var corrupt Entry
		if err := json.Unmarshal(raw, &corrupt); err != nil {
			t.Fatal(err)
		}
		mutation(&corrupt)
		if err := ValidateEntryIntegrity(PrepareEntry(corrupt)); err == nil {
			t.Fatal("accepted corrupt hosted authority")
		}
	}
}
