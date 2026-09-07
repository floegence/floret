package sessiontree

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func controlResultTestPayload() map[string]any {
	return map[string]any{
		"reason_code": "missing_external_input", "required_from_user": []any{"q"}, "evidence_refs": []any{},
		"questions": []any{map[string]any{"id": "q", "header": "Question", "question": "Continue?", "response_mode": "write", "is_secret": false}},
	}
}

func controlResultTestEntries() []Entry {
	return []Entry{
		{ID: "ask", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{
			Role: session.Assistant, Kind: session.MessageKindControlSignal, ToolCallID: "ask", ToolName: "ask_user",
			ControlSignal: &session.ControlSignalView{Name: "ask_user", CallID: "ask", Disposition: "waiting", Payload: controlResultTestPayload()},
		}},
		{ID: "interaction-resolved:ask", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryInteractionDone, Payload: json.RawMessage(`{"accepted":true,"input":{"q":"yes"}}`)},
		{ID: "close", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{
			Role: session.Tool, ToolCallID: "ask", ToolName: "ask_user", ToolResult: &session.ToolResultView{Status: "canceled"},
		}},
	}
}

func TestControlResultOwnershipPreservesCanonicalFacts(t *testing.T) {
	for _, payload := range []string{
		`{"accepted":true,"input":{"q":"yes"}}`,
		`{"accepted":true,"redacted":true}`,
		`{"accepted":false,"outcome":"cancelled"}`,
		`{"accepted":false,"outcome":"failed"}`,
	} {
		t.Run(payload, func(t *testing.T) {
			entries := controlResultTestEntries()
			entries[1].Payload = json.RawMessage(payload)
			before, _ := json.Marshal(entries)
			for _, compacted := range []bool{false, true} {
				path := append([]Entry(nil), entries...)
				if compacted {
					path = append(path, Entry{ID: "compaction", Type: EntryCompaction, Summary: "earlier conversation", FirstKeptEntryID: "ask"})
				}
				messages, err := BuildContextChecked(path, ContextOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := session.ValidateToolHistory(messages); err != nil {
					t.Fatal(err)
				}
				results := 0
				for _, message := range messages {
					if message.Role == session.Tool {
						results++
						if message.EntryID != entries[1].ID {
							t.Fatalf("result identity=%q", message.EntryID)
						}
					}
				}
				if results != 1 {
					t.Fatalf("results=%d", results)
				}
			}
			after, _ := json.Marshal(entries)
			if string(before) != string(after) {
				t.Fatal("projection changed journal facts")
			}
		})
	}
}

func TestControlResultOwnershipRequiresExactExecutionAndSource(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func([]Entry)
		corrupt bool
	}{
		{"other thread", func(e []Entry) { e[2].ThreadID = "other" }, false},
		{"other turn", func(e []Entry) { e[2].TurnID = "other" }, false},
		{"other run", func(e []Entry) { e[2].RunID = "other" }, false},
		{"other call", func(e []Entry) { e[2].Message.ToolCallID = "other" }, false},
		{"ordinary tool", func(e []Entry) { e[0].Message.Kind = ""; e[0].Message.ControlSignal = nil }, false},
		{"signal identity drift", func(e []Entry) { e[0].Message.ControlSignal.CallID = "other" }, false},
		{"invalid waiting control", func(e []Entry) { e[0].Message.ControlSignal.Payload = nil }, false},
		{"other resolution run", func(e []Entry) { e[1].RunID = "other" }, true},
		{"unsettled", func(e []Entry) { e[1].Payload = json.RawMessage(`{"accepted":true}`) }, true},
		{"unknown resolution field", func(e []Entry) { e[1].Payload = json.RawMessage(`{"accepted":true,"input":{"q":"yes"},"extra":1}`) }, true},
		{"wrong name", func(e []Entry) { e[2].Message.ToolName = "other" }, true},
		{"successful tool result", func(e []Entry) { e[2].Message.ToolResult.Status = "success" }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			entries := controlResultTestEntries()
			test.change(entries)
			redundant, err := RedundantControlResultIDs(entries)
			if errors.Is(err, ErrAuthorityCorrupt) != test.corrupt || len(redundant) != 0 {
				t.Fatalf("redundant=%v err=%v, corrupt=%v", redundant, err, test.corrupt)
			}
		})
	}
}

func TestPendingToolsExcludeControlOwnersAcrossRuns(t *testing.T) {
	entries := controlResultTestEntries()[:2]
	ordinary := Entry{ID: "shell", ThreadID: "thread", TurnID: "turn", RunID: "continuation-run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "shell", ToolName: "shell"}}
	entries = append(entries, ordinary)
	if pending := runtimePendingToolCalls(entries, "turn"); !reflect.DeepEqual(pending, []Entry{ordinary}) {
		t.Fatalf("pending tools=%#v", pending)
	}
	for _, disposition := range []string{"failed", "terminal"} {
		entries[0].Message.ControlSignal.Disposition = disposition
		if pending := runtimePendingToolCalls(entries, "turn"); !reflect.DeepEqual(pending, []Entry{ordinary}) {
			t.Fatalf("%s pending tools=%#v", disposition, pending)
		}
	}
}
