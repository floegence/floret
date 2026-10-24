package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestThreadActivityFieldAuthorizesRangesAndRestart(t *testing.T) {
	path := t.TempDir() + "/fields.sqlite"
	open := func() (*Host, ThreadActivityFieldReader) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		agent, err := testAgent(newBlockingThreadGateway())
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service.(ThreadActivityFieldReader)
	}
	host, reader := open()
	const id identity.ThreadID = "activity-field"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("\x00", 22000) + strings.Repeat("中文🌸\n\"\\\x00", 2000)
	activity := &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Title: "Source", Content: text, Format: tools.StructuredActivityRowFormatCode}}}}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "tool", Activity: activity}}, sessiontree.AppendOptions{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "viewer", ThreadIDs: []identity.ThreadID{id}}
	input := ThreadActivityFieldInput{ThreadID: id, ItemID: "tool:turn:tool", EntryID: "source", Path: "/payload/rows/0/content"}
	for restart := range 2 {
		var result strings.Builder
		input.OffsetBytes = 0
		for result.Len() < len(text) {
			part, err := reader.ReadActivityField(t.Context(), scope, input)
			if err != nil {
				t.Fatal(err)
			}
			wire, marshalErr := json.Marshal(part)
			if marshalErr != nil || len(wire) > 64<<10 {
				t.Fatalf("unbounded encoded field: %d %v", len(wire), marshalErr)
			}
			if part.OffsetBytes != result.Len() || part.Text == "" || part.Bytes != len(text) {
				t.Fatal("invalid range")
			}
			input.SourceSHA256, input.SHA256 = part.SourceSHA256, part.SHA256
			result.WriteString(part.Text)
			input.OffsetBytes = result.Len()
			if _, err := reader.ReadActivityField(t.Context(), threadquery.Scope{Authority: "viewer"}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
				t.Fatalf("revoked read accepted: %v", err)
			}
		}
		if result.String() != text {
			t.Fatal("decoded field changed")
		}
		// Random reverse traversal does not retain every preceding block boundary.
		for offset := len(text) - 1; offset > 22000; offset -= 7919 {
			input.AlignStart, input.OffsetBytes, input.LimitBytes = true, offset, 4096
			part, err := reader.ReadActivityField(t.Context(), scope, input)
			if err != nil {
				t.Fatal(err)
			}
			start := offset
			for start > 0 && !utf8.RuneStart(text[start]) {
				start--
			}
			if part.OffsetBytes != start || len(part.Text) > input.LimitBytes || !utf8.ValidString(part.Text) || !strings.HasPrefix(text[start:], part.Text) {
				t.Fatalf("invalid aligned range at %d: %+v", offset, part)
			}
			if start != offset {
				input.AlignStart = false
				if _, err := reader.ReadActivityField(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
					t.Fatalf("strict offset accepted split rune: %v", err)
				}
			}
		}
		input.AlignStart, input.LimitBytes = false, 0
		if restart == 0 {
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, reader = open()
		}
	}
	input.OffsetBytes = 0
	input.ItemID = "tool:turn:other"
	if _, err := reader.ReadActivityField(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("unrelated item accepted source: %v", err)
	}
	input.ItemID = "tool:turn:tool"
	input.SourceSHA256 = "stale"
	if _, err := reader.ReadActivityField(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("stale source accepted: %v", err)
	}
	input.SourceSHA256 = ""
	input.LimitBytes = 65536
	if _, err := reader.ReadActivityField(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
		t.Fatalf("oversized range accepted: %v", err)
	}
}
