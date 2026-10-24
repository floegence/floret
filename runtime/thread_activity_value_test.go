package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestActivityValueCanonicalRestartAndBudget(t *testing.T) {
	path := t.TempDir() + "/value.sqlite"
	row := tools.StructuredActivityRow{Title: "Visible", Content: strings.Repeat("中🌸", 8000), Format: tools.StructuredActivityRowFormatCode}
	for restart := range 2 {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution forbidden") }))
		if err != nil {
			t.Fatal(err)
		}
		const thread identity.ThreadID = "atomic-value"
		if restart == 0 {
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: thread.String()}); err != nil {
				t.Fatal(err)
			}
			for _, entry := range []sessiontree.Entry{
				{ID: "request", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "read"}},
				{ID: "call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "tool", Activity: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Title: "Unread", Content: strings.Repeat("x", 64000), Format: tools.StructuredActivityRowFormatCode}, row}, RowsProvided: true}}}},
			} {
				entry.ThreadID, entry.TurnID, entry.RunID = thread.String(), "turn", "run"
				if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}
		reader := public.(ThreadActivityValueReader)
		scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{thread}}
		input := ThreadActivityValueInput{ThreadID: thread, ItemID: "tool:turn:tool", Path: "/payload/rows/1", MaximumBytes: 96 << 10, MaximumNodes: 32}
		value, err := reader.ReadActivityValue(t.Context(), scope, input)
		if err != nil {
			t.Fatal(err)
		}
		var got tools.StructuredActivityRow
		if json.Unmarshal(value.Value, &got) != nil || got != row || value.TurnID != "turn" || value.RunID != "run" {
			t.Fatalf("incorrect value %s", value.Value)
		}
		selection := input
		selection.Fields = []string{"title", "format"}
		selection.MaximumBytes = 1024
		selected, err := reader.ReadActivityValue(t.Context(), scope, selection)
		if err != nil {
			t.Fatal(err)
		}
		var facts map[string]string
		if json.Unmarshal(selected.Value, &facts) != nil || facts["title"] != row.Title || facts["format"] != string(row.Format) || len(facts) != 2 {
			t.Fatalf("selected snapshot included hidden content: %s", selected.Value)
		}
		selection.TextFields = []string{"content"}
		withSource, err := reader.ReadActivityValue(t.Context(), scope, selection)
		if err != nil || len(withSource.TextFields) != 1 || string(withSource.Value) != string(selected.Value) {
			t.Fatalf("selected text source: %+v %v", withSource, err)
		}
		field := withSource.TextFields[0]
		part, err := public.(ThreadActivityFieldReader).ReadActivityField(t.Context(), scope, ThreadActivityFieldInput{ThreadID: thread, ItemID: input.ItemID, Path: field.SourcePath, EntryID: field.EntryID, SourceSHA256: field.SourceSHA256, SHA256: field.SHA256, OffsetBytes: field.SourceOffsetBytes})
		if err != nil || field.Bytes != len(row.Content) || !strings.HasPrefix(row.Content, part.Text) {
			t.Fatalf("selected text content: %+v %v", part, err)
		}
		input.MaximumBytes = 1024
		if _, err := reader.ReadActivityValue(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
			t.Fatalf("budget: %v", err)
		}
		if _, err := reader.ReadActivityValue(t.Context(), threadquery.Scope{Authority: "revoked"}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("authorization: %v", err)
		}
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestActivityValueLiveFieldsStayInOneSnapshot(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution forbidden") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	created, err := service.CreateAck(t.Context(), CreateThreadInput{RequestKey: "live"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.View(t.Context(), created.ThreadID); err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	install := func(i int) error {
		return actor.apply(t.Context(), func() error {
			text := fmt.Sprint(i)
			actor.state.view.Items = []ThreadItem{{ID: "tool:turn:tool", Kind: ThreadItemTool, TurnID: "turn", RunID: "run", Live: true, Activity: &observation.ActivityItem{Status: observation.ActivityStatusRunning, ToolID: "tool", Presentation: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Title: text, Meta: text, Content: strings.Repeat(text+"\n", 1000), Format: tools.StructuredActivityRowFormatCode}}}}}}}
			actor.state.view.ViewVersion++
			actor.invalidateItemIndex()
			return nil
		})
	}
	if err := install(0); err != nil {
		t.Fatal(err)
	}
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 1; i <= 100; i++ {
			if err := install(i); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	defer writers.Wait()
	for range 100 {
		value, err := service.ReadActivityValue(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}, ThreadActivityValueInput{ThreadID: created.ThreadID, ItemID: "tool:turn:tool", Path: "/payload/rows/0", MaximumBytes: 64 << 10, MaximumNodes: 16})
		if err != nil {
			t.Fatal(err)
		}
		var got tools.StructuredActivityRow
		if json.Unmarshal(value.Value, &got) != nil || got.Title != got.Meta || got.Content != strings.Repeat(got.Title+"\n", 1000) {
			t.Fatal("mixed live field versions")
		}
		selected, err := service.ReadActivityValue(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}, ThreadActivityValueInput{ThreadID: created.ThreadID, ItemID: "tool:turn:tool", Path: "/payload/rows/0", Fields: []string{"title", "meta"}, TextFields: []string{"content"}, MaximumBytes: 1024, MaximumNodes: 8})
		if err != nil || len(selected.TextFields) != 1 {
			t.Fatalf("selected snapshot: %+v %v", selected, err)
		}
		var metadata tools.StructuredActivityRow
		if json.Unmarshal(selected.Value, &metadata) != nil || metadata.Content != "" {
			t.Fatal("selected metadata includes body")
		}
		field := selected.TextFields[0]
		part, err := service.ReadActivityField(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}, ThreadActivityFieldInput{ThreadID: created.ThreadID, ItemID: selected.ItemID, Path: field.Path, SourceID: field.SourceID, RunID: field.RunID})
		if err != nil || part.Text != strings.Repeat(metadata.Title+"\n", 1000) {
			t.Fatalf("mixed metadata and text: %v", err)
		}
	}
}
