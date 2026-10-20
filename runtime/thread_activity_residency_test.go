package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestThreadCanonicalActivityResidencyPreservesEveryAdapter(t *testing.T) {
	path := t.TempDir() + "/activity.sqlite"
	open := func() (*Host, *threadRuntimeService) {
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
		return host, service.(*threadRuntimeService)
	}
	host, service := open()
	id := identity.ThreadID("activity-residency")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	call := &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Operation: "read", Inputs: []tools.StructuredActivityRow{{Title: "Input", Content: "canonical input"}}}}
	result := &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "success", RowsProvided: true, Rows: []tools.StructuredActivityRow{{Title: "File", Content: strings.Repeat("source 中🌸\r\n", 3000), Format: tools.StructuredActivityRowFormatCode}}}}
	appendFact := func(kind sessiontree.EntryType, activity *tools.ActivityPresentation, key string) {
		role := session.Assistant
		if kind == sessiontree.EntryToolResult {
			role = session.Tool
		}
		entry := sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: kind, Message: session.Message{Role: role, ToolName: "read", ToolCallID: "tool", Activity: activity}}
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: key}); err != nil {
			t.Fatal(err)
		}
	}
	appendFact(sessiontree.EntryToolCall, call, "call")
	appendFact(sessiontree.EntryToolResult, result, "result")
	expected, _ := json.Marshal(tools.MergeActivityPresentations(call, result))
	verify := func(view ThreadView) {
		t.Helper()
		if len(view.Items) != 1 || view.Items[0].Activity == nil {
			t.Fatalf("missing tool: %#v", view.Items)
		}
		raw, err := json.Marshal(view.Items[0].Activity.Presentation)
		if err != nil || string(raw) != string(expected) {
			t.Fatalf("complete adapter lost typed presentation: %v", err)
		}
	}
	for restart := range 2 {
		full, err := service.View(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		verify(full)
		actor := service.runtime(id)
		for _, item := range []ThreadItem{actor.state.view.Items[0], actor.canonical.items.items[0]} {
			if !item.activityOmitted || item.Activity.Presentation != nil {
				t.Fatal("canonical activity remained resident")
			}
		}
		sub, err := service.SubscribeWindowUpdates(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}})
		if err != nil {
			t.Fatal(err)
		}
		scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id}); err != nil {
			t.Fatal(err)
		}
		baseline := nextUpdate(t, sub)
		if baseline.Window == nil {
			t.Fatal("missing window")
		}
		verify(ThreadView{Items: baseline.Window.Value.Items})
		var bounded ThreadItem
		if err := actor.apply(t.Context(), func() error {
			var err error
			bounded, err = actor.windowItem(t.Context(), actor.state.view.Items[0], 128)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if bounded.Activity.Presentation != nil || len(bounded.ActivityBodies) != len(actor.state.view.Items[0].activityEntryIDs) {
			t.Fatal("large activity bypassed preview budget")
		}
		merged := (*tools.ActivityPresentation)(nil)
		for _, body := range bounded.ActivityBodies {
			var raw strings.Builder
			for offset := 0; offset < body.Bytes; {
				chunk, err := service.ReadBody(t.Context(), scope, ThreadBodyInput{ThreadID: id, EntryID: body.EntryID, Field: ThreadBodyActivityJSON, OffsetBytes: offset, SHA256: body.SHA256})
				if err != nil || chunk.Text == "" {
					t.Fatalf("activity read: %v", err)
				}
				raw.WriteString(chunk.Text)
				offset += len(chunk.Text)
			}
			var next tools.ActivityPresentation
			if raw.Len() > 0 {
				if err := json.Unmarshal([]byte(raw.String()), &next); err != nil {
					t.Fatal(err)
				}
				merged = tools.MergeActivityPresentations(merged, &next)
			}
		}
		raw, err := json.Marshal(merged)
		if err != nil || string(raw) != string(expected) {
			t.Fatalf("chunked activity changed merge: %v", err)
		}
		sub.Close()
		if restart == 0 {
			legacy, err := service.Subscribe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			native, err := service.SubscribeUpdates(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			next := &tools.ActivityPresentation{Label: "Inspection updated", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "success", RowsProvided: true}}
			appendFact(sessiontree.EntryToolResult, next, "replacement")
			expected, _ = json.Marshal(tools.MergeActivityPresentations(tools.MergeActivityPresentations(call, result), next))
			if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); err != nil {
				t.Fatal(err)
			}
			full, err = service.View(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			verify(full)
			observed, err := legacy.Next(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			verify(observed)
			update := nextUpdate(t, native)
			found := false
			for _, change := range update.Items {
				if change.Item != nil {
					found = true
					verify(ThreadView{Items: []ThreadItem{*change.Item}})
				}
			}
			if !found {
				t.Fatal("missing native tool update")
			}
			legacy.Close()
			native.Close()
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
		}
	}
}
