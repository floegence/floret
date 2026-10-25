package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestActivityBodyDefaultsPreserveSourceMergeAcrossRestart(t *testing.T) {
	for _, missing := range []string{"call", "result"} {
		t.Run(missing, func(t *testing.T) {
			path := t.TempDir() + "/defaults.sqlite"
			const id identity.ThreadID = "activity-defaults"
			for restart := range 2 {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution forbidden") }))
				if err != nil {
					t.Fatal(err)
				}
				service := public.(*threadRuntimeService)
				if restart == 0 {
					if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
						t.Fatal(err)
					}
					large := &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{
						{Content: strings.Repeat("a", 64000), Format: tools.StructuredActivityRowFormatCode},
						{Content: strings.Repeat("b", 64000), Format: tools.StructuredActivityRowFormatCode},
						{Content: strings.Repeat("c", 64000), Format: tools.StructuredActivityRowFormatCode},
					}}}
					for _, key := range []string{"call", "result"} {
						entry := sessiontree.Entry{ID: key, ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "tool", Activity: large}}
						if key == "result" {
							entry.Type, entry.Message.Role = sessiontree.EntryToolResult, session.Tool
						}
						if key == missing {
							entry.Message.Activity = nil
						}
						if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{}); err != nil {
							t.Fatal(err)
						}
					}
				}
				scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
				sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
				if err != nil {
					t.Fatal(err)
				}
				if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 1}); err != nil {
					t.Fatal(err)
				}
				update := nextUpdate(t, sub)
				for update.Window == nil || update.Window.Value == nil {
					update = nextUpdate(t, sub)
				}
				window := update.Window.Value
				if len(window.Items) != 1 || len(window.Items[0].ActivityBodies) != 2 {
					t.Fatalf("missing deferred sources: %+v", window.Items)
				}
				var merged *tools.ActivityPresentation
				for _, source := range window.Items[0].ActivityBodies {
					var next *tools.ActivityPresentation
					if source.Bytes == 0 {
						// Inspect the wire contract so the regression also fails before
						// the additive default field exists in the public Go type.
						raw, _ := json.Marshal(source)
						var value struct {
							Default *tools.ActivityPresentation `json:"default_presentation"`
						}
						if json.Unmarshal(raw, &value) != nil || value.Default == nil {
							t.Fatal("empty activity source has no canonical default")
						}
						next = value.Default
					} else {
						var raw strings.Builder
						for raw.Len() < source.Bytes {
							chunk, err := service.ReadBody(t.Context(), scope, ThreadBodyInput{ThreadID: id, EntryID: source.EntryID, Field: ThreadBodyActivityJSON, OffsetBytes: raw.Len(), SHA256: source.SHA256})
							if err != nil || chunk.Text == "" {
								t.Fatalf("source read: %v", err)
							}
							raw.WriteString(chunk.Text)
						}
						if err := json.Unmarshal([]byte(raw.String()), &next); err != nil {
							t.Fatal(err)
						}
					}
					merged = tools.MergeActivityPresentations(merged, next)
				}
				full, err := service.View(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				want, _ := json.Marshal(full.Items[0].Activity.Presentation)
				got, _ := json.Marshal(merged)
				if string(got) != string(want) {
					t.Fatal("default source merge differs from canonical View")
				}
				sub.Close()
				if err := host.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestActivityBodyDefaultSnapshotIsDetached(t *testing.T) {
	original := []ThreadItem{{ActivityBodies: []ThreadActivityBody{{DefaultPresentation: &tools.ActivityPresentation{
		Label: "Default", Renderer: tools.ActivityRendererStructured,
		Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "original"}}},
	}}}}}
	copy := cloneThreadItems(original)
	copy[0].ActivityBodies[0].DefaultPresentation.Label = "Changed"
	payload := copy[0].ActivityBodies[0].DefaultPresentation.Payload.(tools.StructuredActivityPayload)
	payload.Rows[0].Content = "Changed"
	value := original[0].ActivityBodies[0].DefaultPresentation
	if value.Label != "Default" || value.Payload.(tools.StructuredActivityPayload).Rows[0].Content != "original" {
		t.Fatal("activity default shares mutable state across snapshots")
	}
}
