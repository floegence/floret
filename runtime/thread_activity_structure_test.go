package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestThreadActivityStructureDefaultsAndRestart(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "typed", true: "historical-default"}[missing], func(t *testing.T) {
			path := t.TempDir() + "/activity.sqlite"
			open := func() (*Host, *threadRuntimeService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
				if err != nil {
					t.Fatal(err)
				}
				return host, public.(*threadRuntimeService)
			}
			host, service := open()
			const id identity.ThreadID = "structure"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			var activity *tools.ActivityPresentation
			if !missing {
				activity = &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Content: strings.Repeat("read 中文🌸", 2000), Format: tools.StructuredActivityRowFormatCode}}, RowsProvided: true}}
			}
			entries := []sessiontree.Entry{
				{ID: "request", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect"}},
				{ID: "call", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "tool", Activity: activity}},
				{ID: "result", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "tool", Content: "ok", Activity: activity}},
			}
			for _, entry := range entries {
				if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			for restart := range 2 {
				input := ThreadActivityStructureInput{ThreadID: id, ItemID: "tool:turn:tool", Limit: 2}
				root, err := service.ActivityStructure(t.Context(), scope, input)
				if err != nil {
					t.Fatal(err)
				}
				input.Version = root.Version
				var read func(string) any
				read = func(path string) any {
					request := input
					request.Path = path
					page, err := service.ActivityStructure(t.Context(), scope, request)
					if err != nil {
						t.Fatal(err)
					}
					if page.Version != root.Version || page.ItemID != input.ItemID {
						t.Fatal("wrong structure identity")
					}
					switch page.Field.Kind {
					case "string":
						if page.Field.EntryID == "" {
							return page.Field.InlineText
						}
						var result strings.Builder
						for result.Len() < page.Field.Bytes {
							part, err := service.ReadActivityField(t.Context(), scope, ThreadActivityFieldInput{ThreadID: id, ItemID: input.ItemID, EntryID: page.Field.EntryID, Path: page.Field.SourcePath, SourceSHA256: page.Field.SourceSHA256, SHA256: page.Field.SHA256, OffsetBytes: result.Len() + page.Field.SourceOffsetBytes})
							if err != nil {
								t.Fatal(err)
							}
							if part.Text == "" {
								t.Fatal("nonadvancing field")
							}
							result.WriteString(part.Text[:min(len(part.Text), page.Field.Bytes-result.Len())])
						}
						return result.String()
					case "scalar":
						var value any
						if json.Unmarshal(page.Field.Value, &value) != nil {
							t.Fatal("scalar")
						}
						return value
					}
					children := append([]ThreadActivityField(nil), page.Children...)
					for page.HasMore {
						request.Offset = page.Next
						page, err = service.ActivityStructure(t.Context(), scope, request)
						if err != nil {
							t.Fatal(err)
						}
						children = append(children, page.Children...)
					}
					if page.Field.Kind == "array" {
						values := make([]any, 0, len(children))
						for _, field := range children {
							values = append(values, read(field.Path))
						}
						return values
					}
					values := map[string]any{}
					for _, field := range children {
						values[field.Path[strings.LastIndex(field.Path, "/")+1:]] = read(field.Path)
					}
					return values
				}
				got := read("")
				view, err := service.View(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				var expected *tools.ActivityPresentation
				for _, item := range view.Items {
					if item.ID == input.ItemID {
						expected = item.Activity.Presentation
					}
				}
				raw, err := json.Marshal(expected)
				if err != nil {
					t.Fatal(err)
				}
				var want any
				if json.Unmarshal(raw, &want) != nil {
					t.Fatal("want json")
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("structure differs: got=%#v want=%s", got, raw)
				}
				input.Version = "stale"
				if _, err := service.ActivityStructure(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
					t.Fatalf("stale structure: %v", err)
				}
				if _, err := service.ActivityStructure(t.Context(), threadquery.Scope{Authority: "reader"}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
					t.Fatalf("revoked scope: %v", err)
				}
				if restart == 0 {
					if err := host.Shutdown(t.Context()); err != nil {
						t.Fatal(err)
					}
					host, service = open()
				}
			}
		})
	}
}

// Advance storage after actor selection but before the membership read.
type appendDuringActivityReadRepo struct {
	sessiontree.Repo
	sessiontree.ProjectionSourceScanner
	sessiontree.ActivityStructureReader
	sessiontree.CanonicalBodyMetadataReader
	membership sessiontree.ItemMembershipReader
	append     func() error
}

func (repo *appendDuringActivityReadRepo) ItemMembershipWindow(ctx context.Context, input sessiontree.ItemMembershipWindowOptions) (sessiontree.ItemMembershipWindow, error) {
	if repo.append != nil {
		append := repo.append
		repo.append = nil
		if err := append(); err != nil {
			return sessiontree.ItemMembershipWindow{}, err
		}
	}
	return repo.membership.ItemMembershipWindow(ctx, input)
}
func TestActivityStructureReadSurvivesConcurrentAppend(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/append.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "concurrent-structure"
	repo := host.store.repo
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []sessiontree.Entry{
		{ID: "request", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "read"}},
		{ID: "call", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "tool", Activity: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{}}}},
	} {
		if _, err := repo.Append(t.Context(), entry, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	scope := threadquery.Scope{Authority: "viewer", ThreadIDs: []identity.ThreadID{id}}
	if _, err := service.ActivityStructure(t.Context(), scope, ThreadActivityStructureInput{ThreadID: id, ItemID: "tool:turn:tool"}); err != nil {
		t.Fatal(err)
	}
	host.store.repo = &appendDuringActivityReadRepo{Repo: repo, ProjectionSourceScanner: repo.(sessiontree.ProjectionSourceScanner), ActivityStructureReader: repo.(sessiontree.ActivityStructureReader), CanonicalBodyMetadataReader: repo.(sessiontree.CanonicalBodyMetadataReader), membership: repo.(sessiontree.ItemMembershipReader), append: func() error {
		_, err := repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "continues"}}, sessiontree.AppendOptions{})
		return err
	}}
	defer func() { host.store.repo = repo }()
	page, err := service.ActivityStructure(t.Context(), scope, ThreadActivityStructureInput{ThreadID: id, ItemID: "tool:turn:tool"})
	if err != nil {
		t.Fatalf("append invalidated detail: %v", err)
	}
	if page.ItemID != "tool:turn:tool" || page.Field.Kind != "object" {
		t.Fatalf("wrong structure: %+v", page)
	}
}

func TestItemBodyReadSurvivesConcurrentAppend(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/body-append.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "concurrent-body"
	repo := host.store.repo
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), sessiontree.Entry{ID: "request", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "read history"}}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	input := ThreadItemBodyInput{ThreadID: id, ItemID: "request"}
	want, err := service.ItemBody(t.Context(), scope, input)
	if err != nil {
		t.Fatal(err)
	}
	host.store.repo = &appendDuringActivityReadRepo{Repo: repo, ProjectionSourceScanner: repo.(sessiontree.ProjectionSourceScanner), membership: repo.(sessiontree.ItemMembershipReader), append: func() error {
		_, err := repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "working"}}, sessiontree.AppendOptions{})
		return err
	}}
	defer func() { host.store.repo = repo }()
	got, err := service.ItemBody(t.Context(), scope, input)
	if err != nil || got != want {
		t.Fatalf("append invalidated text locator: got=%+v want=%+v err=%v", got, want, err)
	}
}
