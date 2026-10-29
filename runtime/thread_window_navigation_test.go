package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"testing"
)

func TestWindowRangeNavigationKeepsProtectedRowsAndStableInterest(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprint(warm), func(t *testing.T) {
			host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/navigation.sqlite"), DeferExecution: true})
			if err != nil {
				t.Fatal(err)
			}
			defer host.Shutdown(context.Background())
			public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
			if err != nil {
				t.Fatal(err)
			}
			service := public.(*threadRuntimeService)
			const id identity.ThreadID = "navigation"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			add := func(e sessiontree.Entry) {
				e.ThreadID = id.String()
				e.TurnID = "turn"
				e.RunID = "run"
				if _, err := host.store.repo.Append(t.Context(), e, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			add(sessiontree.Entry{ID: "user", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "request"}})
			for i := range 80 {
				add(sessiontree.Entry{ID: fmt.Sprint(i), Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: fmt.Sprint(i)}})
			}
			add(sessiontree.Entry{ID: "tail", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "answer"}})
			if warm {
				if _, err := service.View(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			}
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			input := ThreadWindowInput{ThreadID: id, AnchorID: "tool:turn:60", Direction: ThreadWindowBefore, Limit: 1, RetainItemIDs: []string{"assistant:entry:tail", "tool:turn:0", "tool:turn:79"}, SkipRange: &ThreadWindowRange{Kinds: []ThreadItemKind{ThreadItemTool}}, ActivityFields: true}
			if err := sub.SetWindow(t.Context(), scope, input); err != nil {
				t.Fatal(err)
			}
			var window *ThreadWindowSnapshot
			for window == nil {
				update := nextUpdate(t, sub)
				if update.Window != nil {
					window = update.Window.Value
				}
			}
			if len(window.Items) != 4 || window.Items[0].ID != "user" || window.Items[3].ID != "assistant:entry:tail" || window.SkippedRange == nil || window.SkippedRange.Counts.Tool != 80 {
				t.Fatalf("navigation: %+v", window)
			}
			interest := sub.windows[id.String()]
			if interest.sourceInput.SkipRange != nil || len(interest.selection) != 4 || interest.tail {
				t.Fatalf("navigation repeated: %+v", interest.sourceInput)
			}
		})
	}
}

func TestWindowNavigationRejectsMalformedSelections(t *testing.T) {
	for _, input := range []ThreadWindowInput{
		{Limit: 40, RetainItemIDs: []string{"x", "x"}},
		{Limit: 149, RetainItemIDs: []string{"x"}},
		{Limit: 40, AnchorID: "x", Direction: ThreadWindowBefore, SkipRange: &ThreadWindowRange{Kinds: []ThreadItemKind{"invalid"}}},
		{Limit: 40, AnchorID: "x", Direction: ThreadWindowBefore, SkipRange: &ThreadWindowRange{Kinds: []ThreadItemKind{ThreadItemTool}, Exclude: 1 << 15}},
	} {
		if validateWindowSelection(input) == nil {
			t.Fatalf("accepted %+v", input)
		}
	}
}
