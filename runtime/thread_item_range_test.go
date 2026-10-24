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

func TestThreadItemRangeAuthorizesCanonicalCountsWithoutExecution(t *testing.T) {
	path := t.TempDir() + "/range.sqlite"
	open := func() (*Host, *threadRuntimeService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
			t.Error("range executed provider preparation")
			return nil, errors.New("unexpected execution")
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service.(*threadRuntimeService)
	}
	host, service := open()
	const id identity.ThreadID = "canonical-range"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(e sessiontree.Entry) {
		t.Helper()
		e.ThreadID = id.String()
		e.TurnID = "turn"
		e.RunID = "run"
		if _, err := host.store.repo.Append(t.Context(), e, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "user", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect"}})
	for i := range 50 {
		appendEntry(sessiontree.Entry{ID: fmt.Sprint("call-", i), Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: fmt.Sprint(i)}})
	}
	appendEntry(sessiontree.Entry{ID: "end", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "done"}})
	scope := threadquery.Scope{Authority: "authorized", ThreadIDs: []identity.ThreadID{id}}
	input := ThreadItemRangeInput{ThreadID: id, AnchorID: "tool:turn:25", Kinds: []ThreadItemKind{ThreadItemTool, ThreadItemThinking}, Exclude: ThreadItemRangeUncertainEffect | ThreadItemRangeApproval}
	check := func(wantErrors int) {
		t.Helper()
		r, err := service.ItemRange(t.Context(), scope, input)
		if err != nil {
			t.Fatal(err)
		}
		if r.FirstItemID != "tool:turn:0" || r.LastItemID != "tool:turn:49" || r.Counts.Tool != 50 || r.Activity.Error != wantErrors || r.Activity.Running != 50-wantErrors || !r.HasBefore || !r.HasAfter || r.FirstOrdinal != 2 || r.LastOrdinal != 51 {
			t.Fatalf("range differs: %+v", r)
		}
	}
	check(0)
	appendEntry(sessiontree.Entry{ID: "late-result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "2", ToolResult: &session.ToolResultView{Status: "error"}}})
	check(1)
	if _, err := service.ItemRange(t.Context(), threadquery.Scope{Authority: "revoked"}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("revoked read: %v", err)
	}
	invalid := input
	invalid.Kinds = []ThreadItemKind{"invalid"}
	if _, err := service.ItemRange(t.Context(), scope, invalid); !errors.Is(err, threadquery.ErrInvalidQuery) {
		t.Fatalf("invalid kinds: %v", err)
	}
	invalid = input
	invalid.AnchorID = "missing"
	if _, err := service.ItemRange(t.Context(), scope, invalid); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("missing identity: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.ItemRange(ctx, scope, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: %v", err)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	check(1)
}
