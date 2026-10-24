package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/threadquery"
)

type noSummaryHistoryScanRepo struct{ *internalstorage.BackendKernel }

func (repo *noSummaryHistoryScanRepo) PathMetadataWindow(context.Context, sessiontree.PathWindowOptions) (sessiontree.PathMetadataWindow, error) {
	return sessiontree.PathMetadataWindow{}, errors.New("summary rescanned historical metadata")
}

func TestPreparedToolWindowSummaryDoesNotRescanHistory(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "tool-window-summary"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry) {
		t.Helper()
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "message", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained summary"}})
	for i := range 80 {
		call := fmt.Sprint(i)
		appendEntry(sessiontree.Entry{ID: "call-" + call, Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: call, ToolArgs: "{}"}})
		appendEntry(sessiontree.Entry{ID: "result-" + call, Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: call, Content: "done", ToolResult: &session.ToolResultView{Status: "success"}}})
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 1, ActivityFields: true}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	for baseline.Window == nil {
		baseline = nextUpdate(t, sub)
	}
	if baseline.Window == nil || len(baseline.Window.Value.Items) != 1 || baseline.Window.Value.Items[0].Kind != ThreadItemTool {
		t.Fatal("expected tool-only window")
	}
	host.store.repo = &noSummaryHistoryScanRepo{host.store.repo.(*internalstorage.BackendKernel)}
	summary, err := service.Summary(t.Context(), id)
	if err != nil || summary.LastItemPreview != "retained summary" {
		t.Fatalf("summary=%q err=%v", summary.LastItemPreview, err)
	}
}
