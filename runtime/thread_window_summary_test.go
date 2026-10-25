package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/threadquery"
)

type noSummaryHistoryScanRepo struct{ *internalstorage.BackendKernel }

func TestWindowSummaryCloneDetachesOutcome(t *testing.T) {
	outcome := TurnOutcome("completed")
	original := ThreadUpdate{Window: &ThreadComponent[*ThreadWindowSnapshot]{Value: &ThreadWindowSnapshot{Summary: &ThreadSummary{LastOutcome: &outcome}}}}
	cloned := cloneThreadUpdate(original)
	*cloned.Window.Value.Summary.LastOutcome = TurnOutcome("failed")
	if *original.Window.Value.Summary.LastOutcome != outcome || outcome != TurnOutcome("completed") {
		t.Fatal("summary outcome aliases the original publication")
	}
}

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
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 1, ActivityFields: true, IncludeSummary: true}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	for baseline.Window == nil {
		baseline = nextUpdate(t, sub)
	}
	if baseline.Window == nil || len(baseline.Window.Value.Items) != 1 || baseline.Window.Value.Items[0].Kind != ThreadItemTool {
		t.Fatal("expected tool-only window")
	}
	if baseline.Window.Value.Summary == nil || baseline.Window.Value.Summary.ID != id || baseline.Window.Value.Summary.LastItemPreview != "retained summary" {
		t.Fatalf("baseline lost canonical preview: %+v", baseline.Window.Value.Summary)
	}
	copy := cloneThreadUpdate(baseline)
	copy.Window.Value.Summary.LastItemPreview = "consumer edit"
	if baseline.Window.Value.Summary.LastItemPreview != "retained summary" {
		t.Fatal("consumer mutated a queued summary")
	}
	host.store.repo = &noSummaryHistoryScanRepo{host.store.repo.(*internalstorage.BackendKernel)}
	summary, err := service.Summary(t.Context(), id)
	if err != nil || summary.LastItemPreview != "retained summary" {
		t.Fatalf("summary=%q err=%v", summary.LastItemPreview, err)
	}
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 1, ActivityFields: true}); err != nil {
		t.Fatal(err)
	}
	for next := nextUpdate(t, sub); ; next = nextUpdate(t, sub) {
		if next.Window != nil {
			if next.Window.Value.Summary != nil {
				t.Fatal("unrequested summary escaped")
			}
			break
		}
	}
}

func TestColdSummaryUsesCurrentCanonicalSourcesWithoutHistoryScan(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "cold-source-summary"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry) {
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	appendEntry(sessiontree.Entry{ID: "message", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained preview"}})
	for i := range 300 {
		appendEntry(sessiontree.Entry{ID: fmt.Sprint("hidden-", i), Type: sessiontree.EntryCustom})
	}
	appendEntry(sessiontree.Entry{ID: "done", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted})
	meta, err := host.store.repo.Thread(t.Context(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	path, err := host.store.repo.Path(t.Context(), id.String(), meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	want, err := completeSummaryOracle(meta, path)
	if err != nil {
		t.Fatal(err)
	}
	host.store.repo = &noSummaryHistoryScanRepo{host.store.repo.(*internalstorage.BackendKernel)}
	before := len(service.runtimes)
	got, err := service.Summary(t.Context(), id)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v want=%+v err=%v", got, want, err)
	}
	if len(service.runtimes) != before {
		t.Fatal("summary created an execution actor")
	}
}

func TestCurrentSummaryMatchesCompleteOracleAtEveryCanonicalPrefix(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "summary-prefixes"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	host.store.repo = &noSummaryHistoryScanRepo{host.store.repo.(*internalstorage.BackendKernel)}
	entries := incrementalProjectionFixture(t)
	encode := func(value any) []byte {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	entries = append(entries,
		sessiontree.Entry{ID: "queued", Type: sessiontree.EntryQueueAdded, Payload: encode(map[string]string{"id": "queued"})},
		sessiontree.Entry{ID: "next-start", RequestKey: "compact-request", TurnID: "next", RunID: "next-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{sessiontree.TurnKindMetadataKey: string(TurnKindContextCompaction)}},
		sessiontree.Entry{ID: "next-failure", TurnID: "next", RunID: "next-run", Type: sessiontree.EntryRunFailure, Error: "provider unavailable", Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: string(ThreadTurnFailureProvider)}},
		sessiontree.Entry{ID: "next-cancel", TurnID: "next", RunID: "next-run", Type: sessiontree.EntryCancelRequested, Metadata: map[string]string{"cancellation_source": "user_stop", "cancellation_mode": string(CancelModeGraceful)}},
		sessiontree.Entry{ID: "next-failed", TurnID: "next", RunID: "next-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnFailed},
		sessiontree.Entry{ID: "queue-deleted", Type: sessiontree.EntryQueueDeleted, Payload: encode("queued")},
		sessiontree.Entry{ID: "third-start", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted},
		sessiontree.Entry{ID: "third-reason", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryCustom, Metadata: map[string]string{"failure_reason": "retained failure metadata", sessiontree.TurnFailureCodeMetadataKey: string(ThreadTurnFailureProvider)}},
		sessiontree.Entry{ID: "third-reason-ignored", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryCustom, Metadata: map[string]string{"failure_reason": "later reason must not replace the first"}},
		sessiontree.Entry{ID: "third-reset", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryRunFailure, Error: "   "},
		sessiontree.Entry{ID: "third-new-reason", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryCustom, Metadata: map[string]string{"failure_reason": "reason after reset"}},
		sessiontree.Entry{ID: "third-no-cancel-source", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryCancelRequested},
		sessiontree.Entry{ID: "third-later-cancel", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryCancelRequested, Metadata: map[string]string{"cancellation_source": "user_stop", "cancellation_mode": string(CancelModeGraceful)}},
		sessiontree.Entry{ID: "third-failed", TurnID: "third", RunID: "third-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnFailed},
	)
	compare := func(label string) {
		t.Helper()
		meta, err := host.store.repo.Thread(t.Context(), id.String())
		if err != nil {
			t.Fatal(err)
		}
		path, err := host.store.repo.Path(t.Context(), id.String(), meta.LeafID)
		if err != nil {
			t.Fatal(err)
		}
		want, err := completeSummaryOracle(meta, path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := service.Summary(t.Context(), id)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got=%+v want=%+v err=%v", label, got, want, err)
		}
		if len(service.runtimes) != 0 {
			t.Fatal("summary created an execution actor")
		}
	}
	for _, entry := range entries {
		entry.ThreadID = id.String()
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatalf("append %s: %v", entry.ID, err)
		}
		compare(entry.ID)
	}
}

func TestCurrentSummaryPreservesPendingOlderControlsAndPathChanges(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "summary-controls"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(cancelHistoryAskArgs), &payload); err != nil {
		t.Fatal(err)
	}
	signal := &session.ControlSignalView{Name: "ask_user", CallID: "ask", Disposition: "waiting", OutputText: "Question", Payload: payload}
	input, _, err := waitingInputFromControlSignal(signal)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(ThreadInteraction{ID: "ask", TurnID: "old", RunID: "old-run", Kind: ThreadInteractionInput, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	entries := []sessiontree.Entry{
		{ID: "old-start", TurnID: "old", RunID: "old-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted},
		{ID: "ask", TurnID: "old", RunID: "old-run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, ToolCallID: "ask", ToolName: "ask_user", ControlSignal: signal}},
		{ID: "interaction-requested:ask", TurnID: "old", RunID: "old-run", Type: sessiontree.EntryInteractionAsked, Payload: raw},
		{ID: "old-wait", TurnID: "old", RunID: "old-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnWaiting},
		{ID: "latest-start", TurnID: "latest", RunID: "latest-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted},
		{ID: "latest-message", TurnID: "latest", RunID: "latest-run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "latest preview"}},
		{ID: "latest-done", TurnID: "latest", RunID: "latest-run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted},
		{ID: "interaction-resolved:ask", TurnID: "old", RunID: "old-run", Type: sessiontree.EntryInteractionDone, Payload: json.RawMessage("{\"accepted\":true}")},
	}
	compare := func(label string) {
		t.Helper()
		meta, err := host.store.repo.Thread(t.Context(), id.String())
		if err != nil {
			t.Fatal(err)
		}
		path, err := host.store.repo.Path(t.Context(), id.String(), meta.LeafID)
		if err != nil {
			t.Fatal(err)
		}
		want, err := completeSummaryOracle(meta, path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := service.Summary(t.Context(), id)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got=%+v want=%+v err=%v", label, got, want, err)
		}
	}
	for _, entry := range entries {
		entry.ThreadID = id.String()
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
		compare(entry.ID)
	}
	if err := host.store.repo.MoveLeaf(t.Context(), id.String(), "old-wait"); err != nil {
		t.Fatal(err)
	}
	compare("restored waiting branch")
}
