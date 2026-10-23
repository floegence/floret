package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

type pagedHydrationRepo struct {
	*internalstorage.BackendKernel
	paths, inventories, pages     int
	failAt                        int
	afterPage                     func(int)
	activityFields, activityReads int
	toolBodyFields                int
}

func (repo *pagedHydrationRepo) Path(ctx context.Context, id, leaf string) ([]sessiontree.Entry, error) {
	repo.paths++
	return repo.BackendKernel.Path(ctx, id, leaf)
}
func (repo *pagedHydrationRepo) Entries(ctx context.Context, id string) ([]sessiontree.Entry, error) {
	repo.inventories++
	return repo.BackendKernel.Entries(ctx, id)
}
func (repo *pagedHydrationRepo) ScanPathProjection(ctx context.Context, id, leaf string, prepare func([]sessiontree.PathMetadata) error, fields func(sessiontree.PathMetadata) []string, visit func(sessiontree.PathProjectionPage) error) error {
	return repo.BackendKernel.ScanPathProjection(ctx, id, leaf, prepare, func(source sessiontree.PathMetadata) []string {
		selected := fields(source)
		for _, field := range selected {
			if field == "activity_json" {
				repo.activityFields++
			}
			if (source.Entry.Type == sessiontree.EntryToolCall || source.Entry.Type == sessiontree.EntryToolResult) && (field == "arguments" || field == "content") {
				repo.toolBodyFields++
			}
		}
		return selected
	}, func(page sessiontree.PathProjectionPage) error {
		repo.pages++
		if repo.pages == repo.failAt {
			return errors.New("injected page failure")
		}
		if repo.afterPage != nil {
			repo.afterPage(repo.pages)
		}
		return visit(page)
	})
}

func (repo *pagedHydrationRepo) ReadCanonicalBody(ctx context.Context, threadID, entryID, field string, offset, limit int) (sessiontree.CanonicalBodyChunk, error) {
	if field == "activity_json" {
		repo.activityReads++
	}
	return repo.BackendKernel.ReadCanonicalBody(ctx, threadID, entryID, field, offset, limit)
}

func TestColdPagedHydrationMatchesExactFrozenCanonicalView(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "cold-pages"
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
	appendEntry(sessiontree.Entry{ID: "started", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	for i := range 1096 {
		appendEntry(sessiontree.Entry{ID: fmt.Sprint(i), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("historical 中🌸\r\n", 100)}})
	}
	for i := range 600 {
		appendEntry(sessiontree.Entry{ID: fmt.Sprint("answer-", i), Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, ContentSourceID: "answer", Content: fmt.Sprint(i) + "中🌸\r\n", Reasoning: "reason 🌸", ReasoningSourceID: "reason"}})
	}
	for i := range 300 {
		presentation := &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "read", Output: fmt.Sprint(i)}}
		kind, role := sessiontree.EntryToolCall, session.Assistant
		if i > 0 {
			kind, role = sessiontree.EntryToolResult, session.Tool
		}
		message := session.Message{Role: role, ToolName: "read", ToolCallID: "tool", Activity: presentation}
		if i == 0 {
			message.ToolArgs = "{}"
		} else {
			message.Content = strings.Repeat("large historical result 🌸", 100)
		}
		appendEntry(sessiontree.Entry{ID: fmt.Sprint("tool-", i), Type: kind, Message: message})
	}
	appendEntry(sessiontree.Entry{ID: "completed", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted})
	meta, err := host.store.repo.Thread(t.Context(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	want, err := service.loadCanonicalThreadView(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	repo := &pagedHydrationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel)}
	host.store.repo = repo
	repo.afterPage = func(page int) {
		if page == 1 {
			if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: "late", ThreadID: id.String(), Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ID: "late"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, err := service.loadRetainedThreadView(t.Context(), id, meta)
	if err != nil {
		t.Fatal(err)
	}
	if repo.paths != 0 || repo.inventories != 0 || repo.pages != 8 || got.projection.leafID != meta.LeafID {
		t.Fatalf("paths=%d inventory=%d pages=%d leaf=%s", repo.paths, repo.inventories, repo.pages, got.projection.leafID)
	}
	for _, item := range got.Items {
		if item.canonicalText && item.Text != "" {
			t.Fatal("cold snapshot retained body")
		}
	}
	if repo.activityFields != 0 || repo.activityReads != 0 {
		t.Fatalf("cold preparation read historical tool presentations: selected=%d reads=%d", repo.activityFields, repo.activityReads)
	}
	if repo.toolBodyFields != 0 {
		t.Fatalf("timeline read irrelevant tool bodies: %d", repo.toolBodyFields)
	}
	complete, err := service.completeViewResult(t.Context(), got.ThreadView, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(complete)
	b, _ := json.Marshal(want.ThreadView)
	if string(a) != string(b) {
		t.Fatal("paged hydration differs from frozen full projection")
	}
	for _, buffer := range got.projection.items.contentBuffers {
		if buffer.text.Len() != 0 {
			t.Fatal("cold reducer retained accumulated body")
		}
	}
	repo.afterPage, repo.pages, repo.failAt = nil, 0, 2
	if failed, err := service.loadRetainedThreadView(t.Context(), id, meta); err == nil || failed.projection != nil {
		t.Fatal("failed page returned partial canonical state")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.loadRetainedThreadView(ctx, id, meta); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	repo.pages, repo.failAt = 0, 0
	ctx, cancel = context.WithCancel(t.Context())
	repo.afterPage = func(page int) {
		if page == 1 {
			cancel()
		}
	}
	if failed, err := service.loadRetainedThreadView(ctx, id, meta); !errors.Is(err, context.Canceled) || failed.projection != nil {
		t.Fatalf("cancelled hydration escaped: %v", err)
	}
	repo.pages = 0
	repo.afterPage = func(page int) {
		if page == 1 {
			if err := repo.MoveLeaf(t.Context(), id.String(), "0"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if failed, err := service.loadRetainedThreadView(t.Context(), id, meta); !errors.Is(err, sessiontree.ErrEntryNotFound) || failed.projection != nil {
		t.Fatalf("changed active path escaped: %v", err)
	}
}

func TestColdActivityLocatorSurvivesRunningToolCompletion(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "cold-running-tool"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry) sessiontree.Entry {
		t.Helper()
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		saved, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID})
		if err != nil {
			t.Fatal(err)
		}
		return saved
	}
	appendEntry(sessiontree.Entry{ID: "started", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	appendEntry(sessiontree.Entry{ID: "call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "read", ToolArgs: "{}", Activity: &tools.ActivityPresentation{Label: "Read exact command", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "retained-command"}}}})
	meta, err := host.store.repo.Thread(t.Context(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	cold, err := service.loadRetainedThreadView(t.Context(), id, meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(cold.Items) != 1 || !cold.Items[0].activityOmitted || cold.Items[0].Activity.Status != observation.ActivityStatusRunning {
		t.Fatal("cold running activity lost its canonical locator")
	}
	result := appendEntry(sessiontree.Entry{ID: "result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "read", Content: "completed", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: "retained-output"}}}})
	cold.projection.resetChanges()
	if err := cold.projection.append([]sessiontree.Entry{result}); err != nil {
		t.Fatal(err)
	}
	items, _ := cold.projection.items.snapshot()
	full, err := service.loadCanonicalThreadView(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(items)
	want, _ := json.Marshal(full.Items)
	if string(got) != string(want) {
		t.Fatal("completion after cold loading changed the canonical tool presentation")
	}
}

func TestColdHydrationPreservesContextDependencyValidation(t *testing.T) {
	for _, entry := range []sessiontree.Entry{
		{Type: sessiontree.EntryCompaction},
		{Type: sessiontree.EntrySteeringRequested},
		{Type: sessiontree.EntrySteeringApplied},
		{Type: sessiontree.EntrySteeringCancelled},
		{Type: sessiontree.EntryInteractionDone},
		{Type: sessiontree.EntryModelContext, Message: session.Message{ModelContextSnapshot: &session.ModelContextSnapshot{}}},
		{Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{sessiontree.RetrySourceTurnIDMetadataKey: "source"}},
	} {
		if !canonicalContextNeedsCompleteValidation(entry) {
			t.Fatalf("dependency ignored: %s", entry.Type)
		}
	}
	if canonicalContextNeedsCompleteValidation(sessiontree.Entry{Type: sessiontree.EntryUserMessage}) {
		t.Fatal("ordinary user requires complete context")
	}
	projection := newThreadCanonicalProjection()
	if ids := projection.pendingSteering("turn"); !reflect.DeepEqual(ids, []string{}) {
		t.Fatal(ids)
	}
}

func TestColdProjectionFieldsKeepSemanticBodies(t *testing.T) {
	for _, test := range []struct {
		name, tool        string
		selected, control bool
		want              []string
	}{
		{"ordinary timeline", "read", false, false, []string{"reasoning", "control_output"}},
		{"selected context", "read", true, false, []string{"content", "arguments", "reasoning", "control_output"}},
		{"control validation", "read", false, true, []string{"content", "arguments", "reasoning", "control_output"}},
		{"goal presentation", "goal", false, false, []string{"content", "arguments", "reasoning", "control_output", "activity_json"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := sessiontree.PathMetadata{Entry: sessiontree.Entry{Type: sessiontree.EntryToolResult, Message: session.Message{ToolName: test.tool}}}
			for _, field := range []string{"content", "arguments", "reasoning", "control_output", "activity_json"} {
				source.Bodies = append(source.Bodies, sessiontree.CanonicalField{Field: field})
			}
			if got := coldProjectionFields(source, test.selected, test.control); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("selected fields = %v, want %v", got, test.want)
			}
		})
	}
}

func TestColdPublicWindowUsesFrozenPagesWithoutCompleteInventory(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "cold-public-window"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	for i := range 600 {
		entry := sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprintf("%d 中文🌸", i)}}
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	repo := &pagedHydrationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel)}
	host.store.repo = repo
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	var update ThreadUpdate
	for update.Window == nil {
		update = nextUpdate(t, sub)
	}
	window := update.Window.Value
	if len(window.Items) != 10 || window.FirstItemID != "590" || window.LastItemID != "599" || window.Items[9].Text != "599 中文🌸" {
		t.Fatalf("public window differs: %+v", window)
	}
	if repo.pages != 3 || repo.paths != 0 || repo.inventories != 0 {
		t.Fatalf("pages=%d paths=%d inventory=%d", repo.pages, repo.paths, repo.inventories)
	}
}

func TestColdPagedRetrySharesBodyTraversalWithContextValidation(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "cold-retry"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	add := func(entry sessiontree.Entry) {
		t.Helper()
		entry.ThreadID = id.String()
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	add(sessiontree.Entry{ID: "started", TurnID: "first", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	add(sessiontree.Entry{ID: "user", TurnID: "first", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "durable input"}})
	for i := range 520 {
		add(sessiontree.Entry{ID: fmt.Sprint("answer-", i), TurnID: "first", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, ContentSourceID: "answer", Content: "historical body 🌸"}})
	}
	add(sessiontree.Entry{ID: "discarded-call", TurnID: "first", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "discarded", ToolArgs: "{}"}})
	add(sessiontree.Entry{ID: "discarded-result", TurnID: "first", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "read", ToolCallID: "discarded", Content: strings.Repeat("discarded context body", 1000), ToolResult: &session.ToolResultView{Status: "success"}}})
	add(sessiontree.Entry{ID: "steer", TurnID: "first", RunID: "run", Type: sessiontree.EntrySteeringRequested, RequestKey: "steer", RequestFingerprint: "steer", Message: session.Message{Role: session.User, Content: "retained correction"}})
	add(sessiontree.Entry{ID: "managed", TurnID: "first", RunID: "run", Type: sessiontree.EntryModelContext, Message: session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "workspace", Title: "Workspace", Text: "retained managed fact"}}}}})
	payload, err := json.Marshal(sessiontree.SteeringResolution{SteeringID: "steer"})
	if err != nil {
		t.Fatal(err)
	}
	add(sessiontree.Entry{ID: "applied", TurnID: "first", RunID: "run", Type: sessiontree.EntrySteeringApplied, Payload: payload})
	add(sessiontree.Entry{ID: "done", TurnID: "first", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted})
	add(sessiontree.Entry{ID: "retry", TurnID: "second", RunID: "run2", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{sessiontree.RetrySourceTurnIDMetadataKey: "first", sessiontree.RetrySourceEntryIDMetadataKey: "user"}})
	add(sessiontree.Entry{ID: "retained-call", TurnID: "second", RunID: "run2", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "retained", ToolArgs: "{}"}})
	add(sessiontree.Entry{ID: "retained-result", TurnID: "second", RunID: "run2", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "read", ToolCallID: "retained", Content: "retained context body", ToolResult: &session.ToolResultView{Status: "success"}}})
	add(sessiontree.Entry{ID: "final", TurnID: "second", RunID: "run2", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, ContentSourceID: "final", Content: "retained answer"}})
	add(sessiontree.Entry{ID: "complete", TurnID: "second", RunID: "run2", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted})
	meta, err := host.store.repo.Thread(t.Context(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	want, err := service.loadCanonicalThreadView(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	repo := &pagedHydrationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel)}
	host.store.repo = repo
	got, err := service.loadRetainedThreadView(t.Context(), id, meta)
	if err != nil {
		t.Fatal(err)
	}
	if got.Context == nil || got.Context.ModelContext == nil || got.Context.ModelContext.EntryID != "managed" || got.Context.ModelContext.Snapshot.Sections[0].Text != "retained managed fact" {
		t.Fatal("Retry lost the managed context interleaved with accepted steering")
	}
	full, err := service.completeViewResult(t.Context(), got.ThreadView, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(full)
	b, _ := json.Marshal(want.ThreadView)
	if string(a) != string(b) || repo.paths != 0 || repo.pages != 3 {
		t.Fatalf("Retry projection differs: paths=%d pages=%d", repo.paths, repo.pages)
	}
	if repo.toolBodyFields != 2 {
		t.Fatalf("selected context must read both retained bodies and no discarded bodies: %d", repo.toolBodyFields)
	}
}
