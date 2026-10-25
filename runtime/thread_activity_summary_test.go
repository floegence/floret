package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestActivityFieldWindowRetainsSafetyWithoutHiddenBodies(t *testing.T) {
	for _, count := range []int{0, 1, 180} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/window.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
			if err != nil {
				t.Fatal(err)
			}
			service := public.(*threadRuntimeService)
			const id identity.ThreadID = "field-window"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			var presentation *tools.ActivityPresentation
			if count > 0 {
				rows := make([]tools.StructuredActivityRow, count)
				for i := range rows {
					rows[i] = tools.StructuredActivityRow{Content: strings.Repeat("HIDDEN", 10000), Format: tools.StructuredActivityRowFormatCode}
				}
				presentation = &tools.ActivityPresentation{Label: "Inspect", Description: "Exact visible summary", Renderer: tools.ActivityRendererStructured, Chips: []tools.ActivityChip{{Kind: "effect_outcome", Label: "Effect", Value: "uncertain"}}, Payload: tools.StructuredActivityPayload{Rows: rows, Error: &tools.ActivityError{Message: "retained evidence"}}}
			}
			if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: "call", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "call", Activity: presentation}}, sessiontree.AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: "result", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call", Activity: presentation}}, sessiontree.AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, ActivityFields: true}); err != nil {
				t.Fatal(err)
			}
			baseline := nextUpdate(t, sub)
			for baseline.Window == nil {
				baseline = nextUpdate(t, sub)
			}
			if baseline.Window == nil || len(baseline.Window.Value.Items) != 1 {
				t.Fatal("missing exact window")
			}
			item := baseline.Window.Value.Items[0]
			if item.Activity == nil || item.Activity.Presentation != nil || len(item.ActivityBodies) != 0 || item.ActivitySummary == nil {
				t.Fatal("hidden activity escaped field mode")
			}
			if count == 0 {
				if item.ActivitySummary.Label != "inspect" || item.ActivitySummary.Renderer != "structured" {
					t.Fatalf("historical default lost: %+v", item.ActivitySummary)
				}
			} else if item.ActivitySummary.Label != "Inspect" || item.ActivitySummary.Description != "Exact visible summary" || !item.ActivitySummary.HasError || !item.ActivitySummary.UncertainEffect {
				t.Fatalf("safety facts lost: %+v", item.ActivitySummary)
			}
			encoded, err := json.Marshal(baseline)
			if err != nil || len(encoded) > 8192 || strings.Contains(string(encoded), "HIDDEN") {
				t.Fatalf("unbounded summary: bytes=%d %v", len(encoded), err)
			}
			// An independent full read retains the existing public behavior.
			full, err := service.View(t.Context(), id)
			if err != nil || full.Items[0].Activity.Presentation == nil || full.Items[0].ActivitySummary != nil {
				t.Fatalf("field option mutated canonical/full view: %v", err)
			}
		})
	}
}

func TestActivityFieldSummaryRetainsGoalDispositionWithoutText(t *testing.T) {
	presentation := &tools.ActivityPresentation{Label: "Goal", Renderer: tools.ActivityRendererStructured,
		Payload: tools.StructuredActivityPayload{Goal: &tools.GoalActivity{Operation: "review", Version: 3, Outcome: "continue",
			Text: strings.Repeat("hidden goal", 10000), Evidence: strings.Repeat("hidden evidence", 10000)}}}
	got := residentActivitySummary(presentation)
	var state map[string]any
	if err := json.Unmarshal(got.Values["/payload/goal"], &state); err != nil {
		t.Fatalf("Goal disposition missing: %v", err)
	}
	if !reflect.DeepEqual(state, map[string]any{"operation": "review", "applied": false, "version": float64(3), "outcome": "continue"}) {
		t.Fatalf("Goal disposition differs: %v", state)
	}
	raw, err := json.Marshal(got)
	if err != nil || len(raw) > 2048 || strings.Contains(string(raw), "hidden") {
		t.Fatalf("Goal summary exposes text: bytes=%d err=%v", len(raw), err)
	}
}

func TestActivityFieldWindowLiveUpdatesNeverTransportHiddenPayload(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	activity := func(text string) ThreadItem {
		return ThreadItem{ID: "tool", TurnID: "turn", RunID: "run", Kind: ThreadItemTool, Activity: &observation.ActivityItem{ItemID: "tool", ToolID: "call", Status: observation.ActivityStatusRunning, Presentation: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: text}}}}
	}
	actor.state.view.Items = []ThreadItem{activity(strings.Repeat("hidden-body", 1000000))}
	actor.invalidateItemIndex()
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", ActivityFields: true}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	if baseline.Window.Value.Items[0].ActivitySummary == nil {
		t.Fatal("missing live summary")
	}
	actor.mu.Lock()
	sub.mu.Lock()
	item := activity(strings.Repeat("changed-body", 1000000))
	update, ok := sub.projectWindowUpdate(actor, ThreadUpdate{ThreadID: "thread-live", BaseVersion: baseline.ViewVersion, ViewVersion: baseline.ViewVersion + 1, Items: []ThreadItemChange{{Kind: ThreadItemReplaced, ItemID: "tool", Item: &item}}})
	sub.mu.Unlock()
	actor.mu.Unlock()
	if !ok || len(update.Items) != 1 || update.Items[0].Item.ActivitySummary == nil {
		t.Fatal("lost live activity update")
	}
	encoded, err := json.Marshal(update)
	if err != nil || len(encoded) > 4096 || strings.Contains(string(encoded), "changed-body") {
		t.Fatalf("hidden live payload transported: bytes=%d %v", len(encoded), err)
	}
}

type appendAfterWindowProjectionRepo struct {
	*internalstorage.BackendKernel
	once   sync.Once
	append func()
}

func (repo *appendAfterWindowProjectionRepo) ReadWindowProjection(ctx context.Context, options sessiontree.ItemMembershipWindowOptions, fields func(sessiontree.PathMetadata, bool) []string) (sessiontree.WindowProjection, error) {
	page, err := repo.BackendKernel.ReadWindowProjection(ctx, options, fields)
	if err == nil {
		repo.once.Do(repo.append)
	}
	return page, err
}

func TestActivityFieldWindowFreezesSummaryWithSelectedSources(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "summary-snapshot"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entryID string, kind sessiontree.EntryType, label string) {
		role := session.Assistant
		if kind == sessiontree.EntryToolResult {
			role = session.Tool
		}
		entry := sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: kind, Message: session.Message{Role: role, ToolName: "inspect", ToolCallID: "call", Activity: &tools.ActivityPresentation{Label: label, Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: "hidden"}}}}
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entryID}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry("call", sessiontree.EntryToolCall, "before")
	host.store.repo = &appendAfterWindowProjectionRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), append: func() { appendEntry("result", sessiontree.EntryToolResult, "after") }}
	view, _, err := service.loadSourceWindow(t.Context(), ThreadWindowInput{ThreadID: id, Limit: 1, ActivityFields: true})
	if err != nil {
		t.Fatal(err)
	}
	item, err := service.runtime(id).windowItemMode(t.Context(), view.Items[0], 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	if item.ActivitySummary == nil || item.ActivitySummary.Label != "before" || item.Activity.Status != observation.ActivityStatusRunning {
		t.Fatalf("mixed snapshot: summary=%+v status=%s", item.ActivitySummary, item.Activity.Status)
	}
}

func TestActivitySummaryShapeMatchesResidentAndCanonical(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "summary-shape"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	p := &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererStructured, TargetRefs: []tools.ActivityTargetRef{{Kind: "file", Label: "source", Path: "source.go"}}, Payload: tools.StructuredActivityPayload{RowsProvided: true, Rows: []tools.StructuredActivityRow{{Content: strings.Repeat("hidden", 10000), Format: tools.StructuredActivityRowFormatCode}}}}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: "call", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "call", Activity: p}}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	view, _, err := service.loadSourceWindow(t.Context(), ThreadWindowInput{ThreadID: id, Limit: 1, ActivityFields: true})
	if err != nil {
		t.Fatal(err)
	}
	got := view.Items[0].activitySummary
	if got == nil {
		t.Fatal("missing canonical summary")
	}
	want := residentActivitySummary(p)
	if !reflect.DeepEqual(got.Values, want.Values) {
		t.Fatalf("display facts differ: got=%s want=%s", got.Values, want.Values)
	}
	normalize := func(fields []ThreadActivityField) string {
		copied := cloneActivitySummaryFields(fields)
		for i := range copied {
			copied[i].EntryID = ""
			copied[i].SourcePath = ""
			copied[i].SourceSHA256 = ""
			if string(copied[i].Value) == "null" && copied[i].Kind != "scalar" {
				copied[i].Value = nil
			}
		}
		raw, _ := json.Marshal(copied)
		return string(raw)
	}
	if normalize(got.Fields) != normalize(want.Fields) {
		t.Fatalf("canonical=%s resident=%s", normalize(got.Fields), normalize(want.Fields))
	}
	item := ThreadItem{ActivitySummary: got, activitySummary: got}
	cloned := cloneThreadItems([]ThreadItem{item})[0]
	for i, field := range cloned.ActivitySummary.Fields {
		if len(field.Value) > 0 {
			cloned.ActivitySummary.Fields[i].Value[0] = 'x'
			break
		}
	}
	if normalize(got.Fields) != normalize(want.Fields) {
		t.Fatal("public clone mutated canonical summary")
	}
	for key, value := range cloned.ActivitySummary.Values {
		if len(value) > 0 {
			value[0] = 'x'
			delete(cloned.ActivitySummary.Values, key)
			break
		}
	}
	if !reflect.DeepEqual(got.Values, want.Values) {
		t.Fatal("public clone shares display values")
	}
	cloned.ActivitySummary.Fields[0].Path = "changed"
	if got.Fields[0].Path == "changed" {
		t.Fatal("public clone shares shape slice")
	}
}

func TestActivitySummaryVersionChangesOnlyWithItsActivity(t *testing.T) {
	_, actor, _, subscription := nativeUpdatesFixture(t)
	subscription.Close()
	actor.state.turnID, actor.state.runID = "turn", "run"
	activity := func(text string) observation.ActivityItem {
		return observation.ActivityItem{ToolID: "call", Kind: observation.ActivityKindTool, Status: observation.ActivityStatusRunning, Presentation: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: text}}}
	}
	actor.applyActivityItems(identity.ThreadID(actor.threadID), "turn", "run", []observation.ActivityItem{activity("before")}, time.Now())
	index := actor.itemIndexByID(threadToolSegmentID("turn", "call"))
	first := actor.freezeWindowItem(actor.state.view.Items[index], 1024, true).ActivitySummary
	actor.state.view.ViewVersion++
	unchanged := actor.freezeWindowItem(actor.state.view.Items[index], 1024, true).ActivitySummary
	if first == nil || first.Version == "" || unchanged.Version != first.Version {
		t.Fatalf("unrelated update changed activity identity: first=%+v next=%+v", first, unchanged)
	}
	actor.state.view.Items[index].activitySummary = first
	actor.applyActivityItems(identity.ThreadID(actor.threadID), "turn", "run", []observation.ActivityItem{activity("change")}, time.Now())
	second := actor.freezeWindowItem(actor.state.view.Items[index], 1024, true).ActivitySummary
	if second == nil || second.Version == first.Version {
		t.Fatalf("equal shape hid changed content: first=%+v next=%+v", first, second)
	}
	if actor.state.view.Items[index].activitySummary != nil {
		t.Fatal("live replacement retained an earlier canonical summary")
	}
}

func TestActivityFieldGoalCanonicalRestart(t *testing.T) {
	path := t.TempDir() + "/goal.sqlite"
	goalText := strings.Repeat("Retain the full canonical goal. ", 3000)
	evidence := strings.Repeat("Verified canonical result. ", 2000)
	args, err := json.Marshal(map[string]any{"operation": "review", "expected_version": 1, "outcome": "completed", "evidence": evidence})
	if err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "goal-fields", StateCompatibilityKey: "test:goal-fields"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, goalCall("review", string(args)), parentEventDone())
	agent, err := testAgent(gateway, WithAgentGoals())
	if err != nil {
		t.Fatal(err)
	}
	var thread identity.ThreadID
	for restart := range 2 {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		if restart == 0 {
			created, err := public.Create(t.Context(), CreateThreadInput{RequestKey: "goal-fields"})
			if err != nil {
				t.Fatal(err)
			}
			thread = created.ThreadID
			if _, err = public.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: thread, RequestKey: "goal", Text: goalText}); err != nil {
				t.Fatal(err)
			}
			if _, err = public.Send(t.Context(), SendInput{ThreadID: thread, RequestKey: "send", Input: UserInput{Text: "Complete the goal"}}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(20 * time.Second)
			for {
				summary, err := public.(ThreadSummaryReader).Summary(t.Context(), thread)
				if err != nil {
					t.Fatal(err)
				}
				if summary.Activity == ThreadActivityIdle && summary.LastOutcome != nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("Goal fixture did not settle")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		scope := threadquery.Scope{Authority: "goal-reader", ThreadIDs: []identity.ThreadID{thread}}
		sub, err := public.(ThreadWindowSource).SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		if err = sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: thread, ActivityFields: true}); err != nil {
			t.Fatal(err)
		}
		update := nextUpdate(t, sub)
		for update.Window == nil {
			update = nextUpdate(t, sub)
		}
		var item ThreadItem
		for _, candidate := range update.Window.Value.Items {
			if candidate.Activity != nil && candidate.Activity.ToolName == "goal" {
				item = candidate
			}
		}
		sub.Close()
		if item.ID == "" || item.ActivitySummary == nil || item.Activity.Presentation != nil {
			t.Fatalf("missing Goal summary on restart %d", restart)
		}
		var state map[string]any
		if err = json.Unmarshal(item.ActivitySummary.Values["/payload/goal"], &state); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(state, map[string]any{"operation": "review", "applied": false, "version": float64(1), "outcome": "completed"}) {
			t.Fatalf("incorrect disposition: %v", state)
		}
		encoded, err := json.Marshal(item)
		if err != nil || len(encoded) > 8192 || strings.Contains(string(encoded), "Retain the full") || strings.Contains(string(encoded), "Verified canonical") {
			t.Fatalf("hidden Goal text transported: bytes=%d err=%v", len(encoded), err)
		}
		value, err := public.(ThreadActivityValueReader).ReadActivityValue(t.Context(), scope, ThreadActivityValueInput{ThreadID: thread, ItemID: item.ID, Path: "/payload/goal", MaximumBytes: 1024, MaximumNodes: 16, Fields: []string{"operation", "applied", "version", "outcome"}, TextFields: []string{"text", "evidence"}})
		if err != nil || len(value.TextFields) != 2 {
			t.Fatalf("missing frozen Goal fields: %+v %v", value, err)
		}
		for _, field := range value.TextFields {
			var text strings.Builder
			input := ThreadActivityFieldInput{ThreadID: thread, ItemID: item.ID, Path: field.Path, SourceID: field.SourceID, RunID: field.RunID, EntryID: field.EntryID, SourceSHA256: field.SourceSHA256, SHA256: field.SHA256, OffsetBytes: field.SourceOffsetBytes}
			if field.SourcePath != "" {
				input.Path = field.SourcePath
			}
			for {
				part, err := public.(ThreadActivityFieldReader).ReadActivityField(t.Context(), scope, input)
				if err != nil {
					t.Fatal(err)
				}
				text.WriteString(part.Text)
				if part.OffsetBytes+len(part.Text) >= part.Bytes {
					break
				}
				if len(part.Text) == 0 {
					t.Fatal("field did not advance")
				}
				input.OffsetBytes = part.OffsetBytes + len(part.Text)
			}
			want := goalText
			if strings.HasSuffix(field.Path, "/evidence") {
				want = evidence
			}
			if text.String() != want {
				t.Fatalf("Goal body changed: path=%s got=%d want=%d", field.Path, text.Len(), len(want))
			}
		}
		if err = host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if len(gateway.Requests()) != 2 {
		t.Fatalf("restart executed provider: %d", len(gateway.Requests()))
	}
}
