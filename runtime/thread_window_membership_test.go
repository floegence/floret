package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func publicationBudgetFixture(t *testing.T) ThreadView {
	t.Helper()
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	view := ThreadView{ThreadID: "thread", TurnID: "turn", RunID: "run", Activity: ThreadActivityActive, ViewVersion: 2,
		Items:        []ThreadItem{{ID: "assistant-intermediate", TurnID: "turn", RunID: "run", Kind: ThreadItemAssistant, Ordinal: 1, Text: strings.Repeat("answer ", 1000), CreatedAt: now}},
		Turns:        []ThreadTurn{{TurnID: "turn", RunID: "run", StartedAt: now, IntermediateItemIDs: []string{"assistant-intermediate"}, FinalItemIDs: []string{}}},
		Interactions: []ThreadInteraction{{ID: "pending", TurnID: "turn", RunID: "run", Kind: ThreadInteractionInput}, {ID: "resolved", TurnID: "turn", RunID: "run", Kind: ThreadInteractionInput, Resolved: true}},
	}
	presentation := tools.ActivityPresentation{Label: strings.Repeat("🌸", 180), Description: strings.Repeat("🌸", 450), Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "detail", Format: tools.StructuredActivityRowFormatText}}}}
	for range 8 {
		presentation.Chips = append(presentation.Chips, tools.ActivityChip{Kind: "detail", Label: strings.Repeat("L", 100), Value: strings.Repeat("V", 100)})
	}
	if err := presentation.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(presentation); err != nil {
		t.Fatal(err)
	}
	summary := residentActivitySummary(&presentation)
	for i := 1; i < threadDisplayPageItems; i++ {
		view.Items = append(view.Items, ThreadItem{ID: fmt.Sprintf("tool-%d", i), TurnID: "turn", RunID: "run", Kind: ThreadItemTool, Ordinal: uint64(i + 1), CreatedAt: now, Activity: &observation.ActivityItem{ToolID: fmt.Sprintf("call-%d", i), ToolName: "inspect", Kind: observation.ActivityKindTool, Status: observation.ActivityStatusSuccess}, activityOmitted: true, activitySummary: &summary})
	}
	return view
}

func assertWindowMembership(t *testing.T, update ThreadUpdate, interest threadWindowInterest) map[string]bool {
	t.Helper()
	if update.Window == nil || update.Window.Value == nil || update.Turns == nil {
		t.Fatal("missing window or turns")
	}
	snapshot := update.Window.Value
	selected := map[string]bool{}
	for _, item := range snapshot.Items {
		selected[item.ID] = true
	}
	if !reflect.DeepEqual(interest.items, selected) {
		t.Fatalf("installed and delivered membership differ: installed=%d delivered=%d", len(interest.items), len(selected))
	}
	if !reflect.DeepEqual(interest.bodies, windowBodyLocators(snapshot.Items)) || interest.anchor != snapshot.FirstItemID {
		t.Fatal("locators or anchor differ from delivered snapshot")
	}
	for _, turn := range update.Turns.Value {
		for _, ids := range [][]string{turn.IntermediateItemIDs, turn.FinalItemIDs} {
			for _, id := range ids {
				if !selected[id] {
					t.Fatalf("dangling turn reference %s", id)
				}
			}
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > threadDisplayPageBytes {
		t.Fatalf("invalid window budget: %d %v", len(raw), err)
	}
	return selected
}

// The actual queue must expose the same bounded members it installs, including
// when later deltas are already queued before a slow consumer reads a baseline.
func TestSourcePublicationMembershipClosure(t *testing.T) {
	for _, source := range []bool{false, true} {
		for _, direction := range []ThreadWindowDirection{ThreadWindowBefore, ThreadWindowAfter, ThreadWindowAround} {
			t.Run(fmt.Sprintf("source=%t/%s", source, direction), func(t *testing.T) {
				view := publicationBudgetFixture(t)
				actor := &threadRuntimeState{threadID: "thread", state: threadRuntimeData{view: view}}
				lifetime := newWindowInterestLifetime()
				defer lifetime.cancel()
				input := ThreadWindowInput{ThreadID: "thread", Limit: threadDisplayPageItems, ActivityFields: true, Direction: direction}
				if direction == ThreadWindowAround {
					input.AnchorID = "tool-75"
				}
				sub := &ThreadUpdateSubscription{windowed: true, updates: make(chan queuedThreadUpdate, threadUpdateQueueCount), done: make(chan struct{}), authorized: map[string]bool{"thread": true}, windows: map[string]threadWindowInterest{"thread": {version: 1, lifetime: lifetime, sourceInput: input, activityFields: true, anchor: "assistant-intermediate", limit: threadDisplayPageItems}}}
				service := &threadRuntimeService{published: map[string]uint64{}, updateSubscribers: map[*ThreadUpdateSubscription]struct{}{sub: {}}}
				actor.service, sub.service = service, service
				defer sub.Close()
				if source {
					frozen := service.prepareSourceWindow(t.Context(), sourceWindowRequest{input: input, lifetime: lifetime}, canonicalThreadView{ThreadView: view}, threadSourceProjection{leaf: "leaf", resolvedAnchor: input.AnchorID})
					if frozen.err != nil {
						t.Fatal(frozen.err)
					}
					actor.sourceWindow = &threadSourceProjection{companions: map[*windowInterestLifetime]frozenSourceWindow{lifetime: frozen}}
				}
				service.deliverUpdateLocked(actor, ThreadUpdate{ThreadID: "thread", BaseVersion: 1, ViewVersion: 2, ItemOrder: &ThreadComponent[[]string]{}})
				installed := sub.windows["thread"]
				installed.items = maps.Clone(installed.items)
				installed.bodies = maps.Clone(installed.bodies)
				// Queue every replacement before consuming, so a late membership repair
				// cannot make previously projected deltas valid by rolling interest back.
				actor.state.view.ViewVersion = 3
				replacements := []ThreadItem{view.Items[0], view.Items[75], view.Items[len(view.Items)-1]}
				changes := make([]ThreadItemChange, len(replacements))
				for i, item := range replacements {
					changes[i] = ThreadItemChange{Kind: ThreadItemReplaced, ItemID: item.ID, Item: &item}
				}
				service.deliverUpdateLocked(actor, ThreadUpdate{ThreadID: "thread", BaseVersion: 2, ViewVersion: 3, Items: changes})
				got, err := sub.Next(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				selected := assertWindowMembership(t, got, installed)
				if len(selected) >= len(view.Items) {
					t.Fatal("fixture did not exceed encoded budget")
				}
				if direction == ThreadWindowAround && !selected[input.AnchorID] {
					t.Fatal("around anchor evicted")
				}
				if len(got.Interactions.Value) != 1 || got.Interactions.Value[0].ID != "pending" {
					t.Fatal("off-window pending control lost")
				}
				if got.ItemOrder != nil {
					t.Fatal("replacement leaked pre-budget order")
				}
				delta, err := sub.Next(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				for _, change := range delta.Items {
					if !selected[change.ItemID] {
						t.Fatalf("replace targets unpublished item %s", change.ItemID)
					}
				}
				if sub.windows["thread"].version != 3 {
					t.Fatal("Next rolled back interest version")
				}
				if len(view.Turns[0].IntermediateItemIDs) != 1 {
					t.Fatal("canonical turn changed")
				}
			})
		}
	}
}

func TestSourcePublicationPreparationFailureIsScoped(t *testing.T) {
	view := publicationBudgetFixture(t)
	service := &threadRuntimeService{}
	lifetime := newWindowInterestLifetime()
	defer lifetime.cancel()
	input := ThreadWindowInput{ThreadID: "thread", ActivityFields: true, ItemIDs: []string{}}
	for _, item := range view.Items {
		input.ItemIDs = append(input.ItemIDs, item.ID)
	}
	oversized := service.prepareSourceWindow(t.Context(), sourceWindowRequest{input: input, lifetime: lifetime}, canonicalThreadView{ThreadView: view}, threadSourceProjection{})
	if oversized.err == nil || oversized.snapshot.Items != nil {
		t.Fatal("explicit selection silently evicted members or exposed partial state")
	}
	input.ItemIDs = []string{"assistant-intermediate"}
	view.Items = view.Items[:1]
	healthy := service.prepareSourceWindow(t.Context(), sourceWindowRequest{input: input, lifetime: lifetime}, canonicalThreadView{ThreadView: view}, threadSourceProjection{})
	if healthy.err != nil || len(healthy.snapshot.Items) != 1 {
		t.Fatal("independent bounded selection failed", healthy.err)
	}
	lifetime.cancel()
	canceled := service.prepareSourceWindow(t.Context(), sourceWindowRequest{input: input, lifetime: lifetime}, canonicalThreadView{ThreadView: view}, threadSourceProjection{})
	if !errors.Is(canceled.err, context.Canceled) || canceled.snapshot.Items != nil {
		t.Fatal("retired preparation published detail")
	}
}

func TestSourceRefreshBudgetMembershipFromStorage(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/windows.sqlite"), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "budget-refresh"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry) {
		t.Helper()
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "started", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	appendEntry(sessiontree.Entry{ID: "intermediate", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "Inspect the files."}})
	presentation := &tools.ActivityPresentation{Label: strings.Repeat("🌸", 180), Description: strings.Repeat("🌸", 450), Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "detail", Format: tools.StructuredActivityRowFormatText}}}}
	for range 8 {
		presentation.Chips = append(presentation.Chips, tools.ActivityChip{Kind: "detail", Label: strings.Repeat("L", 100), Value: strings.Repeat("V", 100)})
	}
	for i := range 148 {
		call := fmt.Sprint("call-", i)
		appendEntry(sessiontree.Entry{ID: call, Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: call, Content: "{}", Activity: presentation}})
		appendEntry(sessiontree.Entry{ID: fmt.Sprint("result-", i), Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: call, Content: "result", Activity: presentation}})
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	subscribe := func() *ThreadUpdateSubscription {
		t.Helper()
		sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sub.Close)
		return sub
	}
	input := ThreadWindowInput{ThreadID: id, Limit: 150, ActivityFields: true}
	first := subscribe()
	if err := first.SetWindow(t.Context(), scope, input); err != nil {
		t.Fatal(err)
	}
	nextWindow := func(sub *ThreadUpdateSubscription) ThreadUpdate {
		t.Helper()
		for {
			update := nextUpdate(t, sub)
			if update.Window != nil {
				return update
			}
		}
	}
	baseline := nextWindow(first)
	assertWindowMembership(t, baseline, first.windows[id.String()])
	appendEntry(sessiontree.Entry{ID: "later", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "Another input"}})
	second := subscribe()
	if err := second.SetWindow(t.Context(), scope, input); err != nil {
		t.Fatal(err)
	}
	replacement := nextWindow(first)
	if service.runtime(id).sourceWindow == nil {
		t.Fatal("test bypassed source-backed publication")
	}
	selected := assertWindowMembership(t, replacement, first.windows[id.String()])
	if len(selected) >= 150 || selected["intermediate"] {
		t.Fatal("storage fixture did not evict intermediate")
	}
	assertWindowMembership(t, nextWindow(second), second.windows[id.String()])
}
