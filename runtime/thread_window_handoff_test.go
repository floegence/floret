package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestWindowHandoffKeepsMixedMutationMembership(t *testing.T) {
	releaseProvider := make(chan struct{})
	defer close(releaseProvider)
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "script", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: strings.Repeat("answer ", 1000)}, {Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "call", Name: "read", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{BlockUntil: releaseProvider, Events: []provider.Event{{Type: provider.EventDelta, Text: "final"}, {Type: provider.EventDone, Reason: "stop"}}})
	tool := tools.Define[map[string]any](tools.Definition{Name: "read", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, Activity: func(tools.Invocation[any]) (*tools.ActivityPresentation, error) {
		return &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "read"}}, nil
	}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		return tools.Result{Text: "result", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: "result"}}}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, public := testThreadServiceWithAgent(t, agent)
	service := public.(*threadRuntimeService)
	root, err := public.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{root.ThreadID}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	input := ThreadWindowInput{ThreadID: root.ThreadID, Limit: 50, PreviewBytes: 1024, ActivityFields: true}
	if err := sub.SetWindow(t.Context(), scope, input); err != nil {
		t.Fatal(err)
	}
	base := nextUpdate(t, sub)
	version := base.ViewVersion
	members := map[string]bool{}
	for _, item := range base.Window.Value.Items {
		members[item.ID] = true
	}
	if _, err := public.(ThreadCommandService).SendAck(t.Context(), SendInput{ThreadID: root.ThreadID, RequestKey: "send", Input: UserInput{Text: "read"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := sub.SetWindow(ctx, scope, input); err != nil {
		t.Fatal(err)
	}
	for {
		update, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if update.Window != nil {
			return
		}
		if update.ViewVersion <= version {
			continue
		}
		if update.BaseVersion != version {
			t.Fatalf("gap %d/%d/%d", version, update.BaseVersion, update.ViewVersion)
		}
		for _, change := range update.Items {
			exists := members[change.ItemID]
			if change.Kind == ThreadItemAdded && exists || change.Kind != ThreadItemAdded && !exists {
				t.Fatalf("invalid membership base=%d next=%d kind=%s exists=%t item=%s", update.BaseVersion, update.ViewVersion, change.Kind, exists, change.ItemID)
			}
			if change.Kind == ThreadItemRemoved {
				delete(members, change.ItemID)
			} else {
				members[change.ItemID] = true
			}
		}
		version = update.ViewVersion
	}
}

func TestWindowHandoffRetiresOnlyOwnedDetailAndPreservesBaselines(t *testing.T) {
	old, middle, latest, other := newWindowInterestLifetime(), newWindowInterestLifetime(), newWindowInterestLifetime(), newWindowInterestLifetime()
	defer latest.cancel()
	defer other.cancel()
	sub := &ThreadUpdateSubscription{updates: make(chan queuedThreadUpdate, threadUpdateQueueCount), done: make(chan struct{}), windows: map[string]threadWindowInterest{
		"a": {lifetime: old, activityFields: true}, "b": {lifetime: other},
	}}
	defer sub.Close()
	semantic := ThreadUpdate{ThreadID: "a", BaseVersion: 1, ViewVersion: 2,
		Facts:        ThreadRuntimeFacts{Activity: ThreadActivityActive, QueueCount: 1},
		Interactions: &ThreadComponent[[]ThreadInteraction]{Value: []ThreadInteraction{{ID: "pending", TurnID: "turn", RunID: "run", Kind: ThreadInteractionInput}, {ID: "resolved", TurnID: "turn", RunID: "run", Kind: ThreadInteractionApproval, Resolved: true}}},
		Queue:        &ThreadComponent[[]QueuedInput]{Value: []QueuedInput{}},
		Context:      &ThreadComponent[*ThreadContextSnapshot]{}, Goal: &ThreadComponent[*ThreadGoal]{},
		ParentEvents: &ThreadComponent[*ParentEventPage]{}, RestoredInputs: &ThreadComponent[[]RestoredInput]{},
		ToolCallPreviews: &ThreadComponent[[]ThreadToolCallPreview]{},
	}
	mixed := semantic
	mixed.Items = []ThreadItemChange{
		{Kind: ThreadItemReplaced, ItemID: "assistant", Item: &ThreadItem{ID: "assistant", textOmitted: true, textBytes: 4096}},
		{Kind: ThreadItemAdded, ItemID: "tool", Item: &ThreadItem{ID: "tool"}},
	}
	if !sub.enqueueWindowLocked(nil, mixed) {
		_, err := json.Marshal(mixed)
		t.Fatalf("mixed batch not queued: %v, %v", sub.closeErr, err)
	}
	resident := ThreadUpdate{ThreadID: "a", BaseVersion: 2, ViewVersion: 3,
		Items:     []ThreadItemChange{{Kind: ThreadItemReplaced, ItemID: "tool", Item: &ThreadItem{ID: "tool"}}},
		ItemOrder: &ThreadComponent[[]string]{Value: []string{"assistant", "tool"}}, Turns: &ThreadComponent[[]ThreadTurn]{},
	}
	if !sub.enqueueWindowLocked(nil, resident) || !sub.enqueueWindowLocked(nil, ThreadUpdate{ThreadID: "b", BaseVersion: 1, ViewVersion: 2, Items: []ThreadItemChange{{Kind: ThreadItemAdded, ItemID: "other", Item: &ThreadItem{ID: "other"}}}}) {
		t.Fatal("resident batches not queued")
	}
	enqueueBaseline := func(request string, v uint64) {
		t.Helper()
		u := ThreadUpdate{ThreadID: "a", BaseVersion: v, ViewVersion: v, Window: &ThreadComponent[*ThreadWindowSnapshot]{Value: &ThreadWindowSnapshot{ThreadID: "a", ViewVersion: v, RequestID: request, Items: []ThreadItem{{ID: "tool"}}}}}
		if !sub.enqueueLocked(u, 256) {
			t.Fatal("baseline not queued")
		}
	}
	enqueueBaseline("B", 3)
	before := sub.queuedBytes
	sub.retireWindowLocked(old)
	if sub.queuedBytes >= before || old.ctx.Err() == nil {
		t.Fatal("retired detail retained its read budget or lifetime")
	}
	sub.windows["a"] = threadWindowInterest{lifetime: middle}
	if !sub.enqueueWindowLocked(nil, ThreadUpdate{ThreadID: "a", BaseVersion: 3, ViewVersion: 4, Items: resident.Items}) {
		t.Fatal("middle batch not queued")
	}
	enqueueBaseline("C", 4)
	sub.retireWindowLocked(middle)
	sub.windows["a"] = threadWindowInterest{lifetime: latest}
	if !sub.enqueueWindowLocked(nil, ThreadUpdate{ThreadID: "a", BaseVersion: 4, ViewVersion: 5, Items: resident.Items}) {
		t.Fatal("latest batch not queued")
	}
	first := nextUpdate(t, sub)
	if !reflect.DeepEqual(first, cloneThreadUpdate(semantic)) {
		t.Fatalf("retirement changed semantic components: %+v", first)
	}
	second := nextUpdate(t, sub)
	if second.Items != nil || second.Turns != nil || second.ItemOrder != nil || second.BaseVersion != 2 || second.ViewVersion != 3 {
		t.Fatalf("resident dependent detail escaped retirement: %+v", second)
	}
	if u := nextUpdate(t, sub); u.ThreadID != "b" || len(u.Items) != 1 {
		t.Fatalf("other window was retired: %+v", u)
	}
	if u := nextUpdate(t, sub); u.Window == nil || u.Window.Value.RequestID != "B" {
		t.Fatalf("first accepted explicit baseline lost: %+v", u)
	}
	if u := nextUpdate(t, sub); u.Items != nil || u.BaseVersion != 3 || u.ViewVersion != 4 {
		t.Fatalf("middle interest detail escaped retirement: %+v", u)
	}
	if u := nextUpdate(t, sub); u.Window == nil || u.Window.Value.RequestID != "C" {
		t.Fatalf("latest explicit baseline lost: %+v", u)
	}
	if u := nextUpdate(t, sub); len(u.Items) != 1 || u.BaseVersion != 4 || u.ViewVersion != 5 {
		t.Fatalf("new-interest delta lost: %+v", u)
	}
	if sub.queuedBytes != 0 || sub.inflight != nil || len(sub.updates) != 0 {
		t.Fatal("handoff retained its budget")
	}
}
