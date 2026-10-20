package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

// applyObservedUpdate models a host applying the public contract, independently
// of the actor's mutation tracker and publication implementation.
func applyObservedUpdate(t *testing.T, view *ThreadView, update ThreadUpdate) {
	t.Helper()
	if update.ThreadID != view.ThreadID || update.BaseVersion != view.ViewVersion {
		t.Fatalf("invalid continuity: baseline=%s/%d update=%s/%d->%d", view.ThreadID, view.ViewVersion, update.ThreadID, update.BaseVersion, update.ViewVersion)
	}
	view.ViewVersion, view.TurnID, view.RunID = update.ViewVersion, update.TurnID, update.RunID
	view.Activity, view.Attention, view.TurnKind = update.Facts.Activity, update.Facts.Attention, update.Facts.TurnKind
	view.LastOutcome, view.Failure, view.Cancellation, view.RunProgress = update.Facts.LastOutcome, update.Facts.Failure, update.Facts.Cancellation, update.Facts.RunProgress
	for _, change := range update.Items {
		index := threadItemIndexByID(view.Items, change.ItemID)
		switch change.Kind {
		case ThreadItemAdded:
			if index >= 0 || change.Item == nil {
				t.Fatalf("invalid insertion: %+v", change)
			}
			view.Items = append(view.Items, *change.Item)
		case ThreadItemReplaced:
			if index < 0 || change.Item == nil {
				t.Fatalf("invalid replacement: %+v", change)
			}
			view.Items[index] = *change.Item
		case ThreadItemRemoved:
			if index < 0 {
				t.Fatalf("invalid removal: %+v", change)
			}
			view.Items = append(view.Items[:index], view.Items[index+1:]...)
		case ThreadItemAppended:
			if index < 0 || change.Append == nil || len(view.Items[index].Text) != change.Append.OffsetBytes {
				t.Fatalf("invalid append: %+v", change)
			}
			view.Items[index].Text += change.Append.Text
		default:
			t.Fatalf("unknown mutation: %+v", change)
		}
	}
	if update.ItemOrder != nil {
		byID := make(map[string]ThreadItem, len(view.Items))
		for _, item := range view.Items {
			byID[item.ID] = item
		}
		if len(byID) != len(update.ItemOrder.Value) {
			t.Fatal("incomplete explicit order")
		}
		view.Items = view.Items[:0]
		for i, id := range update.ItemOrder.Value {
			item, ok := byID[id]
			if !ok {
				t.Fatalf("unknown ordered item %q", id)
			}
			item.Ordinal = uint64(i + 1)
			view.Items = append(view.Items, item)
			delete(byID, id)
		}
	} else {
		sort.SliceStable(view.Items, func(i, j int) bool { return view.Items[i].Ordinal < view.Items[j].Ordinal })
	}
	if update.Context != nil {
		view.Context = update.Context.Value
	}
	if update.Goal != nil {
		view.Goal = update.Goal.Value
	}
	if update.ParentEvents != nil {
		view.ParentEvents = update.ParentEvents.Value
	}
	if update.Turns != nil {
		view.Turns = update.Turns.Value
	}
	if update.Queue != nil {
		view.Queue = update.Queue.Value
	}
	if update.Interactions != nil {
		view.Interactions = update.Interactions.Value
	}
	if update.ToolCallPreviews != nil {
		view.ToolCallPreviews = update.ToolCallPreviews.Value
	}
	if update.RestoredInputs != nil {
		view.RestoredInputs = update.RestoredInputs.Value
	}
	if len(view.Queue) != update.Facts.QueueCount {
		t.Fatal("queue count differs")
	}
}

func assertObservedSnapshot(t *testing.T, subscription *ThreadUpdateSubscription, observed *ThreadView, authoritative ThreadView) {
	t.Helper()
	for observed.ViewVersion < authoritative.ViewVersion {
		update := nextUpdate(t, subscription)
		if update.ViewVersion <= observed.ViewVersion {
			continue
		}
		applyObservedUpdate(t, observed, update)
	}
	want, _ := json.Marshal(authoritative)
	got, _ := json.Marshal(observed)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incremental reconstruction differs\nwant: %s\ngot:  %s", want, got)
	}
}

func TestThreadUpdatesReconstructPublicExecutionAndQueue(t *testing.T) {
	releaseTool := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseTool:
		default:
			close(releaseTool)
		}
	})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "script", StateCompatibilityKey: "script:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "First 世界🌸"}, {Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "read", Name: "read", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Final **answer**"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Queued answer"}, {Type: provider.EventDone, Reason: "stop"}}})
	tool := tools.Define[map[string]any](tools.Definition{Name: "read", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
		select {
		case <-releaseTool:
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		}
		return tools.Result{Text: "read result"}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, public := testThreadServiceWithAgent(t, agent)
	service := public.(*threadRuntimeService)
	sub, err := service.SubscribeUpdates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-updates"})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send-updates", Input: UserInput{Text: "Read"}}); err != nil {
		t.Fatal(err)
	}
	blocked := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		for _, item := range view.Items {
			if item.Activity != nil && item.Activity.ToolID == "read" {
				return true
			}
		}
		return false
	})
	assertObservedSnapshot(t, sub, &observed, blocked)
	queued, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "queue-updates", Input: UserInput{Text: "Next"}})
	if err != nil {
		t.Fatal(err)
	}
	assertObservedSnapshot(t, sub, &observed, queued)
	close(releaseTool)
	final := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && len(view.Queue) == 0 && len(view.Turns) == 2
	})
	assertObservedSnapshot(t, sub, &observed, final)
}
