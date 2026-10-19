package runtime

import (
	"context"
	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
	"testing"
	"time"
)

func TestSchemaCorrectionsPreserveReasoningWithoutTrailingDrafts(t *testing.T) {
	release := make(chan struct{}, 2)
	started := make(chan string, 2)
	t.Cleanup(func() { release <- struct{}{}; release <- struct{}{} })
	step := func(id, reasoning, args string) florettest.Step {
		return florettest.Step{Events: []provider.Event{
			{Type: provider.EventReasoning, Text: reasoning},
			{Type: provider.EventToolCallStart, ToolCallStream: &provider.ToolCallStream{ID: id, Name: "diagnostic_read"}},
			{Type: provider.EventToolCallEnd, ToolCallStream: &provider.ToolCallStream{ID: id, Name: "diagnostic_read"}},
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: id, Name: "diagnostic_read", Args: args}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}}
	}
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "diagnostic:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningSupported, ReasoningCapability: config.ReasoningCapability{Kind: config.ReasoningKindNone}},
		step("invalid-1", "correction-1", `{}`),
		step("invalid-2", "correction-2", `{}`),
		step("valid-1", "valid-reasoning-1", `{"description":"first"}`),
		step("valid-2", "valid-reasoning-2", `{"description":"second"}`),
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	read := tools.Define[map[string]string](tools.Definition{
		Name: "diagnostic_read", InputSchema: tools.StrictObject(map[string]any{"description": tools.String("description")}, []string{"description"}),
		ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
	}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]string]) (tools.Result, error) {
		started <- inv.Args["description"]
		select {
		case <-release:
			return tools.Result{Text: "ok"}, nil
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		}
	})
	agent, err := testAgent(gateway, WithAgentTools(read), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "diagnostic-create"})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	observed := make(chan []ThreadView, 1)
	observeCtx, stopObserve := context.WithCancel(t.Context())
	defer stopObserve()
	go func() {
		var views []ThreadView
		for {
			view, err := subscription.Next(observeCtx)
			if err != nil {
				observed <- views
				return
			}
			views = append(views, view)
		}
	}()
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "diagnostic"}, RequestKey: "diagnostic-send"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for iteration := 1; iteration <= 2; iteration++ {
		select {
		case label := <-started:
			t.Logf("executing tool %s", label)
		case <-ctx.Done():
			v, _ := service.View(t.Context(), created.ThreadID)
			t.Fatalf("timeout requests=%d view=%+v", len(gateway.Requests()), v)
		}
		view, err := service.View(ctx, created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		svc := service.(*threadRuntimeService)
		canonical, err := svc.loadCanonicalThreadView(ctx, created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		actor := svc.runtime(created.ThreadID)
		if err := actor.apply(ctx, func() error { return actor.installCanonicalView(canonical) }); err != nil {
			t.Fatal(err)
		}
		view, err = service.View(ctx, created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		var tail int
		lastTool := -1
		for i, item := range view.Items {
			if item.Kind == ThreadItemTool {
				lastTool = i
			}
		}
		for i, item := range view.Items {
			t.Logf("iteration=%d row=%d kind=%s id=%s canonical=%d text=%q", iteration, i, item.Kind, item.ID, item.CanonicalOrdinal, item.Text)
			if i > lastTool && item.Kind == ThreadItemThinking && item.CanonicalOrdinal == 0 {
				tail++
			}
		}
		if tail != 0 {
			t.Fatalf("stale trailing thinking rows: got %d, want 0", tail)
		}
		release <- struct{}{}
	}
	final := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	if len(final.Items) != 8 {
		t.Fatalf("terminal rows=%d want 8", len(final.Items))
	}
	stopObserve()
	views := <-observed
	sources := map[string]string{}
	ranks := map[string]int{"correction-1": 1, "correction-2": 2, "valid-reasoning-1": 3, "tool:valid-1": 4, "valid-reasoning-2": 5, "tool:valid-2": 6, "done": 7}
	views = append(views, final)
	for _, view := range views {
		previous := -1
		seen := map[string]bool{}
		for index, item := range view.Items {
			if item.Ordinal != uint64(index+1) || seen[item.ID] {
				t.Fatalf("invalid published identity/order: %+v", view.Items)
			}
			seen[item.ID] = true
			key := item.Text
			if item.Activity != nil {
				key = "tool:" + item.Activity.ToolID
			}
			rank, known := ranks[key]
			if !known {
				continue
			}
			if rank <= previous {
				t.Fatalf("published output reordered: %+v", view.Items)
			}
			previous = rank
			if source := sources[key]; source != "" && source != item.ID {
				t.Fatalf("source drift for %s: %s -> %s", key, source, item.ID)
			}
			sources[key] = item.ID
		}
	}
	if len(sources) != 7 {
		t.Fatalf("missing source coverage: %v", sources)
	}
	t.Logf("validated %d intermediate publications and %d terminal rows", len(views), len(final.Items))
}

func TestLiveNativeSourcesFollowEngineOrderAroundHostedActivity(t *testing.T) {
	actor := &threadRuntimeState{}
	actor.state.attemptDraftIDs = map[string]bool{}
	actor.appendLiveTextSegment(ThreadItemThinking, "before", time.Now(), "reason")
	actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: "hosted-row", Kind: ThreadItemTool, Activity: &observation.ActivityItem{Kind: observation.ActivityKindHosted, ToolID: "search"}})
	actor.state.attemptDraftIDs["hosted-row"] = true
	actor.appendLiveTextSegment(ThreadItemAssistant, "answer", time.Now(), "answer")
	actor.appendLiveTextSegment(ThreadItemThinking, "late", time.Now(), "middle")
	actor.orderAttemptOutput([]string{"reason", "middle", "hosted:search", "answer"})
	for i, want := range []string{"reason", "middle", "hosted-row", "answer"} {
		if item := actor.state.view.Items[i]; item.ID != want || item.Ordinal != uint64(i+1) {
			t.Fatalf("native order=%+v", actor.state.view.Items)
		}
	}
	actor.appendLiveTextSegment(ThreadItemThinking, ";suffix", time.Now(), "reason")
	if actor.state.view.Items[0].Text != "before;suffix" || len(actor.state.view.Items) != 4 {
		t.Fatal("suffix created a trailing draft")
	}
}

func TestStoppedProviderRetainsPartialOutputSourcesAndAllowsNextTurn(t *testing.T) {
	gateway := &goalStreamingGateway{events: make(chan provider.Event, 8)}
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "partial-create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "partial-send"}); err != nil {
		t.Fatal(err)
	}
	gateway.events <- provider.Event{Type: provider.EventReasoning, Text: "partial reasoning"}
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: "partial answer"}
	live := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return len(v.Items) == 3 })
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "partial-stop", Mode: CancelModeGraceful}); err != nil {
		t.Fatal(err)
	}
	stopped := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if stopped.LastOutcome == nil || *stopped.LastOutcome != TurnOutcomeCancelled || stopped.Failure != nil {
		t.Fatalf("stop outcome=%+v", stopped)
	}
	canonical, err := service.(*threadRuntimeService).loadCanonicalThreadView(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, view := range []ThreadView{stopped, canonical} {
		if len(view.Items) != len(live.Items) {
			t.Fatalf("partial output lost: live=%+v restored=%+v", live.Items, view.Items)
		}
		for i := 1; i < len(live.Items); i++ {
			if view.Items[i].ID != live.Items[i].ID || view.Items[i].Text != live.Items[i].Text || view.Items[i].CanonicalOrdinal == 0 {
				t.Fatalf("partial source changed: live=%+v restored=%+v", live.Items, view.Items)
			}
		}
	}
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: "recovered"}
	gateway.events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "continue"}, RequestKey: "partial-recover"}); err != nil {
		t.Fatal(err)
	}
	recovered := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.TurnID != stopped.TurnID })
	if recovered.LastOutcome == nil || *recovered.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("recovery=%+v", recovered)
	}
}

func TestSchemaCorrectionExhaustionKeepsSourcesAndReleasesRun(t *testing.T) {
	var steps []florettest.Step
	for _, id := range []string{"bad-1", "bad-2", "bad-3"} {
		steps = append(steps, florettest.Step{Events: []provider.Event{{Type: provider.EventReasoning, Text: "same reasoning"}, {Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: id, Name: "read", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	}
	steps = append(steps, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "recovered"}, {Type: provider.EventDone, Reason: "stop"}}})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "correction", StateCompatibilityKey: "correction:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
	read := tools.Define[map[string]string](tools.Definition{Name: "read", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{"path": tools.String("path")}, []string{"path"}), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]string]) (tools.Result, error) {
		t.Error("invalid call executed")
		return tools.Result{}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(read))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "exhaust-create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "read"}, RequestKey: "exhaust-send"}); err != nil {
		t.Fatal(err)
	}
	failed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if failed.Failure == nil || len(gateway.Requests()) != 3 {
		t.Fatalf("exhaustion=%+v requests=%d", failed, len(gateway.Requests()))
	}
	seen := map[string]bool{}
	reasoning := 0
	for _, item := range failed.Items {
		if item.Kind == ThreadItemTool {
			t.Fatal("invalid call exposed")
		}
		if item.Kind == ThreadItemThinking {
			if seen[item.ID] {
				t.Fatal("identical text conflated sources")
			}
			seen[item.ID] = true
			reasoning++
		}
	}
	if reasoning != 3 {
		t.Fatalf("independent reasoning count=%d", reasoning)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "continue"}, RequestKey: "exhaust-recover"}); err != nil {
		t.Fatal(err)
	}
	recovered := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.TurnID != failed.TurnID })
	if recovered.Failure != nil || recovered.LastOutcome == nil || *recovered.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("recovery=%+v", recovered)
	}
}
