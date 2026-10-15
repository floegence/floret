package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestModelContextRefreshAfterToolAndRestart(t *testing.T) {
	done := florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "read", Name: "read", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, done, done)
	var revision atomic.Int32
	snapshot := func() *ModelContextSnapshot {
		text := "old repository"
		if revision.Load() > 0 {
			text = "new repository"
		}
		return &ModelContextSnapshot{Sections: []ModelContextSection{{Key: "repository", Title: "Repository", Text: text}, {Key: "skills", Title: "Skills", Text: "STABLE_SKILL_CATALOG"}}}
	}
	tool := tools.Define[map[string]any](tools.Definition{Name: "read", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		revision.Add(1)
		return tools.Result{Text: "read complete"}, nil
	})
	registry := tools.NewRegistry(tool)
	agent, err := testAgent(gateway, WithAgentDynamicToolSurface(func(ctx context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
		return ToolSurface{Tools: registry, ModelContextSnapshot: snapshot()}, nil
	}), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	db := t.TempDir() + "/snapshot.sqlite"
	open := func() (*Host, ThreadService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(db)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	in := UserInput{Text: "inspect repository", ModelContextSnapshot: snapshot()}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: in}); err != nil {
		t.Fatal(err)
	}
	in.ModelContextSnapshot.Sections[0].Text = "caller mutation"
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	requests := gateway.Requests()
	if len(requests) != 2 {
		t.Fatalf("requests=%d", len(requests))
	}
	var last string
	for _, message := range requests[1].Messages {
		if strings.Contains(message.Text, "Model context update") {
			last = message.Text
		}
	}
	if !strings.Contains(last, "new repository") || strings.Contains(last, "STABLE_SKILL_CATALOG") {
		t.Fatalf("update=%s", last)
	}
	entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	updates := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryModelContext {
			updates++
		}
	}
	if updates != 1 || countThreadItems(view.Items, ThreadItemUser) != 1 {
		t.Fatalf("updates=%d view=%+v", updates, view)
	}
	current, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
	if err != nil || current.ModelContext == nil || current.ModelContext.UsedByRequestID == "" {
		t.Fatalf("context=%+v err=%v", current.ModelContext, err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "next", Input: UserInput{Text: "continue"}}); err != nil {
		t.Fatal(err)
	}
	view = waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	entries, _ = host.store.repo.Entries(t.Context(), created.ThreadID.String())
	updates = 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryModelContext {
			updates++
		}
	}
	if updates != 1 {
		t.Fatalf("restart duplicated unchanged snapshot: %d", updates)
	}
}

func TestModelContextCollectionFailureSettlesAndAllowsRetry(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})
	var unavailable atomic.Bool
	unavailable.Store(true)
	agent, err := testAgent(gateway, WithAgentDynamicToolSurface(func(context.Context, ToolSurfaceRequest) (ToolSurface, error) {
		if unavailable.Load() {
			return ToolSurface{}, errors.Join(ErrModelContextUnavailable, errors.New("rules source unreachable"))
		}
		return ToolSurface{ModelContextSnapshot: &ModelContextSnapshot{Sections: []ModelContextSection{{Key: "rules", Title: "Rules", Text: "available"}}}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, _ := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure == nil || view.Failure.Code != ThreadTurnFailureModelContextUnavailable || len(gateway.Requests()) != 0 {
		t.Fatalf("view=%+v", view)
	}
	unavailable.Store(false)
	if _, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: view.TurnID, RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	view = waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || countThreadItems(view.Items, ThreadItemUser) != 1 {
		t.Fatalf("view=%+v", view)
	}
}

func TestModelContextForkInheritsOnlyFactsBeforeForkPoint(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:fork"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDone, Reason: "stop"}}})
	host, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func(text string) *ModelContextSnapshot {
		return &ModelContextSnapshot{Sections: []ModelContextSection{{Key: "repository", Title: "Repository", Text: text}}}
	}
	first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "first", Input: UserInput{Text: "first", ModelContextSnapshot: snapshot("AT_FORK")}})
	if err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	entries, _ := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	forkPoint := ""
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryUserMessage {
			forkPoint = entry.ID
		}
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "second", Input: UserInput{Text: "second", ModelContextSnapshot: snapshot("AFTER_FORK")}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.TurnID != first.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, SourceEntryID: forkPoint, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	current, err := service.(ThreadContextReader).Context(t.Context(), fork.ThreadID)
	if err != nil || current.ModelContext == nil || current.ModelContext.Snapshot.Sections[0].Text != "AT_FORK" {
		t.Fatalf("fork=%+v err=%v", current.ModelContext, err)
	}
}

func TestModelContextQueuedPromotionRefreshesAtExecution(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:queue"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway, WithAgentDynamicToolSurface(func(context.Context, ToolSurfaceRequest) (ToolSurface, error) {
		return ToolSurface{ModelContextSnapshot: &ModelContextSnapshot{Sections: []ModelContextSection{{Key: "date", Title: "Date", Text: "EXECUTION_DATE"}}}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	old := UserInput{Text: "queued", ModelContextSnapshot: &ModelContextSnapshot{Sections: []ModelContextSection{{Key: "date", Title: "Date", Text: "QUEUED_DATE"}}}}
	if _, err := service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []ImportedPendingInput{{RequestKey: "queued", Input: old}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.PromoteQueued(t.Context(), PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queued", RequestKey: "promote"}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || len(view.Queue) != 0 || countThreadItems(view.Items, ThreadItemUser) != 1 {
		t.Fatalf("view=%+v", view)
	}
	var last string
	for _, message := range gateway.Requests()[0].Messages {
		if strings.Contains(message.Text, "Model context update") {
			last = message.Text
		}
	}
	if !strings.Contains(last, "EXECUTION_DATE") {
		t.Fatalf("queued request used stale facts: %s", last)
	}
}
