package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestStructuredActivityInputsSurviveLiveHistoryAndRestart(t *testing.T) {
	source := "  // inspect\n" + strings.Repeat("log('page');\n", 1200)
	release := make(chan struct{})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "script", StateCompatibilityKey: "script:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "inspect-1", Name: "inspect", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	tool := tools.Define[map[string]any](tools.Definition{Name: "inspect", InputSchema: tools.StrictObject(nil, nil), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
		Activity: func(tools.Invocation[any]) (*tools.ActivityPresentation, error) {
			return &tools.ActivityPresentation{Label: "Inspect the page", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Content: source, Format: tools.StructuredActivityRowFormatCode, Language: "javascript"}}}}, nil
		},
	}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		}
		return tools.Result{Text: "Page title", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{RowsProvided: true, Rows: []tools.StructuredActivityRow{{Content: "Page title", Truncated: true}}}}}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/activity.db"
	open := func() (*Host, ThreadService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "inspect"}}); err != nil {
		t.Fatal(err)
	}
	find := func(items []ThreadItem) *tools.StructuredActivityPayload {
		for _, item := range items {
			if item.Activity != nil && item.Activity.ToolID == "inspect-1" && item.Activity.Presentation != nil {
				if item.Activity.Presentation.Label != "Inspect the page" {
					t.Fatal("intent changed")
				}
				payload, ok := item.Activity.Presentation.Payload.(tools.StructuredActivityPayload)
				if ok {
					return &payload
				}
			}
		}
		return nil
	}
	live := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return find(v.Items) != nil })
	if p := find(live.Items); len(p.Inputs) != 1 || p.Inputs[0].Content != source || p.RowsProvided {
		t.Fatal("live inputs lost or premature result")
	}
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, live.Items)
	close(release)
	completed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	assert := func(items []ThreadItem) {
		t.Helper()
		p := find(items)
		if p == nil || len(p.Inputs) != 1 || p.Inputs[0].Content != source || p.Inputs[0].Language != "javascript" || !p.RowsProvided || len(p.Rows) != 1 || p.Rows[0].Content != "Page title" || !p.Rows[0].Truncated {
			t.Fatalf("activity lost: %#v", p)
		}
	}
	assert(completed.Items)
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, completed.Items)
	entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	journal, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	assert(journal)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assert(restored.Items)
	history, err := service.History(t.Context(), created.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	assert(history.Items)
}
