package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestLookupToolResultUsesCanonicalExecutionAcrossRestartAndFork(t *testing.T) {
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)
	const args = `{"path":"/host/private.txt"}`
	const content = `{"file_path":"/host/private.txt","content":"confirmed"}`
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "lookup", StateCompatibilityKey: "lookup:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "read", Name: "inspect", Args: args}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{BlockUntil: release, Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	tool := tools.Define[map[string]string](tools.Definition{Name: "inspect", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{"path": tools.String("path")}, []string{"path"})}, nil, nil,
		func(context.Context, tools.Invocation[map[string]string]) (tools.Result, error) {
			return tools.Result{Text: content}, nil
		})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "canonical.sqlite")
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
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "inspect"}})
	if err != nil {
		t.Fatal(err)
	}
	lookup := LookupToolResultInput{ThreadID: created.ThreadID, TurnID: view.TurnID, RunID: view.RunID, ToolCallID: "read"}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 2); err != nil {
		t.Fatal(err)
	}
	check := func(reader ThreadToolResultReader, input LookupToolResultInput) {
		t.Helper()
		result, found, err := reader.LookupToolResult(t.Context(), input)
		if err != nil || !found || result.ToolName != "inspect" || result.Arguments != args || result.Content != content || result.Status != "success" {
			t.Fatalf("canonical result=%+v found=%t err=%v", result, found, err)
		}
		for _, wrong := range []LookupToolResultInput{
			{ThreadID: input.ThreadID, TurnID: "wrong", RunID: input.RunID, ToolCallID: input.ToolCallID},
			{ThreadID: input.ThreadID, TurnID: input.TurnID, RunID: "wrong", ToolCallID: input.ToolCallID},
			{ThreadID: input.ThreadID, TurnID: input.TurnID, RunID: input.RunID, ToolCallID: "missing"},
		} {
			if _, found, err := reader.LookupToolResult(t.Context(), wrong); err != nil || found {
				t.Fatalf("wrong execution accepted: %+v found=%t err=%v", wrong, found, err)
			}
		}
	}
	check(service.(ThreadToolResultReader), lookup)
	finish()
	view = waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	for _, item := range view.Items {
		if item.CreatedAt.IsZero() || item.CreatedAt.UnixMilli() <= 0 {
			t.Fatalf("invalid completed item timestamp: %+v", item)
		}
	}
	check(service.(ThreadToolResultReader), lookup)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	check(service.(ThreadToolResultReader), lookup)
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range fork.Items {
		if item.Kind == ThreadItemTool {
			check(service.(ThreadToolResultReader), LookupToolResultInput{ThreadID: fork.ThreadID, TurnID: item.TurnID, RunID: item.RunID, ToolCallID: item.Activity.ToolID})
		}
	}
	if _, _, err := service.(ThreadToolResultReader).LookupToolResult(t.Context(), LookupToolResultInput{ThreadID: created.ThreadID}); err == nil {
		t.Fatal("missing execution identity accepted")
	}
	if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: created.ThreadID, RequestKey: "delete"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.(ThreadToolResultReader).LookupToolResult(t.Context(), lookup); !errors.Is(err, ErrThreadDeleted) && !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("deleted result accessible: %v", err)
	}
}
