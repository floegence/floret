package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestToolHistoryAfterHostedReasoningSurvivesForkAndRestart(t *testing.T) {
	for _, mode := range []string{"step_reasoning", "no_reasoning"} {
		t.Run(mode, func(t *testing.T) {
			var executions atomic.Int32
			firstReasoning, secondReasoning := "before search;", "after search;"
			if mode == "no_reasoning" {
				firstReasoning, secondReasoning = "", ""
			}
			wantReasoning := firstReasoning + secondReasoning
			callStep := florettest.Step{Events: []provider.Event{
				{Type: provider.EventReasoning, Text: firstReasoning},
				{Type: provider.EventHostedToolCall, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}},
				{Type: provider.EventHostedToolResult, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}, HostedResult: &provider.HostedToolResult{}},
				{Type: provider.EventReasoning, Text: secondReasoning},
				{Type: provider.EventDelta, Text: "Checking two pages."},
				{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{
					{ID: "fetch-1", Name: "fetch", Args: `{}`},
					{ID: "fetch-2", Name: "fetch", Args: `{}`},
				}},
				{Type: provider.EventDone, Reason: "tool_calls"},
			}}
			answer := florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Complete."}, {Type: provider.EventDone, Reason: "stop"}}}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "tools", StateCompatibilityKey: "tools:v1"}, provider.Capabilities{Reasoning: provider.ReasoningSupported, ReasoningCapability: config.ReasoningCapability{Kind: config.ReasoningKindNone}}, callStep, answer, answer, answer, answer, callStep, answer)
			fetch := tools.Define[map[string]any](tools.Definition{Name: "fetch", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(_ context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
				executions.Add(1)
				return tools.Result{Text: "page"}, nil
			})
			agent, err := testAgent(gateway, WithAgentTools(fetch), WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"}), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			database := filepath.Join(t.TempDir(), "history.sqlite")
			open := func() (*Host, ThreadService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(database)})
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
			send := func(threadID identity.ThreadID, key string) {
				receipt, err := service.Send(t.Context(), SendInput{ThreadID: threadID, RequestKey: RequestKey(key), Input: UserInput{Text: "continue"}})
				if err != nil {
					t.Fatal(err)
				}
				view := waitThreadView(t, service, threadID, func(v ThreadView) bool {
					return v.TurnID == receipt.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
				})
				if *view.LastOutcome != TurnOutcomeCompleted {
					t.Fatalf("thread=%s key=%s turn failed: %+v", threadID, key, view.Failure)
				}
			}
			check := func(threadID identity.ThreadID, pairs int) {
				entries, err := host.store.repo.Path(t.Context(), threadID.String(), "")
				if err != nil {
					t.Fatal(err)
				}
				messages, err := sessiontree.BuildContextChecked(entries, sessiontree.ContextOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := session.ValidateToolHistory(messages); err != nil {
					t.Fatal(err)
				}
				calls, results := 0, 0
				for _, m := range messages {
					if m.Role == session.Assistant && m.ToolCallID != "" {
						calls++
						if m.Reasoning != wantReasoning {
							t.Fatalf("reasoning=%q want %q", m.Reasoning, wantReasoning)
						}
					}
					if m.Role == session.Tool {
						results++
					}
				}
				if calls != pairs || results != pairs {
					t.Fatalf("calls=%d results=%d want=%d", calls, results, pairs)
				}
			}
			send(created.ThreadID, "first")
			check(created.ThreadID, 2)
			ids := []identity.ThreadID{created.ThreadID}
			for i := 0; i < 2; i++ {
				fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: ids[len(ids)-1], RequestKey: RequestKey(fmt.Sprintf("fork-%d", i))})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, fork.ThreadID)
				check(fork.ThreadID, 2)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
			for _, id := range ids {
				check(id, 2)
				send(id, "followup")
				check(id, 2)
			}
			if executions.Load() != 2 {
				t.Fatalf("historical tools replayed: %d", executions.Load())
			}
			// Reusing call IDs in a later turn must remain a new invocation.
			send(created.ThreadID, "reuse-call-ids")
			check(created.ThreadID, 4)
			if executions.Load() != 4 {
				t.Fatalf("new tools suppressed: %d", executions.Load())
			}
		})
	}
}
