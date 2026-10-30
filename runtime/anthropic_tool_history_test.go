package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAnthropicSignedToolLoopUsesCommittedCanonicalOutput(t *testing.T) {
	var requests, executions atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		n := requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		content := []any{map[string]any{"type": "text", "text": "complete"}}
		reason := "end_turn"
		if n == 1 {
			content = []any{map[string]any{"type": "thinking", "thinking": "inspect first", "signature": "signed-tool-loop"}, map[string]any{"type": "redacted_thinking", "data": "opaque"}, map[string]any{"type": "text", "text": "Checking."}, map[string]any{"type": "tool_use", "id": "call", "name": "inspect", "input": map[string]any{}}}
			reason = "tool_use"
		} else {
			if !bytes.Contains(raw, []byte("signed-tool-loop")) || !bytes.Contains(raw, []byte("tool result fact")) {
				t.Errorf("next request lost signed output or paired result: %s", raw)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "message", "content": content, "stop_reason": reason, "usage": map[string]any{"input_tokens": 10, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0, "output_tokens": 5}})
	}))
	defer server.Close()
	gateway, err := provider.NewAnthropic(provider.AnthropicOptions{Provider: "anthropic", Model: "fixture", BaseURL: server.URL, APIKey: "key", StateCompatibilityKey: "account", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
	if err != nil {
		t.Fatal(err)
	}
	inspect := tools.Define[map[string]any](tools.Definition{Name: "inspect", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		executions.Add(1)
		return tools.Result{Text: "tool result fact"}, nil
	})
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Inspect.", Context: config.ContextPolicy{ContextWindowTokens: 128000, MaxOutputTokens: 4096}}, gateway, WithAgentTools(inspect), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, r EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: r.EffectAttemptID, RequestFingerprint: r.RequestFingerprint, ThreadID: r.ThreadID, TurnID: r.TurnID, RunID: r.RunID, ToolCallID: r.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	view := sendNativeTurn(t, service, created.ThreadID, "inspect")
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	if requests.Load() != 2 || executions.Load() != 1 {
		t.Fatalf("requests=%d executions=%d", requests.Load(), executions.Load())
	}
}
