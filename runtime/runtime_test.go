package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/mcpclient"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/skills"
	"github.com/floegence/floret/tools"
)

func TestNewEngineFromConfigRunsFakeProvider(t *testing.T) {
	e, err := NewEngine(config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		FakeResponse: "configured",
		RunID:        "run",
		SystemPrompt: "test",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
}

func TestNewEngineUsesConfiguredPromptCacheDir(t *testing.T) {
	dir := t.TempDir()
	e, err := NewEngine(config.Config{
		Provider:             config.ProviderFake,
		Model:                "fake-model",
		FakeResponse:         "configured",
		RunID:                "run",
		PromptCacheDir:       dir,
		PromptCacheRetention: "in_memory",
		SystemPrompt:         "test",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if _, err := os.Stat(filepath.Join(dir, promptCachePathForTest("run"), "raw_segments.jsonl")); err != nil {
		t.Fatalf("prompt cache raw segment file missing: %v", err)
	}
}

func TestNewHarnessRunsDurableThreadWithExplicitPolicies(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	h, err := NewHarness(config.Config{
		Provider:                config.ProviderFake,
		Model:                   "fake-model",
		FakeResponse:            "configured",
		SystemPrompt:            "test",
		MaxEmptyProviderRetries: 7,
		NoProgressLimit:         8,
		DuplicateToolLimit:      9,
	}, HarnessOptions{
		Store: repo,
		LoopLimits: agentharness.LoopLimits{
			WallTime: 250 * time.Millisecond,
		},
		NewID: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "hello", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ID != "thread" || snap.Status != string(engine.Completed) || !snap.CanAppendMessage ||
		len(snap.Messages) != 2 || snap.Messages[0].Content != "hello" || snap.Messages[1].Content != "configured" {
		t.Fatalf("durable thread snapshot = %#v", snap)
	}
}

func TestNewHarnessWithProviderMapsExplicitPoliciesToTurn(t *testing.T) {
	ctx := context.Background()
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("configured"), harness.Done()))
	h := NewHarnessWithProvider(config.Config{
		Provider:                config.ProviderFake,
		Model:                   "fake-model",
		SystemPrompt:            "test",
		MaxEmptyProviderRetries: 7,
		NoProgressLimit:         8,
		DuplicateToolLimit:      9,
	}, scripted, HarnessOptions{
		Store: sessiontree.NewMemoryRepo(),
		TurnPolicy: agentharness.TurnPolicy{
			ContextPolicy: contextpolicy.Policy{
				ContextWindowTokens: 4096,
				MaxOutputTokens:     123,
				RecentTailTokens:    512,
				RecentUserTokens:    321,
			},
			CacheRetention:        promptcache.RetentionLong,
			HostedToolDefinitions: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}},
		},
		LoopLimits: agentharness.LoopLimits{
			MaxEmptyProviderRetries: 3,
			NoProgressLimit:         4,
			DuplicateToolLimit:      5,
			WallTime:                250 * time.Millisecond,
		},
		NewID: deterministicIDs(),
	})
	thread, err := h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "hello", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	req := scripted.Requests[0]
	if req.MaxOutputTokens != 123 || req.ContextPolicy.ContextWindowTokens != 4096 || req.ContextPolicy.RecentUserTokens != 321 {
		t.Fatalf("context policy not mapped into request: %#v", req.ContextPolicy)
	}
	if req.Cache.Retention != promptcache.RetentionLong {
		t.Fatalf("cache retention = %q, want long", req.Cache.Retention)
	}
	if len(req.HostedTools) != 1 || req.HostedTools[0].Name != "web_search" {
		t.Fatalf("hosted tools = %#v", req.HostedTools)
	}
}

func TestNewEngineWithProviderKeepsProvidedZeroMaxOutputTokens(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	e, err := NewEngineWithProvider(config.Config{
		Provider:     "openai",
		Model:        "gpt-5.4",
		SystemPrompt: "test",
		RunID:        "run",
		ContextPolicy: contextpolicy.Policy{
			ContextWindowTokens: 8192,
			MaxOutputTokens:     0,
		},
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
	}, scripted, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	req := scripted.Requests[0]
	if req.MaxOutputTokens != 0 || req.ContextPolicy.MaxOutputTokens != 0 {
		t.Fatalf("max output should remain unset: max=%d policy=%#v", req.MaxOutputTokens, req.ContextPolicy)
	}
	if req.ContextPolicy.ReservedOutputTokens != contextpolicy.DefaultReservedOutputTokens {
		t.Fatalf("reserved output = %d, want default budget", req.ContextPolicy.ReservedOutputTokens)
	}
}

func TestNewEngineWithProviderUsesCatalogMaxOutputWhenPolicyOmitted(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	cfg, err := config.Resolve(config.Config{
		Provider: "openai",
		Model:    "gpt-5.4",
		APIKey:   "token",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEngineWithProvider(cfg, scripted, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	req := scripted.Requests[0]
	if req.MaxOutputTokens != 128000 || req.ContextPolicy.MaxOutputTokens != 128000 {
		t.Fatalf("max output should use catalog max: max=%d policy=%#v", req.MaxOutputTokens, req.ContextPolicy)
	}
}

func TestNewHarnessWithProviderERegistersSkillsAndPromptMaterial(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writeRuntimeSkill(t, root, "review", "---\nname: review\ndescription: Review code.\n---\n# Review\nCheck behavior.\n")
	scripted := harness.NewScriptedProvider(
		harness.Step(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "skill-1", Name: "skill", Args: `{"name":"review"}`}}}, harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("loaded"), harness.Done()),
	)
	rec := &event.Recorder{}
	h, err := NewHarnessWithProviderE(config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		SystemPrompt: "base prompt",
	}, scripted, HarnessOptions{
		Store: sessiontree.NewMemoryRepo(),
		Sink:  rec,
		Capability: CapabilityOptions{
			SkillsEnabled: true,
			SkillSources:  []skills.Source{{Root: root, Kind: skills.SourceRepo, Enabled: true}},
		},
		NewID: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "use review", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "loaded" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 2 {
		t.Fatalf("requests = %#v", scripted.Requests)
	}
	first := scripted.Requests[0]
	if !slices.ContainsFunc(first.Tools, func(def provider.ToolDefinition) bool { return def.Name == "skill" }) {
		t.Fatalf("skill tool not exposed: %#v", first.Tools)
	}
	if !slices.ContainsFunc(first.Messages, func(msg session.Message) bool {
		return msg.Role == session.System && strings.Contains(msg.Content, "<available_skills>") && strings.Contains(msg.Content, "name: review")
	}) {
		t.Fatalf("available skills prompt missing: %#v", first.Messages)
	}
	second := scripted.Requests[1]
	if !slices.ContainsFunc(second.Messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.ToolName == "skill" && strings.Contains(msg.Content, "# Review")
	}) {
		t.Fatalf("skill tool result missing in second request: %#v", second.Messages)
	}
	events := rec.Snapshot()
	if !slices.ContainsFunc(events, func(ev event.Event) bool { return ev.Type == "skill_detected" }) ||
		!slices.ContainsFunc(events, func(ev event.Event) bool { return ev.Type == "skill_disclosure_applied" }) ||
		!slices.ContainsFunc(events, func(ev event.Event) bool { return ev.Type == "skill_loaded" }) {
		t.Fatalf("skill events missing: %#v", events)
	}
}

func TestNewHarnessWithProviderERegistersMCPToolAsLocalTool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		switch req["method"] {
		case "initialize", "notifications/initialized":
			writeRuntimeRPC(w, id, map[string]any{"protocolVersion": mcpclient.ProtocolVersion})
		case "tools/list":
			writeRuntimeRPC(w, id, map[string]any{"tools": []map[string]any{{
				"name":        "lookup",
				"description": "Lookup a value.",
				"inputSchema": tools.StrictObject(map[string]any{"query": tools.String("Query")}, []string{"query"}),
			}}})
		case "tools/call":
			writeRuntimeRPC(w, id, map[string]any{"content": []map[string]any{{"type": "text", "text": "mcp lookup ok"}}})
		default:
			writeRuntimeRPC(w, id, map[string]any{})
		}
	}))
	defer server.Close()
	scripted := harness.NewScriptedProvider(
		harness.Step(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "mcp-1", Name: "mcp__docs__lookup", Args: `{"query":"floret"}`}}}, harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	rec := &event.Recorder{}
	h, err := NewHarnessWithProviderE(config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		SystemPrompt: "test",
	}, scripted, HarnessOptions{
		Store:    sessiontree.NewMemoryRepo(),
		Sink:     rec,
		Approver: allowRuntimeTools,
		Capability: CapabilityOptions{MCPServers: []mcpclient.ServerConfig{{
			Name:      "docs",
			Transport: mcpclient.TransportStreamableHTTP,
			URL:       server.URL,
			Enabled:   true,
		}}},
		NewID: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := h.StartThread(context.Background(), agentharness.StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(context.Background(), "use mcp", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "done" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 2 {
		t.Fatalf("requests = %#v", scripted.Requests)
	}
	if !slices.ContainsFunc(scripted.Requests[0].Tools, func(def provider.ToolDefinition) bool {
		return def.Name == "mcp__docs__lookup" && def.Annotations["source"] == "mcp" && def.Annotations["open_world"] == true
	}) {
		t.Fatalf("MCP tool not exposed as local tool: %#v", scripted.Requests[0].Tools)
	}
	if !slices.ContainsFunc(scripted.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.ToolName == "mcp__docs__lookup" && msg.Content == "mcp lookup ok"
	}) {
		t.Fatalf("MCP result missing from second request: %#v", scripted.Requests[1].Messages)
	}
	events := rec.Snapshot()
	if !slices.ContainsFunc(events, func(ev event.Event) bool { return ev.Type == event.MCPServerReady }) ||
		!slices.ContainsFunc(events, func(ev event.Event) bool { return ev.Type == event.MCPToolsListed }) {
		t.Fatalf("MCP events missing: %#v", events)
	}
}

func deterministicIDs() func(string) string {
	var seq int
	return func(prefix string) string {
		seq++
		return prefix + "-deterministic"
	}
}

func promptCachePathForTest(value string) string {
	return "id_" + base64.RawURLEncoding.EncodeToString([]byte(value))
}

func writeRuntimeSkill(t *testing.T, root, name, content string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeRuntimeRPC(w http.ResponseWriter, id any, result map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func allowRuntimeTools(context.Context, tools.ApprovalRequest) (tools.PermissionDecision, error) {
	return tools.PermissionDecisionAllow, nil
}
