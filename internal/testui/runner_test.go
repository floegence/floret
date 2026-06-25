package testui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/provider/catalog"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage/sqlite"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/internal/tools/mcp"
	"github.com/floegence/floret/observation"
)

func TestRunnerParsesGoTestJSON(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.StorageMode = StorageModeFile
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(strings.Join([]string{
			`{"Action":"run","Package":"github.com/floegence/floret/internal/engine","Test":"TestOne"}`,
			`{"Action":"pass","Package":"github.com/floegence/floret/internal/engine","Test":"TestOne","Elapsed":0.01}`,
			`{"Action":"run","Package":"github.com/floegence/floret/internal/engine","Test":"TestSkip"}`,
			`{"Action":"skip","Package":"github.com/floegence/floret/internal/engine","Test":"TestSkip","Elapsed":0.01}`,
			`{"Action":"pass","Package":"github.com/floegence/floret/internal/engine","Elapsed":0.03}`,
		}, "\n")), 0
	}

	result := runner.Run(context.Background(), TargetUnit)

	if result.Status != "pass" {
		t.Fatalf("status = %q", result.Status)
	}
	if result.TestTotals.Packages != 1 || result.TestTotals.Tests != 2 || result.TestTotals.Passed != 1 || result.TestTotals.Skipped != 1 {
		t.Fatalf("totals = %#v", result.TestTotals)
	}
	if got := result.Command; !slices.Equal(got, []string{"go", "test", "-json", "./..."}) {
		t.Fatalf("command = %#v", got)
	}
}

func TestRunnerReportsFailedCommand(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(`{"Action":"fail","Package":"github.com/floegence/floret/internal/engine","Elapsed":0.01}`), 1
	}

	result := runner.Run(context.Background(), TargetRace)

	if result.Status != "fail" || result.ExitCode != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.Error == "" {
		t.Fatalf("expected command error")
	}
}

func TestRunnerRejectsUnknownStorageMode(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.StorageMode = "bogus"

	if _, err := runner.sessionStorage(context.Background()); err == nil || !strings.Contains(err.Error(), "unsupported storage mode") {
		t.Fatalf("sessionStorage err = %v, want storage mode error", err)
	}
	status := runner.storageStatus(context.Background())
	if status.Error == "" || !strings.Contains(status.Error, "unsupported storage mode") {
		t.Fatalf("storage status = %#v, want storage mode error", status)
	}
}

func TestRunnerAgentSessionDefaultsDoNotSetWallTime(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done())), nil
	}
	idle, err := runner.CreateIdleAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "hello",
		SystemPrompt: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	idleSession, ok := runner.Sessions.get(idle.ID)
	if !ok {
		t.Fatalf("idle session not registered")
	}
	if idleSession.cfg.WallTime != 0 {
		t.Fatalf("idle session wall time = %s, want 0", idleSession.cfg.WallTime)
	}
	run := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "hello",
		SystemPrompt: "test",
	})
	if run.Status != "completed" {
		t.Fatalf("run = %#v", run)
	}
	runSession, ok := runner.Sessions.get(run.SessionID)
	if !ok {
		t.Fatalf("run session not registered")
	}
	if runSession.cfg.WallTime != 0 {
		t.Fatalf("run session wall time = %s, want 0", runSession.cfg.WallTime)
	}
}

func TestRunnerAgentSessionUsesDefaultFloretProfile(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done()))
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile: ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message: "hello",
	})

	if result.Status != "completed" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 || len(scripted.Requests[0].Messages) == 0 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	if scripted.Requests[0].Messages[0].Role != session.System || scripted.Requests[0].Messages[0].Content != config.DefaultFloretSystemPrompt {
		t.Fatalf("system message = %#v", scripted.Requests[0].Messages[0])
	}
	if result.Session.AgentProfile.ID != config.DefaultAgentProfileID || result.Session.AgentProfile.SystemPrompt != "" {
		t.Fatalf("public agent profile = %#v", result.Session.AgentProfile)
	}
	if result.Session.PromptIdentity.Source != config.PromptSourceDefaultFloret || result.Session.PromptIdentity.SystemPromptHash == "" {
		t.Fatalf("prompt identity = %#v", result.Session.PromptIdentity)
	}
	if result.Session.SystemPrompt != "" {
		t.Fatalf("public session exposed system prompt: %q", result.Session.SystemPrompt)
	}
}

func TestRunnerAgentSessionUsesHostAgentProfile(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done()))
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile: ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		AgentProfile: config.AgentProfile{
			ID:           "acme-support",
			Name:         "Acme Support",
			Description:  "Support assistant.",
			SystemPrompt: "You are Acme Support.",
		},
		Message: "hello",
	})

	if result.Status != "completed" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 || scripted.Requests[0].Messages[0].Content != "You are Acme Support." {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	if result.Session.AgentProfile.ID != "acme-support" || result.Session.AgentProfile.SystemPrompt != "" {
		t.Fatalf("public agent profile = %#v", result.Session.AgentProfile)
	}
	if result.Session.PromptIdentity.Source != config.PromptSourceAgentProfile {
		t.Fatalf("prompt identity = %#v", result.Session.PromptIdentity)
	}
}

func TestRunnerAgentSessionUsesEnvPromptWhenRequestOmitsPrompt(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done()))
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if err := os.WriteFile(runner.EnvFile, []byte("FLORET_SYSTEM_PROMPT=You are Env Agent.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile: ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message: "hello",
	})

	if result.Status != "completed" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 || scripted.Requests[0].Messages[0].Content != "You are Env Agent." {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	if result.Session.PromptIdentity.Source != config.PromptSourceEnv || result.Session.AgentProfile.ID != "custom" {
		t.Fatalf("prompt identity = %#v profile=%#v", result.Session.PromptIdentity, result.Session.AgentProfile)
	}
	if result.Session.SystemPrompt != "" || result.Session.AgentProfile.SystemPrompt != "" {
		t.Fatalf("public session exposed raw prompt: %#v", result.Session)
	}
}

func TestRunnerEvalDemoReturnsTraceMetricsAndArtifacts(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()

	result := runner.Run(context.Background(), TargetEvalDemo)

	if result.Status != "pass" {
		t.Fatalf("result = %#v", result)
	}
	if result.Agent == nil {
		t.Fatalf("agent result missing")
	}
	if result.Agent.Metrics.Steps != 2 || result.Agent.Metrics.ToolCalls != 1 {
		t.Fatalf("metrics = %#v", result.Agent.Metrics)
	}
	if len(result.Agent.Events) == 0 {
		t.Fatalf("events missing")
	}
	if result.Agent.Artifacts["oracle_log"].Content == "" || result.Agent.Artifacts["final_diff"].Content == "" {
		t.Fatalf("artifacts = %#v", result.Agent.Artifacts)
	}
	if !strings.Contains(result.Agent.Artifacts["final_diff"].Content, "+floret eval passed") {
		t.Fatalf("diff artifact did not show eval change:\n%s", result.Agent.Artifacts["final_diff"].Content)
	}
}

func TestRunnerProviderSmokeUsesEnvLocalFakeProvider(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, config.DefaultEnvFile)
	if err := os.WriteFile(envPath, []byte("FLORET_PROVIDER=fake\nFLORET_MODEL=fake-visible\nFLORET_FAKE_RESPONSE=smoke-ok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(root)
	runner.Now = fixedClock()

	info := runner.ConfigInfo()
	if !info.EnvFileFound || info.Provider != config.ProviderFake || info.Model != "fake-visible" {
		t.Fatalf("config info = %#v", info)
	}
	result := runner.Run(context.Background(), TargetProviderSmoke)
	if result.Status != "pass" || result.Agent == nil || result.Agent.Output != "smoke-ok" {
		t.Fatalf("result = %#v", result)
	}
	if result.Agent.Config.Model != "fake-visible" {
		t.Fatalf("agent config = %#v", result.Agent.Config)
	}
}

func TestRunnerFullSuiteAggregatesParts(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(`{"Action":"pass","Package":"github.com/floegence/floret","Elapsed":0.01}`), 0
	}

	result := runner.Run(context.Background(), TargetAll)

	if result.Status != "pass" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Parts) != 6 {
		t.Fatalf("parts = %d, want unit, race, eval, tool scenarios, context compaction scenarios, provider smoke", len(result.Parts))
	}
}

func TestRunnerToolScenarioSuitePassesAndPersistsCoverage(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()

	result := runner.Run(context.Background(), TargetToolScenarios)

	if result.Status != "pass" || len(result.Parts) != 3 {
		t.Fatalf("result = %#v", result)
	}
	for _, part := range result.Parts {
		if part.Agent == nil || part.Agent.Artifacts["scenario"].Content == "" {
			t.Fatalf("scenario part missing artifact: %#v", part)
		}
		if part.Agent.Metrics.ToolCalls == 0 {
			t.Fatalf("scenario %s did not execute tools: %#v", part.Title, part.Agent.Metrics)
		}
	}
}

func TestRunnerContextCompactionScenarioSuitePasses(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()

	result := runner.Run(context.Background(), TargetContextCompactionScenarios)

	if result.Status != "pass" || len(result.Parts) != 4 {
		t.Fatalf("result = %#v", result)
	}
	for _, part := range result.Parts {
		if part.Agent == nil || part.Agent.EngineStatus == "" {
			t.Fatalf("scenario part missing agent evidence: %#v", part)
		}
	}
}

func TestRunnerSavesProfilesToEnvLocalAndHidesSecrets(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	state, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "live",
		Profiles: []ProviderProfile{
			{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "ok"},
			{ID: "live", Name: "Live", Provider: config.ProviderOpenAICompatible, Model: "model-a", BaseURL: "https://example.test/v1", APIKey: "secret"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveProfileID != "live" || len(state.Profiles) != 2 {
		t.Fatalf("state = %#v", state)
	}
	if state.Profiles[1].APIKey != "" || !state.Profiles[1].APIKeySet {
		t.Fatalf("secret leaked or key flag missing: %#v", state.Profiles[1])
	}
	data, err := os.ReadFile(filepath.Join(root, config.DefaultEnvFile))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"FLORET_PROVIDER=\"openai-compatible\"", "FLORET_MODEL=\"model-a\"", "FLORET_API_KEY=\"secret\"", "FLORET_TESTUI_PROFILES_B64="} {
		if !strings.Contains(text, want) {
			t.Fatalf(".env.local missing %s:\n%s", want, text)
		}
	}
}

func TestRunnerConfigStateIncludesCatalogAndNormalizesProviderDefaults(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	state, err := runner.ConfigState()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(state.Catalog, func(provider CatalogProvider) bool {
		return provider.ID == "openai" && provider.DefaultModel == "gpt-5.4" && len(provider.Models) > 0
	}) {
		t.Fatalf("catalog missing openai preset: %#v", state.Catalog)
	}
	saved, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "openai",
		Profiles: []ProviderProfile{{
			ID:       "openai",
			Name:     "OpenAI",
			Provider: "openai",
			APIKey:   "secret",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if saved.Profiles[0].Model != "gpt-5.4" || saved.Profiles[0].BaseURL != "https://api.openai.com/v1" {
		t.Fatalf("profile was not defaulted from catalog: %#v", saved.Profiles[0])
	}
	if saved.ContextPolicyDefaults.ContextWindowTokens != 1050000 || saved.ContextPolicyDefaults.MaxOutputTokens != 128000 || saved.ContextPolicyDefaults.RecentTailTokens != 12000 {
		t.Fatalf("context policy defaults were not defaulted from active catalog model: %#v", saved.ContextPolicyDefaults)
	}
}

func TestRunnerConfigStateLoadsHostMCPConfig(t *testing.T) {
	root := t.TempDir()
	server := fakeHTTPMCPServer(t, "lookup", "ok")
	defer server.Close()
	writeEnv(t, root, "FLORET_PROVIDER=fake\nFLORET_MCP_CONFIG=mcp.json\n")
	writeMCPConfig(t, root, []map[string]any{{
		"Name":      "docs",
		"Transport": "streamable_http",
		"URL":       server.URL,
		"Enabled":   true,
	}})
	runner := NewRunner(root)
	state, err := runner.ConfigState()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Capabilities.MCPServers) != 1 {
		t.Fatalf("mcp servers = %#v", state.Capabilities.MCPServers)
	}
	got := state.Capabilities.MCPServers[0]
	if got.Name != "docs" || got.Status != "ready" || got.Transport != "streamable_http" || got.ToolCount != 1 || got.PermissionMode != "ask" {
		t.Fatalf("mcp state = %#v", got)
	}
}

func TestRunnerMCPConfigExposesToolAndDeniesDefaultApproval(t *testing.T) {
	root := t.TempDir()
	server := fakeHTTPMCPServer(t, "lookup", "mcp ok")
	defer server.Close()
	writeEnv(t, root, "FLORET_PROVIDER=fake\nFLORET_MCP_CONFIG=mcp.json\n")
	writeMCPConfig(t, root, []map[string]any{{
		"Name":      "docs",
		"Transport": "streamable_http",
		"URL":       server.URL,
		"Enabled":   true,
	}})
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(
			harness.Step(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "mcp-1", Name: "mcp__docs__lookup", Args: `{"query":"x"}`}}}, harness.DoneReason("tool_calls")),
			harness.Step(harness.Text("recovered"), harness.Done()),
		), nil
	}
	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "use mcp",
		SystemPrompt: "test",
	})
	if result.Status != "completed" || result.Output != "recovered" {
		t.Fatalf("result = %#v", result)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].Tools, func(def provider.ToolDefinition) bool {
		return def.Name == "mcp__docs__lookup" && def.Annotations["source"] == "mcp" && def.Annotations["permission_mode"] == "ask"
	}) {
		t.Fatalf("mcp tool not visible: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	if !slices.ContainsFunc(result.Session.Capabilities.MCPServers, func(server MCPCapabilityState) bool {
		return server.Name == "docs" && server.Status == "ready" && server.ToolCount == 1
	}) {
		t.Fatalf("mcp capability missing: %#v", result.Session.Capabilities)
	}
	if !slices.ContainsFunc(result.Events, func(ev event.Event) bool {
		meta, _ := ev.Metadata.(map[string]any)
		return ev.Type == event.MCPServerReady && meta["server_id"] == "docs"
	}) {
		t.Fatalf("readable mcp event missing: %#v", result.Events)
	}
	if !slices.ContainsFunc(result.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Role == "tool" && msg.ToolName == "mcp__docs__lookup" && msg.Content == "ERROR: tool call rejected"
	}) {
		t.Fatalf("denied mcp result missing: %#v", result.Observation.ActiveContext)
	}
}

func TestRunnerToolCatalogReflectsWebSearchCapability(t *testing.T) {
	t.Run("fake default unavailable", func(t *testing.T) {
		runner := NewRunner(t.TempDir())
		state, err := runner.ConfigState()
		if err != nil {
			t.Fatal(err)
		}
		option := toolOptionByName(t, state.Tools, "web_search")
		if option.Available || option.Source != string(searchcap.WebSearchDisabled) || !strings.Contains(option.Unavailable, "web search disabled") {
			t.Fatalf("web_search option = %#v", option)
		}
	})

	t.Run("provider hosted", func(t *testing.T) {
		runner := NewRunner(t.TempDir())
		state, err := runner.SaveConfigState(SaveConfigRequest{
			ActiveProfileID: "hosted",
			Profiles: []ProviderProfile{{
				ID:        "hosted",
				Name:      "Hosted",
				Provider:  catalog.ProviderAnthropic,
				Model:     "model-a",
				WebSearch: searchcap.Capability{Source: searchcap.WebSearchProviderHosted, Hosted: searchcap.HostedConfig{WireShape: searchcap.WireShapeAnthropicServerWebSearch}},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		option := toolOptionByName(t, state.Tools, "web_search")
		if !option.Available || option.Source != string(searchcap.WebSearchProviderHosted) || option.WireShape != string(searchcap.WireShapeAnthropicServerWebSearch) || option.Exposure != "hosted tool: web_search" {
			t.Fatalf("web_search option = %#v", option)
		}
	})

	t.Run("external brave key gates availability", func(t *testing.T) {
		root := t.TempDir()
		runner := NewRunner(root)
		state, err := runner.SaveConfigState(SaveConfigRequest{
			ActiveProfileID: "external-brave",
			Profiles: []ProviderProfile{{
				ID:        "external-brave",
				Name:      "External Brave",
				Provider:  config.ProviderFake,
				Model:     "fake-model",
				WebSearch: searchcap.Capability{Source: searchcap.WebSearchExternalBrave, Brave: searchcap.BraveConfig{Provider: searchcap.ExternalProviderBrave}},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		option := toolOptionByName(t, state.Tools, "web_search")
		if option.Available || !strings.Contains(option.Unavailable, "API key") {
			t.Fatalf("web_search without key = %#v", option)
		}
		state, err = runner.SaveConfigState(SaveConfigRequest{
			ActiveProfileID: "external-brave",
			Profiles: []ProviderProfile{{
				ID:        "external-brave",
				Name:      "External Brave",
				Provider:  config.ProviderFake,
				Model:     "fake-model",
				WebSearch: searchcap.Capability{Source: searchcap.WebSearchExternalBrave, Brave: searchcap.BraveConfig{Provider: searchcap.ExternalProviderBrave}},
			}},
			SearchProvider: SaveSearchProvider{Provider: "brave", APIKey: "search-key"},
		})
		if err != nil {
			t.Fatal(err)
		}
		option = toolOptionByName(t, state.Tools, "web_search")
		if !option.Available || option.Source != string(searchcap.WebSearchExternalBrave) || option.Exposure != "local tool: web_search" {
			t.Fatalf("web_search with key = %#v", option)
		}
		if data, err := os.ReadFile(filepath.Join(root, config.DefaultEnvFile)); err != nil || !strings.Contains(string(data), "FLORET_BRAVE_SEARCH_API_KEY") {
			t.Fatalf("env data = %q err=%v", data, err)
		}
	})
}

func TestRunnerRejectsUnsupportedHostedSearchWireShapeOnSave(t *testing.T) {
	runner := NewRunner(t.TempDir())
	_, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "bad",
		Profiles: []ProviderProfile{{
			ID:        "bad",
			Name:      "Bad",
			Provider:  catalog.ProviderOpenAI,
			Model:     "model-a",
			WebSearch: searchcap.Capability{Source: searchcap.WebSearchProviderHosted, Hosted: searchcap.HostedConfig{WireShape: "bad_shape"}},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported hosted web_search wire shape") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunnerRunAgentReturnsInteractionObservation(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "fake",
		Profiles: []ProviderProfile{{
			ID:           "fake",
			Name:         "Fake",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "agent-ok",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	result := runner.RunAgent(context.Background(), AgentRunRequest{
		ProfileID:    "fake",
		Message:      "hello",
		SystemPrompt: "test",
	})

	if result.Status != "completed" || result.Output != "agent-ok" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) != 1 {
		t.Fatalf("requests = %#v", result.Observation.ProviderRequests)
	}
	if len(result.Observation.ProviderEvents) == 0 {
		t.Fatalf("provider events missing")
	}
	if len(result.Observation.SessionMessages) == 0 {
		t.Fatalf("session messages missing")
	}
	if len(result.Observation.Transitions) == 0 {
		t.Fatalf("transitions missing")
	}
	if slices.ContainsFunc(result.Observation.ProviderRequests[0].Tools, func(tool provider.ToolDefinition) bool {
		return tool.Name == "task_complete"
	}) {
		t.Fatalf("task_complete should not be a default test UI tool: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].Tools, func(tool provider.ToolDefinition) bool {
		return tool.Name == "ask_user"
	}) {
		t.Fatalf("ask_user interrupt tool definition missing: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	summary := result.Observation.ProviderRequests[0].CacheSummary
	if len(result.Observation.ProviderRequests[0].RawSegments) == 0 || summary.PrefixHash == "" || summary.PayloadHash == "" || summary.ToolsetID == "" || summary.ToolsetEpoch == 0 {
		t.Fatalf("prompt cache observation missing: %#v", result.Observation.ProviderRequests[0])
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].RawSegments, func(segment ObservedRawSegment) bool {
		return segment.Kind == "system" && !segment.Reused
	}) {
		t.Fatalf("raw segment state not exposed: %#v", result.Observation.ProviderRequests[0].RawSegments)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].RawSegments, func(segment ObservedRawSegment) bool {
		return segment.Kind == "system" &&
			segment.Raw != "" &&
			segment.RawPreview != "" &&
			segment.Fingerprint != "" &&
			segment.SchemaVersion != "" &&
			segment.AdapterVersion != "" &&
			segment.Sequence > 0
	}) {
		t.Fatalf("raw segment local inspection fields not exposed: %#v", result.Observation.ProviderRequests[0].RawSegments)
	}
}

func TestRunnerAgentSessionSelectedToolsExplicitlyExposeBuiltInTools(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "fake",
		Profiles: []ProviderProfile{{
			ID:           "fake",
			Name:         "Fake",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	chat := runner.RunAgent(context.Background(), AgentRunRequest{
		ProfileID:     "fake",
		Message:       "hello",
		SystemPrompt:  "test",
		SelectedTools: []string{},
	})
	if chat.Status != "completed" || chat.Session.SelectedTools == nil || len(chat.Session.SelectedTools) != 0 {
		t.Fatalf("chat = %#v", chat)
	}
	if !hasOnlyStrictTools(chat.Observation.ProviderRequests[0].Tools, "ask_user") {
		t.Fatalf("chat mode should expose only control tools: %#v", chat.Observation.ProviderRequests[0].Tools)
	}
	meta, err := runner.loadAgentSessionMetadata(chat.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.SelectedTools == nil || len(meta.SelectedTools) != 0 {
		t.Fatalf("metadata should persist empty selected tools: %#v", meta.SelectedTools)
	}

	grepOnly := runner.RunAgent(context.Background(), AgentRunRequest{
		ProfileID:     "fake",
		Message:       "hello",
		SystemPrompt:  "test",
		SelectedTools: []string{"grep"},
	})
	if grepOnly.Status != "completed" || !slices.Equal(grepOnly.Session.SelectedTools, []string{"grep"}) {
		t.Fatalf("grepOnly = %#v", grepOnly)
	}
	if !hasStrictTool(grepOnly.Observation.ProviderRequests[0].Tools, "grep") {
		t.Fatalf("grep tool missing: %#v", grepOnly.Observation.ProviderRequests[0].Tools)
	}
	if hasStrictTool(grepOnly.Observation.ProviderRequests[0].Tools, "read") || hasStrictTool(grepOnly.Observation.ProviderRequests[0].Tools, "list") {
		t.Fatalf("unselected read tools were exposed: %#v", grepOnly.Observation.ProviderRequests[0].Tools)
	}

	coding := runner.RunAgent(context.Background(), AgentRunRequest{
		ProfileID:     "fake",
		Message:       "hello",
		SystemPrompt:  "test",
		SelectedTools: []string{"read", "list", "glob", "grep", "apply_patch", "write", "shell"},
	})
	if coding.Status != "completed" || len(coding.Session.SelectedTools) != 7 {
		t.Fatalf("coding = %#v", coding)
	}
	for _, name := range []string{"read", "list", "glob", "grep", "apply_patch", "write", "shell"} {
		if !hasStrictTool(coding.Observation.ProviderRequests[0].Tools, name) {
			t.Fatalf("selected tool %s missing: %#v", name, coding.Observation.ProviderRequests[0].Tools)
		}
	}
	if hasStrictTool(coding.Observation.ProviderRequests[0].Tools, "web_search") || hasStrictTool(coding.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("unavailable network tools should not be exposed: %#v", coding.Observation.ProviderRequests[0].Tools)
	}
}

func TestRunnerAgentSessionCarriesReasoningThroughToolFollowUp(t *testing.T) {
	root := t.TempDir()
	scripted := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Inspect workspace before answering."},
			harness.Text("Checking files first."),
			harness.Tool("probe-list", "list", `{"path":null,"limit":5}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Use the list result."},
			harness.Text("done"),
			harness.Done(),
		),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "inspect",
		SystemPrompt:  "test",
		SelectedTools: []string{"list"},
	})

	if result.Status != "completed" || result.Output != "Checking files first.done" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) != 2 {
		t.Fatalf("provider requests = %#v", result.Observation.ProviderRequests)
	}
	if !slices.ContainsFunc(result.Observation.ProviderEvents, func(ev ObservedProviderEvent) bool {
		return ev.Type == provider.Reasoning && ev.Reasoning == "Inspect workspace before answering."
	}) {
		t.Fatalf("reasoning provider event missing: %#v", result.Observation.ProviderEvents)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.Content == "Checking files first." && msg.Reasoning == "Inspect workspace before answering."
	}) {
		t.Fatalf("assistant text reasoning missing: %#v", result.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" &&
			msg.ToolName == "list" &&
			msg.ToolCallID == "probe-list" &&
			msg.Reasoning == "Inspect workspace before answering."
	}) {
		t.Fatalf("tool call reasoning missing: %#v", result.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[1].Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" &&
			msg.ToolName == "list" &&
			msg.ToolCallID == "probe-list" &&
			msg.Reasoning == "Inspect workspace before answering."
	}) {
		t.Fatalf("follow-up request did not replay tool-call reasoning: %#v", result.Observation.ProviderRequests[1].Messages)
	}
}

func TestRunnerAgentSessionExposesToolOutputArtifactMetadata(t *testing.T) {
	root := t.TempDir()
	scripted := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("shell-1", "shell", `{"command":"printf '0123456789abcdef'","max_output_bytes":8}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "run shell",
		SystemPrompt:  "test",
		SelectedTools: []string{"shell"},
	})

	if result.Status != "completed" || result.Output != "done" {
		t.Fatalf("result = %#v", result)
	}
	if !slices.ContainsFunc(scripted.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == "tool" && msg.ToolCallID == "shell-1" && msg.Content == "89abcdef"
	}) {
		t.Fatalf("follow-up request should contain projected shell output: %#v", scripted.Requests[1].Messages)
	}
	var toolMsg ObservedSessionMessage
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		if msg.Role == "tool" && msg.ToolCallID == "shell-1" {
			toolMsg = msg
			return true
		}
		return false
	}) {
		t.Fatalf("tool result message missing: %#v", result.Observation.SessionMessages)
	}
	if toolMsg.Content != "89abcdef" {
		t.Fatalf("local inspection should expose projected tool result content: %#v", toolMsg)
	}
	if toolMsg.ToolResult == nil || !toolMsg.ToolResult.Truncated || toolMsg.ToolResult.FullOutput == nil {
		t.Fatalf("tool result view missing artifact ref: %#v", toolMsg)
	}
	if strings.Contains(toolMsg.ToolResult.FullOutput.URL, root) || !strings.HasPrefix(toolMsg.ToolResult.FullOutput.URL, "/artifacts/") {
		t.Fatalf("artifact ref should be safe and routed: %#v", toolMsg.ToolResult.FullOutput)
	}
	artifactPath := filepath.Join(runner.managedArtifactsRoot(), filepath.FromSlash(strings.TrimPrefix(toolMsg.ToolResult.FullOutput.URL, "/artifacts/")))
	data, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("artifact file missing: %v", err)
	}
	if string(data) != "0123456789abcdef" {
		t.Fatalf("artifact content = %q", data)
	}
	if err := runner.DeleteAgentSession(context.Background(), result.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(toolOutputArtifactSessionDir(runner.managedArtifactsRoot(), result.SessionID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tool output artifact directory still exists or returned unexpected err: %v", err)
	}
}

func TestRunnerAgentSessionCanExecuteExternalBraveWebSearch(t *testing.T) {
	root := t.TempDir()
	searchServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Subscription-Token") != "test-search-key" {
			http.Error(w, "missing search token", http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("q") != "Changsha weather 2026-06-03" {
			http.Error(w, "wrong query", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"web":{"results":[{"title":"Changsha forecast","url":"https://example.com/changsha","description":"Thunderstorms today.","profile":{"name":"Example Weather"}}]}}`))
	}))
	defer searchServer.Close()
	if err := os.WriteFile(filepath.Join(root, config.DefaultEnvFile), []byte("FLORET_BRAVE_SEARCH_API_KEY=test-search-key\nFLORET_BRAVE_SEARCH_ENDPOINT="+searchServer.URL+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	scripted := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("search-1", "web_search", `{"query":"Changsha weather 2026-06-03","count":3,"country":"CN","search_lang":"zh-hans","freshness":null}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("The search result says thunderstorms today."), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       fakeExternalBraveSearchProfile(),
		Message:       "查询长沙天气",
		SystemPrompt:  "test",
		SelectedTools: []string{"web_search"},
	})

	if result.Status != "completed" || !strings.Contains(result.Output, "thunderstorms") {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) != 2 {
		t.Fatalf("provider requests = %#v", result.Observation.ProviderRequests)
	}
	if !hasStrictTool(result.Observation.ProviderRequests[0].Tools, "web_search") || hasStrictTool(result.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("web_search tool contract missing or mixed with fetch: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "tool" && msg.ToolName == "web_search" && strings.Contains(msg.Content, "Changsha forecast")
	}) {
		t.Fatalf("web_search tool result missing: %#v", result.Observation.SessionMessages)
	}
}

func TestRunnerAgentSessionRejectsWebFetchSelection(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "fetch url",
		SystemPrompt:  "test",
		SelectedTools: []string{"web_fetch"},
	})

	if result.Status != "error" || result.StatusCode != http.StatusBadRequest || !strings.Contains(result.Error, `unknown test UI tool "web_fetch"`) {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunnerAgentSessionUsesProviderHostedSearchWithoutLocalWebSearch(t *testing.T) {
	root := t.TempDir()
	newProvider := func() provider.Provider {
		return harness.NewScriptedProvider(
			harness.Step(
				provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: provider.ToolCall{ID: "hosted-search-1", Name: "web_search", Args: `{"query":"Changsha weather"}`}},
				provider.StreamEvent{Type: provider.HostedToolResult, ToolCall: provider.ToolCall{ID: "hosted-search-1", Name: "web_search"}, HostedResult: provider.HostedToolResultData{
					Results: []provider.HostedToolResultItem{{
						Title:   "Changsha forecast",
						URL:     "https://example.com/hosted/changsha",
						Snippet: "provider-hosted result",
						Source:  "Example Weather",
					}},
					Metadata: map[string]any{"encrypted_content": "raw-provider-token"},
				}},
				harness.Text("Hosted search answered."),
				harness.Done(),
			),
		)
	}
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return newProvider(), nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile: ProviderProfile{
			ID:        "hosted",
			Name:      "Hosted",
			Provider:  catalog.ProviderAnthropic,
			Model:     "model",
			APIKey:    "profile-api-key-private",
			WebSearch: searchcap.Capability{Source: searchcap.WebSearchProviderHosted, Hosted: searchcap.HostedConfig{WireShape: searchcap.WireShapeAnthropicServerWebSearch}},
		},
		Message:       "search",
		SystemPrompt:  "system token=hosted-secret",
		SelectedTools: []string{"web_search"},
	})

	if result.Status != "completed" || result.Output != "Hosted search answered." {
		t.Fatalf("result = %#v", result)
	}
	req := result.Observation.ProviderRequests[0]
	if hasStrictTool(req.Tools, "web_search") {
		t.Fatalf("provider-hosted web_search must not enter local tools: %#v", req.Tools)
	}
	if len(req.HostedTools) != 1 || req.HostedTools[0].Name != "web_search" || req.HostedTools[0].Type != "web_search" || req.HostedTools[0].Options["wire_shape"] != string(searchcap.WireShapeAnthropicServerWebSearch) {
		t.Fatalf("hosted tools = %#v", req.HostedTools)
	}
	if !slices.ContainsFunc(result.Events, func(ev event.Event) bool {
		return ev.Type == event.HostedToolCall && ev.ToolName == "web_search"
	}) || !slices.ContainsFunc(result.Events, func(ev event.Event) bool {
		return ev.Type == event.HostedToolResult && ev.ToolName == "web_search"
	}) {
		t.Fatalf("hosted tool events missing: %#v", result.Events)
	}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"profile-api-key-private"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("hosted run response exposed profile secret %q: %s", secret, body)
		}
	}
	for _, want := range []string{"provider-hosted result", "https://example.com/hosted/changsha", "Changsha forecast", "raw-provider-token", "system token=hosted-secret"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("local inspection hosted response missing %q: %s", want, body)
		}
	}
	if len(result.Session.HostedTools) != 1 || result.Session.HostedTools[0].Options["wire_shape"] != string(searchcap.WireShapeAnthropicServerWebSearch) {
		t.Fatalf("local inspection session snapshot missing hosted payload details: %#v", result.Session.HostedTools)
	}
	if result.Session.SystemPrompt != "" {
		t.Fatalf("local inspection session snapshot exposed system prompt: %#v", result.Session.SystemPrompt)
	}
	if result.Session.AgentProfile.SystemPrompt != "" {
		t.Fatalf("public session snapshot exposed agent profile prompt: %#v", result.Session.AgentProfile)
	}

	if !slices.ContainsFunc(result.Observation.ProviderEvents, func(ev ObservedProviderEvent) bool {
		return ev.Type == provider.HostedToolCall &&
			len(ev.ToolCalls) == 1 &&
			ev.ToolCalls[0].ID == "hosted-search-1" &&
			ev.ToolCalls[0].Name == "web_search" &&
			ev.ToolCalls[0].Args == `{"query":"Changsha weather"}`
	}) {
		t.Fatalf("local inspection provider events should expose hosted call args: %#v", result.Observation.ProviderEvents)
	}
	if !slices.ContainsFunc(result.Observation.ProviderEvents, func(ev ObservedProviderEvent) bool {
		return ev.Type == provider.HostedToolResult &&
			ev.HostedResult != nil &&
			len(ev.HostedResult.Results) == 1 &&
			ev.HostedResult.Results[0].Title == "Changsha forecast" &&
			ev.HostedResult.Results[0].URL == "https://example.com/hosted/changsha" &&
			ev.HostedResult.Results[0].Snippet == "provider-hosted result" &&
			ev.HostedResult.Metadata["encrypted_content"] == "raw-provider-token"
	}) {
		t.Fatalf("local inspection provider events should expose hosted result details: %#v", result.Observation.ProviderEvents)
	}
	hostedResults := 0
	for _, ev := range result.Events {
		if ev.Type == event.HostedToolResult && ev.ToolName == "web_search" {
			hostedResults++
		}
	}
	if hostedResults != 1 {
		t.Fatalf("hosted result should be emitted once, got %d: %#v", hostedResults, result.Events)
	}
}

func TestRunnerAgentLocalInspectionExposesRawDataByDefault(t *testing.T) {
	root := t.TempDir()
	newProvider := func() provider.Provider {
		return harness.NewScriptedProvider(
			harness.Step(
				provider.StreamEvent{Type: provider.Reasoning, Text: "secret reasoning token=abc"},
				harness.Tool("list-1", "list", `{"path":null,"limit":2}`),
				harness.DoneReason("tool_calls"),
			),
			harness.Step(harness.Text("public answer token=abc"), harness.Done()),
		)
	}
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return newProvider(), nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "inspect token=abc",
		SystemPrompt:  "system token=abc",
		SelectedTools: []string{"list"},
	})
	if result.Status != "completed" {
		t.Fatalf("result = %#v", result)
	}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"secret reasoning token=abc", "public answer token=abc", `{\"path\":null,\"limit\":2}`} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("local inspection response missing %q: %s", want, body)
		}
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].RawSegments, func(segment ObservedRawSegment) bool {
		return segment.Raw != "" && segment.RawPreview != "" && segment.SHA256 != ""
	}) {
		t.Fatalf("local inspection raw segments should expose ledger and raw text: %#v", result.Observation.ProviderRequests[0].RawSegments)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.ToolCallID == "list-1" && msg.ToolArgs == `{"path":null,"limit":2}`
	}) {
		t.Fatalf("local inspection session messages should expose tool args: %#v", result.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(result.Observation.ProviderEvents, func(ev ObservedProviderEvent) bool {
		return len(ev.ToolCalls) == 1 && ev.ToolCalls[0].Args == `{"path":null,"limit":2}`
	}) {
		t.Fatalf("local inspection provider events should expose tool args: %#v", result.Observation.ProviderEvents)
	}
}

func TestRunnerAgentSessionRejectsUnavailableWebSearch(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "search",
		SystemPrompt:  "test",
		SelectedTools: []string{"web_search"},
	})
	if result.Status != "error" || result.StatusCode != http.StatusBadRequest || !strings.Contains(result.Error, "web_search is unavailable") {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunnerAgentSessionRestoresSelectedToolsForAppend(t *testing.T) {
	root := t.TempDir()
	firstProvider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Next?"}`), harness.Done()),
	)
	firstRunner := NewRunner(root)
	firstRunner.Now = fixedClock()
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return firstProvider, nil
	}

	first := firstRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "start",
		SystemPrompt:  "test",
		SelectedTools: []string{"grep", "shell"},
	})
	if first.Status != "waiting" || !slices.Equal(first.Session.SelectedTools, []string{"grep", "shell"}) {
		t.Fatalf("first = %#v", first)
	}
	if !hasStrictTool(first.Observation.ProviderRequests[0].Tools, "grep") || !hasStrictTool(first.Observation.ProviderRequests[0].Tools, "shell") {
		t.Fatalf("first tools = %#v", first.Observation.ProviderRequests[0].Tools)
	}

	secondProvider := harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done()))
	secondRunner := NewRunner(root)
	secondRunner.Now = fixedClock()
	secondRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return secondProvider, nil
	}
	second := secondRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "continue"})
	if second.Status != "completed" || !slices.Equal(second.Session.SelectedTools, []string{"grep", "shell"}) {
		t.Fatalf("second = %#v", second)
	}
	if len(second.Observation.ProviderRequests) != 1 {
		t.Fatalf("requests = %#v", second.Observation.ProviderRequests)
	}
	if !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "grep") || !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "shell") {
		t.Fatalf("restored tools missing: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if hasStrictTool(second.Observation.ProviderRequests[0].Tools, "read") || hasStrictTool(second.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("unselected tools restored: %#v", second.Observation.ProviderRequests[0].Tools)
	}
}

func TestRunnerAgentSessionToolPatchPersistsAcrossRestore(t *testing.T) {
	root := t.TempDir()
	firstProvider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Continue?"}`), harness.Done()),
	)
	firstRunner := NewRunner(root)
	firstRunner.Now = fixedClock()
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return firstProvider, nil
	}
	first := firstRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "start",
		SystemPrompt:  "test",
		SelectedTools: []string{"grep"},
	})
	if first.Status != "waiting" || !slices.Equal(first.Session.SelectedTools, []string{"grep"}) {
		t.Fatalf("first = %#v", first)
	}
	patched, err := firstRunner.UpdateAgentSessionTools(context.Background(), first.SessionID, AgentToolsUpdateRequest{SelectedTools: toolSelection("read", "shell"), Reason: "restore test"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(patched.SelectedTools, []string{"read", "shell"}) {
		t.Fatalf("patched tools = %#v", patched.SelectedTools)
	}

	restoredRunner := NewRunner(root)
	restoredRunner.Now = fixedClock()
	restoredRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done())), nil
	}
	second := restoredRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "yes"})
	if second.Status != "completed" || !slices.Equal(second.Session.SelectedTools, []string{"read", "shell"}) {
		t.Fatalf("second = %#v", second)
	}
	if !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "read") || !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "shell") {
		t.Fatalf("patched tools missing after restore: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if hasStrictTool(second.Observation.ProviderRequests[0].Tools, "grep") {
		t.Fatalf("previous tool still exposed after restore: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if !slices.ContainsFunc(second.Session.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "active_tools_change" &&
			entry.Metadata["previous_tools"] == "grep" &&
			entry.Metadata["selected_tools"] == "read,shell" &&
			entry.Metadata["reason"] == "restore test"
	}) {
		t.Fatalf("tool patch audit entry missing after restore: %#v", second.Session.PathEntries)
	}
	raw, err := restoredRunner.sessionStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entries, err := raw.repo(root).Entries(context.Background(), first.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryActiveTools &&
			entry.Metadata["previous_tools"] == "grep" &&
			entry.Metadata["selected_tools"] == "read,shell" &&
			entry.Metadata["reason"] == "restore test"
	}) {
		t.Fatalf("durable audit metadata should retain raw reason: %#v", entries)
	}
}

func TestRunnerRestoresLongSessionEntryAndKeepsSelectedTools(t *testing.T) {
	root := t.TempDir()
	longOutput := strings.Repeat("weather payload ", 12_000)
	firstProvider := harness.NewScriptedProvider(
		harness.Step(harness.Text(longOutput), harness.Done()),
	)
	firstRunner := NewRunner(root)
	firstRunner.Now = fixedClock()
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return firstProvider, nil
	}
	first := firstRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "store a long answer",
		SystemPrompt:  "test",
		SelectedTools: []string{"grep", "shell"},
		ContextPolicy: config.ContextPolicy{
			ContextWindowTokens:   1_000_000,
			MaxOutputTokens:       4096,
			RecentTailTokens:      4096,
			ReservedSummaryTokens: 1024,
			ReservedOutputTokens:  4096,
		},
	})
	if first.Status != "completed" || first.Output != longOutput {
		t.Fatalf("first = %#v", first)
	}

	recoveredRunner := NewRunner(root)
	recoveredRunner.Now = fixedClock()
	recoveredRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("restored done"), harness.Done())), nil
	}
	sessions := recoveredRunner.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].ID != first.SessionID || !slices.Equal(sessions[0].SelectedTools, []string{"grep", "shell"}) {
		t.Fatalf("sessions = %#v", sessions)
	}
	second := recoveredRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "continue"})
	if second.Status != "completed" || second.Output != "restored done" {
		t.Fatalf("second = %#v", second)
	}
	if !slices.Equal(second.Session.SelectedTools, []string{"grep", "shell"}) {
		t.Fatalf("selected tools lost after restore: %#v", second.Session.SelectedTools)
	}
	if len(second.Observation.ProviderRequests) != 1 {
		t.Fatalf("provider requests = %#v", second.Observation.ProviderRequests)
	}
	if !slices.ContainsFunc(second.Observation.ProviderRequests[0].Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.Content == longOutput
	}) {
		t.Fatalf("follow-up request missing restored long assistant entry")
	}
	if !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "grep") || !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "shell") {
		t.Fatalf("restored selected tools missing from provider request: %#v", second.Observation.ProviderRequests[0].Tools)
	}
}

func hasStrictTool(defs []provider.ToolDefinition, name string) bool {
	return slices.ContainsFunc(defs, func(tool provider.ToolDefinition) bool {
		return tool.Name == name && tool.Strict && tool.InputSchema["additionalProperties"] == false
	})
}

func hasOnlyStrictTools(defs []provider.ToolDefinition, names ...string) bool {
	if len(defs) != len(names) {
		return false
	}
	for _, name := range names {
		if !hasStrictTool(defs, name) {
			return false
		}
	}
	return true
}

func TestRunnerAgentSessionWaitsAndResumesWithAppendMessage(t *testing.T) {
	root := t.TempDir()
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Which file?"}`), harness.Done()),
		harness.Step(harness.Text("resumed"), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "fake",
		Profiles: []ProviderProfile{{
			ID:           "fake",
			Name:         "Fake",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "unused",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		ProfileID:    "fake",
		Message:      "start",
		SystemPrompt: "test",
	})
	if first.Status != "waiting" || first.WaitingPrompt != "Which file?" || !first.CanAppendMessage {
		t.Fatalf("first = %#v", first)
	}
	if first.SessionID == "" || first.TurnID == "" {
		t.Fatalf("missing session/turn ids: %#v", first)
	}
	if !slices.ContainsFunc(first.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.ToolName == "ask_user" && msg.ToolArgs == `{"question":"Which file?"}`
	}) {
		t.Fatalf("ask_user call not exposed in session messages: %#v", first.Observation.SessionMessages)
	}

	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "main.go"})
	if second.Status != "completed" || second.Output != "resumed" {
		t.Fatalf("second = %#v", second)
	}
	if second.SessionID != first.SessionID || second.TurnID == first.TurnID {
		t.Fatalf("turn/session ids not advanced: first=%s/%s second=%s/%s", first.SessionID, first.TurnID, second.SessionID, second.TurnID)
	}
	if len(second.Session.Turns) != 2 {
		t.Fatalf("turns = %#v", second.Session.Turns)
	}
	if second.Session.AggregateMetrics.LLMRequests != 2 {
		t.Fatalf("aggregate metrics = %#v", second.Session.AggregateMetrics)
	}
	if !slices.ContainsFunc(second.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.Kind == "control_signal" && strings.Contains(msg.Content, "Which file?")
	}) {
		t.Fatalf("active context missing provider-safe ask_user signal: %#v", second.Observation.ActiveContext)
	}
	if !slices.ContainsFunc(second.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Role == "user" && msg.Content == "main.go"
	}) {
		t.Fatalf("active context missing appended user answer: %#v", second.Observation.ActiveContext)
	}
	if !slices.ContainsFunc(second.Observation.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "turn_marker" && entry.TurnStatus == "waiting" && entry.TurnID == first.TurnID
	}) {
		t.Fatalf("path entries missing waiting marker: %#v", second.Observation.PathEntries)
	}
	if len(second.Observation.ProviderRequests) != 2 || second.Observation.ProviderRequests[0].RunID == second.Observation.ProviderRequests[1].RunID {
		t.Fatalf("provider requests missing per-turn run ids: %#v", second.Observation.ProviderRequests)
	}
	if !slices.ContainsFunc(second.Observation.Transitions, func(transition StateTransition) bool {
		return transition.Reason == "provider_request"
	}) {
		t.Fatalf("second turn transitions missing provider request: %#v", second.Observation.Transitions)
	}
	if slices.ContainsFunc(second.Observation.Transitions, func(transition StateTransition) bool {
		return strings.Contains(transition.Details, "Which file?")
	}) {
		t.Fatalf("second turn transitions leaked first turn events: %#v", second.Observation.Transitions)
	}
}

func TestRunnerAgentSessionPersistsAndResumesAfterNewRunner(t *testing.T) {
	root := t.TempDir()
	firstProvider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Which file?"}`), harness.Done()),
	)
	firstRunner := NewRunner(root)
	firstRunner.Now = fixedClock()
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return firstProvider, nil
	}

	first := firstRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "unused"},
		Message:      "start",
		SystemPrompt: "test",
	})
	if first.Status != "waiting" || first.SessionID == "" || len(first.Session.Turns) != 1 {
		t.Fatalf("first = %#v", first)
	}

	recoveredRunner := NewRunner(root)
	recoveredRunner.Now = fixedClock()
	secondProvider := harness.NewScriptedProvider(
		harness.Step(harness.Text("resumed after restart"), harness.Done()),
	)
	recoveredRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return secondProvider, nil
	}

	sessions := recoveredRunner.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].ID != first.SessionID || sessions[0].Status != "waiting" || sessions[0].WaitingPrompt != "Which file?" {
		t.Fatalf("sessions = %#v", sessions)
	}
	if len(sessions[0].Turns) != 1 || sessions[0].Turns[0].ID != first.TurnID {
		t.Fatalf("persisted turns = %#v", sessions[0].Turns)
	}

	snapshot, err := recoveredRunner.AgentSession(context.Background(), first.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "waiting" || !snapshot.CanAppendMessage || snapshot.LatestTurnID != first.TurnID {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	second := recoveredRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "main.go"})
	if second.Status != "completed" || second.Output != "resumed after restart" {
		t.Fatalf("second = %#v", second)
	}
	if second.TurnID == first.TurnID || second.TurnID == "turn-1" {
		t.Fatalf("turn id was not advanced after restart: first=%s second=%s", first.TurnID, second.TurnID)
	}
	if len(second.Session.Turns) != 2 {
		t.Fatalf("turns = %#v", second.Session.Turns)
	}
	if !slices.ContainsFunc(second.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.Kind == "control_signal" && strings.Contains(msg.Content, "Which file?")
	}) {
		t.Fatalf("active context missing restored ask_user projection: %#v", second.Observation.ActiveContext)
	}
	if !slices.ContainsFunc(second.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Role == "user" && msg.Content == "main.go"
	}) {
		t.Fatalf("active context missing restored append message: %#v", second.Observation.ActiveContext)
	}

	afterRestartAgain := NewRunner(root)
	afterRestartAgain.Now = fixedClock()
	after := afterRestartAgain.AgentSessions(context.Background())
	if len(after) != 1 || after[0].ID != first.SessionID || len(after[0].Turns) != 2 || after[0].Status != "completed" {
		t.Fatalf("after = %#v", after)
	}
}

func TestRunnerAgentSessionsKeepCreatedOrderAcrossMemoryAndDisk(t *testing.T) {
	root := t.TempDir()
	clock := fixedClock()
	firstRunner := NewRunner(root)
	firstRunner.Now = clock
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("first"), harness.Done())), nil
	}
	first := firstRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "first",
		SystemPrompt: "test",
	})
	if first.Status != "completed" {
		t.Fatalf("first = %#v", first)
	}

	secondRunner := NewRunner(root)
	secondRunner.Now = clock
	secondRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("second"), harness.Done())), nil
	}
	second := secondRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "second",
		SystemPrompt: "test",
	})
	if second.Status != "completed" {
		t.Fatalf("second = %#v", second)
	}

	listRunner := NewRunner(root)
	listRunner.Now = fixedClock()
	sessions := listRunner.AgentSessions(context.Background())
	if len(sessions) != 2 || sessions[0].ID != second.SessionID || sessions[1].ID != first.SessionID {
		t.Fatalf("sessions = %#v", sessions)
	}
	if sessions[0].Title == "" || sessions[1].Title == "" {
		t.Fatalf("sessions should expose generated titles: %#v", sessions)
	}

	listRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("updated first"), harness.Done())), nil
	}
	updatedFirst := listRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "continue first"})
	if updatedFirst.Status != "completed" {
		t.Fatalf("updated first = %#v", updatedFirst)
	}
	sessions = listRunner.AgentSessions(context.Background())
	if len(sessions) != 2 || sessions[0].ID != second.SessionID || sessions[1].ID != first.SessionID {
		t.Fatalf("updated session should not reorder created-time list: %#v", sessions)
	}
}

func TestRunnerRestoredSessionRehydratesSavedAPIKeyWithoutPersistingSecret(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "live",
		Profiles: []ProviderProfile{{
			ID:       "live",
			Name:     "Live",
			Provider: config.ProviderOpenAICompatible,
			Model:    "model-a",
			BaseURL:  "https://example.test/v1",
			APIKey:   "secret",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	runner.ProviderFactory = func(cfg config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Tool("ask", "ask_user", `{"question":"Need file?"}`), harness.Done())), nil
	}
	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		ProfileID:    "live",
		Message:      "start",
		SystemPrompt: "test",
	})
	if first.Status != "waiting" {
		t.Fatalf("first = %#v", first)
	}
	metadata, err := runner.loadAgentSessionMetadata(first.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metadataJSON), "secret") {
		t.Fatalf("metadata leaked API key:\n%s", string(metadataJSON))
	}

	restored := NewRunner(root)
	restored.Now = fixedClock()
	var seenKey string
	restored.ProviderFactory = func(cfg config.Config) (provider.Provider, error) {
		seenKey = cfg.APIKey
		return nil, errors.New("stop before provider call")
	}
	result := restored.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "main.go"})
	if result.Status != "error" || !strings.Contains(result.Error, "stop before provider call") {
		t.Fatalf("result = %#v", result)
	}
	if seenKey != "secret" {
		t.Fatalf("restored API key = %q", seenKey)
	}
}

func TestRunnerRejectsMalformedAgentSessionMetadata(t *testing.T) {
	runner := NewRunner(t.TempDir())
	for _, tt := range []struct {
		name      string
		sessionID string
		meta      agentSessionMetadata
		wantErr   string
	}{
		{
			name:      "missing id",
			sessionID: "session-a",
			meta:      agentSessionMetadata{Version: agentSessionMetadataVersion},
			wantErr:   "metadata id is required",
		},
		{
			name:      "mismatched id",
			sessionID: "session-a",
			meta:      agentSessionMetadata{Version: agentSessionMetadataVersion, ID: "session-b"},
			wantErr:   "does not match requested session",
		},
		{
			name:      "zero version",
			sessionID: "session-a",
			meta:      agentSessionMetadata{ID: "session-a"},
			wantErr:   "unsupported agent session metadata version",
		},
		{
			name:      "future version",
			sessionID: "session-a",
			meta:      agentSessionMetadata{Version: agentSessionMetadataVersion + 1, ID: "session-a"},
			wantErr:   "unsupported agent session metadata version",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := runner.normalizeAgentSessionMetadata(tt.sessionID, tt.meta); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestRunnerPersistedInterruptedTurnSnapshotsAsInterrupted(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.StorageMode = StorageModeFile
	runner.Now = fixedClock()
	sessionID := "testui-session-interrupted"
	turnID := "turn-1"
	started := runner.now()
	agentProfile, promptIdentity := promptMetadataForTest("test")
	repo := sessiontree.NewFileRepo(runner.agentSessionTreeRoot())
	if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: sessionID, CreatedAt: started, UpdatedAt: started}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(context.Background(), repo, sessionID, turnID, sessiontree.TurnStarted, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(context.Background(), repo, sessionID, turnID, session.Message{Role: session.User, Content: "start"}); err != nil {
		t.Fatal(err)
	}
	if err := runner.saveAgentSessionMetadata(agentSessionMetadata{
		ID:             sessionID,
		CreatedAt:      started,
		UpdatedAt:      started,
		Profile:        ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		AgentProfile:   agentProfile,
		PromptIdentity: promptIdentity,
		SystemPrompt:   "test",
		ContextPolicy:  contextPolicyForTest(8192),
		Engine: agentSessionEngine{
			MaxEmptyProviderRetries: 1,
			NoProgressLimit:         2,
			DuplicateToolLimit:      3,
			WallTime:                60 * time.Second,
		},
		Turns: []AgentTurnSummary{{
			ID:        turnID,
			Status:    "running",
			StartedAt: started,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	restored := NewRunner(root)
	restored.StorageMode = StorageModeFile
	restored.Now = fixedClock()
	sessions := restored.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].Status != "interrupted" || !sessions[0].Recoverable || sessions[0].CanAppendMessage {
		t.Fatalf("sessions = %#v", sessions)
	}
	snapshot, err := restored.AgentSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "interrupted" || !snapshot.Recoverable || snapshot.CanAppendMessage {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	appendResult := restored.RunAgentTurn(context.Background(), sessionID, AgentTurnRequest{Message: "again"})
	if appendResult.Status != "error" || appendResult.StatusCode != 409 || !strings.Contains(appendResult.Error, "cannot accept") {
		t.Fatalf("appendResult = %#v", appendResult)
	}
}

func TestRunnerPersistedAbortedTurnSnapshotsAsCancelled(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.StorageMode = StorageModeFile
	runner.Now = fixedClock()
	sessionID := "testui-session-cancelled"
	turnID := "turn-1"
	started := runner.now()
	agentProfile, promptIdentity := promptMetadataForTest("test")
	repo := sessiontree.NewFileRepo(runner.agentSessionTreeRoot())
	if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: sessionID, CreatedAt: started, UpdatedAt: started}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(context.Background(), repo, sessionID, turnID, sessiontree.TurnStarted, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(context.Background(), repo, sessionID, turnID, sessiontree.TurnAborted, nil); err != nil {
		t.Fatal(err)
	}
	if err := runner.saveAgentSessionMetadata(agentSessionMetadata{
		ID:             sessionID,
		CreatedAt:      started,
		UpdatedAt:      started,
		Profile:        ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		AgentProfile:   agentProfile,
		PromptIdentity: promptIdentity,
		SystemPrompt:   "test",
		ContextPolicy:  contextPolicyForTest(8192),
		Engine: agentSessionEngine{
			MaxEmptyProviderRetries: 1,
			NoProgressLimit:         2,
			DuplicateToolLimit:      3,
			WallTime:                60 * time.Second,
		},
		Turns: []AgentTurnSummary{{
			ID:        turnID,
			Status:    string(engine.Cancelled),
			StartedAt: started,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	restored := NewRunner(root)
	restored.StorageMode = StorageModeFile
	snapshot, err := restored.AgentSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != string(engine.Cancelled) || snapshot.Recoverable || snapshot.CanAppendMessage {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestRunnerRepairsStaleThreadLeafAndMetadataTurnsFromJournal(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.StorageMode = StorageModeFile
	runner.Now = fixedClock()
	sessionID := "testui-session-stale-leaf"
	turnID := "turn-1"
	started := runner.now()
	agentProfile, promptIdentity := promptMetadataForTest("test")
	repo := sessiontree.NewFileRepo(runner.agentSessionTreeRoot())
	if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: sessionID, CreatedAt: started, UpdatedAt: started}); err != nil {
		t.Fatal(err)
	}
	user, err := sessiontree.AppendMessage(context.Background(), repo, sessionID, turnID, session.Message{Role: session.User, Content: "start"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(context.Background(), repo, sessionID, turnID, sessiontree.TurnCompleted, map[string]string{"finish_reason": "stop"}); err != nil {
		t.Fatal(err)
	}
	threadPath := filepath.Join(runner.agentSessionTreeRoot(), safeSessionFileName(sessionID), "thread.json")
	threadData, err := os.ReadFile(threadPath)
	if err != nil {
		t.Fatal(err)
	}
	var thread sessiontree.ThreadMeta
	if err := json.Unmarshal(threadData, &thread); err != nil {
		t.Fatal(err)
	}
	thread.LeafID = user.ID
	thread.UpdatedAt = user.CreatedAt
	staleThreadData, err := json.Marshal(thread)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(threadPath, staleThreadData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.saveAgentSessionMetadata(agentSessionMetadata{
		ID:             sessionID,
		CreatedAt:      started,
		UpdatedAt:      started,
		Profile:        ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		AgentProfile:   agentProfile,
		PromptIdentity: promptIdentity,
		SystemPrompt:   "test",
		ContextPolicy:  contextPolicyForTest(8192),
		Engine: agentSessionEngine{
			MaxEmptyProviderRetries: 1,
			NoProgressLimit:         2,
			DuplicateToolLimit:      3,
			WallTime:                60 * time.Second,
		},
	}); err != nil {
		t.Fatal(err)
	}

	restored := NewRunner(root)
	restored.StorageMode = StorageModeFile
	restored.Now = fixedClock()
	sessions := restored.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].Status != string(engine.Completed) || !sessions[0].CanAppendMessage || len(sessions[0].Turns) != 1 {
		t.Fatalf("sessions = %#v", sessions)
	}
	snapshot, err := restored.AgentSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != string(engine.Completed) || snapshot.LeafID == user.ID || len(snapshot.Turns) != 1 || snapshot.Turns[0].Status != string(engine.Completed) {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	meta, err := restored.loadAgentSessionMetadata(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Turns) != 1 || meta.Turns[0].Status != string(engine.Completed) || !meta.UpdatedAt.After(started) {
		t.Fatalf("metadata was not refreshed from journal: %#v", meta)
	}
}

func TestRunnerAgentSessionCanContinueAfterCompletedTurn(t *testing.T) {
	root := t.TempDir()
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Text("first done"), harness.Done()),
		harness.Step(harness.Text("second done"), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "first",
		SystemPrompt: "test",
	})
	if first.Status != "completed" || !first.CanAppendMessage {
		t.Fatalf("first = %#v", first)
	}

	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "second"})
	if second.Status != "completed" || second.Output != "second done" {
		t.Fatalf("second = %#v", second)
	}
	if second.TurnID == first.TurnID || len(second.Session.Turns) != 2 {
		t.Fatalf("second session = %#v", second.Session)
	}
	if second.Session.AggregateMetrics.LLMRequests != 2 {
		t.Fatalf("aggregate metrics = %#v", second.Session.AggregateMetrics)
	}
	if !slices.ContainsFunc(second.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.Content == "first done"
	}) {
		t.Fatalf("active context missing previous assistant output: %#v", second.Observation.ActiveContext)
	}
}

func TestRunnerDeleteAgentSessionRemovesMetadataTreeAndPromptCache(t *testing.T) {
	root := t.TempDir()
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Text("first done"), harness.Done()),
		harness.Step(harness.Text("second done"), harness.Done()),
	)
	runner := NewRunner(root)
	runner.StorageMode = StorageModeFile
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "first",
		SystemPrompt: "test",
	})
	if first.Status != "completed" {
		t.Fatalf("first = %#v", first)
	}
	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "second"})
	if second.Status != "completed" {
		t.Fatalf("second = %#v", second)
	}
	if first.TurnID == "" || second.TurnID == "" || first.TurnID == second.TurnID {
		t.Fatalf("turn ids = %q, %q", first.TurnID, second.TurnID)
	}
	for _, path := range []string{
		runner.agentSessionMetadataPath(first.SessionID),
		filepath.Join(runner.agentSessionTreeRoot(), safeSessionFileName(first.SessionID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(first.SessionID)),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected persisted session artifact %s: %v", path, err)
		}
	}

	if err := runner.DeleteAgentSession(context.Background(), first.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.AgentSession(context.Background(), first.SessionID); err == nil || !isMissingAgentSessionError(err) {
		t.Fatalf("AgentSession err = %v, want missing", err)
	}
	if sessions := runner.AgentSessions(context.Background()); len(sessions) != 0 {
		t.Fatalf("sessions = %#v", sessions)
	}
	for _, path := range []string{
		runner.agentSessionMetadataPath(first.SessionID),
		filepath.Join(runner.agentSessionTreeRoot(), safeSessionFileName(first.SessionID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(first.SessionID)),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact %s still exists or returned unexpected err: %v", path, err)
		}
	}
}

func TestRunnerDeleteRestoredAgentSessionRemovesPromptCacheWithoutProvider(t *testing.T) {
	root := t.TempDir()
	firstProvider := harness.NewScriptedProvider(
		harness.Step(harness.Text("first done"), harness.Done()),
		harness.Step(harness.Text("second done"), harness.Done()),
	)
	firstRunner := NewRunner(root)
	firstRunner.StorageMode = StorageModeFile
	firstRunner.Now = fixedClock()
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return firstProvider, nil
	}

	first := firstRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "first",
		SystemPrompt: "test",
	})
	if first.Status != "completed" {
		t.Fatalf("first = %#v", first)
	}
	second := firstRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "second"})
	if second.Status != "completed" {
		t.Fatalf("second = %#v", second)
	}

	restored := NewRunner(root)
	restored.StorageMode = StorageModeFile
	restored.Now = fixedClock()
	restored.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return nil, errors.New("delete should not build provider runtime")
	}
	if err := restored.DeleteAgentSession(context.Background(), first.SessionID); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		restored.agentSessionMetadataPath(first.SessionID),
		filepath.Join(restored.agentSessionTreeRoot(), safeSessionFileName(first.SessionID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(first.SessionID)),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact %s still exists or returned unexpected err: %v", path, err)
		}
	}
}

func TestRunnerDefaultSQLitePersistsListsRestoresAndDeletesSession(t *testing.T) {
	root := t.TempDir()
	firstProvider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Continue?"}`), harness.Done()),
	)
	firstRunner := NewRunner(root)
	firstRunner.Now = fixedClock()
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return firstProvider, nil
	}
	first := firstRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "start",
		SystemPrompt:  "test",
		SelectedTools: []string{"grep"},
	})
	if first.Status != "waiting" {
		t.Fatalf("first = %#v", first)
	}
	if _, err := os.Stat(sqlite.DefaultTestUIPath(root)); err != nil {
		t.Fatalf("default sqlite db missing: %v", err)
	}
	if _, err := os.Stat(firstRunner.agentSessionMetadataPath(first.SessionID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default sqlite should not write JSON metadata file, err=%v", err)
	}

	secondProvider := harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done()))
	secondRunner := NewRunner(root)
	secondRunner.Now = fixedClock()
	secondRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return secondProvider, nil
	}
	sessions := secondRunner.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].ID != first.SessionID || !slices.Equal(sessions[0].SelectedTools, []string{"grep"}) {
		t.Fatalf("restarted sessions = %#v", sessions)
	}
	opened, err := secondRunner.AgentSession(context.Background(), first.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if opened.ID != first.SessionID || opened.Status != "waiting" || !opened.CanAppendMessage {
		t.Fatalf("opened = %#v", opened)
	}
	second := secondRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "yes"})
	if second.Status != "completed" || second.Output != "done" {
		t.Fatalf("second = %#v", second)
	}
	if err := secondRunner.DeleteAgentSession(context.Background(), first.SessionID); err != nil {
		t.Fatal(err)
	}
	if sessions := secondRunner.AgentSessions(context.Background()); len(sessions) != 0 {
		t.Fatalf("sessions after delete = %#v", sessions)
	}
	if _, err := secondRunner.AgentSession(context.Background(), first.SessionID); err == nil || !isMissingAgentSessionError(err) {
		t.Fatalf("AgentSession after delete err = %v", err)
	}
}

func TestRunnerInterfaceProbeDoesNotOpenDefaultSQLite(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	result := runner.RunInterfaceProbe(context.Background(), AgentInterfaceProbeRequest{
		SelectedTools: []string{"list"},
		ContextPolicy: contextPolicyForTest(8192),
	})
	if !result.Probe || result.Status != "completed" {
		t.Fatalf("probe result = %#v", result)
	}
	if _, err := os.Stat(sqlite.DefaultTestUIPath(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transient probe should not create sqlite db, err=%v", err)
	}
	if runner.storageSQLite != nil {
		t.Fatalf("transient probe opened sqlite store")
	}
}

func TestRunnerAgentSessionHandlesMultipleToolCallsAndFollowUpUserTurn(t *testing.T) {
	root := t.TempDir()
	contentServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Changsha detailed page: thunderstorms with uncertainty."))
	}))
	defer contentServer.Close()
	contentURL := contentServer.URL + "/changsha-weather"
	searchServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"web":{"results":[{"title":"Changsha weather","url":%q,"description":"Thunderstorms forecast.","profile":{"name":"Weather Source"}}]}}`, contentURL)))
	}))
	defer searchServer.Close()
	scripted := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Search then inspect a known URL with bounded shell curl."},
			harness.Text("I will check sources."),
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "search-1", Name: "web_search", Args: `{"query":"Changsha weather 2026-06-03","count":3}`},
				{ID: "curl-1", Name: "shell", Args: boundedCurlArgs(contentURL)},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("Changsha has thunderstorms."), harness.Done()),
		harness.Step(harness.Text("Sources were search and shell results."), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	if err := os.WriteFile(filepath.Join(root, config.DefaultEnvFile), []byte("FLORET_BRAVE_SEARCH_API_KEY=test-key\nFLORET_BRAVE_SEARCH_ENDPOINT="+searchServer.URL+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       fakeExternalBraveSearchProfile(),
		Message:       "查询长沙天气",
		SystemPrompt:  "test",
		SelectedTools: []string{"web_search", "shell"},
	})
	if first.Status != "completed" || !strings.Contains(first.Output, "Changsha") {
		t.Fatalf("first = %#v", first)
	}
	if len(first.Observation.ProviderRequests) != 2 {
		t.Fatalf("provider requests = %#v", first.Observation.ProviderRequests)
	}
	if !slices.ContainsFunc(first.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == "web_search" && msg.ToolCallID == "search-1" && strings.Contains(msg.Reasoning, "Search then inspect")
	}) {
		t.Fatalf("web_search call missing from session messages: %#v", first.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(first.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == "shell" && msg.ToolCallID == "curl-1" && strings.Contains(msg.Reasoning, "Search then inspect")
	}) {
		t.Fatalf("shell curl call missing from session messages: %#v", first.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(first.Observation.ProviderRequests[1].Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "tool" && msg.ToolName == "web_search" && msg.ToolCallID == "search-1"
	}) || !slices.ContainsFunc(first.Observation.ProviderRequests[1].Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "tool" && msg.ToolName == "shell" && msg.ToolCallID == "curl-1"
	}) {
		t.Fatalf("follow-up request missing tool results: %#v", first.Observation.ProviderRequests[1].Messages)
	}

	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "请给出信息来源和不确定性"})
	if second.Status != "completed" || second.Output != "Sources were search and shell results." || len(second.Session.Turns) != 2 {
		t.Fatalf("second = %#v", second)
	}
	latestRequest := second.Observation.ProviderRequests[len(second.Observation.ProviderRequests)-1]
	if !slices.ContainsFunc(latestRequest.Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "user" && msg.Content == "请给出信息来源和不确定性"
	}) {
		t.Fatalf("second turn request missing appended user message: %#v", latestRequest.Messages)
	}
}

func TestRunnerAgentSessionHandlesRepeatedWeatherToolBatchesAndFollowUp(t *testing.T) {
	root := t.TempDir()
	searchServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"web":{"results":[{"title":"Changsha weather","url":"https://example.com/weather","description":"Rain in Changsha.","profile":{"name":"Weather Source"}}]}}`))
	}))
	defer searchServer.Close()
	if err := os.WriteFile(filepath.Join(root, config.DefaultEnvFile), []byte("FLORET_BRAVE_SEARCH_API_KEY=test-key\nFLORET_BRAVE_SEARCH_ENDPOINT="+searchServer.URL+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	contentServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/china-weather":
			_, _ = w.Write([]byte("Changsha June 3: rain. June 4: cloudy then clear."))
		case "/wttr":
			_, _ = w.Write([]byte("Changsha: showers, 81F"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer contentServer.Close()
	scripted := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "try search"},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "search-1", Name: "web_search", Args: `{"query":"Changsha weather 2026-06-03","count":3}`, Reasoning: "try search"},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "try shell curl calls that fail"},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "curl-bad-00", Name: "shell", Args: boundedCurlArgs(contentServer.URL + "/missing-weather"), Reasoning: "try shell curl calls that fail"},
				{ID: "curl-bad-01", Name: "shell", Args: boundedCurlArgs(contentServer.URL + "/missing-wttr"), Reasoning: "try shell curl calls that fail"},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "retry shell curl with bounded output"},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "curl-good-00", Name: "shell", Args: boundedCurlArgs(contentServer.URL + "/china-weather"), Reasoning: "retry shell curl with bounded output"},
				{ID: "curl-good-01", Name: "shell", Args: boundedCurlArgs(contentServer.URL + "/wttr"), Reasoning: "retry shell curl with bounded output"},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("Changsha weather summary."), harness.Done()),
		harness.Step(harness.Text("Tomorrow is cloudy then clear."), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       fakeExternalBraveSearchProfile(),
		Message:       "今天是2026-06-03，请你获取长沙的天气",
		SystemPrompt:  "test",
		SelectedTools: []string{"web_search", "shell"},
	})
	if first.Status != "completed" || first.Output != "Changsha weather summary." {
		t.Fatalf("first = %#v", first)
	}
	if len(first.Observation.ProviderRequests) != 4 {
		t.Fatalf("provider requests = %#v", first.Observation.ProviderRequests)
	}
	for _, id := range []string{"search-1", "curl-bad-00", "curl-bad-01", "curl-good-00", "curl-good-01"} {
		if got := countObservedToolMessages(first.Observation.SessionMessages, "assistant", id); got != 1 {
			t.Fatalf("assistant tool call %s count = %d in %#v", id, got, first.Observation.SessionMessages)
		}
		if got := countObservedToolMessages(first.Observation.SessionMessages, "tool", id); got != 1 {
			t.Fatalf("tool result %s count = %d in %#v", id, got, first.Observation.SessionMessages)
		}
	}
	if err := assertObservedProviderSafeToolHistory(first.Observation.SessionMessages); err != nil {
		t.Fatalf("first session messages are not provider-safe: %v\n%#v", err, first.Observation.SessionMessages)
	}
	if got := countObservedAssistantContent(first.Observation.SessionMessages, "Changsha weather summary."); got != 1 {
		t.Fatalf("first final assistant count = %d in %#v", got, first.Observation.SessionMessages)
	}
	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "那么明天会天气晴吗，适合出门吗"})
	if second.Status != "completed" || second.Output != "Tomorrow is cloudy then clear." {
		t.Fatalf("second = %#v", second)
	}
	latestRequest := second.Observation.ProviderRequests[len(second.Observation.ProviderRequests)-1]
	if err := assertObservedProviderSafeToolHistory(latestRequest.Messages); err != nil {
		t.Fatalf("follow-up request messages are not provider-safe: %v\n%#v", err, latestRequest.Messages)
	}
	if got := countObservedAssistantContent(second.Observation.SessionMessages, "Changsha weather summary."); got != 1 {
		t.Fatalf("second snapshot duplicated first final assistant: count=%d in %#v", got, second.Observation.SessionMessages)
	}
	if got := countObservedAssistantContent(latestRequest.Messages, "Changsha weather summary."); got != 1 {
		t.Fatalf("follow-up request duplicated first final assistant: count=%d in %#v", got, latestRequest.Messages)
	}
}

func TestRunnerAgentSessionRegistryIsStableForLiteralRunner(t *testing.T) {
	root := t.TempDir()
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Need value?"}`), harness.Done()),
		harness.Step(harness.Text("literal resumed"), harness.Done()),
	)
	runner := Runner{
		Root:    root,
		EnvFile: filepath.Join(root, config.DefaultEnvFile),
		Now:     fixedClock(),
		Exec:    execCommand,
		ProviderFactory: func(config.Config) (provider.Provider, error) {
			return scripted, nil
		},
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "start",
		SystemPrompt: "test",
	})
	if first.Status != "waiting" || first.SessionID == "" {
		t.Fatalf("first = %#v", first)
	}

	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "value"})
	if second.Status != "completed" || second.Output != "literal resumed" {
		t.Fatalf("second = %#v", second)
	}
	if sessions := runner.AgentSessions(context.Background()); len(sessions) != 1 || sessions[0].ID != first.SessionID {
		t.Fatalf("sessions = %#v", sessions)
	}
}

func TestRunnerRejectsAppendWhenSessionCannotAcceptMessage(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Text("failed soon"), harness.Done()),
	)
	scripted.Errs[1] = context.Canceled
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "fail",
		SystemPrompt: "test",
	})
	if first.Status != "cancelled" {
		t.Fatalf("first = %#v", first)
	}

	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "should reject"})
	if second.Status != "error" || second.StatusCode != 409 || !strings.Contains(second.Error, "cannot accept") {
		t.Fatalf("second = %#v", second)
	}
}

func TestRunnerRejectsAppendWhileSessionIsBusy(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done"},
		Message:      "start",
		SystemPrompt: "test",
	})
	if first.Status != "completed" || first.SessionID == "" {
		t.Fatalf("first = %#v", first)
	}
	sess, ok := runner.Sessions.get(first.SessionID)
	if !ok {
		t.Fatalf("session not registered")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()

	busy := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "again"})
	if busy.Status != "error" || busy.StatusCode != 409 || !strings.Contains(busy.Error, "already running") {
		t.Fatalf("busy = %#v", busy)
	}
}

func TestRunnerAgentSessionSnapshotsDoNotBlockOnRunningTurn(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()

	first, err := runner.CreateIdleAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "hello",
		SystemPrompt: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	sess, ok := runner.Sessions.get(first.ID)
	if !ok {
		t.Fatalf("session not registered")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	runner.markAgentSessionRunningLocked(sess, "turn-busy")

	listResult := make(chan []AgentSessionSnapshot, 1)
	go func() {
		listResult <- runner.AgentSessions(context.Background())
	}()
	var sessions []AgentSessionSnapshot
	select {
	case sessions = <-listResult:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("AgentSessions blocked on running session")
	}
	if len(sessions) != 1 || sessions[0].ID != first.ID || !sessionlifecycle.IsRunningStatus(sessions[0].Status, sessions[0].Phase) || sessions[0].CanAppendMessage {
		t.Fatalf("running sessions snapshot = %#v", sessions)
	}
	if sessions[0].LatestTurnID != "turn-busy" {
		t.Fatalf("latest turn id = %q, want turn-busy", sessions[0].LatestTurnID)
	}

	getResult := make(chan AgentSessionSnapshot, 1)
	getErr := make(chan error, 1)
	go func() {
		snapshot, err := runner.AgentSession(context.Background(), first.ID)
		if err != nil {
			getErr <- err
			return
		}
		getResult <- snapshot
	}()
	select {
	case err := <-getErr:
		t.Fatal(err)
	case snapshot := <-getResult:
		if snapshot.ID != first.ID || !sessionlifecycle.IsRunningStatus(snapshot.Status, snapshot.Phase) || snapshot.CanAppendMessage {
			t.Fatalf("running session snapshot = %#v", snapshot)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("AgentSession blocked on running session")
	}
}

func TestRunnerRunningSnapshotUsesRealTurnID(t *testing.T) {
	blocking := newBlockingTestProvider()
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return blocking, nil
	}
	first, err := runner.CreateIdleAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "hello",
		SystemPrompt: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan AgentRunResponse, 1)
	go func() {
		done <- runner.RunAgentTurn(ctx, first.ID, AgentTurnRequest{Message: "hello"})
	}()
	blocking.waitStarted(t)
	snapshot, err := runner.AgentSession(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !sessionlifecycle.IsRunningStatus(snapshot.Status, snapshot.Phase) || snapshot.LatestTurnID != "turn-1" {
		t.Fatalf("running snapshot = %#v", snapshot)
	}
	if !slices.ContainsFunc(snapshot.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-1"
	}) {
		t.Fatalf("running snapshot did not expose persisted user message: %#v", snapshot.PathEntries)
	}
	if len(snapshot.Observation.ProviderRequests) != 1 {
		t.Fatalf("running snapshot should expose provider request observation: %#v", snapshot.Observation)
	}
	if !slices.ContainsFunc(snapshot.Observation.Transitions, func(transition StateTransition) bool {
		return transition.To == "provider_waiting"
	}) {
		t.Fatalf("running snapshot should expose provider waiting transition: %#v", snapshot.Observation.Transitions)
	}
	cancel()
	result := <-done
	if result.Status != string(engine.Cancelled) {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunnerAgentSessionCompactionIsVisibleInActiveContextAndRawSegments(t *testing.T) {
	root := t.TempDir()
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Text("compact summary"), harness.Done()),
		harness.Step(harness.Text("after compact"), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	initial, err := runner.CreateIdleAgentSession(context.Background(), AgentRunRequest{
		Profile: ProviderProfile{
			ID:           "fake",
			Name:         "Fake",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "unused",
		},
		SystemPrompt:  "test",
		ContextPolicy: contextPolicyForTest(1800),
	})
	if err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("history ", 620)
	sess, ok := runner.Sessions.get(initial.ID)
	if !ok {
		t.Fatalf("idle session not registered: %#v", initial)
	}
	if _, err := sessiontree.AppendMessage(context.Background(), sess.repo, initial.ID, "turn-history", session.Message{Role: session.User, Content: long}); err != nil {
		t.Fatal(err)
	}

	result := runner.RunAgentTurn(context.Background(), initial.ID, AgentTurnRequest{Message: "continue"})

	if result.Status != "completed" || result.Output != "after compact" {
		t.Fatalf("result = %#v", result)
	}
	if result.Metrics.Compactions != 1 || result.Session.Compactions != 1 {
		t.Fatalf("compaction metrics result=%#v session=%#v", result.Metrics, result.Session)
	}
	if !slices.ContainsFunc(result.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Kind == "compaction_summary" && msg.CompactionID != ""
	}) {
		t.Fatalf("active context missing structured compaction summary: %#v", result.Observation.ActiveContext)
	}
	var compactionEntry ObservedSessionEntry
	if !slices.ContainsFunc(result.Observation.PathEntries, func(entry ObservedSessionEntry) bool {
		if entry.Type == "compaction" && entry.CompactionID != "" && entry.CompactionGeneration > 0 && len(entry.KeptUserEntryIDs) > 0 {
			compactionEntry = entry
			return true
		}
		return false
	}) {
		t.Fatalf("path entries missing compaction metadata: %#v", result.Observation.PathEntries)
	}
	if compactionEntry.ContextUsageBefore.OutputHeadroom == 0 || compactionEntry.ContextUsageBefore.AutoCompactRatio != contextpolicy.DefaultAutoCompactRatioPercent {
		t.Fatalf("compaction entry missing before budget usage: %#v", compactionEntry.ContextUsageBefore)
	}
	if compactionEntry.ContextUsageAfter.OutputHeadroom == 0 || compactionEntry.ContextUsageAfter.AutoCompactRatio != contextpolicy.DefaultAutoCompactRatioPercent {
		t.Fatalf("compaction entry missing after budget usage: %#v", compactionEntry.ContextUsageAfter)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests, func(request ObservedProviderRequest) bool {
		return slices.ContainsFunc(request.RawSegments, func(segment ObservedRawSegment) bool {
			return segment.Kind == "compaction" &&
				segment.PromptScopeID == result.SessionID &&
				segment.CreatedByRunID != "" &&
				segment.CreatedByTurnID == result.TurnID &&
				segment.EntryID != "" &&
				segment.CompactionGeneration > 0 &&
				segment.CompactionWindowID != ""
		})
	}) {
		t.Fatalf("provider request missing compaction segment: %#v", result.Observation.ProviderRequests)
	}
	if !slices.ContainsFunc(result.Session.ContextStatuses, func(status ObservedContextStatus) bool {
		return status.Phase == observation.ContextPhaseProjectedRequest &&
			status.ThreadID == result.SessionID &&
			status.TurnID == result.TurnID &&
			status.RequestID != "" &&
			status.ContextPressure.ContextWindowTokens == 1800
	}) {
		t.Fatalf("session missing projected context status: %#v", result.Session.ContextStatuses)
	}
	if !slices.ContainsFunc(result.Session.ContextStatuses, func(status ObservedContextStatus) bool {
		return status.Phase == observation.ContextPhaseProviderUsage &&
			status.ThreadID == result.SessionID &&
			status.TurnID == result.TurnID &&
			status.RequestID != "" &&
			status.ContextPressure.ContextWindowTokens == 1800
	}) {
		t.Fatalf("session missing provider usage context status: %#v", result.Session.ContextStatuses)
	}
	if !slices.ContainsFunc(result.Session.CompactionEvents, func(event ObservedCompactionEvent) bool {
		return event.Phase == engine.ContextCompactPhaseComplete &&
			event.Status == observation.CompactionStatusCompacted &&
			event.CompactionID == compactionEntry.CompactionID &&
			event.CompactionGeneration == compactionEntry.CompactionGeneration &&
			event.CompactionWindowID == compactionEntry.CompactionWindowID &&
			event.TokensBefore == compactionEntry.TokensBefore &&
			event.TokensAfterEstimate == compactionEntry.TokensAfterEstimate
	}) {
		t.Fatalf("session missing compaction event: %#v", result.Session.CompactionEvents)
	}

	reopened := NewRunner(root)
	reopened.Now = fixedClock()
	reopened.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(), nil
	}
	snapshot, err := reopened.AgentSession(context.Background(), result.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(snapshot.ContextStatuses, func(status ObservedContextStatus) bool {
		return status.Phase == observation.ContextPhaseProjectedRequest &&
			status.ThreadID == result.SessionID &&
			status.TurnID == result.TurnID &&
			status.RequestID != "" &&
			status.ContextPressure.ContextWindowTokens == 1800
	}) {
		t.Fatalf("reopened snapshot missing projected context status: %#v", snapshot.ContextStatuses)
	}
	if !slices.ContainsFunc(snapshot.ContextStatuses, func(status ObservedContextStatus) bool {
		return status.Phase == observation.ContextPhaseProviderUsage &&
			status.ThreadID == result.SessionID &&
			status.TurnID == result.TurnID &&
			status.RequestID != "" &&
			status.ContextPressure.ContextWindowTokens == 1800
	}) {
		t.Fatalf("reopened snapshot missing provider usage context status: %#v", snapshot.ContextStatuses)
	}
	if !slices.ContainsFunc(snapshot.Observation.ProviderRequests, func(request ObservedProviderRequest) bool {
		return request.ThreadID == result.SessionID &&
			request.PromptScopeID == result.SessionID &&
			request.TurnID == result.TurnID &&
			request.RequestEstimate.EstimatedInputTokens > 0 &&
			request.ProjectedPressure.ContextWindowTokens == 1800 &&
			slices.ContainsFunc(request.RawSegments, func(segment ObservedRawSegment) bool {
				return segment.Kind == "compaction" &&
					segment.PromptScopeID == result.SessionID &&
					segment.CreatedByTurnID == result.TurnID &&
					segment.CompactionGeneration == compactionEntry.CompactionGeneration &&
					segment.CompactionWindowID == compactionEntry.CompactionWindowID
			})
	}) {
		t.Fatalf("reopened snapshot missing provider request observation: %#v", snapshot.Observation.ProviderRequests)
	}
	if !slices.ContainsFunc(snapshot.CompactionEvents, func(event ObservedCompactionEvent) bool {
		return event.Phase == engine.ContextCompactPhaseComplete &&
			event.Status == observation.CompactionStatusCompacted &&
			event.CompactionID == compactionEntry.CompactionID &&
			event.CompactionGeneration == compactionEntry.CompactionGeneration &&
			event.CompactionWindowID == compactionEntry.CompactionWindowID &&
			event.TokensBefore == compactionEntry.TokensBefore &&
			event.TokensAfterEstimate == compactionEntry.TokensAfterEstimate
	}) {
		t.Fatalf("reopened snapshot missing compaction event: %#v", snapshot.CompactionEvents)
	}
}

func TestRunnerRunAgentUsesUnsavedProfileSnapshot(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "saved",
		Profiles: []ProviderProfile{{
			ID:           "saved",
			Name:         "Saved",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "saved-response",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	result := runner.RunAgent(context.Background(), AgentRunRequest{
		ProfileID: "saved",
		Profile: ProviderProfile{
			ID:           "saved",
			Name:         "Unsaved",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "unsaved-response",
		},
		Message:      "hello",
		SystemPrompt: "test",
	})

	if result.Status != "completed" || result.Output != "unsaved-response" {
		t.Fatalf("result did not use unsaved profile snapshot: %#v", result)
	}
}

func TestObserveRawSegmentsMarksTruncatedRaw(t *testing.T) {
	largeRaw := strings.Repeat("x", maxObservedRawSegmentBytes+8)

	segments := observeRawSegments(cache.RawPlan{Segments: []cache.Segment{{
		ID:         "large",
		Kind:       cache.SegmentSystem,
		SHA256:     "abc",
		ByteLength: len(largeRaw),
		Raw:        largeRaw,
	}}})

	if len(segments) != 1 {
		t.Fatalf("segments = %#v", segments)
	}
	if !segments[0].RawTruncated {
		t.Fatalf("RawTruncated = false, want true")
	}
	if len(segments[0].Raw) <= maxObservedRawSegmentBytes || !strings.Contains(segments[0].Raw, "truncated in test UI response") {
		t.Fatalf("raw was not bounded with marker: len=%d raw=%q", len(segments[0].Raw), segments[0].Raw)
	}
	if segments[0].RawPreview == "" || len(segments[0].RawPreview) > 260 {
		t.Fatalf("preview not bounded: %q", segments[0].RawPreview)
	}
}

func TestObservingProviderForwardsPromptRenderer(t *testing.T) {
	inner := rendererProvider{}
	observed := newObservingProvider(inner)

	raw, fragment, err := observed.MessageRaw(cache.SegmentUserMessage, session.Message{Role: session.User, Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if raw != `{"role":"user","content":"hello"}` || fragment != cache.FragmentOpenAIMessage {
		t.Fatalf("MessageRaw = %q, %q", raw, fragment)
	}
	raw, fragment, err = observed.ToolRaw(cache.ToolDefinition{Name: "read"})
	if err != nil {
		t.Fatal(err)
	}
	if raw != `{"type":"function","function":{"name":"read"}}` || fragment != cache.FragmentOpenAITool {
		t.Fatalf("ToolRaw = %q, %q", raw, fragment)
	}
}

func TestObservingProviderRuntimeOnlyExposesExistingEstimator(t *testing.T) {
	noEstimator := newObservingProvider(harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done())))
	if _, ok := observedProviderRuntime(noEstimator).(provider.TokenEstimator); ok {
		t.Fatalf("observing provider should not add token estimator capability")
	}
	withEstimator := newObservingProvider(estimatingTestProvider{Provider: harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))})
	estimator, ok := observedProviderRuntime(withEstimator).(provider.TokenEstimator)
	if !ok {
		t.Fatalf("observing provider should forward existing token estimator capability")
	}
	estimate, err := estimator.EstimateTokens(context.Background(), provider.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if estimate.Source != "test_estimator" {
		t.Fatalf("estimate = %#v", estimate)
	}
	if estimate.Method != provider.TokenEstimateProviderRenderedPayload {
		t.Fatalf("estimate method = %#v, want %q", estimate.Method, provider.TokenEstimateProviderRenderedPayload)
	}
}

func TestContextStatusFromProviderRequestUsesProjectedPressure(t *testing.T) {
	req := ObservedProviderRequest{
		RunID:            "turn-1",
		ThreadID:         "thread-1",
		PromptScopeID:    "thread-1",
		TurnID:           "turn-1",
		Step:             2,
		LogicalRequestID: "logical-1",
		Attempt:          2,
		Provider:         "fake",
		Model:            "fake-model",
		ObservedAt:       time.Unix(10, 0),
		RequestEstimate: contextpolicy.RequestEstimate{
			EstimatedInputTokens: 910,
			Source:               "test_estimator",
			Method:               contextpolicy.EstimateMethodProviderRenderedPayload,
			Confidence:           contextpolicy.EstimateApproximate,
		},
		ProjectedPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 910,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			RequestSafeLimit:     900,
			HardLimitExceeded:    true,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
			Signal:               contextpolicy.PressureSignalProjected,
		},
		CacheSummary: ObservedCacheSummary{
			CompactionGeneration: 3,
			CompactionWindowID:   "window-3",
		},
	}

	status := contextStatusFromProviderRequest(req)

	if status.Phase != observation.ContextPhaseProjectedRequest ||
		status.RequestID != "turn-1:req:2" ||
		status.LogicalRequestID != "logical-1" ||
		status.Attempt != 2 ||
		status.Status != engine.ContextStatusHardLimit ||
		status.UsedRatio != 0.91 ||
		status.ThresholdRatio != 0.8 ||
		status.CompactionGeneration != 3 ||
		status.CompactionWindowID != "window-3" {
		t.Fatalf("context status = %#v", status)
	}
}

func TestContextStatusFromFinalProviderUsageEvent(t *testing.T) {
	ev := event.Event{
		Type:      event.ProviderUsage,
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		Step:      1,
		Provider:  "fake",
		Model:     "fake-model",
		Timestamp: time.Unix(20, 0),
		Metadata: engine.ProviderUsageContextStatus{
			Phase:            engine.ProviderUsagePhaseFinalContextStatus,
			RequestID:        "turn-1:req:1",
			LogicalRequestID: "logical-1",
			Attempt:          1,
			Usage: provider.Usage{
				InputTokens:       400,
				WindowInputTokens: 420,
				OutputTokens:      30,
				Source:            provider.UsageNative,
				Available:         true,
			},
			RequestEstimate: contextpolicy.RequestEstimate{EstimatedInputTokens: 390},
			ContextPressure: contextpolicy.ContextPressure{
				WindowInputTokens:   420,
				ContextWindowTokens: 1000,
				ThresholdTokens:     800,
				Source:              contextpolicy.PressureSourceProviderUsage,
				Signal:              contextpolicy.PressureSignalNativeUsage,
			},
			UsedRatio:            0.42,
			ThresholdRatio:       0.8,
			Status:               engine.ContextStatusStable,
			CompactionGeneration: 1,
			CompactionWindowID:   "window-1",
		},
	}

	status, ok := contextStatusFromEngineEvent(ev)

	if !ok {
		t.Fatalf("final provider usage context status was not converted")
	}
	if status.Phase != observation.ContextPhaseProviderUsage ||
		status.TurnID != "turn-1" ||
		status.RequestID != "turn-1:req:1" ||
		status.Provider != "fake" ||
		status.Usage.WindowInputTokens != 420 ||
		status.ContextPressure.WindowInputTokens != 420 ||
		status.UsedRatio != 0.42 ||
		status.ThresholdRatio != 0.8 ||
		status.Status != engine.ContextStatusStable ||
		status.CompactionGeneration != 1 ||
		status.CompactionWindowID != "window-1" {
		t.Fatalf("status = %#v", status)
	}
	streamUsage := ev
	streamUsage.Metadata = map[string]any{"phase": engine.ProviderUsagePhaseStreamUsage}
	if _, ok := contextStatusFromEngineEvent(streamUsage); ok {
		t.Fatalf("stream usage phase should not become a final context status")
	}
}

func TestContextStatusesFromPromptRecordsSkipResponseWithoutPressure(t *testing.T) {
	created := time.Unix(25, 0)
	statuses := contextStatusesFromPromptRecords([]cache.ProviderRequestRecord{{
		ID:               "turn-1:req:1",
		PromptScopeID:    "thread-1",
		RunID:            "turn-1",
		ThreadID:         "thread-1",
		TurnID:           "turn-1",
		Step:             1,
		LogicalRequestID: "logical-1",
		Attempt:          1,
		Provider:         "fake",
		Model:            "fake-model",
		RequestEstimate:  contextpolicy.RequestEstimate{EstimatedInputTokens: 320},
		ProjectedPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 320,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
		},
		CreatedAt: created,
	}}, []cache.ProviderResponseRecord{{
		RequestID: "turn-1:req:1",
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		CreatedAt: created.Add(time.Second),
	}})

	if len(statuses) != 1 ||
		statuses[0].Phase != observation.ContextPhaseProjectedRequest ||
		statuses[0].RequestID != "turn-1:req:1" {
		t.Fatalf("response without pressure should only keep projected status: %#v", statuses)
	}
}

func TestAgentObservationMergesPromptCacheContextStatusesWithLiveObservation(t *testing.T) {
	req := ObservedProviderRequest{
		RunID:         "turn-1",
		ThreadID:      "thread-1",
		PromptScopeID: "thread-1",
		TurnID:        "turn-1",
		Step:          1,
		Provider:      "fake",
		Model:         "fake-model",
		ObservedAt:    time.Unix(10, 0),
		CacheSummary: ObservedCacheSummary{
			CompactionGeneration: 2,
			CompactionWindowID:   "window-2",
		},
		ProjectedPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 500,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
		},
	}
	promptCacheReq := req
	promptCacheReq.ObservedAt = time.Unix(9, 0)
	projected := contextStatusFromProviderRequest(promptCacheReq)
	finalUsage := ObservedContextStatus{
		RunID:                "turn-1",
		ThreadID:             "thread-1",
		TurnID:               "turn-1",
		Step:                 1,
		RequestID:            "turn-1:req:1",
		Phase:                observation.ContextPhaseProviderUsage,
		Provider:             "fake",
		Model:                "fake-model",
		ObservedAt:           time.Unix(11, 0),
		ContextPressure:      config.ContextPressure{WindowInputTokens: 650, ContextWindowTokens: 1000, ThresholdTokens: 800},
		UsedRatio:            0.65,
		ThresholdRatio:       0.8,
		Status:               engine.ContextStatusStable,
		CompactionGeneration: 2,
		CompactionWindowID:   "window-2",
	}

	runner := NewRunner(t.TempDir())
	sess := &agentSession{
		provider: &observingProvider{reqs: []ObservedProviderRequest{req}},
		recorder: &streamingEventRecorder{},
	}
	got := runner.agentObservationLocked(sess, AgentSessionSnapshot{
		ContextStatuses: []ObservedContextStatus{projected, finalUsage},
	}, engine.Result{}, "turn-1")

	if len(got.ContextStatuses) != 2 {
		t.Fatalf("context statuses = %#v", got.ContextStatuses)
	}
	if !slices.ContainsFunc(got.ContextStatuses, func(status ObservedContextStatus) bool {
		return status.Phase == observation.ContextPhaseProviderUsage &&
			status.RequestID == "turn-1:req:1" &&
			status.ContextPressure.WindowInputTokens == 650
	}) {
		t.Fatalf("prompt-cache provider usage status was not preserved: %#v", got.ContextStatuses)
	}
	if count := countContextStatuses(got.ContextStatuses, observation.ContextPhaseProjectedRequest, "turn-1:req:1"); count != 1 {
		t.Fatalf("projected status count = %d, statuses = %#v", count, got.ContextStatuses)
	}
}

func TestPromptCacheObservationReadsOnlySelectedPromptScope(t *testing.T) {
	ctx := context.Background()
	store := cache.NewMemoryStore()
	created := time.Unix(26, 0)
	foreignReq := cache.ProviderRequestRecord{
		ID:               "turn-1:req:1",
		PromptScopeID:    "thread-b",
		RunID:            "turn-1",
		ThreadID:         "thread-b",
		TurnID:           "turn-1",
		Step:             1,
		LogicalRequestID: "turn-1:logical:1",
		Provider:         "fake",
		Model:            "fake-model",
		RequestEstimate:  contextpolicy.RequestEstimate{EstimatedInputTokens: 308},
		ProjectedPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 308,
			ContextWindowTokens:  256000,
			ThresholdTokens:      192000,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
		},
		CreatedAt: created,
	}
	if err := store.AppendProviderRequest(ctx, foreignReq); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendProviderResponse(ctx, cache.ProviderResponseRecord{
		RequestID:         foreignReq.ID,
		PromptScopeID:     foreignReq.PromptScopeID,
		RunID:             foreignReq.RunID,
		ThreadID:          foreignReq.ThreadID,
		TurnID:            foreignReq.TurnID,
		WindowInputTokens: 308,
		UsageSource:       string(provider.UsageUnavailable),
		UsageAvailable:    false,
		NativePressure:    foreignReq.ProjectedPressure,
		CreatedAt:         created.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(t.TempDir())
	observation := runner.observationFromPromptCache(ctx, store, "thread-a")

	if len(observation.ProviderRequests) != 0 || len(observation.ContextStatuses) != 0 {
		t.Fatalf("foreign prompt cache observations leaked into thread-a: %#v", observation)
	}
}

func TestCompactionEventsFromEngineEventsAndEntries(t *testing.T) {
	start := event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		Step:      2,
		Timestamp: time.Unix(30, 0),
		Metadata: map[string]any{
			"phase":                  engine.ContextCompactPhaseStart,
			"trigger":                compaction.TriggerPostResponse,
			"reason":                 compaction.ReasonThreshold,
			"message_context_before": contextpolicy.Usage{InputTokens: 850},
			"tokens_before":          int64(850),
		},
	}
	complete := event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		Step:      2,
		Message:   "compact-1",
		Result:    "summary text",
		Timestamp: time.Unix(31, 0),
		Metadata: map[string]any{
			"phase":                      engine.ContextCompactPhaseComplete,
			"trigger":                    compaction.TriggerPostResponse,
			"reason":                     compaction.ReasonThreshold,
			"compaction_id":              "compact-1",
			"compaction_generation":      3,
			"compaction_window_id":       "window-3",
			"compacted_through_entry_id": "entry-7",
			"tokens_before":              int64(850),
			"tokens_after_estimate":      int64(240),
			"context_before":             contextpolicy.Usage{InputTokens: 850},
			"context_after":              contextpolicy.Usage{InputTokens: 240},
		},
	}
	failed := event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		Step:      3,
		Err:       "summary failed",
		Timestamp: time.Unix(32, 0),
		Metadata: map[string]any{
			"phase":                  engine.ContextCompactPhaseFailed,
			"trigger":                compaction.TriggerOverflow,
			"reason":                 compaction.ReasonProviderOverflow,
			"message_context_before": contextpolicy.Usage{InputTokens: 990},
			"tokens_before":          int64(990),
		},
	}

	started, ok := compactionEventFromEngineEvent(start)
	if !ok {
		t.Fatalf("start event was not converted")
	}
	if started.Phase != engine.ContextCompactPhaseStart ||
		started.Status != observation.CompactionStatusRunning ||
		started.Trigger != string(compaction.TriggerPostResponse) ||
		started.Reason != string(compaction.ReasonThreshold) ||
		started.TokensBefore != 850 ||
		started.TurnID != "turn-1" {
		t.Fatalf("start compaction = %#v", started)
	}

	done, ok := compactionEventFromEngineEvent(complete)
	if !ok {
		t.Fatalf("complete event was not converted")
	}
	if done.Phase != engine.ContextCompactPhaseComplete ||
		done.Status != observation.CompactionStatusCompacted ||
		done.CompactionID != "compact-1" ||
		done.CompactionGeneration != 3 ||
		done.CompactionWindowID != "window-3" ||
		done.CompactedThroughEntryID != "entry-7" ||
		done.TokensAfterEstimate != 240 ||
		done.SummaryPreview != "summary text" {
		t.Fatalf("complete compaction = %#v", done)
	}

	failedEvent, ok := compactionEventFromEngineEvent(failed)
	if !ok {
		t.Fatalf("failed event was not converted")
	}
	if failedEvent.Phase != engine.ContextCompactPhaseFailed ||
		failedEvent.Status != observation.CompactionStatusFailed ||
		failedEvent.Error != "summary failed" ||
		failedEvent.Trigger != string(compaction.TriggerOverflow) ||
		failedEvent.TokensBefore != 990 {
		t.Fatalf("failed compaction = %#v", failedEvent)
	}
	malformed := start
	malformed.Metadata = map[string]any{"trigger": compaction.TriggerPostResponse}
	if _, ok := compactionEventFromEngineEvent(malformed); ok {
		t.Fatalf("ContextCompact event without explicit phase should not become a DTO")
	}

	entryDone, ok := compactionEventFromEntry(ObservedSessionEntry{
		Type:                    sessiontree.EntryCompaction,
		ThreadID:                "thread-1",
		TurnID:                  "turn-1",
		CreatedAt:               time.Unix(32, 0),
		CompactionID:            "compact-2",
		CompactionGeneration:    4,
		CompactionWindowID:      "window-4",
		CompactedThroughEntryID: "entry-9",
		Summary:                 "entry summary",
		CompactionTrigger:       string(compaction.TriggerOverflow),
		CompactionReason:        string(compaction.ReasonProviderOverflow),
		TokensBefore:            990,
		TokensAfterEstimate:     300,
	})
	if !ok {
		t.Fatalf("compaction entry was not converted")
	}
	if entryDone.Status != observation.CompactionStatusCompacted ||
		entryDone.CompactionGeneration != 4 ||
		entryDone.CompactionWindowID != "window-4" ||
		entryDone.Trigger != string(compaction.TriggerOverflow) ||
		entryDone.Reason != string(compaction.ReasonProviderOverflow) ||
		entryDone.TokensBefore != 990 ||
		entryDone.TokensAfterEstimate != 300 {
		t.Fatalf("entry compaction = %#v", entryDone)
	}
}

func TestObservingProviderStreamsProjectedContextStatus(t *testing.T) {
	inner := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	observed := newObservingProvider(inner)
	stream := newAgentStream(8)
	observed.SetStreamSink(stream)

	providerStream, err := observed.Stream(context.Background(), provider.Request{
		RunID:         "turn-1",
		ThreadID:      "thread-1",
		TurnID:        "turn-1",
		PromptScopeID: "thread-1",
		Step:          1,
		Provider:      "fake",
		Model:         "fake-model",
		RequestEstimate: contextpolicy.RequestEstimate{
			EstimatedInputTokens: 500,
			Source:               "test_estimator",
			Method:               contextpolicy.EstimateMethodGenericPayload,
			Confidence:           contextpolicy.EstimateConservative,
		},
		ContextPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 500,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range providerStream {
	}

	first := <-stream.Events()
	second := <-stream.Events()
	if first.Type != AgentStreamProviderRequest || first.ProviderRequest == nil {
		t.Fatalf("first stream event = %#v", first)
	}
	if second.Type != AgentStreamContextStatus || second.ContextStatus == nil {
		t.Fatalf("second stream event = %#v", second)
	}
	if second.ContextStatus.Phase != observation.ContextPhaseProjectedRequest ||
		second.ContextStatus.ThreadID != "thread-1" ||
		second.ContextStatus.TurnID != "turn-1" ||
		second.ContextStatus.RequestID != "turn-1:req:1" ||
		second.ContextStatus.UsedRatio != 0.5 {
		t.Fatalf("projected stream status = %#v", second.ContextStatus)
	}
}

func TestStreamingEventRecorderStreamsFinalContextStatusAndCompaction(t *testing.T) {
	stream := newAgentStream(8)
	rec := &streamingEventRecorder{}
	rec.SetStreamSink(stream)
	finalUsage := event.Event{
		Type:      event.ProviderUsage,
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		Step:      1,
		Timestamp: time.Unix(40, 0),
		Metadata: engine.ProviderUsageContextStatus{
			Phase:           engine.ProviderUsagePhaseFinalContextStatus,
			RequestID:       "turn-1:req:1",
			ContextPressure: contextpolicy.ContextPressure{WindowInputTokens: 420, ContextWindowTokens: 1000, ThresholdTokens: 800},
			UsedRatio:       0.42,
			ThresholdRatio:  0.8,
			Status:          engine.ContextStatusStable,
		},
	}
	compactionEvent := event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		Step:      2,
		Timestamp: time.Unix(41, 0),
		Result:    "summary",
		Metadata: map[string]any{
			"phase":                 engine.ContextCompactPhaseComplete,
			"trigger":               compaction.TriggerPostResponse,
			"reason":                compaction.ReasonThreshold,
			"compaction_id":         "compact-1",
			"compaction_generation": int64(2),
			"compaction_window_id":  "window-2",
			"tokens_before":         int64(850),
			"tokens_after_estimate": int64(240),
		},
	}

	rec.Emit(finalUsage)
	rec.Emit(compactionEvent)

	statusEvent := <-stream.Events()
	compactEvent := <-stream.Events()
	if statusEvent.Type != AgentStreamContextStatus ||
		statusEvent.ContextStatus == nil ||
		statusEvent.ContextStatus.Phase != observation.ContextPhaseProviderUsage ||
		statusEvent.ContextStatus.UsedRatio != 0.42 {
		t.Fatalf("status stream event = %#v", statusEvent)
	}
	if compactEvent.Type != AgentStreamContextCompaction ||
		compactEvent.Compaction == nil ||
		compactEvent.Compaction.CompactionID != "compact-1" ||
		compactEvent.Compaction.CompactionGeneration != 2 ||
		compactEvent.Compaction.CompactionWindowID != "window-2" ||
		compactEvent.Compaction.Status != observation.CompactionStatusCompacted ||
		compactEvent.Compaction.TokensAfterEstimate != 240 {
		t.Fatalf("compaction stream event = %#v", compactEvent)
	}
}

func TestStreamingEventRecorderStreamsFailedCompaction(t *testing.T) {
	stream := newAgentStream(4)
	rec := &streamingEventRecorder{}
	rec.SetStreamSink(stream)
	rec.Emit(event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		ThreadID:  "thread-1",
		TurnID:    "turn-1",
		Step:      2,
		Timestamp: time.Unix(42, 0),
		Err:       "summary failed",
		Metadata: map[string]any{
			"phase":         engine.ContextCompactPhaseFailed,
			"trigger":       compaction.TriggerOverflow,
			"reason":        compaction.ReasonProviderOverflow,
			"tokens_before": int64(980),
		},
	})

	got := <-stream.Events()
	if got.Type != AgentStreamContextCompaction ||
		got.Compaction == nil ||
		got.Compaction.Phase != engine.ContextCompactPhaseFailed ||
		got.Compaction.Status != observation.CompactionStatusFailed ||
		got.Compaction.Error != "summary failed" ||
		got.Compaction.TokensBefore != 980 {
		t.Fatalf("failed compaction stream event = %#v", got)
	}
}

func TestObservingProviderRecordsHostedToolsSeparatelyFromLocalTools(t *testing.T) {
	inner := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	observed := newObservingProvider(inner)
	stream, err := observed.Stream(context.Background(), provider.Request{
		RunID: "run",
		Step:  1,
		Tools: []provider.ToolDefinition{{
			Name:        "read",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
			Strict:      true,
		}},
		HostedTools: []provider.HostedToolDefinition{{
			Name:    "web_search",
			Type:    "web_search",
			Options: map[string]any{"limit": 3},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range stream {
	}
	snapshot := observed.Snapshot()
	if len(snapshot.ProviderRequests) != 1 {
		t.Fatalf("requests = %#v", snapshot.ProviderRequests)
	}
	req := snapshot.ProviderRequests[0]
	if len(req.Tools) != 1 || req.Tools[0].Name != "read" {
		t.Fatalf("local tools = %#v", req.Tools)
	}
	if len(req.HostedTools) != 1 || req.HostedTools[0].Name != "web_search" || req.HostedTools[0].Type != "web_search" {
		t.Fatalf("hosted tools = %#v", req.HostedTools)
	}
}

type rendererProvider struct{}

type estimatingTestProvider struct {
	provider.Provider
}

func (estimatingTestProvider) EstimateTokens(context.Context, provider.Request) (provider.TokenEstimate, error) {
	return provider.TokenEstimate{
		EstimatedInputTokens: 1,
		Source:               "test_estimator",
		Method:               provider.TokenEstimateProviderRenderedPayload,
		Confidence:           provider.EstimateConservative,
	}, nil
}

func (rendererProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	ch := make(chan provider.StreamEvent)
	close(ch)
	return ch, nil
}

func (rendererProvider) MessageRaw(_ cache.SegmentKind, msg session.Message) (string, string, error) {
	return `{"role":"` + string(msg.Role) + `","content":"` + msg.Content + `"}`, cache.FragmentOpenAIMessage, nil
}

func (rendererProvider) ToolRaw(def cache.ToolDefinition) (string, string, error) {
	return `{"type":"function","function":{"name":"` + def.Name + `"}}`, cache.FragmentOpenAITool, nil
}

func fixedClock() func() time.Time {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	return func() time.Time {
		now = now.Add(250 * time.Millisecond)
		return now
	}
}

func contextPolicyForTest(window int64) config.ContextPolicy {
	return config.ContextPolicy{
		ContextWindowTokens:   window,
		MaxOutputTokens:       32,
		ReservedOutputTokens:  32,
		ReservedSummaryTokens: 32,
		RecentTailTokens:      32,
	}
}

func promptMetadataForTest(systemPrompt string) (config.AgentProfile, config.PromptIdentity) {
	cfg := config.ResolvePrompt(config.Config{SystemPrompt: systemPrompt})
	return cfg.AgentProfile, cfg.PromptIdentity
}

func toolSelection(names ...string) *[]string {
	selected := append([]string(nil), names...)
	return &selected
}

func fakeExternalBraveSearchProfile() ProviderProfile {
	return ProviderProfile{
		ID:       "fake",
		Name:     "Fake",
		Provider: config.ProviderFake,
		Model:    "fake-model",
		WebSearch: searchcap.Capability{
			Source: searchcap.WebSearchExternalBrave,
			Brave:  searchcap.BraveConfig{Provider: searchcap.ExternalProviderBrave},
		},
	}
}

func countContextStatuses(statuses []ObservedContextStatus, phase string, requestID string) int {
	count := 0
	for _, status := range statuses {
		if status.Phase == phase && status.RequestID == requestID {
			count++
		}
	}
	return count
}

func toolOptionByName(t *testing.T, options []AgentToolOption, name string) AgentToolOption {
	t.Helper()
	for _, option := range options {
		if option.Name == name {
			return option
		}
	}
	t.Fatalf("tool option %q not found in %#v", name, options)
	return AgentToolOption{}
}

func countObservedToolMessages(messages []ObservedSessionMessage, role, callID string) int {
	count := 0
	for _, msg := range messages {
		if msg.Role == role && msg.ToolCallID == callID {
			count++
		}
	}
	return count
}

func countObservedAssistantContent(messages []ObservedSessionMessage, content string) int {
	count := 0
	for _, msg := range messages {
		if msg.Role == "assistant" && msg.ToolCallID == "" && msg.Content == content {
			count++
		}
	}
	return count
}

func assertObservedProviderSafeToolHistory(messages []ObservedSessionMessage) error {
	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		if msg.Role == "tool" {
			return fmt.Errorf("orphan tool result %q at %d", msg.ToolCallID, i)
		}
		if msg.Role != "assistant" || msg.ToolCallID == "" || msg.ToolName == "" {
			continue
		}
		var calls []ObservedSessionMessage
		for i < len(messages) && messages[i].Role == "assistant" && messages[i].ToolCallID != "" && messages[i].ToolName != "" {
			calls = append(calls, messages[i])
			i++
		}
		for _, call := range calls {
			if i >= len(messages) {
				return fmt.Errorf("missing result for %q", call.ToolCallID)
			}
			result := messages[i]
			if result.Role != "tool" {
				return fmt.Errorf("got %q before result for %q", result.Role, call.ToolCallID)
			}
			if result.ToolCallID != call.ToolCallID {
				return fmt.Errorf("result %q does not match call %q", result.ToolCallID, call.ToolCallID)
			}
			i++
		}
		i--
	}
	return nil
}

func fakeHTTPMCPServer(t *testing.T, toolName, resultText string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		switch req["method"] {
		case "initialize", "notifications/initialized":
			writeTestRPC(w, id, map[string]any{"protocolVersion": mcp.ProtocolVersion})
		case "tools/list":
			writeTestRPC(w, id, map[string]any{"tools": []map[string]any{{
				"name":        toolName,
				"description": "Lookup from fake MCP.",
				"inputSchema": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{"query": map[string]any{"type": "string"}},
					"required":             []string{"query"},
					"additionalProperties": false,
				},
			}}})
		case "tools/call":
			writeTestRPC(w, id, map[string]any{"content": []map[string]any{{"type": "text", "text": resultText}}})
		default:
			writeTestRPC(w, id, map[string]any{})
		}
	}))
}

func writeMCPConfig(t *testing.T, root string, servers []map[string]any) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"servers": servers})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "mcp.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeEnv(t *testing.T, root, text string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, config.DefaultEnvFile), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeTestRPC(w http.ResponseWriter, id any, result map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}
