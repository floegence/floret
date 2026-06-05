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
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/sqlitestore"
)

func TestRunnerParsesGoTestJSON(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(strings.Join([]string{
			`{"Action":"run","Package":"github.com/floegence/floret/engine","Test":"TestOne"}`,
			`{"Action":"pass","Package":"github.com/floegence/floret/engine","Test":"TestOne","Elapsed":0.01}`,
			`{"Action":"run","Package":"github.com/floegence/floret/engine","Test":"TestSkip"}`,
			`{"Action":"skip","Package":"github.com/floegence/floret/engine","Test":"TestSkip","Elapsed":0.01}`,
			`{"Action":"pass","Package":"github.com/floegence/floret/engine","Elapsed":0.03}`,
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
		return []byte(`{"Action":"fail","Package":"github.com/floegence/floret/engine","Elapsed":0.01}`), 1
	}

	result := runner.Run(context.Background(), TargetRace)

	if result.Status != "fail" || result.ExitCode != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.Error == "" {
		t.Fatalf("expected command error")
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
	if len(result.Parts) != 5 {
		t.Fatalf("parts = %d, want unit, race, eval, tool scenarios, provider smoke", len(result.Parts))
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

func TestRunnerToolCatalogReflectsWebSearchCapability(t *testing.T) {
	t.Run("fake default unavailable", func(t *testing.T) {
		runner := NewRunner(t.TempDir())
		state, err := runner.ConfigState()
		if err != nil {
			t.Fatal(err)
		}
		option := toolOptionByName(t, state.Tools, "web_search")
		if option.Available || option.Source != "disabled" || !strings.Contains(option.Unavailable, "not enabled") {
			t.Fatalf("web_search option = %#v", option)
		}
	})

	t.Run("provider hosted", func(t *testing.T) {
		runner := NewRunner(t.TempDir())
		state, err := runner.SaveConfigState(SaveConfigRequest{
			ActiveProfileID: "hosted",
			Profiles: []ProviderProfile{{
				ID:       "hosted",
				Name:     "Hosted",
				Provider: config.ProviderOpenAICompatible,
				Model:    "model-a",
				WebSearch: searchcap.Capability{ProviderHosted: searchcap.ProviderHostedConfig{
					Enabled:             true,
					WireShape:           searchcap.WireShapeOpenAIChatWebSearchOptions,
					SupportedWireShapes: []string{searchcap.WireShapeOpenAIChatWebSearchOptions},
				}},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		option := toolOptionByName(t, state.Tools, "web_search")
		if !option.Available || option.Source != "provider-hosted" || option.WireShape != searchcap.WireShapeOpenAIChatWebSearchOptions {
			t.Fatalf("web_search option = %#v", option)
		}
	})

	t.Run("client key gates availability", func(t *testing.T) {
		root := t.TempDir()
		runner := NewRunner(root)
		state, err := runner.SaveConfigState(SaveConfigRequest{
			ActiveProfileID: "client",
			Profiles: []ProviderProfile{{
				ID:        "client",
				Name:      "Client",
				Provider:  config.ProviderFake,
				Model:     "fake-model",
				WebSearch: searchcap.Capability{Client: searchcap.ClientConfig{Enabled: true, Provider: searchcap.ClientProviderBrave}},
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
			ActiveProfileID: "client",
			Profiles: []ProviderProfile{{
				ID:        "client",
				Name:      "Client",
				Provider:  config.ProviderFake,
				Model:     "fake-model",
				WebSearch: searchcap.Capability{Client: searchcap.ClientConfig{Enabled: true, Provider: searchcap.ClientProviderBrave}},
			}},
			SearchProvider: SaveSearchProvider{Provider: "brave", APIKey: "search-key"},
		})
		if err != nil {
			t.Fatal(err)
		}
		option = toolOptionByName(t, state.Tools, "web_search")
		if !option.Available || option.Source != "client:brave" {
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
			ID:       "bad",
			Name:     "Bad",
			Provider: config.ProviderOpenAICompatible,
			Model:    "model-a",
			WebSearch: searchcap.Capability{ProviderHosted: searchcap.ProviderHostedConfig{
				Enabled:             true,
				WireShape:           "bad_shape",
				SupportedWireShapes: []string{"bad_shape"},
			}},
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
			segment.Raw == "" &&
			segment.RawPreview == "" &&
			segment.Fingerprint != "" &&
			segment.SchemaVersion != "" &&
			segment.AdapterVersion != "" &&
			segment.Sequence > 0
	}) {
		t.Fatalf("raw segment ledger fields not exposed: %#v", result.Observation.ProviderRequests[0].RawSegments)
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
		SelectedTools: []string{"read", "list", "glob", "grep", "apply_patch", "edit", "write", "shell"},
	})
	if coding.Status != "completed" || len(coding.Session.SelectedTools) != 8 {
		t.Fatalf("coding = %#v", coding)
	}
	for _, name := range []string{"read", "list", "glob", "grep", "apply_patch", "edit", "write", "shell"} {
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
	runner.AllowDebugRaw = true
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "inspect",
		SystemPrompt:  "test",
		SelectedTools: []string{"list"},
		DebugRaw:      true,
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

func TestRunnerAgentSessionCanExecuteClientWebSearch(t *testing.T) {
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
	runner.AllowDebugRaw = true
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       fakeClientSearchProfile(),
		Message:       "查询长沙天气",
		SystemPrompt:  "test",
		SelectedTools: []string{"web_search"},
		DebugRaw:      true,
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
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(
			harness.Step(
				provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: provider.ToolCall{ID: "hosted-search-1", Name: "web_search", Args: `{"query":"Changsha weather"}`}},
				provider.StreamEvent{Type: provider.HostedToolResult, ToolCall: provider.ToolCall{ID: "hosted-search-1", Name: "web_search"}, Text: "provider-hosted result"},
				harness.Text("Hosted search answered."),
				harness.Done(),
			),
		), nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile: ProviderProfile{
			ID:       "hosted",
			Name:     "Hosted",
			Provider: config.ProviderOpenAICompatible,
			Model:    "model",
			APIKey:   "secret",
			WebSearch: searchcap.Capability{ProviderHosted: searchcap.ProviderHostedConfig{
				Enabled:             true,
				WireShape:           searchcap.WireShapeOpenAIChatWebSearchOptions,
				SupportedWireShapes: []string{searchcap.WireShapeOpenAIChatWebSearchOptions},
			}},
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
	if len(req.HostedTools) != 1 || req.HostedTools[0].Name != "web_search" || req.HostedTools[0].Type != "web_search" || req.HostedTools[0].Options != nil {
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
	for _, secret := range []string{`{"query":"Changsha weather"}`, "provider-hosted result", "secret"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("default hosted run response exposed raw value %q: %s", secret, body)
		}
	}
	if strings.Contains(string(body), "system token=hosted-secret") || strings.Contains(string(body), `"Options":{`) || strings.Contains(string(body), `"Parameters":{`) {
		t.Fatalf("default hosted run response exposed system prompt or hosted payload options: %s", body)
	}
	if len(result.Session.HostedTools) != 1 || result.Session.HostedTools[0].Parameters != nil || result.Session.HostedTools[0].Options != nil {
		t.Fatalf("public session snapshot exposed hosted payload details: %#v", result.Session.HostedTools)
	}
	if result.Session.SystemPrompt != "" {
		t.Fatalf("public session snapshot exposed system prompt: %#v", result.Session.SystemPrompt)
	}
}

func TestRunnerAgentPublicObservationSanitizesRawDataUnlessDebugRaw(t *testing.T) {
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

	public := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "inspect token=abc",
		SystemPrompt:  "system token=abc",
		SelectedTools: []string{"list"},
	})
	if public.Status != "completed" {
		t.Fatalf("public = %#v", public)
	}
	publicBody, err := json.Marshal(public)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"secret reasoning token=abc", "public answer token=abc", `{"path":null,"limit":2}`, "README", "go.mod"} {
		if strings.Contains(string(publicBody), raw) {
			t.Fatalf("public response exposed raw value %q: %s", raw, publicBody)
		}
	}
	if !slices.ContainsFunc(public.Observation.ProviderRequests[0].RawSegments, func(segment ObservedRawSegment) bool {
		return segment.Raw == "" && segment.RawPreview == "" && segment.SHA256 != ""
	}) {
		t.Fatalf("public raw segments should keep ledger hashes without raw text: %#v", public.Observation.ProviderRequests[0].RawSegments)
	}

	deniedRaw := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "inspect token=abc",
		SystemPrompt:  "system token=abc",
		SelectedTools: []string{"list"},
		DebugRaw:      true,
	})
	deniedBody, err := json.Marshal(deniedRaw)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"secret reasoning token=abc", "public answer token=abc", `{"path":null,"limit":2}`, "README", "go.mod"} {
		if strings.Contains(string(deniedBody), raw) {
			t.Fatalf("debug raw request without capability exposed raw value %q: %s", raw, deniedBody)
		}
	}

	rawRunner := NewRunner(t.TempDir())
	rawRunner.Now = fixedClock()
	rawRunner.AllowDebugRaw = true
	rawRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return newProvider(), nil
	}
	debug := rawRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "inspect token=abc",
		SystemPrompt:  "system token=abc",
		SelectedTools: []string{"list"},
		DebugRaw:      true,
	})
	debugBody, err := json.Marshal(debug)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(debugBody), "secret reasoning token=abc") {
		t.Fatalf("debug raw response should expose reasoning for local inspection: %s", debugBody)
	}
	if !slices.ContainsFunc(debug.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.ToolCallID == "list-1" && msg.ToolArgs == `{"path":null,"limit":2}`
	}) {
		t.Fatalf("debug raw session messages should expose tool args: %#v", debug.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(debug.Observation.ProviderEvents, func(ev ObservedProviderEvent) bool {
		return len(ev.ToolCalls) == 1 && ev.ToolCalls[0].Args == `{"path":null,"limit":2}`
	}) {
		t.Fatalf("debug raw provider events should expose tool args: %#v", debug.Observation.ProviderEvents)
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
		t.Fatalf("old tool still exposed after restore: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if !slices.ContainsFunc(second.Session.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "active_tools_change" &&
			entry.Metadata["previous_tools"] == "grep" &&
			entry.Metadata["selected_tools"] == "read,shell" &&
			entry.Metadata["reason"] != "restore test" &&
			strings.HasPrefix(entry.Metadata["reason"], "[redacted]")
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
		ContextPolicy: contextpolicy.Policy{
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

func TestRunnerAgentSessionMigratesLegacyToolMode(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	result := runner.RunAgent(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "ok"},
		Message:      "hello",
		SystemPrompt: "test",
		ToolMode:     "read_only",
	})
	if result.Status != "completed" {
		t.Fatalf("result = %#v", result)
	}
	if !slices.Equal(result.Session.SelectedTools, []string{"read", "list", "glob", "grep"}) {
		t.Fatalf("selected tools = %#v", result.Session.SelectedTools)
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
	runner.AllowDebugRaw = true
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
		DebugRaw:     true,
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

	snapshot, err := recoveredRunner.AgentSession(context.Background(), first.SessionID, false)
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

func TestRunnerAgentSessionsSortsNewestFirstAcrossMemoryAndDisk(t *testing.T) {
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

func TestRunnerPersistedInterruptedTurnSnapshotsAsInterrupted(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	sessionID := "testui-session-interrupted"
	turnID := "turn-1"
	started := runner.now()
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
		ID:            sessionID,
		CreatedAt:     started,
		UpdatedAt:     started,
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		SystemPrompt:  "test",
		ContextPolicy: contextPolicyForTest(8192),
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
	restored.Now = fixedClock()
	sessions := restored.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].Status != "interrupted" || !sessions[0].Recoverable || sessions[0].CanAppendMessage {
		t.Fatalf("sessions = %#v", sessions)
	}
	snapshot, err := restored.AgentSession(context.Background(), sessionID, false)
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
	runner.Now = fixedClock()
	sessionID := "testui-session-cancelled"
	turnID := "turn-1"
	started := runner.now()
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
		ID:            sessionID,
		CreatedAt:     started,
		UpdatedAt:     started,
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		SystemPrompt:  "test",
		ContextPolicy: contextPolicyForTest(8192),
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
	snapshot, err := restored.AgentSession(context.Background(), sessionID, false)
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
	runner.Now = fixedClock()
	sessionID := "testui-session-stale-leaf"
	turnID := "turn-1"
	started := runner.now()
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
		ID:            sessionID,
		CreatedAt:     started,
		UpdatedAt:     started,
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		SystemPrompt:  "test",
		ContextPolicy: contextPolicyForTest(8192),
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
	restored.Now = fixedClock()
	sessions := restored.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].Status != string(engine.Completed) || !sessions[0].CanAppendMessage || len(sessions[0].Turns) != 1 {
		t.Fatalf("sessions = %#v", sessions)
	}
	snapshot, err := restored.AgentSession(context.Background(), sessionID, false)
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
	runner.AllowDebugRaw = true
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
	runner.AllowDebugRaw = true
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
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(first.TurnID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(second.TurnID)),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected persisted session artifact %s: %v", path, err)
		}
	}

	if err := runner.DeleteAgentSession(context.Background(), first.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.AgentSession(context.Background(), first.SessionID, false); err == nil || !isMissingAgentSessionError(err) {
		t.Fatalf("AgentSession err = %v, want missing", err)
	}
	if sessions := runner.AgentSessions(context.Background()); len(sessions) != 0 {
		t.Fatalf("sessions = %#v", sessions)
	}
	for _, path := range []string{
		runner.agentSessionMetadataPath(first.SessionID),
		filepath.Join(runner.agentSessionTreeRoot(), safeSessionFileName(first.SessionID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(first.SessionID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(first.TurnID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(second.TurnID)),
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
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(first.TurnID)),
		filepath.Join(root, ".floret-test-ui", "prompt-cache", safeSessionFileName(second.TurnID)),
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
	if _, err := os.Stat(sqlitestore.DefaultTestUIPath(root)); err != nil {
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
	opened, err := secondRunner.AgentSession(context.Background(), first.SessionID, false)
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
	if _, err := secondRunner.AgentSession(context.Background(), first.SessionID, false); err == nil || !isMissingAgentSessionError(err) {
		t.Fatalf("AgentSession after delete err = %v", err)
	}
}

func TestRunnerSQLiteImportsLegacyFileStorageOnce(t *testing.T) {
	root := t.TempDir()
	legacyRunner := NewRunner(root)
	legacyRunner.StorageMode = StorageModeFile
	legacyRunner.Now = fixedClock()
	legacyRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("legacy done"), harness.Done())), nil
	}
	legacy := legacyRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "legacy",
		SystemPrompt: "test",
	})
	if legacy.Status != "completed" {
		t.Fatalf("legacy = %#v", legacy)
	}
	if _, err := os.Stat(legacyRunner.agentSessionMetadataPath(legacy.SessionID)); err != nil {
		t.Fatalf("legacy metadata file missing: %v", err)
	}

	sqliteRunner := NewRunner(root)
	sqliteRunner.Now = fixedClock()
	sessions := sqliteRunner.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].ID != legacy.SessionID || sessions[0].Status != "completed" {
		t.Fatalf("imported sessions = %#v", sessions)
	}
	status := sqliteRunner.storageStatus(context.Background())
	if status.Mode != StorageModeSQLite || status.SchemaVersion != "3" || !strings.Contains(status.LegacyImport, "threads=1") || !strings.Contains(status.LegacyImport, "metadata=1") {
		t.Fatalf("storage status = %#v", status)
	}
	restarted := NewRunner(root)
	restarted.Now = fixedClock()
	if sessions := restarted.AgentSessions(context.Background()); len(sessions) != 1 || sessions[0].ID != legacy.SessionID {
		t.Fatalf("restarted imported sessions = %#v", sessions)
	}
}

func TestRunnerSQLiteSkipsMalformedLegacySessionWithoutHidingValidSession(t *testing.T) {
	root := t.TempDir()
	validRunner := NewRunner(root)
	validRunner.StorageMode = StorageModeFile
	validRunner.Now = fixedClock()
	validRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("valid done"), harness.Done())), nil
	}
	valid := validRunner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:      "valid",
		SystemPrompt: "test",
	})
	if valid.Status != "completed" {
		t.Fatalf("valid = %#v", valid)
	}
	badDir := filepath.Join(validRunner.agentSessionTreeRoot(), "bad")
	if err := os.MkdirAll(badDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "thread.json"), []byte(`{"id":"bad","leaf_id":"bad-entry"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "entries.jsonl"), []byte("{bad json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	metaDir := validRunner.agentSessionMetadataRoot()
	if err := os.WriteFile(filepath.Join(metaDir, "bad.json"), []byte("{bad json\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	sqliteRunner := NewRunner(root)
	sqliteRunner.Now = fixedClock()
	sessions := sqliteRunner.AgentSessions(context.Background())
	if len(sessions) != 1 || sessions[0].ID != valid.SessionID {
		t.Fatalf("valid session should survive malformed legacy data: %#v", sessions)
	}
	status := sqliteRunner.storageStatus(context.Background())
	if !strings.Contains(status.LegacyImport, "skipped=2") {
		t.Fatalf("legacy skipped summary missing: %#v", status)
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
	if _, err := os.Stat(sqlitestore.DefaultTestUIPath(root)); !errors.Is(err, os.ErrNotExist) {
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
	runner.AllowDebugRaw = true
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	if err := os.WriteFile(filepath.Join(root, config.DefaultEnvFile), []byte("FLORET_BRAVE_SEARCH_API_KEY=test-key\nFLORET_BRAVE_SEARCH_ENDPOINT="+searchServer.URL+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	first := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       fakeClientSearchProfile(),
		Message:       "查询长沙天气",
		SystemPrompt:  "test",
		SelectedTools: []string{"web_search", "shell"},
		DebugRaw:      true,
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

	second := runner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "请给出信息来源和不确定性", DebugRaw: true})
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
		Profile:       fakeClientSearchProfile(),
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
	if first.Status != "failed" {
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
		snapshot, err := runner.AgentSession(context.Background(), first.ID, false)
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
	snapshot, err := runner.AgentSession(context.Background(), first.ID, false)
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
	long := strings.Repeat("history ", 220)

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile: ProviderProfile{
			ID:           "fake",
			Name:         "Fake",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "unused",
		},
		Message:       long,
		SystemPrompt:  "test",
		ContextPolicy: contextPolicyForTest(260),
	})

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
	if !slices.ContainsFunc(result.Observation.ActiveContext, func(msg ObservedSessionMessage) bool {
		return msg.Role == "user" && msg.Content == long
	}) {
		t.Fatalf("active context missing kept original user input: %#v", result.Observation.ActiveContext)
	}
	if !slices.ContainsFunc(result.Observation.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "compaction" && entry.CompactionID != "" && entry.CompactionGeneration > 0 && len(entry.KeptUserEntryIDs) > 0
	}) {
		t.Fatalf("path entries missing compaction metadata: %#v", result.Observation.PathEntries)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests, func(request ObservedProviderRequest) bool {
		return slices.ContainsFunc(request.RawSegments, func(segment ObservedRawSegment) bool {
			return segment.Kind == "compaction" &&
				segment.RunID != "" &&
				segment.SessionID == result.SessionID &&
				segment.TurnID == result.TurnID &&
				segment.EntryID != "" &&
				segment.CompactionGeneration > 0 &&
				segment.CompactionWindowID != ""
		})
	}) {
		t.Fatalf("provider request missing compaction segment: %#v", result.Observation.ProviderRequests)
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

	segments := observeRawSegments(promptcache.RawPlan{Segments: []promptcache.Segment{{
		ID:         "large",
		Kind:       promptcache.SegmentSystem,
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

	raw, fragment, err := observed.MessageRaw(promptcache.SegmentUserMessage, session.Message{Role: session.User, Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if raw != `{"role":"user","content":"hello"}` || fragment != promptcache.FragmentOpenAIMessage {
		t.Fatalf("MessageRaw = %q, %q", raw, fragment)
	}
	raw, fragment, err = observed.ToolRaw(promptcache.ToolDefinition{Name: "read"})
	if err != nil {
		t.Fatal(err)
	}
	if raw != `{"type":"function","function":{"name":"read"}}` || fragment != promptcache.FragmentOpenAITool {
		t.Fatalf("ToolRaw = %q, %q", raw, fragment)
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

func (rendererProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	ch := make(chan provider.StreamEvent)
	close(ch)
	return ch, nil
}

func (rendererProvider) MessageRaw(_ promptcache.SegmentKind, msg session.Message) (string, string, error) {
	return `{"role":"` + string(msg.Role) + `","content":"` + msg.Content + `"}`, promptcache.FragmentOpenAIMessage, nil
}

func (rendererProvider) ToolRaw(def promptcache.ToolDefinition) (string, string, error) {
	return `{"type":"function","function":{"name":"` + def.Name + `"}}`, promptcache.FragmentOpenAITool, nil
}

func fixedClock() func() time.Time {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	return func() time.Time {
		now = now.Add(250 * time.Millisecond)
		return now
	}
}

func contextPolicyForTest(window int64) contextpolicy.Policy {
	return contextpolicy.Policy{
		ContextWindowTokens:   window,
		MaxOutputTokens:       32,
		ReservedOutputTokens:  32,
		ReservedSummaryTokens: 32,
		RecentTailTokens:      32,
	}
}

func toolSelection(names ...string) *[]string {
	selected := append([]string(nil), names...)
	return &selected
}

func fakeClientSearchProfile() ProviderProfile {
	return ProviderProfile{
		ID:       "fake",
		Name:     "Fake",
		Provider: config.ProviderFake,
		Model:    "fake-model",
		WebSearch: searchcap.Capability{
			Client: searchcap.ClientConfig{Enabled: true, Provider: searchcap.ClientProviderBrave},
		},
	}
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
