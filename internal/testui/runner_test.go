package testui

import (
	"context"
	"errors"
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
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
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
	if len(result.Parts) != 4 {
		t.Fatalf("parts = %d, want unit, race, eval, provider smoke", len(result.Parts))
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
	metaData, err := os.ReadFile(runner.agentSessionMetadataPath(chat.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(metaData), `"selected_tools": []`) {
		t.Fatalf("metadata should persist empty selected tools: %s", string(metaData))
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
		SelectedTools: []string{"read", "list", "glob", "grep", "apply_patch", "edit", "write", "shell", "web_fetch", "web_search"},
	})
	if coding.Status != "completed" || len(coding.Session.SelectedTools) != 10 {
		t.Fatalf("coding = %#v", coding)
	}
	for _, name := range []string{"read", "list", "glob", "grep", "apply_patch", "edit", "write", "shell", "web_fetch", "web_search"} {
		if !hasStrictTool(coding.Observation.ProviderRequests[0].Tools, name) {
			t.Fatalf("selected tool %s missing: %#v", name, coding.Observation.ProviderRequests[0].Tools)
		}
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
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}

	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
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
	patched, err := firstRunner.UpdateAgentSessionTools(context.Background(), first.SessionID, AgentToolsUpdateRequest{SelectedTools: toolSelection("read", "web_fetch"), Reason: "restore test"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(patched.SelectedTools, []string{"read", "web_fetch"}) {
		t.Fatalf("patched tools = %#v", patched.SelectedTools)
	}

	restoredRunner := NewRunner(root)
	restoredRunner.Now = fixedClock()
	restoredRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done())), nil
	}
	second := restoredRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "yes"})
	if second.Status != "completed" || !slices.Equal(second.Session.SelectedTools, []string{"read", "web_fetch"}) {
		t.Fatalf("second = %#v", second)
	}
	if !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "read") || !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("patched tools missing after restore: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if hasStrictTool(second.Observation.ProviderRequests[0].Tools, "grep") {
		t.Fatalf("old tool still exposed after restore: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if !slices.ContainsFunc(second.Session.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "active_tools_change" &&
			entry.Metadata["previous_tools"] == "grep" &&
			entry.Metadata["selected_tools"] == "read,web_fetch" &&
			entry.Metadata["reason"] == "restore test"
	}) {
		t.Fatalf("tool patch audit entry missing after restore: %#v", second.Session.PathEntries)
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
		SelectedTools: []string{"grep", "web_fetch"},
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
	if len(sessions) != 1 || sessions[0].ID != first.SessionID || !slices.Equal(sessions[0].SelectedTools, []string{"grep", "web_fetch"}) {
		t.Fatalf("sessions = %#v", sessions)
	}
	second := recoveredRunner.RunAgentTurn(context.Background(), first.SessionID, AgentTurnRequest{Message: "continue"})
	if second.Status != "completed" || second.Output != "restored done" {
		t.Fatalf("second = %#v", second)
	}
	if !slices.Equal(second.Session.SelectedTools, []string{"grep", "web_fetch"}) {
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
	if !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "grep") || !hasStrictTool(second.Observation.ProviderRequests[0].Tools, "web_fetch") {
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
	metadata, err := os.ReadFile(runner.agentSessionMetadataPath(first.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metadata), "secret") {
		t.Fatalf("metadata leaked API key:\n%s", string(metadata))
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

func TestRunnerPersistedInterruptedTurnSnapshotsAsCancelled(t *testing.T) {
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
	if len(sessions) != 1 || sessions[0].Status != string(engine.Cancelled) || sessions[0].CanAppendMessage {
		t.Fatalf("sessions = %#v", sessions)
	}
	snapshot, err := restored.AgentSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != string(engine.Cancelled) || snapshot.CanAppendMessage {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	appendResult := restored.RunAgentTurn(context.Background(), sessionID, AgentTurnRequest{Message: "again"})
	if appendResult.Status != "error" || appendResult.StatusCode != 409 || !strings.Contains(appendResult.Error, "cannot accept") {
		t.Fatalf("appendResult = %#v", appendResult)
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
	if !slices.ContainsFunc(result.Observation.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "compaction" && entry.CompactionID != "" && entry.CompactionGeneration > 0
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
