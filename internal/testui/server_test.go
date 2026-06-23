package testui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/catalog"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/observation"
)

func serveInitialAgentSessionTurn(t *testing.T, handler http.Handler, createBody string) *httptest.ResponseRecorder {
	t.Helper()
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody)))
	if createRec.Code != http.StatusOK {
		return createRec
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(createBody), &payload); err != nil {
		t.Fatal(err)
	}
	message, err := json.Marshal(payload.Message)
	if err != nil {
		t.Fatal(err)
	}
	turnRec := httptest.NewRecorder()
	turnPath := "/api/agent/sessions/" + url.PathEscape(snapshot.ID) + "/turns"
	handler.ServeHTTP(turnRec, httptest.NewRequest(http.MethodPost, turnPath, strings.NewReader(`{"message":`+string(message)+`}`)))
	return turnRec
}

func TestServerExposesConfigAndRunAPI(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(`{"Action":"pass","Package":"github.com/floegence/floret","Elapsed":0.01}`), 0
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	configReq := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	configRec := httptest.NewRecorder()
	handler.ServeHTTP(configRec, configReq)
	if configRec.Code != http.StatusOK {
		t.Fatalf("config status = %d", configRec.Code)
	}
	if !strings.Contains(configRec.Body.String(), `"provider":"fake"`) {
		t.Fatalf("config body = %s", configRec.Body.String())
	}
	if !strings.Contains(configRec.Body.String(), `"id":"openai"`) || !strings.Contains(configRec.Body.String(), `"gpt-5.4"`) {
		t.Fatalf("config body missing catalog = %s", configRec.Body.String())
	}
	if !strings.Contains(configRec.Body.String(), `"name":"grep"`) || !strings.Contains(configRec.Body.String(), `"name":"shell"`) || !strings.Contains(configRec.Body.String(), `"name":"web_search"`) || strings.Contains(configRec.Body.String(), `"name":"web_fetch"`) {
		t.Fatalf("config body missing tools = %s", configRec.Body.String())
	}
	if !strings.Contains(configRec.Body.String(), `"search_provider"`) || !strings.Contains(configRec.Body.String(), `"env_key":"FLORET_BRAVE_SEARCH_API_KEY"`) {
		t.Fatalf("config body missing search provider state = %s", configRec.Body.String())
	}
	if !strings.Contains(configRec.Body.String(), `"local_time"`) {
		t.Fatalf("config body missing local time state = %s", configRec.Body.String())
	}
	var configState ConfigState
	if err := json.Unmarshal(configRec.Body.Bytes(), &configState); err != nil {
		t.Fatal(err)
	}
	if configState.LocalTime.Now == "" || !strings.HasPrefix(configState.LocalTime.OffsetLabel, "UTC") {
		t.Fatalf("config local time state is incomplete = %#v", configState.LocalTime)
	}
	if configState.ContextPolicyDefaults.ContextWindowTokens != contextpolicy.DefaultContextWindowTokens || configState.ContextPolicyDefaults.RecentTailTokens != contextpolicy.DefaultRecentTailTokens {
		t.Fatalf("config context policy defaults are incomplete = %#v", configState.ContextPolicyDefaults)
	}
	if strings.Contains(configRec.Body.String(), "debug"+"_raw"+"_enabled") {
		t.Fatalf("config should not expose a debug raw gate = %s", configRec.Body.String())
	}

	catalogReq := httptest.NewRequest(http.MethodGet, "/api/catalog", nil)
	catalogRec := httptest.NewRecorder()
	handler.ServeHTTP(catalogRec, catalogReq)
	if catalogRec.Code != http.StatusOK || !strings.Contains(catalogRec.Body.String(), `"id":"anthropic"`) {
		t.Fatalf("catalog status/body = %d %s", catalogRec.Code, catalogRec.Body.String())
	}

	runReq := httptest.NewRequest(http.MethodPost, "/api/run", bytes.NewBufferString(`{"target":"unit"}`))
	runRec := httptest.NewRecorder()
	handler.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusOK {
		t.Fatalf("run status = %d, body = %s", runRec.Code, runRec.Body.String())
	}
	var result RunResponse
	if err := json.Unmarshal(runRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "pass" || result.Target != TargetUnit {
		t.Fatalf("result = %#v", result)
	}
}

func TestServerRunAPIExposesSavedToolScenarioSuite(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	runReq := httptest.NewRequest(http.MethodPost, "/api/run", bytes.NewBufferString(`{"target":"tool-scenarios"}`))
	runRec := httptest.NewRecorder()
	handler.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusOK {
		t.Fatalf("run status = %d, body = %s", runRec.Code, runRec.Body.String())
	}
	var result RunResponse
	if err := json.Unmarshal(runRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "pass" || result.Target != TargetToolScenarios || len(result.Parts) != 3 {
		t.Fatalf("result = %#v", result)
	}
	for _, part := range result.Parts {
		if part.Agent == nil {
			t.Fatalf("tool scenario part missing agent run: %#v", part)
		}
		var sawToolCall, sawToolResult bool
		for _, ev := range part.Agent.Events {
			switch ev.Type {
			case event.ToolCall:
				sawToolCall = true
				if ev.Args != "" || ev.ArgsHash == "" {
					t.Fatalf("public /api/run should expose only a tool args hash: %#v", ev)
				}
			case event.ToolResult:
				sawToolResult = true
				if ev.Result != "" || ev.Err != "" {
					t.Fatalf("public /api/run should not expose raw tool result details: %#v", ev)
				}
			}
		}
		if !sawToolCall || !sawToolResult {
			t.Fatalf("tool scenario part should include sanitized tool lifecycle events: %#v", part.Agent.Events)
		}
	}
}

func TestServerRunAPILiveToolScenariosUseSelectedSavedProfile(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	recording := &recordingToolScenarioProvider{}
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return recording, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	saveBody := `{"active_profile_id":"hosted-live","profiles":[{"id":"hosted-live","name":"Hosted Live","provider":"anthropic","model":"claude-sonnet-4-6","base_url":"https://api.example.test/v1","api_key":"provider-key","web_search":{"source":"provider_hosted","hosted":{"wire_shape":"anthropic_server_web_search"}}}],"search_provider":{"provider":"brave","api_key":"search-key","endpoint":"https://search.example.test"}}`
	saveReq := httptest.NewRequest(http.MethodPut, "/api/config", strings.NewReader(saveBody))
	saveRec := httptest.NewRecorder()
	handler.ServeHTTP(saveRec, saveReq)
	if saveRec.Code != http.StatusOK {
		t.Fatalf("save status = %d, body = %s", saveRec.Code, saveRec.Body.String())
	}

	runReq := httptest.NewRequest(http.MethodPost, "/api/run", bytes.NewBufferString(`{"target":"live-tool-scenarios","profile_id":"hosted-live"}`))
	runRec := httptest.NewRecorder()
	handler.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusOK {
		t.Fatalf("run status = %d, body = %s", runRec.Code, runRec.Body.String())
	}
	var result RunResponse
	if err := json.Unmarshal(runRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "pass" || result.Target != TargetLiveToolScenarios || len(result.Parts) != 2 {
		t.Fatalf("result = %#v", result)
	}
	for _, part := range result.Parts {
		if part.Agent == nil || part.Agent.Config.Model != "claude-sonnet-4-6" {
			t.Fatalf("live part missing selected profile config: %#v", part)
		}
	}
	requests := recording.Requests()
	if len(requests) < 2 {
		t.Fatalf("live scenario did not call provider enough times: %#v", requests)
	}
	localReq := requests[0]
	for _, name := range []string{"list", "read", "grep"} {
		if !hasProviderTool(localReq.Tools, name) {
			t.Fatalf("local tool scenario should expose %s: %#v", name, localReq.Tools)
		}
	}
	assertProviderToolRequiredFields(t, localReq.Tools, "list")
	assertProviderToolRequiredFields(t, localReq.Tools, "read", "path")
	assertProviderToolRequiredFields(t, localReq.Tools, "grep", "pattern")
	var weatherReq provider.Request
	for _, req := range requests {
		if hasProviderTool(req.Tools, "shell") && len(req.HostedTools) > 0 {
			weatherReq = req
			break
		}
	}
	if hasProviderTool(weatherReq.Tools, "web_search") {
		t.Fatalf("provider-hosted profile should not expose local web_search: %#v", weatherReq.Tools)
	}
	if hasProviderTool(weatherReq.Tools, "web_fetch") {
		t.Fatalf("provider-hosted profile must not expose local web_fetch: %#v", weatherReq.Tools)
	}
	if !hasProviderTool(weatherReq.Tools, "shell") {
		t.Fatalf("provider-hosted profile should expose shell for explicit URL/API access: %#v", weatherReq.Tools)
	}
	if len(weatherReq.HostedTools) != 1 || weatherReq.HostedTools[0].Name != "web_search" {
		t.Fatalf("provider-hosted profile should expose hosted web_search: %#v", weatherReq.HostedTools)
	}
}

func TestServerRunAPILiveToolScenariosTreatWeatherAsDiagnostic(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	recording := &recordingToolScenarioProvider{failWeather: true}
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return recording, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	saveBody := `{"active_profile_id":"hosted-live","profiles":[{"id":"hosted-live","name":"Hosted Live","provider":"anthropic","model":"claude-sonnet-4-6","base_url":"https://api.example.test/v1","api_key":"provider-key","web_search":{"source":"provider_hosted","hosted":{"wire_shape":"anthropic_server_web_search"}}}],"search_provider":{"provider":"brave","api_key":"search-key","endpoint":"https://search.example.test"}}`
	saveReq := httptest.NewRequest(http.MethodPut, "/api/config", strings.NewReader(saveBody))
	saveRec := httptest.NewRecorder()
	handler.ServeHTTP(saveRec, saveReq)
	if saveRec.Code != http.StatusOK {
		t.Fatalf("save status = %d, body = %s", saveRec.Code, saveRec.Body.String())
	}

	runReq := httptest.NewRequest(http.MethodPost, "/api/run", bytes.NewBufferString(`{"target":"live-tool-scenarios","profile_id":"hosted-live"}`))
	runRec := httptest.NewRecorder()
	handler.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusOK {
		t.Fatalf("run status = %d, body = %s", runRec.Code, runRec.Body.String())
	}
	var result RunResponse
	if err := json.Unmarshal(runRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "pass" || !strings.Contains(result.Summary, "external web diagnostic") {
		t.Fatalf("diagnostic failure should not fail required live local suite: %#v", result)
	}
	if len(result.Parts) != 2 || result.Parts[0].Status != "pass" || result.Parts[1].Status == "pass" {
		t.Fatalf("parts = %#v", result.Parts)
	}
}

func TestServerSavesConfigAndRunsAgent(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	saveBody := `{"active_profile_id":"fake","profiles":[{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"server-ok"}],"search_provider":{"provider":"brave","api_key":"search-key","endpoint":"https://search.example.test"}}`
	saveReq := httptest.NewRequest(http.MethodPut, "/api/config", strings.NewReader(saveBody))
	saveRec := httptest.NewRecorder()
	handler.ServeHTTP(saveRec, saveReq)
	if saveRec.Code != http.StatusOK {
		t.Fatalf("save status = %d, body = %s", saveRec.Code, saveRec.Body.String())
	}
	if !strings.Contains(saveRec.Body.String(), `"api_key_set":true`) || !strings.Contains(saveRec.Body.String(), `"endpoint":"https://search.example.test"`) {
		t.Fatalf("save body missing search provider state = %s", saveRec.Body.String())
	}

	resaveBody := `{"active_profile_id":"fake","profiles":[{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"server-ok"}],"search_provider":{"provider":"brave","api_key":"","endpoint":"https://search.example.test/next"}}`
	resaveReq := httptest.NewRequest(http.MethodPut, "/api/config", strings.NewReader(resaveBody))
	resaveRec := httptest.NewRecorder()
	handler.ServeHTTP(resaveRec, resaveReq)
	if resaveRec.Code != http.StatusOK || !strings.Contains(resaveRec.Body.String(), `"api_key_set":true`) || !strings.Contains(resaveRec.Body.String(), `"endpoint":"https://search.example.test/next"`) {
		t.Fatalf("resave status/body = %d %s", resaveRec.Code, resaveRec.Body.String())
	}

	runBody := `{"profile_id":"fake","message":"hello","system_prompt":"test","context_policy":{"context_window_tokens":8192,"max_output_tokens":1024,"recent_tail_tokens":1024}}`
	runReq := httptest.NewRequest(http.MethodPost, "/api/agent/run", strings.NewReader(runBody))
	runRec := httptest.NewRecorder()
	handler.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusOK {
		t.Fatalf("agent status = %d, body = %s", runRec.Code, runRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(runRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Output != "server-ok" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) == 0 || len(result.Observation.Transitions) == 0 {
		t.Fatalf("observation missing: %#v", result.Observation)
	}
}

func TestServerCreatesIdleAgentSessionBeforeInitialTurn(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":["grep","shell"],"context_policy":{"context_window_tokens":8192,"max_output_tokens":1024,"recent_tail_tokens":1024}}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.ID == "" || snapshot.Status != "idle" || len(snapshot.Turns) != 0 || !snapshot.CanAppendMessage {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if !slices.Equal(snapshot.SelectedTools, []string{"grep", "shell"}) {
		t.Fatalf("selected tools = %#v", snapshot.SelectedTools)
	}
	if len(scripted.Requests) != 0 {
		t.Fatalf("create-only session should not call provider: %#v", scripted.Requests)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+snapshot.ID+"/turns", strings.NewReader(`{"message":"hello"}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.SessionID != snapshot.ID || result.Output != "done" || len(result.Session.Turns) != 1 {
		t.Fatalf("result = %#v", result)
	}
}

func TestServerManagesAgentSessionSubAgents(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("child done"), harness.Done()))
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"","system_prompt":"test"}`
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody)))
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var created AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	spawnBody := `{"thread_id":"child","task_name":"Review API","host_profile_ref":"reviewer","fork_mode":"none","message":"review the API"}`
	spawnRec := httptest.NewRecorder()
	handler.ServeHTTP(spawnRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+created.ID+"/subagents", strings.NewReader(spawnBody)))
	if spawnRec.Code != http.StatusOK {
		t.Fatalf("spawn status = %d, body = %s", spawnRec.Code, spawnRec.Body.String())
	}
	var spawned AgentSubAgentActionResponse
	if err := json.Unmarshal(spawnRec.Body.Bytes(), &spawned); err != nil {
		t.Fatal(err)
	}
	if spawned.SubAgent.ThreadID != "child" || spawned.SubAgent.ParentThreadID != created.ID || spawned.SubAgent.TaskName != "review_api" || !strings.Contains(spawned.SubAgent.Path, "review_api") {
		t.Fatalf("spawned = %#v", spawned.SubAgent)
	}

	waitRec := httptest.NewRecorder()
	handler.ServeHTTP(waitRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+created.ID+"/subagents/wait", strings.NewReader(`{"thread_ids":["child"],"timeout_ms":2000}`)))
	if waitRec.Code != http.StatusOK {
		t.Fatalf("wait status = %d, body = %s", waitRec.Code, waitRec.Body.String())
	}
	var waited AgentSubAgentWaitResponse
	if err := json.Unmarshal(waitRec.Body.Bytes(), &waited); err != nil {
		t.Fatal(err)
	}
	if waited.Result.TimedOut || len(waited.Result.Snapshots) != 1 || waited.Result.Snapshots[0].Status != "completed" {
		t.Fatalf("waited = %#v", waited)
	}

	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.ID+"/subagents", nil))
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var listed AgentSubAgentListResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.SubAgents) != 1 || listed.SubAgents[0].LastMessage != "child done" {
		t.Fatalf("listed = %#v", listed)
	}

	closeRec := httptest.NewRecorder()
	handler.ServeHTTP(closeRec, httptest.NewRequest(http.MethodDelete, "/api/agent/sessions/"+created.ID+"/subagents/child", nil))
	if closeRec.Code != http.StatusOK {
		t.Fatalf("close status = %d, body = %s", closeRec.Code, closeRec.Body.String())
	}
	var closed AgentSubAgentActionResponse
	if err := json.Unmarshal(closeRec.Body.Bytes(), &closed); err != nil {
		t.Fatal(err)
	}
	if closed.SubAgent.Status != "closed" || closed.SubAgent.CanSendInput || closed.SubAgent.CanClose {
		t.Fatalf("closed = %#v", closed.SubAgent)
	}
}

func TestServerAgentSessionTurnExposesToolDetailsByDefault(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("shell-1", "shell", `{"command":"printf APPEND_PRIVATE_RESULT","timeout_ms":1000}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte("APPEND_PRIVATE_RESULT"), 0
	}
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"system","selected_tools":["shell"]}`
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody)))
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var created AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+created.ID+"/turns", strings.NewReader(`{"message":"run it"}`)))
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	body := turnRec.Body.String()
	for _, want := range []string{`\"command\":\"printf APPEND_PRIVATE_RESULT\"`, "APPEND_PRIVATE_RESULT"} {
		if !strings.Contains(body, want) {
			t.Fatalf("non-stream turn local inspection missing %q: %s", want, body)
		}
	}
	var result AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.SessionID != created.ID || result.Output != "done" {
		t.Fatalf("turn result = %#v", result)
	}
	if !slices.ContainsFunc(result.Session.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "tool_call" &&
			entry.Message.ToolCallID == "shell-1" &&
			strings.Contains(entry.Message.ToolArgs, "APPEND_PRIVATE_RESULT")
	}) {
		t.Fatalf("turn response should expose tool args by default: %#v", result.Session.PathEntries)
	}
	if !slices.ContainsFunc(result.Session.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "tool_result" &&
			entry.Message.ToolCallID == "shell-1" &&
			strings.Contains(entry.Message.Content, "APPEND_PRIVATE_RESULT")
	}) {
		t.Fatalf("turn response should expose tool result by default: %#v", result.Session.PathEntries)
	}

	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.ID, nil))
	if getRec.Code != http.StatusOK {
		t.Fatalf("session status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(snapshot.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "tool_call" &&
			entry.Message.ToolCallID == "shell-1" &&
			strings.Contains(entry.Message.ToolArgs, "APPEND_PRIVATE_RESULT")
	}) || !slices.ContainsFunc(snapshot.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "tool_result" &&
			entry.Message.ToolCallID == "shell-1" &&
			strings.Contains(entry.Message.Content, "APPEND_PRIVATE_RESULT")
	}) {
		t.Fatalf("GET session should keep non-stream turn tool details visible: %#v", snapshot.PathEntries)
	}
}

func TestServerStreamsAgentTurnEventsBeforeCompletion(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Delta, Text: "looking", Reason: "15ms"},
			harness.Tool("list-1", "list", `{"path":null,"limit":2}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test","selected_tools":["list"]}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+snapshot.ID+"/turns/stream", strings.NewReader(`{"message":"hello"}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("stream status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	events := parseSSEAgentEvents(t, turnRec.Body.String())
	assertStreamEventOrder(t, events,
		AgentStreamTurnStarted,
		AgentStreamUserMessageAppended,
		AgentStreamProviderRequest,
		AgentStreamProviderDelta,
		AgentStreamToolCall,
		AgentStreamToolResult,
		AgentStreamSessionSnapshot,
		AgentStreamTurnCompleted,
	)
	deltaIndex := indexStreamEvent(events, AgentStreamProviderDelta)
	completedIndex := indexStreamEvent(events, AgentStreamTurnCompleted)
	if deltaIndex < 0 || completedIndex < 0 || deltaIndex >= completedIndex {
		t.Fatalf("provider delta should arrive before completion: %#v", events)
	}
	toolCallIndex := indexStreamEventWithEntry(events, AgentStreamToolCall)
	toolResultIndex := indexStreamEventWithEntry(events, AgentStreamToolResult)
	if toolCallIndex < 0 || toolResultIndex < 0 || toolCallIndex >= completedIndex || toolResultIndex >= completedIndex {
		t.Fatalf("tool call/result should stream before completion: %#v", events)
	}
	toolCall := events[toolCallIndex]
	if toolCall.Entry == nil || toolCall.Entry.Type != "tool_call" || toolCall.Entry.Message.ToolName != "list" || toolCall.Entry.Message.ToolCallID != "list-1" || toolCall.Entry.ID == "" {
		t.Fatalf("tool call stream event missing observed entry payload: %#v", toolCall)
	}
	if !slices.ContainsFunc(events, func(ev AgentStreamEvent) bool {
		return ev.Type == AgentStreamToolCall &&
			ev.ActivityTimeline != nil &&
			ev.ActivityTimeline.Summary.TotalItems > 0 &&
			len(ev.ActivityTimeline.Items) > 0 &&
			ev.ActivityTimeline.Items[0].ToolName == "list"
	}) {
		t.Fatalf("tool call stream events missing activity timeline: %#v", events)
	}
	toolResult := events[toolResultIndex]
	if toolResult.Entry == nil || toolResult.Entry.Type != "tool_result" || toolResult.Entry.Message.ToolName != "list" || toolResult.Entry.Message.ToolCallID != "list-1" {
		t.Fatalf("tool result stream event missing observed entry payload: %#v", toolResult)
	}
	if toolResult.Entry.Message.Content == "" {
		t.Fatalf("tool result stream event should expose local inspection result content: %#v", toolResult)
	}
	if !slices.ContainsFunc(events, func(ev AgentStreamEvent) bool {
		return ev.Type == AgentStreamToolResult &&
			ev.ActivityTimeline != nil &&
			ev.ActivityTimeline.Summary.Status == observation.ActivityStatusSuccess
	}) {
		t.Fatalf("tool result stream events should include completed activity timeline: %#v", events)
	}
	completed := events[completedIndex]
	if completed.Result == nil || completed.Result.Status != "completed" || completed.Result.Session.ID != snapshot.ID {
		t.Fatalf("completion event missing final result: %#v", completed)
	}
	if completed.Result.ActivityTimeline.Summary.Status != observation.ActivityStatusSuccess ||
		completed.Result.Session.ActivityTimeline.Summary.TotalItems == 0 ||
		completed.Result.Observation.ActivityTimeline.Summary.TotalItems == 0 {
		t.Fatalf("completion result missing activity timeline: %#v", completed.Result)
	}
}

func TestServerStreamsHostedSearchEventsWithLocalInspectionByDefault(t *testing.T) {
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
				harness.Text("done"),
				harness.Done(),
			),
		)
	}
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return newProvider(), nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"hosted","name":"Hosted","provider":"anthropic","model":"model","api_key":"secret","web_search":{"source":"provider_hosted","hosted":{"wire_shape":"anthropic_server_web_search"}}},"message":"hello","system_prompt":"test","selected_tools":["web_search"]}`
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody)))
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}

	streamRec := httptest.NewRecorder()
	handler.ServeHTTP(streamRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+snapshot.ID+"/turns/stream", strings.NewReader(`{"message":"hello"}`)))
	if streamRec.Code != http.StatusOK {
		t.Fatalf("stream status = %d, body = %s", streamRec.Code, streamRec.Body.String())
	}
	streamBody := streamRec.Body.String()
	for _, want := range []string{`{\"query\":\"Changsha weather\"}`, "provider-hosted result", "https://example.com/hosted/changsha", "Changsha forecast", "raw-provider-token"} {
		if !strings.Contains(streamBody, want) {
			t.Fatalf("local inspection hosted SSE missing %q: %s", want, streamBody)
		}
	}
	events := parseSSEAgentEvents(t, streamBody)
	if !slices.ContainsFunc(events, func(ev AgentStreamEvent) bool {
		return ev.Type == AgentStreamToolCall &&
			ev.EngineEvent != nil &&
			ev.EngineEvent.Type == event.HostedToolCall &&
			ev.EngineEvent.ToolKind == "hosted" &&
			ev.EngineEvent.ToolName == "web_search" &&
			ev.EngineEvent.Args == `{"query":"Changsha weather"}`
	}) {
		t.Fatalf("local inspection hosted call SSE event missing args: %#v", events)
	}
	if !slices.ContainsFunc(events, func(ev AgentStreamEvent) bool {
		return ev.Type == AgentStreamToolResult &&
			ev.EngineEvent != nil &&
			ev.EngineEvent.Type == event.HostedToolResult &&
			ev.EngineEvent.ToolKind == "hosted" &&
			ev.EngineEvent.ToolName == "web_search" &&
			strings.Contains(ev.Message, "provider-hosted result") &&
			strings.Contains(ev.EngineEvent.Result, "https://example.com/hosted/changsha")
	}) {
		t.Fatalf("local inspection hosted result SSE event missing hosted result: %#v", events)
	}
}

func TestServerStreamExposesLocalInspectionEventsByDefault(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "secret reasoning token=abc"},
			harness.Text("delta token=abc"),
			harness.Tool("list-1", "list", `{"path":null,"limit":2}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			harness.Tool("shell-1", "shell", `{"command":"printf PRIVATE_OUTPUT_X; exit 7","timeout_ms":1000}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"system token=abc","selected_tools":["list","shell"]}`
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody)))
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}

	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+snapshot.ID+"/turns/stream", strings.NewReader(`{"message":"hello token=abc"}`)))
	if turnRec.Code != http.StatusOK {
		t.Fatalf("stream status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	body := turnRec.Body.String()
	for _, want := range []string{"secret reasoning token=abc", `{\"path\":null,\"limit\":2}`, "PRIVATE_OUTPUT_X"} {
		if !strings.Contains(body, want) {
			t.Fatalf("SSE local inspection missing %q: %s", want, body)
		}
	}
	events := parseSSEAgentEvents(t, body)
	if indexStreamEvent(events, AgentStreamProviderRequest) < 0 || indexStreamEvent(events, AgentStreamProviderDelta) < 0 || indexStreamEvent(events, AgentStreamToolCall) < 0 || indexStreamEvent(events, AgentStreamToolResult) < 0 {
		t.Fatalf("stream events missing expected lifecycle events: %#v", events)
	}
	if !slices.ContainsFunc(events, func(ev AgentStreamEvent) bool {
		return ev.Entry != nil &&
			ev.Entry.Type == "tool_call" &&
			ev.Entry.Message.ToolCallID == "list-1" &&
			ev.Entry.Message.ToolArgs == `{"path":null,"limit":2}`
	}) {
		t.Fatalf("SSE entry should expose tool call args by default: %#v", events)
	}
	if !slices.ContainsFunc(events, func(ev AgentStreamEvent) bool {
		return ev.EngineEvent != nil &&
			ev.EngineEvent.Type == event.ToolResult &&
			strings.Contains(ev.EngineEvent.Result, "PRIVATE_OUTPUT_X")
	}) {
		t.Fatalf("SSE engine event should expose tool result by default: %#v", events)
	}
	for _, ev := range events {
		if ev.ActivityTimeline == nil {
			continue
		}
		data, err := json.Marshal(ev.ActivityTimeline)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"PRIVATE_OUTPUT_X", `\"path\":null`, "secret reasoning token=abc"} {
			if strings.Contains(string(data), forbidden) {
				t.Fatalf("activity timeline leaked raw inspection payload %q: %s", forbidden, data)
			}
		}
	}
}

func TestServerAgentSessionExposesToolArgsByDefault(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("list-1", "list", `{"path":null,"limit":2}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"system","selected_tools":["list"]}`
	createRec := httptest.NewRecorder()
	createRec = serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var created AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.SessionID, nil))
	if getRec.Code != http.StatusOK {
		t.Fatalf("session status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(snapshot.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "tool_call" && entry.Message.ToolCallID == "list-1" && entry.Message.ToolArgs == `{"path":null,"limit":2}`
	}) {
		t.Fatalf("session should expose tool args by default: %#v", snapshot.PathEntries)
	}
}

func TestServerLocalInspectionKeepsToolBodiesButSanitizesPathMetadata(t *testing.T) {
	root := t.TempDir()
	secretPath := root + "/secret/debug.txt"
	scripted := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("shell-1", "shell", `{"command":"printf 'DEBUG_RAW_OUTPUT `+secretPath+`'","workdir":"`+root+`","timeout_ms":1000}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte("DEBUG_RAW_OUTPUT " + secretPath), 0
	}
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"system","selected_tools":["shell"]}`
	createRec := httptest.NewRecorder()
	createRec = serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var created AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	createBodyText := createRec.Body.String()
	if !strings.Contains(createBodyText, "DEBUG_RAW_OUTPUT") {
		t.Fatalf("local inspection run response should keep tool body: %s", createBodyText)
	}
	if strings.Contains(createBodyText, root) || strings.Contains(createBodyText, secretPath) {
		t.Fatalf("local inspection run response exposed path: %s", createBodyText)
	}
	if !strings.Contains(createBodyText, event.SafePathLabel(secretPath)) {
		t.Fatalf("local inspection run response should include safe path label: %s", createBodyText)
	}

	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.SessionID, nil))
	if getRec.Code != http.StatusOK {
		t.Fatalf("session status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	getBodyText := getRec.Body.String()
	if !strings.Contains(getBodyText, "DEBUG_RAW_OUTPUT") {
		t.Fatalf("local inspection session should keep tool body: %s", getBodyText)
	}
	if strings.Contains(getBodyText, root) || strings.Contains(getBodyText, secretPath) {
		t.Fatalf("local inspection session exposed path: %s", getBodyText)
	}
	if !strings.Contains(getBodyText, event.SafePathLabel(secretPath)) {
		t.Fatalf("local inspection session should include safe path label: %s", getBodyText)
	}

	if !slices.ContainsFunc(created.Events, func(ev event.Event) bool {
		if ev.Type != event.ToolResult {
			return false
		}
		meta, _ := ev.Metadata.(map[string]any)
		workdir, _ := meta["workdir"].(string)
		return strings.Contains(ev.Result, "DEBUG_RAW_OUTPUT") &&
			!strings.Contains(ev.Result, secretPath) &&
			strings.Contains(ev.Result, event.SafePathLabel(secretPath)) &&
			workdir != "" &&
			!strings.Contains(workdir, root)
	}) {
		t.Fatalf("local inspection events should keep result but sanitize workdir: %#v", created.Events)
	}
}

func TestServerLocalInspectionSanitizesHarnessEventMessages(t *testing.T) {
	root := t.TempDir()
	secretPath := root + "/secret/final.txt"
	output := "final output " + secretPath + " https://example.com/docs/path /artifacts/session/run/output.txt"
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text(output), harness.Done()))
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"system"}`
	createRec := httptest.NewRecorder()
	createRec = serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	body := createRec.Body.String()
	if strings.Contains(body, root) || strings.Contains(body, secretPath) {
		t.Fatalf("local inspection response exposed harness event path: %s", body)
	}
	for _, want := range []string{event.SafePathLabel(secretPath), "https://example.com/docs/path", "/artifacts/session/run/output.txt"} {
		if !strings.Contains(body, want) {
			t.Fatalf("local inspection response missing %q: %s", want, body)
		}
	}
}

func TestServerAgentSessionTurnIgnoresServerTimeout(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(provider.StreamEvent{Type: provider.Delta, Text: "slow-ok", Reason: "50ms"}, harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	server.Timeout = time.Nanosecond
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test","selected_tools":[]}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var created AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+created.ID+"/turns", strings.NewReader(`{"message":"hello"}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Output != "slow-ok" {
		t.Fatalf("server timeout should not cancel agent session turn: %#v", result)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.ID, nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "completed" || !snapshot.CanAppendMessage {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestServerAgentSessionCreateAcceptsSelectedTools(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test","selected_tools":["grep","shell"],"context_policy":{"context_window_tokens":8192,"max_output_tokens":1024,"recent_tail_tokens":1024}}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.Session.SelectedTools) != 2 || result.Session.SelectedTools[0] != "grep" || result.Session.SelectedTools[1] != "shell" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) != 1 {
		t.Fatalf("requests = %#v", result.Observation.ProviderRequests)
	}
	if !hasObservedTool(result.Observation.ProviderRequests[0].Tools, "grep") || !hasObservedTool(result.Observation.ProviderRequests[0].Tools, "shell") {
		t.Fatalf("selected tools missing: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	if hasObservedTool(result.Observation.ProviderRequests[0].Tools, "read") || hasObservedTool(result.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("unselected tools exposed: %#v", result.Observation.ProviderRequests[0].Tools)
	}
}

func TestServerAgentSessionCreateRejectsWebFetchTool(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test","selected_tools":["web_fetch"]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusBadRequest {
		t.Fatalf("create status/body = %d %s", createRec.Code, createRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.Error, `unknown test UI tool "web_fetch"`) {
		t.Fatalf("result = %#v", result)
	}
}

func TestServerAgentSessionCreateAcceptsExternalBraveWebSearchTool(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "fake",
		Profiles: []ProviderProfile{{
			ID:        "fake",
			Name:      "Fake",
			Provider:  config.ProviderFake,
			Model:     "fake-model",
			WebSearch: searchcap.Capability{Source: searchcap.WebSearchExternalBrave, Brave: searchcap.BraveConfig{Provider: searchcap.ExternalProviderBrave}},
		}},
		SearchProvider: SaveSearchProvider{Provider: "brave", APIKey: "search-key"},
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile_id":"fake","message":"hello","system_prompt":"test","selected_tools":["web_search"]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || !slices.Equal(result.Session.SelectedTools, []string{"web_search"}) {
		t.Fatalf("result = %#v", result)
	}
	if !hasObservedTool(result.Observation.ProviderRequests[0].Tools, "web_search") || hasObservedTool(result.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("web_search not isolated in provider request: %#v", result.Observation.ProviderRequests[0].Tools)
	}
}

func TestServerAgentInterfaceProbeExposesSelectedToolsAndDoesNotPersistSession(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	allTools := availableAgentToolNamesForTest(t, runner)
	body, err := json.Marshal(AgentInterfaceProbeRequest{
		SelectedTools: allTools,
		ContextPolicy: contextPolicyForTest(8192),
	})
	if err != nil {
		t.Fatal(err)
	}

	probeReq := httptest.NewRequest(http.MethodPost, "/api/agent/interface-probe", bytes.NewReader(body))
	probeRec := httptest.NewRecorder()
	handler.ServeHTTP(probeRec, probeReq)
	if probeRec.Code != http.StatusOK {
		t.Fatalf("probe status = %d, body = %s", probeRec.Code, probeRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(probeRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Probe || result.Status != "completed" || result.CanAppendMessage || result.Session.CanAppendMessage {
		t.Fatalf("probe result = %#v", result)
	}
	if !slices.Equal(result.Session.SelectedTools, allTools) {
		t.Fatalf("selected tools = %#v, want %#v", result.Session.SelectedTools, allTools)
	}
	if len(result.Observation.ProviderRequests) != 2 {
		t.Fatalf("provider requests = %#v", result.Observation.ProviderRequests)
	}
	firstReq := result.Observation.ProviderRequests[0]
	if len(firstReq.Tools) != len(allTools)+1 || !hasObservedTool(firstReq.Tools, "ask_user") {
		t.Fatalf("first request tools = %#v", firstReq.Tools)
	}
	for _, name := range allTools {
		if !hasObservedTool(firstReq.Tools, name) {
			t.Fatalf("selected tool %q missing from provider request: %#v", name, firstReq.Tools)
		}
	}
	if !slices.ContainsFunc(result.Observation.ProviderEvents, func(ev ObservedProviderEvent) bool {
		return ev.Type == provider.Reasoning && ev.Reasoning == "Inspect selected tool contract."
	}) {
		t.Fatalf("reasoning provider event missing: %#v", result.Observation.ProviderEvents)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == "list" && msg.ToolCallID == "probe-list" && msg.Reasoning == "Inspect selected tool contract." && msg.ToolArgs == `{"path":null,"limit":5}`
	}) {
		t.Fatalf("tool call message missing: %#v", result.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "tool" && msg.ToolName == "list" && msg.ToolCallID == "probe-list"
	}) {
		t.Fatalf("tool result message missing: %#v", result.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[1].Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == "list" && msg.Reasoning == "Inspect selected tool contract." && msg.ToolArgs == `{"path":null,"limit":5}`
	}) {
		t.Fatalf("follow-up request missing assistant tool-call: %#v", result.Observation.ProviderRequests[1].Messages)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions", nil)
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var sessions []AgentSessionSnapshot
	if err := json.Unmarshal(listRec.Body.Bytes(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("probe should not persist sessions: %#v", sessions)
	}
}

func TestServerRejectsOpenAIChatProviderHostedWebSearch(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	saveBody := `{"active_profile_id":"bad-openai","profiles":[{"id":"bad-openai","name":"Bad OpenAI","provider":"openai","model":"gpt-5.4","web_search":{"source":"provider_hosted","hosted":{"wire_shape":"anthropic_server_web_search"}}}],"search_provider":{"provider":"brave","api_key":"search-key"}}`
	saveReq := httptest.NewRequest(http.MethodPut, "/api/config", strings.NewReader(saveBody))
	saveRec := httptest.NewRecorder()
	handler.ServeHTTP(saveRec, saveReq)
	if saveRec.Code != http.StatusBadRequest || !strings.Contains(saveRec.Body.String(), "not supported by this profile") {
		t.Fatalf("save status/body = %d %s", saveRec.Code, saveRec.Body.String())
	}
	if strings.Contains(saveRec.Body.String(), "anthropic_server_web_search") {
		t.Fatalf("save rejection should not echo unsupported hosted wire shape: %s", saveRec.Body.String())
	}

	state, err := runner.ConfigState()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Profiles) != 1 || state.Profiles[0].Provider != config.ProviderFake || state.Profiles[0].WebSearch.Source != searchcap.WebSearchDisabled {
		t.Fatalf("invalid hosted save should not mutate active profile or silently change source: %#v", state.Profiles)
	}

	runBody := `{"profile":{"id":"bad-openai","name":"Bad OpenAI","provider":"openai","model":"gpt-5.4","web_search":{"source":"provider_hosted","hosted":{"wire_shape":"anthropic_server_web_search"}}},"message":"search","system_prompt":"test","selected_tools":["web_search"]}`
	runRec := serveInitialAgentSessionTurn(t, handler, runBody)
	if runRec.Code != http.StatusBadRequest || !strings.Contains(runRec.Body.String(), "not supported by this profile") {
		t.Fatalf("run status/body = %d %s", runRec.Code, runRec.Body.String())
	}
	if strings.Contains(runRec.Body.String(), "anthropic_server_web_search") {
		t.Fatalf("run rejection should not echo unsupported hosted wire shape: %s", runRec.Body.String())
	}
	listReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions", nil)
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var sessions []AgentSessionSnapshot
	if err := json.Unmarshal(listRec.Body.Bytes(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("invalid hosted run should not persist a session: %#v", sessions)
	}
}

func TestServerAgentInterfaceProbeUsesSelectedProfileWebSearchSource(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "external",
		Profiles: []ProviderProfile{
			{
				ID:        "external",
				Name:      "External",
				Provider:  config.ProviderFake,
				Model:     "fake-model",
				WebSearch: searchcap.Capability{Source: searchcap.WebSearchExternalBrave, Brave: searchcap.BraveConfig{Provider: searchcap.ExternalProviderBrave}},
			},
			{
				ID:        "hosted",
				Name:      "Hosted",
				Provider:  catalog.ProviderAnthropic,
				Model:     "claude-sonnet-4-6",
				WebSearch: searchcap.Capability{Source: searchcap.WebSearchProviderHosted, Hosted: searchcap.HostedConfig{WireShape: searchcap.WireShapeAnthropicServerWebSearch}},
			},
		},
		SearchProvider: SaveSearchProvider{Provider: "brave", APIKey: "search-key"},
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	body, err := json.Marshal(AgentInterfaceProbeRequest{
		ProfileID:     "hosted",
		SelectedTools: []string{"web_search"},
		ContextPolicy: contextPolicyForTest(8192),
	})
	if err != nil {
		t.Fatal(err)
	}
	probeRec := httptest.NewRecorder()
	handler.ServeHTTP(probeRec, httptest.NewRequest(http.MethodPost, "/api/agent/interface-probe", bytes.NewReader(body)))
	if probeRec.Code != http.StatusOK {
		t.Fatalf("probe status = %d, body = %s", probeRec.Code, probeRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(probeRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Probe || result.Status != "completed" || !slices.Equal(result.Session.SelectedTools, []string{"web_search"}) {
		t.Fatalf("probe result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) != 1 {
		t.Fatalf("provider requests = %#v", result.Observation.ProviderRequests)
	}
	req := result.Observation.ProviderRequests[0]
	if hasProviderTool(req.Tools, "web_search") {
		t.Fatalf("selected hosted profile should not expose local web_search: %#v", req.Tools)
	}
	if len(req.HostedTools) != 1 || req.HostedTools[0].Name != "web_search" || req.HostedTools[0].Type != "web_search" {
		t.Fatalf("selected hosted profile should expose hosted web_search: %#v", req.HostedTools)
	}
}

func TestServerAgentSessionAppendIgnoresSelectedToolsPayload(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Need file?"}`), harness.Done()),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":[]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != "waiting" || len(first.Session.SelectedTools) != 0 {
		t.Fatalf("first = %#v", first)
	}
	if !hasOnlyObservedTools(first.Observation.ProviderRequests[0].Tools, "ask_user") {
		t.Fatalf("empty selected_tools should expose only control tools: %#v", first.Observation.ProviderRequests[0].Tools)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+first.SessionID+"/turns", strings.NewReader(`{"message":"main.go","selected_tools":["grep","shell"]}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	var second AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != "completed" || len(second.Session.SelectedTools) != 0 {
		t.Fatalf("second = %#v", second)
	}
	if !hasOnlyObservedTools(second.Observation.ProviderRequests[0].Tools, "ask_user") {
		t.Fatalf("append payload changed session tools: %#v", second.Observation.ProviderRequests[0].Tools)
	}
}

func TestServerAgentSessionToolsPatchUpdatesNextProviderRequest(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Need file?"}`), harness.Done()),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":[]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+first.SessionID+"/tools", strings.NewReader(`{"selected_tools":["grep","shell"],"reason":"need command access PRIVATE_REASON_X"}`))
	patchRec := httptest.NewRecorder()
	handler.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body = %s", patchRec.Code, patchRec.Body.String())
	}
	if !strings.Contains(patchRec.Body.String(), "PRIVATE_REASON_X") {
		t.Fatalf("patch response should expose local tool update reason: %s", patchRec.Body.String())
	}
	var patched AgentSessionSnapshot
	if err := json.Unmarshal(patchRec.Body.Bytes(), &patched); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(patched.SelectedTools, []string{"grep", "shell"}) {
		t.Fatalf("patched selected tools = %#v", patched.SelectedTools)
	}
	if !slices.ContainsFunc(patched.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "active_tools_change" &&
			entry.Metadata["previous_tools"] == "" &&
			entry.Metadata["selected_tools"] == "grep,shell" &&
			entry.Metadata["reason"] == "need command access PRIVATE_REASON_X"
	}) {
		t.Fatalf("tool audit entry missing: %#v", patched.PathEntries)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+first.SessionID+"/turns", strings.NewReader(`{"message":"main.go"}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	var second AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != "completed" || !slices.Equal(second.Session.SelectedTools, []string{"grep", "shell"}) {
		t.Fatalf("second = %#v", second)
	}
	if !hasObservedTool(second.Observation.ProviderRequests[0].Tools, "grep") || !hasObservedTool(second.Observation.ProviderRequests[0].Tools, "shell") {
		t.Fatalf("patched tools missing from provider request: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if hasObservedTool(second.Observation.ProviderRequests[0].Tools, "read") || hasObservedTool(second.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("unselected read tool exposed after patch: %#v", second.Observation.ProviderRequests[0].Tools)
	}
}

func TestServerAgentSessionToolsPatchSameToolsetDoesNotAddAuditNoise(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test","selected_tools":["grep"]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+first.SessionID+"/tools", strings.NewReader(`{"selected_tools":["grep"],"reason":"no-op"}`))
	patchRec := httptest.NewRecorder()
	handler.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body = %s", patchRec.Code, patchRec.Body.String())
	}
	var patched AgentSessionSnapshot
	if err := json.Unmarshal(patchRec.Body.Bytes(), &patched); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(patched.SelectedTools, []string{"grep"}) {
		t.Fatalf("patched selected tools = %#v", patched.SelectedTools)
	}
	if slices.ContainsFunc(patched.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "active_tools_change"
	}) {
		t.Fatalf("same toolset patch wrote audit entry: %#v", patched.PathEntries)
	}
}

func TestServerAgentSessionToolsPatchRequiresSelectedToolsField(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test","selected_tools":["grep"]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+first.SessionID+"/tools", strings.NewReader(`{"reason":"missing tools"}`))
	patchRec := httptest.NewRecorder()
	handler.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusBadRequest || !strings.Contains(patchRec.Body.String(), "selected_tools is required") {
		t.Fatalf("patch status/body = %d %s", patchRec.Code, patchRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+first.SessionID, nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshot.SelectedTools, []string{"grep"}) {
		t.Fatalf("missing selected_tools patch changed tools: %#v", snapshot.SelectedTools)
	}
}

func TestServerAgentSessionToolsPatchCanClearLocalTools(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Continue?"}`), harness.Done()),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":["grep"]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+first.SessionID+"/tools", strings.NewReader(`{"selected_tools":[],"reason":"chat only"}`))
	patchRec := httptest.NewRecorder()
	handler.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body = %s", patchRec.Code, patchRec.Body.String())
	}
	var patched AgentSessionSnapshot
	if err := json.Unmarshal(patchRec.Body.Bytes(), &patched); err != nil {
		t.Fatal(err)
	}
	if len(patched.SelectedTools) != 0 {
		t.Fatalf("patched selected tools = %#v", patched.SelectedTools)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+first.SessionID+"/turns", strings.NewReader(`{"message":"yes"}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	var second AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if !hasOnlyObservedTools(second.Observation.ProviderRequests[0].Tools, "ask_user") {
		t.Fatalf("cleared tools should expose only control tools: %#v", second.Observation.ProviderRequests[0].Tools)
	}
}

func TestServerAgentSessionToolsPatchRejectsUnknownTool(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test"}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+first.SessionID+"/tools", strings.NewReader(`{"selected_tools":["missing"]}`))
	patchRec := httptest.NewRecorder()
	handler.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusBadRequest {
		t.Fatalf("patch status/body = %d %s", patchRec.Code, patchRec.Body.String())
	}
}

func TestServerAgentSessionToolsPatchRejectsRunningSession(t *testing.T) {
	blocking := newBlockingTestProvider()
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return blocking, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":[]}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+snapshot.ID+"/turns", strings.NewReader(`{"message":"hello"}`)).WithContext(runCtx)
		turnRec := httptest.NewRecorder()
		handler.ServeHTTP(turnRec, turnReq)
	}()
	blocking.waitStarted(t)

	patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+snapshot.ID+"/tools", strings.NewReader(`{"selected_tools":["grep"]}`))
	patchRec := httptest.NewRecorder()
	handler.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusConflict {
		cancelRun()
		<-done
		t.Fatalf("patch status/body = %d %s", patchRec.Code, patchRec.Body.String())
	}
	cancelRun()
	<-done
}

func TestServerAgentSessionReadsDoNotBlockRunningSession(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":[]}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status/body = %d %s", createRec.Code, createRec.Body.String())
	}
	var created AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	sess, ok := runner.Sessions.get(created.ID)
	if !ok {
		t.Fatalf("session not registered")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	runner.markAgentSessionRunningLocked(sess, "turn-busy")

	listDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		listReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions", nil)
		listRec := httptest.NewRecorder()
		handler.ServeHTTP(listRec, listReq)
		listDone <- listRec
	}()
	var listRec *httptest.ResponseRecorder
	select {
	case listRec = <-listDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("GET /api/agent/sessions blocked on running session")
	}
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status/body = %d %s", listRec.Code, listRec.Body.String())
	}
	var sessions []AgentSessionSnapshot
	if err := json.Unmarshal(listRec.Body.Bytes(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].ID != created.ID || !sessionlifecycle.IsRunningStatus(sessions[0].Status, sessions[0].Phase) || sessions[0].CanAppendMessage {
		t.Fatalf("sessions = %#v", sessions)
	}

	getDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		getReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.ID, nil)
		getRec := httptest.NewRecorder()
		handler.ServeHTTP(getRec, getReq)
		getDone <- getRec
	}()
	var getRec *httptest.ResponseRecorder
	select {
	case getRec = <-getDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("GET /api/agent/sessions/{id} blocked on running session")
	}
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status/body = %d %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.ID != created.ID || !sessionlifecycle.IsRunningStatus(snapshot.Status, snapshot.Phase) || snapshot.CanAppendMessage {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestServerAgentSessionCreateRejectsUnknownSelectedTool(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test","selected_tools":["missing"]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusBadRequest {
		t.Fatalf("create status/body = %d %s", createRec.Code, createRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.Error, `unknown test UI tool "missing"`) {
		t.Fatalf("result = %#v", result)
	}
}

func TestServerAgentSessionCreateAndAppendTurn(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Need file?"}`), harness.Done()),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","context_policy":{"context_window_tokens":8192,"max_output_tokens":1024,"recent_tail_tokens":1024}}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != "waiting" || first.SessionID == "" || first.WaitingPrompt != "Need file?" {
		t.Fatalf("first = %#v", first)
	}

	waitingGetReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+first.SessionID, nil)
	waitingGetRec := httptest.NewRecorder()
	handler.ServeHTTP(waitingGetRec, waitingGetReq)
	if waitingGetRec.Code != http.StatusOK {
		t.Fatalf("waiting get status = %d, body = %s", waitingGetRec.Code, waitingGetRec.Body.String())
	}
	var waitingSnapshot AgentSessionSnapshot
	if err := json.Unmarshal(waitingGetRec.Body.Bytes(), &waitingSnapshot); err != nil {
		t.Fatal(err)
	}
	if waitingSnapshot.Status != "waiting" || waitingSnapshot.WaitingPrompt != "Need file?" || !waitingSnapshot.CanAppendMessage || len(waitingSnapshot.Turns) != 1 {
		t.Fatalf("waiting snapshot = %#v", waitingSnapshot)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+first.SessionID+"/turns", strings.NewReader(`{"message":"main.go"}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	var second AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != "completed" || second.SessionID != first.SessionID || second.Output != "done" || len(second.Session.Turns) != 2 {
		t.Fatalf("second = %#v", second)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+first.SessionID, nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.ID != first.SessionID || snapshot.Status != "completed" || len(snapshot.Turns) != 2 {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions", nil)
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var sessions []AgentSessionSnapshot
	if err := json.Unmarshal(listRec.Body.Bytes(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].ID != first.SessionID || len(sessions[0].Turns) != 2 {
		t.Fatalf("sessions = %#v", sessions)
	}
}

func TestServerAgentSessionDeleteRemovesCompletedSession(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done()))
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test"}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != "completed" || first.SessionID == "" {
		t.Fatalf("first = %#v", first)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agent/sessions/"+first.SessionID, nil)
	deleteRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK || !strings.Contains(deleteRec.Body.String(), `"deleted":true`) {
		t.Fatalf("delete status/body = %d %s", deleteRec.Code, deleteRec.Body.String())
	}
	getReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+first.SessionID, nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("get deleted status/body = %d %s", getRec.Code, getRec.Body.String())
	}
	listReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions", nil)
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var sessions []AgentSessionSnapshot
	if err := json.Unmarshal(listRec.Body.Bytes(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("sessions = %#v", sessions)
	}
}

func TestServerAgentSessionDeleteRemovesToolOutputArtifactRoute(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("shell-1", "shell", `{"command":"printf '0123456789abcdef'","max_output_bytes":8}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test","selected_tools":["shell"]}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != "completed" || first.SessionID == "" {
		t.Fatalf("first = %#v", first)
	}
	var artifactURL string
	for _, msg := range first.Observation.SessionMessages {
		if msg.Role == "tool" && msg.ToolResult != nil && msg.ToolResult.FullOutput != nil {
			artifactURL = msg.ToolResult.FullOutput.URL
			break
		}
	}
	if artifactURL == "" || !strings.HasPrefix(artifactURL, "/artifacts/") {
		t.Fatalf("artifact route missing from observation: %#v", first.Observation.SessionMessages)
	}

	artifactReq := httptest.NewRequest(http.MethodGet, artifactURL, nil)
	artifactRec := httptest.NewRecorder()
	handler.ServeHTTP(artifactRec, artifactReq)
	if artifactRec.Code != http.StatusOK || artifactRec.Body.String() != "0123456789abcdef" {
		t.Fatalf("artifact before delete status/body = %d %q", artifactRec.Code, artifactRec.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agent/sessions/"+first.SessionID, nil)
	deleteRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK || !strings.Contains(deleteRec.Body.String(), `"deleted":true`) {
		t.Fatalf("delete status/body = %d %s", deleteRec.Code, deleteRec.Body.String())
	}

	artifactAfterDeleteRec := httptest.NewRecorder()
	handler.ServeHTTP(artifactAfterDeleteRec, httptest.NewRequest(http.MethodGet, artifactURL, nil))
	if artifactAfterDeleteRec.Code != http.StatusNotFound {
		t.Fatalf("artifact after delete status/body = %d %s", artifactAfterDeleteRec.Code, artifactAfterDeleteRec.Body.String())
	}
}

func TestServerAgentSessionDeleteRejectsRunningSession(t *testing.T) {
	blocking := newBlockingTestProvider()
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return blocking, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test"}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status/body = %d %s", createRec.Code, createRec.Body.String())
	}
	var created AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+created.ID+"/turns", strings.NewReader(`{"message":"hello"}`)).WithContext(runCtx)
		turnRec := httptest.NewRecorder()
		handler.ServeHTTP(turnRec, turnReq)
	}()
	blocking.waitStarted(t)
	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agent/sessions/"+created.ID, nil)
	deleteRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusConflict {
		cancelRun()
		<-done
		t.Fatalf("delete status/body = %d %s", deleteRec.Code, deleteRec.Body.String())
	}
	cancelRun()
	<-done
}

func TestServerAgentSessionDeadlineTurnIsTerminalOnLaterRead(t *testing.T) {
	blocking := newBlockingTestProvider()
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return blocking, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test"}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status/body = %d %s", createRec.Code, createRec.Body.String())
	}
	var created AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	turnDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+created.ID+"/turns", strings.NewReader(`{"message":"hello"}`)).WithContext(runCtx)
		turnRec := httptest.NewRecorder()
		handler.ServeHTTP(turnRec, turnReq)
		turnDone <- turnRec
	}()
	blocking.waitStarted(t)
	cancelRun()
	turnRec := <-turnDone
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status/body = %d %s", turnRec.Code, turnRec.Body.String())
	}
	var turn AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &turn); err != nil {
		t.Fatal(err)
	}
	if (turn.Status != string(engine.Cancelled) && turn.Status != string(engine.Failed)) ||
		(!strings.Contains(turn.Error, context.DeadlineExceeded.Error()) && !strings.Contains(turn.Error, context.Canceled.Error())) {
		t.Fatalf("turn = %#v", turn)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.ID, nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status/body = %d %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if (snapshot.Status != string(engine.Cancelled) && snapshot.Status != string(engine.Failed)) || sessionlifecycle.IsRunningStatus(snapshot.Status, snapshot.Phase) || snapshot.CanAppendMessage {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if len(snapshot.Turns) != 1 || snapshot.Turns[0].ID == "" || snapshot.Turns[0].Status != snapshot.Status {
		t.Fatalf("turn summaries = %#v", snapshot.Turns)
	}

	meta, err := runner.loadAgentSessionMetadata(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Turns) != 1 || meta.Turns[0].Status != snapshot.Status || !meta.UpdatedAt.After(meta.CreatedAt) {
		t.Fatalf("metadata = %#v", meta)
	}
}

func TestServerAgentSessionPersistsAcrossServerRestart(t *testing.T) {
	root := t.TempDir()
	firstProvider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("ask", "ask_user", `{"question":"Need file?"}`), harness.Done()),
	)
	firstRunner := NewRunner(root)
	firstRunner.Now = fixedClock()
	firstRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return firstProvider, nil
	}
	firstServer, err := NewServer(firstRunner)
	if err != nil {
		t.Fatal(err)
	}
	firstHandler := firstServer.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","context_policy":{"context_window_tokens":8192,"max_output_tokens":1024,"recent_tail_tokens":1024}}`
	createRec := serveInitialAgentSessionTurn(t, firstHandler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != "waiting" || first.SessionID == "" {
		t.Fatalf("first = %#v", first)
	}

	secondProvider := harness.NewScriptedProvider(
		harness.Step(harness.Text("done after restart"), harness.Done()),
	)
	secondRunner := NewRunner(root)
	secondRunner.Now = fixedClock()
	secondRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return secondProvider, nil
	}
	secondServer, err := NewServer(secondRunner)
	if err != nil {
		t.Fatal(err)
	}
	secondHandler := secondServer.Handler()

	listReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions", nil)
	listRec := httptest.NewRecorder()
	secondHandler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var sessions []AgentSessionSnapshot
	if err := json.Unmarshal(listRec.Body.Bytes(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].ID != first.SessionID || sessions[0].Status != "waiting" || sessions[0].WaitingPrompt != "Need file?" {
		t.Fatalf("sessions = %#v", sessions)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+first.SessionID, nil)
	getRec := httptest.NewRecorder()
	secondHandler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var snapshot AgentSessionSnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "waiting" || !snapshot.CanAppendMessage || len(snapshot.Turns) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+first.SessionID+"/turns", strings.NewReader(`{"message":"main.go"}`))
	turnRec := httptest.NewRecorder()
	secondHandler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusOK {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
	var second AgentRunResponse
	if err := json.Unmarshal(turnRec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != "completed" || second.Output != "done after restart" || second.SessionID != first.SessionID || second.TurnID == first.TurnID || len(second.Session.Turns) != 2 {
		t.Fatalf("second = %#v", second)
	}
}

func TestServerAgentSessionTurnErrorsUseClientStatuses(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(`{"message":"","system_prompt":"test"}`))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var created AgentSessionSnapshot
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	emptyReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+created.ID+"/turns", strings.NewReader(`{"message":""}`))
	emptyRec := httptest.NewRecorder()
	handler.ServeHTTP(emptyRec, emptyReq)
	if emptyRec.Code != http.StatusBadRequest {
		t.Fatalf("empty turn status = %d, body = %s", emptyRec.Code, emptyRec.Body.String())
	}

	missingReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/missing/turns", strings.NewReader(`{"message":"hello"}`))
	missingRec := httptest.NewRecorder()
	handler.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, body = %s", missingRec.Code, missingRec.Body.String())
	}
}

func TestServerAgentSessionRejectsAppendAfterFailedTurn(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("fail"), harness.Done()))
	scripted.Errs[1] = context.Canceled
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test"}`
	createRec := serveInitialAgentSessionTurn(t, handler, createBody)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != "cancelled" || first.SessionID == "" {
		t.Fatalf("first = %#v", first)
	}

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+first.SessionID+"/turns", strings.NewReader(`{"message":"again"}`))
	turnRec := httptest.NewRecorder()
	handler.ServeHTTP(turnRec, turnReq)
	if turnRec.Code != http.StatusConflict {
		t.Fatalf("turn status = %d, body = %s", turnRec.Code, turnRec.Body.String())
	}
}

func TestServerRejectsInvalidRunJSON(t *testing.T) {
	server, err := NewServer(NewRunner(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/run", strings.NewReader("{"))
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestServerServesStaticConsole(t *testing.T) {
	server, err := NewServer(NewRunner(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/sessions", "/sessions/new", "/settings"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d", path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "Floret Agent Console") {
			t.Fatalf("%s body did not contain console title", path)
		}
	}
	moduleReq := httptest.NewRequest(http.MethodGet, "/views/sessionWorkspace.js", nil)
	moduleRec := httptest.NewRecorder()
	server.Handler().ServeHTTP(moduleRec, moduleReq)
	if moduleRec.Code != http.StatusOK || !strings.Contains(moduleRec.Body.String(), "renderSessionWorkspace") {
		t.Fatalf("module status/body = %d %s", moduleRec.Code, moduleRec.Body.String())
	}
}

func hasObservedTool(defs []provider.ToolDefinition, name string) bool {
	for _, def := range defs {
		if def.Name == name {
			return true
		}
	}
	return false
}

func hasOnlyObservedTools(defs []provider.ToolDefinition, names ...string) bool {
	if len(defs) != len(names) {
		return false
	}
	for _, name := range names {
		if !hasObservedTool(defs, name) {
			return false
		}
	}
	return true
}

func allAgentToolNamesForTest() []string {
	names := make([]string, 0, len(agentToolOptions))
	for _, option := range agentToolOptions {
		names = append(names, option.Name)
	}
	return names
}

func availableAgentToolNamesForTest(t *testing.T, runner Runner) []string {
	t.Helper()
	state, err := runner.ConfigState()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(state.Tools))
	for _, option := range state.Tools {
		if option.Available {
			names = append(names, option.Name)
		}
	}
	return names
}

func parseSSEAgentEvents(t *testing.T, body string) []AgentStreamEvent {
	t.Helper()
	events := []AgentStreamEvent{}
	for _, frame := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n\n") {
		var data []string
		for _, line := range strings.Split(frame, "\n") {
			if strings.HasPrefix(line, "data:") {
				data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
		if len(data) == 0 {
			continue
		}
		var ev AgentStreamEvent
		if err := json.Unmarshal([]byte(strings.Join(data, "\n")), &ev); err != nil {
			t.Fatalf("invalid SSE event %q: %v", strings.Join(data, "\n"), err)
		}
		events = append(events, ev)
	}
	return events
}

func assertStreamEventOrder(t *testing.T, events []AgentStreamEvent, wants ...AgentStreamEventType) {
	t.Helper()
	cursor := 0
	for _, want := range wants {
		found := false
		for cursor < len(events) {
			if events[cursor].Type == want {
				found = true
				cursor++
				break
			}
			cursor++
		}
		if !found {
			t.Fatalf("stream event %q not found in order; events = %#v", want, events)
		}
	}
}

func indexStreamEvent(events []AgentStreamEvent, typ AgentStreamEventType) int {
	for i, ev := range events {
		if ev.Type == typ {
			return i
		}
	}
	return -1
}

func indexStreamEventWithEntry(events []AgentStreamEvent, typ AgentStreamEventType) int {
	for i, ev := range events {
		if ev.Type == typ && ev.Entry != nil {
			return i
		}
	}
	return -1
}

type recordingToolScenarioProvider struct {
	mu          sync.Mutex
	requests    []provider.Request
	failWeather bool
}

func (p *recordingToolScenarioProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	requestNo := len(p.requests)
	p.mu.Unlock()
	events := []provider.StreamEvent{harness.Text("live-ok"), harness.Done()}
	if p.failWeather && hasProviderTool(req.Tools, "shell") && len(req.HostedTools) > 0 {
		events = []provider.StreamEvent{harness.Truncated("diagnostic unavailable")}
	}
	if hasProviderTool(req.Tools, "list") && hasProviderTool(req.Tools, "read") && hasProviderTool(req.Tools, "grep") && requestNo == 1 {
		events = []provider.StreamEvent{
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "live-list", Name: "list", Args: `{"path":"."}`},
				{ID: "live-read", Name: "read", Args: `{"path":"README.md"}`},
				{ID: "live-grep", Name: "grep", Args: `{"pattern":"Status","path":"notes"}`},
			}},
			harness.DoneReason("tool_calls"),
		}
	}
	ch := make(chan provider.StreamEvent, len(events))
	go func() {
		defer close(ch)
		for _, ev := range events {
			select {
			case <-ctx.Done():
				return
			case ch <- ev:
			}
		}
	}()
	return ch, nil
}

func (p *recordingToolScenarioProvider) Requests() []provider.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]provider.Request(nil), p.requests...)
}

func hasProviderTool(defs []provider.ToolDefinition, name string) bool {
	return slices.ContainsFunc(defs, func(def provider.ToolDefinition) bool {
		return def.Name == name
	})
}

func assertProviderToolRequiredFields(t *testing.T, defs []provider.ToolDefinition, name string, want ...string) {
	t.Helper()
	var schema map[string]any
	for _, def := range defs {
		if def.Name == name {
			schema = def.InputSchema
			break
		}
	}
	if schema == nil {
		t.Fatalf("tool %s missing from provider definitions", name)
	}
	required, _ := schema["required"].([]any)
	if len(required) != len(want) {
		t.Fatalf("%s required fields = %#v, want %#v", name, schema["required"], want)
	}
	for i, field := range want {
		if required[i] != field {
			t.Fatalf("%s required fields = %#v, want %#v", name, schema["required"], want)
		}
	}
}
