package testui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/provider"
)

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
	if !strings.Contains(configRec.Body.String(), `"name":"grep"`) || !strings.Contains(configRec.Body.String(), `"name":"web_fetch"`) || !strings.Contains(configRec.Body.String(), `"name":"web_search"`) {
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

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":["grep","web_search"],"context_policy":{"context_window_tokens":8192,"max_output_tokens":1024,"recent_tail_tokens":1024}}`
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
	if !slices.Equal(snapshot.SelectedTools, []string{"grep", "web_search"}) {
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

func TestServerAgentSessionCreateAcceptsSelectedTools(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test","selected_tools":["grep","web_fetch"],"context_policy":{"context_window_tokens":8192,"max_output_tokens":1024,"recent_tail_tokens":1024}}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var result AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || len(result.Session.SelectedTools) != 2 || result.Session.SelectedTools[0] != "grep" || result.Session.SelectedTools[1] != "web_fetch" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) != 1 {
		t.Fatalf("requests = %#v", result.Observation.ProviderRequests)
	}
	if !hasObservedTool(result.Observation.ProviderRequests[0].Tools, "grep") || !hasObservedTool(result.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("selected tools missing: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	if hasObservedTool(result.Observation.ProviderRequests[0].Tools, "read") || hasObservedTool(result.Observation.ProviderRequests[0].Tools, "shell") {
		t.Fatalf("unselected tools exposed: %#v", result.Observation.ProviderRequests[0].Tools)
	}
}

func TestServerAgentSessionCreateAcceptsClientWebSearchTool(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"ok"},"message":"hello","system_prompt":"test","selected_tools":["web_search"]}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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
	allTools := allAgentToolNamesForTest()
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
		return ev.Type == provider.Reasoning && strings.Contains(ev.Reasoning, "Inspect selected tool contract")
	}) {
		t.Fatalf("reasoning provider event missing: %#v", result.Observation.ProviderEvents)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == "list" && msg.ToolCallID == "probe-list" && strings.Contains(msg.Reasoning, "Inspect selected tool contract")
	}) {
		t.Fatalf("tool call message with reasoning missing: %#v", result.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "tool" && msg.ToolName == "list" && msg.ToolCallID == "probe-list"
	}) {
		t.Fatalf("tool result message missing: %#v", result.Observation.SessionMessages)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[1].Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == "list" && strings.Contains(msg.Reasoning, "Inspect selected tool contract")
	}) {
		t.Fatalf("follow-up request did not carry assistant tool-call reasoning: %#v", result.Observation.ProviderRequests[1].Messages)
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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

	turnReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+first.SessionID+"/turns", strings.NewReader(`{"message":"main.go","selected_tools":["grep","web_fetch"]}`))
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+first.SessionID+"/tools", strings.NewReader(`{"selected_tools":["grep","web_fetch"],"reason":"need network read"}`))
	patchRec := httptest.NewRecorder()
	handler.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body = %s", patchRec.Code, patchRec.Body.String())
	}
	var patched AgentSessionSnapshot
	if err := json.Unmarshal(patchRec.Body.Bytes(), &patched); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(patched.SelectedTools, []string{"grep", "web_fetch"}) {
		t.Fatalf("patched selected tools = %#v", patched.SelectedTools)
	}
	if !slices.ContainsFunc(patched.PathEntries, func(entry ObservedSessionEntry) bool {
		return entry.Type == "active_tools_change" &&
			entry.Metadata["previous_tools"] == "" &&
			entry.Metadata["selected_tools"] == "grep,web_fetch" &&
			entry.Metadata["reason"] == "need network read"
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
	if second.Status != "completed" || !slices.Equal(second.Session.SelectedTools, []string{"grep", "web_fetch"}) {
		t.Fatalf("second = %#v", second)
	}
	if !hasObservedTool(second.Observation.ProviderRequests[0].Tools, "grep") || !hasObservedTool(second.Observation.ProviderRequests[0].Tools, "web_fetch") {
		t.Fatalf("patched tools missing from provider request: %#v", second.Observation.ProviderRequests[0].Tools)
	}
	if hasObservedTool(second.Observation.ProviderRequests[0].Tools, "read") {
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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
	scripted := harness.NewScriptedProvider(harness.Step(harness.Hang()))
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	server.Timeout = 250 * time.Millisecond
	handler := server.Handler()
	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"unused"},"message":"hello","system_prompt":"test","selected_tools":[]}`

	done := make(chan struct{})
	go func() {
		defer close(done)
		createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
		createRec := httptest.NewRecorder()
		handler.ServeHTTP(createRec, createReq)
	}()
	for i := 0; i < 50; i++ {
		registry := runner.sessionRegistry()
		registry.mu.Lock()
		var sessionID string
		if len(registry.order) == 1 {
			sessionID = registry.order[0]
		}
		registry.mu.Unlock()
		if sessionID != "" {
			patchReq := httptest.NewRequest(http.MethodPatch, "/api/agent/sessions/"+sessionID+"/tools", strings.NewReader(`{"selected_tools":["grep"]}`))
			patchRec := httptest.NewRecorder()
			handler.ServeHTTP(patchRec, patchReq)
			if patchRec.Code != http.StatusConflict {
				t.Fatalf("patch status/body = %d %s", patchRec.Code, patchRec.Body.String())
			}
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session did not appear while running")
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
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

func TestServerAgentSessionDeleteRejectsRunningSession(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Hang()))
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return scripted, nil
	}
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	server.Timeout = 250 * time.Millisecond
	handler := server.Handler()
	createBody := `{"profile":{"id":"fake","name":"Fake","provider":"fake","model":"fake-model"},"message":"hello","system_prompt":"test"}`

	done := make(chan struct{})
	go func() {
		defer close(done)
		createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
		createRec := httptest.NewRecorder()
		handler.ServeHTTP(createRec, createReq)
	}()
	for i := 0; i < 50; i++ {
		registry := runner.sessionRegistry()
		registry.mu.Lock()
		var sessionID string
		if len(registry.order) == 1 {
			sessionID = registry.order[0]
		}
		registry.mu.Unlock()
		if sessionID != "" {
			deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agent/sessions/"+sessionID, nil)
			deleteRec := httptest.NewRecorder()
			handler.ServeHTTP(deleteRec, deleteReq)
			if deleteRec.Code != http.StatusConflict {
				t.Fatalf("delete status/body = %d %s", deleteRec.Code, deleteRec.Body.String())
			}
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session did not appear while running")
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	firstHandler.ServeHTTP(createRec, createReq)
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

	emptyReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(`{"message":""}`))
	emptyRec := httptest.NewRecorder()
	handler.ServeHTTP(emptyRec, emptyReq)
	if emptyRec.Code != http.StatusBadRequest {
		t.Fatalf("empty create status = %d, body = %s", emptyRec.Code, emptyRec.Body.String())
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/run", strings.NewReader(createBody))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var first AgentRunResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != "failed" || first.SessionID == "" {
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
