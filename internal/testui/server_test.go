package testui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

	saveBody := `{"active_profile_id":"fake","profiles":[{"id":"fake","name":"Fake","provider":"fake","model":"fake-model","fake_response":"server-ok"}]}`
	saveReq := httptest.NewRequest(http.MethodPut, "/api/config", strings.NewReader(saveBody))
	saveRec := httptest.NewRecorder()
	handler.ServeHTTP(saveRec, saveReq)
	if saveRec.Code != http.StatusOK {
		t.Fatalf("save status = %d, body = %s", saveRec.Code, saveRec.Body.String())
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
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

	emptyReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(`{"message":""}`))
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
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(createBody))
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
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Floret Agent Console") {
		t.Fatalf("body did not contain console title")
	}
}
