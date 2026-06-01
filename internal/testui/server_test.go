package testui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

	runBody := `{"profile_id":"fake","message":"hello","system_prompt":"test","max_steps":4,"max_context_messages":8}`
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
