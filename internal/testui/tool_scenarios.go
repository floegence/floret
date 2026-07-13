package testui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/internal/tools/builtin"
)

type runOptions struct {
	ProfileID string
}

type toolScenario struct {
	ID            string
	Title         string
	Description   string
	Message       string
	FollowUps     []string
	SelectedTools []string
	Setup         func(context.Context, string) (toolScenarioRuntime, error)
	Verify        func(toolScenarioRun) error
}

type toolScenarioRuntime struct {
	Provider     provider.Provider
	Profile      ProviderProfile
	SystemPrompt string
	Env          map[string]string
	Cleanup      func()
	Check        func() error
}

type toolScenarioRun struct {
	Scenario toolScenario
	Runtime  toolScenarioRuntime
	Results  []AgentRunResponse
}

const liveToolScenarioTimeout = 90 * time.Second

func deterministicToolScenarios() []toolScenario {
	return []toolScenario{
		{
			ID:          "read-grep-shell-followup",
			Title:       "Read, grep, shell, then follow-up",
			Description: "Exercises workspace read tools, a low-risk shell call, and a second user turn over the same session history.",
			Message:     "Inspect the local fixture and summarize the project status.",
			FollowUps:   []string{"Now cite the exact file and command you used."},
			SelectedTools: []string{
				builtin.ToolList,
				builtin.ToolRead,
				builtin.ToolGrep,
				builtin.ToolShell,
			},
			Setup:  setupReadGrepShellScenario,
			Verify: verifyReadGrepShellScenario,
		},
		{
			ID:            "mutation-error-recovery",
			Title:         "Mutation tool error recovery",
			Description:   "Calls write and apply_patch, intentionally triggers one patch context error, then recovers with a valid patch before the follow-up.",
			Message:       "Create a report file, correct it, and mention how you recovered from the failed patch.",
			FollowUps:     []string{"Read the report back and confirm the final contents."},
			SelectedTools: []string{builtin.ToolWrite, builtin.ToolApplyPatch, builtin.ToolRead},
			Setup:         setupMutationRecoveryScenario,
			Verify:        verifyMutationRecoveryScenario,
		},
		{
			ID:            "search-shell-curl-followup",
			Title:         "Search, shell curl, and follow-up",
			Description:   "Exercises external Brave web_search, bounded shell curl, repeated multi-tool batches, and a second user turn.",
			Message:       "今天是 2026-06-03，请查询长沙天气并给出来源。",
			FollowUps:     []string{"那么明天会晴吗，适合出门吗？"},
			SelectedTools: []string{builtin.ToolWebSearch, builtin.ToolShell},
			Setup:         setupSearchShellScenario,
			Verify:        verifySearchShellScenario,
		},
	}
}

func (r Runner) runToolScenarioSuite(ctx context.Context, resp RunResponse) RunResponse {
	return r.runToolScenarioSuiteWith(ctx, resp, deterministicToolScenarios(), "Deterministic tool scenario suite")
}

func (r Runner) runToolScenarioSuiteWith(ctx context.Context, resp RunResponse, scenarios []toolScenario, title string) RunResponse {
	resp.Title = title
	resp.Kind = "suite"
	status := "pass"
	for _, scenario := range scenarios {
		part := r.runToolScenario(ctx, scenario)
		resp.Parts = append(resp.Parts, part)
		if part.Status != "pass" && status == "pass" {
			status = part.Status
		}
	}
	resp.Status = status
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	resp.Summary = fmt.Sprintf("%d tool scenario(s) finished, status %s.", len(resp.Parts), status)
	return resp
}

func (r Runner) runToolScenario(ctx context.Context, scenario toolScenario) RunResponse {
	started := r.now()
	resp := RunResponse{
		ID:        fmt.Sprintf("%s-%d", scenario.ID, started.UnixNano()),
		Target:    TargetToolScenarios,
		Title:     scenario.Title,
		Kind:      "agent",
		StartedAt: started,
	}
	workspace, err := r.newRunWorkspace("tool-scenario-" + scenario.ID)
	if err != nil {
		return r.failAgent(resp, err)
	}
	runtime, err := scenario.Setup(ctx, workspace)
	if runtime.Cleanup != nil {
		defer runtime.Cleanup()
	}
	if err != nil {
		return r.failAgent(resp, err)
	}
	for key, value := range runtime.Env {
		if err := appendDotEnvValue(filepath.Join(workspace, config.DefaultEnvFile), key, value); err != nil {
			return r.failAgent(resp, err)
		}
	}
	scenarioRunner := NewRunner(workspace)
	scenarioRunner.Now = r.Now
	scenarioRunner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return runtime.Provider, nil
	}
	message := scenario.Message
	systemPrompt := runtime.SystemPrompt
	if strings.TrimSpace(systemPrompt) == "" {
		systemPrompt = "You are Floret's deterministic test UI tool scenario runner. Follow the scripted provider behavior and exercise the selected tool contracts."
	}
	first := scenarioRunner.CreateAgentSession(ctx, AgentRunRequest{
		Profile:       runtime.Profile,
		Message:       message,
		SystemPrompt:  systemPrompt,
		SelectedTools: scenario.SelectedTools,
		ContextPolicy: scenarioContextPolicy(),
	})
	run := toolScenarioRun{Scenario: scenario, Runtime: runtime, Results: []AgentRunResponse{first}}
	for _, followUp := range scenario.FollowUps {
		if first.SessionID == "" {
			break
		}
		next := scenarioRunner.RunAgentTurn(ctx, first.SessionID, AgentTurnRequest{Message: followUp})
		run.Results = append(run.Results, next)
	}
	if scenario.Verify != nil {
		if err := scenario.Verify(run); err != nil {
			resp.Status = "fail"
			resp.Error = err.Error()
			resp.Summary = err.Error()
		}
	}
	if resp.Status == "" {
		resp.Status = "pass"
		toolCalls := 0
		for _, result := range run.Results {
			toolCalls += result.Metrics.ToolCalls
		}
		resp.Summary = fmt.Sprintf("%s passed with %d turn(s), %d local tool call(s).", scenario.ID, len(run.Results), toolCalls)
	}
	last := run.Results[len(run.Results)-1]
	resp.Agent = &AgentRun{
		EngineStatus: last.Status,
		Output:       last.Output,
		Metrics:      last.Session.AggregateMetrics,
		Events:       last.Events,
		Config: ConfigInfo{
			Provider: runtime.Profile.Provider,
			Model:    runtime.Profile.Model,
		},
		Artifacts: map[string]ArtifactSnapshot{
			"scenario": {
				Path:    scenario.ID,
				Content: renderToolScenarioArtifact(scenario, run.Results),
			},
		},
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) runLiveToolScenarios(ctx context.Context, resp RunResponse, opts runOptions) RunResponse {
	profile, err := r.profileByID(opts.ProfileID)
	if err != nil {
		return r.failAgent(resp, err)
	}
	resp.Title = "Configured provider tool scenarios"
	resp.Kind = "suite"
	parts := []RunResponse{
		r.runLiveLocalToolScenario(ctx, profile),
		r.runLiveWeatherToolScenario(ctx, profile),
	}
	status := "pass"
	requiredFailures := 0
	diagnosticFailures := 0
	for _, part := range parts {
		resp.Parts = append(resp.Parts, part)
		if part.Status != "pass" {
			if part.ID != "" && strings.HasPrefix(part.ID, "live-weather-") {
				diagnosticFailures++
			} else {
				requiredFailures++
				if status == "pass" {
					status = part.Status
				}
			}
		}
	}
	if requiredFailures == 0 {
		status = "pass"
	}
	resp.Status = status
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	if diagnosticFailures > 0 && requiredFailures == 0 {
		resp.Summary = fmt.Sprintf("%d live provider tool scenario(s) finished with status %s; %d external web diagnostic scenario(s) reported provider/tool availability issues.", len(resp.Parts), resp.Status, diagnosticFailures)
	} else {
		resp.Summary = fmt.Sprintf("%d live provider tool scenario(s) finished with status %s.", len(resp.Parts), resp.Status)
	}
	return resp
}

func (r Runner) runLiveLocalToolScenario(ctx context.Context, profile ProviderProfile) RunResponse {
	started := r.now()
	resp := RunResponse{
		ID:        fmt.Sprintf("live-local-tools-%d", started.UnixNano()),
		Target:    TargetLiveToolScenarios,
		Title:     "Live local tool calling",
		Kind:      "agent",
		StartedAt: started,
	}
	workspace, err := r.newRunWorkspace("live-local-tools")
	if err != nil {
		return r.failAgent(resp, err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("# Live Fixture\n\nStatus: green\n"), 0o644); err != nil {
		return r.failAgent(resp, err)
	}
	if err := os.MkdirAll(filepath.Join(workspace, "notes"), 0o755); err != nil {
		return r.failAgent(resp, err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "notes", "status.txt"), []byte("Status: green\nOwner: live scenario\n"), 0o644); err != nil {
		return r.failAgent(resp, err)
	}
	req := AgentRunRequest{
		Profile: profile,
		Message: "Use the available tools before answering: call list on the workspace root, read README.md, and grep for Status under notes. Then summarize what you found.",
		SystemPrompt: strings.Join([]string{
			"You are Floret's live local tool scenario runner.",
			"You must call list, read, and grep before the final answer.",
			"When a schema shows nullable fields, include them with null when you do not need a value.",
			"Do not answer from memory for this scenario.",
		}, " "),
		SelectedTools: []string{builtin.ToolList, builtin.ToolRead, builtin.ToolGrep},
		ContextPolicy: scenarioContextPolicy(),
	}
	results, err := r.runLiveAgentTurnsWithTimeout(ctx, workspace, req, []string{"Cite the exact file paths from the previous answer."})
	if err != nil {
		return r.failLiveScenario(resp, profile, "live-local-tools", []string{builtin.ToolList, builtin.ToolRead, builtin.ToolGrep}, err, results)
	}
	if err := verifyLiveLocalToolScenario(results); err != nil {
		return r.failLiveScenario(resp, profile, "live-local-tools", []string{builtin.ToolList, builtin.ToolRead, builtin.ToolGrep}, err, results)
	}
	last := results[len(results)-1]
	resp.Status = "pass"
	resp.Summary = fmt.Sprintf("Live provider completed local tool scenario with %d turn(s) and %d tool call(s).", len(results), last.Session.AggregateMetrics.ToolCalls)
	resp.Agent = liveScenarioAgent(profile, "live-local-tools", "Live local tool calling", results)
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) runLiveWeatherToolScenario(ctx context.Context, profile ProviderProfile) RunResponse {
	started := r.now()
	resp := RunResponse{
		ID:        fmt.Sprintf("live-weather-%d", started.UnixNano()),
		Target:    TargetLiveToolScenarios,
		Title:     "Live weather query with available web tools",
		Kind:      "agent",
		StartedAt: started,
	}
	selected, err := liveWeatherScenarioTools(profile, r.EnvFile)
	if err != nil {
		return r.failAgent(resp, err)
	}
	workspace, err := r.newRunWorkspace("live-tool-scenario")
	if err != nil {
		return r.failAgent(resp, err)
	}
	req := AgentRunRequest{
		Profile:       profile,
		Message:       "请查询长沙在 2026-06-03 这一天的天气。请优先使用可用的 web_search 能力；如果需要打开具体来源或 HTTP API，请使用 shell 运行有输出上限的 curl 命令。",
		SystemPrompt:  liveToolScenarioSystemPrompt(),
		SelectedTools: selected,
		ContextPolicy: scenarioContextPolicy(),
	}
	results, err := r.runLiveAgentTurnsWithTimeout(ctx, workspace, req, []string{"请继续给出信息来源、不确定性，以及你实际使用了哪些工具。"})
	if err != nil {
		return r.failLiveScenario(resp, profile, "live-weather", selected, err, results)
	}
	if err := verifyLiveToolScenario(results, selected); err != nil {
		return r.failLiveScenario(resp, profile, "live-weather", selected, err, results)
	}
	resp.Status = "pass"
	resp.Summary = fmt.Sprintf("Live provider completed %d turn(s) using selected tools: %s.", len(results), strings.Join(selected, ", "))
	resp.Agent = liveScenarioAgent(profile, "live-weather", "Live weather query", results)
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) runLiveAgentTurnsWithTimeout(ctx context.Context, workspace string, req AgentRunRequest, followUps []string) ([]AgentRunResponse, error) {
	liveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		results []AgentRunResponse
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		liveRunner := NewRunner(workspace)
		liveRunner.EnvFile = r.EnvFile
		liveRunner.Now = r.Now
		liveRunner.Exec = r.Exec
		liveRunner.ProviderFactory = r.ProviderFactory
		liveRunner.Sessions = newAgentSessionRegistry()
		first := liveRunner.CreateAgentSession(liveCtx, req)
		results := []AgentRunResponse{first}
		if first.Status == string(engine.Completed) {
			for _, followUp := range followUps {
				next := liveRunner.RunAgentTurn(liveCtx, first.SessionID, AgentTurnRequest{Message: followUp})
				results = append(results, next)
				if next.Status != string(engine.Completed) {
					break
				}
			}
		}
		done <- outcome{results: results}
	}()
	timer := time.NewTimer(liveToolScenarioTimeout)
	defer timer.Stop()
	select {
	case out := <-done:
		return out.results, out.err
	case <-timer.C:
		cancel()
		return nil, fmt.Errorf("live tool scenario timed out after %s", liveToolScenarioTimeout)
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
}

func setupReadGrepShellScenario(ctx context.Context, workspace string) (toolScenarioRuntime, error) {
	if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("# Fixture\n\nStatus: green\nOwner: test-ui\n"), 0o644); err != nil {
		return toolScenarioRuntime{}, err
	}
	if err := os.MkdirAll(filepath.Join(workspace, "notes"), 0o755); err != nil {
		return toolScenarioRuntime{}, err
	}
	if err := os.WriteFile(filepath.Join(workspace, "notes", "plan.txt"), []byte("alpha\nbravo\nstatus=green\n"), 0o644); err != nil {
		return toolScenarioRuntime{}, err
	}
	prov := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Inspect files and command output."},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "list-root", Name: builtin.ToolList, Args: `{"path":null,"limit":20}`},
				{ID: "grep-status", Name: builtin.ToolGrep, Args: `{"pattern":"status","path":null,"glob":"*.txt","ignore_case":true,"literal":true,"context":null,"limit":20}`},
				{ID: "shell-date", Name: builtin.ToolShell, Args: `{"command":"printf scenario-shell-ok"}`},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Read the primary file before summarizing."},
			harness.Tool("read-readme", builtin.ToolRead, `{"path":"README.md","offset":0,"limit":40}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("Fixture status is green; shell returned scenario-shell-ok."), harness.Done()),
		harness.Step(harness.Text("Sources: README.md, notes/plan.txt via grep, and printf scenario-shell-ok."), harness.Done()),
	)
	return toolScenarioRuntime{Provider: prov, Profile: fakeScenarioProfile(), SystemPrompt: "Exercise read, grep, and shell tools deterministically."}, ctx.Err()
}

func setupMutationRecoveryScenario(ctx context.Context, workspace string) (toolScenarioRuntime, error) {
	missingPatch := strings.Join([]string{
		"*** Begin Patch",
		"*** Update File: reports/weather.md",
		"@@",
		"-Missing text",
		"+Recovered",
		"*** End Patch",
	}, "\n")
	recoveryPatch := strings.Join([]string{
		"*** Begin Patch",
		"*** Update File: reports/weather.md",
		"@@",
		"-Status: draft",
		"+Status: verified",
		"*** End Patch",
	}, "\n")
	prov := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Create the report before editing it."},
			harness.Tool("write-report", builtin.ToolWrite, `{"path":"reports/weather.md","content":"Status: draft\nCity: Changsha\n"}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Intentionally try the known-missing patch after the file exists."},
			harness.Tool("patch-missing", builtin.ToolApplyPatch, fmt.Sprintf(`{"patch":%q}`, missingPatch)),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Recover from the failed patch with the valid edit."},
			harness.Tool("patch-report", builtin.ToolApplyPatch, fmt.Sprintf(`{"patch":%q}`, recoveryPatch)),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Read the report only after the recovery patch completes."},
			harness.Tool("read-report", builtin.ToolRead, `{"path":"reports/weather.md","offset":0,"limit":40}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("Recovered from the failed patch and verified the report."), harness.Done()),
		harness.Step(
			harness.Tool("read-report-followup", builtin.ToolRead, `{"path":"reports/weather.md","offset":0,"limit":40}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("Final report content is verified."), harness.Done()),
	)
	return toolScenarioRuntime{Provider: prov, Profile: fakeScenarioProfile(), SystemPrompt: "Exercise mutation tools and error recovery deterministically."}, ctx.Err()
}

func setupSearchShellScenario(ctx context.Context, workspace string) (toolScenarioRuntime, error) {
	var searchMu sync.Mutex
	searchCount := 0
	var searchErr error
	contentServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/changsha-weather":
			_, _ = w.Write([]byte("Changsha 2026-06-03: thunderstorms and showers, 29-31C."))
		case "/tomorrow":
			_, _ = w.Write([]byte("Changsha 2026-06-04: cloudy then clearing, outdoor plans need caution."))
		default:
			http.NotFound(w, r)
		}
	}))
	searchServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		searchMu.Lock()
		searchCount++
		if r.Header.Get("X-Subscription-Token") != "test-key" {
			searchErr = fmt.Errorf("missing Brave search token header")
		}
		query := r.URL.Query()
		checks := map[string]string{
			"q":           "长沙 2026-06-03 天气",
			"count":       "3",
			"country":     "CN",
			"search_lang": "zh-hans",
			"freshness":   "pd",
		}
		for key, want := range checks {
			if got := query.Get(key); got != want && searchErr == nil {
				searchErr = fmt.Errorf("search query %s = %q, want %q", key, got, want)
			}
		}
		searchMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"web":{"results":[{"title":"Changsha weather","url":%q,"description":"Thunderstorms forecast.","profile":{"name":"Weather Source"}}]}}`, contentServer.URL+"/changsha-weather")))
	}))
	cleanup := func() {
		searchServer.Close()
		contentServer.Close()
	}
	prov := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Search and inspect current weather with bounded shell curl."},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "search-weather", Name: builtin.ToolWebSearch, Args: `{"query":"长沙 2026-06-03 天气","count":3,"country":"CN","search_lang":"zh-hans","freshness":"pd"}`},
				{ID: "curl-weather", Name: builtin.ToolShell, Args: boundedCurlArgs(contentServer.URL + "/changsha-weather")},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("长沙 2026-06-03 有雷阵雨/小雨，约 29-31C。"), harness.Done()),
		harness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "Inspect tomorrow after the user's follow-up with bounded shell curl."},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "curl-tomorrow", Name: builtin.ToolShell, Args: boundedCurlArgs(contentServer.URL + "/tomorrow")},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("明天多云转晴，出门仍要留意阵雨风险。"), harness.Done()),
	)
	return toolScenarioRuntime{
		Provider:     prov,
		Profile:      scenarioFakeExternalBraveSearchProfile(),
		SystemPrompt: "Exercise web_search and bounded shell curl as separate deterministic tools.",
		Env: map[string]string{
			braveSearchKey:      "test-key",
			braveSearchEndpoint: searchServer.URL,
		},
		Cleanup: cleanup,
		Check: func() error {
			searchMu.Lock()
			defer searchMu.Unlock()
			if searchErr != nil {
				return searchErr
			}
			if searchCount != 1 {
				return fmt.Errorf("Brave search request count = %d, want 1", searchCount)
			}
			return nil
		},
	}, ctx.Err()
}

func verifyReadGrepShellScenario(run toolScenarioRun) error {
	if err := verifyScenarioBase(run, 2); err != nil {
		return err
	}
	first := run.Results[0]
	for _, id := range []string{"list-root", "grep-status", "shell-date", "read-readme"} {
		if err := assertExactlyOneToolCallAndResult(first.Observation.SessionMessages, id); err != nil {
			return err
		}
	}
	if !strings.Contains(run.Results[1].Output, "README.md") {
		return fmt.Errorf("follow-up output did not cite README.md: %q", run.Results[1].Output)
	}
	return nil
}

func verifyMutationRecoveryScenario(run toolScenarioRun) error {
	if err := verifyScenarioBase(run, 2); err != nil {
		return err
	}
	first := run.Results[0]
	for _, id := range []string{"write-report", "patch-missing", "patch-report", "read-report"} {
		if err := assertExactlyOneToolCallAndResult(first.Observation.SessionMessages, id); err != nil {
			return err
		}
	}
	if !hasToolResultContaining(first.Observation.SessionMessages, "patch-missing", "did not match") {
		return fmt.Errorf("expected failed patch result to be visible")
	}
	if !hasToolResultContaining(first.Observation.SessionMessages, "read-report", "Status: verified") {
		return fmt.Errorf("expected recovered report readback")
	}
	if err := assertExactlyOneToolCallAndResult(run.Results[1].Observation.SessionMessages, "read-report-followup"); err != nil {
		return err
	}
	return nil
}

func verifySearchShellScenario(run toolScenarioRun) error {
	if err := verifyScenarioBase(run, 2); err != nil {
		return err
	}
	first := run.Results[0]
	for _, id := range []string{"search-weather", "curl-weather"} {
		if err := assertExactlyOneToolCallAndResult(first.Observation.SessionMessages, id); err != nil {
			return err
		}
	}
	if !hasToolResultContaining(first.Observation.SessionMessages, "search-weather", "Changsha weather") {
		return fmt.Errorf("expected web_search result content")
	}
	if !hasToolResultContaining(first.Observation.SessionMessages, "curl-weather", "29-31C") {
		return fmt.Errorf("expected shell curl weather content")
	}
	second := run.Results[1]
	if err := assertExactlyOneToolCallAndResult(second.Observation.SessionMessages, "curl-tomorrow"); err != nil {
		return err
	}
	if !hasToolResultContaining(second.Observation.SessionMessages, "curl-tomorrow", "2026-06-04") {
		return fmt.Errorf("expected second-turn shell curl tomorrow content")
	}
	if !latestRequestContainsUser(second.Observation.ProviderRequests, "那么明天会晴吗") {
		return fmt.Errorf("second-turn provider request did not include appended user follow-up")
	}
	if run.Runtime.Check != nil {
		if err := run.Runtime.Check(); err != nil {
			return err
		}
	}
	return nil
}

func verifyScenarioBase(run toolScenarioRun, wantTurns int) error {
	if len(run.Results) != wantTurns {
		return fmt.Errorf("%s returned %d turn(s), want %d", run.Scenario.ID, len(run.Results), wantTurns)
	}
	for i, result := range run.Results {
		if result.Status != string(engine.Completed) {
			return fmt.Errorf("%s turn %d status = %s, error = %s", run.Scenario.ID, i+1, result.Status, result.Error)
		}
		if result.Session.ID == "" {
			return fmt.Errorf("%s turn %d missing session snapshot", run.Scenario.ID, i+1)
		}
		latest := latestObservedRequest(result.Observation.ProviderRequests)
		if latest == nil {
			return fmt.Errorf("%s turn %d missing provider request observation", run.Scenario.ID, i+1)
		}
		if err := scenarioAssertProviderSafeToolHistory(latest.Messages); err != nil {
			return fmt.Errorf("%s turn %d provider history is unsafe: %w", run.Scenario.ID, i+1, err)
		}
	}
	return nil
}

func verifyLiveLocalToolScenario(results []AgentRunResponse) error {
	if len(results) != 2 {
		return fmt.Errorf("live local tool scenario returned %d turn(s), want 2", len(results))
	}
	first := results[0]
	if first.Status != string(engine.Completed) {
		return fmt.Errorf("live local first turn status = %s, error = %s", first.Status, first.Error)
	}
	for _, name := range []string{builtin.ToolList, builtin.ToolRead, builtin.ToolGrep} {
		if !hasAnyToolMessage(first.Observation.SessionMessages, name) {
			return fmt.Errorf("live local scenario did not call %s", name)
		}
		if !providerRequestExposesLocalTool(first.Observation.ProviderRequests, name) {
			return fmt.Errorf("live local scenario did not expose %s in provider request", name)
		}
	}
	for _, result := range results {
		for _, req := range result.Observation.ProviderRequests {
			if err := scenarioAssertProviderSafeToolHistory(req.Messages); err != nil {
				return fmt.Errorf("live local provider request history is unsafe: %w", err)
			}
		}
	}
	if results[1].Status != string(engine.Completed) {
		return fmt.Errorf("live local follow-up status = %s, error = %s", results[1].Status, results[1].Error)
	}
	return nil
}

func verifyLiveToolScenario(results []AgentRunResponse, selected []string) error {
	if len(results) == 0 {
		return fmt.Errorf("live scenario did not run")
	}
	first := results[0]
	if first.Status != string(engine.Completed) {
		return fmt.Errorf("live first turn status = %s, error = %s", first.Status, first.Error)
	}
	if len(first.Observation.ProviderRequests) == 0 {
		return fmt.Errorf("live scenario did not record a provider request")
	}
	exposure := newWebCapabilityExposure()
	for _, req := range first.Observation.ProviderRequests {
		exposure.Observe(req)
		if err := scenarioAssertProviderSafeToolHistory(req.Messages); err != nil {
			return fmt.Errorf("live provider request history is unsafe: %w", err)
		}
	}
	if err := exposure.Require(selected); err != nil {
		return err
	}
	for i, result := range results[1:] {
		if result.Status != string(engine.Completed) {
			return fmt.Errorf("live follow-up turn %d status = %s, error = %s", i+2, result.Status, result.Error)
		}
	}
	return nil
}

type webCapabilityExposure struct {
	hostedSearch bool
	localSearch  bool
}

func newWebCapabilityExposure() webCapabilityExposure {
	return webCapabilityExposure{}
}

func (e *webCapabilityExposure) Observe(req ObservedProviderRequest) {
	e.hostedSearch = e.hostedSearch || slices.ContainsFunc(req.HostedTools, func(tool provider.HostedToolDefinition) bool {
		return tool.Name == builtin.ToolWebSearch
	})
	e.localSearch = e.localSearch || slices.ContainsFunc(req.Tools, func(tool provider.ToolDefinition) bool {
		return tool.Name == builtin.ToolWebSearch
	})
}

func (e webCapabilityExposure) Require(selected []string) error {
	for _, name := range selected {
		switch name {
		case builtin.ToolWebSearch:
			if !e.hostedSearch && !e.localSearch {
				return fmt.Errorf("live scenario did not expose web_search as hosted or local tool")
			}
		}
	}
	if e.hostedSearch && e.localSearch {
		return fmt.Errorf("live scenario exposed hosted and local web_search at the same time")
	}
	return nil
}

func providerRequestExposesLocalTool(requests []ObservedProviderRequest, name string) bool {
	for _, req := range requests {
		if slices.ContainsFunc(req.Tools, func(tool provider.ToolDefinition) bool { return tool.Name == name }) {
			return true
		}
	}
	return false
}

func liveWeatherScenarioTools(profile ProviderProfile, envFile string) ([]string, error) {
	tools := []string{builtin.ToolWebSearch, builtin.ToolShell}
	selected, err := normalizeAgentSessionToolsForProfile(tools, profile, envFile)
	if err != nil {
		return nil, err
	}
	return selected, nil
}

func liveToolScenarioSystemPrompt() string {
	return strings.Join([]string{
		"You are Floret's live provider tool scenario runner.",
		"Call available tools when they are relevant; do not answer tool-backed requests from memory.",
		"Use available web_search when the user asks for web-backed information.",
		"For explicit URLs or HTTP APIs, use shell with bounded commands such as curl -fsSL URL | head -c 20000, jq, sed, or python, and keep max_output_bytes low.",
		"After using tools, provide a concise answer and mention uncertainty.",
	}, " ")
}

func boundedCurlArgs(rawURL string) string {
	command := fmt.Sprintf("curl -fsSL %q | head -c 2000", rawURL)
	return fmt.Sprintf(`{"command":%q,"workdir":null,"timeout_ms":5000,"max_output_bytes":2000}`, command)
}

func renderToolScenarioArtifact(scenario toolScenario, results []AgentRunResponse) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n%s\n\n", scenario.Title, scenario.Description)
	for i, result := range results {
		fmt.Fprintf(&b, "## Turn %d\n\nStatus: %s\nOutput: %s\n", i+1, result.Status, strings.TrimSpace(result.Output))
		if result.Error != "" {
			fmt.Fprintf(&b, "Error: %s\n", result.Error)
		}
		fmt.Fprintf(&b, "Provider requests: %d\nTool calls: %d\n\n", len(result.Observation.ProviderRequests), result.Metrics.ToolCalls)
	}
	return strings.TrimSpace(b.String())
}

func liveScenarioAgent(profile ProviderProfile, id string, title string, results []AgentRunResponse) *AgentRun {
	var last AgentRunResponse
	if len(results) > 0 {
		last = results[len(results)-1]
	}
	return &AgentRun{
		EngineStatus: last.Status,
		Output:       last.Output,
		Metrics:      last.Session.AggregateMetrics,
		Events:       last.Events,
		Config: ConfigInfo{
			Provider:     profile.Provider,
			Model:        profile.Model,
			LiveProvider: profile.Provider != "" && profile.Provider != config.ProviderFake,
			BaseURL:      redactURL(profile.BaseURL),
		},
		Artifacts: map[string]ArtifactSnapshot{
			"scenario": {
				Path:    id,
				Content: renderToolScenarioArtifact(toolScenario{ID: id, Title: title}, results),
			},
		},
	}
}

func (r Runner) failLiveScenario(resp RunResponse, profile ProviderProfile, id string, selected []string, err error, results []AgentRunResponse) RunResponse {
	resp.Status = "fail"
	resp.Error = err.Error()
	resp.Summary = err.Error()
	resp.Agent = liveScenarioAgent(profile, id, resp.Title, results)
	if resp.Agent.Artifacts == nil {
		resp.Agent.Artifacts = map[string]ArtifactSnapshot{}
	}
	resp.Agent.Artifacts["scenario"] = ArtifactSnapshot{
		Path:    id,
		Content: renderLiveFailureArtifact(resp.Title, profile, selected, err, results, r.EnvFile),
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func renderLiveFailureArtifact(title string, profile ProviderProfile, selected []string, err error, results []AgentRunResponse, envFile string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", title)
	fmt.Fprintf(&b, "Status: fail\n")
	fmt.Fprintf(&b, "Error: %s\n\n", err)
	fmt.Fprintf(&b, "Provider: %s\n", profile.Provider)
	fmt.Fprintf(&b, "Model: %s\n", profile.Model)
	fmt.Fprintf(&b, "Selected tools: %s\n", strings.Join(selected, ", "))
	if slices.Contains(selected, builtin.ToolWebSearch) {
		resolved, resolveErr := searchcap.Resolve(searchcap.ResolveInput{
			Provider:       profile.Provider,
			Capability:     profile.WebSearch,
			BraveAvailable: searchOptionsFromEnvFile(envFile).APIKey != "",
		})
		if resolveErr != nil {
			fmt.Fprintf(&b, "web_search source: invalid (%s)\n", resolveErr)
		} else {
			fmt.Fprintf(&b, "web_search source: %s (%s)\n", resolved.Source, resolved.Status)
			if resolved.WireShape != "" {
				fmt.Fprintf(&b, "web_search wire shape: %s\n", resolved.WireShape)
			}
		}
	}
	if len(results) == 0 {
		fmt.Fprintf(&b, "\nNo completed provider response was captured before the scenario timeout.\n")
		return strings.TrimSpace(b.String())
	}
	fmt.Fprintf(&b, "\n%s\n", renderToolScenarioArtifact(toolScenario{Title: title}, results))
	return strings.TrimSpace(b.String())
}

func appendDotEnvValue(path, key, value string) error {
	if key == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%s=%s\n", key, envQuote(value))
	return err
}

func scenarioContextPolicy() config.ContextPolicy {
	return config.ContextPolicy{
		ContextWindowTokens: 128000,
		MaxOutputTokens:     2048,
		RecentTailTokens:    4096,
	}
}

func fakeScenarioProfile() ProviderProfile {
	return ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "unused"}
}

func scenarioFakeExternalBraveSearchProfile() ProviderProfile {
	return ProviderProfile{
		ID:       "fake-external-brave-search",
		Name:     "Fake external Brave search",
		Provider: config.ProviderFake,
		Model:    "fake-model",
		WebSearch: searchcap.Capability{
			Source: searchcap.WebSearchExternalBrave,
			Brave:  searchcap.BraveConfig{Provider: searchcap.ExternalProviderBrave},
		},
	}
}

func latestObservedRequest(requests []ObservedProviderRequest) *ObservedProviderRequest {
	if len(requests) == 0 {
		return nil
	}
	return &requests[len(requests)-1]
}

func latestRequestContainsUser(requests []ObservedProviderRequest, text string) bool {
	latest := latestObservedRequest(requests)
	if latest == nil {
		return false
	}
	return slices.ContainsFunc(latest.Messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "user" && strings.Contains(msg.Content, text)
	})
}

func assertExactlyOneToolCallAndResult(messages []ObservedSessionMessage, callID string) error {
	if got := scenarioCountObservedToolMessages(messages, "assistant", callID); got != 1 {
		return fmt.Errorf("assistant tool call %s count = %d", callID, got)
	}
	if got := scenarioCountObservedToolMessages(messages, "tool", callID); got != 1 {
		return fmt.Errorf("tool result %s count = %d", callID, got)
	}
	return nil
}

func scenarioCountObservedToolMessages(messages []ObservedSessionMessage, role, callID string) int {
	count := 0
	for _, msg := range messages {
		if msg.Role == role && msg.ToolCallID == callID {
			count++
		}
	}
	return count
}

func scenarioAssertProviderSafeToolHistory(messages []ObservedSessionMessage) error {
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

func hasToolResultContaining(messages []ObservedSessionMessage, callID string, text string) bool {
	return slices.ContainsFunc(messages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "tool" && msg.ToolCallID == callID && strings.Contains(msg.Content, text)
	})
}

func hasAnyToolMessage(messages []ObservedSessionMessage, name string) bool {
	return slices.ContainsFunc(messages, func(msg ObservedSessionMessage) bool {
		return msg.ToolName == name
	})
}
