package testui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/floegence/floret/adapters"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/eval"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

const (
	TargetUnit          = "unit"
	TargetRace          = "race"
	TargetEvalDemo      = "eval-demo"
	TargetProviderSmoke = "provider-smoke"
	TargetAll           = "all"
)

type Runner struct {
	Root    string
	EnvFile string
	Now     func() time.Time
	Exec    func(context.Context, string, []string, string, []string) ([]byte, int)
}

func NewRunner(root string) Runner {
	return Runner{
		Root:    root,
		EnvFile: filepath.Join(root, config.DefaultEnvFile),
		Now:     time.Now,
		Exec:    execCommand,
	}
}

func (r Runner) ConfigInfo() ConfigInfo {
	info := ConfigInfo{EnvFile: r.EnvFile}
	if _, err := os.Stat(r.EnvFile); err == nil {
		info.EnvFileFound = true
	}
	cfg, err := config.Load(config.WithPath(r.EnvFile))
	if err != nil {
		info.Provider = "invalid"
		info.Model = err.Error()
		return info
	}
	info.Provider = cfg.Provider
	info.Model = cfg.Model
	info.LiveProvider = cfg.Provider != "" && cfg.Provider != config.ProviderFake
	info.BaseURL = redactURL(cfg.BaseURL)
	return info
}

func (r Runner) Catalog() []CatalogProvider {
	return modelcatalog.Providers()
}

func (r Runner) RunAgent(ctx context.Context, req AgentRunRequest) AgentRunResponse {
	started := r.now()
	resp := AgentRunResponse{
		ID:        fmt.Sprintf("%d", started.UnixNano()),
		StartedAt: started,
	}
	if strings.TrimSpace(req.Message) == "" {
		resp.Status = "error"
		resp.Error = "message is required"
		resp.Summary = resp.Error
		resp.FinishedAt = r.now()
		resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
		return resp
	}
	profile, err := r.profileByID(req.ProfileID)
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	resp.Profile = stripProfileSecret(profile)
	cfg := config.Config{
		Provider:                profile.Provider,
		Model:                   profile.Model,
		BaseURL:                 profile.BaseURL,
		APIKey:                  profile.APIKey,
		FakeResponse:            profile.FakeResponse,
		RunID:                   "testui-agent-" + resp.ID,
		SystemPrompt:            strings.TrimSpace(req.SystemPrompt),
		MaxContextMessages:      req.MaxContextMessages,
		MaxSteps:                req.MaxSteps,
		HardMaxSteps:            req.MaxSteps,
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
		WallTime:                60 * time.Second,
	}
	if cfg.SystemPrompt == "" {
		cfg.SystemPrompt = "You are Floret. Use task_complete when the user's request is complete, or ask_user if you need missing information."
	}
	if cfg.MaxContextMessages <= 0 {
		cfg.MaxContextMessages = 32
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 8
		cfg.HardMaxSteps = 8
	}
	cfg, err = config.Resolve(cfg, nil)
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	resp.Profile = stripProfileSecret(ProviderProfile{
		ID:           profile.ID,
		Name:         profile.Name,
		Provider:     cfg.Provider,
		Model:        cfg.Model,
		BaseURL:      cfg.BaseURL,
		APIKey:       cfg.APIKey,
		APIKeySet:    cfg.APIKey != "" || profile.APIKeySet,
		FakeResponse: cfg.FakeResponse,
	})
	p, err := adapters.NewProvider(cfg)
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	observed := newObservingProvider(p)
	rec := &event.Recorder{}
	store := session.NewMemoryStore()
	promptStore := promptcache.NewFileStore(filepath.Join(r.Root, ".floret-test-ui", "prompt-cache"))
	registry := tools.NewRegistry()
	if err := registerSignalTools(registry); err != nil {
		return r.failAgentRun(resp, err)
	}
	eng := &engine.Engine{
		Provider: observed,
		Store:    store,
		Prompt:   promptStore,
		Memory: &memory.Manager{
			SystemPrompt: cfg.SystemPrompt,
			MaxMessages:  cfg.MaxContextMessages,
		},
		Tools: registry,
		Sink:  rec,
		Options: engine.Options{
			RunID:                   cfg.RunID,
			SessionID:               cfg.RunID,
			TraceID:                 cfg.RunID,
			ProviderName:            cfg.Provider,
			Model:                   cfg.Model,
			CacheRetention:          config.PromptCacheRetention(cfg),
			MaxSteps:                cfg.MaxSteps,
			HardMaxSteps:            cfg.HardMaxSteps,
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
			MaxTotalTokens:          8000,
			MaxCostUSD:              1.00,
		},
	}
	result := eng.Run(ctx, req.Message)
	messages, _ := store.Messages(cfg.RunID)
	observation := observed.Snapshot()
	observation.SessionMessages = observeMessages(messages)
	observation.Transitions = buildTransitions(rec.Snapshot(), result)
	resp.Status = string(result.Status)
	resp.Output = result.Output
	resp.Metrics = result.Metrics
	resp.Events = rec.Snapshot()
	resp.Observation = observation
	if result.Err != nil {
		resp.Error = result.Err.Error()
	}
	resp.Summary = agentSummary(result)
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) failAgentRun(resp AgentRunResponse, err error) AgentRunResponse {
	resp.Status = "error"
	resp.Error = err.Error()
	resp.Summary = err.Error()
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) Run(ctx context.Context, target string) RunResponse {
	if target == "" {
		target = TargetUnit
	}
	started := r.now()
	resp := RunResponse{
		ID:        fmt.Sprintf("%d", started.UnixNano()),
		Target:    target,
		StartedAt: started,
	}
	switch target {
	case TargetUnit:
		resp.Title = "Go package tests"
		resp.Kind = "command"
		resp = r.runGoTest(ctx, resp, false)
	case TargetRace:
		resp.Title = "Race-enabled package tests"
		resp.Kind = "command"
		resp = r.runGoTest(ctx, resp, true)
	case TargetEvalDemo:
		resp.Title = "Deterministic agent eval demo"
		resp.Kind = "agent"
		resp = r.runEvalDemo(ctx, resp)
	case TargetProviderSmoke:
		resp.Title = "Configured provider smoke"
		resp.Kind = "agent"
		resp = r.runProviderSmoke(ctx, resp)
	case TargetAll:
		resp.Title = "Full local suite"
		resp.Kind = "suite"
		resp = r.runAll(ctx, resp)
	default:
		resp.Status = "error"
		resp.Summary = "Unknown target."
		resp.Error = fmt.Sprintf("unknown run target %q", target)
		resp.FinishedAt = r.now()
		resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	}
	return resp
}

func (r Runner) runAll(ctx context.Context, resp RunResponse) RunResponse {
	targets := []string{TargetUnit, TargetRace, TargetEvalDemo}
	cfg := r.ConfigInfo()
	if cfg.LiveProvider || cfg.Provider == config.ProviderFake {
		targets = append(targets, TargetProviderSmoke)
	}
	status := "pass"
	for _, target := range targets {
		part := r.Run(ctx, target)
		resp.Parts = append(resp.Parts, part)
		if part.Status != "pass" && status == "pass" {
			status = part.Status
		}
	}
	resp.Status = status
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	resp.Summary = fmt.Sprintf("%d checks finished, status %s.", len(resp.Parts), status)
	return resp
}

func (r Runner) runGoTest(ctx context.Context, resp RunResponse, race bool) RunResponse {
	args := []string{"test", "-json"}
	if race {
		args = append(args, "-race")
	}
	args = append(args, "./...")
	resp.Command = append([]string{"go"}, args...)
	env := append(os.Environ(), "GOFLAGS=")
	output, exitCode := r.exec(ctx, "go", args, r.Root, env)
	resp.Output = string(output)
	resp.ExitCode = exitCode
	resp.Packages, resp.TestTotals = parseGoTestJSON(output)
	if ctx.Err() != nil {
		resp.Status = "error"
		resp.Error = ctx.Err().Error()
	} else if exitCode == 0 && resp.TestTotals.Failed == 0 {
		resp.Status = "pass"
	} else {
		resp.Status = "fail"
		resp.Error = "go test reported failures"
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	resp.Summary = fmt.Sprintf("%d packages, %d tests, %d failed.", resp.TestTotals.Packages, resp.TestTotals.Tests, resp.TestTotals.Failed)
	return resp
}

func (r Runner) runEvalDemo(ctx context.Context, resp RunResponse) RunResponse {
	workspace, err := r.newRunWorkspace("eval")
	if err != nil {
		return r.failAgent(resp, err)
	}
	if err := initDemoGitWorkspace(ctx, workspace); err != nil {
		return r.failAgent(resp, err)
	}
	rec := &event.Recorder{}
	registry := tools.NewRegistry()
	if err := registerSignalTools(registry); err != nil {
		return r.failAgent(resp, err)
	}
	if err := registry.Register(tools.Tool{
		Name:        "write_file",
		Description: "Write a text file in the eval workspace.",
		Handler: func(_ context.Context, args string) (string, error) {
			path, content, err := parseWriteArgs(args)
			if err != nil {
				return "", err
			}
			if filepath.IsAbs(path) || strings.Contains(filepath.Clean(path), "..") {
				return "", fmt.Errorf("unsafe path %q", path)
			}
			full := filepath.Join(workspace, filepath.Clean(path))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				return "", err
			}
			if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
				return "", err
			}
			return "wrote " + path, nil
		},
	}); err != nil {
		return r.failAgent(resp, err)
	}
	prov := harness.NewScriptedProvider(
		harness.Step(
			harness.Usage(provider.Usage{InputTokens: 42, OutputTokens: 8, CostUSD: 0, Source: provider.UsageNative}),
			harness.Tool("write-readme", "write_file", "RESULT.txt=floret eval passed\n"),
			harness.Done(),
		),
		harness.Step(
			harness.Usage(provider.Usage{InputTokens: 18, OutputTokens: 5, Source: provider.UsageEstimated}),
			harness.Tool("done", "task_complete", "Created RESULT.txt and verified the eval oracle."),
			harness.Done(),
		),
	)
	eng := &engine.Engine{
		Provider: prov,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "You are a deterministic Floret eval agent.", MaxMessages: 8},
		Tools:    registry,
		Sink:     rec,
		Options: engine.Options{
			RunID:              "testui-eval-demo",
			SessionID:          "testui-eval-demo",
			TraceID:            "testui-eval-demo",
			ProviderName:       "scripted",
			Model:              "scripted-eval",
			MaxSteps:           4,
			MaxTotalTokens:     200,
			DuplicateToolLimit: 3,
		},
	}
	artifactsDir := filepath.Join(workspace, "artifacts")
	result, err := eval.Runner{
		Suite:        "test-ui",
		AgentVersion: "local",
		Provider:     "scripted",
		Model:        "scripted-eval",
		Workspace:    workspace,
		ArtifactsDir: artifactsDir,
		Engine:       eng,
		Trace:        rec,
	}.Run(ctx, eval.Case{
		ID:       "write-result",
		Title:    "Write and verify RESULT.txt",
		Category: "smoke",
		Prompt:   "Create RESULT.txt with the expected text.",
		Budgets:  eval.Budgets{MaxSteps: 4, MaxTotalTokens: 200},
		Oracle:   eval.Oracle{ExpectedFiles: map[string]string{"RESULT.txt": "floret eval passed\n"}},
	})
	if err != nil {
		return r.failAgent(resp, err)
	}
	events := rec.Snapshot()
	resp.Agent = &AgentRun{
		EngineStatus: string(result.EngineStatus),
		Output:       "Created RESULT.txt and verified the eval oracle.",
		Metrics:      result.Metrics,
		Events:       events,
		Eval:         &result,
		Artifacts:    readArtifacts(result.Artifacts),
		Config:       ConfigInfo{Provider: "scripted", Model: "scripted-eval"},
	}
	resp.Status = string(result.Status)
	if result.Status == eval.Pass {
		resp.Summary = fmt.Sprintf("Oracle passed with %d steps, %d tool call, %d tokens.", result.Metrics.Steps, result.Metrics.ToolCalls, result.Metrics.Usage.Normalized().TotalTokens)
	} else {
		resp.Summary = fmt.Sprintf("Eval failed: %s.", result.FailureCategory)
		resp.Error = result.EngineError
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) runProviderSmoke(ctx context.Context, resp RunResponse) RunResponse {
	cfg, err := config.Load(config.WithPath(r.EnvFile))
	if err != nil {
		return r.failAgent(resp, err)
	}
	cfg.RunID = fmt.Sprintf("testui-provider-%d", r.now().UnixNano())
	if cfg.WallTime == 0 {
		cfg.WallTime = 45 * time.Second
	}
	if cfg.MaxSteps <= 0 || cfg.MaxSteps > 4 {
		cfg.MaxSteps = 4
	}
	if cfg.HardMaxSteps < cfg.MaxSteps {
		cfg.HardMaxSteps = cfg.MaxSteps
	}
	p, err := adapters.NewProvider(cfg)
	if err != nil {
		return r.failAgent(resp, err)
	}
	rec := &event.Recorder{}
	registry := tools.NewRegistry()
	promptStore := promptcache.NewFileStore(filepath.Join(r.Root, ".floret-test-ui", "prompt-cache"))
	if err := registerSignalTools(registry); err != nil {
		return r.failAgent(resp, err)
	}
	eng := &engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Prompt:   promptStore,
		Memory: &memory.Manager{
			SystemPrompt: "You are Floret's smoke-test assistant. Complete the request using task_complete with a short success message. Do not call normal tools.",
			MaxMessages:  cfg.MaxContextMessages,
		},
		Tools: registry,
		Sink:  rec,
		Options: engine.Options{
			RunID:                   cfg.RunID,
			SessionID:               cfg.RunID,
			TraceID:                 cfg.RunID,
			ProviderName:            cfg.Provider,
			Model:                   cfg.Model,
			CacheRetention:          config.PromptCacheRetention(cfg),
			MaxSteps:                cfg.MaxSteps,
			HardMaxSteps:            cfg.HardMaxSteps,
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
			MaxTotalTokens:          4000,
			MaxCostUSD:              0.25,
		},
	}
	result := eng.Run(ctx, "Reply with a concise confirmation that the configured provider can run Floret.")
	resp.Agent = &AgentRun{
		EngineStatus: string(result.Status),
		Output:       result.Output,
		Metrics:      result.Metrics,
		Events:       rec.Snapshot(),
		Config:       r.ConfigInfo(),
	}
	if result.Err != nil {
		resp.Error = result.Err.Error()
	}
	if result.Status == engine.Completed {
		resp.Status = "pass"
		resp.Summary = fmt.Sprintf("Provider completed in %d step(s) with %d total tokens.", result.Metrics.Steps, result.Metrics.Usage.Normalized().TotalTokens)
	} else {
		resp.Status = "fail"
		resp.Summary = fmt.Sprintf("Provider ended with engine status %s.", result.Status)
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) newRunWorkspace(prefix string) (string, error) {
	root := filepath.Join(r.Root, ".floret-test-ui", "runs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	return os.MkdirTemp(root, prefix+"-*")
}

func registerSignalTools(registry *tools.Registry) error {
	if err := registry.Register(tools.Tool{
		Name:        "task_complete",
		Description: "Signal that the task is complete. The argument is the final answer.",
		Handler: func(context.Context, string) (string, error) {
			return "", nil
		},
	}); err != nil {
		return err
	}
	if err := registry.Register(tools.Tool{
		Name:        "ask_user",
		Description: "Ask the user for missing information. The argument is the question to show.",
		Handler: func(context.Context, string) (string, error) {
			return "", nil
		},
	}); err != nil {
		return err
	}
	return nil
}

func (r Runner) failAgent(resp RunResponse, err error) RunResponse {
	resp.Status = "error"
	resp.Error = err.Error()
	resp.Summary = err.Error()
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func buildTransitions(events []event.Event, result engine.Result) []StateTransition {
	transitions := []StateTransition{}
	current := "created"
	add := func(at time.Time, step int, to string, reason string, details string) {
		if to == "" || to == current {
			return
		}
		transitions = append(transitions, StateTransition{
			At:      at,
			Step:    step,
			From:    current,
			To:      to,
			Reason:  reason,
			Details: details,
		})
		current = to
	}
	for _, ev := range events {
		switch ev.Type {
		case event.StepStart:
			add(ev.Timestamp, ev.Step, "step_running", "step_start", fmt.Sprintf("step %d started", ev.Step))
		case event.ProviderRequest:
			add(ev.Timestamp, ev.Step, "provider_waiting", "provider_request", ev.Message)
		case event.ProviderDelta:
			add(ev.Timestamp, ev.Step, "receiving_model_output", "provider_delta", trimForDisplay(ev.Message, 80))
		case event.ProviderRetry:
			add(ev.Timestamp, ev.Step, "retrying_provider", "provider_retry", ev.Message)
		case event.ContextCompact:
			add(ev.Timestamp, ev.Step, "compacting_context", "context_compact", "context was compacted")
		case event.ToolCall:
			add(ev.Timestamp, ev.Step, "tool_calling", "tool_call", ev.ToolName)
		case event.ToolResult:
			add(ev.Timestamp, ev.Step, "tool_result_received", "tool_result", eventDetails(ev))
		case event.BudgetExceeded:
			add(ev.Timestamp, ev.Step, "budget_exceeded", "budget_exceeded", ev.Message)
		case event.StepEnd:
			add(ev.Timestamp, ev.Step, "step_finished", "step_end", fmt.Sprintf("step %d ended", ev.Step))
		case event.RunEnd:
			add(ev.Timestamp, ev.Step, string(result.Status), "run_end", eventDetails(ev))
		}
	}
	if len(transitions) == 0 {
		add(time.Now(), 0, string(result.Status), "run_end", "")
	}
	return transitions
}

func agentSummary(result engine.Result) string {
	switch result.Status {
	case engine.Completed:
		return fmt.Sprintf("Completed in %d step(s), %d provider request(s), %d normal tool call(s).", result.Metrics.Steps, result.Metrics.LLMRequests, result.Metrics.ToolCalls)
	case engine.Waiting:
		return "The agent stopped to ask the user for more information."
	case engine.Cancelled:
		return "The run was cancelled or timed out."
	case engine.Failed:
		if result.Err != nil {
			return "The agent failed: " + result.Err.Error()
		}
		return "The agent failed."
	default:
		return "The agent run finished."
	}
}

func trimForDisplay(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func eventDetails(ev event.Event) string {
	if ev.Err != "" {
		return ev.Err
	}
	if ev.Result != "" {
		return trimForDisplay(ev.Result, 120)
	}
	return trimForDisplay(ev.Message, 120)
}

func (r Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r Runner) exec(ctx context.Context, name string, args []string, dir string, env []string) ([]byte, int) {
	if r.Exec != nil {
		return r.Exec(ctx, name, args, dir, env)
	}
	return execCommand(ctx, name, args, dir, env)
}

func execCommand(ctx context.Context, name string, args []string, dir string, env []string) ([]byte, int) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	if err == nil {
		return output, 0
	}
	var exitErr *exec.ExitError
	if ok := errors.As(err, &exitErr); ok {
		return output, exitErr.ExitCode()
	}
	return append(output, []byte(err.Error())...), 1
}

func initDemoGitWorkspace(ctx context.Context, workspace string) error {
	for _, args := range [][]string{
		{"init"},
		{"config", "user.email", "test@example.com"},
		{"config", "user.name", "Test"},
	} {
		if err := runGit(ctx, workspace, args...); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(workspace, "RESULT.txt"), []byte("before\n"), 0o644); err != nil {
		return err
	}
	for _, args := range [][]string{
		{"add", "RESULT.txt"},
		{"commit", "-m", "initial"},
	} {
		if err := runGit(ctx, workspace, args...); err != nil {
			return err
		}
	}
	return nil
}

func runGit(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, output)
	}
	return nil
}

func parseWriteArgs(args string) (string, string, error) {
	path, content, ok := strings.Cut(args, "=")
	if !ok {
		return "", "", fmt.Errorf("expected PATH=CONTENT")
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return "", "", fmt.Errorf("path is required")
	}
	return path, content, nil
}

func readArtifacts(paths map[string]string) map[string]ArtifactSnapshot {
	out := map[string]ArtifactSnapshot{}
	for key, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			out[key] = ArtifactSnapshot{Path: path, Content: err.Error()}
			continue
		}
		text := string(data)
		if len(text) > 20000 {
			text = text[:20000] + "\n...[truncated]"
		}
		out[key] = ArtifactSnapshot{Path: path, Content: text}
	}
	return out
}

func parseGoTestJSON(output []byte) ([]PackageSummary, TestTotals) {
	type testEvent struct {
		Action  string  `json:"Action"`
		Package string  `json:"Package"`
		Test    string  `json:"Test"`
		Elapsed float64 `json:"Elapsed"`
	}
	packages := map[string]*PackageSummary{}
	order := []string{}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var ev testEvent
		if err := json.Unmarshal(line, &ev); err != nil || ev.Package == "" {
			continue
		}
		pkg := packages[ev.Package]
		if pkg == nil {
			pkg = &PackageSummary{Name: ev.Package}
			packages[ev.Package] = pkg
			order = append(order, ev.Package)
		}
		if ev.Test != "" {
			switch ev.Action {
			case "pass":
				pkg.Passed++
			case "fail":
				pkg.Failed++
			case "skip":
				pkg.Skipped++
			}
			if ev.Action == "pass" || ev.Action == "fail" || ev.Action == "skip" {
				pkg.Tests++
			}
		} else {
			switch ev.Action {
			case "pass", "fail", "skip":
				pkg.Status = ev.Action
				pkg.ElapsedSec = ev.Elapsed
			}
		}
	}
	out := make([]PackageSummary, 0, len(order))
	totals := TestTotals{}
	for _, name := range order {
		pkg := *packages[name]
		if pkg.Status == "" {
			pkg.Status = "unknown"
		}
		out = append(out, pkg)
		totals.Packages++
		totals.Tests += pkg.Tests
		totals.Passed += pkg.Passed
		totals.Failed += pkg.Failed
		totals.Skipped += pkg.Skipped
	}
	return out, totals
}

func redactURL(raw string) string {
	if raw == "" {
		return ""
	}
	if at := strings.LastIndex(raw, "@"); at >= 0 {
		if scheme := strings.Index(raw, "://"); scheme >= 0 {
			return raw[:scheme+3] + "redacted@" + raw[at+1:]
		}
		return "redacted@" + raw[at+1:]
	}
	return raw
}
