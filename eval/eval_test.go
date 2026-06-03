package eval

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

func TestRunnerPassesOnlyWhenEngineCompletesAndOraclePasses(t *testing.T) {
	workspace := t.TempDir()
	artifacts := filepath.Join(t.TempDir(), "artifacts")
	rec := &event.Recorder{}
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[writeArgs](
		tools.Definition{
			Name: "write",
			InputSchema: tools.StrictObject(map[string]any{
				"content": tools.String("content"),
			}, []string{"content"}),
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[writeArgs]) (tools.Result, error) {
			return tools.Result{Text: "wrote"}, os.WriteFile(filepath.Join(workspace, "answer.txt"), []byte(inv.Args.Content), 0o644)
		},
	)); err != nil {
		t.Fatal(err)
	}
	runner := Runner{
		Suite:        "smoke",
		AgentVersion: "test",
		Provider:     "fake",
		Model:        "fake",
		Workspace:    workspace,
		ArtifactsDir: artifacts,
		Trace:        rec,
		Engine: &engine.Engine{
			Provider: harness.NewScriptedProvider(
				harness.Step(
					harness.Usage(provider.Usage{InputTokens: 10, OutputTokens: 2}),
					harness.Tool("write-1", "write", `{"content":"done"}`),
				),
				harness.Step(harness.Text("ok"), harness.Done()),
			),
			Store:   session.NewMemoryStore(),
			Memory:  &memory.Manager{SystemPrompt: "test"},
			Tools:   registry,
			Sink:    rec,
			Options: engine.Options{RunID: "eval"},
		},
	}
	result, err := runner.Run(context.Background(), Case{
		ID:       "write-one-file",
		Category: "smoke",
		Prompt:   "finish",
		Oracle:   Oracle{ExpectedFiles: map[string]string{"answer.txt": "done"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Pass || result.EngineStatus != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if result.Metrics.Usage.TotalTokens != 12 {
		t.Fatalf("metrics = %#v", result.Metrics)
	}
	if result.Artifacts["oracle_log"] == "" || result.Artifacts["final_diff"] == "" {
		t.Fatalf("artifacts missing: %#v", result.Artifacts)
	}
	if result.Artifacts["trace"] == "" {
		t.Fatalf("trace artifact missing: %#v", result.Artifacts)
	}
}

type writeArgs struct {
	Content string `json:"content"`
}

func TestRunnerFailsWhenOracleFailsDespiteNaturalCompletion(t *testing.T) {
	workspace := t.TempDir()
	runner := Runner{
		Workspace: workspace,
		Engine: newEvalEngine(harness.NewScriptedProvider(harness.Step(
			harness.Text("ok"),
			harness.Done(),
		))),
	}
	result, err := runner.Run(context.Background(), Case{
		ID:     "oracle-fail",
		Prompt: "finish",
		Oracle: Oracle{ExpectedFiles: map[string]string{"missing.txt": "done"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Fail || result.FailureCategory != "oracle_failed" {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunnerWritesArtifactsForEngineFailure(t *testing.T) {
	workspace := t.TempDir()
	artifacts := filepath.Join(t.TempDir(), "artifacts")
	rec := &event.Recorder{}
	runner := Runner{
		Workspace:    workspace,
		ArtifactsDir: artifacts,
		Trace:        rec,
		Engine: &engine.Engine{
			Provider: harness.NewScriptedProvider(harness.Step(harness.Usage(provider.Usage{InputTokens: 101}))),
			Store:    session.NewMemoryStore(),
			Memory:   &memory.Manager{SystemPrompt: "test"},
			Tools:    tools.NewRegistry(),
			Sink:     rec,
			Options:  engine.Options{RunID: "eval"},
		},
	}
	result, err := runner.Run(context.Background(), Case{
		ID:      "engine-failure",
		Prompt:  "finish",
		Budgets: Budgets{MaxTotalTokens: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Fail {
		t.Fatalf("result = %#v", result)
	}
	for _, key := range []string{"trace", "final_diff"} {
		path := result.Artifacts[key]
		if path == "" {
			t.Fatalf("artifact %s missing: %#v", key, result.Artifacts)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) == 0 {
			t.Fatalf("artifact %s is empty", key)
		}
	}
}

func TestCompareDetectsPassAndCostRegression(t *testing.T) {
	baseline := []Result{{Status: Pass, Metrics: engine.RunMetrics{Usage: provider.Usage{CostUSD: 1}}}}
	candidate := []Result{{Status: Fail, Metrics: engine.RunMetrics{Usage: provider.Usage{CostUSD: 2}}}}
	got := Compare(baseline, candidate)
	if !got.Regressed || got.BaselinePasses != 1 || got.CandidatePasses != 0 || got.CostDeltaUSD != 1 {
		t.Fatalf("comparison = %#v", got)
	}
}

func TestWriteJSONPersistsMachineReadableResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	want := Result{CaseID: "case", Status: Pass, Metrics: engine.RunMetrics{Usage: provider.Usage{InputTokens: 1}}}
	if err := WriteJSON(path, want); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got Result
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("result is not JSON: %v", err)
	}
	if got.CaseID != "case" || got.Status != Pass || got.Metrics.Usage.InputTokens != 1 {
		t.Fatalf("got = %#v", got)
	}
}

func TestRunnerAppliesBudgetsAndRejectsUnsafeOraclePaths(t *testing.T) {
	workspace := t.TempDir()
	runner := Runner{
		Workspace: workspace,
		Engine: newEvalEngine(harness.NewScriptedProvider(harness.Step(
			harness.Usage(provider.Usage{InputTokens: 101}),
			harness.Text("ok"),
			harness.Done(),
		))),
	}
	result, err := runner.Run(context.Background(), Case{
		ID:      "budget",
		Prompt:  "finish",
		Budgets: Budgets{MaxTotalTokens: 100},
		Oracle:  Oracle{ExpectedFiles: map[string]string{"../escape": "bad"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Fail || result.FailureCategory != "cost_budget_exceeded" {
		t.Fatalf("result = %#v", result)
	}

	runner.Engine = newEvalEngine(harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done())))
	result, err = runner.Run(context.Background(), Case{
		ID:     "unsafe-path",
		Prompt: "finish",
		Oracle: Oracle{ExpectedFiles: map[string]string{"../escape": "bad"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Fail || result.FailureCategory != "oracle_failed" || !strings.Contains(result.OracleLog, "path traversal") {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunnerCapturesGitDiffArtifact(t *testing.T) {
	workspace := t.TempDir()
	runCmd(t, workspace, "git init")
	runCmd(t, workspace, "git config user.email test@example.com")
	runCmd(t, workspace, "git config user.name Test")
	if err := os.WriteFile(filepath.Join(workspace, "answer.txt"), []byte("before"), 0o644); err != nil {
		t.Fatal(err)
	}
	runCmd(t, workspace, "git add answer.txt && git commit -m initial")
	if err := os.WriteFile(filepath.Join(workspace, "answer.txt"), []byte("after"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifacts := filepath.Join(t.TempDir(), "artifacts")
	runner := Runner{
		Workspace:    workspace,
		ArtifactsDir: artifacts,
		Engine:       newEvalEngine(harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))),
	}
	result, err := runner.Run(context.Background(), Case{
		ID:     "diff",
		Prompt: "finish",
		Oracle: Oracle{ExpectedFiles: map[string]string{"answer.txt": "after"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	diff, err := os.ReadFile(result.Artifacts["final_diff"])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(diff), "-before") || !strings.Contains(string(diff), "+after") {
		t.Fatalf("diff artifact did not contain real git diff:\n%s", diff)
	}
}

func runCmd(t *testing.T, dir string, command string) {
	t.Helper()
	cmd := exec.Command("sh", "-lc", command)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s failed: %v\n%s", command, err, output)
	}
}

func newEvalEngine(p provider.Provider) *engine.Engine {
	return &engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "eval"},
	}
}
