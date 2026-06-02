package eval

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
)

type Case struct {
	ID       string
	Title    string
	Category string
	Prompt   string
	Budgets  Budgets
	Oracle   Oracle
}

type Budgets struct {
	MaxWallTime    time.Duration
	MaxTotalTokens int64
	MaxCostUSD     float64
}

type Oracle struct {
	Commands       []string
	ExpectedFiles  map[string]string
	ForbiddenFiles []string
}

type Status string

const (
	Pass    Status = "pass"
	Fail    Status = "fail"
	Timeout Status = "timeout"
	Error   Status = "error"
)

type Result struct {
	CaseID          string            `json:"case_id"`
	Suite           string            `json:"suite,omitempty"`
	AgentVersion    string            `json:"agent_version,omitempty"`
	Provider        string            `json:"provider,omitempty"`
	Model           string            `json:"model,omitempty"`
	Status          Status            `json:"status"`
	FailureCategory string            `json:"failure_category,omitempty"`
	EngineStatus    engine.Status     `json:"engine_status"`
	EngineError     string            `json:"engine_error,omitempty"`
	Metrics         engine.RunMetrics `json:"metrics"`
	OracleLog       string            `json:"oracle_log,omitempty"`
	Artifacts       map[string]string `json:"artifacts,omitempty"`
}

type Runner struct {
	Suite        string
	AgentVersion string
	Provider     string
	Model        string
	Workspace    string
	ArtifactsDir string
	Engine       *engine.Engine
	Trace        *event.Recorder
}

func (r Runner) Run(ctx context.Context, c Case) (Result, error) {
	if c.ID == "" {
		return Result{}, errors.New("eval case id is required")
	}
	if c.Prompt == "" {
		return Result{}, errors.New("eval case prompt is required")
	}
	if r.Engine == nil {
		return Result{}, errors.New("engine is required")
	}
	artifacts := map[string]string{}
	if r.ArtifactsDir != "" {
		if err := os.MkdirAll(r.ArtifactsDir, 0o755); err != nil {
			return Result{}, err
		}
	}
	writeArtifacts := func(result *Result) {
		if r.ArtifactsDir == "" {
			return
		}
		if r.Trace != nil {
			tracePath := filepath.Join(r.ArtifactsDir, c.ID+"-trace.jsonl")
			if err := writeTrace(tracePath, r.Trace.Snapshot()); err == nil {
				artifacts["trace"] = tracePath
			}
		}
		if result.OracleLog != "" {
			oraclePath := filepath.Join(r.ArtifactsDir, c.ID+"-oracle.log")
			_ = os.WriteFile(oraclePath, []byte(result.OracleLog), 0o644)
			artifacts["oracle_log"] = oraclePath
		}
		diffPath := filepath.Join(r.ArtifactsDir, c.ID+"-final.diff")
		_ = os.WriteFile(diffPath, []byte(r.workspaceDiff(ctx)), 0o644)
		artifacts["final_diff"] = diffPath
	}
	applyBudgets(r.Engine, c.Budgets)
	start := time.Now()
	eng := r.Engine.Run(ctx, c.Prompt)
	result := Result{
		CaseID:       c.ID,
		Suite:        r.Suite,
		AgentVersion: r.AgentVersion,
		Provider:     r.Provider,
		Model:        r.Model,
		EngineStatus: eng.Status,
		Metrics:      eng.Metrics,
		Artifacts:    artifacts,
	}
	if eng.Err != nil {
		result.EngineError = eng.Err.Error()
		result.FailureCategory = classify(eng.Err)
	}
	if c.Budgets.MaxWallTime > 0 && time.Since(start) > c.Budgets.MaxWallTime {
		result.Status = Timeout
		result.FailureCategory = "timeout"
		writeArtifacts(&result)
		return result, nil
	}
	if eng.Status != engine.Completed {
		result.Status = Fail
		if result.FailureCategory == "" {
			result.FailureCategory = "loop_guard_hit"
		}
		writeArtifacts(&result)
		return result, nil
	}
	oracleLog, err := r.runOracle(ctx, c)
	result.OracleLog = oracleLog
	if err != nil {
		result.Status = Fail
		result.FailureCategory = "oracle_failed"
		writeArtifacts(&result)
		return result, nil
	}
	result.Status = Pass
	writeArtifacts(&result)
	return result, nil
}

func (r Runner) runOracle(ctx context.Context, c Case) (string, error) {
	var log bytes.Buffer
	for path, want := range c.Oracle.ExpectedFiles {
		full, err := safeJoin(r.Workspace, path)
		if err != nil {
			fmt.Fprintf(&log, "unsafe path %s: %v\n", path, err)
			return log.String(), err
		}
		got, err := os.ReadFile(full)
		if err != nil {
			fmt.Fprintf(&log, "read %s: %v\n", path, err)
			return log.String(), err
		}
		if string(got) != want {
			fmt.Fprintf(&log, "file %s mismatch\n", path)
			return log.String(), fmt.Errorf("file %s mismatch", path)
		}
		fmt.Fprintf(&log, "file %s matched\n", path)
	}
	for _, path := range c.Oracle.ForbiddenFiles {
		full, err := safeJoin(r.Workspace, path)
		if err != nil {
			fmt.Fprintf(&log, "unsafe path %s: %v\n", path, err)
			return log.String(), err
		}
		if _, err := os.Stat(full); err == nil {
			fmt.Fprintf(&log, "forbidden file %s exists\n", path)
			return log.String(), fmt.Errorf("forbidden file %s exists", path)
		}
	}
	for _, command := range c.Oracle.Commands {
		cmd := exec.CommandContext(ctx, "sh", "-lc", command)
		cmd.Dir = r.Workspace
		output, err := cmd.CombinedOutput()
		fmt.Fprintf(&log, "$ %s\n%s", command, output)
		if err != nil {
			return log.String(), err
		}
	}
	return log.String(), nil
}

func applyBudgets(e *engine.Engine, b Budgets) {
	if b.MaxWallTime > 0 {
		e.Options.WallTime = b.MaxWallTime
	}
	if b.MaxTotalTokens > 0 {
		e.Options.MaxTotalTokens = b.MaxTotalTokens
	}
	if b.MaxCostUSD > 0 {
		e.Options.MaxCostUSD = b.MaxCostUSD
	}
}

func writeTrace(path string, events []event.Event) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := event.NewTraceWriter(f)
	for _, ev := range events {
		writer.Emit(ev)
	}
	return nil
}

func (r Runner) workspaceDiff(ctx context.Context) string {
	if r.Workspace == "" {
		return "workspace unavailable\n"
	}
	cmd := exec.CommandContext(ctx, "git", "diff", "--no-ext-diff")
	cmd.Dir = r.Workspace
	output, err := cmd.CombinedOutput()
	if err != nil {
		if len(output) == 0 {
			return fmt.Sprintf("git diff unavailable: %v\n", err)
		}
		return string(output)
	}
	if len(output) == 0 {
		return "no diff\n"
	}
	return string(output)
}

func safeJoin(root, rel string) (string, error) {
	if root == "" {
		return "", errors.New("workspace is required")
	}
	if filepath.IsAbs(rel) {
		return "", errors.New("absolute paths are not allowed")
	}
	clean := filepath.Clean(rel)
	parts := strings.Split(clean, string(os.PathSeparator))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || slices.Contains(parts, "..") {
		return "", errors.New("path traversal is not allowed")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	full := filepath.Join(rootAbs, clean)
	relToRoot, err := filepath.Rel(rootAbs, full)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(relToRoot, "..") || filepath.IsAbs(relToRoot) {
		return "", errors.New("path escapes workspace")
	}
	return full, nil
}

func WriteJSON(path string, result Result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

type Comparison struct {
	BaselinePasses  int     `json:"baseline_passes"`
	CandidatePasses int     `json:"candidate_passes"`
	CostDeltaUSD    float64 `json:"cost_delta_usd"`
	Regressed       bool    `json:"regressed"`
}

func Compare(baseline, candidate []Result) Comparison {
	bp, cp := 0, 0
	bc, cc := 0.0, 0.0
	for _, result := range baseline {
		if result.Status == Pass {
			bp++
		}
		bc += result.Metrics.Usage.CostUSD
	}
	for _, result := range candidate {
		if result.Status == Pass {
			cp++
		}
		cc += result.Metrics.Usage.CostUSD
	}
	return Comparison{
		BaselinePasses:  bp,
		CandidatePasses: cp,
		CostDeltaUSD:    cc - bc,
		Regressed:       cp < bp || cc > bc*1.2 && bc > 0,
	}
}

func classify(err error) string {
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "budget"):
		return "cost_budget_exceeded"
	case strings.Contains(text, "context"):
		return "context_missing"
	case strings.Contains(text, "tool"):
		return "wrong_tool"
	case strings.Contains(text, "deadline") || strings.Contains(text, "timeout"):
		return "timeout"
	default:
		return "provider_error"
	}
}
