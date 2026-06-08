package builtin

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/floegence/floret/tools"
)

type CommandRunner interface {
	Run(context.Context, CommandRequest) (CommandResult, error)
}

type CommandRequest struct {
	Command   string
	Workdir   string
	TimeoutMS int
}

type CommandResult struct {
	Stdout     string
	Stderr     string
	ExitCode   int
	DurationMS int64
}

type shellArgs struct {
	Command        string  `json:"command"`
	Workdir        *string `json:"workdir"`
	TimeoutMS      *int    `json:"timeout_ms"`
	MaxOutputBytes *int    `json:"max_output_bytes"`
}

func RegisterShell(reg *tools.Registry, opts ShellOptions) error {
	if opts.CWD == "" {
		opts.CWD = "."
	}
	abs, err := filepath.Abs(opts.CWD)
	if err != nil {
		return err
	}
	opts.CWD = abs
	if opts.DefaultTimeoutMS <= 0 {
		opts.DefaultTimeoutMS = 30_000
	}
	if opts.MaxOutputBytes <= 0 {
		opts.MaxOutputBytes = 64 * 1024
	}
	if opts.Runner == nil {
		opts.Runner = localCommandRunner{}
	}
	return reg.Register(shellTool(opts))
}

func shellTool(opts ShellOptions) tools.Tool {
	return tools.Define[shellArgs](
		tools.Definition{
			Name:        "shell",
			Title:       "Shell",
			Description: "Run a non-interactive shell command in the workspace. Commands run from the configured workspace root by default; set workdir only when a different directory is needed. timeout_ms and max_output_bytes have runtime defaults. Stdin is closed; use read/grep/list/apply_patch/write for file operations when possible. For explicit URL or HTTP API access, use bounded commands such as curl -fsSL URL | head -c 20000, jq, sed, or python, and keep max_output_bytes low.",
			InputSchema: tools.StrictObject(map[string]any{
				"command":          tools.String("Shell command to execute."),
				"workdir":          tools.Nullable(tools.String("Optional working directory. Omit or use null for the configured workspace root.")),
				"timeout_ms":       tools.Nullable(tools.Integer("Optional timeout in milliseconds. Omit or use null for the configured default.")),
				"max_output_bytes": tools.Nullable(tools.Integer("Optional maximum output bytes visible to the model. Full output is preserved when projection truncates it.")),
			}, []string{"command"}),
			Effects:      []tools.Effect{tools.EffectShell},
			OpenWorld:    true,
			Destructive:  true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"command", "directory"}},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: opts.MaxOutputBytes, Strategy: tools.OutputTail, PreserveFull: true},
		},
		nil,
		func(inv tools.Invocation[shellArgs]) ([]tools.ResourceRef, error) {
			workdir := opts.CWD
			if inv.Args.Workdir != nil && *inv.Args.Workdir != "" {
				if filepath.IsAbs(*inv.Args.Workdir) {
					workdir = *inv.Args.Workdir
				} else {
					workdir = filepath.Join(opts.CWD, *inv.Args.Workdir)
				}
			}
			return []tools.ResourceRef{{Kind: "command", Value: inv.Args.Command}, {Kind: "directory", Value: workdir}}, nil
		},
		func(ctx context.Context, inv tools.Invocation[shellArgs]) (tools.Result, error) {
			timeout := valueOr(inv.Args.TimeoutMS, opts.DefaultTimeoutMS)
			workdir := opts.CWD
			if inv.Args.Workdir != nil && *inv.Args.Workdir != "" {
				if filepath.IsAbs(*inv.Args.Workdir) {
					workdir = *inv.Args.Workdir
				} else {
					workdir = filepath.Join(opts.CWD, *inv.Args.Workdir)
				}
			}
			result, err := opts.Runner.Run(ctx, CommandRequest{Command: inv.Args.Command, Workdir: workdir, TimeoutMS: timeout})
			if err != nil {
				return tools.Result{}, err
			}
			limit := valueOr(inv.Args.MaxOutputBytes, opts.MaxOutputBytes)
			metadata := map[string]any{}
			metadata["exit_code"] = result.ExitCode
			metadata["duration_ms"] = result.DurationMS
			metadata["workdir"] = workdir
			return tools.Result{
				Title:    inv.Args.Command,
				Text:     combinedShellOutput(result),
				Metadata: metadata,
				OutputPolicy: &tools.OutputPolicy{
					VisibleMaxBytes: limit,
					Strategy:        tools.OutputTail,
					PreserveFull:    true,
				},
				IsError: result.ExitCode != 0,
			}, nil
		},
	)
}

type localCommandRunner struct{}

func (localCommandRunner) Run(ctx context.Context, req CommandRequest) (CommandResult, error) {
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now()
	cmd := exec.CommandContext(runCtx, "sh", "-lc", req.Command)
	cmd.Dir = req.Workdir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		if runCtx.Err() != nil {
			return CommandResult{}, runCtx.Err()
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			exit = exitErr.ExitCode()
		} else {
			return CommandResult{}, err
		}
	}
	return CommandResult{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exit, DurationMS: time.Since(started).Milliseconds()}, nil
}

func combinedShellOutput(result CommandResult) string {
	var parts []string
	if result.Stdout != "" {
		parts = append(parts, result.Stdout)
	}
	if result.Stderr != "" {
		parts = append(parts, "stderr:\n"+result.Stderr)
	}
	text := strings.TrimRight(strings.Join(parts, "\n"), "\n")
	if text == "" {
		text = fmt.Sprintf("(no output, exit %d)", result.ExitCode)
	}
	return text
}
