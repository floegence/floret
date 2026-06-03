package builtintools

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/floegence/floret/tools"
)

type WorkspaceOptions struct {
	Root              string
	MaxReadBytes      int
	MaxReadLines      int
	AllowExternalRead bool
}

type ShellOptions struct {
	CWD              string
	Runner           CommandRunner
	DefaultTimeoutMS int
	MaxOutputBytes   int
}

type NetworkOptions struct {
	DefaultTimeoutMS int
	MaxBytes         int
	AllowPrivateIPs  bool
}

type SelectedOptions struct {
	Workspace WorkspaceOptions
	Shell     ShellOptions
	Network   NetworkOptions
}

const (
	ToolRead       = "read"
	ToolList       = "list"
	ToolGlob       = "glob"
	ToolGrep       = "grep"
	ToolApplyPatch = "apply_patch"
	ToolEdit       = "edit"
	ToolWrite      = "write"
	ToolShell      = "shell"
	ToolWebFetch   = "web_fetch"
)

func RegisterSelected(reg *tools.Registry, opts SelectedOptions, names ...string) error {
	names = normalizeSelectedToolNames(names)
	workspace, err := workspaceOptionsForSelection(opts.Workspace, names)
	if err != nil {
		return err
	}
	var errs []error
	for _, name := range names {
		switch name {
		case ToolRead:
			errs = append(errs, reg.Register(readTool(workspace)))
		case ToolList:
			errs = append(errs, reg.Register(listTool(workspace)))
		case ToolGlob:
			errs = append(errs, reg.Register(globTool(workspace)))
		case ToolGrep:
			errs = append(errs, reg.Register(grepTool(workspace)))
		case ToolApplyPatch:
			errs = append(errs, reg.Register(applyPatchTool(workspace)))
		case ToolEdit:
			errs = append(errs, reg.Register(editTool(workspace)))
		case ToolWrite:
			errs = append(errs, reg.Register(writeTool(workspace)))
		case ToolShell:
			errs = append(errs, RegisterShell(reg, opts.Shell))
		case ToolWebFetch:
			errs = append(errs, RegisterNetwork(reg, opts.Network))
		default:
			errs = append(errs, fmt.Errorf("unknown built-in tool %q", name))
		}
	}
	return mergeRegisterErrors(errs...)
}

func workspaceOptionsForSelection(opts WorkspaceOptions, names []string) (WorkspaceOptions, error) {
	for _, name := range names {
		switch name {
		case ToolRead, ToolList, ToolGlob, ToolGrep, ToolApplyPatch, ToolEdit, ToolWrite:
			return normalizeWorkspaceOptions(opts)
		}
	}
	return opts, nil
}

func normalizeSelectedToolNames(names []string) []string {
	out := make([]string, 0, len(names))
	seen := map[string]struct{}{}
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func normalizeWorkspaceOptions(opts WorkspaceOptions) (WorkspaceOptions, error) {
	root := opts.Root
	if root == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return opts, err
	}
	if opts.MaxReadBytes <= 0 {
		opts.MaxReadBytes = 64 * 1024
	}
	if opts.MaxReadLines <= 0 {
		opts.MaxReadLines = 400
	}
	opts.Root = abs
	return opts, nil
}

func safeJoin(root, path string, allowExternal bool) (string, string, error) {
	if strings.TrimSpace(path) == "" {
		path = "."
	}
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) {
		if allowExternal {
			return clean, clean, nil
		}
		return "", "", fmt.Errorf("absolute paths are outside the workspace: %s", path)
	}
	full := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, full)
	if err != nil {
		return "", "", err
	}
	if rel == "." {
		return full, rel, nil
	}
	if strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." {
		return "", "", fmt.Errorf("path escapes workspace: %s", path)
	}
	return full, filepath.ToSlash(rel), nil
}

func resource(kind, value string) []tools.ResourceRef {
	if value == "" {
		return nil
	}
	return []tools.ResourceRef{{Kind: kind, Value: value}}
}

func mergeRegisterErrors(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
