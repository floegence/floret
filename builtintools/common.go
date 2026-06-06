package builtintools

import (
	"fmt"
	"net/http"
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

type SearchOptions struct {
	Provider         string
	APIKey           string
	Endpoint         string
	DefaultTimeoutMS int
	HTTPClient       *http.Client
}

type SelectedOptions struct {
	Workspace WorkspaceOptions
	Shell     ShellOptions
	Search    SearchOptions
}

const (
	ToolRead       = "read"
	ToolList       = "list"
	ToolGlob       = "glob"
	ToolGrep       = "grep"
	ToolApplyPatch = "apply_patch"
	ToolWrite      = "write"
	ToolShell      = "shell"
	ToolWebSearch  = "web_search"
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
		case ToolWrite:
			errs = append(errs, reg.Register(writeTool(workspace)))
		case ToolShell:
			errs = append(errs, RegisterShell(reg, opts.Shell))
		case ToolWebSearch:
			errs = append(errs, RegisterSearch(reg, opts.Search))
		default:
			errs = append(errs, fmt.Errorf("unknown built-in tool %q", name))
		}
	}
	return mergeRegisterErrors(errs...)
}

func workspaceOptionsForSelection(opts WorkspaceOptions, names []string) (WorkspaceOptions, error) {
	for _, name := range names {
		switch name {
		case ToolRead, ToolList, ToolGlob, ToolGrep, ToolApplyPatch, ToolWrite:
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
	if !allowExternal {
		if err := ensureWithinWorkspace(root, full, path); err != nil {
			return "", "", err
		}
	}
	return full, filepath.ToSlash(rel), nil
}

func ensureWithinWorkspace(root, full, originalPath string) error {
	rootReal, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("workspace root is not accessible: %w", err)
	}
	fullReal, err := filepath.EvalSymlinks(full)
	if err == nil {
		return ensureRealPathWithin(rootReal, fullReal, originalPath)
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(full)
	for {
		parentReal, parentErr := filepath.EvalSymlinks(parent)
		if parentErr == nil {
			return ensureRealPathWithin(rootReal, parentReal, originalPath)
		}
		if !os.IsNotExist(parentErr) {
			return parentErr
		}
		next := filepath.Dir(parent)
		if next == parent {
			return fmt.Errorf("path escapes workspace: %s", originalPath)
		}
		parent = next
	}
}

func ensureRealPathWithin(rootReal, targetReal, originalPath string) error {
	rel, err := filepath.Rel(rootReal, targetReal)
	if err != nil {
		return err
	}
	if rel == "." || (!strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != "..") {
		return nil
	}
	return fmt.Errorf("path escapes workspace via symlink: %s", originalPath)
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
