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
