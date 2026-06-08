package builtin

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/floegence/floret/tools"
)

type readArgs struct {
	Path   string `json:"path"`
	Offset *int   `json:"offset"`
	Limit  *int   `json:"limit"`
}

type listArgs struct {
	Path  *string `json:"path"`
	Limit *int    `json:"limit"`
}

type globArgs struct {
	Pattern    string  `json:"pattern"`
	Path       *string `json:"path"`
	Limit      *int    `json:"limit"`
	IgnoreCase bool    `json:"ignore_case"`
}

type grepArgs struct {
	Pattern    string  `json:"pattern"`
	Path       *string `json:"path"`
	Glob       *string `json:"glob"`
	IgnoreCase bool    `json:"ignore_case"`
	Literal    bool    `json:"literal"`
	Context    *int    `json:"context"`
	Limit      *int    `json:"limit"`
}

func RegisterReadOnlyWorkspace(reg *tools.Registry, opts WorkspaceOptions) error {
	opts, err := normalizeWorkspaceOptions(opts)
	if err != nil {
		return err
	}
	return mergeRegisterErrors(
		reg.Register(readTool(opts)),
		reg.Register(listTool(opts)),
		reg.Register(globTool(opts)),
		reg.Register(grepTool(opts)),
	)
}

func readTool(opts WorkspaceOptions) tools.Tool {
	schema := tools.StrictObject(map[string]any{
		"path":   tools.String("Workspace-relative file or directory path to read."),
		"offset": tools.Nullable(tools.Integer("Zero-based line offset for text files.")),
		"limit":  tools.Nullable(tools.Integer("Maximum number of lines to return.")),
	}, []string{"path"})
	return tools.Define[readArgs](
		tools.Definition{
			Name:         "read",
			Title:        "Read",
			Description:  "Read a workspace text file or list a directory. Use offset and limit to continue large files.",
			InputSchema:  schema,
			Effects:      []tools.Effect{tools.EffectRead},
			ReadOnly:     true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow, ResourceKinds: []string{"file", "directory"}},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: opts.MaxReadBytes, VisibleMaxLines: opts.MaxReadLines, Strategy: tools.OutputHead, PreserveFull: true},
		},
		nil,
		func(inv tools.Invocation[readArgs]) ([]tools.ResourceRef, error) {
			_, rel, err := safeJoin(opts.Root, inv.Args.Path, opts.AllowExternalRead)
			if err != nil {
				return nil, err
			}
			return resource("file", rel), nil
		},
		func(ctx context.Context, inv tools.Invocation[readArgs]) (tools.Result, error) {
			if err := ctx.Err(); err != nil {
				return tools.Result{}, err
			}
			full, rel, err := safeJoin(opts.Root, inv.Args.Path, opts.AllowExternalRead)
			if err != nil {
				return tools.Result{}, err
			}
			info, err := os.Stat(full)
			if err != nil {
				return tools.Result{}, err
			}
			if info.IsDir() {
				text, err := listDirectory(full, valueOr(inv.Args.Limit, 200))
				if err != nil {
					return tools.Result{}, err
				}
				return tools.Result{Title: "Directory " + rel, Text: text, Metadata: map[string]any{"path": rel, "kind": "directory"}}, nil
			}
			data, err := os.ReadFile(full)
			if err != nil {
				return tools.Result{}, err
			}
			if bytes.IndexByte(data[:min(len(data), 8000)], 0) >= 0 {
				return tools.Result{}, fmt.Errorf("file %s appears to be binary", rel)
			}
			text := string(data)
			lines := strings.Split(text, "\n")
			offset := max(0, valueOr(inv.Args.Offset, 0))
			limit := valueOr(inv.Args.Limit, opts.MaxReadLines)
			if offset > len(lines) {
				offset = len(lines)
			}
			end := len(lines)
			if limit > 0 && offset+limit < end {
				end = offset + limit
			}
			return tools.Result{
				Title: "Read " + rel,
				Text:  strings.Join(lines[offset:end], "\n"),
				Metadata: map[string]any{
					"path":        rel,
					"line_start":  offset,
					"line_end":    end,
					"total_lines": len(lines),
				},
			}, nil
		},
	)
}

func listTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[listArgs](
		tools.Definition{
			Name:        "list",
			Title:       "List",
			Description: "List entries in a workspace directory. Directories are suffixed with /.",
			InputSchema: tools.StrictObject(map[string]any{
				"path":  tools.Nullable(tools.String("Workspace-relative directory path. Use null for the workspace root.")),
				"limit": tools.Nullable(tools.Integer("Maximum number of entries to return.")),
			}, []string{}),
			Effects:      []tools.Effect{tools.EffectRead},
			ReadOnly:     true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow, ResourceKinds: []string{"directory"}},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: opts.MaxReadBytes, VisibleMaxLines: opts.MaxReadLines, Strategy: tools.OutputHead, PreserveFull: true},
		},
		nil,
		func(inv tools.Invocation[listArgs]) ([]tools.ResourceRef, error) {
			path := "."
			if inv.Args.Path != nil {
				path = *inv.Args.Path
			}
			_, rel, err := safeJoin(opts.Root, path, opts.AllowExternalRead)
			if err != nil {
				return nil, err
			}
			return resource("directory", rel), nil
		},
		func(ctx context.Context, inv tools.Invocation[listArgs]) (tools.Result, error) {
			if err := ctx.Err(); err != nil {
				return tools.Result{}, err
			}
			path := "."
			if inv.Args.Path != nil {
				path = *inv.Args.Path
			}
			full, rel, err := safeJoin(opts.Root, path, opts.AllowExternalRead)
			if err != nil {
				return tools.Result{}, err
			}
			text, err := listDirectory(full, valueOr(inv.Args.Limit, 200))
			if err != nil {
				return tools.Result{}, err
			}
			return tools.Result{Title: "List " + rel, Text: text, Metadata: map[string]any{"path": rel}}, nil
		},
	)
}

func globTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[globArgs](
		tools.Definition{
			Name:        "glob",
			Title:       "Glob",
			Description: "Find workspace files by path glob pattern. Use grep for file contents.",
			InputSchema: tools.StrictObject(map[string]any{
				"pattern":     tools.String("Glob pattern, for example **/*.go."),
				"path":        tools.Nullable(tools.String("Workspace-relative directory to search from.")),
				"limit":       tools.Nullable(tools.Integer("Maximum number of paths to return.")),
				"ignore_case": tools.Boolean("Whether matching ignores case."),
			}, []string{"pattern"}),
			Effects:      []tools.Effect{tools.EffectRead},
			ReadOnly:     true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow, ResourceKinds: []string{"directory"}},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: opts.MaxReadBytes, VisibleMaxLines: opts.MaxReadLines, Strategy: tools.OutputHead, PreserveFull: true},
		},
		nil,
		func(inv tools.Invocation[globArgs]) ([]tools.ResourceRef, error) {
			path := "."
			if inv.Args.Path != nil {
				path = *inv.Args.Path
			}
			_, rel, err := safeJoin(opts.Root, path, opts.AllowExternalRead)
			if err != nil {
				return nil, err
			}
			return resource("directory", rel), nil
		},
		func(ctx context.Context, inv tools.Invocation[globArgs]) (tools.Result, error) {
			path := "."
			if inv.Args.Path != nil {
				path = *inv.Args.Path
			}
			full, _, err := safeJoin(opts.Root, path, opts.AllowExternalRead)
			if err != nil {
				return tools.Result{}, err
			}
			limit := valueOr(inv.Args.Limit, 200)
			var matches []string
			err = filepath.WalkDir(full, func(current string, d fs.DirEntry, err error) error {
				if err != nil || ctx.Err() != nil {
					return firstErr(err, ctx.Err())
				}
				if d.IsDir() && d.Name() == ".git" {
					return filepath.SkipDir
				}
				rel, err := filepath.Rel(opts.Root, current)
				if err != nil {
					return err
				}
				rel = filepath.ToSlash(rel)
				if rel == "." {
					return nil
				}
				pattern := inv.Args.Pattern
				candidate := rel
				if inv.Args.IgnoreCase {
					pattern = strings.ToLower(pattern)
					candidate = strings.ToLower(candidate)
				}
				ok, err := filepath.Match(strings.ReplaceAll(pattern, "**/", "*"), candidate)
				if err != nil {
					return err
				}
				if ok || simpleGlobMatch(pattern, candidate) {
					if d.IsDir() {
						rel += "/"
					}
					matches = append(matches, rel)
					if limit > 0 && len(matches) >= limit {
						return fs.SkipAll
					}
				}
				return nil
			})
			if err != nil {
				return tools.Result{}, err
			}
			slices.Sort(matches)
			return tools.Result{Title: "Glob " + inv.Args.Pattern, Text: strings.Join(matches, "\n"), Metadata: map[string]any{"matches": len(matches)}}, nil
		},
	)
}

func grepTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[grepArgs](
		tools.Definition{
			Name:        "grep",
			Title:       "Grep",
			Description: "Search workspace file contents. Returns path:line matches with optional context.",
			InputSchema: tools.StrictObject(map[string]any{
				"pattern":     tools.String("Regex or literal pattern to search for."),
				"path":        tools.Nullable(tools.String("Workspace-relative directory or file to search.")),
				"glob":        tools.Nullable(tools.String("File glob filter, for example *.go.")),
				"ignore_case": tools.Boolean("Whether matching ignores case."),
				"literal":     tools.Boolean("Whether to treat pattern as literal text."),
				"context":     tools.Nullable(tools.Integer("Number of context lines around each match.")),
				"limit":       tools.Nullable(tools.Integer("Maximum number of matching lines to return.")),
			}, []string{"pattern"}),
			Effects:      []tools.Effect{tools.EffectRead},
			ReadOnly:     true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow, ResourceKinds: []string{"directory", "file"}},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: opts.MaxReadBytes, VisibleMaxLines: opts.MaxReadLines, Strategy: tools.OutputHead, PreserveFull: true},
		},
		nil,
		func(inv tools.Invocation[grepArgs]) ([]tools.ResourceRef, error) {
			path := "."
			if inv.Args.Path != nil {
				path = *inv.Args.Path
			}
			_, rel, err := safeJoin(opts.Root, path, opts.AllowExternalRead)
			if err != nil {
				return nil, err
			}
			return resource("file", rel), nil
		},
		func(ctx context.Context, inv tools.Invocation[grepArgs]) (tools.Result, error) {
			path := "."
			if inv.Args.Path != nil {
				path = *inv.Args.Path
			}
			full, rel, err := safeJoin(opts.Root, path, opts.AllowExternalRead)
			if err != nil {
				return tools.Result{}, err
			}
			args := []string{"--line-number", "--no-heading", "--color", "never"}
			if inv.Args.IgnoreCase {
				args = append(args, "--ignore-case")
			}
			if inv.Args.Literal {
				args = append(args, "--fixed-strings")
			}
			if inv.Args.Context != nil && *inv.Args.Context > 0 {
				args = append(args, "-C", fmt.Sprintf("%d", *inv.Args.Context))
			}
			if inv.Args.Glob != nil && *inv.Args.Glob != "" {
				args = append(args, "--glob", *inv.Args.Glob)
			}
			args = append(args, inv.Args.Pattern, full)
			cmd := exec.CommandContext(ctx, "rg", args...)
			cmd.Dir = opts.Root
			output, err := cmd.CombinedOutput()
			if err != nil {
				if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 {
					return tools.Result{Title: "Grep " + inv.Args.Pattern, Text: "", Metadata: map[string]any{"path": rel, "matches": 0}}, nil
				}
				return tools.Result{}, fmt.Errorf("rg failed: %w: %s", err, strings.TrimSpace(string(output)))
			}
			lines := splitLimit(string(output), valueOr(inv.Args.Limit, 200))
			return tools.Result{Title: "Grep " + inv.Args.Pattern, Text: strings.Join(lines, "\n"), Metadata: map[string]any{"path": rel, "matches": len(lines)}}, nil
		},
	)
}

func listDirectory(path string, limit int) (string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			name += "/"
		}
		names = append(names, name)
	}
	slices.Sort(names)
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}
	return strings.Join(names, "\n"), nil
}

func splitLimit(text string, limit int) []string {
	scanner := bufio.NewScanner(strings.NewReader(text))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if limit > 0 && len(lines) >= limit {
			break
		}
	}
	return lines
}

func valueOr(ptr *int, fallback int) int {
	if ptr == nil {
		return fallback
	}
	return *ptr
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func simpleGlobMatch(pattern, rel string) bool {
	if strings.HasPrefix(pattern, "**/") {
		ok, _ := filepath.Match(strings.TrimPrefix(pattern, "**/"), filepath.Base(rel))
		return ok
	}
	if strings.Contains(pattern, "**") {
		needle := strings.Trim(pattern, "*")
		return needle != "" && strings.Contains(rel, needle)
	}
	return false
}
