package builtintools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/floegence/floret/tools"
)

type applyPatchArgs struct {
	Patch string `json:"patch"`
}

type editArgs struct {
	Path       string `json:"path"`
	OldText    string `json:"old_text"`
	NewText    string `json:"new_text"`
	ReplaceAll bool   `json:"replace_all"`
}

type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func RegisterWorkspaceMutation(reg *tools.Registry, opts WorkspaceOptions) error {
	opts, err := normalizeWorkspaceOptions(opts)
	if err != nil {
		return err
	}
	return mergeRegisterErrors(
		reg.Register(applyPatchTool(opts)),
		reg.Register(editTool(opts)),
		reg.Register(writeTool(opts)),
	)
}

func applyPatchTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[applyPatchArgs](
		tools.Definition{
			Name:        "apply_patch",
			Title:       "Apply patch",
			Description: "Apply a structured patch to workspace files. Use this for multi-file edits with explicit add, update, and delete sections.",
			InputSchema: tools.StrictObject(map[string]any{
				"patch": tools.String("Patch text beginning with *** Begin Patch and ending with *** End Patch."),
			}, []string{"patch"}),
			Effects:     []tools.Effect{tools.EffectWrite},
			Destructive: true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}},
			ResultLimit: tools.ResultLimit{MaxBytes: 32 * 1024, MaxLines: 400, Strategy: "head"},
		},
		nil,
		func(inv tools.Invocation[applyPatchArgs]) ([]tools.ResourceRef, error) {
			ops, err := parsePatch(inv.Args.Patch)
			if err != nil {
				return nil, err
			}
			refs := make([]tools.ResourceRef, 0, len(ops))
			for _, op := range ops {
				_, rel, err := safeJoin(opts.Root, op.path, false)
				if err != nil {
					return nil, err
				}
				refs = append(refs, tools.ResourceRef{Kind: "file", Value: rel})
			}
			return refs, nil
		},
		func(ctx context.Context, inv tools.Invocation[applyPatchArgs]) (tools.Result, error) {
			if err := ctx.Err(); err != nil {
				return tools.Result{}, err
			}
			ops, err := parsePatch(inv.Args.Patch)
			if err != nil {
				return tools.Result{}, err
			}
			if err := applyParsedPatch(opts.Root, ops); err != nil {
				return tools.Result{}, err
			}
			files := make([]string, 0, len(ops))
			for _, op := range ops {
				files = append(files, op.path)
			}
			return tools.Result{Title: "Applied patch", Text: "applied patch to " + strings.Join(files, ", "), Metadata: map[string]any{"files": files}}, nil
		},
	)
}

func editTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[editArgs](
		tools.Definition{
			Name:        "edit",
			Title:       "Edit",
			Description: "Replace exact text in one workspace file. By default old_text must match exactly once.",
			InputSchema: tools.StrictObject(map[string]any{
				"path":        tools.String("Workspace-relative file path."),
				"old_text":    tools.String("Exact text to replace."),
				"new_text":    tools.String("Replacement text."),
				"replace_all": tools.Boolean("Replace every occurrence instead of requiring a single match."),
			}, []string{"new_text", "old_text", "path", "replace_all"}),
			Effects:     []tools.Effect{tools.EffectWrite},
			Destructive: true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}},
			ResultLimit: tools.ResultLimit{MaxBytes: 16 * 1024, MaxLines: 200, Strategy: "head"},
		},
		nil,
		func(inv tools.Invocation[editArgs]) ([]tools.ResourceRef, error) {
			_, rel, err := safeJoin(opts.Root, inv.Args.Path, false)
			if err != nil {
				return nil, err
			}
			return resource("file", rel), nil
		},
		func(ctx context.Context, inv tools.Invocation[editArgs]) (tools.Result, error) {
			if err := ctx.Err(); err != nil {
				return tools.Result{}, err
			}
			full, rel, err := safeJoin(opts.Root, inv.Args.Path, false)
			if err != nil {
				return tools.Result{}, err
			}
			data, err := os.ReadFile(full)
			if err != nil {
				return tools.Result{}, err
			}
			text := string(data)
			count := strings.Count(text, inv.Args.OldText)
			if inv.Args.OldText == "" || count == 0 {
				return tools.Result{}, fmt.Errorf("old_text was not found in %s", rel)
			}
			if !inv.Args.ReplaceAll && count != 1 {
				return tools.Result{}, fmt.Errorf("old_text matched %d times in %s; set replace_all=true or provide a more specific match", count, rel)
			}
			replaceN := 1
			if inv.Args.ReplaceAll {
				replaceN = -1
			}
			next := strings.Replace(text, inv.Args.OldText, inv.Args.NewText, replaceN)
			if err := os.WriteFile(full, []byte(next), 0o644); err != nil {
				return tools.Result{}, err
			}
			return tools.Result{Title: "Edited " + rel, Text: fmt.Sprintf("edited %s (%d replacement%s)", rel, count, plural(count)), Metadata: map[string]any{"path": rel, "replacements": count}}, nil
		},
	)
}

func writeTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[writeArgs](
		tools.Definition{
			Name:        "write",
			Title:       "Write",
			Description: "Create or overwrite one workspace file with the provided complete content.",
			InputSchema: tools.StrictObject(map[string]any{
				"path":    tools.String("Workspace-relative file path."),
				"content": tools.String("Complete file content."),
			}, []string{"content", "path"}),
			Effects:     []tools.Effect{tools.EffectWrite},
			Destructive: true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}},
			ResultLimit: tools.ResultLimit{MaxBytes: 8 * 1024, MaxLines: 100, Strategy: "head"},
		},
		nil,
		func(inv tools.Invocation[writeArgs]) ([]tools.ResourceRef, error) {
			_, rel, err := safeJoin(opts.Root, inv.Args.Path, false)
			if err != nil {
				return nil, err
			}
			return resource("file", rel), nil
		},
		func(ctx context.Context, inv tools.Invocation[writeArgs]) (tools.Result, error) {
			if err := ctx.Err(); err != nil {
				return tools.Result{}, err
			}
			full, rel, err := safeJoin(opts.Root, inv.Args.Path, false)
			if err != nil {
				return tools.Result{}, err
			}
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				return tools.Result{}, err
			}
			if err := os.WriteFile(full, []byte(inv.Args.Content), 0o644); err != nil {
				return tools.Result{}, err
			}
			return tools.Result{Title: "Wrote " + rel, Text: fmt.Sprintf("wrote %s (%d bytes)", rel, len(inv.Args.Content)), Metadata: map[string]any{"path": rel, "bytes": len(inv.Args.Content)}}, nil
		},
	)
}

type patchOp struct {
	kind string
	path string
	body []string
}

func parsePatch(patch string) ([]patchOp, error) {
	lines := strings.Split(strings.ReplaceAll(patch, "\r\n", "\n"), "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[0]) != "*** Begin Patch" || strings.TrimSpace(lines[len(lines)-1]) != "*** End Patch" {
		return nil, fmt.Errorf("patch must start with *** Begin Patch and end with *** End Patch")
	}
	var ops []patchOp
	var current *patchOp
	for _, line := range lines[1 : len(lines)-1] {
		switch {
		case strings.HasPrefix(line, "*** Add File: "):
			if current != nil {
				ops = append(ops, *current)
			}
			current = &patchOp{kind: "add", path: strings.TrimSpace(strings.TrimPrefix(line, "*** Add File: "))}
		case strings.HasPrefix(line, "*** Delete File: "):
			if current != nil {
				ops = append(ops, *current)
			}
			ops = append(ops, patchOp{kind: "delete", path: strings.TrimSpace(strings.TrimPrefix(line, "*** Delete File: "))})
			current = nil
		case strings.HasPrefix(line, "*** Update File: "):
			if current != nil {
				ops = append(ops, *current)
			}
			current = &patchOp{kind: "update", path: strings.TrimSpace(strings.TrimPrefix(line, "*** Update File: "))}
		default:
			if current == nil {
				if strings.TrimSpace(line) == "" {
					continue
				}
				return nil, fmt.Errorf("patch content appears before a file operation")
			}
			current.body = append(current.body, line)
		}
	}
	if current != nil {
		ops = append(ops, *current)
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("patch contains no file operations")
	}
	return ops, nil
}

func applyParsedPatch(root string, ops []patchOp) error {
	originals := map[string][]byte{}
	added := map[string]struct{}{}
	for _, op := range ops {
		full, _, err := safeJoin(root, op.path, false)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(full)
		if err == nil {
			originals[full] = data
		} else if !os.IsNotExist(err) && op.kind != "add" {
			return err
		} else if os.IsNotExist(err) {
			added[full] = struct{}{}
		}
	}
	for _, op := range ops {
		full, _, err := safeJoin(root, op.path, false)
		if err != nil {
			restoreFiles(originals, added)
			return err
		}
		switch op.kind {
		case "add":
			content := patchBodyContent(op.body, '+')
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				restoreFiles(originals, added)
				return err
			}
			if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
				restoreFiles(originals, added)
				return err
			}
		case "delete":
			if err := os.Remove(full); err != nil {
				restoreFiles(originals, added)
				return err
			}
		case "update":
			data, err := os.ReadFile(full)
			if err != nil {
				restoreFiles(originals, added)
				return err
			}
			next, err := applySimpleUpdate(string(data), op.body)
			if err != nil {
				restoreFiles(originals, added)
				return err
			}
			if err := os.WriteFile(full, []byte(next), 0o644); err != nil {
				restoreFiles(originals, added)
				return err
			}
		}
	}
	return nil
}

func applySimpleUpdate(original string, body []string) (string, error) {
	next := original
	for i := 0; i < len(body); i++ {
		if !strings.HasPrefix(body[i], "-") {
			continue
		}
		oldText := strings.TrimPrefix(body[i], "-")
		var additions []string
		for i+1 < len(body) && strings.HasPrefix(body[i+1], "+") {
			i++
			additions = append(additions, strings.TrimPrefix(body[i], "+"))
		}
		if strings.Count(next, oldText) != 1 {
			return "", fmt.Errorf("patch context %q did not match exactly once", oldText)
		}
		next = strings.Replace(next, oldText, strings.Join(additions, "\n"), 1)
	}
	return next, nil
}

func patchBodyContent(lines []string, prefix byte) string {
	var out []string
	for _, line := range lines {
		if len(line) > 0 && line[0] == prefix {
			out = append(out, line[1:])
		}
	}
	return strings.Join(out, "\n")
}

func restoreFiles(originals map[string][]byte, added map[string]struct{}) {
	for path := range added {
		if _, existed := originals[path]; !existed {
			_ = os.Remove(path)
		}
	}
	for path, data := range originals {
		_ = os.WriteFile(path, data, 0o644)
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
