package builtintools

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/floegence/floret/tools"
)

type applyPatchArgs struct {
	Patch string `json:"patch"`
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
		reg.Register(writeTool(opts)),
	)
}

func applyPatchTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[applyPatchArgs](
		tools.Definition{
			Name:        "apply_patch",
			Title:       "Apply patch",
			Description: "Apply a structured Codex-style patch to workspace files. Prefer this for code edits, multi-file changes, renames, and audited local modifications.",
			InputSchema: tools.StrictObject(map[string]any{
				"patch": tools.String("Patch text beginning with *** Begin Patch and ending with *** End Patch. File operation markers must use a colon, for example *** Update File: path/to/file. Supports *** Add File: path, *** Update File: path, *** Move to: path, *** Delete File: path, @@ context chunks, standard unified range headers such as @@ -1,2 +1,2 @@, + additions, - removals, and context lines prefixed by a space."),
			}, []string{"patch"}),
			Effects:     []tools.Effect{tools.EffectWrite},
			Destructive: true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}},
			ResultLimit: tools.ResultLimit{MaxBytes: 32 * 1024, MaxLines: 400, Strategy: "head"},
		},
		nil,
		func(inv tools.Invocation[applyPatchArgs]) ([]tools.ResourceRef, error) {
			doc, err := parsePatchDocument(inv.Args.Patch)
			if err != nil {
				return nil, err
			}
			refs := make([]tools.ResourceRef, 0, len(doc.Ops)*2)
			for _, op := range doc.Ops {
				paths := []string{op.Path}
				if op.MovePath != "" {
					paths = append(paths, op.MovePath)
				}
				for _, rawPath := range paths {
					_, rel, err := safeJoin(opts.Root, rawPath, false)
					if err != nil {
						return nil, err
					}
					if rel == "." {
						return nil, fmt.Errorf("patch file path must name a file")
					}
					refs = append(refs, tools.ResourceRef{Kind: "file", Value: rel})
				}
			}
			return refs, nil
		},
		func(ctx context.Context, inv tools.Invocation[applyPatchArgs]) (tools.Result, error) {
			if err := ctx.Err(); err != nil {
				return tools.Result{}, err
			}
			doc, err := parsePatchDocument(inv.Args.Patch)
			if err != nil {
				return tools.Result{}, err
			}
			changes, err := planPatchChanges(opts.Root, doc)
			if err != nil {
				return tools.Result{}, err
			}
			if err := applyPatchChanges(changes); err != nil {
				return tools.Result{}, err
			}
			files := make([]string, 0, len(doc.Ops)*2)
			for _, op := range doc.Ops {
				files = append(files, op.Path)
				if op.MovePath != "" {
					files = append(files, op.MovePath)
				}
			}
			return tools.Result{Title: "Applied patch", Text: "applied patch to " + strings.Join(files, ", "), Metadata: map[string]any{"files": files}}, nil
		},
	)
}

func writeTool(opts WorkspaceOptions) tools.Tool {
	return tools.Define[writeArgs](
		tools.Definition{
			Name:        "write",
			Title:       "Write",
			Description: "Create or overwrite one workspace file with the provided complete content. Use apply_patch for code edits, multi-file changes, or audited modifications.",
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
			if rel == "." {
				return nil, fmt.Errorf("write path must name a file")
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
			if rel == "." {
				return tools.Result{}, fmt.Errorf("write path must name a file")
			}
			if info, err := os.Stat(full); err == nil && info.IsDir() {
				return tools.Result{}, fmt.Errorf("write path is a directory, not a file: %s", rel)
			} else if err != nil && !os.IsNotExist(err) {
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
	Kind       patchOpKind
	Path       string
	MovePath   string
	AddLines   []string
	Chunks     []patchChunk
	SourceLine int
}

type patchOpKind string

const (
	patchAdd    patchOpKind = "add"
	patchUpdate patchOpKind = "update"
	patchDelete patchOpKind = "delete"
)

type patchDocument struct {
	Ops []patchOp
}

type patchChunk struct {
	Anchor            string
	Range             patchRange
	OldLines          []string
	NewLines          []string
	NewLineOldIndexes []int
	EndOfFile         bool
	SourceLine        int
}

type patchRange struct {
	OldStart int
	NewStart int
	Present  bool
}

type patchFileChange struct {
	Kind       patchOpKind
	Path       string
	FullPath   string
	MovePath   string
	MoveFull   string
	OldContent []byte
	NewContent []byte
	Mode       fs.FileMode
}

type patchReplacement struct {
	Start int
	OldN  int
	Lines []string
}

var unifiedHunkHeaderPattern = regexp.MustCompile(`^-([0-9]+)(?:,([0-9]+))?\s+\+([0-9]+)(?:,([0-9]+))?(?:\s+@@\s*(.*))?$`)

func parsePatchDocument(patch string) (patchDocument, error) {
	lines := strings.Split(strings.ReplaceAll(strings.ReplaceAll(patch, "\r\n", "\n"), "\r", "\n"), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) < 2 || strings.TrimSpace(lines[0]) != "*** Begin Patch" || strings.TrimSpace(lines[len(lines)-1]) != "*** End Patch" {
		return patchDocument{}, fmt.Errorf("patch must start with *** Begin Patch and end with *** End Patch")
	}
	doc := patchDocument{}
	var current *patchOp
	var currentChunk *patchChunk
	flushChunk := func() error {
		if currentChunk == nil {
			return nil
		}
		if len(currentChunk.OldLines) == 0 && len(currentChunk.NewLines) == 0 {
			return fmt.Errorf("line %d: update hunk has no changed lines", currentChunk.SourceLine)
		}
		current.Chunks = append(current.Chunks, *currentChunk)
		currentChunk = nil
		return nil
	}
	flushOp := func() error {
		if current == nil {
			return nil
		}
		if err := flushChunk(); err != nil {
			return err
		}
		if current.Path == "" {
			return fmt.Errorf("line %d: patch file path is required", current.SourceLine)
		}
		switch current.Kind {
		case patchAdd:
			if len(current.AddLines) == 0 {
				return fmt.Errorf("line %d: add file hunk must contain at least one + line", current.SourceLine)
			}
		case patchUpdate:
			if len(current.Chunks) == 0 && current.MovePath == "" {
				return fmt.Errorf("line %d: update file hunk must contain a change or move", current.SourceLine)
			}
		case patchDelete:
		default:
			return fmt.Errorf("line %d: unknown patch operation", current.SourceLine)
		}
		doc.Ops = append(doc.Ops, *current)
		current = nil
		return nil
	}
	for idx, line := range lines[1 : len(lines)-1] {
		lineNo := idx + 2
		trimmed := strings.TrimSpace(line)
		if path, ok := patchMarkerArg(line, "*** Add File:"); ok {
			if err := flushOp(); err != nil {
				return patchDocument{}, err
			}
			current = &patchOp{Kind: patchAdd, Path: path, SourceLine: lineNo}
			continue
		}
		if path, ok := patchMarkerArg(line, "*** Update File:"); ok {
			if err := flushOp(); err != nil {
				return patchDocument{}, err
			}
			current = &patchOp{Kind: patchUpdate, Path: path, SourceLine: lineNo}
			continue
		}
		if path, ok := patchMarkerArg(line, "*** Delete File:"); ok {
			if err := flushOp(); err != nil {
				return patchDocument{}, err
			}
			if path == "" {
				return patchDocument{}, fmt.Errorf("line %d: patch file path is required", lineNo)
			}
			doc.Ops = append(doc.Ops, patchOp{Kind: patchDelete, Path: path, SourceLine: lineNo})
			current = nil
			currentChunk = nil
			continue
		}
		switch {
		default:
			if current == nil {
				if trimmed == "" {
					continue
				}
				if strings.HasPrefix(trimmed, "***") {
					return patchDocument{}, invalidPatchOperationError(lineNo, trimmed)
				}
				return patchDocument{}, fmt.Errorf("line %d: patch content appears before a file operation", lineNo)
			}
			switch current.Kind {
			case patchAdd:
				if !strings.HasPrefix(line, "+") {
					return patchDocument{}, fmt.Errorf("line %d: add file lines must start with +", lineNo)
				}
				current.AddLines = append(current.AddLines, strings.TrimPrefix(line, "+"))
			case patchDelete:
				return patchDocument{}, fmt.Errorf("line %d: delete file hunk cannot contain content", lineNo)
			case patchUpdate:
				if path, ok := patchMarkerArg(line, "*** Move to:"); ok {
					if current.MovePath != "" {
						return patchDocument{}, fmt.Errorf("line %d: update file hunk has multiple move targets", lineNo)
					}
					if currentChunk != nil || len(current.Chunks) > 0 {
						return patchDocument{}, fmt.Errorf("line %d: move target must appear before update chunks", lineNo)
					}
					current.MovePath = path
					if current.MovePath == "" {
						return patchDocument{}, fmt.Errorf("line %d: move target path is required", lineNo)
					}
					continue
				}
				if trimmed == "@@" || strings.HasPrefix(trimmed, "@@ ") {
					if err := flushChunk(); err != nil {
						return patchDocument{}, err
					}
					currentChunk = parsePatchChunkHeader(trimmed, lineNo)
					continue
				}
				if currentChunk == nil {
					currentChunk = &patchChunk{SourceLine: lineNo}
				}
				switch {
				case strings.HasPrefix(line, " "):
					text := strings.TrimPrefix(line, " ")
					oldIndex := len(currentChunk.OldLines)
					currentChunk.OldLines = append(currentChunk.OldLines, text)
					currentChunk.NewLines = append(currentChunk.NewLines, text)
					currentChunk.NewLineOldIndexes = append(currentChunk.NewLineOldIndexes, oldIndex)
				case strings.HasPrefix(line, "-"):
					currentChunk.OldLines = append(currentChunk.OldLines, strings.TrimPrefix(line, "-"))
				case strings.HasPrefix(line, "+"):
					currentChunk.NewLines = append(currentChunk.NewLines, strings.TrimPrefix(line, "+"))
					currentChunk.NewLineOldIndexes = append(currentChunk.NewLineOldIndexes, -1)
				case trimmed == "*** End of File":
					currentChunk.EndOfFile = true
				default:
					return patchDocument{}, fmt.Errorf("line %d: update lines must start with space, +, -, @@, or *** End of File", lineNo)
				}
			}
		}
	}
	if err := flushOp(); err != nil {
		return patchDocument{}, err
	}
	if len(doc.Ops) == 0 {
		return patchDocument{}, fmt.Errorf("patch contains no file operations")
	}
	return doc, nil
}

func patchMarkerArg(line, marker string) (string, bool) {
	trimmedLeft := strings.TrimLeft(line, " \t")
	if !strings.HasPrefix(trimmedLeft, marker) {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(trimmedLeft, marker)), true
}

func invalidPatchOperationError(lineNo int, line string) error {
	return fmt.Errorf("line %d: invalid patch file operation: %s (expected one of: *** Add File: path, *** Update File: path, *** Delete File: path)", lineNo, line)
}

func parsePatchChunkHeader(trimmed string, lineNo int) *patchChunk {
	header := strings.TrimSpace(strings.TrimPrefix(trimmed, "@@"))
	if strings.HasSuffix(header, "@@") {
		header = strings.TrimSpace(strings.TrimSuffix(header, "@@"))
	}
	chunk := &patchChunk{SourceLine: lineNo}
	if header == "" {
		return chunk
	}
	if rng, ok := parseUnifiedHunkRange(header); ok {
		chunk.Range = rng
		return chunk
	}
	chunk.Anchor = header
	return chunk
}

func parseUnifiedHunkRange(header string) (patchRange, bool) {
	match := unifiedHunkHeaderPattern.FindStringSubmatch(header)
	if match == nil {
		return patchRange{}, false
	}
	oldStart, _ := strconv.Atoi(match[1])
	newStart, _ := strconv.Atoi(match[3])
	return patchRange{
		OldStart: oldStart,
		NewStart: newStart,
		Present:  true,
	}, true
}

func planPatchChanges(root string, doc patchDocument) ([]patchFileChange, error) {
	planned := map[string]patchFileChange{}
	changes := make([]patchFileChange, 0, len(doc.Ops))
	for _, op := range doc.Ops {
		full, rel, err := safeJoin(root, op.Path, false)
		if err != nil {
			return nil, err
		}
		if rel == "." {
			return nil, fmt.Errorf("line %d: patch file path must name a file", op.SourceLine)
		}
		if _, exists := planned[full]; exists {
			return nil, fmt.Errorf("line %d: patch touches %s more than once", op.SourceLine, rel)
		}
		switch op.Kind {
		case patchAdd:
			if _, err := os.Stat(full); err == nil {
				return nil, fmt.Errorf("line %d: cannot add existing file %s", op.SourceLine, rel)
			} else if !os.IsNotExist(err) {
				return nil, err
			}
			change := patchFileChange{Kind: patchAdd, Path: rel, FullPath: full, NewContent: []byte(strings.Join(op.AddLines, "\n") + "\n"), Mode: 0o644}
			planned[full] = change
			changes = append(changes, change)
		case patchDelete:
			data, mode, err := readPatchFile(full, rel, op.SourceLine)
			if err != nil {
				return nil, err
			}
			change := patchFileChange{Kind: patchDelete, Path: rel, FullPath: full, OldContent: data, Mode: mode}
			planned[full] = change
			changes = append(changes, change)
		case patchUpdate:
			data, mode, err := readPatchFile(full, rel, op.SourceLine)
			if err != nil {
				return nil, err
			}
			next, err := deriveUpdatedContent(string(data), op)
			if err != nil {
				return nil, err
			}
			change := patchFileChange{Kind: patchUpdate, Path: rel, FullPath: full, OldContent: data, NewContent: []byte(next), Mode: mode}
			if op.MovePath != "" {
				moveFull, moveRel, err := safeJoin(root, op.MovePath, false)
				if err != nil {
					return nil, err
				}
				if moveRel == "." {
					return nil, fmt.Errorf("line %d: move target path must name a file", op.SourceLine)
				}
				if moveFull == full {
					return nil, fmt.Errorf("line %d: move target must differ from source path", op.SourceLine)
				}
				if _, exists := planned[moveFull]; exists {
					return nil, fmt.Errorf("line %d: move target %s is already touched by this patch", op.SourceLine, moveRel)
				}
				if _, err := os.Stat(moveFull); err == nil {
					return nil, fmt.Errorf("line %d: cannot move to existing file %s", op.SourceLine, moveRel)
				} else if !os.IsNotExist(err) {
					return nil, err
				}
				change.MovePath = moveRel
				change.MoveFull = moveFull
				planned[moveFull] = change
			}
			planned[full] = change
			changes = append(changes, change)
		}
	}
	return changes, nil
}

func readPatchFile(full, rel string, line int) ([]byte, fs.FileMode, error) {
	info, err := os.Stat(full)
	if os.IsNotExist(err) {
		return nil, 0, fmt.Errorf("line %d: file does not exist: %s", line, rel)
	}
	if err != nil {
		return nil, 0, err
	}
	if info.IsDir() {
		return nil, 0, fmt.Errorf("line %d: path is a directory, not a file: %s", line, rel)
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return nil, 0, err
	}
	return data, info.Mode().Perm(), nil
}

func deriveUpdatedContent(original string, op patchOp) (string, error) {
	originalLines := splitPatchLines(original)
	replacements := make([]patchReplacement, 0, len(op.Chunks))
	cursor := 0
	for _, chunk := range op.Chunks {
		oldLines := patchLinesWithNewlines(chunk.OldLines)
		if len(oldLines) == 0 {
			insertAt := insertionIndex(originalLines, chunk)
			newLines := patchLinesWithNewlines(chunk.NewLines)
			replacements = append(replacements, patchReplacement{Start: insertAt, Lines: newLines})
			if chunk.Range.Present {
				cursor = insertAt
			}
			continue
		}
		matchStart := findChunk(originalLines, cursor, oldLines, chunk)
		if matchStart < 0 {
			return "", fmt.Errorf("line %d: patch context did not match %s", chunk.SourceLine, op.Path)
		}
		newLines := deriveNewLines(originalLines[matchStart:matchStart+len(oldLines)], chunk)
		replacements = append(replacements, patchReplacement{Start: matchStart, OldN: len(oldLines), Lines: newLines})
		cursor = matchStart + len(oldLines)
	}
	for i := len(replacements) - 1; i >= 0; i-- {
		repl := replacements[i]
		next := make([]string, 0, len(originalLines)-repl.OldN+len(repl.Lines))
		next = append(next, originalLines[:repl.Start]...)
		next = append(next, repl.Lines...)
		next = append(next, originalLines[repl.Start+repl.OldN:]...)
		originalLines = next
	}
	if len(originalLines) == 0 || !strings.HasSuffix(originalLines[len(originalLines)-1], "\n") {
		originalLines = append(originalLines, "\n")
	}
	return strings.Join(originalLines, ""), nil
}

func insertionIndex(lines []string, chunk patchChunk) int {
	if chunk.Range.Present {
		return clampIndex(chunk.Range.NewStart-1, 0, len(lines))
	}
	return len(lines)
}

func deriveNewLines(matchedOldLines []string, chunk patchChunk) []string {
	newLines := patchLinesWithNewlines(chunk.NewLines)
	for idx, oldIndex := range chunk.NewLineOldIndexes {
		if oldIndex < 0 || idx >= len(newLines) || oldIndex >= len(matchedOldLines) {
			continue
		}
		newLines[idx] = matchedOldLines[oldIndex]
	}
	return newLines
}

func splitPatchLines(content string) []string {
	lines := strings.SplitAfter(content, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func patchLinesWithNewlines(lines []string) []string {
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i] = line + "\n"
	}
	return out
}

func findChunk(lines []string, cursor int, oldLines []string, chunk patchChunk) int {
	if len(oldLines) == 0 {
		return cursor
	}
	start := cursor
	if chunk.Anchor != "" {
		anchorStart := seekPatchLines(lines, []string{chunk.Anchor + "\n"}, start, false)
		if anchorStart < 0 {
			return -1
		}
		start = anchorStart
	}
	end := len(lines) - len(oldLines)
	if end < start {
		return -1
	}
	if chunk.EndOfFile {
		start = end
	}
	if chunk.Range.Present && chunk.Anchor == "" && !chunk.EndOfFile {
		hinted := clampIndex(chunk.Range.OldStart-1, start, end)
		if found := seekPatchLinesAround(lines, oldLines, hinted, start, end); found >= 0 {
			return found
		}
	}
	if found := seekPatchLines(lines, oldLines, start, chunk.EndOfFile); found >= 0 {
		return found
	}
	return -1
}

func clampIndex(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func sameLineSequence(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if normalizePatchLine(a[i]) != normalizePatchLine(b[i]) {
			return false
		}
	}
	return true
}

func normalizePatchLine(line string) string {
	return strings.TrimSuffix(line, "\n")
}

func seekPatchLines(lines []string, pattern []string, start int, eof bool) int {
	if len(pattern) == 0 {
		return start
	}
	if len(pattern) > len(lines) {
		return -1
	}
	searchStart := start
	if searchStart < 0 {
		searchStart = 0
	}
	last := len(lines) - len(pattern)
	if searchStart > last {
		return -1
	}
	if eof {
		if sameLineSequence(lines[last:last+len(pattern)], pattern) {
			return last
		}
	}
	for _, same := range patchLineComparators() {
		if eof && samePatchSequence(lines[last:last+len(pattern)], pattern, same) {
			return last
		}
		for i := searchStart; i <= last; i++ {
			if samePatchSequence(lines[i:i+len(pattern)], pattern, same) {
				return i
			}
		}
	}
	return -1
}

func seekPatchLinesAround(lines []string, pattern []string, hinted, start, end int) int {
	if len(pattern) == 0 || len(pattern) > len(lines) || end < start {
		return -1
	}
	for _, same := range patchLineComparators() {
		for radius := 0; radius <= 3; radius++ {
			candidates := []int{hinted}
			if radius > 0 {
				candidates = []int{hinted - radius, hinted + radius}
			}
			for _, candidate := range candidates {
				if candidate < start || candidate > end {
					continue
				}
				if samePatchSequence(lines[candidate:candidate+len(pattern)], pattern, same) {
					return candidate
				}
			}
		}
	}
	return -1
}

func samePatchSequence(lines []string, pattern []string, same func(string, string) bool) bool {
	for j := range pattern {
		if !same(lines[j], pattern[j]) {
			return false
		}
	}
	return true
}

func patchLineComparators() []func(string, string) bool {
	return []func(string, string) bool{
		func(a, b string) bool { return normalizePatchLine(a) == normalizePatchLine(b) },
		func(a, b string) bool {
			return strings.TrimRight(normalizePatchLine(a), " \t") == strings.TrimRight(normalizePatchLine(b), " \t")
		},
		func(a, b string) bool {
			return strings.TrimSpace(normalizePatchLine(a)) == strings.TrimSpace(normalizePatchLine(b))
		},
		func(a, b string) bool {
			return normalizePatchUnicode(strings.TrimSpace(normalizePatchLine(a))) == normalizePatchUnicode(strings.TrimSpace(normalizePatchLine(b)))
		},
	}
}

func normalizePatchUnicode(line string) string {
	var b strings.Builder
	for _, r := range line {
		switch r {
		case '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2015', '\u2212':
			b.WriteRune('-')
		case '\u2018', '\u2019', '\u201A', '\u201B':
			b.WriteRune('\'')
		case '\u201C', '\u201D', '\u201E', '\u201F':
			b.WriteRune('"')
		case '\u00A0', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200A', '\u202F', '\u205F', '\u3000':
			b.WriteRune(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func applyPatchChanges(changes []patchFileChange) error {
	tx := patchTransaction{}
	if err := tx.preflight(changes); err != nil {
		return err
	}
	for _, change := range changes {
		if err := tx.applyOne(change); err != nil {
			if rollbackErr := tx.rollback(); rollbackErr != nil {
				return fmt.Errorf("%w; rollback failed: %v", err, rollbackErr)
			}
			return err
		}
	}
	return nil
}

type patchTransaction struct {
	undos      []patchUndo
	createdDir map[string]struct{}
}

type patchUndo struct {
	Path    string
	Content []byte
	Mode    fs.FileMode
	Existed bool
}

func (tx *patchTransaction) preflight(changes []patchFileChange) error {
	for _, change := range changes {
		switch change.Kind {
		case patchAdd:
			if err := requireMissingPath(change.FullPath, change.Path); err != nil {
				return err
			}
		case patchUpdate:
			current, _, err := readPatchFile(change.FullPath, change.Path, 0)
			if err != nil {
				return err
			}
			if change.MoveFull != "" {
				if err := requireMissingPath(change.MoveFull, change.MovePath); err != nil {
					return err
				}
			}
			if string(current) != string(change.OldContent) {
				return fmt.Errorf("file changed before patch could be applied: %s", change.Path)
			}
		case patchDelete:
			current, _, err := readPatchFile(change.FullPath, change.Path, 0)
			if err != nil {
				return err
			}
			if string(current) != string(change.OldContent) {
				return fmt.Errorf("file changed before patch could be applied: %s", change.Path)
			}
		default:
			return fmt.Errorf("unknown patch operation")
		}
	}
	return nil
}

func (tx *patchTransaction) applyOne(change patchFileChange) error {
	switch change.Kind {
	case patchAdd:
		tx.undos = append(tx.undos, patchUndo{Path: change.FullPath})
		if err := tx.ensureParentDir(change.FullPath); err != nil {
			return err
		}
		return writeFileExclusive(change.FullPath, change.NewContent, change.Mode)
	case patchUpdate:
		tx.undos = append(tx.undos, patchUndo{Path: change.FullPath, Content: change.OldContent, Mode: change.Mode, Existed: true})
		if change.MoveFull == "" {
			if err := tx.ensureParentDir(change.FullPath); err != nil {
				return err
			}
			return os.WriteFile(change.FullPath, change.NewContent, change.Mode)
		}
		tx.undos = append(tx.undos, patchUndo{Path: change.MoveFull})
		if err := tx.ensureParentDir(change.MoveFull); err != nil {
			return err
		}
		if err := writeFileExclusive(change.MoveFull, change.NewContent, change.Mode); err != nil {
			return err
		}
		return os.Remove(change.FullPath)
	case patchDelete:
		tx.undos = append(tx.undos, patchUndo{Path: change.FullPath, Content: change.OldContent, Mode: change.Mode, Existed: true})
		return os.Remove(change.FullPath)
	default:
		return fmt.Errorf("unknown patch operation")
	}
}

func (tx *patchTransaction) ensureParentDir(path string) error {
	parent := filepath.Dir(path)
	missing, err := missingParentDirs(parent)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	if tx.createdDir == nil {
		tx.createdDir = map[string]struct{}{}
	}
	for _, dir := range missing {
		tx.createdDir[dir] = struct{}{}
	}
	return nil
}

func (tx *patchTransaction) rollback() error {
	var errs []string
	for i := len(tx.undos) - 1; i >= 0; i-- {
		undo := tx.undos[i]
		var err error
		if undo.Existed {
			err = os.WriteFile(undo.Path, undo.Content, undo.Mode)
		} else {
			err = os.Remove(undo.Path)
			if os.IsNotExist(err) {
				err = nil
			}
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	dirs := make([]string, 0, len(tx.createdDir))
	for dir := range tx.createdDir {
		dirs = append(dirs, dir)
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		err := os.Remove(dir)
		if err != nil && !os.IsNotExist(err) {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "; "))
	}
	return nil
}

func requireMissingPath(path, rel string) error {
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("patch target already exists: %s", rel)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func missingParentDirs(parent string) ([]string, error) {
	var dirs []string
	for {
		info, err := os.Stat(parent)
		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("patch parent path is not a directory: %s", parent)
			}
			return dirs, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
		dirs = append(dirs, parent)
		next := filepath.Dir(parent)
		if next == parent {
			return nil, fmt.Errorf("patch parent path is not reachable: %s", parent)
		}
		parent = next
	}
}

func writeFileExclusive(path string, content []byte, mode fs.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(content)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
