package skills

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/floegence/floret/tools"
)

var (
	ErrInvalidSkill  = errors.New("invalid skill")
	ErrSkillNotFound = errors.New("skill not found")
)

type SourceKind string

const (
	SourceRepo   SourceKind = "repo"
	SourceUser   SourceKind = "user"
	SourceAdmin  SourceKind = "admin"
	SourceConfig SourceKind = "config"
)

type Scope string

const (
	ScopeProject Scope = "project"
	ScopeUser    Scope = "user"
	ScopeAdmin   Scope = "admin"
	ScopeConfig  Scope = "config"
)

type Source struct {
	Root         string
	Kind         SourceKind
	Scope        Scope
	DisplayLabel string
	Enabled      bool
}

type SourceInfo struct {
	Kind         SourceKind
	Root         string
	RelativePath string
	DisplayLabel string
}

type Skill struct {
	Name        string
	Description string
	Path        string
	Scope       Scope
	SourceInfo  SourceInfo
	Enabled     bool
	ContentHash string
}

type Diagnostic struct {
	Kind       string
	SkillName  string
	Path       string
	SourceKind SourceKind
	Message    string
}

type Catalog struct {
	Skills      []Skill
	Diagnostics []Diagnostic
}

type PromptOptions struct {
	MaxBytes int
}

type ToolOptions struct {
	ResultLimit tools.ResultLimit
	OnLoad      func(SkillLoad)
}

type SkillLoad struct {
	Name        string
	Path        string
	SourceKind  SourceKind
	ContentHash string
	Bytes       int
}

type skillArgs struct {
	Name string `json:"name"`
}

var skillNameRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$`)

func Discover(sources []Source) (Catalog, error) {
	var catalog Catalog
	seen := map[string]string{}
	for _, source := range sources {
		if !source.Enabled {
			continue
		}
		root := strings.TrimSpace(source.Root)
		if root == "" {
			catalog.Diagnostics = append(catalog.Diagnostics, Diagnostic{
				Kind:       "source_invalid",
				SourceKind: source.Kind,
				Message:    "skill source root is required",
			})
			continue
		}
		abs, err := filepath.Abs(root)
		if err != nil {
			catalog.Diagnostics = append(catalog.Diagnostics, Diagnostic{
				Kind:       "source_invalid",
				SourceKind: source.Kind,
				Path:       root,
				Message:    err.Error(),
			})
			continue
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			catalog.Diagnostics = append(catalog.Diagnostics, Diagnostic{
				Kind:       "source_unreadable",
				SourceKind: source.Kind,
				Path:       abs,
				Message:    err.Error(),
			})
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			path := filepath.Join(abs, entry.Name(), "SKILL.md")
			skill, err := readSkill(path, source, abs)
			if err != nil {
				catalog.Diagnostics = append(catalog.Diagnostics, diagnosticForError(err, path, source.Kind))
				continue
			}
			if previous, ok := seen[skill.Name]; ok {
				catalog.Diagnostics = append(catalog.Diagnostics, Diagnostic{
					Kind:       "skill_duplicate",
					SkillName:  skill.Name,
					SourceKind: source.Kind,
					Path:       path,
					Message:    fmt.Sprintf("skill %q already discovered at %s", skill.Name, previous),
				})
				continue
			}
			seen[skill.Name] = path
			catalog.Skills = append(catalog.Skills, skill)
		}
	}
	slices.SortFunc(catalog.Skills, func(a, b Skill) int {
		return strings.Compare(a.Name, b.Name)
	})
	return catalog, nil
}

func BuildPrompt(skills []Skill, opts PromptOptions) (string, []Diagnostic) {
	enabled := make([]Skill, 0, len(skills))
	for _, skill := range skills {
		if skill.Enabled {
			enabled = append(enabled, skill)
		}
	}
	if len(enabled) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("<available_skills>\n")
	b.WriteString("Call the read-only `skill` tool with a skill name when one of these workflows is relevant. Do not assume the full instructions until the skill tool returns them.\n")
	for _, skill := range enabled {
		line := fmt.Sprintf("- name: %s\n  description: %s\n  source: %s\n", skill.Name, oneLine(skill.Description), sourceLabel(skill))
		if opts.MaxBytes > 0 && b.Len()+len(line)+len("</available_skills>\n") > opts.MaxBytes {
			b.WriteString("</available_skills>\n")
			return b.String(), []Diagnostic{{
				Kind:    "prompt_truncated",
				Message: fmt.Sprintf("available skills prompt was truncated at %d bytes", opts.MaxBytes),
			}}
		}
		b.WriteString(line)
	}
	b.WriteString("</available_skills>\n")
	return b.String(), nil
}

func DefineSkillTool(skills []Skill, opts ToolOptions) (tools.Tool, error) {
	index := map[string]Skill{}
	for _, skill := range skills {
		if !skill.Enabled {
			continue
		}
		if _, ok := index[skill.Name]; ok {
			return tools.Tool{}, fmt.Errorf("%w: duplicate skill %q", ErrInvalidSkill, skill.Name)
		}
		index[skill.Name] = skill
	}
	return tools.Define[skillArgs](
		tools.Definition{
			Name:        "skill",
			Title:       "Skill",
			Description: "Read the full SKILL.md instructions for one available agent skill. Use this only after matching the skill name from <available_skills>.",
			InputSchema: tools.StrictObject(map[string]any{
				"name": tools.String("Skill name from the available skills list."),
			}, []string{"name"}),
			Effects:      []tools.Effect{tools.EffectRead},
			ReadOnly:     true,
			ParallelSafe: true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow, ResourceKinds: []string{"skill"}},
			ResultLimit:  opts.ResultLimit,
			Annotations: map[string]any{
				"source":          "skill",
				"permission_mode": string(tools.PermissionAllow),
				"read_path":       "SKILL.md",
			},
		},
		nil,
		func(inv tools.Invocation[skillArgs]) ([]tools.ResourceRef, error) {
			name := strings.TrimSpace(inv.Args.Name)
			skill, ok := index[name]
			if !ok {
				return nil, ErrSkillNotFound
			}
			return []tools.ResourceRef{{Kind: "skill", Value: skill.Name}}, nil
		},
		func(ctx context.Context, inv tools.Invocation[skillArgs]) (tools.Result, error) {
			if err := ctx.Err(); err != nil {
				return tools.Result{}, err
			}
			name := strings.TrimSpace(inv.Args.Name)
			skill, ok := index[name]
			if !ok {
				return tools.Result{}, fmt.Errorf("%w: %s", ErrSkillNotFound, name)
			}
			data, err := os.ReadFile(skill.Path)
			if err != nil {
				return tools.Result{}, err
			}
			hash := sha256Hex(data)
			if opts.OnLoad != nil {
				opts.OnLoad(SkillLoad{
					Name:        skill.Name,
					Path:        skill.Path,
					SourceKind:  skill.SourceInfo.Kind,
					ContentHash: hash,
					Bytes:       len(data),
				})
			}
			return tools.Result{
				Title: "Skill " + skill.Name,
				Text:  string(data),
				Metadata: map[string]any{
					"capability":   "skill",
					"skill_name":   skill.Name,
					"source_kind":  string(skill.SourceInfo.Kind),
					"source_label": skill.SourceInfo.DisplayLabel,
					"content_hash": hash,
					"bytes":        len(data),
				},
			}, nil
		},
	), nil
}

func readSkill(path string, source Source, root string) (Skill, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Skill{}, fmt.Errorf("%w: %v", ErrInvalidSkill, err)
	}
	meta, err := parseFrontmatter(string(data))
	if err != nil {
		return Skill{}, err
	}
	name := strings.TrimSpace(meta["name"])
	description := strings.TrimSpace(meta["description"])
	if name == "" {
		name = filepath.Base(filepath.Dir(path))
	}
	if err := validateName(name); err != nil {
		return Skill{}, err
	}
	if description == "" {
		return Skill{}, fmt.Errorf("%w: skill %q description is required", ErrInvalidSkill, name)
	}
	if filepath.Base(filepath.Dir(path)) != name {
		return Skill{}, fmt.Errorf("%w: skill directory must match name %q", ErrInvalidSkill, name)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = filepath.Base(filepath.Dir(path)) + "/SKILL.md"
	}
	scope := source.Scope
	if scope == "" {
		scope = scopeForSource(source.Kind)
	}
	return Skill{
		Name:        name,
		Description: description,
		Path:        path,
		Scope:       scope,
		SourceInfo: SourceInfo{
			Kind:         source.Kind,
			Root:         root,
			RelativePath: rel,
			DisplayLabel: displayLabel(source),
		},
		Enabled:     true,
		ContentHash: sha256Hex(data),
	}, nil
}

func parseFrontmatter(text string) (map[string]string, error) {
	trimmed := strings.TrimLeft(text, "\ufeff\r\n\t ")
	if !strings.HasPrefix(trimmed, "---\n") && !strings.HasPrefix(trimmed, "---\r\n") {
		return map[string]string{}, nil
	}
	lines := strings.Split(strings.ReplaceAll(trimmed, "\r\n", "\n"), "\n")
	meta := map[string]string{}
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "---" {
			return meta, nil
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("%w: invalid frontmatter line %q", ErrInvalidSkill, line)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		if key == "" {
			return nil, fmt.Errorf("%w: empty frontmatter key", ErrInvalidSkill)
		}
		meta[key] = value
	}
	return nil, fmt.Errorf("%w: unterminated frontmatter", ErrInvalidSkill)
}

func validateName(name string) error {
	if !skillNameRE.MatchString(name) || strings.Contains(name, "--") {
		return fmt.Errorf("%w: invalid skill name %q", ErrInvalidSkill, name)
	}
	return nil
}

func diagnosticForError(err error, path string, kind SourceKind) Diagnostic {
	return Diagnostic{
		Kind:       "skill_invalid",
		Path:       path,
		SourceKind: kind,
		Message:    err.Error(),
	}
}

func scopeForSource(kind SourceKind) Scope {
	switch kind {
	case SourceUser:
		return ScopeUser
	case SourceAdmin:
		return ScopeAdmin
	case SourceConfig:
		return ScopeConfig
	default:
		return ScopeProject
	}
}

func displayLabel(source Source) string {
	if strings.TrimSpace(source.DisplayLabel) != "" {
		return strings.TrimSpace(source.DisplayLabel)
	}
	if source.Kind != "" {
		return string(source.Kind)
	}
	return "skill source"
}

func sourceLabel(skill Skill) string {
	label := strings.TrimSpace(skill.SourceInfo.DisplayLabel)
	if label == "" {
		label = string(skill.SourceInfo.Kind)
	}
	if label == "" {
		label = "unknown"
	}
	return label
}

func oneLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
