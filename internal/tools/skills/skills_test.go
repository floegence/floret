package skills_test

import (
	"context"
	"github.com/floegence/floret/internal/tools/skills"
	"github.com/floegence/floret/tools"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiscoverBuildsSkillsAndDiagnostics(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "pdf", "---\nname: pdf\ndescription: Work with PDF files.\n---\n# PDF\n")
	writeSkill(t, root, "bad_name", "---\nname: Bad Name\ndescription: bad\n---\n")

	catalog, err := skills.Discover([]skills.Source{{Root: root, Kind: skills.SourceRepo, Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Skills) != 1 || catalog.Skills[0].Name != "pdf" {
		t.Fatalf("skills = %#v", catalog.Skills)
	}
	if len(catalog.Diagnostics) != 1 || catalog.Diagnostics[0].Kind != "skill_invalid" {
		t.Fatalf("diagnostics = %#v", catalog.Diagnostics)
	}
	if catalog.Skills[0].SourceInfo.RelativePath != filepath.Join("pdf", "SKILL.md") {
		t.Fatalf("relative path = %q", catalog.Skills[0].SourceInfo.RelativePath)
	}
}

func TestDiscoverRejectsDuplicateSkillNames(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	writeSkill(t, first, "review", "---\nname: review\ndescription: Review code.\n---\n")
	writeSkill(t, second, "review", "---\nname: review\ndescription: Review again.\n---\n")

	catalog, err := skills.Discover([]skills.Source{
		{Root: first, Kind: skills.SourceRepo, Enabled: true},
		{Root: second, Kind: skills.SourceUser, Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Skills) != 1 {
		t.Fatalf("skills = %#v", catalog.Skills)
	}
	if len(catalog.Diagnostics) != 1 || catalog.Diagnostics[0].Kind != "skill_duplicate" {
		t.Fatalf("diagnostics = %#v", catalog.Diagnostics)
	}
}

func TestBuildPromptListsOnlyMetadataAndHonorsBudget(t *testing.T) {
	all := []skills.Skill{
		{Name: "alpha", Description: "Alpha workflow", SourceInfo: skills.SourceInfo{Kind: skills.SourceRepo, DisplayLabel: "repo"}, Enabled: true},
		{Name: "beta", Description: "Beta workflow", SourceInfo: skills.SourceInfo{Kind: skills.SourceUser, DisplayLabel: "user"}, Enabled: true},
	}
	full, none := skills.BuildPrompt(all, skills.PromptOptions{})
	if len(none) != 0 {
		t.Fatalf("full prompt diagnostics = %#v", none)
	}
	betaAt := strings.Index(full, "name: beta")
	if betaAt < 0 {
		t.Fatalf("full prompt missing beta: %q", full)
	}
	text, diagnostics := skills.BuildPrompt(all, skills.PromptOptions{MaxBytes: betaAt + len("</available_skills>\n")})
	if !strings.Contains(text, "<available_skills>") || !strings.Contains(text, "name: alpha") {
		t.Fatalf("prompt = %q", text)
	}
	if strings.Contains(text, "# ") {
		t.Fatalf("prompt should not include full skill body: %q", text)
	}
	if len(diagnostics) != 1 || diagnostics[0].Kind != "prompt_truncated" {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
}

func TestSkillToolReadsFullSkillMarkdown(t *testing.T) {
	root := t.TempDir()
	body := "---\nname: pdf\ndescription: Work with PDF files.\n---\n# PDF\nUse the renderer.\n"
	writeSkill(t, root, "pdf", body)
	catalog, err := skills.Discover([]skills.Source{{Root: root, Kind: skills.SourceRepo, Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	var loaded skills.SkillLoad
	tool, err := skills.DefineSkillTool(catalog.Skills, skills.ToolOptions{OnLoad: func(load skills.SkillLoad) {
		loaded = load
	}})
	if err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry(tool)
	result := reg.Run(context.Background(), tools.ToolCall{ID: "call-1", Name: "skill", Args: `{"name":"pdf"}`}, nil)
	if result.IsError || result.Text != body {
		t.Fatalf("result = %#v", result)
	}
	if loaded.Name != "pdf" || loaded.ContentHash == "" || loaded.Bytes != len(body) {
		t.Fatalf("load = %#v", loaded)
	}
	if result.Metadata["skill_name"] != "pdf" || result.Metadata["capability"] != "skill" {
		t.Fatalf("metadata = %#v", result.Metadata)
	}
}

func TestSkillToolRejectsUnknownSkill(t *testing.T) {
	tool, err := skills.DefineSkillTool([]skills.Skill{{Name: "pdf", Description: "PDF", Path: "/missing", Enabled: true}}, skills.ToolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry(tool)
	result := reg.Run(context.Background(), tools.ToolCall{ID: "call-1", Name: "skill", Args: `{"name":"other"}`}, nil)
	if !result.IsError || !strings.Contains(result.Text, "skill not found") {
		t.Fatalf("result = %#v", result)
	}
}

func writeSkill(t *testing.T, root, name, content string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
