package testui

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/testing/harness"
	"github.com/floegence/floret/tools/builtin"
)

func TestParseGitHubSkillURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want githubSkillSource
	}{
		{
			name: "tree",
			url:  "https://github.com/anthropics/skills/tree/main/skills/frontend-design",
			want: githubSkillSource{Owner: "anthropics", Repo: "skills", Ref: "main", SourcePath: "skills/frontend-design"},
		},
		{
			name: "blob",
			url:  "https://github.com/anthropics/skills/blob/main/skills/frontend-design/SKILL.md",
			want: githubSkillSource{Owner: "anthropics", Repo: "skills", Ref: "main", SourcePath: "skills/frontend-design", SingleFile: "SKILL.md"},
		},
		{
			name: "raw",
			url:  "https://raw.githubusercontent.com/anthropics/skills/main/skills/frontend-design/SKILL.md",
			want: githubSkillSource{Owner: "anthropics", Repo: "skills", Ref: "main", SourcePath: "skills/frontend-design", SingleFile: "SKILL.md"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGitHubSkillURL(tt.url)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("source = %#v, want %#v", got, tt.want)
			}
		})
	}
	if _, err := parseGitHubSkillURL("https://example.com/skills/frontend-design"); err == nil {
		t.Fatalf("non-GitHub URL should be rejected")
	}
	if _, err := parseGitHubSkillURL("https://github.com/anthropics/skills/blob/main/skills/frontend-design/README.md"); err == nil {
		t.Fatalf("blob URL that is not SKILL.md should be rejected")
	}
}

func TestRunnerPreviewAndInstallSkillFromGitHubTree(t *testing.T) {
	root := t.TempDir()
	server := fakeGitHubSkillServer(t)
	defer server.Close()
	withFakeGitHubDownloadBases(t, server.URL)
	runner := NewRunner(root)
	preview, err := runner.PreviewSkillInstall(context.Background(), SkillInstallPreviewRequest{
		URL: "https://github.com/anthropics/skills/tree/main/skills/frontend-design",
	})
	if err != nil {
		t.Fatal(err)
	}
	if preview.Name != "frontend-design" || preview.Description == "" || preview.License != "Complete terms in LICENSE.txt" || len(preview.Files) != 2 || preview.PreviewToken == "" {
		t.Fatalf("preview = %#v", preview)
	}
	if preview.RequiresReplace || preview.TargetPath != filepath.Join(root, managedSkillRootRel, "frontend-design") {
		t.Fatalf("preview target/replacement = %#v", preview)
	}
	resp, err := runner.InstallSkill(context.Background(), SkillInstallRequest{URL: preview.URL, PreviewToken: preview.PreviewToken})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.EnvUpdated || resp.SourceRoot != filepath.Join(root, managedSkillRootRel) {
		t.Fatalf("install response = %#v", resp)
	}
	if _, err := os.Stat(filepath.Join(root, managedSkillRootRel, "frontend-design", "SKILL.md")); err != nil {
		t.Fatalf("installed skill missing: %v", err)
	}
	env, err := os.ReadFile(filepath.Join(root, config.DefaultEnvFile))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(env), "FLORET_SKILLS_ENABLED") || !strings.Contains(string(env), filepath.Join(root, managedSkillRootRel)) {
		t.Fatalf("env was not updated: %s", env)
	}
	state, err := runner.ConfigState()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(state.Capabilities.Skills, func(skill SkillCapabilityState) bool {
		return skill.Name == "frontend-design" && skill.License == "Complete terms in LICENSE.txt" && skill.ContentHash != ""
	}) {
		t.Fatalf("installed skill not reflected in capabilities: %#v", state.Capabilities)
	}
	if !slices.ContainsFunc(state.Capabilities.SkillSources, func(source SkillSourceState) bool {
		return source.Managed && source.Enabled && source.SkillCount == 1
	}) {
		t.Fatalf("managed source missing: %#v", state.Capabilities.SkillSources)
	}
	again, err := runner.PreviewSkillInstall(context.Background(), SkillInstallPreviewRequest{URL: preview.URL})
	if err != nil {
		t.Fatal(err)
	}
	if !again.RequiresReplace || again.ExistingHash == "" {
		t.Fatalf("duplicate preview should require replace: %#v", again)
	}
	if again.ExistingHash != again.ContentHash {
		t.Fatalf("existing/content hashes should use the same content-hash basis: existing=%s content=%s", again.ExistingHash, again.ContentHash)
	}
	if _, err := runner.InstallSkill(context.Background(), SkillInstallRequest{URL: preview.URL, PreviewToken: again.PreviewToken}); err == nil {
		t.Fatalf("duplicate install without replace should be rejected")
	}
	if _, err := runner.InstallSkill(context.Background(), SkillInstallRequest{URL: preview.URL, PreviewToken: again.PreviewToken, Replace: true}); err != nil {
		t.Fatalf("replace install failed: %v", err)
	}
}

func TestRunnerPreviewRejectsUnsafeOrMalformedSkillMetadata(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "unsafe name",
			body: "---\nname: ../../escape\ndescription: bad\n---\n# Bad\n",
		},
		{
			name: "directory mismatch",
			body: "---\nname: other-skill\ndescription: bad\n---\n# Bad\n",
		},
		{
			name: "malformed frontmatter",
			body: "---\nname frontend-design\ndescription: bad\n---\n# Bad\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			server := fakeGitHubSkillServerWithBody(t, tt.body)
			defer server.Close()
			withFakeGitHubDownloadBases(t, server.URL)
			runner := NewRunner(root)
			if _, err := runner.PreviewSkillInstall(context.Background(), SkillInstallPreviewRequest{
				URL: "https://github.com/anthropics/skills/tree/main/skills/frontend-design",
			}); err == nil {
				t.Fatalf("preview should reject %s", tt.name)
			}
			if _, err := os.Stat(filepath.Join(root, "..", "escape")); !os.IsNotExist(err) {
				t.Fatalf("unsafe preview should not write outside managed root, stat err=%v", err)
			}
		})
	}
}

func TestRunnerPreviewBlobAndRawInstallWholeSkillDirectory(t *testing.T) {
	root := t.TempDir()
	server := fakeGitHubSkillServer(t)
	defer server.Close()
	withFakeGitHubDownloadBases(t, server.URL)
	runner := NewRunner(root)
	for _, rawURL := range []string{
		"https://github.com/anthropics/skills/blob/main/skills/frontend-design/SKILL.md",
		"https://raw.githubusercontent.com/anthropics/skills/main/skills/frontend-design/SKILL.md",
	} {
		preview, err := runner.PreviewSkillInstall(context.Background(), SkillInstallPreviewRequest{URL: rawURL})
		if err != nil {
			t.Fatalf("preview %s: %v", rawURL, err)
		}
		if len(preview.Files) != 2 || preview.License != "Complete terms in LICENSE.txt" {
			t.Fatalf("blob/raw preview should stage the whole directory: %#v", preview)
		}
	}
}

func TestRunnerPreviewStopsGitHubDownloadAtFileLimit(t *testing.T) {
	root := t.TempDir()
	server := fakeLargeGitHubSkillServer(t, maxSkillInstallFiles+1)
	defer server.Close()
	withFakeGitHubDownloadBases(t, server.URL)
	runner := NewRunner(root)
	if _, err := runner.PreviewSkillInstall(context.Background(), SkillInstallPreviewRequest{
		URL: "https://github.com/anthropics/skills/tree/main/skills/frontend-design",
	}); err == nil || !strings.Contains(err.Error(), "too many files") {
		t.Fatalf("preview should stop at file limit, err=%v", err)
	}
}

func TestEnsureManagedSkillEnvDeduplicatesPaths(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	managed := filepath.Join(root, managedSkillRootRel)
	other := filepath.Join(root, "other-skills")
	writeEnv(t, root, "FLORET_PROVIDER=fake\nFLORET_SKILLS_ENABLED=false\nFLORET_SKILLS_PATHS="+other+","+managed+","+other+"\n")

	updated, err := runner.ensureManagedSkillEnv(managed)
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatalf("env should be rewritten when enabled flag or path list changes")
	}
	env, err := os.ReadFile(filepath.Join(root, config.DefaultEnvFile))
	if err != nil {
		t.Fatal(err)
	}
	text := string(env)
	if !strings.Contains(text, `FLORET_SKILLS_ENABLED="true"`) {
		t.Fatalf("skills flag was not enabled: %s", text)
	}
	if strings.Count(text, managed) != 1 || strings.Count(text, other) != 1 {
		t.Fatalf("skill paths were not de-duplicated: %s", text)
	}
	updated, err = runner.ensureManagedSkillEnv(managed)
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatalf("second env update should be a no-op")
	}
}

func TestWriteStagedSkillReplacesInstalledDirectory(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "skills", "frontend-design")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "SKILL.md"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "old-only.txt"), []byte("remove me"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := writeStagedSkill(target, []stagedSkillFile{
		{Path: "SKILL.md", Data: []byte("new")},
		{Path: "assets/style.css", Data: []byte("body{}")},
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(target, "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Fatalf("replacement did not write new skill: %s", data)
	}
	if _, err := os.Stat(filepath.Join(target, "old-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("old-only file should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "assets", "style.css")); err != nil {
		t.Fatalf("nested file missing: %v", err)
	}
}

func TestServerSkillInstallAPIsAndArtifactRoute(t *testing.T) {
	root := t.TempDir()
	github := fakeGitHubSkillServer(t)
	defer github.Close()
	withFakeGitHubDownloadBases(t, github.URL)
	runner := NewRunner(root)
	server, err := NewServer(runner)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	previewReq := httptest.NewRequest(http.MethodPost, "/api/skills/preview", strings.NewReader(`{"url":"https://github.com/anthropics/skills/tree/main/skills/frontend-design"}`))
	previewRec := httptest.NewRecorder()
	handler.ServeHTTP(previewRec, previewReq)
	if previewRec.Code != http.StatusOK {
		t.Fatalf("preview status/body = %d %s", previewRec.Code, previewRec.Body.String())
	}
	var preview SkillInstallPreview
	if err := json.Unmarshal(previewRec.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	installReq := httptest.NewRequest(http.MethodPost, "/api/skills/install", strings.NewReader(`{"url":"`+preview.URL+`","preview_token":"`+preview.PreviewToken+`"}`))
	installRec := httptest.NewRecorder()
	handler.ServeHTTP(installRec, installReq)
	if installRec.Code != http.StatusOK {
		t.Fatalf("install status/body = %d %s", installRec.Code, installRec.Body.String())
	}
	artifact := filepath.Join(root, managedArtifactsRootRel, "frontend-design-landing", "index.html")
	if err := os.MkdirAll(filepath.Dir(artifact), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("<!doctype html><title>demo</title>"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifactRec := httptest.NewRecorder()
	handler.ServeHTTP(artifactRec, httptest.NewRequest(http.MethodGet, "/artifacts/frontend-design-landing/index.html", nil))
	if artifactRec.Code != http.StatusOK || !strings.Contains(artifactRec.Body.String(), "<title>demo</title>") {
		t.Fatalf("artifact status/body = %d %s", artifactRec.Code, artifactRec.Body.String())
	}
	escapeRec := httptest.NewRecorder()
	handler.ServeHTTP(escapeRec, httptest.NewRequest(http.MethodGet, "/artifacts/%2e%2e/.env.local", nil))
	if escapeRec.Code != http.StatusNotFound {
		t.Fatalf("path traversal should 404, got %d", escapeRec.Code)
	}
	secretDir := filepath.Join(root, "secret")
	if err := os.MkdirAll(secretDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secretDir, "secret.txt"), []byte("do not serve"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secretDir, filepath.Join(root, managedArtifactsRootRel, "leak")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	symlinkRec := httptest.NewRecorder()
	handler.ServeHTTP(symlinkRec, httptest.NewRequest(http.MethodGet, "/artifacts/leak/secret.txt", nil))
	if symlinkRec.Code != http.StatusNotFound {
		t.Fatalf("symlink traversal should 404, got %d body=%s", symlinkRec.Code, symlinkRec.Body.String())
	}
}

func TestRunnerSkillToolDemoWritesArtifact(t *testing.T) {
	root := t.TempDir()
	writeEnv(t, root, "FLORET_PROVIDER=fake\nFLORET_SKILLS_ENABLED=true\nFLORET_SKILLS_PATHS="+filepath.Join(root, managedSkillRootRel)+"\n")
	skillDir := filepath.Join(root, managedSkillRootRel, "frontend-design")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	skillBody := "---\nname: frontend-design\ndescription: Create distinctive frontend interfaces.\nlicense: Apache-2.0\n---\n# Frontend Design\n"
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(skillBody), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(
			harness.Step(
				provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "skill-1", Name: "skill", Args: `{"name":"frontend-design"}`}}},
				harness.DoneReason("tool_calls"),
			),
			harness.Step(
				provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "write-1", Name: builtin.ToolWrite, Args: `{"path":".floret-test-ui/artifacts/frontend-design-landing/index.html","content":"<!doctype html><title>Floret demo</title>"}`}}},
				harness.DoneReason("tool_calls"),
			),
			harness.Step(harness.Text("Landing page created at /artifacts/frontend-design-landing/index.html"), harness.Done()),
		), nil
	}
	result := runner.CreateAgentSession(context.Background(), AgentRunRequest{
		Profile:       ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		Message:       "Use frontend-design to build a landing page.",
		SystemPrompt:  "test",
		SelectedTools: []string{builtin.ToolRead, builtin.ToolList, builtin.ToolGlob, builtin.ToolGrep, builtin.ToolWrite},
	})
	if result.Status != "completed" {
		t.Fatalf("result = %#v", result)
	}
	if !slices.ContainsFunc(result.Events, func(ev event.Event) bool {
		meta, _ := ev.Metadata.(map[string]any)
		return ev.Type == event.SkillLoaded && meta["skill_id"] == "frontend-design"
	}) {
		t.Fatalf("skill loaded event missing: %#v", result.Events)
	}
	if !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == "skill" && msg.ToolCallID == "skill-1"
	}) || !slices.ContainsFunc(result.Observation.SessionMessages, func(msg ObservedSessionMessage) bool {
		return msg.Role == "assistant" && msg.ToolName == builtin.ToolWrite && msg.ToolCallID == "write-1"
	}) {
		t.Fatalf("skill/write tool calls missing: %#v", result.Observation.SessionMessages)
	}
	if _, err := os.Stat(filepath.Join(root, managedArtifactsRootRel, "frontend-design-landing", "index.html")); err != nil {
		t.Fatalf("artifact missing: %v", err)
	}
}

func fakeGitHubSkillServer(t *testing.T) *httptest.Server {
	return fakeGitHubSkillServerWithBody(t, "---\nname: frontend-design\ndescription: Create distinctive frontend interfaces.\nlicense: Complete terms in LICENSE.txt\n---\n# Frontend Design\n")
}

func fakeGitHubSkillServerWithBody(t *testing.T, skillBody string) *httptest.Server {
	t.Helper()
	var base string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/anthropics/skills/zip/refs/heads/main":
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write(fakeSkillArchive(t, skillBody, 0))
		case "/repos/anthropics/skills/contents/skills/frontend-design":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"name": "SKILL.md", "path": "skills/frontend-design/SKILL.md", "type": "file", "download_url": base + "/raw/skills/frontend-design/SKILL.md"},
				{"name": "LICENSE.txt", "path": "skills/frontend-design/LICENSE.txt", "type": "file", "download_url": base + "/raw/skills/frontend-design/LICENSE.txt"},
			})
		case "/raw/skills/frontend-design/SKILL.md":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(skillBody))
		case "/raw/skills/frontend-design/LICENSE.txt":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("Apache License\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	base = server.URL
	return server
}

func fakeLargeGitHubSkillServer(t *testing.T, fileCount int) *httptest.Server {
	t.Helper()
	var base string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/anthropics/skills/zip/refs/heads/main":
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write(fakeSkillArchive(t, "---\nname: frontend-design\ndescription: Create distinctive frontend interfaces.\n---\n# Frontend Design\n", fileCount))
		case "/repos/anthropics/skills/contents/skills/frontend-design":
			entries := []map[string]any{{
				"name": "SKILL.md", "path": "skills/frontend-design/SKILL.md", "type": "file", "size": 96, "download_url": base + "/raw/skills/frontend-design/SKILL.md",
			}}
			for i := 0; i < fileCount; i++ {
				name := fmt.Sprintf("asset-%02d.txt", i)
				entries = append(entries, map[string]any{"name": name, "path": "skills/frontend-design/" + name, "type": "file", "size": 1, "download_url": base + "/raw/skills/frontend-design/" + name})
			}
			_ = json.NewEncoder(w).Encode(entries)
		case "/raw/skills/frontend-design/SKILL.md":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("---\nname: frontend-design\ndescription: Create distinctive frontend interfaces.\n---\n# Frontend Design\n"))
		default:
			if strings.HasPrefix(r.URL.Path, "/raw/skills/frontend-design/asset-") {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write([]byte("x"))
				return
			}
			http.NotFound(w, r)
		}
	}))
	base = server.URL
	return server
}

func fakeSkillArchive(t *testing.T, skillBody string, extraFiles int) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	writeZipFile(t, zw, "skills-main/skills/frontend-design/SKILL.md", skillBody)
	writeZipFile(t, zw, "skills-main/skills/frontend-design/LICENSE.txt", "Apache License\n")
	for i := 0; i < extraFiles; i++ {
		writeZipFile(t, zw, fmt.Sprintf("skills-main/skills/frontend-design/asset-%02d.txt", i), "x")
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func writeZipFile(t *testing.T, zw *zip.Writer, name string, text string) {
	t.Helper()
	w, err := zw.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
}

func withFakeGitHubDownloadBases(t *testing.T, base string) {
	t.Helper()
	oldAPI := githubContentsAPIBase
	oldRaw := githubRawContentBase
	oldArchive := githubArchiveBase
	githubContentsAPIBase = base + "/repos"
	githubRawContentBase = base + "/raw"
	githubArchiveBase = base
	t.Cleanup(func() {
		githubContentsAPIBase = oldAPI
		githubRawContentBase = oldRaw
		githubArchiveBase = oldArchive
	})
}
