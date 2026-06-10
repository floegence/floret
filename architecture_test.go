package floret_test

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const modulePath = "github.com/floegence/floret"

func TestImportBoundaries(t *testing.T) {
	for _, rule := range []struct {
		name      string
		dir       string
		recursive bool
		forbidden []string
	}{
		{name: "tools", dir: "tools", forbidden: []string{modulePath + "/engine", modulePath + "/event", modulePath + "/provider/cache", modulePath + "/sessiontree", modulePath + "/tools/builtin", modulePath + "/tools/mcp", modulePath + "/tools/skills", modulePath + "/internal/testui"}},
		{name: "tools/builtin", dir: filepath.Join("tools", "builtin"), recursive: true, forbidden: []string{modulePath + "/engine", modulePath + "/provider/adapters", modulePath + "/provider/cache", modulePath + "/sessiontree", modulePath + "/internal/testui"}},
		{name: "tools/mcp", dir: filepath.Join("tools", "mcp"), recursive: true, forbidden: []string{modulePath + "/engine", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/internal/testui"}},
		{name: "tools/skills", dir: filepath.Join("tools", "skills"), recursive: true, forbidden: []string{modulePath + "/engine", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/internal/testui"}},
		{name: "engine", dir: "engine", forbidden: []string{modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/tools/builtin", modulePath + "/tools/mcp", modulePath + "/tools/skills", modulePath + "/internal/sessionlifecycle", modulePath + "/internal/testui"}},
		{name: "engine/compaction", dir: filepath.Join("engine", "compaction"), recursive: true, forbidden: []string{modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/tools/builtin", modulePath + "/tools/mcp", modulePath + "/tools/skills", modulePath + "/internal/testui"}},
		{name: "session/compaction", dir: filepath.Join("session", "compaction"), recursive: true, forbidden: []string{modulePath + "/provider", modulePath + "/engine", modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/tools"}},
		{name: "agentharness", dir: "agentharness", forbidden: []string{modulePath + "/runtime", modulePath + "/tools/builtin", modulePath + "/tools/mcp", modulePath + "/tools/skills", modulePath + "/provider/adapters", modulePath + "/internal/testui"}},
		{name: "provider", dir: "provider", forbidden: []string{modulePath + "/engine", modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/tools", modulePath + "/sessiontree", modulePath + "/internal/testui"}},
		{name: "provider/adapters", dir: filepath.Join("provider", "adapters"), recursive: true, forbidden: []string{modulePath + "/engine", modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/tools/builtin", modulePath + "/internal/testui"}},
		{name: "observation", dir: "observation", forbidden: []string{modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/internal/testui"}},
		{name: "sessiontree", dir: "sessiontree", forbidden: []string{modulePath + "/engine", modulePath + "/engine/compaction", modulePath + "/runtime", modulePath + "/internal/sessionlifecycle"}},
		{name: "runtime/storage", dir: filepath.Join("runtime", "storage"), forbidden: []string{modulePath + "/runtime/storage/sqlite"}},
	} {
		t.Run(rule.name, func(t *testing.T) {
			imports := packageImports(t, rule.dir, rule.recursive, false)
			for _, forbidden := range rule.forbidden {
				if imports[forbidden] {
					t.Fatalf("%s imports forbidden package %s", rule.name, forbidden)
				}
			}
		})
	}
}

func TestTransitiveImportBoundaries(t *testing.T) {
	for _, rule := range []struct {
		name      string
		pkg       string
		forbidden []string
	}{
		{name: "provider", pkg: "./provider", forbidden: []string{modulePath + "/engine", modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/tools", modulePath + "/sessiontree", modulePath + "/internal/testui"}},
		{name: "tools", pkg: "./tools", forbidden: []string{modulePath + "/engine", modulePath + "/event", modulePath + "/sessiontree", modulePath + "/tools/builtin", modulePath + "/tools/mcp", modulePath + "/tools/skills", modulePath + "/internal/testui"}},
		{name: "engine", pkg: "./engine", forbidden: []string{modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/tools/builtin", modulePath + "/tools/mcp", modulePath + "/tools/skills", modulePath + "/internal/sessionlifecycle", modulePath + "/internal/testui"}},
		{name: "sessiontree", pkg: "./sessiontree", forbidden: []string{modulePath + "/engine", modulePath + "/engine/compaction", modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/internal/sessionlifecycle", modulePath + "/internal/testui"}},
		{name: "session/compaction", pkg: "./session/compaction", forbidden: []string{modulePath + "/provider", modulePath + "/engine", modulePath + "/runtime", modulePath + "/agentharness", modulePath + "/sessiontree", modulePath + "/tools"}},
	} {
		t.Run(rule.name, func(t *testing.T) {
			deps := packageDeps(t, rule.pkg)
			for _, forbidden := range rule.forbidden {
				for dep := range deps {
					if dep == forbidden || strings.HasPrefix(dep, forbidden+"/") {
						t.Fatalf("%s transitively depends on forbidden package %s via %s", rule.name, forbidden, dep)
					}
				}
			}
		})
	}
}

func TestTopLevelPackageLayoutIsConstrained(t *testing.T) {
	allowed := map[string]bool{
		".githooks":    true,
		"agentharness": true,
		"cmd":          true,
		"config":       true,
		"docs":         true,
		"engine":       true,
		"event":        true,
		"internal":     true,
		"observation":  true,
		"provider":     true,
		"runtime":      true,
		"scripts":      true,
		"session":      true,
		"sessiontree":  true,
		"testing":      true,
		"tools":        true,
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == ".git" || name == ".floret-test-ui" || name == "node_modules" || name == "vendor" {
			continue
		}
		if !allowed[name] {
			t.Fatalf("unexpected top-level directory %q; add public packages under an existing domain package", name)
		}
	}
}

func TestDeprecatedRootPackagesAreRemoved(t *testing.T) {
	for _, dir := range deprecatedRootPackages() {
		if _, err := os.Stat(dir); err == nil {
			t.Fatalf("deprecated root package directory still exists: %s", dir)
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

func TestNoDeprecatedRootImportPaths(t *testing.T) {
	files := textFiles(t, ".")
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, dir := range deprecatedRootPackages() {
			oldPath := modulePath + "/" + dir
			if strings.Contains(text, oldPath) {
				if !allowedDeprecatedImportMapping(file, text, oldPath) {
					t.Fatalf("%s still references deprecated import path %s", file, oldPath)
				}
			}
		}
	}
}

func TestReadmeDoesNotAdvertiseDeprecatedRootPackages(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, dir := range deprecatedRootPackages() {
		token := "`" + dir + "`"
		if strings.Contains(text, token) {
			t.Fatalf("README still advertises deprecated root package %s", token)
		}
	}
	for _, want := range []string{
		"`provider/adapters`",
		"`provider/cache`",
		"`provider/catalog`",
		"`observation`",
		"`runtime/storage`",
		"`runtime/storage/sqlite`",
		"`session/compaction`",
		"`session/contextpolicy`",
		"`testing/eval`",
		"`testing/harness`",
		"`tools/builtin`",
		"`tools/mcp`",
		"`tools/skills`",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("README missing new package path %s", want)
		}
	}
}

func TestEngineConfigKeepsMemoryInternal(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("engine", "engine.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "SystemPrompt string") {
		t.Fatalf("engine.Config should expose SystemPrompt instead of memory.Manager")
	}
	for _, forbidden := range []string{"Memory *memory.Manager", modulePath + "/memory"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("engine.Config must not expose deprecated memory API: %s", forbidden)
		}
	}
}

func TestKernelBoundaryFilesAvoidHostProductConcepts(t *testing.T) {
	for _, file := range []string{
		filepath.Join("engine", "control.go"),
		filepath.Join("engine", "engine.go"),
		filepath.Join("event", "event.go"),
		filepath.Join("provider", "provider.go"),
		filepath.Join("tools", "invocation.go"),
		filepath.Join("tools", "permission.go"),
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ToLower(string(data))
		for _, forbidden := range []string{
			"flower",
			"redeven",
			"message block",
			"messageblock",
			"target_id",
			"endpoint_id",
			"plan_mode",
			"handoff",
			"followups",
			"followup queue",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s must not expose host product concept %q", file, forbidden)
			}
		}
	}
}

func TestSessionLifecycleBoundaryIsEnforced(t *testing.T) {
	lifecycle, err := os.ReadFile(filepath.Join("internal", "sessionlifecycle", "lifecycle.go"))
	if err != nil {
		t.Fatal(err)
	}
	lifecycleText := string(lifecycle)
	if !strings.Contains(lifecycleText, "IMPORTANT: SessionLifecycle is the only host/UI boundary") {
		t.Fatalf("session lifecycle boundary must be protected by an IMPORTANT comment")
	}
	for _, forbidden := range []string{"type status string", "const (\n\tstatusIdle", "func Derive("} {
		if !strings.Contains(lifecycleText, forbidden) {
			t.Fatalf("session lifecycle package missing expected constrained construct %q", forbidden)
		}
	}

	testUIRunner, err := os.ReadFile(filepath.Join("internal", "testui", "runner.go"))
	if err != nil {
		t.Fatal(err)
	}
	testUIText := string(testUIRunner)
	for _, forbidden := range []string{
		"latestSessionStatus",
		"status == string(engine.Waiting)",
		"status == string(engine.Completed)",
		"status == \"idle\"",
		"Status == \"running\"",
		"Phase == \"turn\"",
	} {
		if strings.Contains(testUIText, forbidden) {
			t.Fatalf("test UI must derive lifecycle decisions through internal/sessionlifecycle, found %q", forbidden)
		}
	}

	agents, err := os.ReadFile("AGENTS.md")
	if err != nil {
		t.Fatal(err)
	}
	agentsText := string(agents)
	for _, want := range []string{
		"## IMPORTANT Design Constraints",
		"`IMPORTANT:` comments mark product, security, or interaction invariants",
		"Do not work around an `IMPORTANT:` constraint with hidden fallback behavior",
	} {
		if !strings.Contains(agentsText, want) {
			t.Fatalf("AGENTS.md missing IMPORTANT constraint rule %q", want)
		}
	}
}

func TestWebSearchCapabilityBoundaryIsEnforced(t *testing.T) {
	searchCap, err := os.ReadFile(filepath.Join("internal", "searchcap", "searchcap.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(searchCap)
	if !strings.Contains(text, "IMPORTANT: Web search source selection must be derived from provider profile") {
		t.Fatalf("web search capability resolver must be protected by an IMPORTANT comment")
	}
	for _, forbidden := range []string{
		"ProviderDeepSeek",
		"ProviderOpenAI",
		"ProviderOpenRouter",
		"ProviderGoogle",
		"ProviderQwen",
		"ProviderMoonshot",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("web search capability resolver must not special-case provider names, found %q", forbidden)
		}
	}
	testUI, err := os.ReadFile(filepath.Join("internal", "testui", "tool_selection.go"))
	if err != nil {
		t.Fatal(err)
	}
	testUIText := string(testUI)
	for _, want := range []string{"resolved.Available", "resolved.Source == searchcap.WebSearchProviderHosted", "removeToolName(localSelected, builtin.ToolWebSearch)"} {
		if !strings.Contains(testUIText, want) {
			t.Fatalf("test UI tool selection missing single-source search guard %q", want)
		}
	}
}

func TestNoBuiltInWebFetchBoundaryIsEnforced(t *testing.T) {
	builtins, err := os.ReadFile(filepath.Join("tools", "builtin", "common.go"))
	if err != nil {
		t.Fatal(err)
	}
	testUI, err := os.ReadFile(filepath.Join("internal", "testui", "tool_selection.go"))
	if err != nil {
		t.Fatal(err)
	}
	staticMatrix, err := os.ReadFile(filepath.Join("internal", "testui", "static", "components", "toolMatrix.js"))
	if err != nil {
		t.Fatal(err)
	}
	for path, text := range map[string]string{
		"tools/builtin/common.go":                         string(builtins),
		"internal/testui/tool_selection.go":               string(testUI),
		"internal/testui/static/components/toolMatrix.js": string(staticMatrix),
	} {
		if strings.Contains(text, "web_fetch") || strings.Contains(text, "ToolWebFetch") || strings.Contains(text, "RegisterNetwork") {
			t.Fatalf("%s must not expose built-in web_fetch", path)
		}
	}
	if !strings.Contains(string(testUI), "IMPORTANT: Floret core does not expose a built-in URL fetch/browser-lite") {
		t.Fatalf("web fetch boundary must be protected by an IMPORTANT comment")
	}
}

func TestTestUIDoesNotDefaultAgentTurnsToWallTime(t *testing.T) {
	for _, file := range []string{
		filepath.Join("internal", "testui", "runner.go"),
		filepath.Join("internal", "testui", "session_metadata.go"),
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		if strings.Contains(text, "WallTime:                60 * time.Second") || strings.Contains(text, "cfg.WallTime = 60 * time.Second") {
			t.Fatalf("%s must not default ordinary agent sessions to a 60s wall-time", file)
		}
	}
}

func TestPreCommitQualityGateIsEnforced(t *testing.T) {
	hook, err := os.ReadFile(filepath.Join(".githooks", "pre-commit"))
	if err != nil {
		t.Fatal(err)
	}
	hookText := string(hook)
	if !strings.Contains(hookText, "exec scripts/pre-commit.sh") {
		t.Fatalf("committed pre-commit hook must delegate to scripts/pre-commit.sh")
	}
	script, err := os.ReadFile(filepath.Join("scripts", "pre-commit.sh"))
	if err != nil {
		t.Fatal(err)
	}
	scriptText := string(script)
	for _, want := range []string{
		"go test ./...",
		"TestServerStreamsAgentTurnEventsBeforeCompletion",
		"TestServerAgentSessionTurnIgnoresServerTimeout",
		"TestRunnerRunningSnapshotUsesRealTurnID",
		"go test ./testing/eval -run TestCleanCommandEnvRemovesHookRepositoryVariables -count=1",
		"node --check internal/testui/static/*.js internal/testui/static/views/*.js internal/testui/static/components/*.js",
		"git diff --check",
	} {
		if !strings.Contains(scriptText, want) {
			t.Fatalf("pre-commit quality gate missing %q", want)
		}
	}
}

func TestTurnFinalizationInvariantIsDocumented(t *testing.T) {
	harness, err := os.ReadFile(filepath.Join("agentharness", "harness.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(harness), "IMPORTANT: Turn finalization must outlive caller cancellation") {
		t.Fatalf("turn finalization cancellation boundary must be protected by an IMPORTANT comment")
	}
}

func TestNoLegacyToolHandlerOrHostedDispatchInLocalTools(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("tools", "tools.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, forbidden := range []string{"type Handler func(context.Context, string)", "RequiresApproval bool"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("legacy tool contract still present: %s", forbidden)
		}
	}
	if strings.Contains(text, "HostedToolDefinition") || strings.Contains(text, "HostedTools") {
		t.Fatalf("generic local tool runtime must not dispatch provider-hosted tools")
	}
}

func TestNoDirectEngineLiteralConstructionOutsideTests(t *testing.T) {
	files := goFiles(t, ".")
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "&engine.Engine{") || strings.Contains(string(data), "new(engine.Engine)") {
			t.Fatalf("%s must construct engines through engine.New(engine.Config)", file)
		}
	}
}

func TestNoGoWorkFilesInRepository(t *testing.T) {
	for _, file := range goFilesAndWorkspaces(t, ".") {
		if filepath.Base(file) == "go.work" || filepath.Base(file) == "go.work.sum" {
			t.Fatalf("repository must not introduce %s", file)
		}
	}
}

func packageImports(t *testing.T, dir string, recursive, includeTests bool) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	fset := token.NewFileSet()
	for _, path := range goFilesInDir(t, dir, recursive) {
		if !includeTests && strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			out[strings.Trim(imp.Path.Value, `"`)] = true
		}
	}
	return out
}

func goFilesInDir(t *testing.T, dir string, recursive bool) []string {
	t.Helper()
	if !recursive {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var files []string
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				continue
			}
			files = append(files, filepath.Join(dir, entry.Name()))
		}
		return files
	}
	files, err := walkFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(files, func(file string) bool {
		return !strings.HasSuffix(file, ".go")
	})
}

func packageDeps(t *testing.T, pkg string) map[string]bool {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", pkg)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps %s: %v", pkg, err)
	}
	deps := map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			deps[line] = true
		}
	}
	return deps
}

func goFiles(t *testing.T, root string) []string {
	t.Helper()
	files, err := walkFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(files, func(file string) bool {
		return !strings.HasSuffix(file, ".go")
	})
}

func textFiles(t *testing.T, root string) []string {
	t.Helper()
	files, err := walkFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(files, func(file string) bool {
		switch filepath.Ext(file) {
		case ".go", ".md", ".sh", ".js":
			return false
		default:
			return true
		}
	})
}

func deprecatedRootPackages() []string {
	return []string{
		"adapters",
		"builtintools",
		"compaction",
		"contextpolicy",
		"control",
		"eval",
		"harness",
		"mcpclient",
		"memory",
		"modelcatalog",
		"promptcache",
		"skills",
		"sqlitestore",
		"storage",
	}
}

func allowedDeprecatedImportMapping(file, text, oldPath string) bool {
	if file != "README.md" {
		return false
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, oldPath) {
			continue
		}
		if !strings.Contains(line, " -> "+modulePath+"/") {
			return false
		}
		if strings.Contains(line, "`") {
			return false
		}
		return true
	}
	return false
}

func goFilesAndWorkspaces(t *testing.T, root string) []string {
	t.Helper()
	files, err := walkFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func walkFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".floret-test-ui", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		files = append(files, path)
		return nil
	})
	return files, err
}
