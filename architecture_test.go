package floret_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestImportBoundaries(t *testing.T) {
	root := "."
	for _, rule := range []struct {
		pkg       string
		forbidden []string
	}{
		{pkg: "tools", forbidden: []string{"github.com/floegence/floret/engine", "github.com/floegence/floret/event", "github.com/floegence/floret/sessiontree", "github.com/floegence/floret/promptcache", "github.com/floegence/floret/internal/testui"}},
		{pkg: "builtintools", forbidden: []string{"github.com/floegence/floret/engine", "github.com/floegence/floret/adapters", "github.com/floegence/floret/sessiontree", "github.com/floegence/floret/promptcache", "github.com/floegence/floret/internal/testui"}},
		{pkg: "engine", forbidden: []string{"github.com/floegence/floret/builtintools", "github.com/floegence/floret/internal/sessionlifecycle", "github.com/floegence/floret/mcpclient", "github.com/floegence/floret/skills"}},
		{pkg: "agentharness", forbidden: []string{"github.com/floegence/floret/mcpclient", "github.com/floegence/floret/skills"}},
		{pkg: "mcpclient", forbidden: []string{"github.com/floegence/floret/engine", "github.com/floegence/floret/agentharness", "github.com/floegence/floret/sessiontree", "github.com/floegence/floret/internal/testui"}},
		{pkg: "skills", forbidden: []string{"github.com/floegence/floret/engine", "github.com/floegence/floret/agentharness", "github.com/floegence/floret/sessiontree", "github.com/floegence/floret/internal/testui"}},
		{pkg: "sessiontree", forbidden: []string{"github.com/floegence/floret/internal/sessionlifecycle"}},
		{pkg: "adapters", forbidden: []string{"github.com/floegence/floret/builtintools"}},
	} {
		t.Run(rule.pkg, func(t *testing.T) {
			imports := packageImports(t, filepath.Join(root, rule.pkg))
			for _, forbidden := range rule.forbidden {
				if imports[forbidden] {
					t.Fatalf("%s imports forbidden package %s", rule.pkg, forbidden)
				}
			}
		})
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
	for _, want := range []string{"resolved.ProviderHosted", "resolved.Client", "removeToolName(localSelected, builtintools.ToolWebSearch)"} {
		if !strings.Contains(testUIText, want) {
			t.Fatalf("test UI tool selection missing hosted/client search guard %q", want)
		}
	}
}

func TestNoBuiltInWebFetchBoundaryIsEnforced(t *testing.T) {
	builtins, err := os.ReadFile(filepath.Join("builtintools", "common.go"))
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
		"builtintools/common.go":                          string(builtins),
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
		"go test ./eval -run TestCleanCommandEnvRemovesHookRepositoryVariables -count=1",
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

func packageImports(t *testing.T, dir string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, entry.Name()), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			out[strings.Trim(imp.Path.Value, `"`)] = true
		}
	}
	return out
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
