package floret_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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
		{pkg: "engine", forbidden: []string{"github.com/floegence/floret/builtintools", "github.com/floegence/floret/internal/sessionlifecycle"}},
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
