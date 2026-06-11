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

func TestPublicPackageAllowlist(t *testing.T) {
	out, err := exec.Command("go", "list", "./...").Output()
	if err != nil {
		t.Fatalf("go list ./...: %v", err)
	}
	allowed := map[string]bool{
		modulePath:                  true,
		modulePath + "/config":      true,
		modulePath + "/runtime":     true,
		modulePath + "/tools":       true,
		modulePath + "/observation": true,
	}
	for _, line := range strings.Split(string(out), "\n") {
		pkg := strings.TrimSpace(line)
		if pkg == "" || strings.Contains(pkg, "/internal/") || strings.HasPrefix(pkg, modulePath+"/cmd/") {
			continue
		}
		if !allowed[pkg] {
			t.Fatalf("unexpected public package %s", pkg)
		}
	}
}

func TestTopLevelPackageLayoutIsConstrained(t *testing.T) {
	allowed := map[string]bool{
		".githooks":   true,
		"cmd":         true,
		"config":      true,
		"internal":    true,
		"observation": true,
		"runtime":     true,
		"scripts":     true,
		"tools":       true,
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
			t.Fatalf("unexpected top-level directory %q", name)
		}
	}
}

func TestImplementationPackagesAreInternalOnly(t *testing.T) {
	for _, dir := range []string{
		"agentharness",
		"engine",
		"event",
		"provider",
		"session",
		"sessiontree",
		filepath.Join("runtime", "storage"),
		"testing",
	} {
		if _, err := os.Stat(dir); err == nil {
			t.Fatalf("implementation package must live under internal, found root %s", dir)
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{
		filepath.Join("internal", "agentharness"),
		filepath.Join("internal", "engine"),
		filepath.Join("internal", "event"),
		filepath.Join("internal", "provider"),
		filepath.Join("internal", "session"),
		filepath.Join("internal", "sessiontree"),
		filepath.Join("internal", "storage"),
		filepath.Join("internal", "testing"),
		filepath.Join("internal", "tools", "builtin"),
		filepath.Join("internal", "tools", "mcp"),
		filepath.Join("internal", "tools", "skills"),
	} {
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Fatalf("internal implementation package missing: %s", dir)
		}
	}
}

func TestPublicPackagesDoNotExposeInternalContracts(t *testing.T) {
	for _, pkg := range []string{".", "./config", "./runtime", "./tools", "./observation"} {
		out, err := exec.Command("go", "doc", "-all", pkg).CombinedOutput()
		if err != nil {
			t.Fatalf("go doc -all %s: %v\n%s", pkg, err, out)
		}
		text := string(out)
		for _, forbidden := range []string{
			"/internal/",
			"agentharness.",
			"artifact.",
			"builtin.",
			"cache.",
			"contextpolicy.",
			"engine.",
			"event.",
			"mcp.",
			"provider.",
			"session.",
			"sessiontree.",
			"skills.",
			"storage.",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s public docs expose internal contract %q", pkg, forbidden)
			}
		}
	}
}

func TestRootPackageStaysLightweight(t *testing.T) {
	text := readTextFile(t, "floret.go")
	for _, want := range []string{"const Version", "type ThreadID = runtime.ThreadID", "type PromptScopeID = runtime.PromptScopeID"} {
		if !strings.Contains(text, want) {
			t.Fatalf("root package missing lightweight export %q", want)
		}
	}
	for _, forbidden := range []string{"func New", "type Host", "type Store", "internal/"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("root package must remain lightweight, found %q", forbidden)
		}
	}
}

func TestPublicPackagesDoNotImportForbiddenImplementationPackages(t *testing.T) {
	for _, rule := range []struct {
		dir       string
		forbidden []string
	}{
		{dir: "tools", forbidden: []string{modulePath + "/internal/provider", modulePath + "/internal/engine", modulePath + "/internal/sessiontree", modulePath + "/internal/storage", modulePath + "/internal/testui"}},
		{dir: "observation", forbidden: []string{modulePath + "/internal/", modulePath + "/runtime"}},
		{dir: "runtime", forbidden: []string{modulePath + "/internal/testui", modulePath + "/cmd/"}},
	} {
		imports := packageImports(t, rule.dir, false, false)
		for _, forbidden := range rule.forbidden {
			for imp := range imports {
				if imp == forbidden || strings.HasPrefix(imp, forbidden+"/") || strings.HasPrefix(imp, forbidden) && strings.HasSuffix(forbidden, "/") {
					t.Fatalf("%s imports forbidden package %s", rule.dir, imp)
				}
			}
		}
	}
}

func TestReadmeOnlyDocumentsStableDownstreamAPI(t *testing.T) {
	text := readTextFile(t, "README.md")
	for _, want := range []string{"runtime.NewHost", "runtime.NewMemoryStore", "runtime.OpenSQLiteStore", "tools.Registry", "observation"} {
		if !strings.Contains(text, want) {
			t.Fatalf("README missing stable downstream API %q", want)
		}
	}
	for _, forbidden := range publicDocsDenylist() {
		if strings.Contains(text, forbidden) {
			t.Fatalf("README advertises internal/downstream-forbidden API %q", forbidden)
		}
	}
}

func TestDocumentationDoesNotTeachForbiddenDownstreamImports(t *testing.T) {
	for _, file := range textFiles(t, ".") {
		if file == "architecture_test.go" {
			continue
		}
		if strings.HasPrefix(file, filepath.Join("internal", "testui", "static")+string(filepath.Separator)) {
			continue
		}
		text := readTextFile(t, file)
		for _, forbidden := range forbiddenDownstreamImportPaths() {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s references forbidden downstream import %s", file, forbidden)
			}
		}
	}
}

func TestProviderSDKImportsStayInInternalAdapters(t *testing.T) {
	for _, file := range goFiles(t, ".") {
		if file == "architecture_test.go" {
			continue
		}
		if strings.HasPrefix(file, filepath.Join("internal", "provider", "adapters")+string(filepath.Separator)) {
			continue
		}
		text := readTextFile(t, file)
		for _, marker := range []string{
			"github.com/openai/openai-go",
			"github.com/anthropics/anthropic-sdk-go",
			"google.golang.org/genai",
		} {
			if strings.Contains(text, marker) {
				t.Fatalf("provider SDK import %q outside internal/provider/adapters: %s", marker, file)
			}
		}
	}
}

func TestSQLiteDriverImportsStayInInternalStorage(t *testing.T) {
	for _, file := range goFiles(t, ".") {
		if file == "architecture_test.go" {
			continue
		}
		if strings.HasPrefix(file, filepath.Join("internal", "storage", "sqlite")+string(filepath.Separator)) {
			continue
		}
		if strings.Contains(readTextFile(t, file), "github.com/mattn/go-sqlite3") {
			t.Fatalf("sqlite driver import outside internal/storage/sqlite: %s", file)
		}
	}
}

func TestKernelBoundaryFilesAvoidHostProductConcepts(t *testing.T) {
	for _, file := range []string{
		filepath.Join("internal", "engine", "control.go"),
		filepath.Join("internal", "engine", "engine.go"),
		filepath.Join("internal", "event", "event.go"),
		filepath.Join("internal", "provider", "provider.go"),
		filepath.Join("tools", "invocation.go"),
		filepath.Join("tools", "permission.go"),
	} {
		text := strings.ToLower(readTextFile(t, file))
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

func TestCoreIdentityDoesNotUseHostSessionID(t *testing.T) {
	for _, file := range goFiles(t, ".") {
		if file == "architecture_test.go" {
			continue
		}
		if strings.HasPrefix(file, filepath.Join("internal", "testui")+string(filepath.Separator)) {
			continue
		}
		text := readTextFile(t, file)
		for _, forbidden := range []string{"Session" + "ID", `json:"session_` + `id"`} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s uses host session identity %q outside test UI", file, forbidden)
			}
		}
	}
}

func TestPromptCacheIdentityUsesPromptScope(t *testing.T) {
	cacheText := readTextFile(t, filepath.Join("internal", "provider", "cache", "promptcache.go"))
	for _, want := range []string{"PromptScopeID", `json:"prompt_scope_id"`, "CreatedByRunID", "CreatedByTurnID", "DeletePromptScopes"} {
		if !strings.Contains(cacheText, want) {
			t.Fatalf("prompt cache contract missing %q", want)
		}
	}
	for _, forbidden := range []string{"DeleteRuns", "runIDFromRequest", "cacheScopeID"} {
		if strings.Contains(cacheText, forbidden) {
			t.Fatalf("prompt cache still contains removed scope helper %q", forbidden)
		}
	}

	sqliteText := readTextFile(t, filepath.Join("internal", "storage", "sqlite", "sqlitestore.go"))
	for _, want := range []string{"prompt_scope_id TEXT NOT NULL", "DeletePromptScopes", "DeleteThreadData"} {
		if !strings.Contains(sqliteText, want) {
			t.Fatalf("sqlite storage contract missing %q", want)
		}
	}
	if strings.Contains(sqliteText, "run_id TEXT NOT NULL") || strings.Contains(sqliteText, "DeleteRuns") || strings.Contains(sqliteText, "DeleteSession") {
		t.Fatalf("sqlite storage still uses removed run/session cache ownership")
	}
}

func TestWebSearchCapabilityBoundaryIsEnforced(t *testing.T) {
	text := readTextFile(t, filepath.Join("internal", "searchcap", "searchcap.go"))
	if !strings.Contains(text, "IMPORTANT: Web search source selection must be derived from provider profile") {
		t.Fatalf("web search capability resolver must be protected by an IMPORTANT comment")
	}
	for _, forbidden := range []string{"ProviderDeepSeek", "ProviderOpenAI", "ProviderOpenRouter", "ProviderGoogle", "ProviderQwen", "ProviderMoonshot"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("web search capability resolver must not special-case provider names, found %q", forbidden)
		}
	}
	testUI := readTextFile(t, filepath.Join("internal", "testui", "tool_selection.go"))
	for _, want := range []string{"resolved.Available", "resolved.Source == searchcap.WebSearchProviderHosted", "removeToolName(localSelected, builtin.ToolWebSearch)"} {
		if !strings.Contains(testUI, want) {
			t.Fatalf("test UI tool selection missing single-source search guard %q", want)
		}
	}
}

func TestNoBuiltInWebFetchBoundaryIsEnforced(t *testing.T) {
	builtins := readTextFile(t, filepath.Join("internal", "tools", "builtin", "common.go"))
	testUI := readTextFile(t, filepath.Join("internal", "testui", "tool_selection.go"))
	staticMatrix := readTextFile(t, filepath.Join("internal", "testui", "static", "components", "toolMatrix.js"))
	for path, text := range map[string]string{
		"internal/tools/builtin/common.go":                builtins,
		"internal/testui/tool_selection.go":               testUI,
		"internal/testui/static/components/toolMatrix.js": staticMatrix,
	} {
		if strings.Contains(text, "web_fetch") || strings.Contains(text, "ToolWebFetch") || strings.Contains(text, "RegisterNetwork") {
			t.Fatalf("%s must not expose built-in web_fetch", path)
		}
	}
	if !strings.Contains(testUI, "IMPORTANT: Floret core does not expose a built-in URL fetch/browser-lite") {
		t.Fatalf("web fetch boundary must be protected by an IMPORTANT comment")
	}
}

func TestSessionLifecycleBoundaryIsEnforced(t *testing.T) {
	lifecycleText := readTextFile(t, filepath.Join("internal", "sessionlifecycle", "lifecycle.go"))
	if !strings.Contains(lifecycleText, "IMPORTANT: SessionLifecycle is the only host/UI boundary") {
		t.Fatalf("session lifecycle boundary must be protected by an IMPORTANT comment")
	}
	for _, want := range []string{"type status string", "const (\n\tstatusIdle", "func Derive("} {
		if !strings.Contains(lifecycleText, want) {
			t.Fatalf("session lifecycle package missing expected constrained construct %q", want)
		}
	}

	testUIText := readTextFile(t, filepath.Join("internal", "testui", "runner.go"))
	for _, forbidden := range []string{"latestSessionStatus", "status == string(engine.Waiting)", "status == string(engine.Completed)", "status == \"idle\"", "Status == \"running\"", "Phase == \"turn\""} {
		if strings.Contains(testUIText, forbidden) {
			t.Fatalf("test UI must derive lifecycle decisions through internal/sessionlifecycle, found %q", forbidden)
		}
	}
}

func TestTurnFinalizationInvariantIsDocumented(t *testing.T) {
	harness := readTextFile(t, filepath.Join("internal", "agentharness", "harness.go"))
	if !strings.Contains(harness, "IMPORTANT: Turn finalization must outlive caller cancellation") {
		t.Fatalf("turn finalization cancellation boundary must be protected by an IMPORTANT comment")
	}
}

func TestConceptVocabularyIsDocumented(t *testing.T) {
	text := readTextFile(t, "AGENTS.md")
	for _, want := range []string{
		"## Concept Vocabulary and Identity Rules",
		"`ThreadID` identifies a durable conversation journal",
		"`TurnID` identifies one user-facing turn",
		"`RunID` identifies one engine/provider execution",
		"`PromptScopeID` identifies the reuse boundary",
		"`SessionID` and `session_id` are not core execution identities",
		"`TranscriptStore` stores engine-level transcript messages",
		"Prompt-cache rows and JSON must use `prompt_scope_id` / `PromptScopeID`",
		"Provider raw plans are provider-specific rendered fragments",
		"Floret has not launched",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("AGENTS.md missing concept rule %q", want)
		}
	}
}

func TestRemovedCompatibilityShapesDoNotReturn(t *testing.T) {
	for _, file := range append(goFiles(t, "."), textFiles(t, ".")...) {
		if file == "architecture_test.go" {
			continue
		}
		if strings.HasPrefix(file, filepath.Join("internal", "testui", "static")+string(filepath.Separator)) {
			continue
		}
		text := strings.ToLower(readTextFile(t, file))
		for _, forbidden := range []string{"legacy shape", "compatibility fallback", "backward compatibility", "old contract", "old shape fallback"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s contains removed compatibility marker %q", file, forbidden)
			}
		}
	}
}

func TestRemovedToolHandlerOrHostedDispatchDoesNotReturn(t *testing.T) {
	text := readTextFile(t, filepath.Join("tools", "tools.go"))
	for _, forbidden := range []string{"type Handler func(context.Context, string)", "RequiresApproval bool"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("removed tool contract returned: %s", forbidden)
		}
	}
	if strings.Contains(text, "HostedToolDefinition") || strings.Contains(text, "HostedTools") {
		t.Fatalf("generic local tool runtime must not dispatch provider-hosted tools")
	}
}

func TestNoDirectEngineLiteralConstructionOutsideTests(t *testing.T) {
	for _, file := range goFiles(t, ".") {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		text := readTextFile(t, file)
		if strings.Contains(text, "&engine.Engine{") || strings.Contains(text, "new(engine.Engine)") {
			t.Fatalf("%s must construct engines through engine.New(engine.Config)", file)
		}
	}
}

func TestNoGoWorkFilesInRepository(t *testing.T) {
	for _, file := range walkAllFiles(t, ".") {
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
			out[strings.Trim(imp.Path.Value, "\"")] = true
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
	files := walkAllFiles(t, dir)
	return slices.DeleteFunc(files, func(file string) bool { return !strings.HasSuffix(file, ".go") })
}

func goFiles(t *testing.T, root string) []string {
	t.Helper()
	files := walkAllFiles(t, root)
	return slices.DeleteFunc(files, func(file string) bool { return !strings.HasSuffix(file, ".go") })
}

func textFiles(t *testing.T, root string) []string {
	t.Helper()
	files := walkAllFiles(t, root)
	return slices.DeleteFunc(files, func(file string) bool {
		switch filepath.Ext(file) {
		case ".go", ".md", ".sh", ".js":
			return false
		default:
			return true
		}
	})
}

func walkAllFiles(t *testing.T, root string) []string {
	t.Helper()
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
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func readTextFile(t *testing.T, file string) string {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func forbiddenDownstreamImportPaths() []string {
	return []string{
		modulePath + "/agentharness",
		modulePath + "/engine",
		modulePath + "/event",
		modulePath + "/provider",
		modulePath + "/session",
		modulePath + "/sessiontree",
		modulePath + "/runtime/storage",
		modulePath + "/testing",
		modulePath + "/tools/builtin",
		modulePath + "/tools/mcp",
		modulePath + "/tools/skills",
	}
}

func publicDocsDenylist() []string {
	return []string{
		"agentharness",
		"engine.Engine",
		"engine.New",
		"provider.Provider",
		"provider/cache",
		"provider/adapters",
		"provider/catalog",
		"sessiontree",
		"runtime/storage",
		"tools/builtin",
		"tools/mcp",
		"tools/skills",
	}
}
