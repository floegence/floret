package floret_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	floretRuntime "github.com/floegence/floret/v2/runtime"
	floretTools "github.com/floegence/floret/v2/tools"
)

const (
	modulePath        = "github.com/floegence/floret/v2"
	redevenModulePath = "github.com/floegence/redeven"
)

func TestMain(m *testing.M) {
	root, err := findRepoRoot()
	if err != nil {
		panic(err)
	}
	if err := os.Chdir(root); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		next := filepath.Dir(dir)
		if next == dir {
			return "", os.ErrNotExist
		}
		dir = next
	}
}

func TestPublicPackageAllowlist(t *testing.T) {
	out, err := exec.Command("go", "list", "./...").Output()
	if err != nil {
		t.Fatalf("go list ./...: %v", err)
	}
	allowed := map[string]bool{
		modulePath + "/config":      true,
		modulePath + "/provider":    true,
		modulePath + "/runtime":     true,
		modulePath + "/storage":     true,
		modulePath + "/tools":       true,
		modulePath + "/observation": true,
	}
	testOnly := map[string]bool{
		modulePath + "/florettest": true,
	}
	for _, line := range strings.Split(string(out), "\n") {
		pkg := strings.TrimSpace(line)
		if pkg == "" || strings.Contains(pkg, "/internal/") || strings.HasPrefix(pkg, modulePath+"/cmd/") {
			continue
		}
		if !allowed[pkg] && !testOnly[pkg] {
			t.Fatalf("unexpected public package %s", pkg)
		}
	}
}

func TestProductionPublicPackagesHavePackageDocumentation(t *testing.T) {
	for _, dir := range []string{"config", "provider", "runtime", "storage", "tools", "observation"} {
		fset := token.NewFileSet()
		packages, err := parser.ParseDir(fset, dir, func(info os.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse public package %s: %v", dir, err)
		}
		pkg := packages[dir]
		if pkg == nil {
			t.Fatalf("public package %s is missing", dir)
		}
		hasPackageComment := false
		for _, file := range pkg.Files {
			if file.Doc != nil && strings.HasPrefix(strings.TrimSpace(file.Doc.Text()), "Package "+dir) {
				hasPackageComment = true
				break
			}
		}
		if !hasPackageComment {
			t.Fatalf("public package %s requires a Package %s comment", dir, dir)
		}
	}
}

func TestFloretTestIsTheOnlyTestOnlyPublicPackage(t *testing.T) {
	imports := packageImports(t, "florettest", false, true)
	allowedFloretImports := map[string]bool{
		modulePath + "/config":      true,
		modulePath + "/provider":    true,
		modulePath + "/runtime":     true,
		modulePath + "/storage":     true,
		modulePath + "/tools":       true,
		modulePath + "/observation": true,
		modulePath + "/florettest":  true,
	}
	for imported := range imports {
		if strings.HasPrefix(imported, modulePath+"/") {
			if !allowedFloretImports[imported] {
				t.Fatalf("florettest imports non-public Floret package %s", imported)
			}
			continue
		}
		first := strings.SplitN(imported, "/", 2)[0]
		if strings.Contains(first, ".") {
			t.Fatalf("florettest imports third-party package %s", imported)
		}
	}
	for imported := range packageImports(t, ".", true, false) {
		if imported == modulePath+"/florettest" || strings.HasPrefix(imported, modulePath+"/florettest/") {
			t.Fatalf("Floret production code imports test-only package %s", imported)
		}
	}
}

func TestTopLevelPackageLayoutIsConstrained(t *testing.T) {
	allowed := map[string]bool{
		".github":     true,
		".githooks":   true,
		"cmd":         true,
		"config":      true,
		"florettest":  true,
		"internal":    true,
		"observation": true,
		"okf":         true,
		"provider":    true,
		"runtime":     true,
		"scripts":     true,
		"storage":     true,
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

func TestCanonicalExactReadDoesNotDelegateToPageReaders(t *testing.T) {
	for _, file := range []string{
		filepath.Join("internal", "sessiontree", "canonical_turn_read.go"),
		filepath.Join("internal", "storage", "sqlite", "canonical_turn_read.go"),
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		contents := string(data)
		for _, forbidden := range []string{"listCanonicalTurnsBackwardLocked", "listSQLiteCanonicalTurnsWithRunner"} {
			if strings.Contains(contents, forbidden) {
				t.Fatalf("%s exact reader must not call page reader %s", file, forbidden)
			}
		}
	}
}

func TestCommandPackagesRemainCommands(t *testing.T) {
	dirs := []string{filepath.Join("cmd", "floret-store")}
	examples, err := os.ReadDir(filepath.Join("cmd", "examples"))
	if err != nil {
		t.Fatal(err)
	}
	for _, example := range examples {
		if example.IsDir() {
			dirs = append(dirs, filepath.Join("cmd", "examples", example.Name()))
		}
	}
	for _, dir := range dirs {
		fset := token.NewFileSet()
		for _, file := range goFilesInDir(t, dir, false) {
			parsed, err := parser.ParseFile(fset, file, nil, parser.PackageClauseOnly)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Name.Name != "main" {
				t.Fatalf("%s must remain package main, got package %s", file, parsed.Name.Name)
			}
		}
	}
}

func TestExamplesUseOnlyPublicFloretPackages(t *testing.T) {
	allowed := map[string]bool{
		modulePath + "/config":      true,
		modulePath + "/provider":    true,
		modulePath + "/runtime":     true,
		modulePath + "/storage":     true,
		modulePath + "/tools":       true,
		modulePath + "/observation": true,
	}
	for imported := range packageImports(t, filepath.Join("cmd", "examples"), true, true) {
		if strings.HasPrefix(imported, modulePath+"/") && !allowed[imported] {
			t.Fatalf("examples must use only production public Floret packages; found %s", imported)
		}
	}
}

func TestFloretStoreCommandUsesOnlyPublicStorageContracts(t *testing.T) {
	imports := packageImports(t, filepath.Join("cmd", "floret-store"), false, true)
	if !imports[modulePath+"/runtime"] {
		t.Fatal("floret-store must delegate maintenance to the public runtime package")
	}
	allowed := map[string]bool{modulePath + "/runtime": true, modulePath + "/storage": true}
	for imported := range imports {
		if strings.HasPrefix(imported, modulePath+"/") && !allowed[imported] {
			t.Fatalf("floret-store must not import Floret package %s", imported)
		}
	}
}

func TestPublicPackagesDoNotExposeInternalContracts(t *testing.T) {
	for _, pkg := range []string{"./config", "./provider", "./runtime", "./storage", "./tools", "./observation"} {
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
			"session.",
			"sessiontree.",
			"skills.",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s public docs expose internal contract %q", pkg, forbidden)
			}
		}
	}
}

func TestPublicConfigDoesNotExposeExecutionStorageWiring(t *testing.T) {
	text := readTextFile(t, filepath.Join("config", "config.go"))
	for _, forbidden := range []string{"RunID", "PromptScopeID", "PromptCacheDir", "FLORET_RUN_ID", "FLORET_PROMPT_CACHE_DIR"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("config package exposes runtime/storage wiring %q", forbidden)
		}
	}
}

func TestRootPackageIsNotPublicAPI(t *testing.T) {
	if _, err := os.Stat("floret.go"); err == nil {
		t.Fatalf("root package must not expose public downstream API")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestModuleUsesV2SemanticImportPath(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(data), "module github.com/floegence/floret/v2\n") {
		t.Fatal("Floret v2 must use the /v2 semantic import path")
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

func TestReadmeOnlyDocumentsDownstreamIntegrationSurface(t *testing.T) {
	text := readTextFile(t, "README.md")
	for _, want := range []string{"runtime.ConfigureHostCapabilities", "runtime.NewTurnExecutionHostBinder", "runtime.TurnExecutionHost", "runtime.NewThreadCompactionHostBinder", "runtime.NewSubAgentHostBinder", "runtime.CompactThreadRequest", "runtime.ModelGateway", "runtime.NewMemoryStore", "runtime.OpenSQLiteStore", "tools.Registry", "observation"} {
		if !strings.Contains(text, want) {
			t.Fatalf("README downstream integration surface is missing API %q", want)
		}
	}
	for _, forbidden := range publicDocsDenylist() {
		if strings.Contains(text, forbidden) {
			t.Fatalf("README advertises internal/downstream-forbidden API %q", forbidden)
		}
	}
}

func TestCurrentCapabilityDocsDoNotAdvertiseRemovedFacade(t *testing.T) {
	for _, file := range []string{
		"README.md",
		filepath.Join("okf", "api", "runtime.md"),
		filepath.Join("okf", "architecture", "runtime-layers.md"),
		filepath.Join("okf", "architecture", "boundaries.md"),
		filepath.Join("okf", "architecture", "host-capability-authority.md"),
		filepath.Join("okf", "decisions", "public-api-boundary.md"),
	} {
		text := readTextFile(t, file)
		for _, forbidden := range []string{"`runtime.Host`", "runtime.NewHost(", "`HostOptions`", "`HostRuntime`", "ThreadCapabilityOptions", "ThreadMaintenanceHost", "NewThreadMaintenanceHost", "ThreadMaintenanceHostOptions"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s advertises removed capability facade %q", file, forbidden)
			}
		}
	}
}

func TestRuntimePublicAPIDoesNotExposeContextLifecycleBackdoors(t *testing.T) {
	text := readTextFile(t, filepath.Join("runtime", "projected_turn.go")) + "\n" + readTextFile(t, filepath.Join("runtime", "runtime.go"))
	for _, forbidden := range []string{
		"RunProjectedTurn",
		"ProjectedTurnOptions",
		"ProjectedTurnRequest",
		"ProjectedTurnResult",
		"TranscriptMessage",
		"ProjectedContextCompaction",
		"CompactProjectedContext",
		"ProjectedCompactionSummary",
		"ActiveTranscript",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("runtime public API exposes context lifecycle backdoor %q", forbidden)
		}
	}
}

func TestRuntimeThreadCreationContractIsExplicit(t *testing.T) {
	text := readTextFile(t, filepath.Join("runtime", "runtime.go")) + "\n" + readTextFile(t, filepath.Join("runtime", "thread_capabilities.go"))
	for _, want := range []string{"type CreateThreadRequest struct", ") CreateThread("} {
		if !strings.Contains(text, want) {
			t.Fatalf("runtime public API is missing explicit thread creation contract %q", want)
		}
	}
	for _, forbidden := range []string{
		"Ensure" + "ThreadRequest",
		") Ensure" + "Thread(",
		"type Start" + "ThreadRequest struct",
		") Start" + "Thread(",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("runtime public API retains ambiguous thread creation contract %q", forbidden)
		}
	}
}

func TestAgentHarnessProductionCannotAcquireLifecycleAuthority(t *testing.T) {
	forbiddenCalls := map[string]bool{
		"AcquireTurnLease":    true,
		"CreateThread":        true,
		"DeleteProviderState": true,
		"DeleteThread":        true,
		"Fork":                true,
		"MoveLeaf":            true,
		"PutProviderState":    true,
		"ReleaseTurnLease":    true,
		"UpdateThread":        true,
	}
	for _, path := range goFiles(t, filepath.Join("internal", "agentharness")) {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv != nil && (fn.Name.Name == "StartThread" || fn.Name.Name == "CreateThread") {
				t.Fatalf("production AgentHarness lifecycle creation method %s returned in %s", fn.Name.Name, path)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && forbiddenCalls[selector.Sel.Name] {
				t.Fatalf("production AgentHarness calls forbidden lifecycle primitive %s in %s", selector.Sel.Name, path)
			}
			return true
		})
	}
	harness := readTextFile(t, filepath.Join("internal", "agentharness", "harness.go"))
	if !strings.Contains(harness, "Repo                     sessiontree.JournalRepo") {
		t.Fatal("AgentHarness Options must retain only sessiontree.JournalRepo")
	}
}

func TestStorageAuthorityHasOnlyCanonicalRootTreeDelete(t *testing.T) {
	for _, path := range []string{
		filepath.Join("internal", "sessiontree", "sessiontree.go"),
		filepath.Join("internal", "storage", "storage.go"),
		filepath.Join("internal", "storage", "sqlite", "sqlitestore.go"),
	} {
		text := readTextFile(t, path)
		for _, forbidden := range []string{
			"DeleteThreadTreeData",
			"DeleteThreadTree(",
			"DeleteThread(context.Context, string) error",
			"func (r *MemoryRepo) DeleteThread(",
			"func (r *FileRepo) DeleteThread(",
			"func (r *FileRepo) DeleteRootTree(",
			"func (s *Store) DeleteThread(",
			"DeleteThreadArtifacts",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s retains non-canonical delete capability %q", path, forbidden)
			}
		}
	}
	authorityKernel := readTextFile(t, filepath.Join("internal", "storage", "sqlite", "authority_kernel.go"))
	if strings.Contains(authorityKernel, "DELETE FROM metadata_records") {
		t.Fatal("Floret root delete must not delete host-owned metadata records")
	}
}

func TestRuntimeCapabilityMethodSetsAreNarrow(t *testing.T) {
	methodNames := func(typ reflect.Type) map[string]struct{} {
		out := make(map[string]struct{}, typ.NumMethod())
		for i := 0; i < typ.NumMethod(); i++ {
			out[typ.Method(i).Name] = struct{}{}
		}
		return out
	}
	exact := func(name string, typ reflect.Type, want ...string) {
		t.Helper()
		got := methodNames(typ)
		wantSet := make(map[string]struct{}, len(want))
		for _, method := range want {
			wantSet[method] = struct{}{}
		}
		if !reflect.DeepEqual(got, wantSet) {
			t.Fatalf("%s exported method set = %#v, want %#v", name, got, wantSet)
		}
	}

	exact("Host", reflect.TypeOf((*floretRuntime.Host)(nil)),
		"Close", "InterruptedTurnRecovery", "PendingToolRecovery", "SubAgentManager", "SubAgentReader",
		"ThreadCompactor", "ThreadCreator", "ThreadDeleter", "ThreadForker", "ThreadInventory",
		"ThreadReader", "ThreadTitleEditor", "TurnRunner")
	exact("Agent", reflect.TypeOf((*floretRuntime.Agent)(nil)), "Config", "ProviderIdentity", "ToolDefinitions")
	exact("ThreadCreator", reflect.TypeOf((*floretRuntime.ThreadCreator)(nil)), "Create")
	exact("ThreadReader", reflect.TypeOf((*floretRuntime.ThreadReader)(nil)), "Read", "ReadTurn")
	exact("ThreadTitleEditor", reflect.TypeOf((*floretRuntime.ThreadTitleEditor)(nil)), "Set")
	exact("ThreadForker", reflect.TypeOf((*floretRuntime.ThreadForker)(nil)), "Fork")
	exact("ThreadDeleter", reflect.TypeOf((*floretRuntime.ThreadDeleter)(nil)), "Delete")
	exact("TurnRunner", reflect.TypeOf((*floretRuntime.TurnRunner)(nil)), "Run")
	exact("ThreadCompactor", reflect.TypeOf((*floretRuntime.ThreadCompactor)(nil)), "Compact")
	exact("SubAgentManager", reflect.TypeOf((*floretRuntime.SubAgentManager)(nil)),
		"Close", "PublishPendingToolCompletion", "SendInput", "Spawn", "Wait")
	exact("SubAgentReader", reflect.TypeOf((*floretRuntime.SubAgentReader)(nil)),
		"ActivityTimeline", "List", "ReadDetail")
	exact("PendingToolRecovery", reflect.TypeOf((*floretRuntime.PendingToolRecovery)(nil)), "Settle")
	exact("InterruptedTurnRecovery", reflect.TypeOf((*floretRuntime.InterruptedTurnRecovery)(nil)), "Recover")
	exact("ThreadInventory", reflect.TypeOf((*floretRuntime.ThreadInventory)(nil)), "List")

	for name, typ := range map[string]reflect.Type{
		"Host":                    reflect.TypeOf(floretRuntime.Host{}),
		"Agent":                   reflect.TypeOf(floretRuntime.Agent{}),
		"ThreadCreator":           reflect.TypeOf(floretRuntime.ThreadCreator{}),
		"ThreadReader":            reflect.TypeOf(floretRuntime.ThreadReader{}),
		"ThreadTitleEditor":       reflect.TypeOf(floretRuntime.ThreadTitleEditor{}),
		"ThreadForker":            reflect.TypeOf(floretRuntime.ThreadForker{}),
		"ThreadDeleter":           reflect.TypeOf(floretRuntime.ThreadDeleter{}),
		"TurnRunner":              reflect.TypeOf(floretRuntime.TurnRunner{}),
		"ThreadCompactor":         reflect.TypeOf(floretRuntime.ThreadCompactor{}),
		"SubAgentManager":         reflect.TypeOf(floretRuntime.SubAgentManager{}),
		"SubAgentReader":          reflect.TypeOf(floretRuntime.SubAgentReader{}),
		"PendingToolRecovery":     reflect.TypeOf(floretRuntime.PendingToolRecovery{}),
		"InterruptedTurnRecovery": reflect.TypeOf(floretRuntime.InterruptedTurnRecovery{}),
		"ThreadInventory":         reflect.TypeOf(floretRuntime.ThreadInventory{}),
	} {
		for index := 0; index < typ.NumField(); index++ {
			if typ.Field(index).PkgPath == "" {
				t.Fatalf("%s exposes exported field %q", name, typ.Field(index).Name)
			}
		}
	}

	exactFields := func(name string, typ reflect.Type, want ...string) {
		t.Helper()
		got := make([]string, 0, typ.NumField())
		for index := 0; index < typ.NumField(); index++ {
			got = append(got, typ.Field(index).Name)
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s fields = %#v, want %#v", name, got, want)
		}
	}
	exactFields("ArtifactRef", reflect.TypeOf(floretRuntime.ArtifactRef{}),
		"ID", "Kind", "MIME", "SHA256", "SafeLabel", "SizeBytes")
	exactFields("ReadArtifactRequest", reflect.TypeOf(floretRuntime.ReadArtifactRequest{}),
		"ArtifactID", "ThreadID")
	exactFields("ArtifactContent", reflect.TypeOf(floretRuntime.ArtifactContent{}), "Ref", "Text")
}

func TestRuntimePrivateProviderHostDoesNotCrossPublicTypes(t *testing.T) {
	for _, path := range walkAllFiles(t, "runtime") {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			group, ok := declaration.(*ast.GenDecl)
			if !ok || group.Tok != token.TYPE {
				continue
			}
			for _, specification := range group.Specs {
				typeSpec := specification.(*ast.TypeSpec)
				if !ast.IsExported(typeSpec.Name.Name) {
					continue
				}
				usesProviderHost := false
				ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
					identifier, ok := node.(*ast.Ident)
					if ok && identifier.Name == "providerHost" {
						usesProviderHost = true
					}
					return true
				})
				if usesProviderHost {
					t.Fatalf("exported runtime type %s wraps private providerHost", typeSpec.Name.Name)
				}
			}
		}
	}
}

func TestRuntimeStoreAndBootstrapStayPrivate(t *testing.T) {
	for _, path := range walkAllFiles(t, "runtime") {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || !ast.IsExported(function.Name.Name) {
				continue
			}
			leaksAuthority := false
			ast.Inspect(function.Type, func(node ast.Node) bool {
				identifier, ok := node.(*ast.Ident)
				if ok && (identifier.Name == "Store" || identifier.Name == "hostBootstrap") {
					leaksAuthority = true
				}
				return true
			})
			if leaksAuthority {
				t.Fatalf("runtime authority leaks through public function %s in %s", function.Name.Name, path)
			}
		}
	}
}

func TestBoundHandleRequestsDoNotRepeatBoundIdentity(t *testing.T) {
	for name, contract := range map[string]struct {
		typ       reflect.Type
		forbidden []string
	}{
		"TurnRequest":                          {reflect.TypeOf(floretRuntime.TurnRequest{}), []string{"ThreadID"}},
		"ThreadForkRequest":                    {reflect.TypeOf(floretRuntime.ThreadForkRequest{}), []string{"SourceThreadID"}},
		"ThreadCompactionRequest":              {reflect.TypeOf(floretRuntime.ThreadCompactionRequest{}), []string{"ThreadID"}},
		"SpawnSubAgent":                        {reflect.TypeOf(floretRuntime.SpawnSubAgent{}), []string{"ParentThreadID"}},
		"SendSubAgentInput":                    {reflect.TypeOf(floretRuntime.SendSubAgentInput{}), []string{"ParentThreadID"}},
		"PublishSubAgentPendingToolCompletion": {reflect.TypeOf(floretRuntime.PublishSubAgentPendingToolCompletion{}), []string{"ParentThreadID"}},
		"WaitSubAgents":                        {reflect.TypeOf(floretRuntime.WaitSubAgents{}), []string{"ParentThreadID"}},
		"CloseSubAgent":                        {reflect.TypeOf(floretRuntime.CloseSubAgent{}), []string{"ParentThreadID"}},
		"SubAgentDetailRequest":                {reflect.TypeOf(floretRuntime.SubAgentDetailRequest{}), []string{"ParentThreadID"}},
		"PendingToolRecoveryRequest":           {reflect.TypeOf(floretRuntime.PendingToolRecoveryRequest{}), []string{"ThreadID", "ParentThreadID", "Target", "TurnID", "RunID", "ToolCallID"}},
	} {
		for _, field := range contract.forbidden {
			if _, ok := contract.typ.FieldByName(field); ok {
				t.Fatalf("%s repeats bound identity field %s", name, field)
			}
		}
	}
}

func TestV2PublicAPISurfaceMatchesBaseline(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "./internal/architecture/apibaseline", "-root", root)
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	actual, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generate public API baseline: %v\n%s", err, actual)
	}
	expected, err := os.ReadFile(filepath.Join(root, "internal", "architecture", "testdata", "v2-public-api.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(expected) {
		t.Fatalf("public API differs from the v2 baseline; update the baseline only with API decision docs and compatibility review\n%s", firstSurfaceDifference(string(expected), string(actual)))
	}
}

func TestV2RemovesLegacyHostCapabilityGraph(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "./internal/architecture/apibaseline", "-root", root)
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generate public API surface: %v\n%s", err, output)
	}
	for _, forbidden := range []string{
		"HostBootstrap",
		"Binder",
		"Factory",
		"TurnExecutionHostOptions",
		"ThreadCompactionHostOptions",
		"SubAgentHostOptions",
		"NewMemoryStore",
		"OpenSQLiteStore",
		"ConfigureHostCapabilities",
	} {
		if strings.Contains(string(output), forbidden) {
			t.Errorf("v2 public API retains legacy contract %q", forbidden)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "cmd", "floret-host-init")); err == nil {
		t.Error("v2 retains the floret-host-init generator")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func firstSurfaceDifference(expected, actual string) string {
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")
	count := min(len(expectedLines), len(actualLines))
	for index := 0; index < count; index++ {
		if expectedLines[index] != actualLines[index] {
			return fmt.Sprintf("line %d\n- %s\n+ %s", index+1, expectedLines[index], actualLines[index])
		}
	}
	return fmt.Sprintf("line count changed from %d to %d", len(expectedLines), len(actualLines))
}

func TestRuntimeCapabilityRootsAndAggregatesStayExplicit(t *testing.T) {
	capabilityTypes := map[string]bool{
		"Host": true, "Agent": true, "ThreadCreator": true, "ThreadReader": true,
		"ThreadTitleEditor": true, "ThreadForker": true, "ThreadDeleter": true,
		"TurnRunner": true, "ThreadCompactor": true, "SubAgentManager": true,
		"SubAgentReader": true, "PendingToolRecovery": true,
		"InterruptedTurnRecovery": true, "ThreadInventory": true,
	}
	allowedConstructors := map[string]bool{"Open": true, "NewAgent": true}
	foundConstructors := map[string]bool{}

	for _, path := range walkAllFiles(t, "runtime") {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if typed.Recv != nil || !ast.IsExported(typed.Name.Name) || typed.Type.Results == nil {
					continue
				}
				returnsCapability := false
				ast.Inspect(typed.Type.Results, func(node ast.Node) bool {
					identifier, ok := node.(*ast.Ident)
					if ok && capabilityTypes[identifier.Name] {
						returnsCapability = true
					}
					return true
				})
				if !returnsCapability {
					continue
				}
				if !allowedConstructors[typed.Name.Name] {
					t.Fatalf("runtime exposes unreviewed authority constructor %s", typed.Name.Name)
				}
				foundConstructors[typed.Name.Name] = true
			case *ast.GenDecl:
				if typed.Tok != token.TYPE {
					continue
				}
				for _, specification := range typed.Specs {
					typeSpec := specification.(*ast.TypeSpec)
					if !ast.IsExported(typeSpec.Name.Name) {
						continue
					}
					if typeSpec.Assign.IsValid() {
						aliasesCapability := false
						ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
							identifier, ok := node.(*ast.Ident)
							if ok && capabilityTypes[identifier.Name] {
								aliasesCapability = true
							}
							return true
						})
						if aliasesCapability {
							t.Fatalf("runtime exported alias %s re-exports authority", typeSpec.Name.Name)
						}
					}
					shape, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range shape.Fields.List {
						if len(field.Names) == 0 || !ast.IsExported(field.Names[0].Name) {
							continue
						}
						containsCapability := false
						ast.Inspect(field.Type, func(node ast.Node) bool {
							identifier, ok := node.(*ast.Ident)
							if ok && capabilityTypes[identifier.Name] {
								containsCapability = true
							}
							return true
						})
						if containsCapability {
							t.Fatalf("runtime exported struct %s aggregates authority field %s", typeSpec.Name.Name, field.Names[0].Name)
						}
					}
				}
			}
		}
	}
	if !reflect.DeepEqual(foundConstructors, allowedConstructors) {
		t.Fatalf("authority constructor set = %#v, want %#v", foundConstructors, allowedConstructors)
	}
}

func TestRuntimePublicAPIDoesNotExposeForkIdentityMapsOrDuplicateSubAgentPages(t *testing.T) {
	var source strings.Builder
	for _, file := range walkAllFiles(t, "runtime") {
		if filepath.Ext(file) != ".go" || strings.HasSuffix(file, "_test.go") {
			continue
		}
		source.WriteString(readTextFile(t, file))
		source.WriteByte('\n')
	}
	for _, forbidden := range []string{
		"func NewHost(",
		"type HostOptions struct",
		"ForkedTurnRef",
		"ListSubAgentDetailEvents",
		"ListSubAgentDetailEventsRequest",
		"SubAgentDetailEvents",
	} {
		if strings.Contains(source.String(), forbidden) {
			t.Fatalf("runtime public API retains duplicate authority contract %q", forbidden)
		}
	}
}

func TestObservationPublicAPIDoesNotExposeCompactionInternals(t *testing.T) {
	text := readTextFile(t, filepath.Join("observation", "context.go")) + "\n" +
		readTextFile(t, filepath.Join("observation", "compaction.go")) + "\n" +
		readTextFile(t, filepath.Join("observation", "compaction_debug.go"))
	for _, forbidden := range []string{
		"CompactionID",
		"CompactionGeneration",
		"CompactionWindowID",
		"CompactedThroughEntryID",
		"FirstKeptEntryID",
		"KeptUserEntryIDs",
		"SummarySchemaVersion",
		"ActiveTranscript",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("observation public API exposes compaction internal %q", forbidden)
		}
	}
}

func TestRuntimeThreadDetailCompactionExposesOnlySanitizedLifecycle(t *testing.T) {
	typ := reflect.TypeOf(floretRuntime.ThreadDetailCompaction{})
	want := []string{
		"OperationID", "RequestID", "Source", "Trigger", "Reason", "Phase",
		"TokensBefore", "TokensAfterEstimate", "Metadata",
	}
	got := make([]string, 0, typ.NumField())
	for index := 0; index < typ.NumField(); index++ {
		got = append(got, typ.Field(index).Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime ThreadDetailCompaction fields=%v, want %v", got, want)
	}
	source := readTextFile(t, filepath.Join("runtime", "runtime.go"))
	if !strings.Contains(source, "out[index].Metadata = safeStringMetadata(out[index].Metadata)") {
		t.Fatal("runtime compaction detail must sanitize top-level metadata at the public projection boundary")
	}
}

func TestRuntimeTurnReadModelsKeepJournalNavigationOpaque(t *testing.T) {
	retryType := reflect.TypeOf(floretRuntime.ThreadTurnRetrySource{})
	if retryType.NumField() != 1 || retryType.Field(0).Name != "TurnID" {
		t.Fatalf("runtime retry source fields are not turn-only: %v", retryType)
	}
	cursorType := reflect.TypeOf(floretRuntime.ThreadTurnCursor(""))
	requestType := reflect.TypeOf(floretRuntime.ListThreadTurnsRequest{})
	pageType := reflect.TypeOf(floretRuntime.ThreadTurnsPage{})
	turnType := reflect.TypeOf(floretRuntime.ThreadTurnSnapshot{})
	originType := reflect.TypeOf(floretRuntime.ThreadUserMessageOrigin(""))
	originField, ok := turnType.FieldByName("UserMessageOrigin")
	if !ok || originField.Type != originType || originField.Tag.Get("json") != "user_message_origin,omitempty" {
		t.Fatalf("runtime ThreadTurnSnapshot.UserMessageOrigin=%v found=%v tag=%q, want typed optional origin", originField.Type, ok, originField.Tag.Get("json"))
	}
	for _, origin := range []floretRuntime.ThreadUserMessageOrigin{
		floretRuntime.ThreadUserMessageOriginUser,
		floretRuntime.ThreadUserMessageOriginDelegatedMission,
		floretRuntime.ThreadUserMessageOriginSubAgentInput,
		floretRuntime.ThreadUserMessageOriginPendingToolCompletion,
	} {
		if strings.TrimSpace(string(origin)) == "" {
			t.Fatal("runtime user message origin vocabulary contains an empty value")
		}
	}
	for _, field := range []struct {
		typ  reflect.Type
		name string
		want reflect.Type
	}{
		{requestType, "BeforeCursor", reflect.PointerTo(cursorType)},
		{requestType, "SinceCursor", reflect.PointerTo(cursorType)},
		{pageType, "BeforeCursor", reflect.PointerTo(cursorType)},
		{pageType, "SinceCursor", cursorType},
	} {
		got, ok := field.typ.FieldByName(field.name)
		if !ok || got.Type != field.want {
			t.Fatalf("%s.%s type=%v found=%v, want %v", field.typ.Name(), field.name, got.Type, ok, field.want)
		}
	}
	source := readTextFile(t, filepath.Join("runtime", "thread_turns.go"))
	for _, forbidden := range []string{"ThreadTurnsBeforeCursor", "ThreadTurnsSinceCursor"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("runtime turn reads expose removed cursor contract %q", forbidden)
		}
	}
}

func TestReadmeKeepsPolishedPresentation(t *testing.T) {
	text := readTextFile(t, "README.md")
	for _, want := range []string{
		"pkg.go.dev/badge/github.com/floegence/floret/v2/runtime.svg",
		"img.shields.io/badge/license-MIT",
		`<a href="#-why-floret">Why Floret</a>`,
		"## \U00002728 Why Floret",
		"## \U0001F9ED At a glance",
		"## \U0001F4E6 Downstream integration surface",
		"| You need to... | Use... |",
		"| Tool concern | Floret handles | Host handles |",
		"Host UI/API",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("README lost polished presentation marker %q", want)
		}
	}
}

func TestDocumentationDoesNotTeachForbiddenDownstreamImports(t *testing.T) {
	for _, file := range textFiles(t, ".") {
		if isArchitectureTest(file) {
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

func TestOKFProjectKnowledgeBundleConforms(t *testing.T) {
	root := "okf"
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("OKF bundle missing: %s", root)
	}

	requiredFiles := []string{
		"index.md",
		"log.md",
		"project.md",
		filepath.Join("architecture", "index.md"),
		filepath.Join("architecture", "boundaries.md"),
		filepath.Join("architecture", "identities.md"),
		filepath.Join("architecture", "runtime-layers.md"),
		filepath.Join("architecture", "tools-permissions.md"),
		filepath.Join("architecture", "observation-events.md"),
		filepath.Join("api", "index.md"),
		filepath.Join("api", "config.md"),
		filepath.Join("api", "runtime.md"),
		filepath.Join("api", "tools.md"),
		filepath.Join("api", "observation.md"),
		filepath.Join("workflows", "index.md"),
		filepath.Join("workflows", "change-public-api.md"),
		filepath.Join("workflows", "add-tool.md"),
		filepath.Join("workflows", "add-provider.md"),
		filepath.Join("workflows", "quality-gate.md"),
		filepath.Join("decisions", "index.md"),
		filepath.Join("decisions", "public-api-boundary.md"),
		filepath.Join("decisions", "prompt-scope-identity.md"),
		filepath.Join("decisions", "no-host-product-concerns.md"),
	}
	for _, rel := range requiredFiles {
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			t.Fatalf("OKF required file missing %s: %v", rel, err)
		}
	}

	rootIndex := filepath.Join(root, "index.md")
	rootMeta, rootBody, _ := parseOKFFrontmatter(t, rootIndex, true)
	if got := rootMeta["okf_version"]; got != "0.1" {
		t.Fatalf("%s okf_version = %q, want 0.1", rootIndex, got)
	}
	if strings.TrimSpace(rootBody) == "" {
		t.Fatalf("%s must contain an index body", rootIndex)
	}

	concepts := 0
	logs := 0
	for _, file := range walkAllFiles(t, root) {
		if filepath.Ext(file) != ".md" {
			continue
		}
		text := readTextFile(t, file)
		for _, forbidden := range forbiddenDownstreamImportPaths() {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s references forbidden downstream import %s", file, forbidden)
			}
		}

		switch filepath.Base(file) {
		case "index.md":
			if filepath.Clean(file) == filepath.Clean(rootIndex) {
				continue
			}
			if hasOKFFrontmatter(text) {
				t.Fatalf("%s must not contain frontmatter", file)
			}
		case "log.md":
			logs++
			if hasOKFFrontmatter(text) {
				t.Fatalf("%s must not contain frontmatter", file)
			}
			assertOKFLogDates(t, file, text)
		default:
			meta, body, _ := parseOKFFrontmatter(t, file, true)
			if strings.TrimSpace(meta["type"]) == "" {
				t.Fatalf("%s missing required OKF type", file)
			}
			if _, ok := meta["okf_version"]; ok {
				t.Fatalf("%s must not declare okf_version", file)
			}
			if strings.TrimSpace(body) == "" {
				t.Fatalf("%s must contain a body", file)
			}
			concepts++
		}
	}
	if concepts == 0 {
		t.Fatalf("OKF bundle must include concept documents")
	}
	if logs == 0 {
		t.Fatalf("OKF bundle must include log.md")
	}
}

func TestAGENTSDocumentsOKFMaintenanceRules(t *testing.T) {
	text := readTextFile(t, "AGENTS.md")
	for _, want := range []string{
		"## OKF Project Knowledge Bundle",
		"`okf/` is this repository's OKF v0.1 project knowledge bundle",
		"`okf/` is documentation only",
		"Every non-reserved `.md` file under `okf/`",
		"Only the root `okf/index.md` may declare `okf_version: \"0.1\"`",
		"must not teach downstream applications to import or depend on `internal/*`",
		"OKF conformance tests",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("AGENTS.md missing OKF rule %q", want)
		}
	}
}

func TestProviderSDKImportsStayInInternalAdapters(t *testing.T) {
	for _, file := range goFiles(t, ".") {
		if isArchitectureTest(file) {
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

func TestSQLiteDriverImportsStayInOfficialStorage(t *testing.T) {
	for _, file := range goFiles(t, ".") {
		if isArchitectureTest(file) {
			continue
		}
		if strings.HasPrefix(file, filepath.Join("internal", "storage", "sqlite")+string(filepath.Separator)) {
			continue
		}
		if file == filepath.Join("storage", "sqlite.go") {
			continue
		}
		text := readTextFile(t, file)
		for _, marker := range []string{
			"github.com/mattn/go-sqlite3",
			"modernc.org/sqlite",
		} {
			if strings.Contains(text, marker) {
				t.Fatalf("sqlite driver import %q outside official storage implementations: %s", marker, file)
			}
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
		if isArchitectureTest(file) {
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

	sqliteText := strings.Join([]string{
		readTextFile(t, filepath.Join("internal", "storage", "sqlite", "schema.go")),
		readTextFile(t, filepath.Join("internal", "storage", "sqlite", "sqlitestore.go")),
		readTextFile(t, filepath.Join("internal", "storage", "sqlite", "authority_kernel.go")),
	}, "\n")
	for _, want := range []string{"prompt_scope_id TEXT NOT NULL", "DeletePromptScopes", "DeleteRootTree"} {
		if !strings.Contains(sqliteText, want) {
			t.Fatalf("sqlite storage contract missing %q", want)
		}
	}
	for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
		if strings.Contains(sqlTableDDL(t, sqliteText, table), "run_id") {
			t.Fatalf("prompt cache table %s still stores run-owned cache identity", table)
		}
	}
	if strings.Contains(sqliteText, "DeleteRuns") || strings.Contains(sqliteText, "DeleteSession") {
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
}

func TestNoBuiltInWebFetchBoundaryIsEnforced(t *testing.T) {
	for _, path := range append(walkAllFiles(t, "runtime"), walkAllFiles(t, filepath.Join("internal", "tools", "builtin"))...) {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		text := readTextFile(t, path)
		if strings.Contains(text, "web_fetch") || strings.Contains(text, "ToolWebFetch") || strings.Contains(text, "RegisterNetwork") {
			t.Fatalf("%s must not expose built-in web_fetch", path)
		}
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

func TestToolRegistryExposesOnlyAuthorityGatedDispatch(t *testing.T) {
	registryType := reflect.TypeOf((*floretTools.Registry)(nil))
	got := make([]string, 0, registryType.NumMethod())
	for index := 0; index < registryType.NumMethod(); index++ {
		got = append(got, registryType.Method(index).Name)
	}
	want := []string{"ActivityForCall", "Definition", "Definitions", "Dispatch", "DispatchBatch", "ExposedDefinitions", "OutputPolicyFor", "Register", "Seal"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tools.Registry exported method set = %#v, want %#v", got, want)
	}
	text := readTextFile(t, filepath.Join("tools", "tools.go"))
	for _, want := range []string{
		"if opts.EffectDispatcher == nil",
		"ErrEffectDispatcherRequired",
		"result = dispatcher(ctx, p.request, p.invoke)",
		"result.effectFinalizationRequired = true",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("tools.Registry dispatch boundary is missing %q", want)
		}
	}
}

func TestToolsPermissionSourceDoesNotRestoreApprovalCallbackContract(t *testing.T) {
	text := readTextFile(t, filepath.Join("tools", "permission.go"))
	for _, removed := range []string{
		"type Approval" + "Request struct",
		"type Permission" + "Decision struct",
		"type Permission" + "DecisionState string",
		"type Appro" + "ver func",
		"Permission" + "DecisionAllow",
		"Permission" + "DecisionDeny",
	} {
		if strings.Contains(text, removed) {
			t.Fatalf("tools permission source restored orphan approval contract %q", removed)
		}
	}
	for _, retained := range []string{"type PermissionSpec struct", "type PermissionResolver func", "type ResourceRef struct"} {
		if !strings.Contains(text, retained) {
			t.Fatalf("tools permission source dropped retained contract %q", retained)
		}
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

func TestNoLocalModuleReplacementWiring(t *testing.T) {
	for _, file := range walkAllFiles(t, ".") {
		if filepath.Base(file) != "go.mod" {
			continue
		}
		inReplaceBlock := false
		for lineNumber, rawLine := range strings.Split(readTextFile(t, file), "\n") {
			line := strings.TrimSpace(strings.SplitN(rawLine, "//", 2)[0])
			switch {
			case line == "replace (":
				inReplaceBlock = true
				continue
			case inReplaceBlock && line == ")":
				inReplaceBlock = false
				continue
			case strings.HasPrefix(line, "replace "):
				line = strings.TrimSpace(strings.TrimPrefix(line, "replace "))
			case !inReplaceBlock:
				continue
			}

			arrow := strings.Index(line, "=>")
			if arrow < 0 {
				continue
			}
			target := strings.Fields(line[arrow+len("=>"):])
			if len(target) > 0 && isLocalModulePath(target[0]) {
				t.Fatalf("%s:%d must not wire a local module replacement %q", file, lineNumber+1, target[0])
			}
		}
	}
}

func TestNoRedevenDependencyWiring(t *testing.T) {
	for imported := range packageImports(t, ".", true, true) {
		if imported == redevenModulePath || strings.HasPrefix(imported, redevenModulePath+"/") {
			t.Fatalf("Floret must not import downstream Redeven package %s", imported)
		}
	}
	for _, file := range walkAllFiles(t, ".") {
		if filepath.Base(file) == "go.mod" && strings.Contains(readTextFile(t, file), redevenModulePath) {
			t.Fatalf("%s must not declare downstream Redeven module dependency", file)
		}
	}
}

func isLocalModulePath(path string) bool {
	return path == "." ||
		path == ".." ||
		strings.HasPrefix(path, "./") ||
		strings.HasPrefix(path, "../") ||
		strings.HasPrefix(path, `.\`) ||
		strings.HasPrefix(path, `..\`) ||
		strings.HasPrefix(path, "/") ||
		strings.HasPrefix(path, `\\`) ||
		regexp.MustCompile(`^[A-Za-z]:[\\/]`).MatchString(path)
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
		case ".md", ".sh", ".js":
			return false
		default:
			return true
		}
	})
}

func sqlTableDDL(t *testing.T, text, table string) string {
	t.Helper()
	startMarker := "CREATE TABLE " + table + " ("
	start := strings.Index(text, startMarker)
	if start < 0 {
		t.Fatalf("sqlite schema missing table %s", table)
	}
	rest := text[start:]
	end := strings.Index(rest, ");")
	if end < 0 {
		t.Fatalf("sqlite schema table %s is not closed", table)
	}
	return rest[:end]
}

func isArchitectureTest(file string) bool {
	return filepath.Clean(file) == filepath.Join("internal", "architecture", "architecture_test.go")
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

func hasOKFFrontmatter(text string) bool {
	trimmed := strings.TrimLeft(text, "\ufeff\r\n\t ")
	return strings.HasPrefix(trimmed, "---\n") || strings.HasPrefix(trimmed, "---\r\n")
}

func parseOKFFrontmatter(t *testing.T, file string, required bool) (map[string]string, string, bool) {
	t.Helper()
	text := strings.TrimLeft(readTextFile(t, file), "\ufeff\r\n\t ")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	if !strings.HasPrefix(text, "---\n") {
		if required {
			t.Fatalf("%s missing OKF frontmatter", file)
		}
		return nil, text, false
	}
	lines := strings.Split(text, "\n")
	meta := map[string]string{}
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "---" {
			return meta, strings.Join(lines[i+1:], "\n"), true
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatalf("%s has invalid OKF frontmatter line %q", file, line)
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "" {
			t.Fatalf("%s has empty OKF frontmatter key", file)
		}
		meta[key] = value
	}
	t.Fatalf("%s has unterminated OKF frontmatter", file)
	return nil, "", false
}

func assertOKFLogDates(t *testing.T, file, text string) {
	t.Helper()
	dateHeading := regexp.MustCompile(`^## [0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	seen := false
	for _, line := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "## ") {
			continue
		}
		seen = true
		if !dateHeading.MatchString(line) {
			t.Fatalf("%s has non-ISO OKF log date heading %q", file, line)
		}
	}
	if !seen {
		t.Fatalf("%s must include at least one OKF log date heading", file)
	}
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
		modulePath + "/internal/agentharness",
		modulePath + "/internal/engine",
		modulePath + "/internal/event",
		modulePath + "/internal/provider",
		modulePath + "/internal/session",
		modulePath + "/internal/sessiontree",
		modulePath + "/internal/storage",
		modulePath + "/internal/testing",
		modulePath + "/internal/tools/builtin",
		modulePath + "/internal/tools/mcp",
		modulePath + "/internal/tools/skills",
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
		"RunProjectedTurn",
		"ProjectedTurnOptions",
		"ProjectedTurnRequest",
		"TranscriptMessage",
		"ProjectedContextCompaction",
		"CompactProjectedContext",
		"ProjectedCompactionSummary",
		"ActiveTranscript",
	}
}
