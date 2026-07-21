package floret_test

import (
	"context"
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

	floretRuntime "github.com/floegence/floret/runtime"
	floretTools "github.com/floegence/floret/tools"
)

const modulePath = "github.com/floegence/floret"

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
		"okf":         true,
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

func TestCommandPackagesRemainCommands(t *testing.T) {
	for _, dir := range []string{filepath.Join("cmd", "floret-test-ui")} {
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

func TestPublicPackagesDoNotExposeInternalContracts(t *testing.T) {
	for _, pkg := range []string{"./config", "./runtime", "./tools", "./observation"} {
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
	for _, want := range []string{"runtime.ConfigureHostCapabilities", "runtime.NewTurnExecutionHostBinder", "runtime.TurnExecutionHost", "runtime.NewThreadCompactionHostBinder", "runtime.NewSubAgentHostBinder", "runtime.CompactThreadRequest", "runtime.ModelGateway", "runtime.NewMemoryStore", "runtime.OpenSQLiteStore", "tools.Registry", "observation"} {
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

func TestStorageAuthorityHasNoAlternateTreeDeleteOrHostMetadataCleanup(t *testing.T) {
	for _, path := range []string{
		filepath.Join("internal", "sessiontree", "sessiontree.go"),
		filepath.Join("internal", "storage", "storage.go"),
		filepath.Join("internal", "storage", "sqlite", "sqlitestore.go"),
	} {
		text := readTextFile(t, path)
		for _, forbidden := range []string{"DeleteThreadTreeData", "DeleteThreadTree("} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s retains alternate tree delete capability %q", path, forbidden)
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
	exact("HostBootstrap", reflect.TypeOf((*floretRuntime.HostBootstrap)(nil)))
	exact("ThreadCreateHostBinder", reflect.TypeOf((*floretRuntime.ThreadCreateHostBinder)(nil)), "Bind")
	exact("ThreadReadHostBinder", reflect.TypeOf((*floretRuntime.ThreadReadHostBinder)(nil)), "NewHost")
	exact("ThreadTitleHostBinder", reflect.TypeOf((*floretRuntime.ThreadTitleHostBinder)(nil)), "NewHost")
	exact("ThreadForkHostBinder", reflect.TypeOf((*floretRuntime.ThreadForkHostBinder)(nil)), "NewHost")
	exact("ThreadDeleteHostBinder", reflect.TypeOf((*floretRuntime.ThreadDeleteHostBinder)(nil)), "NewHost")
	exact("TurnExecutionHostBinder", reflect.TypeOf((*floretRuntime.TurnExecutionHostBinder)(nil)), "Bind")
	exact("ThreadCompactionHostBinder", reflect.TypeOf((*floretRuntime.ThreadCompactionHostBinder)(nil)), "Bind")
	exact("SubAgentHostBinder", reflect.TypeOf((*floretRuntime.SubAgentHostBinder)(nil)), "Bind")
	exact("SubAgentReadHostBinder", reflect.TypeOf((*floretRuntime.SubAgentReadHostBinder)(nil)), "NewHost")
	exact("PendingToolRecoveryHostBinder", reflect.TypeOf((*floretRuntime.PendingToolRecoveryHostBinder)(nil)), "NewSubAgentHost", "NewThreadHost")
	exact("InterruptedTurnRecoveryHostBinder", reflect.TypeOf((*floretRuntime.InterruptedTurnRecoveryHostBinder)(nil)), "BindSubAgent", "BindThread")
	exact("TurnExecutionHostFactory", reflect.TypeOf((*floretRuntime.TurnExecutionHostFactory)(nil)), "NewHost")
	exact("ThreadCompactionHostFactory", reflect.TypeOf((*floretRuntime.ThreadCompactionHostFactory)(nil)), "NewHost")
	exact("SubAgentHostFactory", reflect.TypeOf((*floretRuntime.SubAgentHostFactory)(nil)), "NewHost")
	exact("InterruptedTurnRecoveryHostFactory", reflect.TypeOf((*floretRuntime.InterruptedTurnRecoveryHostFactory)(nil)), "NewHost")
	exact("ThreadCreateHost", reflect.TypeOf((*floretRuntime.ThreadCreateHost)(nil)), "CreateThread")
	exact("ThreadTitleHost", reflect.TypeOf((*floretRuntime.ThreadTitleHost)(nil)), "SetThreadTitle")
	exact("ThreadForkHost", reflect.TypeOf((*floretRuntime.ThreadForkHost)(nil)), "ForkThread")
	exact("ThreadDeleteHost", reflect.TypeOf((*floretRuntime.ThreadDeleteHost)(nil)), "DeleteThread")
	exact("SubAgentReadHost", reflect.TypeOf((*floretRuntime.SubAgentReadHost)(nil)),
		"ListSubAgentActivityTimeline", "ListSubAgents", "ReadArtifact", "ReadSubAgentDetail")
	exact("PendingToolRecoveryHost", reflect.TypeOf((*floretRuntime.PendingToolRecoveryHost)(nil)), "SettlePendingTool")
	exact("InterruptedTurnRecoveryHost", reflect.TypeOf((*floretRuntime.InterruptedTurnRecoveryHost)(nil)), "RecoverInterruptedTurn")
	exact("TurnExecutionHost", reflect.TypeOf((*floretRuntime.TurnExecutionHost)(nil)),
		"CompletePendingTool", "ReadApprovalQueue", "ResolveApproval", "RetryTurn", "RunTurn", "SettlePendingTool", "UpdateThreadAgentTodos")
	exact("ThreadCompactionHost", reflect.TypeOf((*floretRuntime.ThreadCompactionHost)(nil)), "CompactThread")
	exact("SubAgentHost", reflect.TypeOf((*floretRuntime.SubAgentHost)(nil)),
		"CloseSubAgent", "PublishPendingToolCompletion", "SendSubAgentInput", "SettlePendingTool", "SpawnSubAgent", "WaitSubAgents")
	exact("ThreadReadHost", reflect.TypeOf((*floretRuntime.ThreadReadHost)(nil)),
		"ListThreadDetailEvents", "ListThreadTurns", "ReadLatestThreadTurn", "ReadThread",
		"ReadApprovalQueue", "ReadArtifact", "ReadThreadAgentTodos", "ReadThreadContext", "ReadThreadOverview", "ReadTurnProjection")
	exact("Store", reflect.TypeOf((*floretRuntime.Store)(nil)), "Close")
	for name, typ := range map[string]reflect.Type{
		"HostBootstrap":                      reflect.TypeOf(floretRuntime.HostBootstrap{}),
		"ThreadCreateHostBinder":             reflect.TypeOf(floretRuntime.ThreadCreateHostBinder{}),
		"ThreadReadHostBinder":               reflect.TypeOf(floretRuntime.ThreadReadHostBinder{}),
		"ThreadTitleHostBinder":              reflect.TypeOf(floretRuntime.ThreadTitleHostBinder{}),
		"ThreadForkHostBinder":               reflect.TypeOf(floretRuntime.ThreadForkHostBinder{}),
		"ThreadDeleteHostBinder":             reflect.TypeOf(floretRuntime.ThreadDeleteHostBinder{}),
		"TurnExecutionHostBinder":            reflect.TypeOf(floretRuntime.TurnExecutionHostBinder{}),
		"ThreadCompactionHostBinder":         reflect.TypeOf(floretRuntime.ThreadCompactionHostBinder{}),
		"SubAgentHostBinder":                 reflect.TypeOf(floretRuntime.SubAgentHostBinder{}),
		"SubAgentReadHostBinder":             reflect.TypeOf(floretRuntime.SubAgentReadHostBinder{}),
		"PendingToolRecoveryHostBinder":      reflect.TypeOf(floretRuntime.PendingToolRecoveryHostBinder{}),
		"InterruptedTurnRecoveryHostBinder":  reflect.TypeOf(floretRuntime.InterruptedTurnRecoveryHostBinder{}),
		"InterruptedTurnRecoveryHostFactory": reflect.TypeOf(floretRuntime.InterruptedTurnRecoveryHostFactory{}),
		"TurnExecutionHostFactory":           reflect.TypeOf(floretRuntime.TurnExecutionHostFactory{}),
		"ThreadCompactionHostFactory":        reflect.TypeOf(floretRuntime.ThreadCompactionHostFactory{}),
		"SubAgentHostFactory":                reflect.TypeOf(floretRuntime.SubAgentHostFactory{}),
		"TurnExecutionHost":                  reflect.TypeOf(floretRuntime.TurnExecutionHost{}),
		"ThreadCompactionHost":               reflect.TypeOf(floretRuntime.ThreadCompactionHost{}),
		"SubAgentHost":                       reflect.TypeOf(floretRuntime.SubAgentHost{}),
		"SubAgentReadHost":                   reflect.TypeOf(floretRuntime.SubAgentReadHost{}),
		"ThreadCreateHost":                   reflect.TypeOf(floretRuntime.ThreadCreateHost{}),
		"ThreadReadHost":                     reflect.TypeOf(floretRuntime.ThreadReadHost{}),
		"ThreadTitleHost":                    reflect.TypeOf(floretRuntime.ThreadTitleHost{}),
		"ThreadForkHost":                     reflect.TypeOf(floretRuntime.ThreadForkHost{}),
		"ThreadDeleteHost":                   reflect.TypeOf(floretRuntime.ThreadDeleteHost{}),
		"PendingToolRecoveryHost":            reflect.TypeOf(floretRuntime.PendingToolRecoveryHost{}),
		"InterruptedTurnRecoveryHost":        reflect.TypeOf(floretRuntime.InterruptedTurnRecoveryHost{}),
	} {
		for i := 0; i < typ.NumField(); i++ {
			if typ.Field(i).PkgPath == "" {
				t.Fatalf("%s exposes exported field %q", name, typ.Field(i).Name)
			}
		}
	}
	compactionOptions := reflect.TypeOf(floretRuntime.ThreadCompactionHostOptions{})
	for _, forbidden := range []string{"Tools", "Approver", "ToolSurfaceProvider", "SubAgentRunTimeout", "Capabilities", "ThreadTitleMode"} {
		if _, ok := compactionOptions.FieldByName(forbidden); ok {
			t.Fatalf("ThreadCompactionHostOptions exposes unrelated field %q", forbidden)
		}
	}
	exactFields := func(name string, typ reflect.Type, want ...string) {
		t.Helper()
		got := make([]string, 0, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			got = append(got, typ.Field(i).Name)
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s fields = %#v, want %#v", name, got, want)
		}
	}
	exactFields("TurnExecutionHostOptions", reflect.TypeOf(floretRuntime.TurnExecutionHostOptions{}),
		"Capabilities", "Config", "EffectAuthorizationGate", "IDGenerator", "LoopLimits", "ModelGateway",
		"ModelGatewayIdentity", "Sink", "ThreadTitleMode", "ToolSurfaceProvider", "Tools")
	exactFields("ThreadCompactionHostOptions", compactionOptions,
		"Config", "IDGenerator", "LoopLimits", "ModelGateway", "ModelGatewayIdentity", "Sink")
	exactFields("SubAgentHostOptions", reflect.TypeOf(floretRuntime.SubAgentHostOptions{}),
		"Capabilities", "Config", "EffectAuthorizationGate", "IDGenerator", "LoopLimits", "ModelGateway",
		"ModelGatewayIdentity", "Sink", "SubAgentRunTimeout", "ThreadTitleMode",
		"ToolSurfaceProvider", "Tools")
	exactFields("ArtifactRef", reflect.TypeOf(floretRuntime.ArtifactRef{}),
		"ID", "Kind", "MIME", "SHA256", "SafeLabel", "SizeBytes")
	exactFields("ReadArtifactRequest", reflect.TypeOf(floretRuntime.ReadArtifactRequest{}),
		"ArtifactID", "ThreadID")
	exactFields("ArtifactContent", reflect.TypeOf(floretRuntime.ArtifactContent{}), "Ref", "Text")
}

func TestTestUIAgentSessionCannotRetainCapabilityIssuers(t *testing.T) {
	path := filepath.Join("internal", "testui", "runner.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec := spec.(*ast.TypeSpec)
			if typeSpec.Name.Name != "agentSession" {
				continue
			}
			found = true
			shape := typeSpec.Type.(*ast.StructType)
			for _, field := range shape.Fields.List {
				ast.Inspect(field.Type, func(node ast.Node) bool {
					ident, ok := node.(*ast.Ident)
					if ok && (strings.HasSuffix(ident.Name, "Binder") || ident.Name == "testUIRuntimeCapabilityBinders") {
						t.Fatalf("test UI agentSession retains Store-wide capability issuer %s", ident.Name)
					}
					return true
				})
			}
		}
	}
	if !found {
		t.Fatal("test UI agentSession type not found")
	}
}

func TestTestUIRuntimeConstructionChecksExactAuthorityBeforeHostSideEffects(t *testing.T) {
	text := readTextFile(t, filepath.Join("internal", "testui", "runner.go"))
	start := strings.Index(text, "func (sess *agentSession) prepareRuntime(")
	if start < 0 {
		t.Fatal("test UI prepareRuntime function not found")
	}
	tail := text[start:]
	end := strings.Index(tail, "\nfunc (sess *agentSession) applyRuntime(")
	if end < 0 {
		t.Fatal("test UI prepareRuntime function end not found")
	}
	body := tail[:end]
	turnAuthority := strings.Index(body, "sess.turnFactory.NewHost(")
	subAgentAuthority := strings.Index(body, "sess.subagentFactory.NewHost(")
	if turnAuthority < 0 || subAgentAuthority < 0 {
		t.Fatal("test UI prepareRuntime must construct exact turn and SubAgent authorities")
	}
	for _, sideEffect := range []string{
		"registerAgentSessionTools(",
		"r.registerAgentCapabilities(",
		"r.providerFactory()(",
		"r.titleProviderFactory()(",
	} {
		position := strings.Index(body, sideEffect)
		if position < 0 {
			t.Fatalf("test UI prepareRuntime is missing %q", sideEffect)
		}
		if position < turnAuthority || position < subAgentAuthority {
			t.Fatalf("test UI prepareRuntime starts %q before exact authority construction", sideEffect)
		}
	}
}

func TestProviderCapabilityHostConstructionRequiresAuthorityContext(t *testing.T) {
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	for _, item := range []struct {
		typ    reflect.Type
		method string
	}{
		{reflect.TypeOf((*floretRuntime.ThreadReadHostBinder)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.ThreadTitleHostBinder)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.ThreadForkHostBinder)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.ThreadDeleteHostBinder)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.SubAgentReadHostBinder)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.PendingToolRecoveryHostBinder)(nil)), "NewThreadHost"},
		{reflect.TypeOf((*floretRuntime.PendingToolRecoveryHostBinder)(nil)), "NewSubAgentHost"},
		{reflect.TypeOf((*floretRuntime.InterruptedTurnRecoveryHostBinder)(nil)), "BindThread"},
		{reflect.TypeOf((*floretRuntime.InterruptedTurnRecoveryHostBinder)(nil)), "BindSubAgent"},
		{reflect.TypeOf((*floretRuntime.InterruptedTurnRecoveryHostFactory)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.TurnExecutionHostFactory)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.ThreadCompactionHostFactory)(nil)), "NewHost"},
		{reflect.TypeOf((*floretRuntime.SubAgentHostFactory)(nil)), "NewHost"},
	} {
		method, ok := item.typ.MethodByName(item.method)
		if !ok {
			t.Fatalf("%s is missing %s", item.typ.Elem().Name(), item.method)
		}
		if method.Type.NumIn() < 2 || method.Type.In(1) != contextType {
			t.Fatalf("%s.%s must receive context.Context first, got %s", item.typ.Elem().Name(), item.method, method.Type)
		}
	}
}

func TestRuntimePrivateProviderHostOnlyBacksApprovedCapabilities(t *testing.T) {
	allowed := map[string]bool{
		"TurnExecutionHost":    true,
		"ThreadCompactionHost": true,
		"SubAgentHost":         true,
	}
	found := map[string]bool{}
	for _, path := range walkAllFiles(t, "runtime") {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				typeSpec := spec.(*ast.TypeSpec)
				if !ast.IsExported(typeSpec.Name.Name) {
					continue
				}
				usesProviderHost := false
				ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
					ident, ok := node.(*ast.Ident)
					if ok && ident.Name == "providerHost" {
						usesProviderHost = true
					}
					return true
				})
				if !usesProviderHost {
					continue
				}
				if !allowed[typeSpec.Name.Name] {
					t.Fatalf("exported runtime type %s wraps private providerHost", typeSpec.Name.Name)
				}
				found[typeSpec.Name.Name] = true
			}
		}
	}
	if !reflect.DeepEqual(found, allowed) {
		t.Fatalf("providerHost facade set = %#v, want %#v", found, allowed)
	}
}

func TestRuntimeBootstrapAuthorityIsConfinedToCompositionConstructors(t *testing.T) {
	allowed := map[string]bool{
		"ConfigureHostCapabilities":            true,
		"NewInterruptedTurnRecoveryHostBinder": true,
		"NewPendingToolRecoveryHostBinder":     true,
		"NewSubAgentHostBinder":                true,
		"NewSubAgentReadHostBinder":            true,
		"NewThreadCompactionHostBinder":        true,
		"NewThreadCreateHostBinder":            true,
		"NewThreadDeleteHostBinder":            true,
		"NewThreadForkHostBinder":              true,
		"NewThreadReadHostBinder":              true,
		"NewThreadTitleHostBinder":             true,
		"NewTurnExecutionHostBinder":           true,
	}
	found := map[string]bool{}
	for _, path := range walkAllFiles(t, "runtime") {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			switch typed := decl.(type) {
			case *ast.FuncDecl:
				usesBootstrap := false
				ast.Inspect(typed.Type, func(node ast.Node) bool {
					ident, ok := node.(*ast.Ident)
					if ok && ident.Name == "HostBootstrap" {
						usesBootstrap = true
					}
					return true
				})
				if !usesBootstrap {
					continue
				}
				if !ast.IsExported(typed.Name.Name) {
					continue
				}
				if typed.Recv != nil || !allowed[typed.Name.Name] {
					t.Fatalf("runtime bootstrap authority leaks through %s in %s", typed.Name.Name, path)
				}
				found[typed.Name.Name] = true
			case *ast.GenDecl:
				if typed.Tok != token.TYPE {
					continue
				}
				for _, spec := range typed.Specs {
					typeSpec := spec.(*ast.TypeSpec)
					if typeSpec.Name.Name == "HostBootstrap" {
						continue
					}
					containsBootstrap := false
					ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
						ident, ok := node.(*ast.Ident)
						if ok && ident.Name == "HostBootstrap" {
							containsBootstrap = true
						}
						return true
					})
					if containsBootstrap {
						t.Fatalf("runtime type %s retains bootstrap authority in %s", typeSpec.Name.Name, path)
					}
				}
			}
		}
	}
	if !reflect.DeepEqual(found, allowed) {
		t.Fatalf("bootstrap constructor set = %#v, want %#v", found, allowed)
	}
}

func TestRuntimeStoreAuthorityCrossesOnlyCompositionBoundary(t *testing.T) {
	allowed := map[string]bool{
		"ConfigureHostCapabilities": true,
		"NewMemoryStore":            true,
		"OpenSQLiteStore":           true,
	}
	found := map[string]bool{}
	for _, path := range walkAllFiles(t, "runtime") {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !ast.IsExported(fn.Name.Name) {
				continue
			}
			usesStore := false
			ast.Inspect(fn.Type, func(node ast.Node) bool {
				ident, ok := node.(*ast.Ident)
				if ok && ident.Name == "Store" {
					usesStore = true
				}
				return true
			})
			if !usesStore {
				continue
			}
			if !allowed[fn.Name.Name] {
				t.Fatalf("runtime Store authority leaks through public function %s in %s", fn.Name.Name, path)
			}
			found[fn.Name.Name] = true
		}
	}
	if !reflect.DeepEqual(found, allowed) {
		t.Fatalf("public Store boundary = %#v, want %#v", found, allowed)
	}
}

func TestRuntimeHostOptionsDoNotCarryAuthorityRoots(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(floretRuntime.TurnExecutionHostOptions{}),
		reflect.TypeOf(floretRuntime.ThreadCompactionHostOptions{}),
		reflect.TypeOf(floretRuntime.SubAgentHostOptions{}),
	} {
		for _, field := range []string{"Store", "Bootstrap", "Runtime"} {
			if _, ok := typ.FieldByName(field); ok {
				t.Fatalf("%s exposes authority root field %q", typ.Name(), field)
			}
		}
	}
}

func TestRuntimeCapabilityConstructorsAndAggregatesStayExplicit(t *testing.T) {
	allowedConstructors := map[string]bool{
		"NewPendingToolRecoveryHostBinder":     true,
		"NewInterruptedTurnRecoveryHostBinder": true,
		"NewSubAgentHostBinder":                true,
		"NewSubAgentReadHostBinder":            true,
		"NewThreadCompactionHostBinder":        true,
		"NewThreadCreateHostBinder":            true,
		"NewThreadDeleteHostBinder":            true,
		"NewThreadForkHostBinder":              true,
		"NewThreadReadHostBinder":              true,
		"NewThreadTitleHostBinder":             true,
		"NewTurnExecutionHostBinder":           true,
	}
	foundConstructors := map[string]bool{}
	authorityOwners := map[string]string{
		"CloseSubAgent":                "SubAgentHost",
		"CompactThread":                "ThreadCompactionHost",
		"CompletePendingTool":          "TurnExecutionHost",
		"CreateThread":                 "ThreadCreateHost",
		"DeleteThread":                 "ThreadDeleteHost",
		"ForkThread":                   "ThreadForkHost",
		"ListPendingApprovals":         "TurnExecutionHost",
		"ListSubAgentActivityTimeline": "SubAgentReadHost",
		"ListSubAgents":                "SubAgentReadHost",
		"ListThreadDetailEvents":       "ThreadReadHost",
		"ListThreadTurns":              "ThreadReadHost",
		"ReadLatestThreadTurn":         "ThreadReadHost",
		"ReadSubAgentDetail":           "SubAgentReadHost",
		"ReadThread":                   "ThreadReadHost",
		"ReadThreadAgentTodos":         "ThreadReadHost",
		"ReadThreadContext":            "ThreadReadHost",
		"ReadThreadOverview":           "ThreadReadHost",
		"ReadTurnProjection":           "ThreadReadHost",
		"RetryTurn":                    "TurnExecutionHost",
		"RunTurn":                      "TurnExecutionHost",
		"SendSubAgentInput":            "SubAgentHost",
		"SetThreadTitle":               "ThreadTitleHost",
		"SpawnSubAgent":                "SubAgentHost",
		"UpdateThreadAgentTodos":       "TurnExecutionHost",
		"WaitSubAgents":                "SubAgentHost",
	}
	capabilityTypes := map[string]bool{
		"HostBootstrap": true, "PendingToolRecoveryHost": true, "PendingToolRecoveryHostBinder": true,
		"InterruptedTurnRecoveryHost": true, "InterruptedTurnRecoveryHostBinder": true,
		"InterruptedTurnRecoveryHostFactory": true,
		"ThreadCreateHostBinder":             true, "ThreadReadHostBinder": true, "ThreadTitleHostBinder": true,
		"ThreadForkHostBinder": true, "ThreadDeleteHostBinder": true,
		"SubAgentHost": true, "SubAgentHostBinder": true, "SubAgentHostFactory": true,
		"SubAgentReadHost": true, "SubAgentReadHostBinder": true,
		"ThreadCompactionHost": true, "ThreadCreateHost": true, "ThreadDeleteHost": true,
		"ThreadCompactionHostBinder": true, "ThreadCompactionHostFactory": true,
		"ThreadForkHost": true, "ThreadReadHost": true, "ThreadTitleHost": true,
		"TurnExecutionHost": true, "TurnExecutionHostBinder": true, "TurnExecutionHostFactory": true,
	}
	settlementOwners := map[string]bool{
		"PendingToolRecoveryHost": true,
		"SubAgentHost":            true,
		"TurnExecutionHost":       true,
	}
	for _, path := range walkAllFiles(t, "runtime") {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			switch typed := decl.(type) {
			case *ast.FuncDecl:
				returnsCapability := false
				if typed.Recv == nil && ast.IsExported(typed.Name.Name) && typed.Type.Results != nil {
					ast.Inspect(typed.Type.Results, func(node ast.Node) bool {
						ident, ok := node.(*ast.Ident)
						if ok && capabilityTypes[ident.Name] {
							returnsCapability = true
						}
						return true
					})
				}
				if returnsCapability && !allowedConstructors[typed.Name.Name] {
					t.Fatalf("runtime exposes unreviewed authority constructor %s", typed.Name.Name)
				}
				if typed.Recv == nil && ast.IsExported(typed.Name.Name) && strings.HasPrefix(typed.Name.Name, "New") && strings.Contains(typed.Name.Name, "Host") {
					if !allowedConstructors[typed.Name.Name] {
						t.Fatalf("runtime exposes unreviewed host constructor %s", typed.Name.Name)
					}
					foundConstructors[typed.Name.Name] = true
				}
				if typed.Recv != nil && ast.IsExported(typed.Name.Name) {
					if typed.Name.Name == "SettlePendingTool" {
						receiver := ""
						ast.Inspect(typed.Recv.List[0].Type, func(node ast.Node) bool {
							if ident, ok := node.(*ast.Ident); ok {
								receiver = ident.Name
							}
							return true
						})
						if !settlementOwners[receiver] {
							t.Fatalf("runtime settlement receiver = %s", receiver)
						}
						continue
					}
					owner, authorityMethod := authorityOwners[typed.Name.Name]
					if !authorityMethod {
						continue
					}
					receiver := ""
					ast.Inspect(typed.Recv.List[0].Type, func(node ast.Node) bool {
						if ident, ok := node.(*ast.Ident); ok {
							receiver = ident.Name
						}
						return true
					})
					if ast.IsExported(receiver) && receiver != owner {
						t.Fatalf("runtime authority method %s receiver = %s, want %s", typed.Name.Name, receiver, owner)
					}
				}
			case *ast.GenDecl:
				if typed.Tok != token.TYPE {
					continue
				}
				for _, spec := range typed.Specs {
					typeSpec := spec.(*ast.TypeSpec)
					if ast.IsExported(typeSpec.Name.Name) && typeSpec.Assign.IsValid() {
						aliasesCapability := false
						ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
							ident, ok := node.(*ast.Ident)
							if ok && capabilityTypes[ident.Name] {
								aliasesCapability = true
							}
							return true
						})
						if aliasesCapability {
							t.Fatalf("runtime exported alias %s re-exports an authority capability", typeSpec.Name.Name)
						}
					}
					switch shape := typeSpec.Type.(type) {
					case *ast.InterfaceType:
						for _, field := range shape.Methods.List {
							if ast.IsExported(typeSpec.Name.Name) && len(field.Names) == 0 {
								t.Fatalf("runtime exported interface %s embeds another contract", typeSpec.Name.Name)
							}
							for _, name := range field.Names {
								if _, ok := authorityOwners[name.Name]; ok || name.Name == "SettlePendingTool" {
									t.Fatalf("runtime interface %s aggregates authority method %s", typeSpec.Name.Name, name.Name)
								}
							}
						}
					case *ast.StructType:
						if !ast.IsExported(typeSpec.Name.Name) {
							continue
						}
						for _, field := range shape.Fields.List {
							containsCapability := false
							ast.Inspect(field.Type, func(node ast.Node) bool {
								ident, ok := node.(*ast.Ident)
								if ok && capabilityTypes[ident.Name] {
									containsCapability = true
								}
								return true
							})
							if !containsCapability {
								continue
							}
							t.Fatalf("runtime exported struct %s aggregates an authority capability field", typeSpec.Name.Name)
						}
					}
				}
			}
		}
	}
	if !reflect.DeepEqual(foundConstructors, allowedConstructors) {
		t.Fatalf("host constructor set = %#v, want %#v", foundConstructors, allowedConstructors)
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
		"type Host struct",
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

func TestReadmeKeepsPolishedPresentation(t *testing.T) {
	text := readTextFile(t, "README.md")
	for _, want := range []string{
		"pkg.go.dev/badge/github.com/floegence/floret/runtime.svg",
		"img.shields.io/badge/license-MIT",
		`<a href="#-why-floret">Why Floret</a>`,
		"## \U00002728 Why Floret",
		"## \U0001F9ED At a glance",
		"## \U0001F4E6 Stable downstream API",
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

func TestSQLiteDriverImportsStayInInternalStorage(t *testing.T) {
	for _, file := range goFiles(t, ".") {
		if isArchitectureTest(file) {
			continue
		}
		if strings.HasPrefix(file, filepath.Join("internal", "storage", "sqlite")+string(filepath.Separator)) {
			continue
		}
		text := readTextFile(t, file)
		for _, marker := range []string{
			"github.com/mattn/go-sqlite3",
			"modernc.org/sqlite",
		} {
			if strings.Contains(text, marker) {
				t.Fatalf("sqlite driver import %q outside internal/storage/sqlite: %s", marker, file)
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
	want := []string{"ActivityForCall", "Definition", "Definitions", "Dispatch", "DispatchBatch", "ExposedDefinitions", "OutputPolicyFor", "Register"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tools.Registry exported method set = %#v, want %#v", got, want)
	}
	text := readTextFile(t, filepath.Join("tools", "tools.go"))
	for _, want := range []string{
		"if opts.EffectDispatcher == nil",
		"ErrEffectDispatcherRequired",
		"result = dispatcher(ctx, p.request, func() Result { return p.invoke(ctx) })",
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
