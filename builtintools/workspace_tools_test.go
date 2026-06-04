package builtintools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/tools"
)

func TestReadListGlobAndGrepWorkspaceTools(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\nfunc main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "pkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "pkg", "util.go"), []byte("package pkg\nconst Name = \"Floret\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := RegisterReadOnlyWorkspace(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}

	read := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: `{"path":"main.go","offset":0,"limit":1}`}, nil)
	if read.IsError || read.Text != "package main" {
		t.Fatalf("read = %#v", read)
	}
	readDefaultRange := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: `{"path":"main.go"}`}, nil)
	if readDefaultRange.IsError || !strings.Contains(readDefaultRange.Text, "func main()") {
		t.Fatalf("read with default range = %#v", readDefaultRange)
	}
	assertRequiredFields(t, reg, "read", "path")
	list := reg.Run(context.Background(), provider.ToolCall{Name: "list", Args: `{"path":null,"limit":10}`}, nil)
	if list.IsError || !strings.Contains(list.Text, "pkg/") || !strings.Contains(list.Text, "main.go") {
		t.Fatalf("list = %#v", list)
	}
	listDefaultLimit := reg.Run(context.Background(), provider.ToolCall{Name: "list", Args: `{"path":"."}`}, nil)
	if listDefaultLimit.IsError || !strings.Contains(listDefaultLimit.Text, "pkg/") || !strings.Contains(listDefaultLimit.Text, "main.go") {
		t.Fatalf("list with default limit = %#v", listDefaultLimit)
	}
	assertRequiredFields(t, reg, "list")
	glob := reg.Run(context.Background(), provider.ToolCall{Name: "glob", Args: `{"pattern":"**/*.go","path":null,"limit":10}`}, nil)
	if glob.IsError || !strings.Contains(glob.Text, "main.go") || !strings.Contains(glob.Text, "pkg/util.go") {
		t.Fatalf("glob = %#v", glob)
	}
	globDefaultRange := reg.Run(context.Background(), provider.ToolCall{Name: "glob", Args: `{"pattern":"**/*.go"}`}, nil)
	if globDefaultRange.IsError || !strings.Contains(globDefaultRange.Text, "main.go") || !strings.Contains(globDefaultRange.Text, "pkg/util.go") {
		t.Fatalf("glob with default range = %#v", globDefaultRange)
	}
	globIgnoreCase := reg.Run(context.Background(), provider.ToolCall{Name: "glob", Args: `{"pattern":"**/UTIL.GO","ignore_case":true}`}, nil)
	if globIgnoreCase.IsError || !strings.Contains(globIgnoreCase.Text, "pkg/util.go") {
		t.Fatalf("glob with ignore_case = %#v", globIgnoreCase)
	}
	assertRequiredFields(t, reg, "glob", "pattern")
	grep := reg.Run(context.Background(), provider.ToolCall{Name: "grep", Args: `{"pattern":"Floret","path":null,"glob":"*.go","ignore_case":false,"literal":true,"context":null,"limit":10}`}, nil)
	if grep.IsError || !strings.Contains(grep.Text, "util.go") {
		t.Fatalf("grep = %#v", grep)
	}
	grepDefaults := reg.Run(context.Background(), provider.ToolCall{Name: "grep", Args: `{"pattern":"Floret","path":"pkg"}`}, nil)
	if grepDefaults.IsError || !strings.Contains(grepDefaults.Text, "util.go") {
		t.Fatalf("grep with defaults = %#v", grepDefaults)
	}
	assertRequiredFields(t, reg, "grep", "pattern")
}

func assertRequiredFields(t *testing.T, reg *tools.Registry, name string, want ...string) {
	t.Helper()
	def, ok := reg.Definition(name)
	if !ok {
		t.Fatalf("%s definition missing", name)
	}
	required, _ := def.InputSchema["required"].([]any)
	if len(required) != len(want) {
		t.Fatalf("%s required fields = %#v, want %#v", name, def.InputSchema["required"], want)
	}
	for i, field := range want {
		if required[i] != field {
			t.Fatalf("%s required fields = %#v, want %#v", name, def.InputSchema["required"], want)
		}
	}
}

func TestRegisterSelectedExposesOnlyRequestedTools(t *testing.T) {
	reg := tools.NewRegistry()
	if err := RegisterSelected(reg, SelectedOptions{
		Workspace: WorkspaceOptions{Root: t.TempDir()},
		Shell:     ShellOptions{CWD: t.TempDir(), Runner: fakeRunner{result: CommandResult{Stdout: "ok\n", ExitCode: 0}}},
	}, ToolGrep, ToolShell); err != nil {
		t.Fatal(err)
	}

	defs := reg.Definitions()
	if len(defs) != 2 || !hasToolDefinition(defs, ToolGrep) || !hasToolDefinition(defs, ToolShell) {
		t.Fatalf("definitions = %#v", defs)
	}
	if hasToolDefinition(defs, ToolRead) || hasToolDefinition(defs, ToolWrite) || hasToolDefinition(defs, ToolWebSearch) {
		t.Fatalf("unselected tools were registered: %#v", defs)
	}
}

func TestRegisterSelectedRejectsUnknownTool(t *testing.T) {
	for _, name := range []string{"missing", "web_fetch"} {
		err := RegisterSelected(tools.NewRegistry(), SelectedOptions{Workspace: WorkspaceOptions{Root: t.TempDir()}}, name)
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf(`unknown built-in tool %q`, name)) {
			t.Fatalf("%s err = %v", name, err)
		}
	}
}

func TestRegisterSelectedOnlyRequiresOptionsForSelectedTools(t *testing.T) {
	reg := tools.NewRegistry()
	if err := RegisterSelected(reg, SelectedOptions{
		Workspace: WorkspaceOptions{Root: string([]byte{0})},
		Shell:     ShellOptions{CWD: t.TempDir(), Runner: fakeRunner{result: CommandResult{Stdout: "ok\n", ExitCode: 0}}},
	}, ToolShell); err != nil {
		t.Fatal(err)
	}
	defs := reg.Definitions()
	if len(defs) != 1 || !hasToolDefinition(defs, ToolShell) {
		t.Fatalf("definitions = %#v", defs)
	}
}

func TestWorkspaceMutationRequiresApprovalAndWritesFile(t *testing.T) {
	root := t.TempDir()
	reg := tools.NewRegistry()
	if err := RegisterWorkspaceMutation(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	denied := reg.Run(context.Background(), provider.ToolCall{Name: "write", Args: `{"path":"answer.txt","content":"no"}`}, nil)
	if !denied.IsError || denied.Text != tools.ErrRejected.Error() {
		t.Fatalf("denied = %#v", denied)
	}
	approved := reg.Run(context.Background(), provider.ToolCall{Name: "write", Args: `{"path":"answer.txt","content":"yes"}`}, allowAll)
	if approved.IsError {
		t.Fatalf("approved = %#v", approved)
	}
	data, err := os.ReadFile(filepath.Join(root, "answer.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "yes" {
		t.Fatalf("file = %q", data)
	}
	edit := reg.Run(context.Background(), provider.ToolCall{Name: "edit", Args: `{"path":"answer.txt","old_text":"yes","new_text":"done","replace_all":false}`}, allowAll)
	if edit.IsError {
		t.Fatalf("edit = %#v", edit)
	}
	data, _ = os.ReadFile(filepath.Join(root, "answer.txt"))
	if string(data) != "done" {
		t.Fatalf("edited file = %q", data)
	}
}

func TestShellRequiresApprovalAndReturnsExitMetadata(t *testing.T) {
	reg := tools.NewRegistry()
	if err := RegisterShell(reg, ShellOptions{CWD: t.TempDir(), Runner: fakeRunner{result: CommandResult{Stdout: "ok\n", ExitCode: 0, DurationMS: 7}}}); err != nil {
		t.Fatal(err)
	}
	denied := reg.Run(context.Background(), provider.ToolCall{Name: "shell", Args: `{"command":"echo ok","workdir":null,"timeout_ms":null,"max_output_bytes":null}`}, nil)
	if !denied.IsError {
		t.Fatalf("shell without approval should fail: %#v", denied)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: "shell", Args: `{"command":"echo ok","workdir":null,"timeout_ms":null,"max_output_bytes":null}`}, allowAll)
	if got.IsError || got.Text != "ok" || got.Metadata["exit_code"] != 0 {
		t.Fatalf("shell = %#v", got)
	}
}

func TestWebSearchRequiresAPIKeyAndSearchQueryApproval(t *testing.T) {
	t.Setenv("FLORET_BRAVE_SEARCH_API_KEY", "")
	reg := tools.NewRegistry()
	if err := RegisterSearch(reg, SearchOptions{APIKey: ""}); err != nil {
		if !strings.Contains(err.Error(), "FLORET_BRAVE_SEARCH_API_KEY") {
			t.Fatalf("register error = %v", err)
		}
	} else {
		t.Fatalf("expected missing API key registration error")
	}
	if got := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"Changsha weather 2026-06-03"}`}, allowAll); !got.IsError || !strings.Contains(got.Text, "unknown tool") {
		t.Fatalf("unregistered web_search = %#v", got)
	}

	reg = tools.NewRegistry()
	if err := RegisterSearch(reg, SearchOptions{APIKey: "brave-key", Endpoint: "http://127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	denied := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"Changsha weather 2026-06-03","count":8,"country":null,"search_lang":null,"freshness":null}`}, nil)
	if !denied.IsError || denied.Text != tools.ErrRejected.Error() {
		t.Fatalf("denied = %#v", denied)
	}
	var approval tools.ApprovalRequest
	_ = reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"Changsha weather 2026-06-03","count":8,"country":null,"search_lang":null,"freshness":null}`}, func(_ context.Context, req tools.ApprovalRequest) (tools.PermissionDecision, error) {
		approval = req
		return tools.PermissionDecisionDeny, nil
	})
	if len(approval.Resources) != 1 || approval.Resources[0].Kind != "search_query" || approval.Resources[0].Value != "Changsha weather 2026-06-03" {
		t.Fatalf("approval resources = %#v", approval.Resources)
	}
}

func TestWebSearchCallsBraveAndReturnsCompactResults(t *testing.T) {
	var seenPath string
	var seenToken string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.RawQuery
		seenToken = r.Header.Get("X-Subscription-Token")
		writeTestJSON(t, w, map[string]any{
			"web": map[string]any{
				"results": []map[string]any{
					{
						"title":       "Changsha weather",
						"url":         "https://example.com/weather",
						"description": "Thunderstorms in Changsha today.",
						"age":         "2 hours ago",
						"profile":     map[string]any{"name": "Example Weather"},
					},
				},
			},
			"query": map[string]any{"original": "Changsha weather 2026-06-03"},
		})
	}))
	defer server.Close()
	reg := tools.NewRegistry()
	if err := RegisterSearch(reg, SearchOptions{APIKey: "brave-key", Endpoint: server.URL}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"Changsha weather 2026-06-03","count":3,"country":"CN","search_lang":"zh-hans","freshness":"pd"}`}, allowAll)
	if got.IsError || !strings.Contains(got.Text, "Changsha weather") || !strings.Contains(got.Text, "https://example.com/weather") {
		t.Fatalf("web_search = %#v", got)
	}
	if got.Metadata["provider"] != "brave" || got.Metadata["query"] != "Changsha weather 2026-06-03" || got.Metadata["count"] != 3 || got.Metadata["result_count"] != 1 {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
	if seenToken != "brave-key" || !strings.Contains(seenPath, "q=Changsha+weather+2026-06-03") || !strings.Contains(seenPath, "count=3") || !strings.Contains(seenPath, "country=CN") || !strings.Contains(seenPath, "search_lang=zh-hans") || !strings.Contains(seenPath, "freshness=pd") {
		t.Fatalf("request query=%q token=%q", seenPath, seenToken)
	}
}

func TestWebSearchAcceptsQueryOnlyAndUsesDefaults(t *testing.T) {
	var seenPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.RawQuery
		writeTestJSON(t, w, map[string]any{
			"web": map[string]any{"results": []map[string]any{
				{"title": "Changsha weather", "url": "https://example.com/weather", "description": "Rain."},
			}},
		})
	}))
	defer server.Close()
	reg := tools.NewRegistry()
	if err := RegisterSearch(reg, SearchOptions{APIKey: "brave-key", Endpoint: server.URL}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"Changsha weather 2026-06-03"}`}, allowAll)
	if got.IsError || !strings.Contains(got.Text, "Changsha weather") || got.Metadata["count"] != 8 {
		t.Fatalf("web_search query-only = %#v", got)
	}
	if !strings.Contains(seenPath, "q=Changsha+weather+2026-06-03") || !strings.Contains(seenPath, "count=8") || strings.Contains(seenPath, "country=") || strings.Contains(seenPath, "search_lang=") || strings.Contains(seenPath, "freshness=") {
		t.Fatalf("request query=%q", seenPath)
	}
}

func TestWebSearchHandlesEmptyErrorsAndTimeouts(t *testing.T) {
	t.Run("empty results", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeTestJSON(t, w, map[string]any{"web": map[string]any{"results": []any{}}})
		}))
		defer server.Close()
		reg := tools.NewRegistry()
		if err := RegisterSearch(reg, SearchOptions{APIKey: "key", Endpoint: server.URL}); err != nil {
			t.Fatal(err)
		}
		got := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"no such result","count":null,"country":null,"search_lang":null,"freshness":null}`}, allowAll)
		if got.IsError || !strings.Contains(got.Text, "No web search results") || got.Metadata["result_count"] != 0 {
			t.Fatalf("empty = %#v", got)
		}
	})

	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusBadGateway} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, http.StatusText(status), status)
			}))
			defer server.Close()
			reg := tools.NewRegistry()
			if err := RegisterSearch(reg, SearchOptions{APIKey: "key", Endpoint: server.URL}); err != nil {
				t.Fatal(err)
			}
			got := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"error","count":null,"country":null,"search_lang":null,"freshness":null}`}, allowAll)
			if !got.IsError || !strings.Contains(got.Text, fmt.Sprintf("status %d", status)) {
				t.Fatalf("http status %d = %#v", status, got)
			}
		})
	}

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			_, _ = w.Write([]byte(`{"web":{"results":[]}}`))
		}))
		defer server.Close()
		reg := tools.NewRegistry()
		if err := RegisterSearch(reg, SearchOptions{APIKey: "key", Endpoint: server.URL, DefaultTimeoutMS: 20}); err != nil {
			t.Fatal(err)
		}
		got := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: `{"query":"slow","count":null,"country":null,"search_lang":null,"freshness":null}`}, allowAll)
		if !got.IsError || !strings.Contains(got.Text, "deadline") && !strings.Contains(got.Text, "timeout") {
			t.Fatalf("timeout = %#v", got)
		}
	})
}

func TestWebSearchRejectsInvalidArgumentsBeforeExecution(t *testing.T) {
	reg := tools.NewRegistry()
	if err := RegisterSearch(reg, SearchOptions{APIKey: "key"}); err != nil {
		t.Fatal(err)
	}
	for _, args := range []string{
		`{"query":"","count":8,"country":null,"search_lang":null,"freshness":null}`,
		`{"query":"x","count":0,"country":null,"search_lang":null,"freshness":null}`,
		`{"query":"x","count":21,"country":null,"search_lang":null,"freshness":null}`,
		`{"query":"x","count":8,"country":null,"search_lang":null,"freshness":"bad"}`,
	} {
		got := reg.Run(context.Background(), provider.ToolCall{Name: ToolWebSearch, Args: args}, allowAll)
		if !got.IsError {
			t.Fatalf("args %s should fail: %#v", args, got)
		}
	}
}

func TestReadRejectsPathEscapeAbsoluteAndBinary(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bin.dat"), []byte{0, 1, 2}, 0o644); err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := RegisterReadOnlyWorkspace(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	for _, args := range []string{
		`{"path":"../outside","offset":null,"limit":null}`,
		`{"path":"/etc/passwd","offset":null,"limit":null}`,
	} {
		got := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: args}, nil)
		if !got.IsError || !strings.Contains(got.Text, "workspace") {
			t.Fatalf("read should reject %s: %#v", args, got)
		}
	}
	binary := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: `{"path":"bin.dat","offset":null,"limit":null}`}, nil)
	if !binary.IsError || !strings.Contains(binary.Text, "binary") {
		t.Fatalf("binary read = %#v", binary)
	}
}

func TestReadOffsetBeyondEOFAndDirectoryMetadata(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("one\ntwo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := RegisterReadOnlyWorkspace(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: `{"path":"a.txt","offset":99,"limit":10}`}, nil)
	if got.IsError || got.Text != "" || got.Metadata["line_start"] != 3 || got.Metadata["line_end"] != 3 {
		t.Fatalf("offset beyond eof = %#v", got)
	}
	dir := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: `{"path":"dir","offset":null,"limit":10}`}, nil)
	if dir.IsError || dir.Metadata["kind"] != "directory" {
		t.Fatalf("directory read = %#v", dir)
	}
}

func TestApplyPatchAtomicRollbackAndRejectsEscape(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := RegisterWorkspaceMutation(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	patch := strings.Join([]string{
		"*** Begin Patch",
		"*** Add File: created.txt",
		"+created",
		"*** Update File: a.txt",
		"-missing",
		"+new",
		"*** End Patch",
	}, "\n")
	got := reg.Run(context.Background(), provider.ToolCall{Name: "apply_patch", Args: `{"patch":` + quoteJSON(patch) + `}`}, allowAll)
	if !got.IsError || !strings.Contains(got.Text, "did not match") {
		t.Fatalf("patch should fail on context mismatch: %#v", got)
	}
	if _, err := os.Stat(filepath.Join(root, "created.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created file should have been rolled back, err=%v", err)
	}
	data, err := os.ReadFile(filepath.Join(root, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old\n" {
		t.Fatalf("updated file should have been rolled back: %q", data)
	}
	escape := strings.Join([]string{"*** Begin Patch", "*** Add File: ../escape.txt", "+x", "*** End Patch"}, "\n")
	escaped := reg.Run(context.Background(), provider.ToolCall{Name: "apply_patch", Args: `{"patch":` + quoteJSON(escape) + `}`}, allowAll)
	if !escaped.IsError || !strings.Contains(escaped.Text, "workspace") {
		t.Fatalf("escape patch = %#v", escaped)
	}
}

func TestApplyPatchAddUpdateDeleteHappyPath(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "update.txt"), []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "delete.txt"), []byte("remove\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := RegisterWorkspaceMutation(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	patch := strings.Join([]string{
		"*** Begin Patch",
		"*** Add File: add.txt",
		"+added",
		"*** Update File: update.txt",
		"-old",
		"+new",
		"*** Delete File: delete.txt",
		"*** End Patch",
	}, "\n")
	got := reg.Run(context.Background(), provider.ToolCall{Name: "apply_patch", Args: `{"patch":` + quoteJSON(patch) + `}`}, allowAll)
	if got.IsError {
		t.Fatalf("patch = %#v", got)
	}
	added, err := os.ReadFile(filepath.Join(root, "add.txt"))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := os.ReadFile(filepath.Join(root, "update.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(added) != "added" || string(updated) != "new\n" {
		t.Fatalf("added=%q updated=%q", added, updated)
	}
	if _, err := os.Stat(filepath.Join(root, "delete.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete.txt should be removed, err=%v", err)
	}
	files, ok := got.Metadata["files"].([]string)
	if !ok || !sameStrings(files, []string{"add.txt", "update.txt", "delete.txt"}) {
		t.Fatalf("files metadata = %#v", got.Metadata["files"])
	}
}

func TestEditRejectsMultipleMatchesUnlessReplaceAll(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("x\nx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := RegisterWorkspaceMutation(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	one := reg.Run(context.Background(), provider.ToolCall{Name: "edit", Args: `{"path":"a.txt","old_text":"x","new_text":"y","replace_all":false}`}, allowAll)
	if !one.IsError || !strings.Contains(one.Text, "matched 2 times") {
		t.Fatalf("single edit = %#v", one)
	}
	all := reg.Run(context.Background(), provider.ToolCall{Name: "edit", Args: `{"path":"a.txt","old_text":"x","new_text":"y","replace_all":true}`}, allowAll)
	if all.IsError {
		t.Fatalf("replace all = %#v", all)
	}
	data, err := os.ReadFile(filepath.Join(root, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "y\ny\n" {
		t.Fatalf("file = %q", data)
	}
}

func TestWriteCreatesParentsOverwritesExistingEmptyAndRejectsEscape(t *testing.T) {
	root := t.TempDir()
	reg := tools.NewRegistry()
	if err := RegisterWorkspaceMutation(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	create := reg.Run(context.Background(), provider.ToolCall{Name: "write", Args: `{"path":"nested/a.txt","content":"first"}`}, allowAll)
	if create.IsError {
		t.Fatalf("create = %#v", create)
	}
	overwrite := reg.Run(context.Background(), provider.ToolCall{Name: "write", Args: `{"path":"nested/a.txt","content":"second"}`}, allowAll)
	if overwrite.IsError {
		t.Fatalf("overwrite = %#v", overwrite)
	}
	empty := reg.Run(context.Background(), provider.ToolCall{Name: "write", Args: `{"path":"empty.txt","content":""}`}, allowAll)
	if empty.IsError || empty.Metadata["bytes"] != 0 {
		t.Fatalf("empty = %#v", empty)
	}
	data, err := os.ReadFile(filepath.Join(root, "nested", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "second" {
		t.Fatalf("file = %q", data)
	}
	escape := reg.Run(context.Background(), provider.ToolCall{Name: "write", Args: `{"path":"../escape.txt","content":"x"}`}, allowAll)
	if !escape.IsError || !strings.Contains(escape.Text, "workspace") {
		t.Fatalf("escape = %#v", escape)
	}
}

func TestListGlobGrepBoundaries(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{"b.txt", "a.txt", ".git/config", "pkg/one.go", "pkg/two.go"} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		content := path + "\n"
		if strings.HasSuffix(path, ".go") {
			content = "package pkg\nfunc Target() {}\n"
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reg := tools.NewRegistry()
	if err := RegisterReadOnlyWorkspace(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	list := reg.Run(context.Background(), provider.ToolCall{Name: "list", Args: `{"path":null,"limit":2}`}, nil)
	if list.IsError || list.Text != ".git/\na.txt" {
		t.Fatalf("list = %#v", list)
	}
	listEscape := reg.Run(context.Background(), provider.ToolCall{Name: "list", Args: `{"path":"../escape","limit":null}`}, nil)
	if !listEscape.IsError || !strings.Contains(listEscape.Text, "workspace") {
		t.Fatalf("list escape = %#v", listEscape)
	}
	glob := reg.Run(context.Background(), provider.ToolCall{Name: "glob", Args: `{"pattern":"**/*.go","path":null,"limit":1}`}, nil)
	if glob.IsError || strings.Contains(glob.Text, ".git") || strings.Count(strings.TrimSpace(glob.Text), "\n") > 0 || !strings.Contains(glob.Text, ".go") {
		t.Fatalf("glob = %#v", glob)
	}
	globNone := reg.Run(context.Background(), provider.ToolCall{Name: "glob", Args: `{"pattern":"**/*.missing","path":null,"limit":10}`}, nil)
	if globNone.IsError || globNone.Text != "" || globNone.Metadata["matches"] != 0 {
		t.Fatalf("glob none = %#v", globNone)
	}
	globEscape := reg.Run(context.Background(), provider.ToolCall{Name: "glob", Args: `{"pattern":"*","path":"../escape","limit":null}`}, nil)
	if !globEscape.IsError || !strings.Contains(globEscape.Text, "workspace") {
		t.Fatalf("glob escape = %#v", globEscape)
	}
	grep := reg.Run(context.Background(), provider.ToolCall{Name: "grep", Args: `{"pattern":"target","path":"pkg","glob":"*.go","ignore_case":true,"literal":false,"context":1,"limit":3}`}, nil)
	if grep.IsError || !strings.Contains(grep.Text, "Target") || grep.Metadata["matches"] != 3 {
		t.Fatalf("grep = %#v", grep)
	}
	grepNone := reg.Run(context.Background(), provider.ToolCall{Name: "grep", Args: `{"pattern":"missing","path":null,"glob":null,"ignore_case":false,"literal":true,"context":null,"limit":10}`}, nil)
	if grepNone.IsError || grepNone.Text != "" || grepNone.Metadata["matches"] != 0 {
		t.Fatalf("grep none = %#v", grepNone)
	}
	grepEscape := reg.Run(context.Background(), provider.ToolCall{Name: "grep", Args: `{"pattern":"x","path":"../escape","glob":null,"ignore_case":false,"literal":true,"context":null,"limit":10}`}, nil)
	if !grepEscape.IsError || !strings.Contains(grepEscape.Text, "workspace") {
		t.Fatalf("grep escape = %#v", grepEscape)
	}
}

func TestGlobSkipsGitDirectoryEvenWhenFilesMatchPattern(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{".git/ignored.go", "pkg/visible.go"} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("package demo\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reg := tools.NewRegistry()
	if err := RegisterReadOnlyWorkspace(reg, WorkspaceOptions{Root: root}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: "glob", Args: `{"pattern":"**/*.go","path":null,"limit":10}`}, nil)
	if got.IsError || !strings.Contains(got.Text, "pkg/visible.go") || strings.Contains(got.Text, ".git/ignored.go") {
		t.Fatalf("glob should skip .git matches: %#v", got)
	}
}

func TestShellNonZeroAndMaxOutputBytesTruncate(t *testing.T) {
	reg := tools.NewRegistry()
	if err := RegisterShell(reg, ShellOptions{CWD: t.TempDir(), Runner: fakeRunner{result: CommandResult{Stdout: "0123456789abcdef", Stderr: "bad\n", ExitCode: 9, DurationMS: 3}}, MaxOutputBytes: 8}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: "shell", Args: `{"command":"fail","workdir":null,"timeout_ms":null,"max_output_bytes":8}`}, allowAll)
	if !got.IsError || got.Metadata["exit_code"] != 9 || got.Metadata["truncated"] != true || !strings.HasSuffix(got.Text, "bad") {
		t.Fatalf("shell = %#v", got)
	}
}

func TestShellTimeoutWorkdirAndClosedStdin(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	seen := recordingRunner{}
	reg := tools.NewRegistry()
	if err := RegisterShell(reg, ShellOptions{CWD: root, Runner: &seen, DefaultTimeoutMS: 1234}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: "shell", Args: `{"command":"cat","workdir":"sub","timeout_ms":77,"max_output_bytes":null}`}, allowAll)
	if got.IsError {
		t.Fatalf("shell = %#v", got)
	}
	if seen.request.Command != "cat" || seen.request.TimeoutMS != 77 || seen.request.Workdir != filepath.Join(root, "sub") {
		t.Fatalf("request = %#v", seen.request)
	}

	live := tools.NewRegistry()
	if err := RegisterShell(live, ShellOptions{CWD: root, DefaultTimeoutMS: 200}); err != nil {
		t.Fatal(err)
	}
	stdin := live.Run(context.Background(), provider.ToolCall{Name: "shell", Args: `{"command":"cat","workdir":null,"timeout_ms":200,"max_output_bytes":null}`}, allowAll)
	if stdin.IsError || !strings.Contains(stdin.Text, "no output") {
		t.Fatalf("stdin should be closed, got %#v", stdin)
	}
	started := time.Now()
	timeout := live.Run(context.Background(), provider.ToolCall{Name: "shell", Args: `{"command":"sleep 1","workdir":null,"timeout_ms":20,"max_output_bytes":null}`}, allowAll)
	if !timeout.IsError || !strings.Contains(timeout.Text, "deadline") || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("timeout = %#v elapsed=%s", timeout, time.Since(started))
	}
}

func allowAll(context.Context, tools.ApprovalRequest) (tools.PermissionDecision, error) {
	return tools.PermissionDecisionAllow, nil
}

type fakeRunner struct {
	result CommandResult
}

func (r fakeRunner) Run(context.Context, CommandRequest) (CommandResult, error) {
	return r.result, nil
}

type recordingRunner struct {
	request CommandRequest
}

func (r *recordingRunner) Run(_ context.Context, req CommandRequest) (CommandResult, error) {
	r.request = req
	return CommandResult{Stdout: "ok", ExitCode: 0}, nil
}

func quoteJSON(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return `"` + value + `"`
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func hasToolDefinition(defs []provider.ToolDefinition, name string) bool {
	for _, def := range defs {
		if def.Name == name {
			return true
		}
	}
	return false
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}
