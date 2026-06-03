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
		{pkg: "engine", forbidden: []string{"github.com/floegence/floret/builtintools"}},
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

func TestNoLegacyToolHandlerOrLocalWebSearch(t *testing.T) {
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
	if strings.Contains(text, `"web_search"`) {
		t.Fatalf("local executable web_search must not be registered in tools")
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
