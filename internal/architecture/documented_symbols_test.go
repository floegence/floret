package floret_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestDocumentedRuntimeSymbolsExist(t *testing.T) {
	exported := map[string]bool{}
	files, err := filepath.Glob(filepath.Join("runtime", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			switch value := declaration.(type) {
			case *ast.FuncDecl:
				if value.Recv == nil && value.Name.IsExported() {
					exported[value.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, raw := range value.Specs {
					switch spec := raw.(type) {
					case *ast.TypeSpec:
						if spec.Name.IsExported() {
							exported[spec.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if name.IsExported() {
								exported[name.Name] = true
							}
						}
					}
				}
			}
		}
	}

	reference := regexp.MustCompile("`runtime\\.([A-Z][A-Za-z0-9_]*)")
	paths := []string{"README.md"}
	err = filepath.WalkDir("okf", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || filepath.Ext(path) != ".md" || filepath.Base(path) == "log.md" {
			return walkErr
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range reference.FindAllStringSubmatch(string(data), -1) {
			if !exported[match[1]] {
				t.Errorf("%s references missing runtime symbol %s", path, match[1])
			}
		}
	}
}
