package floret_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	floretruntime "github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestV7ModuleAndIdentityPackageBoundary(t *testing.T) {
	module, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(module), "module github.com/floegence/floret/v7\n") {
		t.Fatal("Floret v7 must use the /v7 module path")
	}
	if _, err := os.Stat("identity"); err != nil {
		t.Fatal("Floret v7 requires the public identity package")
	}
}

func TestCIGatesUseCurrentMajor(t *testing.T) {
	workflow, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	content := string(workflow)
	if !strings.Contains(content, "scripts/check_v7_api_compatibility.sh") || strings.Contains(content, "scripts/check_v6_api_compatibility.sh") {
		t.Fatal("CI must run only the v7 API compatibility gate")
	}
}

func TestSemanticIdentityFieldsDoNotUseString(t *testing.T) {
	semantic := map[string]bool{
		"ThreadID": true, "TurnID": true, "RunID": true, "PromptScopeID": true,
		"TraceID": true, "LogicalRequestID": true, "ArtifactID": true,
	}
	for _, dir := range []string{"provider", "runtime", "tools", "observation"} {
		err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return walkErr
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			for _, declaration := range file.Decls {
				generic, ok := declaration.(*ast.GenDecl)
				if !ok || generic.Tok != token.TYPE {
					continue
				}
				for _, rawSpec := range generic.Specs {
					spec := rawSpec.(*ast.TypeSpec)
					if semantic[spec.Name.Name] && dir != "identity" {
						t.Fatalf("%s redeclares semantic identity %s", path, spec.Name.Name)
					}
					structure, ok := spec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range structure.Fields.List {
						ident, isString := field.Type.(*ast.Ident)
						if !isString || ident.Name != "string" {
							continue
						}
						for _, name := range field.Names {
							if semantic[name.Name] {
								t.Fatalf("%s uses string for semantic identity field %s.%s", path, spec.Name.Name, name.Name)
							}
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestNoV2ImportsRemain(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || filepath.Ext(path) != ".go" {
			return walkErr
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if value == "github.com/floegence/floret/v2" || strings.HasPrefix(value, "github.com/floegence/floret/v2/") {
				t.Fatalf("%s retains v2 import %s", path, value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRetiredCompletionToolExistsOnlyInMigrationFixtureAndChangeLog(t *testing.T) {
	needle := "task_" + "complete"
	allowed := map[string]bool{
		"CHANGELOG.md": true,
		"internal/sessiontree/backend_domain_v8_migration_test.go": true,
	}
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		path = filepath.ToSlash(strings.TrimPrefix(path, "./"))
		if allowed[path] {
			return nil
		}
		extension := filepath.Ext(path)
		if extension != ".go" && extension != ".md" && extension != ".yaml" && extension != ".yml" && extension != ".tsv" && extension != ".sh" && path != "go.mod" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), needle) {
			t.Fatalf("%s retains the retired completion tool", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestApplicationStorageSourceIsOpaque(t *testing.T) {
	sourceType := reflect.TypeOf((*storage.Source)(nil)).Elem()
	if sourceType.NumMethod() != 0 {
		t.Fatalf("storage.Source exposes %d methods; application storage must be opaque", sourceType.NumMethod())
	}
	field, ok := reflect.TypeOf(floretruntime.Options{}).FieldByName("Storage")
	if !ok || field.Type != sourceType {
		t.Fatalf("runtime.Options.Storage type = %v, want storage.Source", field.Type)
	}
}

func TestPublicPackageDependencyDirection(t *testing.T) {
	allowed := map[string]map[string]bool{
		"tools":       {"identity": true, "tools": true},
		"observation": {"config": true, "identity": true, "tools": true},
	}
	for directory, packageAllowlist := range allowed {
		err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return walkErr
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imported := range file.Imports {
				value, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				const module = "github.com/floegence/floret/v7/"
				if !strings.HasPrefix(value, module) {
					continue
				}
				dependency := strings.TrimPrefix(value, module)
				if strings.Contains(dependency, "/") || !packageAllowlist[dependency] {
					t.Fatalf("%s imports forbidden public dependency %s", path, value)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
