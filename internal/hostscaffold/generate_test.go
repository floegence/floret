package hostscaffold

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestGenerateProfilesAreDeterministicAndParse(t *testing.T) {
	for _, profile := range Profiles() {
		t.Run(string(profile), func(t *testing.T) {
			first, err := Generate(Config{Profile: profile, Package: "composition"})
			if err != nil {
				t.Fatal(err)
			}
			second, err := Generate(Config{Profile: profile, Package: "composition"})
			if err != nil {
				t.Fatal(err)
			}
			wantFiles := 2
			if profile == ProfileApproval || profile == ProfileDurableBasic || profile == ProfileProductionRecovery {
				wantFiles = 3
			}
			if len(first) != wantFiles || len(second) != len(first) {
				t.Fatalf("generated file count first=%d second=%d", len(first), len(second))
			}
			for index := range first {
				if first[index].Name != second[index].Name || !bytes.Equal(first[index].Content, second[index].Content) {
					t.Fatalf("non-deterministic output at index %d", index)
				}
				if _, err := parser.ParseFile(token.NewFileSet(), first[index].Name, first[index].Content, parser.AllErrors); err != nil {
					t.Fatalf("parse generated source: %v\n%s", err, first[index].Content)
				}
				text := string(first[index].Content)
				for _, forbidden := range []string{"internal/", "go.work", "replace ", "backup"} {
					if strings.Contains(text, forbidden) {
						t.Fatalf("generated source contains forbidden %q", forbidden)
					}
				}
			}
		})
	}
}

func TestGenerateRejectsUnknownProfileAndInvalidPackage(t *testing.T) {
	if _, err := Generate(Config{Profile: "basic", Package: "composition"}); err == nil {
		t.Fatal("unknown profile generated")
	}
	if _, err := Generate(Config{Profile: ProfileMemory, Package: "not-valid"}); err == nil {
		t.Fatal("invalid package generated")
	}
}

func TestGeneratedCompositionKeepsAggregateAuthorityPackagePrivate(t *testing.T) {
	for _, profile := range Profiles() {
		t.Run(string(profile), func(t *testing.T) {
			files, err := Generate(Config{Profile: profile, Package: "composition"})
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), files[0].Name, files[0].Content, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			for _, declaration := range parsed.Decls {
				typeDeclaration, ok := declaration.(*ast.GenDecl)
				if !ok || typeDeclaration.Tok != token.TYPE {
					continue
				}
				for _, specification := range typeDeclaration.Specs {
					typeSpec := specification.(*ast.TypeSpec)
					if !typeSpec.Name.IsExported() {
						continue
					}
					ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
						selector, ok := node.(*ast.SelectorExpr)
						if !ok {
							return true
						}
						name := selector.Sel.Name
						if name == "Store" || name == "HostBootstrap" || strings.HasSuffix(name, "Binder") {
							t.Errorf("exported type %s leaks aggregate authority %s", typeSpec.Name.Name, name)
						}
						return true
					})
				}
			}
			text := string(files[0].Content)
			for _, localContract := range []string{"type floretThreadCreator interface", "type floretThreadReader interface", "type floretTurnRunner interface"} {
				if !strings.Contains(text, localContract) {
					t.Errorf("generated composition omitted local contract %q", localContract)
				}
			}
			if strings.Contains(text, "func (composition *floretComposition) Create") ||
				strings.Contains(text, "func (composition *floretComposition) Run") ||
				strings.Contains(text, "func (composition *floretComposition) Read") {
				t.Fatal("composition implements a cross-family business facade")
			}
		})
	}
}
