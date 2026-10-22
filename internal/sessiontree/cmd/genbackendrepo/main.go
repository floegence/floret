package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var readMethods = map[string]bool{
	"ParentEvents":         true,
	"JournalMetadata":      true,
	"ArtifactClosure":      true,
	"CanonicalTurnEntries": true, "EffectAttempt": true, "Entries": true, "FindTurnRequest": true, "Entry": true,
	"ListCanonicalTurns": true, "ListSubAgentInputs": true, "ListThreads": true,
	"Path": true, "PathPage": true, "PathMetadataPage": true, "PathMetadataWindow": true, "PathEntriesWindow": true, "PendingAutomaticThreadTitles": true,
	"ProviderState": true, "ReadAgentTodoState": true,
	"SourceBodyPage": true, "CanonicalBodyMetadata": true, "ReadArtifact": true, "ReadCanonicalTurn": true, "ReadCanonicalBody": true, "ReadCompaction": true,
	"ReadPendingToolCompletion": true, "ReadSubAgentInput": true,
	"Thread": true, "ThreadTombstone": true,
	"ValidateArtifactForkDestination": true, "ValidateInterruptedTurnResolution": true,
}

var handwritten = map[string]bool{
	"BootstrapParentEvents": true,
	"MoveLeaf":              true,
	"Fork":                  true, "ForkWithInitialEntry": true,
	"ReadArtifact": true, "ArtifactClosure": true, "ValidateArtifactForkDestination": true,
	"ParentEvents": true, "AdmitParentEvents": true, "ReportToParent": true,
	"SubmitGoal": true,
	"MutateGoal": true, "RecordGoalReview": true,
	"ReadCanonicalTurn": true, "ListCanonicalTurns": true,
	"CommitContextCompaction": true,
	"EffectAttempt":           true, "PrepareEffectAttempt": true, "RejectEffectAttempt": true, "BeginEffectDispatch": true, "FinishEffectDispatch": true,
	"CancelTurn": true, "FailUnknownEffectTurn": true,
	"FinishTurn":      true,
	"FindTurnRequest": true,
	"Entries":         true, "CanonicalTurnEntries": true,
	"AppendRuntimeFacts": true, "CommitProviderContext": true,
	"Append":         true,
	"SetThreadTitle": true, "BeginAutomaticThreadTitle": true, "CompleteAutomaticThreadTitle": true, "FailAutomaticThreadTitle": true,
	"CompareAndSwapAgentTodoState": true, "PutProviderState": true, "DeleteProviderState": true, "UpdateThread": true,
	"SourceBodyPage":     true,
	"ReadAgentTodoState": true, "ProviderState": true,
	"Thread": true, "Entry": true, "Path": true, "CanonicalBodyMetadata": true, "ReadCanonicalBody": true,
	"PathPage": true, "PathMetadataPage": true, "JournalMetadata": true,
	"PathMetadataWindow": true, "PathEntriesWindow": true,
	"Query": true, "Read": true, "Aggregate": true, "CanonicalMediaEntries": true, "CanonicalToolExchange": true,
	"CanonicalMediaEntriesForThread": true,
	"EncodeMemoryState":              true,
	"ListRootThreadInventory":        true, "ListThreads": true,
	"PendingAutomaticThreadTitles": true, "AcceptTurn": true, "ReadAcceptedTurn": true,
	"CreateThread": true, "CreateThreadWithInitialEntry": true,
	"ThreadOrigin": true, "ThreadTombstone": true, "DeleteRootTree": true, "DeleteRootTreeWithRequest": true,
}

var unscoped = map[string]bool{
	"BootstrapParentEvents":           true,
	"CreateThread":                    true,
	"CreateThreadWithInitialEntry":    true,
	"DeleteRootTree":                  true,
	"DeleteRootTreeWithRequest":       true,
	"ListThreads":                     true,
	"PendingAutomaticThreadTitles":    true,
	"ReportToParent":                  true,
	"ReadArtifact":                    true,
	"ArtifactClosure":                 true,
	"ValidateArtifactForkDestination": true,
}

// Some operations carry both a source thread and a destination entry. The
// source thread owns the canonical read/write transaction even when another
// argument also happens to contain a ThreadID field.
var explicitScopes = map[string]string{
	"CommitContextCompaction": "req.Summary.ThreadID",
	"ForkWithInitialEntry":    "opts.SourceThreadID",
}

func main() {
	dir := flag.String("dir", "internal/sessiontree", "sessiontree package directory")
	out := flag.String("out", "internal/sessiontree/backend_repo_generated.go", "generated output")
	flag.Parse()
	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, *dir, func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go") && info.Name() != filepath.Base(*out)
	}, 0)
	if err != nil {
		panic(err)
	}
	var methods []*ast.FuncDecl
	typeFields := map[string]map[string]bool{}
	for _, file := range packages["sessiontree"].Files {
		for _, declaration := range file.Decls {
			if typeDeclaration, ok := declaration.(*ast.GenDecl); ok && typeDeclaration.Tok == token.TYPE {
				for _, specification := range typeDeclaration.Specs {
					typeSpec, ok := specification.(*ast.TypeSpec)
					if !ok {
						continue
					}
					structure, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					fields := map[string]bool{}
					for _, field := range structure.Fields.List {
						for _, name := range field.Names {
							fields[name.Name] = true
						}
					}
					typeFields[typeSpec.Name.Name] = fields
				}
			}
			method, ok := declaration.(*ast.FuncDecl)
			if !ok || method.Recv == nil || !ast.IsExported(method.Name.Name) || handwritten[method.Name.Name] {
				continue
			}
			star, ok := method.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			identifier, receiverIsIdentifier := star.X.(*ast.Ident)
			if !receiverIsIdentifier || identifier.Name != "MemoryRepo" {
				continue
			}
			methods = append(methods, method)
		}
	}
	sort.Slice(methods, func(i, j int) bool { return methods[i].Name.Name < methods[j].Name.Name })
	var generated bytes.Buffer
	generated.WriteString("// Code generated by genbackendrepo; DO NOT EDIT.\n\npackage sessiontree\n\n")
	if len(methods) > 0 {
		generated.WriteString("import \"context\"\n\n")
	}
	for _, method := range methods {
		generateMethod(&generated, fset, method, typeFields)
	}
	formatted, err := format.Source(generated.Bytes())
	if err != nil {
		panic(fmt.Errorf("format generated source: %w\n%s", err, generated.Bytes()))
	}
	if err := os.WriteFile(*out, formatted, 0o644); err != nil {
		panic(err)
	}
}

func generateMethod(out *bytes.Buffer, fset *token.FileSet, method *ast.FuncDecl, typeFields map[string]map[string]bool) {
	params, arguments := parameters(fset, method.Type.Params)
	results, resultTypes := results(fset, method.Type.Results)
	fmt.Fprintf(out, "func (repo *BackendRepo) %s%s%s {\n", method.Name.Name, params, results)
	for index, resultType := range resultTypes {
		fmt.Fprintf(out, "\tvar result%d %s\n", index, resultType)
	}
	operation := "update"
	if readMethods[method.Name.Name] {
		operation = "view"
	}
	scope := explicitScopes[method.Name.Name]
	if scope == "" {
		scope = methodScope(method, typeFields)
	}
	if scope != "" {
		operation += "Thread"
		fmt.Fprintf(out, "\terr := repo.%s(ctx, %s, func(memory *MemoryRepo) error {\n", operation, scope)
	} else {
		fmt.Fprintf(out, "\terr := repo.%s(ctx, func(memory *MemoryRepo) error {\n", operation)
	}
	if len(resultTypes) == 0 {
		fmt.Fprintf(out, "\t\treturn memory.%s(%s)\n", method.Name.Name, strings.Join(arguments, ", "))
	} else {
		assigned := make([]string, len(resultTypes))
		for index := range assigned {
			assigned[index] = fmt.Sprintf("result%d", index)
		}
		fmt.Fprintf(out, "\t\tvar callErr error\n\t\t%s, callErr = memory.%s(%s)\n\t\treturn callErr\n", strings.Join(assigned, ", "), method.Name.Name, strings.Join(arguments, ", "))
	}
	out.WriteString("\t})\n")
	returns := make([]string, len(resultTypes)+1)
	for index := range resultTypes {
		returns[index] = fmt.Sprintf("result%d", index)
	}
	returns[len(returns)-1] = "err"
	fmt.Fprintf(out, "\treturn %s\n}\n\n", strings.Join(returns, ", "))
}

func methodScope(method *ast.FuncDecl, typeFields map[string]map[string]bool) string {
	if unscoped[method.Name.Name] {
		return ""
	}
	if method.Type.Params == nil {
		return ""
	}
	var candidates []string
	for _, field := range method.Type.Params.List {
		name := ""
		if len(field.Names) > 0 {
			name = field.Names[0].Name
		}
		if name == "threadID" {
			return name
		}
		if name == "_" || name == "ctx" || name == "" {
			continue
		}
		typeName := ""
		switch typed := field.Type.(type) {
		case *ast.Ident:
			typeName = typed.Name
		case *ast.StarExpr:
			if identifier, ok := typed.X.(*ast.Ident); ok {
				typeName = identifier.Name
			}
		}
		if typeName == "ThreadMeta" {
			candidates = append(candidates, name+".ID")
			continue
		}
		fields := typeFields[typeName]
		if fields["ThreadID"] {
			candidates = append(candidates, name+".ThreadID")
			continue
		}
		if fields["SourceThreadID"] {
			candidates = append(candidates, name+".SourceThreadID")
		}
	}
	if len(candidates) != 1 {
		return ""
	}
	return candidates[0]
}

func parameters(fset *token.FileSet, fields *ast.FieldList) (string, []string) {
	arguments := make([]string, 0)
	index := 0
	for _, field := range fields.List {
		if len(field.Names) == 0 {
			field.Names = []*ast.Ident{ast.NewIdent(fmt.Sprintf("arg%d", index))}
		}
		for _, name := range field.Names {
			if name.Name == "_" {
				name.Name = "ctx"
			}
			argument := name.Name
			if _, variadic := field.Type.(*ast.Ellipsis); variadic {
				argument += "..."
			}
			arguments = append(arguments, argument)
			index++
		}
	}
	return renderFieldList(fset, fields), arguments
}

func results(fset *token.FileSet, fields *ast.FieldList) (string, []string) {
	if fields == nil || len(fields.List) == 0 {
		return "", nil
	}
	types := make([]string, 0)
	for index, field := range fields.List {
		typeName := render(fset, field.Type)
		count := len(field.Names)
		if count == 0 {
			count = 1
		}
		if index == len(fields.List)-1 && typeName == "error" {
			continue
		}
		for range count {
			types = append(types, typeName)
		}
	}
	return " " + renderFieldList(fset, fields), types
}

func renderFieldList(fset *token.FileSet, fields *ast.FieldList) string {
	parts := make([]string, 0, len(fields.List))
	for _, field := range fields.List {
		typeName := render(fset, field.Type)
		if len(field.Names) == 0 {
			parts = append(parts, typeName)
			continue
		}
		names := make([]string, len(field.Names))
		for index, name := range field.Names {
			names[index] = name.Name
		}
		parts = append(parts, strings.Join(names, ", ")+" "+typeName)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func render(fset *token.FileSet, node any) string {
	var output bytes.Buffer
	if err := format.Node(&output, fset, node.(ast.Node)); err != nil {
		panic(err)
	}
	return output.String()
}
