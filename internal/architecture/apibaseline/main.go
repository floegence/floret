package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

var publicPackages = []string{
	"github.com/floegence/floret/v6/identity",
	"github.com/floegence/floret/v6/config",
	"github.com/floegence/floret/v6/provider",
	"github.com/floegence/floret/v6/runtime",
	"github.com/floegence/floret/v6/storage",
	"github.com/floegence/floret/v6/storage/spi",
	"github.com/floegence/floret/v6/tools",
	"github.com/floegence/floret/v6/tools/webfetch",
	"github.com/floegence/floret/v6/observation",
	"github.com/floegence/floret/v6/florettest",
}

type listedPackage struct {
	ImportPath string
	Export     string
}

func main() {
	root := flag.String("root", ".", "Floret repository root")
	flag.Parse()
	if flag.NArg() != 0 {
		fatalf("positional arguments are not supported")
	}
	output, err := generate(*root)
	if err != nil {
		fatalf("%v", err)
	}
	fmt.Print(output)
}

func generate(root string) (string, error) {
	args := append([]string{"list", "-deps", "-export", "-json"}, publicPackages...)
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	exports := map[string]string{}
	decoder := json.NewDecoder(stdout)
	for {
		var listed listedPackage
		if err := decoder.Decode(&listed); err != nil {
			if err == io.EOF {
				break
			}
			return "", fmt.Errorf("decode go list output: %w", err)
		}
		if listed.ImportPath != "" && listed.Export != "" {
			exports[listed.ImportPath] = listed.Export
		}
	}
	if err := cmd.Wait(); err != nil {
		return "", err
	}

	fset := token.NewFileSet()
	compiled := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		export, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(export)
	})
	var output strings.Builder
	output.WriteString("# Floret v6 public API baseline\n")
	output.WriteString("# Design authority: internal/architecture/testdata/v6-public-api.txt\n\n")
	for index, path := range publicPackages {
		pkg, err := compiled.Import(path)
		if err != nil {
			return "", fmt.Errorf("import %s: %w", path, err)
		}
		writePackage(&output, pkg)
		if index < len(publicPackages)-1 {
			output.WriteByte('\n')
		}
	}
	return output.String(), nil
}

func writePackage(output *strings.Builder, pkg *types.Package) {
	fmt.Fprintf(output, "package %s\n", pkg.Path())
	names := pkg.Scope().Names()
	sort.Strings(names)
	for _, name := range names {
		if !token.IsExported(name) {
			continue
		}
		object := pkg.Scope().Lookup(name)
		switch typed := object.(type) {
		case *types.Const:
			fmt.Fprintf(output, "const %s %s = %s\n", name, typeString(typed.Type()), typed.Val().ExactString())
		case *types.Var:
			fmt.Fprintf(output, "var %s %s\n", name, typeString(typed.Type()))
		case *types.Func:
			fmt.Fprintf(output, "func %s %s\n", name, typeString(typed.Type()))
		case *types.TypeName:
			writeType(output, typed)
		}
	}
}

func writeType(output *strings.Builder, object *types.TypeName) {
	name := object.Name()
	if object.IsAlias() {
		fmt.Fprintf(output, "type %s = %s\n", name, typeString(object.Type()))
		return
	}
	named, ok := object.Type().(*types.Named)
	if !ok {
		fmt.Fprintf(output, "type %s %s\n", name, typeString(object.Type().Underlying()))
		return
	}
	switch underlying := named.Underlying().(type) {
	case *types.Struct:
		fmt.Fprintf(output, "type %s struct\n", name)
		for index := 0; index < underlying.NumFields(); index++ {
			field := underlying.Field(index)
			if !field.Exported() {
				continue
			}
			tag := underlying.Tag(index)
			if field.Embedded() {
				fmt.Fprintf(output, "field %s.<embedded> %s", name, typeString(field.Type()))
			} else {
				fmt.Fprintf(output, "field %s.%s %s", name, field.Name(), typeString(field.Type()))
			}
			if tag != "" {
				fmt.Fprintf(output, " tag=%s", strconv.Quote(tag))
			}
			output.WriteByte('\n')
		}
	case *types.Interface:
		fmt.Fprintf(output, "type %s interface %s\n", name, typeString(underlying))
	default:
		fmt.Fprintf(output, "type %s %s\n", name, typeString(underlying))
	}
	for index := 0; index < named.NumMethods(); index++ {
		method := named.Method(index)
		if !method.Exported() {
			continue
		}
		receiver := name
		if signature, ok := method.Type().(*types.Signature); ok {
			if _, pointer := signature.Recv().Type().(*types.Pointer); pointer {
				receiver = "*" + receiver
			}
		}
		fmt.Fprintf(output, "method %s.%s %s\n", receiver, method.Name(), typeString(method.Type()))
	}
}

func typeString(value types.Type) string {
	return types.TypeString(value, func(pkg *types.Package) string { return pkg.Path() })
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "Floret API baseline: "+format+"\n", args...)
	os.Exit(1)
}
