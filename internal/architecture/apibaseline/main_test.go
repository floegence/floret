package main

import (
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestAliasIncludesExportedSurface(t *testing.T) {
	pkg := types.NewPackage("example.com/contracts", "contracts")
	field := types.NewField(token.NoPos, pkg, "Stage", types.Typ[types.String], false)
	base := types.NewNamed(types.NewTypeName(token.NoPos, pkg, "Details", nil), types.NewStruct([]*types.Var{field}, []string{`json:"stage"`}), nil)
	receiver := types.NewVar(token.NoPos, pkg, "d", base)
	signature := types.NewSignatureType(receiver, nil, nil, types.NewTuple(), types.NewTuple(), false)
	base.AddMethod(types.NewFunc(token.NoPos, pkg, "Validate", signature))
	alias := types.NewTypeName(token.NoPos, pkg, "PublicDetails", nil)
	types.NewAlias(alias, base)
	var out strings.Builder
	writeType(&out, alias)
	for _, required := range []string{"type PublicDetails =", `field PublicDetails.Stage string tag="json:\"stage\""`, "method PublicDetails.Validate func()"} {
		if !strings.Contains(out.String(), required) {
			t.Fatalf("missing %q in %s", required, out.String())
		}
	}
}
