package webfetch_test

import (
	"testing"

	"github.com/floegence/floret/v7/tools"
	"github.com/floegence/floret/v7/tools/webfetch"
)

func TestPublicConstructorRegistersWithoutHostInternals(t *testing.T) {
	t.Parallel()

	tool := webfetch.New(webfetch.Options{
		Permission: tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"web_url"}},
		PermissionFor: func(tools.PermissionRequest) (tools.PermissionSpec, error) {
			return tools.PermissionSpec{Mode: tools.PermissionDeny}, nil
		},
	})
	if tool.Definition.Name != webfetch.ToolName {
		t.Fatalf("tool name = %q", tool.Definition.Name)
	}
	if err := tools.NewRegistry().Register(tool); err != nil {
		t.Fatalf("register public web fetch tool: %v", err)
	}
}
