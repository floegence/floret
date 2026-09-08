package runtime

import (
	"context"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func TestAgentHostedToolsFreezeAndDetach(t *testing.T) {
	definitions := []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search", Options: map[string]any{"nested": []string{"original"}}}}
	builder := agentBuilder{agent: &Agent{}}
	if err := WithAgentHostedTools(definitions...).apply(&builder); err != nil {
		t.Fatal(err)
	}
	definitions[0].Name = "mutated"
	definitions[0].Options["nested"].([]string)[0] = "mutated"
	first, err := builder.agent.toolSurface(context.Background(), ToolSurfaceRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if first.HostedToolDefinitions[0].Name != "web_search" || first.HostedToolDefinitions[0].Options["nested"].([]any)[0] != "original" {
		t.Fatal(first)
	}
	first.HostedToolDefinitions[0].Options["nested"].([]any)[0] = "another mutation"
	second, err := builder.agent.toolSurface(context.Background(), ToolSurfaceRequest{})
	if err != nil || second.HostedToolDefinitions[0].Options["nested"].([]any)[0] != "original" {
		t.Fatal(second, err)
	}
	if WithAgentHostedTools().category != WithAgentDynamicToolSurface(nil).category {
		t.Fatal("static and dynamic surfaces must be mutually exclusive")
	}
	for _, definitions := range [][]provider.HostedToolDefinition{
		{{Name: "", Type: "web_search"}}, {{Name: "a", Type: ""}}, {{Name: "a", Type: "web_search"}, {Name: "a", Type: "web_search"}}, {{Name: "a", Type: "web_search", Options: map[string]any{"invalid": func() {}}}},
	} {
		if err := WithAgentHostedTools(definitions...).apply(&builder); err == nil {
			t.Fatal("accepted invalid hosted tools", definitions)
		}
	}
}
