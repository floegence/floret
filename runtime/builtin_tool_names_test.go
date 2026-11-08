package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestBuiltinToolNamesValidationAndOwnership(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "test", StateCompatibilityKey: "test"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported})
	for _, names := range []BuiltinToolNames{{AskUser: "user.ask"}, {Goal: "goal manage"}, {Goal: strings.Repeat("a", 65)}, {AskUser: "same", Goal: "same"}, {AskUser: "goal"}, {Goal: "ask_user"}} {
		if _, err := testAgent(gateway, WithAgentBuiltinToolNames(names)); err == nil {
			t.Fatalf("accepted invalid names: %+v", names)
		}
	}
	for _, name := range []string{"user_ask", "goal_manage", "goal"} {
		hostTool := tools.Define[map[string]any](tools.Definition{Name: name, ReadOnly: true, InputSchema: tools.StrictObject(nil, nil)}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			return tools.Result{}, nil
		})
		if _, err := testAgent(gateway, WithAgentGoals(), WithAgentBuiltinToolNames(BuiltinToolNames{AskUser: "user_ask", Goal: "goal_manage"}), WithAgentTools(hostTool)); err == nil {
			t.Fatalf("host replaced %s", name)
		}
	}
	if _, err := testAgent(gateway, WithAgentBuiltinToolNames(BuiltinToolNames{})); err != nil {
		t.Fatal(err)
	}
}
