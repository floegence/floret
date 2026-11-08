package runtime

import (
	"fmt"
	"regexp"
)

// BuiltinToolNames selects the callable names of engine-owned tools. Empty
// fields retain the published defaults. This does not register execution aliases
// or rewrite historical calls, results, provider state, or user content.
type BuiltinToolNames struct {
	AskUser string
	Goal    string
}

// WithAgentBuiltinToolNames configures the names of the built-in input control
// and optional Goal tool. Names must be distinct ASCII identifiers compatible
// with the common function-tool transports, with at most 64 characters.
func WithAgentBuiltinToolNames(names BuiltinToolNames) AgentOption {
	return AgentOption{category: "builtin_tool_names", apply: func(builder *agentBuilder) error {
		if names.AskUser == "" {
			names.AskUser = "ask_user"
		}
		if names.Goal == "" {
			names.Goal = "goal"
		}
		valid := regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
		if !valid.MatchString(names.AskUser) || !valid.MatchString(names.Goal) || names.AskUser == names.Goal || names.AskUser == "goal" || names.Goal == "ask_user" {
			return fmt.Errorf("built-in tool names must be distinct ASCII identifiers of 1 to 64 characters")
		}
		builder.agent.builtinToolNames = names
		return nil
	}}
}
