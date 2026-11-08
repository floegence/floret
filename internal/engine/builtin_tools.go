package engine

import (
	"github.com/floegence/floret/v7/internal/control"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

type BuiltinToolNames struct{ AskUser, Goal string }

func (n BuiltinToolNames) InputName() string {
	if n.AskUser == "" {
		return control.AskUserTool
	}
	return n.AskUser
}
func (n BuiltinToolNames) GoalName() string {
	if n.Goal == "" {
		return "goal"
	}
	return n.Goal
}

func NamedControlSpec(name string) ControlSpec {
	spec := DefaultControlSpec()
	if name == "" || name == control.AskUserTool {
		return spec
	}
	spec.inputName = name
	spec.Definitions[0].Name = name
	project := spec.Project
	spec.Project = func(call provider.ToolCall) (ControlSignal, bool, error) {
		if call.Name != name {
			return ControlSignal{}, false, nil
		}
		call.Name = control.AskUserTool
		signal, ok, err := project(call)
		signal.Name = name
		signal.Builtin = session.BuiltinAskUser
		return signal, ok, err
	}
	return spec
}
