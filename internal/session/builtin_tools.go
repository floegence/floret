package session

// BuiltinTool records engine ownership independently of a host-selected name.
// Empty means a released pre-marker record, whose original built-in name is
// authoritative. This historical interpretation never installs a tool alias.
type BuiltinTool string

const (
	BuiltinAskUser BuiltinTool = "ask_user"
	BuiltinGoal    BuiltinTool = "goal"
)

func IsGoalTool(message Message) bool {
	return message.BuiltinTool == BuiltinGoal || message.BuiltinTool == "" && message.ToolName == "goal"
}
func IsAskUserSignal(signal *ControlSignalView) bool {
	return signal != nil && (signal.Builtin == BuiltinAskUser || signal.Builtin == "" && signal.Name == "ask_user")
}
func IsAskUserTool(message Message) bool {
	return IsAskUserSignal(message.ControlSignal) || message.BuiltinTool == "" && message.ToolName == "ask_user"
}
