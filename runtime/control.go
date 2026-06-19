package runtime

import (
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/control"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/tools"
)

const (
	CoreControlAskUser      = tools.ControlAskUser
	CoreControlTaskComplete = tools.ControlTaskComplete
)

// CoreControlDefinitions returns product-neutral control signal tools for hosts
// that want Floret to own common ask-user/task-complete schema validation.
func CoreControlDefinitions(includeTaskComplete bool) []tools.ToolDefinition {
	defs := control.ToolDefinitions(includeTaskComplete)
	out := make([]tools.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, tools.ToolDefinition{
			Name:         def.Name,
			Title:        def.Title,
			Description:  def.Description,
			InputSchema:  cloneAnyMap(def.InputSchema),
			OutputSchema: cloneAnyMap(def.OutputSchema),
			Strict:       def.Strict,
			Annotations:  cloneAnyMap(def.Annotations),
		})
	}
	return out
}

// ProjectCoreControlSignal projects ask_user/task_complete tool calls into
// Floret control signals. Host-specific modes and UI payloads stay outside this
// helper.
func ProjectCoreControlSignal(call tools.ToolCall) (TurnSignal, bool, error) {
	signal, ok, err := control.Project(provider.ToolCall{
		ID:        call.ID,
		Name:      call.Name,
		Args:      call.Args,
		Reasoning: call.Reasoning,
	})
	if err != nil || !ok {
		return TurnSignal{}, ok, err
	}
	switch signal.Kind {
	case control.SignalAskUser:
		return TurnSignal{
			Disposition: SignalWaiting,
			Name:        control.AskUserTool,
			CallID:      call.ID,
			OutputText:  strings.TrimSpace(signal.Prompt),
			Payload:     map[string]any{"question": strings.TrimSpace(signal.Prompt)},
		}, true, nil
	case control.SignalTaskComplete:
		return TurnSignal{
			Disposition: SignalTerminal,
			Name:        control.TaskCompleteTool,
			CallID:      call.ID,
			OutputText:  strings.TrimSpace(signal.Output),
			Payload:     map[string]any{"output": strings.TrimSpace(signal.Output)},
		}, true, nil
	default:
		return TurnSignal{}, false, nil
	}
}

// ProviderSafeCoreControlText returns provider-visible transcript text for
// product-neutral core control signals.
func ProviderSafeCoreControlText(signal TurnSignal) string {
	text := strings.TrimSpace(signal.OutputText)
	switch strings.TrimSpace(signal.Name) {
	case control.AskUserTool:
		if text != "" {
			return "Agent requested user input: " + text
		}
		return "Agent requested user input."
	case control.TaskCompleteTool:
		if text != "" {
			return "Agent completed the task: " + text
		}
		return "Agent completed the task."
	default:
		if text != "" {
			return fmt.Sprintf("Agent control signal %q: %s", signal.Name, text)
		}
		return fmt.Sprintf("Agent control signal %q was emitted.", signal.Name)
	}
}
