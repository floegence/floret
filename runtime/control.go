package runtime

import (
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/internal/control"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/tools"
)

const (
	CoreControlAskUser = tools.ControlAskUser
)

// CoreControlDefinitions returns Floret's product-neutral ask-user control tool.
func CoreControlDefinitions() []tools.ToolDefinition {
	defs := control.ToolDefinitions()
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

// ProjectCoreControlSignal projects ask_user calls into Floret control signals.
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
		payload := cloneAnyMap(signal.Payload)
		if payload == nil {
			payload = map[string]any{}
		}
		payload["question"] = strings.TrimSpace(signal.Prompt)
		return TurnSignal{
			Disposition: SignalWaiting,
			Name:        control.AskUserTool,
			CallID:      call.ID,
			OutputText:  strings.TrimSpace(signal.Prompt),
			Payload:     payload,
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
	default:
		if text != "" {
			return fmt.Sprintf("Agent control signal %q: %s", signal.Name, text)
		}
		return fmt.Sprintf("Agent control signal %q was emitted.", signal.Name)
	}
}
