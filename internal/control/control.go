package control

import (
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/tools"
)

const (
	AskUserTool      = "ask_user"
	TaskCompleteTool = "task_complete"
)

type SignalKind string

const (
	SignalNone         SignalKind = ""
	SignalAskUser      SignalKind = "ask_user"
	SignalTaskComplete SignalKind = "task_complete"
)

type Signal struct {
	Kind    SignalKind
	Prompt  string
	Output  string
	Payload map[string]any
}

func ToolDefinitions(includeTaskComplete bool) []provider.ToolDefinition {
	defs := []provider.ToolDefinition{{
		Name:        AskUserTool,
		Title:       "Ask user",
		Description: "Ask the user for missing information and wait for their response.",
		InputSchema: askUserInputSchema(),
		Strict:      true,
		Annotations: map[string]any{
			"kind": "control",
		},
	}}
	if includeTaskComplete {
		defs = append(defs, provider.ToolDefinition{
			Name:        TaskCompleteTool,
			Title:       "Task complete",
			Description: "Signal that the requested task is complete. Include output or result when the same assistant response does not already contain the final answer.",
			InputSchema: taskCompleteInputSchema(),
			Strict:      true,
			Annotations: map[string]any{
				"kind": "control",
			},
		})
	}
	return defs
}

func IsControlTool(name string) bool {
	name = strings.TrimSpace(name)
	return name == AskUserTool || name == TaskCompleteTool
}

func Project(call provider.ToolCall) (Signal, bool, error) {
	switch strings.TrimSpace(call.Name) {
	case AskUserTool:
		payload, err := tools.Validate(askUserInputSchema(), []byte(strings.TrimSpace(call.Args)))
		if err != nil {
			return Signal{}, true, fmt.Errorf("%s", tools.InvalidArgumentsText(AskUserTool, err))
		}
		question := firstNonEmptyString(controlString(payload["question"]), firstQuestionText(payload["questions"]))
		if question == "" {
			return Signal{}, true, fmt.Errorf("question or questions[0].question is required")
		}
		return Signal{Kind: SignalAskUser, Prompt: question, Payload: cloneControlMap(payload)}, true, nil
	case TaskCompleteTool:
		payload, err := tools.Validate(taskCompleteInputSchema(), []byte(strings.TrimSpace(call.Args)))
		if err != nil {
			return Signal{}, true, fmt.Errorf("%s", tools.InvalidArgumentsText(TaskCompleteTool, err))
		}
		output := firstNonEmptyString(controlString(payload["output"]), controlString(payload["result"]))
		return Signal{Kind: SignalTaskComplete, Output: output, Payload: cloneControlMap(payload)}, true, nil
	default:
		return Signal{}, false, nil
	}
}

func ProjectMessage(msg session.Message) (session.Message, bool) {
	if msg.Role != session.Assistant {
		return msg, false
	}
	switch msg.ToolName {
	case AskUserTool:
		content := msg.ToolArgs
		if signal, ok, err := Project(provider.ToolCall{Name: msg.ToolName, Args: msg.ToolArgs}); ok && err == nil {
			content = signal.Prompt
		}
		return session.Message{
			Role:          session.Assistant,
			Content:       "Agent requested user input: " + content,
			EntryID:       msg.EntryID,
			ParentEntryID: msg.ParentEntryID,
			Kind:          session.MessageKindControlSignal,
		}, true
	case TaskCompleteTool:
		content := msg.ToolArgs
		if signal, ok, err := Project(provider.ToolCall{Name: msg.ToolName, Args: msg.ToolArgs}); ok && err == nil {
			content = signal.Output
		}
		return session.Message{
			Role:          session.Assistant,
			Content:       "Agent completed the task: " + content,
			EntryID:       msg.EntryID,
			ParentEntryID: msg.ParentEntryID,
			Kind:          session.MessageKindControlSignal,
		}, true
	default:
		return msg, false
	}
}

func ProjectHistory(history []session.Message) []session.Message {
	out := make([]session.Message, 0, len(history))
	for _, msg := range history {
		if projected, ok := ProjectMessage(msg); ok {
			out = append(out, projected)
			continue
		}
		out = append(out, msg)
	}
	return out
}

func Completion(calls []provider.ToolCall) (Signal, bool) {
	for _, call := range calls {
		if signal, ok, err := Project(call); ok && err == nil && signal.Kind == SignalTaskComplete {
			return signal, true
		}
	}
	return Signal{}, false
}

func AskUser(calls []provider.ToolCall) (Signal, bool) {
	for _, call := range calls {
		if signal, ok, err := Project(call); ok && err == nil && signal.Kind == SignalAskUser {
			return signal, true
		}
	}
	return Signal{}, false
}

func askUserInputSchema() map[string]any {
	questionItem := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"question": tools.String("Question text."),
		},
		"required":             []string{"question"},
		"additionalProperties": true,
	}
	return tools.StrictObject(map[string]any{
		"question":           tools.String("The concise question to ask the user."),
		"questions":          tools.Array(questionItem, "Structured user-input questions."),
		"reason_code":        tools.String("Product-neutral reason code for why input is needed."),
		"required_from_user": tools.Array(tools.String("Required user input."), "Concrete user inputs or decisions needed to proceed."),
		"evidence_refs":      tools.Array(tools.String("Evidence reference."), "Relevant evidence references."),
	}, []string{})
}

func taskCompleteInputSchema() map[string]any {
	return tools.StrictObject(map[string]any{
		"output":          tools.String("Final answer or completion summary when not already present in the same assistant response."),
		"result":          tools.String("Final answer or completion summary when not already present in the same assistant response."),
		"evidence_refs":   tools.Array(tools.String("Evidence reference."), "Relevant evidence references."),
		"remaining_risks": tools.Array(tools.String("Remaining risk."), "Remaining risks."),
		"next_actions":    tools.Array(tools.String("Next action."), "Suggested next actions."),
	}, []string{})
}

func controlString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func firstQuestionText(value any) string {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return ""
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		return ""
	}
	return controlString(item["question"])
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func cloneControlMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneControlAny(value)
	}
	return out
}

func cloneControlAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneControlMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneControlAny(item)
		}
		return out
	default:
		return value
	}
}
