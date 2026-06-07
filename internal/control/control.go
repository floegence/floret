package control

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
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
	Kind   SignalKind
	Prompt string
	Output string
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
			Description: "Signal that the requested task is complete.",
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
		raw, err := validateControlArgs(AskUserTool, call.Args, askUserInputSchema())
		if err != nil {
			return Signal{}, true, err
		}
		var payload struct {
			Question *string `json:"question"`
		}
		if err := decodeControlArgs(raw, &payload); err != nil {
			return Signal{}, true, err
		}
		if payload.Question == nil {
			return Signal{}, true, fmt.Errorf("question is required")
		}
		return Signal{Kind: SignalAskUser, Prompt: *payload.Question}, true, nil
	case TaskCompleteTool:
		raw, err := validateControlArgs(TaskCompleteTool, call.Args, taskCompleteInputSchema())
		if err != nil {
			return Signal{}, true, err
		}
		var payload struct {
			Output *string `json:"output"`
		}
		if err := decodeControlArgs(raw, &payload); err != nil {
			return Signal{}, true, err
		}
		if payload.Output == nil {
			return Signal{}, true, fmt.Errorf("output is required")
		}
		return Signal{Kind: SignalTaskComplete, Output: *payload.Output}, true, nil
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

func validateControlArgs(name, raw string, schema map[string]any) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}
	if _, err := tools.Validate(schema, []byte(raw)); err != nil {
		return "", fmt.Errorf("%s", tools.InvalidArgumentsText(name, err))
	}
	return raw, nil
}

func decodeControlArgs(raw string, target any) error {
	if strings.TrimSpace(raw) == "" {
		raw = "{}"
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("expected exactly one JSON object")
	}
	return nil
}

func askUserInputSchema() map[string]any {
	return tools.StrictObject(map[string]any{
		"question": tools.String("The concise question to ask the user."),
	}, []string{"question"})
}

func taskCompleteInputSchema() map[string]any {
	return tools.StrictObject(map[string]any{
		"output": tools.String("Final answer or completion summary."),
	}, []string{"output"})
}
