package control

import (
	"strings"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
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

func ProjectMessage(msg session.Message) (session.Message, bool) {
	if msg.Role != session.Assistant {
		return msg, false
	}
	switch msg.ToolName {
	case AskUserTool:
		return session.Message{
			Role:          session.Assistant,
			Content:       "Agent requested user input: " + msg.ToolArgs,
			EntryID:       msg.EntryID,
			ParentEntryID: msg.ParentEntryID,
			Kind:          session.MessageKindControlSignal,
		}, true
	case TaskCompleteTool:
		return session.Message{
			Role:          session.Assistant,
			Content:       "Agent completed the task: " + msg.ToolArgs,
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
		if strings.TrimSpace(call.Name) == TaskCompleteTool {
			return Signal{Kind: SignalTaskComplete, Output: call.Args}, true
		}
	}
	return Signal{}, false
}

func AskUser(calls []provider.ToolCall) (Signal, bool) {
	for _, call := range calls {
		if strings.TrimSpace(call.Name) == AskUserTool {
			return Signal{Kind: SignalAskUser, Prompt: call.Args}, true
		}
	}
	return Signal{}, false
}
