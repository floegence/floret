package memory

import "github.com/floegence/floret/session"

type Manager struct {
	SystemPrompt string
	MaxMessages  int
	Compactions  int
}

func (m *Manager) Assemble(history []session.Message) []session.Message {
	out := make([]session.Message, 0, len(history)+1)
	if m.SystemPrompt != "" {
		out = append(out, session.Message{Role: session.System, Content: m.SystemPrompt})
	}
	if m.MaxMessages <= 0 || len(history) <= m.MaxMessages {
		return append(out, history...)
	}
	return append(out, preserveStableSystemMessages(history, preserveTailPairs(history, m.MaxMessages))...)
}

func (m *Manager) Compact(history []session.Message) []session.Message {
	m.Compactions++
	if len(history) == 0 {
		return history
	}
	keep := m.MaxMessages
	if keep <= 0 || keep > len(history) {
		keep = len(history)
	}
	tail := preserveTailPairs(history, keep)
	summary := session.Message{
		Role:    session.System,
		Content: "Previous conversation was compacted. Preserve task goals, constraints, tool results, and unresolved user intent.",
	}
	return append([]session.Message{summary}, tail...)
}

func preserveTailPairs(history []session.Message, max int) []session.Message {
	if max <= 0 || len(history) <= max {
		return append([]session.Message(nil), history...)
	}
	start := len(history) - max
	for start > 0 && history[start].Role == session.Tool && history[start].ToolCallID != "" {
		start--
	}
	return append([]session.Message(nil), history[start:]...)
}

func preserveStableSystemMessages(history, tail []session.Message) []session.Message {
	if len(history) == 0 || len(tail) == 0 {
		return tail
	}
	out := make([]session.Message, 0, len(tail)+1)
	tailStart := len(history) - len(tail)
	for i, msg := range history {
		if i >= tailStart {
			break
		}
		if msg.Role == session.System {
			out = append(out, msg)
		}
	}
	return append(out, tail...)
}
