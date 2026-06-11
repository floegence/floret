package memory

import "github.com/floegence/floret/internal/session"

type Manager struct {
	SystemPrompt string
}

func (m *Manager) Assemble(history []session.Message) []session.Message {
	out := make([]session.Message, 0, len(history)+1)
	if m.SystemPrompt != "" {
		out = append(out, session.Message{Role: session.System, Content: m.SystemPrompt})
	}
	return append(out, history...)
}
