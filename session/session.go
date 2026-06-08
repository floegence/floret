package session

import (
	"sync"

	"github.com/floegence/floret/session/artifact"
)

type Role string

const (
	System    Role = "system"
	User      Role = "user"
	Assistant Role = "assistant"
	Tool      Role = "tool"
)

type MessageKind string

const (
	MessageKindNormal            MessageKind = ""
	MessageKindCompactionSummary MessageKind = "compaction_summary"
	MessageKindControlSignal     MessageKind = "control_signal"
)

type ToolResultView struct {
	Truncated     bool          `json:"truncated,omitempty"`
	OriginalBytes int           `json:"original_bytes,omitempty"`
	VisibleBytes  int           `json:"visible_bytes,omitempty"`
	OriginalLines int           `json:"original_lines,omitempty"`
	VisibleLines  int           `json:"visible_lines,omitempty"`
	Strategy      string        `json:"strategy,omitempty"`
	ContentSHA256 string        `json:"content_sha256,omitempty"`
	FullOutput    *artifact.Ref `json:"full_output,omitempty"`
}

type Message struct {
	Role                 Role
	Content              string
	Reasoning            string
	ToolCallID           string
	ToolName             string
	ToolArgs             string
	EntryID              string
	ParentEntryID        string
	Kind                 MessageKind
	ToolResult           *ToolResultView
	CompactionID         string
	CompactionGeneration int
	CompactionWindowID   string
}

type Store interface {
	Append(runID string, messages ...Message) error
	Messages(runID string) ([]Message, error)
	Replace(runID string, messages []Message) error
}

type MemoryStore struct {
	mu   sync.Mutex
	runs map[string][]Message
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: map[string][]Message{}}
}

func (s *MemoryStore) Append(runID string, messages ...Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, msg := range messages {
		s.runs[runID] = append(s.runs[runID], CloneMessage(msg))
	}
	return nil
}

func (s *MemoryStore) Messages(runID string) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return CloneMessages(s.runs[runID]), nil
}

func (s *MemoryStore) Replace(runID string, messages []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[runID] = CloneMessages(messages)
	return nil
}

func CloneMessages(messages []Message) []Message {
	if messages == nil {
		return nil
	}
	out := make([]Message, len(messages))
	for i, msg := range messages {
		out[i] = CloneMessage(msg)
	}
	return out
}

func CloneMessage(msg Message) Message {
	if msg.ToolResult != nil {
		view := *msg.ToolResult
		if view.FullOutput != nil {
			ref := *view.FullOutput
			view.FullOutput = &ref
		}
		msg.ToolResult = &view
	}
	return msg
}
