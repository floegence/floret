package session

import "sync"

type Role string

const (
	System    Role = "system"
	User      Role = "user"
	Assistant Role = "assistant"
	Tool      Role = "tool"
)

type MessageKind string

const (
	MessageKindNormal             MessageKind = ""
	MessageKindCompactionSummary  MessageKind = "compaction_summary"
	MessageKindControlSignal      MessageKind = "control_signal"
	MessageKindMicrocompactMarker MessageKind = "microcompact_marker"
)

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
	s.runs[runID] = append(s.runs[runID], messages...)
	return nil
}

func (s *MemoryStore) Messages(runID string) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.runs[runID]...), nil
}

func (s *MemoryStore) Replace(runID string, messages []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[runID] = append([]Message(nil), messages...)
	return nil
}
