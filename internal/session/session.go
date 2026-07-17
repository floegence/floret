package session

import (
	"sync"

	"github.com/floegence/floret/internal/session/artifact"
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
	Status        string        `json:"status,omitempty"`
	Truncated     bool          `json:"truncated,omitempty"`
	OriginalBytes int           `json:"original_bytes,omitempty"`
	VisibleBytes  int           `json:"visible_bytes,omitempty"`
	OriginalLines int           `json:"original_lines,omitempty"`
	VisibleLines  int           `json:"visible_lines,omitempty"`
	Strategy      string        `json:"strategy,omitempty"`
	ContentSHA256 string        `json:"content_sha256,omitempty"`
	FullOutput    *artifact.Ref `json:"full_output,omitempty"`
}

type ActivityChip struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	Value string `json:"value,omitempty"`
	Tone  string `json:"tone,omitempty"`
}

type ActivityTargetRef struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	URI   string `json:"uri,omitempty"`
	Path  string `json:"path,omitempty"`
	Line  int    `json:"line,omitempty"`
}

type ActivityPresentation struct {
	Label       string              `json:"label,omitempty"`
	Description string              `json:"description,omitempty"`
	Renderer    string              `json:"renderer,omitempty"`
	Chips       []ActivityChip      `json:"chips,omitempty"`
	TargetRefs  []ActivityTargetRef `json:"target_refs,omitempty"`
	Payload     map[string]any      `json:"payload,omitempty"`
}

type ControlSignalView struct {
	Name        string         `json:"name,omitempty"`
	CallID      string         `json:"call_id,omitempty"`
	Disposition string         `json:"disposition,omitempty"`
	OutputText  string         `json:"output_text,omitempty"`
	ArgsHash    string         `json:"args_hash,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
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
	Activity             *ActivityPresentation `json:"activity,omitempty"`
	ControlSignal        *ControlSignalView    `json:"control_signal,omitempty"`
	CompactionID         string
	CompactionGeneration int
	CompactionWindowID   string
}

type TranscriptStore interface {
	AppendTranscript(runID string, messages ...Message) error
	Transcript(runID string) ([]Message, error)
	ReplaceTranscript(runID string, messages []Message) error
}

type MemoryStore struct {
	mu   sync.Mutex
	runs map[string][]Message
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: map[string][]Message{}}
}

func (s *MemoryStore) AppendTranscript(runID string, messages ...Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, msg := range messages {
		s.runs[runID] = append(s.runs[runID], CloneMessage(msg))
	}
	return nil
}

func (s *MemoryStore) Transcript(runID string) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return CloneMessages(s.runs[runID]), nil
}

func (s *MemoryStore) ReplaceTranscript(runID string, messages []Message) error {
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
	msg.Activity = CloneActivityPresentation(msg.Activity)
	msg.ControlSignal = CloneControlSignalView(msg.ControlSignal)
	return msg
}

func CloneControlSignalView(in *ControlSignalView) *ControlSignalView {
	if in == nil {
		return nil
	}
	return &ControlSignalView{
		Name:        in.Name,
		CallID:      in.CallID,
		Disposition: in.Disposition,
		OutputText:  in.OutputText,
		ArgsHash:    in.ArgsHash,
		Payload:     cloneActivityPayload(in.Payload),
	}
}

func CloneActivityPresentation(in *ActivityPresentation) *ActivityPresentation {
	if in == nil {
		return nil
	}
	return &ActivityPresentation{
		Label:       in.Label,
		Description: in.Description,
		Renderer:    in.Renderer,
		Chips:       append([]ActivityChip(nil), in.Chips...),
		TargetRefs:  append([]ActivityTargetRef(nil), in.TargetRefs...),
		Payload:     cloneActivityPayload(in.Payload),
	}
}

func cloneActivityPayload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneActivityPayloadValue(value)
	}
	return out
}

func cloneActivityPayloadValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneActivityPayload(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneActivityPayloadValue(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return typed
	}
}
