package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

const (
	DefaultKind              = "tool_output"
	DefaultMIME              = "text/plain; charset=utf-8"
	DefaultSafeLabelMaxChars = 80
)

type Ref struct {
	ID        string `json:"id,omitempty"`
	SafeLabel string `json:"safe_label,omitempty"`
	URL       string `json:"url,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

type Store interface {
	PutToolOutput(context.Context, ToolOutputArtifact) (Ref, error)
	DeleteThreadArtifacts(context.Context, string) error
}

type ToolOutputArtifact struct {
	RunID         string         `json:"run_id,omitempty"`
	ThreadID      string         `json:"thread_id,omitempty"`
	TurnID        string         `json:"turn_id,omitempty"`
	PromptScopeID string         `json:"prompt_scope_id,omitempty"`
	Step          int            `json:"step,omitempty"`
	CallID        string         `json:"call_id,omitempty"`
	ToolName      string         `json:"tool_name,omitempty"`
	Text          string         `json:"text,omitempty"`
	MIME          string         `json:"mime,omitempty"`
	Kind          string         `json:"kind,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type MemoryStore struct {
	mu    sync.Mutex
	items map[string]ToolOutputArtifact
	refs  map[string]Ref
	seq   int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		items: map[string]ToolOutputArtifact{},
		refs:  map[string]Ref{},
	}
}

func (s *MemoryStore) PutToolOutput(_ context.Context, output ToolOutputArtifact) (Ref, error) {
	if s == nil {
		return Ref{}, fmt.Errorf("artifact memory store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if output.MIME == "" {
		output.MIME = DefaultMIME
	}
	if output.Kind == "" {
		output.Kind = DefaultKind
	}
	sum := sha256.Sum256([]byte(output.Text))
	hash := hex.EncodeToString(sum[:])
	s.seq++
	id := fmt.Sprintf("%s-%06d-%s", SafeLabel(output.ToolName, 32), s.seq, hash[:12])
	label := SafeLabel(fmt.Sprintf("%s-output-%06d.log", output.ToolName, s.seq), DefaultSafeLabelMaxChars)
	ref := Ref{
		ID:        id,
		SafeLabel: label,
		URL:       "/api/artifacts/" + id,
		Kind:      output.Kind,
		MIME:      output.MIME,
		SizeBytes: int64(len(output.Text)),
		SHA256:    hash,
	}
	s.items[id] = cloneToolOutputArtifact(output)
	s.refs[id] = ref
	return ref, nil
}

func (s *MemoryStore) DeleteThreadArtifacts(_ context.Context, threadID string) error {
	if s == nil {
		return fmt.Errorf("artifact memory store is nil")
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return fmt.Errorf("thread id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, item := range s.items {
		if item.ThreadID == threadID {
			delete(s.items, id)
			delete(s.refs, id)
		}
	}
	return nil
}

func (s *MemoryStore) Ref(id string) (Ref, bool) {
	if s == nil {
		return Ref{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.refs[id]
	return ref, ok
}

func (s *MemoryStore) Text(id string) (string, bool) {
	if s == nil {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	return item.Text, ok
}

var unsafeLabelChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func SafeLabel(value string, maxChars int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "artifact"
	}
	value = unsafeLabelChars.ReplaceAllString(value, "-")
	value = strings.Trim(value, ".-_")
	if value == "" {
		value = "artifact"
	}
	if maxChars <= 0 {
		maxChars = DefaultSafeLabelMaxChars
	}
	runes := []rune(value)
	if len(runes) <= maxChars {
		return value
	}
	return string(runes[:maxChars])
}

func cloneToolOutputArtifact(in ToolOutputArtifact) ToolOutputArtifact {
	if in.Metadata != nil {
		out := make(map[string]any, len(in.Metadata))
		for key, value := range in.Metadata {
			out[key] = value
		}
		in.Metadata = out
	}
	return in
}
