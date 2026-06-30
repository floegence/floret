package tools

import (
	"context"

	"github.com/floegence/floret/observation"
)

type ToolCall struct {
	ID        string
	Name      string
	Args      string
	Reasoning string
}

type ToolDefinition struct {
	Name         string
	Title        string
	Description  string
	InputSchema  map[string]any
	OutputSchema map[string]any
	Strict       bool
	Annotations  map[string]any
}

type ArtifactRef struct {
	ID        string `json:"id,omitempty"`
	SafeLabel string `json:"safe_label,omitempty"`
	URL       string `json:"url,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
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

type ArtifactStore interface {
	PutToolOutput(context.Context, ToolOutputArtifact) (ArtifactRef, error)
}

type Invocation[T any] struct {
	CallID          string
	Name            string
	RawArgs         string
	Args            T
	RunID           string
	ThreadID        string
	TurnID          string
	PromptScopeID   string
	Step            int
	Labels          map[string]string
	HostContext     map[string]string
	ActivityUpdater func(ActivityUpdate)
}

type ActivityUpdate struct {
	Activity *observation.ActivityPresentation
	Metadata map[string]any
}

func (i Invocation[T]) UpdateActivity(update ActivityUpdate) {
	if i.ActivityUpdater == nil {
		return
	}
	i.ActivityUpdater(update)
}

type Result struct {
	CallID       string
	Name         string
	Title        string
	Text         string
	Structured   map[string]any
	Metadata     map[string]any
	Activity     *observation.ActivityPresentation
	Artifacts    []ArtifactRef
	OutputPolicy *OutputPolicy
	Pending      *PendingToolResult
	IsError      bool
}

func ErrorResult(callID, name, text string) Result {
	return Result{CallID: callID, Name: name, Text: text, IsError: true}
}

func (r Result) withCall(callID, name string) Result {
	if r.CallID == "" {
		r.CallID = callID
	}
	if r.Name == "" {
		r.Name = name
	}
	return r
}
