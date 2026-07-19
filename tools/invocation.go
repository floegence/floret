package tools

import "github.com/floegence/floret/observation"

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
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
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
	DispatchErr  error

	effectFinalizationRequired bool
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

// RequiresEffectFinalization reports whether the result crossed the effect
// authority dispatcher and therefore requires its atomic result finalizer.
func (r Result) RequiresEffectFinalization() bool {
	return r.effectFinalizationRequired
}
