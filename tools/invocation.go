package tools

import "github.com/floegence/floret/v7/identity"

type ToolCall struct {
	ID        string
	Name      string
	Args      string
	Reasoning string
}

type ToolDefinition struct {
	Name         string         `json:"name"`
	Title        string         `json:"title,omitempty"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"input_schema"`
	OutputSchema map[string]any `json:"output_schema,omitempty"`
	Strict       bool           `json:"strict,omitempty"`
	Annotations  map[string]any `json:"annotations,omitempty"`
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
	RunID           identity.RunID
	ThreadID        identity.ThreadID
	TurnID          identity.TurnID
	PromptScopeID   identity.PromptScopeID
	Step            int
	Labels          map[string]string
	HostContext     map[string]string
	ActivityUpdater func(ActivityUpdate)
}

type ActivityUpdate struct {
	Activity *ActivityPresentation
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
	Activity     *ActivityPresentation
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

const ResultOutcomeDeclined = "declined"

// DeclinedResult reports a normal user decision that prevented execution. It
// is provider-visible, but it is not a tool dispatch or execution failure.
func DeclinedResult(callID, name string) Result {
	return Result{
		CallID: callID,
		Name:   name,
		Text:   "The user declined this tool call. The tool was not executed.",
		Structured: map[string]any{
			"outcome":         ResultOutcomeDeclined,
			"decision_source": "user",
			"executed":        false,
		},
		Metadata: map[string]any{
			"outcome":         ResultOutcomeDeclined,
			"decision_source": "user",
			"executed":        false,
		},
	}
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
