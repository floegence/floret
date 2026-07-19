package tools

type Effect string

const (
	EffectRead    Effect = "read"
	EffectWrite   Effect = "write"
	EffectShell   Effect = "shell"
	EffectNetwork Effect = "network"
)

type PermissionMode string

const (
	PermissionAllow PermissionMode = "allow"
	PermissionAsk   PermissionMode = "ask"
	PermissionDeny  PermissionMode = "deny"
)

type PermissionSpec struct {
	Mode          PermissionMode
	ResourceKinds []string
}

type PermissionRequest struct {
	CallID        string
	Name          string
	RawArgs       string
	Args          any
	RunID         string
	ThreadID      string
	TurnID        string
	PromptScopeID string
	Step          int
	Labels        map[string]string
	HostContext   map[string]string
}

type PermissionResolver func(PermissionRequest) (PermissionSpec, error)

type ResourceRef struct {
	Kind  string
	Value string
}
