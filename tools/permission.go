package tools

import "context"

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

type ResourceRef struct {
	Kind  string
	Value string
}

type ApprovalRequest struct {
	ID            string
	Name          string
	Args          string
	ValidatedArgs any
	Resources     []ResourceRef
	Effects       []Effect
	ReadOnly      bool
	Destructive   bool
	OpenWorld     bool
	CWD           string
}

type PermissionDecision string

const (
	PermissionDecisionAllow PermissionDecision = "allow"
	PermissionDecisionDeny  PermissionDecision = "deny"
)

type Approver func(context.Context, ApprovalRequest) (PermissionDecision, error)
