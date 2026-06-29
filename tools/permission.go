package tools

import (
	"context"

	"github.com/floegence/floret/observation"
)

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

type ApprovalRequest struct {
	ApprovalID    string
	ID            string
	Name          string
	Args          string
	ArgsHash      string
	ValidatedArgs any
	Activity      *observation.ActivityPresentation
	RunID         string
	ThreadID      string
	TurnID        string
	PromptScopeID string
	Step          int
	Resources     []ResourceRef
	Effects       []Effect
	Labels        map[string]string
	HostContext   map[string]string
	ReadOnly      bool
	Destructive   bool
	OpenWorld     bool
}

type PermissionDecisionState string

const (
	PermissionDecisionStateAllow PermissionDecisionState = "allow"
	PermissionDecisionStateDeny  PermissionDecisionState = "deny"
)

type PermissionDecision struct {
	State  PermissionDecisionState
	Reason string
}

var (
	PermissionDecisionAllow = PermissionDecision{State: PermissionDecisionStateAllow}
	PermissionDecisionDeny  = PermissionDecision{State: PermissionDecisionStateDeny}
)

func PermissionDecisionDenied(reason string) PermissionDecision {
	return PermissionDecision{State: PermissionDecisionStateDeny, Reason: reason}
}

func (d PermissionDecision) Allowed() bool {
	return d.State == PermissionDecisionStateAllow
}

func (d PermissionDecision) RejectionReason() string {
	return d.Reason
}

type Approver func(context.Context, ApprovalRequest) (PermissionDecision, error)
