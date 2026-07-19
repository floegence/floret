// Package tooltest provides repository-internal helpers for unit-testing local
// tool handlers without exposing a production authority bypass from package tools.
package tooltest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"

	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

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
	BatchIndex    int
	BatchSize     int
	Resources     []tools.ResourceRef
	Effects       []tools.Effect
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

// Run executes one registered handler under an explicitly test-only dispatcher.
func Run(ctx context.Context, registry *tools.Registry, call tools.ToolCall, approver Approver) tools.Result {
	opts := tools.DispatchOptions{}
	opts.EffectDispatcher = Dispatcher(approver)
	return registry.Dispatch(ctx, call, opts)
}

// RunBatch executes registered handlers under an explicitly test-only dispatcher.
func RunBatch(ctx context.Context, registry *tools.Registry, calls []tools.ToolCall, approver Approver) []tools.Result {
	results := make([]tools.Result, len(calls))
	var wg sync.WaitGroup
	for index := range calls {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			opts := tools.DispatchOptions{
				BatchIndex: index,
				BatchSize:  len(calls),
			}
			opts.EffectDispatcher = Dispatcher(approver)
			results[index] = registry.Dispatch(ctx, calls[index], opts)
		}(index)
	}
	wg.Wait()
	return results
}

// Dispatcher returns an in-memory authorization adapter for repository tests.
// It must not be used by production packages.
func Dispatcher(approver Approver) tools.EffectDispatcher {
	return func(ctx context.Context, req tools.EffectDispatchRequest, invoke func() tools.Result) tools.Result {
		switch req.Permission.Mode {
		case tools.PermissionDeny:
			return tools.ErrorResult(req.CallID, req.Name, tools.ErrRejected.Error())
		case tools.PermissionAsk:
			if approver == nil {
				return tools.ErrorResult(req.CallID, req.Name, tools.ErrRejected.Error())
			}
			decision, err := approver(ctx, ApprovalRequest{
				ApprovalID:    req.CallID,
				ID:            req.CallID,
				Name:          req.Name,
				Args:          strings.TrimSpace(req.RawArgs),
				ArgsHash:      stableArgsHash(req.RawArgs),
				RunID:         req.RunID,
				ThreadID:      req.ThreadID,
				TurnID:        req.TurnID,
				PromptScopeID: req.PromptScopeID,
				Step:          req.Step,
				BatchIndex:    req.BatchIndex,
				BatchSize:     req.BatchSize,
				Resources:     append([]tools.ResourceRef(nil), req.Resources...),
				Effects:       append([]tools.Effect(nil), req.Effects...),
				Labels:        cloneStrings(req.Labels),
				HostContext:   cloneStrings(req.HostContext),
				ReadOnly:      req.ReadOnly,
				Destructive:   req.Destructive,
				OpenWorld:     req.OpenWorld,
			})
			if err != nil {
				return tools.ErrorResult(req.CallID, req.Name, err.Error())
			}
			if !decision.Allowed() {
				reason := strings.TrimSpace(decision.RejectionReason())
				if reason == "" {
					reason = tools.ErrRejected.Error()
				}
				return tools.ErrorResult(req.CallID, req.Name, reason)
			}
		}
		return invoke()
	}
}

func stableArgsHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])
}

func cloneStrings(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
