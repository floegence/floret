package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"

	"github.com/floegence/floret/observation"
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

// Run exists only in package tests. Production callers must supply Floret's
// durable effect dispatcher through Dispatch.
func (r *Registry) Run(ctx context.Context, call ToolCall, approver Approver) Result {
	return runWithOptionsForTest(r, ctx, call, approver, DispatchOptions{})
}

// RunBatch exists only in package tests. It is not part of the public API.
func (r *Registry) RunBatch(ctx context.Context, calls []ToolCall, approver Approver) []Result {
	results := make([]Result, len(calls))
	var wg sync.WaitGroup
	for index := range calls {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index] = runWithOptionsForTest(r, ctx, calls[index], approver, DispatchOptions{
				BatchIndex: index,
				BatchSize:  len(calls),
			})
		}(index)
	}
	wg.Wait()
	return results
}

func runWithOptionsForTest(r *Registry, ctx context.Context, call ToolCall, approver Approver, opts DispatchOptions) Result {
	opts.EffectDispatcher = r.testEffectDispatcher(approver)
	return r.Dispatch(ctx, call, opts)
}

func (r *Registry) testEffectDispatcher(approver Approver) EffectDispatcher {
	return func(ctx context.Context, req EffectDispatchRequest, invoke func() Result) Result {
		r.mu.RLock()
		tool, ok := r.tools[req.Name]
		r.mu.RUnlock()
		if !ok {
			return ErrorResult(req.CallID, req.Name, "unknown tool")
		}
		args, err := tool.decode([]byte(req.RawArgs))
		if err != nil {
			return ErrorResult(req.CallID, req.Name, err.Error())
		}
		call := ToolCall{ID: req.CallID, Name: req.Name, Args: req.RawArgs}
		opts := DispatchOptions{
			RunID: req.RunID, ThreadID: req.ThreadID, TurnID: req.TurnID,
			PromptScopeID: req.PromptScopeID, Step: req.Step,
			BatchIndex: req.BatchIndex, BatchSize: req.BatchSize,
			Labels: cloneStringMap(req.Labels), HostContext: cloneStringMap(req.HostContext),
		}
		if denied := r.permissionDenied(ctx, tool.Definition, req.Permission, call, args, req.Resources, opts, approver); denied != "" {
			return ErrorResult(req.CallID, req.Name, denied)
		}
		return invoke()
	}
}

func (r *Registry) permissionDenied(ctx context.Context, def Definition, permission PermissionSpec, call ToolCall, args any, resources []ResourceRef, opts DispatchOptions, approver Approver) string {
	switch permission.Mode {
	case PermissionDeny:
		return ErrRejected.Error()
	case PermissionAllow:
		return ""
	case PermissionAsk:
		if approver == nil {
			return ErrRejected.Error()
		}
		decision, err := approver(ctx, ApprovalRequest{
			ApprovalID: approvalID(call), ID: call.ID, Name: call.Name, Args: call.Args,
			ArgsHash: stableApprovalArgsHash(call.Args), ValidatedArgs: args,
			Activity: activityForApprovalRequest(def, call, args, opts),
			RunID:    opts.RunID, ThreadID: opts.ThreadID, TurnID: opts.TurnID, PromptScopeID: opts.PromptScopeID,
			Step: opts.Step, BatchIndex: opts.BatchIndex, BatchSize: opts.BatchSize,
			Resources: resources, Effects: append([]Effect(nil), def.Effects...),
			Labels: cloneStringMap(opts.Labels), HostContext: cloneStringMap(opts.HostContext),
			ReadOnly: def.ReadOnly, Destructive: def.Destructive, OpenWorld: def.OpenWorld,
		})
		if err != nil {
			return err.Error()
		}
		if !decision.Allowed() {
			if reason := strings.TrimSpace(decision.RejectionReason()); reason != "" {
				return reason
			}
			return ErrRejected.Error()
		}
	default:
		return ErrRejected.Error()
	}
	return ""
}

func activityForApprovalRequest(def Definition, call ToolCall, args any, opts DispatchOptions) *observation.ActivityPresentation {
	if def.Activity == nil {
		return nil
	}
	activity, err := def.Activity(Invocation[any]{
		CallID: call.ID, Name: call.Name, RawArgs: strings.TrimSpace(call.Args), Args: args,
		RunID: opts.RunID, ThreadID: opts.ThreadID, TurnID: opts.TurnID,
		PromptScopeID: opts.PromptScopeID, Step: opts.Step,
		Labels: cloneStringMap(opts.Labels), HostContext: cloneStringMap(opts.HostContext),
	})
	if err != nil {
		return nil
	}
	return activity
}

func approvalID(call ToolCall) string {
	if id := strings.TrimSpace(call.ID); id != "" {
		return id
	}
	if name := strings.TrimSpace(call.Name); name != "" {
		return name
	}
	return "tool"
}

func stableApprovalArgsHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
