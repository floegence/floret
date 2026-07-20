package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/tools"
)

var (
	ErrEffectUnauthorized        = errors.New("effect is unauthorized")
	ErrAuthorizationUnavailable  = errors.New("effect authorization is unavailable")
	ErrInvalidAuthorizationProof = errors.New("effect authorization proof is invalid")
	ErrEffectDispatchConsumed    = errors.New("authorized effect dispatch was already consumed")
	ErrAuthorizationContract     = errors.New("effect authorization gate contract failed")
)

type CommittedEffectError struct {
	EffectAttemptID string
	Err             error
}

func (e *CommittedEffectError) Error() string {
	if e == nil || e.Err == nil {
		return "effect handler dispatch committed"
	}
	return "effect handler dispatch committed: " + e.Err.Error()
}

func (e *CommittedEffectError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type EffectAuthorizationRequest struct {
	EffectAttemptID    string
	RequestFingerprint string
	ThreadID           string
	TurnID             string
	RunID              string
	ToolCallID         string
	ToolName           string
	ArgumentHash       string
	Step               int
	BatchIndex         int
	BatchSize          int
	Labels             map[string]string
	HostContext        map[string]string
	Resources          []tools.ResourceRef
	Effects            []tools.Effect
	Permission         tools.PermissionSpec
	ReadOnly           bool
	Destructive        bool
	OpenWorld          bool
	LeaseOwnerID       string
	LeaseGeneration    int64
	ObservedHeartbeat  int64
}

type EffectAuthorizationProof struct {
	EffectAttemptID    string
	RequestFingerprint string
	ThreadID           string
	TurnID             string
	RunID              string
	ToolCallID         string
	LeaseOwnerID       string
	LeaseGeneration    int64
	PolicyRevision     string
	ApprovalID         string
	AuditReference     string
	AuditHash          string
	AuthorizedAt       time.Time
}

type EffectDispatchResult struct {
	seal         string
	finalization engine.EffectResultFinalizationResult
}

type AuthorizedEffect func(EffectAuthorizationProof) (EffectDispatchResult, error)

type EffectAuthorizationGate interface {
	Dispatch(context.Context, EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error)
}

type EffectAuthorizationGateFunc func(context.Context, EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error)

func (f EffectAuthorizationGateFunc) Dispatch(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	return f(ctx, req, effect)
}

type effectGateOutcome struct {
	result EffectDispatchResult
	err    error
}

type effectDispatchPhase uint8

const (
	effectDispatchOpen effectDispatchPhase = iota
	effectDispatchBeginning
	effectDispatchBegun
	effectDispatchNotBegun
	effectDispatchCancelled
)

type effectDispatchBoundary struct {
	mu       sync.Mutex
	phase    effectDispatchPhase
	resolved chan struct{}
	once     sync.Once
}

func newEffectDispatchBoundary() *effectDispatchBoundary {
	return &effectDispatchBoundary{phase: effectDispatchOpen, resolved: make(chan struct{})}
}

func (b *effectDispatchBoundary) begin() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.phase != effectDispatchOpen {
		return false
	}
	b.phase = effectDispatchBeginning
	return true
}

func (b *effectDispatchBoundary) resolve(begun bool) {
	b.mu.Lock()
	if begun {
		b.phase = effectDispatchBegun
	} else {
		b.phase = effectDispatchNotBegun
	}
	b.mu.Unlock()
	b.once.Do(func() { close(b.resolved) })
}

func (b *effectDispatchBoundary) cancelOrObserve() effectDispatchPhase {
	b.mu.Lock()
	if b.phase == effectDispatchOpen {
		b.phase = effectDispatchCancelled
	}
	phase := b.phase
	b.mu.Unlock()
	if phase == effectDispatchCancelled {
		b.once.Do(func() { close(b.resolved) })
	}
	return phase
}

type effectFinalizeRequest struct {
	ctx     context.Context
	request engine.EffectResultFinalizationRequest
}

func effectFinalizerKey(runID, turnID, toolCallID string) string {
	return strings.TrimSpace(runID) + "\x00" + strings.TrimSpace(turnID) + "\x00" + strings.TrimSpace(toolCallID)
}

func (t *Thread) registerEffectFinalizer(key string, finalize func(context.Context, engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error)) error {
	if t == nil || strings.TrimSpace(key) == "" || finalize == nil {
		return ErrAuthorizationContract
	}
	t.effectFinalizeMu.Lock()
	defer t.effectFinalizeMu.Unlock()
	if t.effectFinalizers == nil {
		t.effectFinalizers = make(map[string]func(context.Context, engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error))
	}
	if _, exists := t.effectFinalizers[key]; exists {
		return ErrAuthorizationContract
	}
	t.effectFinalizers[key] = finalize
	return nil
}

func (t *Thread) removeEffectFinalizer(key string) {
	if t == nil {
		return
	}
	t.effectFinalizeMu.Lock()
	delete(t.effectFinalizers, key)
	t.effectFinalizeMu.Unlock()
}

func (t *Thread) finalizeEffectResult(ctx context.Context, req engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
	if t == nil || req.ThreadID != t.id || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.ToolCallID) == "" {
		return engine.EffectResultFinalizationResult{}, ErrAuthorizationContract
	}
	key := effectFinalizerKey(req.RunID, req.TurnID, req.ToolCallID)
	t.effectFinalizeMu.Lock()
	finalize, ok := t.effectFinalizers[key]
	if ok {
		delete(t.effectFinalizers, key)
	}
	t.effectFinalizeMu.Unlock()
	if !ok {
		return engine.EffectResultFinalizationResult{}, ErrEffectDispatchConsumed
	}
	req.Message = session.CloneMessage(req.Message)
	req.FullOutput = cloneEffectFullOutput(req.FullOutput)
	return finalize(ctx, req)
}

func cloneEffectFullOutput(in *artifact.FullOutput) *artifact.FullOutput {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneEffectFinalizationRequest(in engine.EffectResultFinalizationRequest) engine.EffectResultFinalizationRequest {
	in.Message = session.CloneMessage(in.Message)
	in.FullOutput = cloneEffectFullOutput(in.FullOutput)
	return in
}

func (t *Thread) effectDispatcher() tools.EffectDispatcher {
	return func(ctx context.Context, request tools.EffectDispatchRequest, invoke func() tools.Result) tools.Result {
		return t.dispatchAuthorizedEffect(ctx, request, invoke)
	}
}

func (t *Thread) dispatchAuthorizedEffect(ctx context.Context, request tools.EffectDispatchRequest, invoke func() tools.Result) tools.Result {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return effectDispatchError(request.CallID, request.Name, ErrAuthorizationUnavailable)
	}
	lease, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok || lease.ThreadID != t.id || lease.TurnID != request.TurnID || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return effectDispatchError(request.CallID, request.Name, sessiontree.ErrStaleAuthority)
	}
	local, ok := t.ownedActiveTurnLease(request.TurnID)
	if !ok || local.OwnerID != lease.OwnerID || local.Generation != lease.Generation {
		return effectDispatchError(request.CallID, request.Name, sessiontree.ErrStaleAuthority)
	}
	repo, ok := t.harness.options.Repo.(sessiontree.EffectAttemptAuthorityRepo)
	if !ok {
		return effectDispatchError(request.CallID, request.Name, errors.New("session tree repo does not support atomic effect authority"))
	}
	argumentHash := sessiontree.StableHash(strings.TrimSpace(request.RawArgs))
	fingerprint, err := effectRequestFingerprint(request, lease, argumentHash)
	if err != nil {
		return effectDispatchError(request.CallID, request.Name, err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, sessiontree.PrepareEffectAttemptRequest{
		Lease: lease, RequestFingerprint: fingerprint, Now: t.harness.now(),
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID,
			ToolCallID: request.CallID, ToolName: request.Name, ArgumentHash: argumentHash,
		},
	})
	if err != nil {
		return effectDispatchError(request.CallID, request.Name, err)
	}
	if prepared.Replayed && prepared.Attempt.State != sessiontree.EffectAttemptPrepared {
		return t.replayEffectResult(ctx, prepared.Attempt)
	}
	authorizationRequest := EffectAuthorizationRequest{
		EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
		ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.CallID,
		ToolName: request.Name, ArgumentHash: argumentHash, Resources: append([]tools.ResourceRef(nil), request.Resources...),
		Step: request.Step, BatchIndex: request.BatchIndex, BatchSize: request.BatchSize,
		Labels: cloneStringMap(request.Labels), HostContext: cloneStringMap(request.HostContext),
		Effects: append([]tools.Effect(nil), request.Effects...), Permission: request.Permission,
		ReadOnly: request.ReadOnly, Destructive: request.Destructive, OpenWorld: request.OpenWorld,
		LeaseOwnerID: lease.OwnerID, LeaseGeneration: lease.Generation, ObservedHeartbeat: lease.Heartbeat,
	}
	gate := t.harness.options.EffectAuthorizationGate
	if gate == nil {
		return t.rejectEffectAttempt(ctx, repo, lease, prepared.Attempt, fingerprint, ErrAuthorizationUnavailable)
	}
	approvalRequested := request.Permission.Mode == tools.PermissionAsk
	if approvalRequested {
		if err := t.recordEffectApproval(ctx, event.ToolApprovalRequested, authorizationRequest, ""); err != nil {
			return t.rejectEffectAttempt(ctx, repo, lease, prepared.Attempt, fingerprint, err)
		}
	}
	ready := make(chan tools.Result, 1)
	finalize := make(chan effectFinalizeRequest, 1)
	gateDone := make(chan effectGateOutcome, 1)
	dispatchBoundary := newEffectDispatchBoundary()
	var active atomic.Bool
	var consumed atomic.Bool
	active.Store(true)
	seal := "effect-dispatch:" + sessiontree.StableHash(prepared.Attempt.EffectAttemptID+"\x00"+fingerprint)
	go func() {
		releaseAuthority, authorityErr := t.enterProviderRequest(ctx)
		if authorityErr != nil {
			active.Store(false)
			gateDone <- effectGateOutcome{err: authorityErr}
			return
		}
		defer releaseAuthority()
		result, gateErr := gate.Dispatch(ctx, authorizationRequest, func(proof EffectAuthorizationProof) (EffectDispatchResult, error) {
			if !active.Load() || !consumed.CompareAndSwap(false, true) {
				return EffectDispatchResult{}, ErrEffectDispatchConsumed
			}
			if err := validateEffectAuthorizationProof(authorizationRequest, proof); err != nil {
				return EffectDispatchResult{}, err
			}
			if approvalRequested {
				if err := t.recordEffectApproval(ctx, event.ToolApprovalApproved, authorizationRequest, ""); err != nil {
					return EffectDispatchResult{}, err
				}
			}
			current, ok := sessiontree.TurnLeaseFromContext(ctx)
			if !ok || current.OwnerID != lease.OwnerID || current.Generation != lease.Generation {
				return EffectDispatchResult{}, sessiontree.ErrStaleAuthority
			}
			if !dispatchBoundary.begin() {
				return EffectDispatchResult{}, ctx.Err()
			}
			if _, err := repo.BeginEffectDispatch(ctx, sessiontree.BeginEffectDispatchRequest{
				Lease: current, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
				ObservedHeartbeat:      authorizationRequest.ObservedHeartbeat,
				AuthorizationProofHash: sessiontree.StableHash(proof.AuditReference + "\x00" + proof.AuditHash + "\x00" + proof.PolicyRevision), Now: t.harness.now(),
			}); err != nil {
				dispatchBoundary.resolve(false)
				return EffectDispatchResult{}, err
			}
			dispatchBoundary.resolve(true)
			handlerResult := invoke()
			finalizerKey := effectFinalizerKey(request.RunID, request.TurnID, request.CallID)
			if err := t.registerEffectFinalizer(finalizerKey, func(finalizeCtx context.Context, request engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
				finalize <- effectFinalizeRequest{ctx: finalizeCtx, request: cloneEffectFinalizationRequest(request)}
				outcome := <-gateDone
				if outcome.err != nil {
					var committed *CommittedEffectError
					if errors.As(outcome.err, &committed) {
						return engine.EffectResultFinalizationResult{}, outcome.err
					}
					return engine.EffectResultFinalizationResult{}, &CommittedEffectError{EffectAttemptID: prepared.Attempt.EffectAttemptID, Err: outcome.err}
				}
				return outcome.result.finalization, nil
			}); err != nil {
				return EffectDispatchResult{}, err
			}
			defer t.removeEffectFinalizer(finalizerKey)
			ready <- handlerResult
			finalization := <-finalize
			finishCtx, cancelFinish := t.harness.effectFinalizationContext(finalization.ctx)
			current, ok = sessiontree.TurnLeaseFromContext(finishCtx)
			if !ok || current.OwnerID != lease.OwnerID || current.Generation != lease.Generation {
				cancelFinish()
				return EffectDispatchResult{}, sessiontree.ErrStaleAuthority
			}
			outcomeFingerprint, err := effectOutcomeFingerprint(handlerResult, finalization.request.Message, finalization.request.FullOutput)
			if err != nil {
				cancelFinish()
				return EffectDispatchResult{}, err
			}
			finished, err := repo.FinishEffectDispatch(finishCtx, sessiontree.FinishEffectDispatchRequest{
				Lease: current, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
				OutcomeFingerprint: outcomeFingerprint, Failed: handlerResult.IsError || effectMessageFailed(finalization.request.Message), Now: t.harness.now(),
				Result:     sessiontree.Entry{ThreadID: request.ThreadID, TurnID: request.TurnID, Type: sessiontree.EntryToolResult, Message: session.CloneMessage(finalization.request.Message)},
				FullOutput: cloneEffectFullOutput(finalization.request.FullOutput),
			})
			cancelFinish()
			if err != nil {
				unknownFingerprint := sessiontree.StableHash(prepared.Attempt.EffectAttemptID + "\x00unknown\x00" + err.Error())
				unknownCtx, cancelUnknown := t.harness.effectFinalizationContext(finalization.ctx)
				_, markErr := repo.MarkEffectUnknown(unknownCtx, sessiontree.MarkEffectUnknownRequest{
					Lease: current, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
					OutcomeFingerprint: unknownFingerprint, Now: t.harness.now(),
				})
				cancelUnknown()
				if markErr != nil {
					return EffectDispatchResult{}, &CommittedEffectError{EffectAttemptID: prepared.Attempt.EffectAttemptID, Err: errors.Join(err, markErr)}
				}
				return EffectDispatchResult{}, &CommittedEffectError{EffectAttemptID: prepared.Attempt.EffectAttemptID, Err: err}
			}
			committedFinalization, validateErr := validateCommittedEffectFinalization(finalization.request, prepared.Attempt, finished)
			if validateErr != nil {
				return EffectDispatchResult{}, &CommittedEffectError{EffectAttemptID: prepared.Attempt.EffectAttemptID, Err: validateErr}
			}
			entry := finished.Result
			if !finished.Replayed {
				t.harness.emitEntryCommitted(entry, request.RunID)
				t.harness.emit(HarnessEvent{
					Type: EventEntryAppended, RunID: request.RunID, ThreadID: request.ThreadID, TurnID: request.TurnID,
					EntryID: entry.ID, ParentID: entry.ParentID,
				})
			}
			return EffectDispatchResult{seal: seal, finalization: committedFinalization}, nil
		})
		active.Store(false)
		if gateErr == nil && result.seal != seal {
			gateErr = ErrAuthorizationContract
		}
		gateDone <- effectGateOutcome{result: result, err: gateErr}
	}()
	select {
	case handlerResult := <-ready:
		return handlerResult
	case outcome := <-gateDone:
		if outcome.err == nil {
			outcome.err = ErrAuthorizationContract
		}
		if approvalRequested {
			_ = t.recordEffectApproval(ctx, event.ToolApprovalRejected, authorizationRequest, outcome.err.Error())
		}
		return t.rejectEffectAttempt(ctx, repo, lease, prepared.Attempt, fingerprint, outcome.err)
	case <-ctx.Done():
		for {
			switch dispatchBoundary.cancelOrObserve() {
			case effectDispatchBeginning:
				select {
				case <-dispatchBoundary.resolved:
					continue
				case outcome := <-gateDone:
					if outcome.err == nil {
						outcome.err = ErrAuthorizationContract
					}
					return effectDispatchError(request.CallID, request.Name, outcome.err)
				}
			case effectDispatchBegun:
				select {
				case handlerResult := <-ready:
					return handlerResult
				case outcome := <-gateDone:
					if outcome.err == nil {
						outcome.err = ErrAuthorizationContract
					}
					return effectDispatchError(request.CallID, request.Name, outcome.err)
				}
			case effectDispatchOpen:
				continue
			case effectDispatchCancelled, effectDispatchNotBegun:
				active.Store(false)
				persistCtx, cancelPersist := turnFinalizationContext(ctx)
				defer cancelPersist()
				return t.rejectEffectAttempt(persistCtx, repo, lease, prepared.Attempt, fingerprint, ctx.Err())
			default:
				return effectDispatchError(request.CallID, request.Name, ErrAuthorizationContract)
			}
		}
	}
}

func effectMessageFailed(message session.Message) bool {
	return message.ToolResult != nil && strings.EqualFold(strings.TrimSpace(message.ToolResult.Status), "error")
}

func validateCommittedEffectFinalization(req engine.EffectResultFinalizationRequest, prepared sessiontree.EffectAttempt, finished sessiontree.FinishEffectDispatchResult) (engine.EffectResultFinalizationResult, error) {
	requested := sessiontree.Entry{
		ThreadID: req.ThreadID, TurnID: req.TurnID, Type: sessiontree.EntryToolResult,
		Message: session.CloneMessage(req.Message),
	}
	if finished.Attempt.EffectAttemptID != prepared.EffectAttemptID || finished.Attempt.ResultEntryID == "" ||
		finished.Result.ID != finished.Attempt.ResultEntryID ||
		!sessiontree.EffectResultRequestMatches(finished.Result, requested, prepared.EffectAttemptID) {
		return engine.EffectResultFinalizationResult{}, sessiontree.ErrAuthorityCorrupt
	}
	committedRef := finished.Result.Message.ToolResult.FullOutput
	if req.FullOutput == nil {
		if committedRef != nil || finished.Artifact != nil {
			return engine.EffectResultFinalizationResult{}, sessiontree.ErrAuthorityCorrupt
		}
	} else {
		expected, err := artifact.RefForEffect(prepared.EffectAttemptID, prepared.Invocation.ToolName, *req.FullOutput)
		if err != nil {
			return engine.EffectResultFinalizationResult{}, err
		}
		if committedRef == nil || finished.Artifact == nil || *committedRef != expected || *finished.Artifact != expected {
			return engine.EffectResultFinalizationResult{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	return engine.EffectResultFinalizationResult{
		Handled: true, Message: session.CloneMessage(finished.Result.Message), Replayed: finished.Replayed,
	}, nil
}

func (t *Thread) recordEffectApproval(ctx context.Context, typ event.Type, req EffectAuthorizationRequest, reason string) error {
	resources := make([]map[string]string, 0, len(req.Resources))
	for _, resource := range req.Resources {
		resources = append(resources, map[string]string{"kind": resource.Kind, "value": resource.Value})
	}
	effects := make([]string, 0, len(req.Effects))
	for _, effect := range req.Effects {
		effects = append(effects, string(effect))
	}
	ev := event.Event{
		Type: typ, RunID: req.RunID, ThreadID: req.ThreadID, TurnID: req.TurnID, Step: req.Step,
		ToolID: req.ToolCallID, ToolName: req.ToolName, ToolKind: "local", ArgsHash: req.ArgumentHash,
		Err: strings.TrimSpace(reason), Timestamp: t.harness.now(),
		Metadata: map[string]any{
			"approval_id": req.EffectAttemptID, "resources": resources, "effects": effects,
			"read_only": req.ReadOnly, "destructive": req.Destructive, "open_world": req.OpenWorld,
			"batch_index": req.BatchIndex, "batch_size": req.BatchSize,
			"labels": cloneStringMap(req.Labels), "host_context": cloneStringMap(req.HostContext),
		},
	}
	t.harness.updatePendingApproval(ev)
	if t.harness.options.Sink != nil {
		t.harness.options.Sink.Emit(event.SanitizeWithPolicy(ev, t.harness.options.SinkPolicy))
	}
	return t.appendApprovalEvent(ctx, req.TurnID, req.RunID, ev)
}

func (t *Thread) rejectEffectAttempt(ctx context.Context, repo sessiontree.EffectAttemptAuthorityRepo, lease sessiontree.TurnLease, attempt sessiontree.EffectAttempt, requestFingerprint string, cause error) tools.Result {
	code := "authorization_unavailable"
	public := ErrAuthorizationUnavailable
	if errors.Is(cause, ErrEffectUnauthorized) || errors.Is(cause, tools.ErrRejected) {
		code = "unauthorized"
		public = ErrEffectUnauthorized
	}
	rejectionFingerprint := sessiontree.StableHash(strings.Join([]string{attempt.EffectAttemptID, requestFingerprint, code, strings.TrimSpace(cause.Error())}, "\x00"))
	current, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok {
		current = lease
	}
	if _, err := repo.RejectEffectAttempt(ctx, sessiontree.RejectEffectAttemptRequest{
		Lease: current, EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: requestFingerprint,
		RejectionCode: code, RejectionFingerprint: rejectionFingerprint, Now: t.harness.now(),
	}); err != nil {
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, err)
	}
	return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, fmt.Errorf("%w: %v", public, cause))
}

func (t *Thread) replayEffectResult(ctx context.Context, attempt sessiontree.EffectAttempt) tools.Result {
	switch attempt.State {
	case sessiontree.EffectAttemptCompleted, sessiontree.EffectAttemptFailed:
		entries, err := t.harness.options.Repo.Entries(ctx, attempt.Invocation.ThreadID)
		if err != nil {
			return committedEffectDispatchError(attempt, err)
		}
		for _, entry := range entries {
			if entry.ID == attempt.ResultEntryID {
				if err := t.validateReplayedEffectArtifact(ctx, entry); err != nil {
					return committedEffectDispatchError(attempt, err)
				}
				key := effectFinalizerKey(attempt.Invocation.RunID, attempt.Invocation.TurnID, attempt.Invocation.ToolCallID)
				if err := t.registerEffectFinalizer(key, func(_ context.Context, req engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
					if req.RunID != attempt.Invocation.RunID || req.ThreadID != attempt.Invocation.ThreadID || req.TurnID != attempt.Invocation.TurnID || req.ToolCallID != attempt.Invocation.ToolCallID {
						return engine.EffectResultFinalizationResult{}, &CommittedEffectError{EffectAttemptID: attempt.EffectAttemptID, Err: sessiontree.ErrAuthorityCorrupt}
					}
					return engine.EffectResultFinalizationResult{Handled: true, Message: session.CloneMessage(entry.Message), Replayed: true}, nil
				}); err != nil {
					return committedEffectDispatchError(attempt, err)
				}
				text := entry.Message.Content
				if attempt.State == sessiontree.EffectAttemptFailed {
					text = strings.TrimPrefix(text, "ERROR: ")
				}
				return tools.Result{
					CallID: attempt.Invocation.ToolCallID, Name: attempt.Invocation.ToolName, Text: text,
					IsError: attempt.State == sessiontree.EffectAttemptFailed,
					OutputPolicy: &tools.OutputPolicy{
						VisibleMaxBytes: len(text) + 1, VisibleMaxLines: strings.Count(text, "\n") + 2,
						Strategy: tools.OutputStrategy(entry.Message.ToolResult.Strategy), PreserveFullSet: true, PreserveFull: false,
					},
				}
			}
		}
		return committedEffectDispatchError(attempt, sessiontree.ErrAuthorityCorrupt)
	case sessiontree.EffectAttemptRejected:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, ErrEffectUnauthorized)
	case sessiontree.EffectAttemptUnknown, sessiontree.EffectAttemptDispatching:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, sessiontree.ErrEffectOutcomeUnknown)
	default:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, ErrAuthorizationUnavailable)
	}
}

func committedEffectDispatchError(attempt sessiontree.EffectAttempt, err error) tools.Result {
	return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, &CommittedEffectError{EffectAttemptID: attempt.EffectAttemptID, Err: err})
}

func (t *Thread) validateReplayedEffectArtifact(ctx context.Context, entry sessiontree.Entry) error {
	if entry.Message.ToolResult == nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	ref := entry.Message.ToolResult.FullOutput
	if ref == nil {
		return nil
	}
	reader, ok := t.harness.options.Repo.(sessiontree.ArtifactAuthorityRepo)
	if !ok {
		return sessiontree.ErrUnsupportedStoreCapability
	}
	meta, err := t.harness.options.Repo.Thread(ctx, entry.ThreadID)
	if err != nil {
		return err
	}
	content, err := reader.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{
		ParentThreadID: meta.ParentThreadID, ThreadID: entry.ThreadID, ArtifactID: ref.ID,
	})
	if err != nil {
		return err
	}
	if content.Ref != *ref || content.Text == "" && ref.SizeBytes != 0 {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func effectDispatchError(callID, toolName string, err error) tools.Result {
	result := tools.ErrorResult(callID, toolName, err.Error())
	result.DispatchErr = err
	return result
}

func validateEffectAuthorizationProof(req EffectAuthorizationRequest, proof EffectAuthorizationProof) error {
	if proof.EffectAttemptID != req.EffectAttemptID || proof.RequestFingerprint != req.RequestFingerprint ||
		proof.ThreadID != req.ThreadID || proof.TurnID != req.TurnID || proof.RunID != req.RunID ||
		proof.ToolCallID != req.ToolCallID || proof.LeaseOwnerID != req.LeaseOwnerID || proof.LeaseGeneration != req.LeaseGeneration ||
		strings.TrimSpace(proof.PolicyRevision) == "" || strings.TrimSpace(proof.AuditReference) == "" || strings.TrimSpace(proof.AuditHash) == "" || proof.AuthorizedAt.IsZero() {
		return ErrInvalidAuthorizationProof
	}
	return nil
}

func effectRequestFingerprint(req tools.EffectDispatchRequest, lease sessiontree.TurnLease, argumentHash string) (string, error) {
	payload, err := json.Marshal(struct {
		Request      tools.EffectDispatchRequest `json:"request"`
		ArgumentHash string                      `json:"argument_hash"`
		OwnerID      string                      `json:"owner_id"`
		Generation   int64                       `json:"generation"`
	}{Request: req, ArgumentHash: argumentHash, OwnerID: lease.OwnerID, Generation: lease.Generation})
	if err != nil {
		return "", err
	}
	return sessiontree.StableHash(string(payload)), nil
}

func effectOutcomeFingerprint(result tools.Result, message session.Message, fullOutput *artifact.FullOutput) (string, error) {
	type fullOutputFingerprint struct {
		Present bool   `json:"present"`
		SHA256  string `json:"sha256,omitempty"`
		Kind    string `json:"kind,omitempty"`
		MIME    string `json:"mime,omitempty"`
	}
	full := fullOutputFingerprint{}
	if fullOutput != nil {
		normalized := artifact.NormalizeFullOutput(*fullOutput)
		full = fullOutputFingerprint{Present: true, SHA256: artifact.TextSHA256(normalized.Text), Kind: normalized.Kind, MIME: normalized.MIME}
	}
	payload, err := json.Marshal(struct {
		Message    session.Message       `json:"message"`
		Error      bool                  `json:"error"`
		FullOutput fullOutputFingerprint `json:"full_output"`
	}{Message: session.CloneMessage(message), Error: result.IsError, FullOutput: full})
	if err != nil {
		return "", err
	}
	return sessiontree.StableHash(string(payload)), nil
}
