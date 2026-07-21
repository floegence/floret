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
	"github.com/floegence/floret/observation"
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

type effectAttemptRejectedError struct {
	Err error
}

func (e *effectAttemptRejectedError) Error() string {
	if e == nil || e.Err == nil {
		return "effect attempt was rejected"
	}
	return e.Err.Error()
}

func (e *effectAttemptRejectedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
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

// AuthorizedEffect crosses the canonical effect boundary under the execution
// context selected by the host authorization gate.
type AuthorizedEffect func(context.Context, EffectAuthorizationProof) (EffectDispatchResult, error)

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

type effectGatePromise struct {
	done    chan struct{}
	once    sync.Once
	outcome effectGateOutcome
}

func newEffectGatePromise() *effectGatePromise {
	return &effectGatePromise{done: make(chan struct{})}
}

func (p *effectGatePromise) resolve(outcome effectGateOutcome) {
	p.once.Do(func() {
		p.outcome = outcome
		close(p.done)
	})
}

func (p *effectGatePromise) result() effectGateOutcome {
	<-p.done
	return p.outcome
}

type effectDispatchPhase uint8

const (
	effectDispatchOpen effectDispatchPhase = iota
	effectDispatchBeginning
	effectDispatchBegun
	effectDispatchNotBegun
	effectDispatchCancelled
)

func contextCancellationError(ctx context.Context) error {
	if ctx == nil || ctx.Err() == nil {
		return nil
	}
	cause := context.Cause(ctx)
	if cause == nil || cause == ctx.Err() {
		return ctx.Err()
	}
	if errors.Is(cause, ctx.Err()) {
		return cause
	}
	return errors.Join(ctx.Err(), cause)
}

func isContextCancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

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
	return func(ctx context.Context, request tools.EffectDispatchRequest, invoke func(context.Context) tools.Result) tools.Result {
		return t.dispatchAuthorizedEffect(ctx, request, invoke)
	}
}

func (t *Thread) dispatchAuthorizedEffect(ctx context.Context, request tools.EffectDispatchRequest, invoke func(context.Context) tools.Result) tools.Result {
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
	approvalRequested := request.Permission.Mode == tools.PermissionAsk
	cancelApprovalForExecution := func(cancellation error) error {
		if err := t.cancelApprovalBatchForTurn(ctx, lease, request.RunID); err != nil {
			return err
		}
		if cancellation == nil {
			return context.Canceled
		}
		return cancellation
	}
	if approvalRequested && ctx.Err() != nil {
		return effectDispatchError(request.CallID, request.Name, cancelApprovalForExecution(contextCancellationError(ctx)))
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, sessiontree.PrepareEffectAttemptRequest{
		Lease: lease, RequestFingerprint: fingerprint, Now: t.harness.now(),
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID,
			ToolCallID: request.CallID, ToolName: request.Name, ArgumentHash: argumentHash,
		},
	})
	if err != nil {
		if approvalRequested && ctx.Err() != nil {
			return effectDispatchError(request.CallID, request.Name, cancelApprovalForExecution(contextCancellationError(ctx)))
		}
		return effectDispatchError(request.CallID, request.Name, err)
	}
	if prepared.Replayed && prepared.Attempt.State != sessiontree.EffectAttemptPrepared {
		return t.replayEffectResult(ctx, prepared.Attempt)
	}
	authorizationRequest := effectAuthorizationRequest(request, lease, prepared.Attempt, fingerprint)
	gate := t.harness.options.EffectAuthorizationGate
	if gate == nil && request.Permission.Mode != tools.PermissionAsk {
		return t.rejectEffectAttempt(ctx, repo, lease, prepared.Attempt, fingerprint, ErrAuthorizationUnavailable)
	}
	var approval *effectApproval
	if approvalRequested {
		approval, err = t.bindEffectApproval(ctx, authorizationRequest)
		if err != nil {
			if ctx.Err() != nil {
				return effectDispatchError(request.CallID, request.Name, cancelApprovalForExecution(contextCancellationError(ctx)))
			}
			return t.rejectEffectAttempt(ctx, repo, lease, prepared.Attempt, fingerprint, err)
		}
		receipt, waitErr := approval.wait(ctx)
		if waitErr != nil {
			if isContextCancellationError(waitErr) {
				waitErr = cancelApprovalForExecution(waitErr)
			}
			return effectDispatchError(request.CallID, request.Name, waitErr)
		}
		switch receipt.State {
		case sessiontree.ApprovalRejected:
			return effectDispatchError(request.CallID, request.Name, ErrEffectUnauthorized)
		case sessiontree.ApprovalDecisionSubmitted:
		default:
			return effectDispatchError(request.CallID, request.Name, sessiontree.ErrAuthorityCorrupt)
		}
		if gate == nil {
			if err := t.finalizeEffectApproval(ctx, approval, authorizationRequest, sessiontree.ApprovalFailed, sessiontree.ApprovalReasonAuthorizationUnavailable, receipt.DecisionID); err != nil {
				return effectDispatchError(request.CallID, request.Name, err)
			}
			return effectDispatchError(request.CallID, request.Name, ErrAuthorizationUnavailable)
		}
	}
	ready := make(chan tools.Result, 1)
	finalize := make(chan effectFinalizeRequest, 1)
	gateOutcome := newEffectGatePromise()
	dispatchBoundary := newEffectDispatchBoundary()
	abortCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	var abortOnce sync.Once
	var active atomic.Bool
	var callbackState atomic.Uint32
	var knownResultMu sync.Mutex
	var knownResult EffectDispatchResult
	knownResultAvailable := false
	active.Store(true)
	seal := "effect-dispatch:" + sessiontree.StableHash(prepared.Attempt.EffectAttemptID+"\x00"+fingerprint)
	prepareRequest := sessiontree.PrepareEffectAttemptRequest{
		Lease: lease, RequestFingerprint: fingerprint, Now: t.harness.now(), Invocation: prepared.Attempt.Invocation,
	}
	markUnknown := func(finalizationCtx context.Context, reason string, cause error) error {
		return t.convergeDispatchedEffect(finalizationCtx, repo, prepareRequest, prepared.Attempt, reason, cause)
	}
	var completionMu sync.Mutex
	var convergenceOnce sync.Once
	var convergenceErr error
	convergeFailure := func(reason string, cause error, contractFailure bool) error {
		convergenceOnce.Do(func() {
			completionMu.Lock()
			defer completionMu.Unlock()
			failureErr := cause
			if failureErr == nil {
				failureErr = errors.New(strings.TrimSpace(reason))
			}
			if contractFailure {
				failureErr = errors.Join(fmt.Errorf("%w: %s", ErrAuthorizationContract, strings.TrimSpace(reason)), failureErr)
			}
			persistCtx, cancelPersist := t.harness.effectFinalizationContext(ctx)
			defer cancelPersist()
			observed, observeErr := repo.PrepareEffectAttempt(persistCtx, prepareRequest)
			if observeErr != nil {
				convergenceErr = errors.Join(failureErr, observeErr)
				return
			}
			switch observed.Attempt.State {
			case sessiontree.EffectAttemptPrepared:
				if !contractFailure {
					failureErr = errors.Join(ErrAuthorizationContract, failureErr)
				}
				if approvalRequested {
					convergenceErr = failureErr
					return
				}
				convergenceErr = &effectAttemptRejectedError{Err: t.rejectEffectAttemptCause(persistCtx, repo, lease, observed.Attempt, fingerprint, failureErr)}
			case sessiontree.EffectAttemptDispatching:
				convergenceErr = markUnknown(persistCtx, reason, failureErr)
			case sessiontree.EffectAttemptUnknown:
				convergenceErr = &CommittedEffectError{EffectAttemptID: observed.Attempt.EffectAttemptID, Err: errors.Join(sessiontree.ErrEffectOutcomeUnknown, failureErr)}
			case sessiontree.EffectAttemptCompleted, sessiontree.EffectAttemptFailed:
				convergenceErr = &CommittedEffectError{EffectAttemptID: observed.Attempt.EffectAttemptID, Err: failureErr}
			default:
				convergenceErr = failureErr
			}
		})
		return convergenceErr
	}
	convergeContractFailure := func(reason string, cause error) error {
		return convergeFailure(reason, cause, true)
	}
	convergeUnknown := func(reason string, cause error) error {
		return convergeFailure(reason, cause, false)
	}
	abort := func() {
		active.Store(false)
		abortOnce.Do(func() { close(abortCallback) })
	}
	recordKnownResult := func(result EffectDispatchResult) {
		knownResultMu.Lock()
		knownResult = result
		knownResultAvailable = true
		knownResultMu.Unlock()
	}
	readKnownResult := func() (EffectDispatchResult, bool) {
		knownResultMu.Lock()
		defer knownResultMu.Unlock()
		return knownResult, knownResultAvailable
	}
	authorizedEffect := func(dispatchCtx context.Context, proof EffectAuthorizationProof) (dispatchResult EffectDispatchResult, dispatchErr error) {
		if !active.Load() || !callbackState.CompareAndSwap(0, 1) {
			return EffectDispatchResult{}, ErrEffectDispatchConsumed
		}
		finalizerKey := effectFinalizerKey(request.RunID, request.TurnID, request.CallID)
		finalizerRegistered := false
		readyDelivered := false
		keepFinalizer := false
		defer func() {
			var recoveredOutcome *effectGateOutcome
			if recovered := recover(); recovered != nil {
				if completed, ok := readKnownResult(); ok {
					dispatchResult = completed
					dispatchErr = nil
					outcome := effectGateOutcome{result: completed}
					recoveredOutcome = &outcome
				} else {
					dispatchErr = convergeContractFailure("effect callback panicked", fmt.Errorf("panic: %v", recovered))
					if completed, ok := readKnownResult(); ok {
						dispatchResult = completed
						dispatchErr = nil
						outcome := effectGateOutcome{result: completed}
						recoveredOutcome = &outcome
					} else {
						dispatchResult = EffectDispatchResult{}
						keepFinalizer = readyDelivered
						outcome := effectGateOutcome{err: dispatchErr}
						recoveredOutcome = &outcome
					}
				}
			}
			callbackState.Store(2)
			if finalizerRegistered && !keepFinalizer {
				t.removeEffectFinalizer(finalizerKey)
			}
			close(callbackDone)
			if recoveredOutcome != nil {
				gateOutcome.resolve(*recoveredOutcome)
			}
		}()
		if dispatchCtx == nil {
			return EffectDispatchResult{}, ErrAuthorizationContract
		}
		dispatchCtx, cancelDispatch := context.WithCancelCause(dispatchCtx)
		stopTurnCancellation := context.AfterFunc(ctx, func() {
			cancelDispatch(contextCancellationError(ctx))
		})
		if cause := contextCancellationError(ctx); cause != nil {
			cancelDispatch(cause)
		}
		defer func() {
			stopTurnCancellation()
			cancelDispatch(context.Canceled)
		}()
		dispatchCtx = sessiontree.ContextWithTurnLease(dispatchCtx, lease)
		if err := validateEffectAuthorizationProof(authorizationRequest, proof); err != nil {
			return EffectDispatchResult{}, err
		}
		current, ok := sessiontree.TurnLeaseFromContext(dispatchCtx)
		if !ok || current.OwnerID != lease.OwnerID || current.Generation != lease.Generation {
			return EffectDispatchResult{}, sessiontree.ErrStaleAuthority
		}
		if err := contextCancellationError(dispatchCtx); err != nil {
			return EffectDispatchResult{}, err
		}
		if !dispatchBoundary.begin() {
			if err := contextCancellationError(ctx); err != nil {
				return EffectDispatchResult{}, err
			}
			return EffectDispatchResult{}, ErrAuthorizationContract
		}
		proofHash := sessiontree.StableHash(proof.AuditReference + "\x00" + proof.AuditHash + "\x00" + proof.PolicyRevision)
		var beginErr error
		var approvedEvent event.Event
		var approvalCommit sessiontree.CommitApprovalDispatchResult
		if approvalRequested {
			approvedEvent = t.effectApprovalEvent(event.ToolApprovalApproved, authorizationRequest, "")
			approvedEntry := approvalEventEntry(t.id, authorizationRequest.TurnID, approvedEvent)
			approvedEntry.ID = sessiontree.ApprovalDispatchEntryID(approval.receipt.DecisionID, approval.record.ApprovalID)
			approvalCommit, beginErr = approval.commitDispatch(dispatchCtx, current, proofHash, approvedEntry)
		} else {
			_, beginErr = repo.BeginEffectDispatch(dispatchCtx, sessiontree.BeginEffectDispatchRequest{
				Lease: current, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
				ObservedHeartbeat: authorizationRequest.ObservedHeartbeat, AuthorizationProofHash: proofHash, Now: t.harness.now(),
			})
		}
		if beginErr != nil {
			dispatchBoundary.resolve(false)
			return EffectDispatchResult{}, beginErr
		}
		dispatchBoundary.resolve(true)
		select {
		case <-abortCallback:
			return EffectDispatchResult{}, convergeContractFailure("authorization gate returned before effect callback", nil)
		default:
		}
		if approvalRequested && approvalCommit.Replayed {
			return EffectDispatchResult{}, convergeUnknown("approval_dispatch_replayed", nil)
		}
		if approvalRequested {
			t.emitCommittedApprovalEvent(approvalCommit.ApprovedEntry, authorizationRequest.RunID, approvedEvent)
			if t.harness.options.Sink != nil {
				t.harness.options.Sink.Emit(event.SanitizeWithPolicy(approvedEvent, t.harness.options.SinkPolicy))
			}
		}
		handlerResult := invoke(dispatchCtx)
		if handlerResult.DispatchErr != nil {
			return EffectDispatchResult{}, convergeUnknown("effect_handler_panic", handlerResult.DispatchErr)
		}
		select {
		case <-abortCallback:
			return EffectDispatchResult{}, convergeContractFailure("authorization gate returned before effect callback", nil)
		default:
		}
		if err := t.registerEffectFinalizer(finalizerKey, func(finalizeCtx context.Context, request engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
			select {
			case finalize <- effectFinalizeRequest{ctx: finalizeCtx, request: cloneEffectFinalizationRequest(request)}:
			case <-abortCallback:
			}
			outcome := gateOutcome.result()
			if outcome.err != nil {
				var committed *CommittedEffectError
				if errors.As(outcome.err, &committed) {
					return engine.EffectResultFinalizationResult{}, outcome.err
				}
				return engine.EffectResultFinalizationResult{}, &CommittedEffectError{EffectAttemptID: prepared.Attempt.EffectAttemptID, Err: outcome.err}
			}
			return outcome.result.finalization, nil
		}); err != nil {
			if t.harness.effectFinalizerRegistration != nil {
				t.harness.effectFinalizerRegistration(err)
			}
			return EffectDispatchResult{}, convergeUnknown("register_effect_finalizer_error", err)
		}
		finalizerRegistered = true
		if t.harness.effectFinalizerRegistration != nil {
			t.harness.effectFinalizerRegistration(nil)
		}
		completionMu.Lock()
		select {
		case <-abortCallback:
			completionMu.Unlock()
			return EffectDispatchResult{}, convergeContractFailure("authorization gate returned before effect callback", nil)
		default:
			ready <- handlerResult
			readyDelivered = true
			completionMu.Unlock()
		}
		var finalization effectFinalizeRequest
		select {
		case finalization = <-finalize:
		case <-abortCallback:
			keepFinalizer = true
			return EffectDispatchResult{}, convergeContractFailure("authorization gate returned before effect callback", nil)
		}
		select {
		case <-abortCallback:
			return EffectDispatchResult{}, convergeContractFailure("authorization gate returned before effect callback", nil)
		default:
		}
		var completed EffectDispatchResult
		var failureReason string
		var failureCause error
		var committedErr error
		aborted := false
		func() {
			completionMu.Lock()
			defer completionMu.Unlock()
			select {
			case <-abortCallback:
				aborted = true
				return
			default:
			}
			finishCtx, cancelFinish := t.harness.effectFinalizationContext(finalization.ctx)
			defer cancelFinish()
			current, ok = sessiontree.TurnLeaseFromContext(finishCtx)
			if !ok || current.OwnerID != lease.OwnerID || current.Generation != lease.Generation {
				failureReason = "effect_finalization_authority_error"
				failureCause = sessiontree.ErrStaleAuthority
				return
			}
			outcomeFingerprint, fingerprintErr := t.harness.effectOutcomeFingerprinter(handlerResult, finalization.request.Message, finalization.request.FullOutput)
			if fingerprintErr != nil {
				failureReason = "outcome_fingerprint_error"
				failureCause = fingerprintErr
				return
			}
			finished, finishErr := repo.FinishEffectDispatch(finishCtx, sessiontree.FinishEffectDispatchRequest{
				Lease: current, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
				OutcomeFingerprint: outcomeFingerprint, Failed: handlerResult.IsError || effectMessageFailed(finalization.request.Message), Now: t.harness.now(),
				Result:     sessiontree.Entry{ThreadID: request.ThreadID, TurnID: request.TurnID, Type: sessiontree.EntryToolResult, Message: session.CloneMessage(finalization.request.Message)},
				FullOutput: cloneEffectFullOutput(finalization.request.FullOutput),
			})
			if finishErr != nil {
				failureReason = "finish_effect_dispatch_error"
				failureCause = finishErr
				return
			}
			committedFinalization, validateErr := validateCommittedEffectFinalization(finalization.request, prepared.Attempt, finished)
			if validateErr != nil {
				committedErr = &CommittedEffectError{EffectAttemptID: prepared.Attempt.EffectAttemptID, Err: validateErr}
				return
			}
			entry := finished.Result
			completed = EffectDispatchResult{seal: seal, finalization: committedFinalization}
			recordKnownResult(completed)
			if !finished.Replayed {
				t.harness.emitEntryCommitted(entry, request.RunID)
				t.harness.emit(HarnessEvent{
					Type: EventEntryAppended, RunID: request.RunID, ThreadID: request.ThreadID, TurnID: request.TurnID,
					EntryID: entry.ID, ParentID: entry.ParentID,
				})
			}
		}()
		if aborted {
			return EffectDispatchResult{}, convergeContractFailure("authorization gate returned before effect callback", nil)
		}
		if failureReason != "" {
			return EffectDispatchResult{}, convergeUnknown(failureReason, failureCause)
		}
		if committedErr != nil {
			return EffectDispatchResult{}, committedErr
		}
		return completed, nil
	}
	go func() {
		var outcome effectGateOutcome
		defer func() {
			if recovered := recover(); recovered != nil {
				if completed, ok := readKnownResult(); ok {
					outcome = effectGateOutcome{result: completed}
				} else {
					abort()
					contractErr := convergeContractFailure("authorization gate panicked", fmt.Errorf("panic: %v", recovered))
					if callbackState.Load() == 1 {
						<-callbackDone
					}
					if completed, ok := readKnownResult(); ok {
						outcome = effectGateOutcome{result: completed}
					} else {
						outcome = effectGateOutcome{err: contractErr}
					}
				}
			}
			active.Store(false)
			gateOutcome.resolve(outcome)
		}()
		releaseAuthority, authorityErr := t.enterProviderRequest(ctx)
		if authorityErr != nil {
			outcome.err = authorityErr
			return
		}
		defer releaseAuthority()
		outcome.result, outcome.err = gate.Dispatch(ctx, authorizationRequest, authorizedEffect)
		if callbackState.Load() == 1 {
			abort()
			if completed, ok := readKnownResult(); ok {
				<-callbackDone
				outcome = effectGateOutcome{result: completed}
			} else {
				contractErr := convergeContractFailure("authorization gate returned before effect callback", outcome.err)
				<-callbackDone
				if completed, ok := readKnownResult(); ok {
					outcome = effectGateOutcome{result: completed}
				} else {
					outcome = effectGateOutcome{err: contractErr}
				}
			}
			return
		}
		if outcome.err == nil && outcome.result.seal != seal {
			outcome.err = ErrAuthorizationContract
		}
	}()
	select {
	case handlerResult := <-ready:
		return handlerResult
	case <-gateOutcome.done:
		select {
		case handlerResult := <-ready:
			return handlerResult
		default:
		}
		if cause := contextCancellationError(ctx); cause != nil {
			switch dispatchBoundary.cancelOrObserve() {
			case effectDispatchCancelled, effectDispatchNotBegun:
				active.Store(false)
				if approvalRequested {
					return effectDispatchError(request.CallID, request.Name, cancelApprovalForExecution(cause))
				}
				return effectDispatchError(request.CallID, request.Name, cause)
			}
		}
		outcome := gateOutcome.result()
		if isContextCancellationError(outcome.err) {
			switch dispatchBoundary.cancelOrObserve() {
			case effectDispatchCancelled, effectDispatchNotBegun:
				active.Store(false)
				if approvalRequested {
					return effectDispatchError(request.CallID, request.Name, cancelApprovalForExecution(outcome.err))
				}
				return effectDispatchError(request.CallID, request.Name, outcome.err)
			}
		}
		if outcome.err == nil {
			outcome.err = ErrAuthorizationContract
		}
		if approvalRequested {
			var committed *CommittedEffectError
			if errors.As(outcome.err, &committed) {
				return effectDispatchError(request.CallID, request.Name, outcome.err)
			}
			state, reason := approvalFailure(outcome.err)
			if err := t.finalizeEffectApproval(ctx, approval, authorizationRequest, state, reason, approval.receipt.DecisionID); err != nil {
				return effectDispatchError(request.CallID, request.Name, err)
			}
			return effectDispatchError(request.CallID, request.Name, outcome.err)
		}
		var committed *CommittedEffectError
		if errors.As(outcome.err, &committed) {
			return effectDispatchError(request.CallID, request.Name, outcome.err)
		}
		var rejected *effectAttemptRejectedError
		if errors.As(outcome.err, &rejected) {
			return effectDispatchError(request.CallID, request.Name, outcome.err)
		}
		return t.rejectEffectAttempt(ctx, repo, lease, prepared.Attempt, fingerprint, outcome.err)
	case <-ctx.Done():
		for {
			switch dispatchBoundary.cancelOrObserve() {
			case effectDispatchBeginning:
				select {
				case <-dispatchBoundary.resolved:
					continue
				case <-gateOutcome.done:
					outcome := gateOutcome.result()
					boundaryResolved := false
					select {
					case <-dispatchBoundary.resolved:
						boundaryResolved = true
					default:
					}
					if boundaryResolved {
						switch dispatchBoundary.cancelOrObserve() {
						case effectDispatchCancelled, effectDispatchNotBegun:
							cancellation := contextCancellationError(ctx)
							if cancellation == nil && isContextCancellationError(outcome.err) {
								cancellation = outcome.err
							}
							if cancellation == nil {
								cancellation = context.Canceled
							}
							active.Store(false)
							if approvalRequested {
								return effectDispatchError(request.CallID, request.Name, cancelApprovalForExecution(cancellation))
							}
							return effectDispatchError(request.CallID, request.Name, cancellation)
						case effectDispatchBegun:
						default:
							return effectDispatchError(request.CallID, request.Name, ErrAuthorizationContract)
						}
					}
					if outcome.err == nil {
						outcome.err = ErrAuthorizationContract
					}
					if approvalRequested {
						var committed *CommittedEffectError
						if errors.As(outcome.err, &committed) {
							return effectDispatchError(request.CallID, request.Name, outcome.err)
						}
						state, reason := approvalFailure(outcome.err)
						if err := t.finalizeEffectApproval(ctx, approval, authorizationRequest, state, reason, approval.receipt.DecisionID); err != nil {
							return effectDispatchError(request.CallID, request.Name, err)
						}
					}
					return effectDispatchError(request.CallID, request.Name, outcome.err)
				}
			case effectDispatchBegun:
				select {
				case handlerResult := <-ready:
					return handlerResult
				case <-gateOutcome.done:
					select {
					case handlerResult := <-ready:
						return handlerResult
					default:
					}
					outcome := gateOutcome.result()
					if outcome.err == nil {
						outcome.err = ErrAuthorizationContract
					}
					return effectDispatchError(request.CallID, request.Name, outcome.err)
				}
			case effectDispatchOpen:
				continue
			case effectDispatchCancelled, effectDispatchNotBegun:
				active.Store(false)
				if approvalRequested {
					return effectDispatchError(request.CallID, request.Name, cancelApprovalForExecution(contextCancellationError(ctx)))
				}
				return effectDispatchError(request.CallID, request.Name, contextCancellationError(ctx))
			default:
				return effectDispatchError(request.CallID, request.Name, ErrAuthorizationContract)
			}
		}
	}
}

func effectMessageFailed(message session.Message) bool {
	return message.ToolResult != nil && strings.EqualFold(strings.TrimSpace(message.ToolResult.Status), "error")
}

func (t *Thread) convergeDispatchedEffect(
	ctx context.Context,
	repo sessiontree.EffectAttemptAuthorityRepo,
	prepare sessiontree.PrepareEffectAttemptRequest,
	attempt sessiontree.EffectAttempt,
	reason string,
	cause error,
) error {
	unknownCtx, cancelUnknown := t.harness.effectFinalizationContext(ctx)
	_, markErr := repo.MarkEffectUnknown(unknownCtx, sessiontree.MarkEffectUnknownRequest{
		Lease: prepare.Lease, EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: prepare.RequestFingerprint,
		OutcomeFingerprint: sessiontree.StableHash(attempt.EffectAttemptID + "\x00unknown\x00" + strings.TrimSpace(reason)),
		Now:                t.harness.now(),
	})
	cancelUnknown()
	if markErr != nil {
		observeCtx, cancelObserve := t.harness.effectFinalizationContext(ctx)
		observed, observeErr := repo.PrepareEffectAttempt(observeCtx, prepare)
		cancelObserve()
		if observeErr == nil {
			switch observed.Attempt.State {
			case sessiontree.EffectAttemptCompleted, sessiontree.EffectAttemptFailed:
				if cause == nil {
					cause = ErrAuthorizationContract
				}
				return &CommittedEffectError{EffectAttemptID: attempt.EffectAttemptID, Err: cause}
			case sessiontree.EffectAttemptUnknown:
				markErr = nil
			}
		} else {
			markErr = errors.Join(markErr, observeErr)
		}
	}
	unknownErr := error(sessiontree.ErrEffectOutcomeUnknown)
	if cause != nil && !errors.Is(cause, sessiontree.ErrEffectOutcomeUnknown) {
		unknownErr = errors.Join(unknownErr, cause)
	}
	if markErr != nil {
		unknownErr = errors.Join(unknownErr, markErr)
	}
	return &CommittedEffectError{EffectAttemptID: attempt.EffectAttemptID, Err: unknownErr}
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
		CanonicalEntryID: finished.Result.ID,
	}, nil
}

func (t *Thread) effectApprovalEvent(typ event.Type, req EffectAuthorizationRequest, reason string) event.Event {
	resources := make([]map[string]string, 0, len(req.Resources))
	for _, resource := range req.Resources {
		resources = append(resources, map[string]string{"kind": resource.Kind, "value": resource.Value})
	}
	effects := make([]string, 0, len(req.Effects))
	for _, effect := range req.Effects {
		effects = append(effects, string(effect))
	}
	return event.Event{
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
}

func (t *Thread) rejectEffectAttempt(ctx context.Context, repo sessiontree.EffectAttemptAuthorityRepo, lease sessiontree.TurnLease, attempt sessiontree.EffectAttempt, requestFingerprint string, cause error) tools.Result {
	return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, t.rejectEffectAttemptCause(ctx, repo, lease, attempt, requestFingerprint, cause))
}

func (t *Thread) rejectEffectAttemptCause(ctx context.Context, repo sessiontree.EffectAttemptAuthorityRepo, lease sessiontree.TurnLease, attempt sessiontree.EffectAttempt, requestFingerprint string, cause error) error {
	code := "authorization_unavailable"
	public := ErrAuthorizationUnavailable
	switch {
	case errors.Is(cause, ErrEffectUnauthorized), errors.Is(cause, tools.ErrRejected):
		code = "unauthorized"
		public = ErrEffectUnauthorized
	case errors.Is(cause, ErrAuthorizationContract), errors.Is(cause, ErrInvalidAuthorizationProof), errors.Is(cause, ErrEffectDispatchConsumed):
		code = sessiontree.ApprovalReasonAuthorizationContract
		public = ErrAuthorizationContract
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
		return err
	}
	return fmt.Errorf("%w: %v", public, cause)
}

func (t *Thread) replayEffectResult(ctx context.Context, attempt sessiontree.EffectAttempt) tools.Result {
	switch attempt.State {
	case sessiontree.EffectAttemptCompleted, sessiontree.EffectAttemptFailed:
		entry, err := t.harness.options.Repo.Entry(ctx, attempt.Invocation.ThreadID, attempt.ResultEntryID)
		if err != nil {
			if errors.Is(err, sessiontree.ErrEntryNotFound) || errors.Is(err, sessiontree.ErrThreadNotFound) {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return committedEffectDispatchError(attempt, err)
		}
		if err := validateReplayedEffectEntry(attempt, entry); err != nil {
			return committedEffectDispatchError(attempt, err)
		}
		if err := t.validateReplayedEffectArtifact(ctx, entry); err != nil {
			return committedEffectDispatchError(attempt, err)
		}
		key := effectFinalizerKey(attempt.Invocation.RunID, attempt.Invocation.TurnID, attempt.Invocation.ToolCallID)
		if err := t.registerEffectFinalizer(key, func(_ context.Context, req engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
			if req.RunID != attempt.Invocation.RunID || req.ThreadID != attempt.Invocation.ThreadID || req.TurnID != attempt.Invocation.TurnID || req.ToolCallID != attempt.Invocation.ToolCallID {
				return engine.EffectResultFinalizationResult{}, &CommittedEffectError{EffectAttemptID: attempt.EffectAttemptID, Err: sessiontree.ErrAuthorityCorrupt}
			}
			return engine.EffectResultFinalizationResult{Handled: true, Message: session.CloneMessage(entry.Message), Replayed: true, CanonicalEntryID: entry.ID}, nil
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
	case sessiontree.EffectAttemptRejected:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, ErrEffectUnauthorized)
	case sessiontree.EffectAttemptUnknown, sessiontree.EffectAttemptDispatching:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, sessiontree.ErrEffectOutcomeUnknown)
	case sessiontree.EffectAttemptCancelled:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, context.Canceled)
	default:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, ErrAuthorizationUnavailable)
	}
}

func validateReplayedEffectEntry(attempt sessiontree.EffectAttempt, entry sessiontree.Entry) error {
	invocation := attempt.Invocation
	if strings.TrimSpace(attempt.ResultEntryID) == "" || entry.ID != attempt.ResultEntryID ||
		entry.ThreadID != invocation.ThreadID || entry.TurnID != invocation.TurnID || entry.Type != sessiontree.EntryToolResult ||
		entry.Message.Role != session.Tool || entry.Message.ToolCallID != invocation.ToolCallID || entry.Message.ToolName != invocation.ToolName ||
		strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) != strings.TrimSpace(attempt.EffectAttemptID) {
		return sessiontree.ErrAuthorityCorrupt
	}
	if entry.Message.ToolResult == nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	status := observation.ActivityStatus(entry.Message.ToolResult.Status)
	switch attempt.State {
	case sessiontree.EffectAttemptCompleted:
		if status != observation.ActivityStatusSuccess {
			return sessiontree.ErrAuthorityCorrupt
		}
	case sessiontree.EffectAttemptFailed:
		if status != observation.ActivityStatusError {
			return sessiontree.ErrAuthorityCorrupt
		}
	default:
		return sessiontree.ErrAuthorityCorrupt
	}
	if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
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
