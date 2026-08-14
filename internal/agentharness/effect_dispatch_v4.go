package agentharness

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v4/internal/engine"
	"github.com/floegence/floret/v4/internal/session"
	"github.com/floegence/floret/v4/internal/sessiontree"
	"github.com/floegence/floret/v4/tools"
)

type effectRetryContextKey struct{}

type effectRetryIdentity struct {
	requestKey      string
	sourceAttemptID string
}

func withEffectRetry(ctx context.Context, requestKey, sourceAttemptID string) context.Context {
	return context.WithValue(ctx, effectRetryContextKey{}, effectRetryIdentity{
		requestKey: strings.TrimSpace(requestKey), sourceAttemptID: strings.TrimSpace(sourceAttemptID),
	})
}

// dispatchAuthorizedEffect is the v4 effect path. Approval is intentionally
// absent here: ThreadRuntime exposes it as one journal-backed interaction and
// only invokes the host gate after that interaction is resolved.
func (t *Thread) dispatchAuthorizedEffect(ctx context.Context, request tools.EffectDispatchRequest, invoke func(context.Context) tools.Result) tools.Result {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return effectDispatchError(request.CallID, request.Name, ErrAuthorizationUnavailable)
	}
	repo, ok := t.harness.options.Repo.(sessiontree.EffectAttemptRepo)
	if !ok {
		return effectDispatchError(request.CallID, request.Name, errors.New("session tree repo does not support effect attempts"))
	}
	argumentHash := sessiontree.StableHash(strings.TrimSpace(request.RawArgs))
	fingerprint, err := effectRequestFingerprint(request, argumentHash)
	if err != nil {
		return effectDispatchError(request.CallID, request.Name, err)
	}
	invocation := sessiontree.EffectInvocationIdentity{
		ThreadID: request.ThreadID.String(), TurnID: request.TurnID.String(), RunID: request.RunID.String(),
		ToolCallID: request.CallID, ToolName: request.Name, ArgumentHash: argumentHash,
	}
	if retry, ok := ctx.Value(effectRetryContextKey{}).(effectRetryIdentity); ok {
		invocation.RetryKey = retry.requestKey
		invocation.SourceEffectAttemptID = retry.sourceAttemptID
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, sessiontree.PrepareEffectAttemptRequest{
		Invocation:         invocation,
		RequestFingerprint: fingerprint,
		Now:                t.harness.now(),
	})
	if err != nil {
		return effectDispatchError(request.CallID, request.Name, err)
	}
	if prepared.Replayed && prepared.Attempt.State != sessiontree.EffectAttemptPrepared {
		return t.replayEffectResult(ctx, prepared.Attempt)
	}
	gate := t.harness.options.EffectAuthorizationGate
	if gate == nil {
		return t.rejectEffectAttempt(ctx, repo, prepared.Attempt, fingerprint, ErrAuthorizationUnavailable)
	}
	authorizationRequest := effectAuthorizationRequest(request, prepared.Attempt, fingerprint)
	ready := make(chan tools.Result, 1)
	finalize := make(chan effectFinalizeRequest, 1)
	gateDone := make(chan effectGateOutcome, 1)
	dispatchStarted := make(chan struct{})
	seal := "effect-dispatch:" + sessiontree.StableHash(prepared.Attempt.EffectAttemptID+"\x00"+fingerprint)
	prepareRequest := sessiontree.PrepareEffectAttemptRequest{
		RequestFingerprint: fingerprint, Now: t.harness.now(), Invocation: prepared.Attempt.Invocation,
	}
	authorizedEffect := func(dispatchCtx context.Context, proof EffectAuthorizationProof) (EffectDispatchResult, error) {
		if dispatchCtx == nil {
			return EffectDispatchResult{}, ErrAuthorizationContract
		}
		if err := validateEffectAuthorizationProof(authorizationRequest, proof); err != nil {
			return EffectDispatchResult{}, err
		}
		proofHash := sessiontree.StableHash(proof.AuditReference + "\x00" + proof.AuditHash + "\x00" + proof.PolicyRevision)
		if _, err := repo.BeginEffectDispatch(dispatchCtx, sessiontree.BeginEffectDispatchRequest{
			EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
			AuthorizationProofHash: proofHash, Now: t.harness.now(),
		}); err != nil {
			return EffectDispatchResult{}, err
		}
		close(dispatchStarted)
		handlerResult := invoke(dispatchCtx)
		if handlerResult.DispatchErr != nil {
			return EffectDispatchResult{}, t.convergeDispatchedEffect(dispatchCtx, repo, prepareRequest, prepared.Attempt, "effect_handler_panic", handlerResult.DispatchErr)
		}
		finalizerKey := effectFinalizerKey(request.RunID.String(), request.TurnID.String(), request.CallID)
		if err := t.registerEffectFinalizer(finalizerKey, func(finalizeCtx context.Context, request engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
			pending := effectFinalizeRequest{
				ctx: finalizeCtx, request: cloneEffectFinalizationRequest(request),
				outcome: make(chan effectFinalizeOutcome, 1),
			}
			finalize <- pending
			outcome := <-pending.outcome
			return outcome.result, outcome.err
		}); err != nil {
			return EffectDispatchResult{}, t.convergeDispatchedEffect(dispatchCtx, repo, prepareRequest, prepared.Attempt, "register_effect_finalizer_error", err)
		}
		if t.harness.effectFinalizerRegistration != nil {
			t.harness.effectFinalizerRegistration(nil)
		}
		ready <- handlerResult
		finalization := <-finalize
		var finalizationOutcome effectFinalizeOutcome
		defer func() { finalization.outcome <- finalizationOutcome }()
		defer t.removeEffectFinalizer(finalizerKey)
		finishCtx, cancelFinish := t.harness.effectFinalizationContext(finalization.ctx)
		defer cancelFinish()
		outcomeFingerprint, fingerprintErr := t.harness.effectOutcomeFingerprinter(handlerResult, finalization.request.Message, finalization.request.FullOutput)
		if fingerprintErr != nil {
			finalizationOutcome.err = fingerprintErr
			return EffectDispatchResult{}, t.convergeDispatchedEffect(finishCtx, repo, prepareRequest, prepared.Attempt, "outcome_fingerprint_error", fingerprintErr)
		}
		finished, finishErr := repo.FinishEffectDispatch(finishCtx, sessiontree.FinishEffectDispatchRequest{
			EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: fingerprint,
			OutcomeFingerprint: outcomeFingerprint, Failed: handlerResult.IsError || effectMessageFailed(finalization.request.Message), Now: t.harness.now(),
			Result:     sessiontree.Entry{ThreadID: request.ThreadID.String(), TurnID: request.TurnID.String(), Type: sessiontree.EntryToolResult, Message: session.CloneMessage(finalization.request.Message)},
			FullOutput: cloneEffectFullOutput(finalization.request.FullOutput),
		})
		if finishErr != nil {
			finalizationOutcome.err = finishErr
			return EffectDispatchResult{}, t.convergeDispatchedEffect(finishCtx, repo, prepareRequest, prepared.Attempt, "finish_effect_dispatch_error", finishErr)
		}
		committed, validateErr := validateCommittedEffectFinalization(finalization.request, prepared.Attempt, finished)
		if validateErr != nil {
			finalizationOutcome.err = validateErr
			return EffectDispatchResult{}, &CommittedEffectError{EffectAttemptID: prepared.Attempt.EffectAttemptID, Err: validateErr}
		}
		finalizationOutcome.result = committed
		if !finished.Replayed {
			t.harness.emitEntryCommitted(finished.Result, request.RunID.String())
			t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: request.RunID.String(), ThreadID: request.ThreadID.String(), TurnID: request.TurnID.String(), EntryID: finished.Result.ID, ParentID: finished.Result.ParentID})
		}
		return EffectDispatchResult{seal: seal, finalization: committed}, nil
	}
	go func() {
		outcome := effectGateOutcome{}
		defer func() {
			if recovered := recover(); recovered != nil {
				outcome.err = fmt.Errorf("%w: authorization gate panicked: %v", ErrAuthorizationContract, recovered)
			}
			gateDone <- outcome
		}()
		outcome.result, outcome.err = gate.Dispatch(ctx, authorizationRequest, authorizedEffect)
		if outcome.err == nil && outcome.result.seal != seal {
			outcome.err = ErrAuthorizationContract
		}
	}()
	select {
	case handlerResult := <-ready:
		return handlerResult
	case outcome := <-gateDone:
		if outcome.err == nil {
			outcome.err = ErrAuthorizationContract
		}
		if request.Permission.Mode == tools.PermissionAsk && (errors.Is(outcome.err, ErrEffectUnauthorized) || errors.Is(outcome.err, tools.ErrRejected)) {
			_ = t.rejectEffectAttemptCause(ctx, repo, prepared.Attempt, fingerprint, tools.ErrRejected)
			return tools.DeclinedResult(request.CallID, request.Name)
		}
		var committed *CommittedEffectError
		if errors.As(outcome.err, &committed) {
			return effectDispatchError(request.CallID, request.Name, outcome.err)
		}
		return t.rejectEffectAttempt(ctx, repo, prepared.Attempt, fingerprint, outcome.err)
	case <-ctx.Done():
		select {
		case <-dispatchStarted:
			persistCtx, cancelPersist := t.harness.effectFinalizationContext(ctx)
			_ = t.convergeDispatchedEffect(persistCtx, repo, prepareRequest, prepared.Attempt, "turn_cancelled_after_dispatch", contextCancellationError(ctx))
			cancelPersist()
		default:
			_ = t.rejectEffectAttemptCause(context.Background(), repo, prepared.Attempt, fingerprint, contextCancellationError(ctx))
		}
		return effectDispatchError(request.CallID, request.Name, contextCancellationError(ctx))
	}
}
