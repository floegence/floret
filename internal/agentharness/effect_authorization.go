package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v5/internal/engine"
	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/session/artifact"
	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/observation"
	"github.com/floegence/floret/v5/tools"
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
	Arguments          string
	ArgumentHash       string
	Step               int
	BatchIndex         int
	BatchSize          int
	Labels             map[string]string
	HostContext        map[string]string
	Activity           *tools.ActivityPresentation
	Resources          []tools.ResourceRef
	Effects            []tools.Effect
	Permission         tools.PermissionSpec
	ReadOnly           bool
	Destructive        bool
	OpenWorld          bool
}

type EffectAuthorizationProof struct {
	EffectAttemptID    string
	RequestFingerprint string
	ThreadID           string
	TurnID             string
	RunID              string
	ToolCallID         string
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

type effectFinalizeRequest struct {
	ctx     context.Context
	request engine.EffectResultFinalizationRequest
	outcome chan effectFinalizeOutcome
}

type effectFinalizeOutcome struct {
	result engine.EffectResultFinalizationResult
	err    error
}

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

func effectMessageFailed(message session.Message) bool {
	return message.ToolResult != nil && strings.EqualFold(strings.TrimSpace(message.ToolResult.Status), "error")
}

func (t *Thread) convergeDispatchedEffect(ctx context.Context, repo sessiontree.EffectAttemptRepo, prepare sessiontree.PrepareEffectAttemptRequest, attempt sessiontree.EffectAttempt, reason string, cause error) error {
	unknownCtx, cancelUnknown := t.harness.effectFinalizationContext(ctx)
	_, markErr := repo.MarkEffectUnknown(unknownCtx, sessiontree.MarkEffectUnknownRequest{
		EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: prepare.RequestFingerprint,
		OutcomeFingerprint: sessiontree.StableHash(attempt.EffectAttemptID + "\x00unknown\x00" + strings.TrimSpace(reason)), Now: t.harness.now(),
	})
	cancelUnknown()
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
	requested := sessiontree.Entry{ThreadID: req.ThreadID, TurnID: req.TurnID, Type: sessiontree.EntryToolResult, Message: session.CloneMessage(req.Message)}
	if finished.Attempt.EffectAttemptID != prepared.EffectAttemptID || finished.Attempt.ResultEntryID == "" || finished.Result.ID != finished.Attempt.ResultEntryID ||
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
	return engine.EffectResultFinalizationResult{Handled: true, Message: session.CloneMessage(finished.Result.Message), Replayed: finished.Replayed, CanonicalEntryID: finished.Result.ID}, nil
}

func (t *Thread) rejectEffectAttempt(ctx context.Context, repo sessiontree.EffectAttemptRepo, attempt sessiontree.EffectAttempt, requestFingerprint string, cause error) tools.Result {
	return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, t.rejectEffectAttemptCause(ctx, repo, attempt, requestFingerprint, cause))
}

func (t *Thread) rejectEffectAttemptCause(ctx context.Context, repo sessiontree.EffectAttemptRepo, attempt sessiontree.EffectAttempt, requestFingerprint string, cause error) error {
	code := "authorization_unavailable"
	public := ErrAuthorizationUnavailable
	switch {
	case errors.Is(cause, ErrEffectUnauthorized), errors.Is(cause, tools.ErrRejected):
		code, public = "unauthorized", ErrEffectUnauthorized
	case errors.Is(cause, ErrAuthorizationContract), errors.Is(cause, ErrInvalidAuthorizationProof), errors.Is(cause, ErrEffectDispatchConsumed):
		code, public = "authorization_contract", ErrAuthorizationContract
	}
	rejectionFingerprint := sessiontree.StableHash(strings.Join([]string{attempt.EffectAttemptID, requestFingerprint, code, strings.TrimSpace(cause.Error())}, "\x00"))
	if _, err := repo.RejectEffectAttempt(ctx, sessiontree.RejectEffectAttemptRequest{
		EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: requestFingerprint,
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
				return engine.EffectResultFinalizationResult{}, sessiontree.ErrAuthorityCorrupt
			}
			return engine.EffectResultFinalizationResult{Handled: true, Message: session.CloneMessage(entry.Message), Replayed: true, CanonicalEntryID: entry.ID}, nil
		}); err != nil {
			return committedEffectDispatchError(attempt, err)
		}
		text := entry.Message.Content
		if attempt.State == sessiontree.EffectAttemptFailed {
			text = strings.TrimPrefix(text, "ERROR: ")
		}
		return tools.Result{CallID: attempt.Invocation.ToolCallID, Name: attempt.Invocation.ToolName, Text: text, IsError: attempt.State == sessiontree.EffectAttemptFailed,
			OutputPolicy: &tools.OutputPolicy{VisibleMaxBytes: len(text) + 1, VisibleMaxLines: strings.Count(text, "\n") + 2, Strategy: tools.OutputStrategy(entry.Message.ToolResult.Strategy), PreserveFullSet: true}}
	case sessiontree.EffectAttemptRejected:
		return tools.DeclinedResult(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName)
	case sessiontree.EffectAttemptUnknown, sessiontree.EffectAttemptRetrying, sessiontree.EffectAttemptDispatching:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, sessiontree.ErrEffectOutcomeUnknown)
	case sessiontree.EffectAttemptCancelled:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, context.Canceled)
	default:
		return effectDispatchError(attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, ErrAuthorizationUnavailable)
	}
}

func validateReplayedEffectEntry(attempt sessiontree.EffectAttempt, entry sessiontree.Entry) error {
	invocation := attempt.Invocation
	if strings.TrimSpace(attempt.ResultEntryID) == "" || entry.ID != attempt.ResultEntryID || entry.ThreadID != invocation.ThreadID || entry.TurnID != invocation.TurnID ||
		entry.Type != sessiontree.EntryToolResult || entry.Message.Role != session.Tool || entry.Message.ToolCallID != invocation.ToolCallID || entry.Message.ToolName != invocation.ToolName ||
		strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) != strings.TrimSpace(attempt.EffectAttemptID) || entry.Message.ToolResult == nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	status := observation.ActivityStatus(entry.Message.ToolResult.Status)
	if attempt.State == sessiontree.EffectAttemptCompleted && status != observation.ActivityStatusSuccess || attempt.State == sessiontree.EffectAttemptFailed && status != observation.ActivityStatusError {
		return sessiontree.ErrAuthorityCorrupt
	}
	return sessiontree.ValidateEntryIntegrity(entry)
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
	content, err := reader.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ParentThreadID: meta.ParentThreadID, ThreadID: entry.ThreadID, ArtifactID: ref.ID})
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
	if proof.EffectAttemptID != req.EffectAttemptID || proof.RequestFingerprint != req.RequestFingerprint || proof.ThreadID != req.ThreadID ||
		proof.TurnID != req.TurnID || proof.RunID != req.RunID || proof.ToolCallID != req.ToolCallID || strings.TrimSpace(proof.PolicyRevision) == "" ||
		strings.TrimSpace(proof.AuditReference) == "" || strings.TrimSpace(proof.AuditHash) == "" || proof.AuthorizedAt.IsZero() {
		return ErrInvalidAuthorizationProof
	}
	return nil
}

func effectRequestFingerprint(req tools.EffectDispatchRequest, argumentHash string) (string, error) {
	payload, err := json.Marshal(struct {
		Request      tools.EffectDispatchRequest `json:"request"`
		ArgumentHash string                      `json:"argument_hash"`
	}{Request: req, ArgumentHash: argumentHash})
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
	}{
		Message: session.CloneMessage(message), Error: result.IsError, FullOutput: full,
	})
	if err != nil {
		return "", err
	}
	return sessiontree.StableHash(string(payload)), nil
}
