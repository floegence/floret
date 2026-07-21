package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/provider/catalog"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/storage/sqlite"
	"github.com/floegence/floret/internal/tools/skills"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

type ThreadID string
type TurnID string
type RunID string
type ArtifactID string
type ForkOperationID string
type CreateIntentID string
type PromptScopeID string
type TraceID string

type FinishReason = observation.FinishReason
type CompletionReason = observation.CompletionReason
type ContinuationReason = observation.ContinuationReason

const (
	FinishReasonUnknown       = observation.FinishReasonUnknown
	FinishReasonStop          = observation.FinishReasonStop
	FinishReasonToolCalls     = observation.FinishReasonToolCalls
	FinishReasonLength        = observation.FinishReasonLength
	FinishReasonContentFilter = observation.FinishReasonContentFilter
	FinishReasonError         = observation.FinishReasonError
	FinishReasonCancelled     = observation.FinishReasonCancelled

	CompletionReasonNaturalStop = observation.CompletionReasonNaturalStop
	CompletionReasonToolSignal  = observation.CompletionReasonToolSignal
	CompletionReasonHookStop    = observation.CompletionReasonHookStop

	ContinuationReasonToolResults       = observation.ContinuationReasonToolResults
	ContinuationReasonCompaction        = observation.ContinuationReasonCompaction
	ContinuationReasonProviderTruncated = observation.ContinuationReasonProviderTruncated
	ContinuationReasonRetryEmpty        = observation.ContinuationReasonRetryEmpty
	ContinuationReasonNoProgress        = observation.ContinuationReasonNoProgress
	ContinuationReasonHook              = observation.ContinuationReasonHook
)

var (
	// ErrThreadNotFound reports that a requested durable thread was not found.
	ErrThreadNotFound = errors.New("floret thread not found")
	// ErrThreadDeleted reports that a requested durable identity is permanently tombstoned.
	ErrThreadDeleted = errors.New("floret thread is deleted")
	// ErrThreadNotActive reports that an active-only capability no longer owns the thread mutation.
	ErrThreadNotActive = errors.New("floret thread is not active")
	// ErrThreadBusy reports that another active turn or mutation currently owns the thread.
	ErrThreadBusy = errors.New("floret thread is busy")
	// ErrTurnNotFound reports that a requested durable turn was not found.
	ErrTurnNotFound = errors.New("floret turn not found")
	// ErrInterruptedTurnNotFound reports that a live exact recovery target has no active turn lease.
	ErrInterruptedTurnNotFound = errors.New("floret interrupted turn not found")
	// ErrRecoveryTargetResolved reports that an exact interrupted-turn target no longer owns its bound lease generation.
	ErrRecoveryTargetResolved = errors.New("floret interrupted turn recovery target is resolved")
	// ErrRunNotFound reports that a requested durable run was not found.
	ErrRunNotFound = errors.New("floret run not found")
	// ErrArtifactNotFound reports that a requested durable artifact was not found.
	ErrArtifactNotFound = errors.New("floret artifact not found")
	// ErrNoRetryTarget reports that a thread has no canonical turn eligible for retry.
	ErrNoRetryTarget = errors.New("floret thread has no retry target")
	// ErrPendingToolNotFound reports that a settlement target does not identify a canonical tool call.
	ErrPendingToolNotFound = errors.New("floret pending tool not found")
	// ErrPendingToolNotActive reports that a settlement target is not an active pending tool result.
	ErrPendingToolNotActive = errors.New("floret pending tool is not active")
	// ErrPendingToolSettlementConflict reports that a pending tool was already settled differently.
	ErrPendingToolSettlementConflict = errors.New("floret pending tool settlement conflict")
	// ErrSubAgentNotFound reports that a requested parent-scoped child thread was not found.
	ErrSubAgentNotFound = errors.New("floret subagent not found")
	// ErrSubAgentClosed reports that a requested child mutation targets a closed SubAgent.
	ErrSubAgentClosed = errors.New("floret subagent is closed")
	// ErrSubAgentClosing reports that an explicit close operation owns the child subtree.
	ErrSubAgentClosing = errors.New("floret subagent is closing")
	// ErrStaleAuthority reports that a local proof no longer owns the durable generation.
	ErrStaleAuthority = errors.New("floret authority proof is stale")
	// ErrRequestConflict reports durable request identity reuse with changed input.
	ErrRequestConflict = errors.New("floret request conflicts with persisted authority")
	// ErrAuthorityCorrupt reports an impossible durable authority shape.
	ErrAuthorityCorrupt = errors.New("floret authority state is corrupt")
	// ErrUnsupportedStoreCapability reports a backend that lacks required atomicity.
	ErrUnsupportedStoreCapability = errors.New("floret store capability is unsupported")
	// ErrEffectUnauthorized reports a current host-policy denial before handler entry.
	ErrEffectUnauthorized = errors.New("floret effect is unauthorized")
	// ErrAuthorizationUnavailable reports a host-policy, approval, audit, or gate failure before handler entry.
	ErrAuthorizationUnavailable = errors.New("floret effect authorization is unavailable")
	// ErrInvalidAuthorizationProof reports a proof that does not match the canonical invocation.
	ErrInvalidAuthorizationProof = errors.New("floret effect authorization proof is invalid")
	// ErrEffectDispatchConsumed reports reuse or deferred use of a one-shot authorized effect.
	ErrEffectDispatchConsumed = errors.New("floret authorized effect dispatch was consumed")
	// ErrEffectOutcomeUnknown reports an invocation that crossed dispatch without a known result.
	ErrEffectOutcomeUnknown = errors.New("floret effect outcome is unknown")
	// ErrAuthorizationContract reports a host gate that did not return the closure's sealed result.
	ErrAuthorizationContract = errors.New("floret effect authorization contract failed")
	// ErrStoreClosed reports that the Store has started closing.
	ErrStoreClosed = errors.New("floret store is closed")
	// ErrSubAgentParentRequired reports that a child operation used a root-thread capability.
	ErrSubAgentParentRequired = errors.New("floret subagent operation requires parent authority")
	// ErrForkOperationConflict reports that an operation ID was reused with a different fork request.
	ErrForkOperationConflict = errors.New("floret fork operation conflicts with existing request")
	// ErrForkDestinationConflict reports that a planned destination is owned by another operation or node.
	ErrForkDestinationConflict = errors.New("floret fork destination conflicts with operation plan")
	// ErrAgentTodoVersionConflict reports that a todo update was based on a stale canonical version.
	ErrAgentTodoVersionConflict = errors.New("floret agent todo version conflict")
	// ErrJournalInvariant reports an ambiguous active path that Floret refuses to repair heuristically.
	ErrJournalInvariant = errors.New("floret thread journal invariant violated")
	// ErrThreadAuthorityInvariant reports invalid durable root/SubAgent ownership metadata.
	ErrThreadAuthorityInvariant = errors.New("floret thread authority invariant violated")
)

// CommittedCleanupError reports that canonical deletion committed and only
// physical or auxiliary cleanup remains retryable.
type CommittedCleanupError struct {
	ThreadID ThreadID
	Err      error
}

type AuthorityBusyKind string

const (
	AuthorityBusyTurn      AuthorityBusyKind = "turn"
	AuthorityBusyAuthority AuthorityBusyKind = "authority"
)

// AuthorityBusyError classifies which durable authority family blocked an
// operation without exposing an owner identity.
type AuthorityBusyError struct {
	Kind AuthorityBusyKind
	Err  error
}

func (e *AuthorityBusyError) Error() string {
	if e == nil {
		return ErrThreadBusy.Error()
	}
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", ErrThreadBusy, e.Kind)
	}
	return fmt.Sprintf("%s: %s: %v", ErrThreadBusy, e.Kind, e.Err)
}

func (e *AuthorityBusyError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *AuthorityBusyError) Is(target error) bool {
	return target == ErrThreadBusy || e != nil && errors.Is(e.Err, target)
}

// RequestConflictError identifies the immutable request key that was reused
// with different input. It never exposes the stored request payload.
type RequestConflictError struct {
	Operation string
	RequestID string
	Err       error
}

// UnsupportedStoreSchemaError reports an exact SQLite schema contract
// mismatch without mutating or replacing the observed store.
type UnsupportedStoreSchemaError struct {
	ObservedVersion        string
	ObservedFingerprint    string
	CurrentVersion         string
	CurrentFingerprint     string
	PredecessorVersion     string
	PredecessorFingerprint string
}

func (e *UnsupportedStoreSchemaError) Error() string {
	if e == nil {
		return "unsupported floret store schema"
	}
	return fmt.Sprintf(
		"unsupported floret store schema version %q fingerprint %q; accepted current is version %q fingerprint %q and empty predecessor is version %q fingerprint %q",
		e.ObservedVersion, e.ObservedFingerprint, e.CurrentVersion, e.CurrentFingerprint,
		e.PredecessorVersion, e.PredecessorFingerprint,
	)
}

type StoreLeasePolicy struct {
	TTL                time.Duration
	RenewInterval      time.Duration
	ClockSkewAllowance time.Duration
}

// StoreLeasePolicyMismatchError reports that an opener requested a lease
// policy different from the authority policy already persisted in the Store.
type StoreLeasePolicyMismatchError struct {
	Configured StoreLeasePolicy
	Persisted  StoreLeasePolicy
}

func (e *StoreLeasePolicyMismatchError) Error() string {
	if e == nil {
		return "floret store lease policy mismatch"
	}
	return fmt.Sprintf("floret store lease policy mismatch: configured=%+v persisted=%+v", e.Configured, e.Persisted)
}

func (e *RequestConflictError) Error() string {
	if e == nil {
		return ErrRequestConflict.Error()
	}
	identity := strings.TrimSpace(e.Operation)
	if requestID := strings.TrimSpace(e.RequestID); requestID != "" {
		identity += " " + fmt.Sprintf("%q", requestID)
	}
	if identity == "" {
		identity = "authority request"
	}
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", ErrRequestConflict, identity)
	}
	return fmt.Sprintf("%s: %s: %v", ErrRequestConflict, identity, e.Err)
}

func (e *RequestConflictError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *RequestConflictError) Is(target error) bool {
	return target == ErrRequestConflict || e != nil && errors.Is(e.Err, target)
}

func requestConflictError(err error, operation, requestID string) error {
	if !errors.Is(err, ErrRequestConflict) {
		return err
	}
	var existing *RequestConflictError
	if errors.As(err, &existing) && existing != nil && existing.Err != nil {
		err = existing.Err
	}
	return &RequestConflictError{Operation: strings.TrimSpace(operation), RequestID: strings.TrimSpace(requestID), Err: err}
}

func (e *CommittedCleanupError) Error() string {
	if e == nil {
		return "floret canonical cleanup committed"
	}
	return fmt.Sprintf("floret canonical cleanup committed for thread %q: %v", e.ThreadID, e.Err)
}

func (e *CommittedCleanupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type CommittedEffectError struct {
	EffectAttemptID string
	Err             error
}

func (e *CommittedEffectError) Error() string {
	if e == nil || e.Err == nil {
		return "floret effect handler dispatch committed"
	}
	return "floret effect handler dispatch committed: " + e.Err.Error()
}

func (e *CommittedEffectError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type providerHost struct {
	cfg                       config.Config
	store                     *Store
	sink                      EventSink
	harness                   *agentharness.AgentHarness
	supportsOpaqueAttachments bool
}

// ThreadTitleMode selects who owns durable thread title generation.
type ThreadTitleMode string

const (
	ThreadTitleModeHostOwned ThreadTitleMode = "host_owned"
	ThreadTitleModeProvider  ThreadTitleMode = "provider"
)

func normalizeThreadTitleMode(mode ThreadTitleMode) (ThreadTitleMode, error) {
	switch mode {
	case "", ThreadTitleModeHostOwned:
		return ThreadTitleModeHostOwned, nil
	case ThreadTitleModeProvider:
		return ThreadTitleModeProvider, nil
	default:
		return "", fmt.Errorf("unsupported thread title mode %q", mode)
	}
}

type providerHostOptions struct {
	Config                  config.Config
	ModelGateway            ModelGateway
	ModelGatewayIdentity    ModelGatewayIdentity
	Store                   *Store
	Tools                   *tools.Registry
	EffectAuthorizationGate EffectAuthorizationGate
	Sink                    EventSink
	ToolSurfaceProvider     ToolSurfaceProvider
	IDGenerator             func(string) string
	LoopLimits              LoopLimits
	SubAgentRunTimeout      time.Duration
	Capabilities            CapabilityOptions
	ThreadTitleMode         ThreadTitleMode
}

type LoopLimits struct {
	MaxEmptyProviderRetries int
	NoProgressLimit         int
	DuplicateToolLimit      int
	WallTime                time.Duration
}

type CapabilityOptions struct {
	SkillsEnabled          bool
	SkillSources           []string
	SkillPromptBudgetBytes int
}

type CreateThreadRequest struct {
	ThreadID       ThreadID
	CreateIntentID CreateIntentID
}

type SetThreadTitleRequest struct {
	ThreadID ThreadID `json:"thread_id"`
	Title    string   `json:"title"`
}

type ForkThreadRequest struct {
	OperationID         ForkOperationID
	SourceThreadID      ThreadID
	DestinationThreadID ThreadID
}

type ForkThreadResult struct {
	OperationID ForkOperationID `json:"operation_id"`
	Thread      ThreadSummary   `json:"thread"`
}

type RecoverInterruptedTurnResult struct {
	ThreadID ThreadID           `json:"thread_id"`
	TurnID   TurnID             `json:"turn_id"`
	RunID    RunID              `json:"run_id"`
	Status   TurnStatus         `json:"status"`
	Failure  *ThreadTurnFailure `json:"failure,omitempty"`
	Replayed bool               `json:"replayed"`
}

// TurnSupplementalContextItem is host-provided context that is visible only to
// the current model turn. It does not change the user's input text, durable
// thread history, working directory, permissions, or provider continuation
// state.
type TurnSupplementalContextItem struct {
	Kind      string
	Title     string
	Text      string
	Metadata  map[string]string
	Sensitive bool
	Truncated bool
}

// MessageAttachment identifies one host-owned resource attached to a durable
// user message. ResourceRef is opaque to Floret and is resolved only by the
// host's ModelGateway implementation.
type MessageAttachment struct {
	ResourceRef string `json:"resource_ref"`
	Name        string `json:"name"`
	MIMEType    string `json:"mime_type"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
}

type MessageReferenceKind string

const (
	MessageReferenceText      MessageReferenceKind = "text"
	MessageReferenceFile      MessageReferenceKind = "file"
	MessageReferenceDirectory MessageReferenceKind = "directory"
	MessageReferenceTerminal  MessageReferenceKind = "terminal"
	MessageReferenceProcess   MessageReferenceKind = "process"

	MaxMessageReferencesPerTurn           = 128
	MaxMessageReferenceIDBytes            = 128
	MaxMessageReferenceLabelRunes         = 256
	MaxMessageReferenceTextRunes          = 12_000
	MaxMessageReferenceResourceRefBytes   = 8_192
	MaxMessageReferencesPayloadBytes      = 256 * 1024
	MaxTurnSupplementalContextItems       = 128
	MaxTurnSupplementalContextKindRunes   = 128
	MaxTurnSupplementalContextTitleRunes  = 256
	MaxTurnSupplementalContextTextRunes   = 16_384
	MaxTurnSupplementalMetadataPairs      = 32
	MaxTurnSupplementalMetadataKeyBytes   = 128
	MaxTurnSupplementalMetadataValueRunes = 4_096
	MaxTurnSupplementalPayloadBytes       = 256 * 1024
)

// MessageReference is one ordered, durable, user-visible reference associated
// with a canonical user message. ResourceRef is opaque to Floret.
type MessageReference struct {
	ReferenceID string               `json:"reference_id"`
	Kind        MessageReferenceKind `json:"kind"`
	Label       string               `json:"label"`
	Text        string               `json:"text,omitempty"`
	ResourceRef string               `json:"resource_ref,omitempty"`
	Truncated   bool                 `json:"truncated,omitempty"`
}

type EffectAuthorizationRequest struct {
	EffectAttemptID    string               `json:"effect_attempt_id"`
	RequestFingerprint string               `json:"request_fingerprint"`
	ThreadID           ThreadID             `json:"thread_id"`
	TurnID             TurnID               `json:"turn_id"`
	RunID              RunID                `json:"run_id"`
	ToolCallID         string               `json:"tool_call_id"`
	ToolName           string               `json:"tool_name"`
	ArgumentHash       string               `json:"argument_hash"`
	Step               int                  `json:"step"`
	BatchIndex         int                  `json:"batch_index"`
	BatchSize          int                  `json:"batch_size"`
	Labels             map[string]string    `json:"labels,omitempty"`
	HostContext        map[string]string    `json:"host_context,omitempty"`
	Resources          []tools.ResourceRef  `json:"resources,omitempty"`
	Effects            []tools.Effect       `json:"effects,omitempty"`
	Permission         tools.PermissionSpec `json:"permission"`
	ReadOnly           bool                 `json:"read_only"`
	Destructive        bool                 `json:"destructive"`
	OpenWorld          bool                 `json:"open_world"`
	LeaseOwnerID       string               `json:"lease_owner_id"`
	LeaseGeneration    int64                `json:"lease_generation"`
	ObservedHeartbeat  int64                `json:"observed_heartbeat"`
}

type EffectAuthorizationProof struct {
	EffectAttemptID    string    `json:"effect_attempt_id"`
	RequestFingerprint string    `json:"request_fingerprint"`
	ThreadID           ThreadID  `json:"thread_id"`
	TurnID             TurnID    `json:"turn_id"`
	RunID              RunID     `json:"run_id"`
	ToolCallID         string    `json:"tool_call_id"`
	LeaseOwnerID       string    `json:"lease_owner_id"`
	LeaseGeneration    int64     `json:"lease_generation"`
	PolicyRevision     string    `json:"policy_revision"`
	ApprovalID         string    `json:"approval_id,omitempty"`
	AuditReference     string    `json:"audit_reference"`
	AuditHash          string    `json:"audit_hash"`
	AuthorizedAt       time.Time `json:"authorized_at"`
}

type EffectDispatchResult struct {
	result agentharness.EffectDispatchResult
}

type AuthorizedEffect func(EffectAuthorizationProof) (EffectDispatchResult, error)

type EffectAuthorizationGate interface {
	Dispatch(context.Context, EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error)
}

type EffectAuthorizationGateFunc func(context.Context, EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error)

func (f EffectAuthorizationGateFunc) Dispatch(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	return f(ctx, req, effect)
}

func runtimeEffectAuthorizationGate(gate EffectAuthorizationGate) agentharness.EffectAuthorizationGate {
	if gate == nil {
		return nil
	}
	return agentharness.EffectAuthorizationGateFunc(func(ctx context.Context, req agentharness.EffectAuthorizationRequest, effect agentharness.AuthorizedEffect) (agentharness.EffectDispatchResult, error) {
		result, err := gate.Dispatch(ctx, EffectAuthorizationRequest{
			EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
			ThreadID: ThreadID(req.ThreadID), TurnID: TurnID(req.TurnID), RunID: RunID(req.RunID),
			ToolCallID: req.ToolCallID, ToolName: req.ToolName, ArgumentHash: req.ArgumentHash,
			Step: req.Step, BatchIndex: req.BatchIndex, BatchSize: req.BatchSize,
			Labels: cloneStringMap(req.Labels), HostContext: cloneStringMap(req.HostContext),
			Resources: append([]tools.ResourceRef(nil), req.Resources...), Effects: append([]tools.Effect(nil), req.Effects...),
			Permission: req.Permission, ReadOnly: req.ReadOnly, Destructive: req.Destructive, OpenWorld: req.OpenWorld,
			LeaseOwnerID: req.LeaseOwnerID, LeaseGeneration: req.LeaseGeneration, ObservedHeartbeat: req.ObservedHeartbeat,
		}, func(proof EffectAuthorizationProof) (EffectDispatchResult, error) {
			internalResult, err := effect(agentharness.EffectAuthorizationProof{
				EffectAttemptID: proof.EffectAttemptID, RequestFingerprint: proof.RequestFingerprint,
				ThreadID: string(proof.ThreadID), TurnID: string(proof.TurnID), RunID: string(proof.RunID), ToolCallID: proof.ToolCallID,
				LeaseOwnerID: proof.LeaseOwnerID, LeaseGeneration: proof.LeaseGeneration,
				PolicyRevision: proof.PolicyRevision, ApprovalID: proof.ApprovalID,
				AuditReference: proof.AuditReference, AuditHash: proof.AuditHash, AuthorizedAt: proof.AuthorizedAt,
			})
			return EffectDispatchResult{result: internalResult}, runtimeEffectAuthorizationError(err)
		})
		return result.result, runtimeEffectAuthorizationError(err)
	})
}

func runtimeEffectAuthorizationError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrEffectUnauthorized):
		return fmt.Errorf("%w: %v", agentharness.ErrEffectUnauthorized, err)
	case errors.Is(err, ErrAuthorizationUnavailable):
		return fmt.Errorf("%w: %v", agentharness.ErrAuthorizationUnavailable, err)
	case errors.Is(err, ErrInvalidAuthorizationProof):
		return fmt.Errorf("%w: %v", agentharness.ErrInvalidAuthorizationProof, err)
	case errors.Is(err, ErrEffectDispatchConsumed):
		return fmt.Errorf("%w: %v", agentharness.ErrEffectDispatchConsumed, err)
	case errors.Is(err, ErrAuthorizationContract):
		return fmt.Errorf("%w: %v", agentharness.ErrAuthorizationContract, err)
	default:
		return err
	}
}

func (a MessageAttachment) Validate() error {
	if strings.TrimSpace(a.ResourceRef) == "" {
		return errors.New("message attachment resource ref is required")
	}
	if strings.TrimSpace(a.Name) == "" {
		return errors.New("message attachment name is required")
	}
	if strings.TrimSpace(a.MIMEType) == "" {
		return errors.New("message attachment MIME type is required")
	}
	if a.SizeBytes < 0 {
		return errors.New("message attachment size must be non-negative")
	}
	return nil
}

type TurnInput struct {
	Text        string              `json:"text,omitempty"`
	Attachments []MessageAttachment `json:"attachments,omitempty"`
	References  []MessageReference  `json:"references,omitempty"`
}

func (i TurnInput) Validate() error {
	if strings.TrimSpace(i.Text) == "" && len(i.Attachments) == 0 && len(i.References) == 0 {
		return errors.New("turn input requires text, attachments, or references")
	}
	seen := make(map[string]struct{}, len(i.Attachments))
	for index, attachment := range i.Attachments {
		if err := attachment.Validate(); err != nil {
			return fmt.Errorf("turn input attachment %d: %w", index, err)
		}
		ref := strings.TrimSpace(attachment.ResourceRef)
		if _, ok := seen[ref]; ok {
			return fmt.Errorf("turn input contains duplicate attachment resource ref %q", ref)
		}
		seen[ref] = struct{}{}
	}
	return validateMessageReferences(i.References)
}

func validateMessageReferences(references []MessageReference) error {
	return session.ValidateMessageReferences(sessionMessageReferences(references))
}

func (r MessageReference) Validate() error {
	return session.MessageReference{
		ReferenceID: r.ReferenceID,
		Kind:        session.MessageReferenceKind(r.Kind),
		Label:       r.Label,
		Text:        r.Text,
		ResourceRef: r.ResourceRef,
		Truncated:   r.Truncated,
	}.Validate()
}

type RunTurnRequest struct {
	RunID               RunID
	ThreadID            ThreadID
	TurnID              TurnID
	Input               TurnInput
	SupplementalContext []TurnSupplementalContextItem
	Labels              RunLabels
	Completion          TurnCompletionPolicy
	Signals             TurnSignalSpec
	Limits              TurnLimits
	Reasoning           ReasoningSelection
	ManualCompactions   ManualCompactionSource
	ToolSurfaceProvider ToolSurfaceProvider
}

type RetryTurnRequest struct {
	ThreadID ThreadID
	Reason   string
	Labels   RunLabels
}

type CompactThreadRequest struct {
	ThreadID  ThreadID
	RequestID string
	Source    string
	Labels    RunLabels
	Limits    TurnLimits
	Reasoning ReasoningSelection
}

// ReadTurnProjectionRequest identifies a durable hosted turn projection to rebuild from Floret detail.
// RunID is required and must match the execution identity recorded for the turn.
type ReadTurnProjectionRequest struct {
	ThreadID ThreadID
	TurnID   TurnID
	RunID    RunID
}

type AgentTodoStatus string

const (
	AgentTodoPending    AgentTodoStatus = "pending"
	AgentTodoInProgress AgentTodoStatus = "in_progress"
	AgentTodoCompleted  AgentTodoStatus = "completed"
)

func (s AgentTodoStatus) Valid() bool {
	switch s {
	case AgentTodoPending, AgentTodoInProgress, AgentTodoCompleted:
		return true
	default:
		return false
	}
}

type AgentTodo struct {
	ID      string          `json:"id"`
	Content string          `json:"content"`
	Status  AgentTodoStatus `json:"status"`
}

type ThreadAgentTodoState struct {
	ThreadID          ThreadID    `json:"thread_id"`
	Version           int64       `json:"version"`
	Items             []AgentTodo `json:"items"`
	UpdatedAt         time.Time   `json:"updated_at,omitempty"`
	UpdatedByTurnID   TurnID      `json:"updated_by_turn_id,omitempty"`
	UpdatedByRunID    RunID       `json:"updated_by_run_id,omitempty"`
	UpdatedByToolCall string      `json:"updated_by_tool_call_id,omitempty"`
}

type UpdateThreadAgentTodosRequest struct {
	ThreadID        ThreadID
	ExpectedVersion int64
	Items           []AgentTodo
	TurnID          TurnID
	RunID           RunID
	ToolCallID      string
}

// PendingToolCompletionStatus describes the observed outcome of host-owned work
// that was previously exposed to the agent as a pending tool result.
type PendingToolCompletionStatus string

const (
	PendingToolCompletionCompleted PendingToolCompletionStatus = "completed"
	PendingToolCompletionFailed    PendingToolCompletionStatus = "failed"
	PendingToolCompletionCanceled  PendingToolCompletionStatus = "canceled"
)

// PendingToolCompletionRequest asks Floret to append a host-authored follow-up
// turn for work whose lifecycle was owned outside Floret.
type PendingToolCompletionRequest struct {
	CompletionRequestID string
	Target              PendingToolSettlementTarget
	ContinuationTurnID  TurnID
	ContinuationRunID   RunID
	Status              PendingToolCompletionStatus
	Summary             string
	Output              string
	Input               TurnInput
	Labels              RunLabels
}

// PendingToolCompletionResult reports the one durable continuation admission.
// Turn is present only once that continuation has reached a terminal state.
type PendingToolCompletionResult struct {
	CompletionRequestID string      `json:"completion_request_id"`
	ThreadID            ThreadID    `json:"thread_id"`
	TurnID              TurnID      `json:"turn_id"`
	RunID               RunID       `json:"run_id"`
	Status              TurnStatus  `json:"status"`
	Replayed            bool        `json:"replayed,omitempty"`
	Turn                *TurnResult `json:"turn,omitempty"`
}

func (r PendingToolCompletionResult) Validate() error {
	if strings.TrimSpace(r.CompletionRequestID) == "" || strings.TrimSpace(string(r.ThreadID)) == "" ||
		strings.TrimSpace(string(r.TurnID)) == "" || strings.TrimSpace(string(r.RunID)) == "" {
		return errors.New("pending tool completion result requires completion request, thread, turn, and run identities")
	}
	if !r.Status.Valid() {
		return fmt.Errorf("invalid pending tool completion status %q", r.Status)
	}
	if r.Status == TurnStatusRunning {
		if r.Turn != nil {
			return errors.New("running pending tool completion cannot include a terminal turn")
		}
		return nil
	}
	if r.Turn == nil {
		return errors.New("terminal pending tool completion requires a turn result")
	}
	if err := r.Turn.Validate(); err != nil {
		return err
	}
	if r.Turn.ThreadID != r.ThreadID || r.Turn.TurnID != r.TurnID || r.Turn.RunID != r.RunID || r.Turn.Status != r.Status {
		return errors.New("pending tool completion turn identity mismatch")
	}
	return nil
}

// PendingToolSettlementStatus describes a host-owned pending tool outcome that
// should update Floret activity without adding provider-visible context.
type PendingToolSettlementStatus string

const (
	PendingToolSettlementCompleted PendingToolSettlementStatus = "completed"
	PendingToolSettlementFailed    PendingToolSettlementStatus = "failed"
	PendingToolSettlementCanceled  PendingToolSettlementStatus = "canceled"
)

// PendingToolSettlementTarget identifies the exact pending tool result that a
// host owns and intends to settle.
type PendingToolSettlementTarget struct {
	ThreadID        ThreadID `json:"thread_id"`
	TurnID          TurnID   `json:"turn_id"`
	RunID           RunID    `json:"run_id"`
	ToolCallID      string   `json:"tool_call_id"`
	ToolName        string   `json:"tool_name"`
	Handle          string   `json:"handle"`
	EffectAttemptID string   `json:"effect_attempt_id,omitempty"`
}

// PendingToolSettlementRequest records a host-owned pending tool outcome as a
// detail/activity event only. It does not resume the provider loop.
type PendingToolSettlementRequest struct {
	Target   PendingToolSettlementTarget
	Status   PendingToolSettlementStatus
	Summary  string
	Output   string
	Activity *observation.ActivityPresentation
}

type SubAgentStatus string

const (
	SubAgentStatusIdle        SubAgentStatus = "idle"
	SubAgentStatusRunning     SubAgentStatus = "running"
	SubAgentStatusWaiting     SubAgentStatus = "waiting"
	SubAgentStatusCompleted   SubAgentStatus = "completed"
	SubAgentStatusFailed      SubAgentStatus = "failed"
	SubAgentStatusCancelled   SubAgentStatus = "cancelled"
	SubAgentStatusInterrupted SubAgentStatus = "interrupted"
	SubAgentStatusClosing     SubAgentStatus = "closing"
	SubAgentStatusClosed      SubAgentStatus = "closed"
)

type SubAgentForkMode string

const (
	SubAgentForkNone     SubAgentForkMode = "none"
	SubAgentForkFullPath SubAgentForkMode = "full_path"
)

type SpawnSubAgentRequest struct {
	PublicationID   string
	ParentThreadID  ThreadID
	ParentTurnID    TurnID
	ThreadID        ThreadID
	TaskName        string
	TaskDescription string
	Message         string
	Attachments     []MessageAttachment
	References      []MessageReference
	HostProfileRef  string
	ForkMode        SubAgentForkMode
	Labels          RunLabels
}

type SendSubAgentInputRequest struct {
	InputRequestID string
	ParentThreadID ThreadID
	ChildThreadID  ThreadID
	Message        string
	Attachments    []MessageAttachment
	References     []MessageReference
	Interrupt      bool
	Labels         RunLabels
}

type PublishSubAgentPendingToolCompletionRequest struct {
	InputRequestID string
	ParentThreadID ThreadID
	ChildThreadID  ThreadID
	Target         PendingToolSettlementTarget
	Status         PendingToolCompletionStatus
	Summary        string
	Output         string
	Input          TurnInput
	Labels         RunLabels
}

type WaitSubAgentsRequest struct {
	ParentThreadID ThreadID
	ChildThreadIDs []ThreadID
	Timeout        time.Duration
}

type CloseSubAgentRequest struct {
	CloseOperationID string
	ParentThreadID   ThreadID
	ChildThreadID    ThreadID
	Reason           string
}

type ReadSubAgentDetailRequest struct {
	ParentThreadID ThreadID
	ChildThreadID  ThreadID
	AfterOrdinal   int64
	Limit          int
	IncludeRaw     bool
}

type ListSubAgentActivityTimelineRequest struct {
	ParentThreadID ThreadID
	Meta           observation.ActivityRunMeta
}

type ListThreadDetailEventsRequest struct {
	ThreadID     ThreadID
	AfterOrdinal int64
	Limit        int
	IncludeRaw   bool
}

type ReadApprovalQueueRequest struct {
	ThreadID ThreadID
}

type ApprovalDecision string

const (
	ApprovalDecisionApprove ApprovalDecision = "approve"
	ApprovalDecisionReject  ApprovalDecision = "reject"
)

type ApprovalIdentity struct {
	ApprovalID      string   `json:"approval_id"`
	ThreadID        ThreadID `json:"thread_id"`
	TurnID          TurnID   `json:"turn_id"`
	RunID           RunID    `json:"run_id"`
	ToolCallID      string   `json:"tool_call_id"`
	EffectAttemptID string   `json:"effect_attempt_id"`
}

func (i ApprovalIdentity) Validate() error {
	if strings.TrimSpace(i.ApprovalID) == "" || strings.TrimSpace(string(i.ThreadID)) == "" ||
		strings.TrimSpace(string(i.TurnID)) == "" || strings.TrimSpace(string(i.RunID)) == "" ||
		strings.TrimSpace(i.ToolCallID) == "" || strings.TrimSpace(i.EffectAttemptID) == "" {
		return errors.New("approval identity is incomplete")
	}
	return nil
}

type ResolveApprovalRequest struct {
	DecisionID               string           `json:"decision_id"`
	ExpectedRootThreadID     ThreadID         `json:"expected_root_thread_id"`
	ExpectedGeneration       int64            `json:"expected_generation"`
	ExpectedRevision         int64            `json:"expected_revision"`
	ExpectedCurrent          ApprovalIdentity `json:"expected_current"`
	ExpectedApprovalRevision int64            `json:"expected_approval_revision"`
	Decision                 ApprovalDecision `json:"decision"`
}

func (r ResolveApprovalRequest) Validate() error {
	if strings.TrimSpace(r.DecisionID) == "" || strings.TrimSpace(string(r.ExpectedRootThreadID)) == "" {
		return errors.New("approval decision requires decision and root thread identities")
	}
	if r.ExpectedGeneration <= 0 || r.ExpectedRevision <= 0 || r.ExpectedApprovalRevision <= 0 {
		return errors.New("approval decision authority versions must be positive")
	}
	if err := r.ExpectedCurrent.Validate(); err != nil {
		return err
	}
	if r.ExpectedCurrent.ThreadID == "" || (r.Decision != ApprovalDecisionApprove && r.Decision != ApprovalDecisionReject) {
		return errors.New("approval decision is invalid")
	}
	return nil
}

type SubAgentSnapshot struct {
	ThreadID        ThreadID         `json:"thread_id"`
	Path            string           `json:"path"`
	TaskName        string           `json:"task_name"`
	TaskDescription string           `json:"task_description,omitempty"`
	ParentThreadID  ThreadID         `json:"parent_thread_id"`
	ParentTurnID    TurnID           `json:"parent_turn_id,omitempty"`
	HostProfileRef  string           `json:"host_profile_ref,omitempty"`
	ForkMode        SubAgentForkMode `json:"fork_mode,omitempty"`
	Status          SubAgentStatus   `json:"status"`
	LatestTurnID    TurnID           `json:"latest_turn_id,omitempty"`
	LastMessage     string           `json:"last_message,omitempty"`
	WaitingPrompt   string           `json:"waiting_prompt,omitempty"`
	QueuedInputs    int              `json:"queued_inputs,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
	Closed          bool             `json:"closed,omitempty"`
	CanSendInput    bool             `json:"can_send_input"`
	CanInterrupt    bool             `json:"can_interrupt"`
	CanClose        bool             `json:"can_close"`
}

type WaitSubAgentsResult struct {
	Snapshots []SubAgentSnapshot `json:"snapshots"`
	TimedOut  bool               `json:"timed_out,omitempty"`
}

type SubAgentDetail struct {
	Snapshot         SubAgentSnapshot             `json:"snapshot"`
	Events           []ThreadDetailEvent          `json:"events"`
	ActivityTimeline observation.ActivityTimeline `json:"activity_timeline"`
	Context          ThreadContextSnapshot        `json:"context,omitempty"`
	NextOrdinal      int64                        `json:"next_ordinal,omitempty"`
	HasMore          bool                         `json:"has_more,omitempty"`
	RetainedFrom     int64                        `json:"retained_from,omitempty"`
	GeneratedAt      time.Time                    `json:"generated_at"`
}

type ThreadContextSnapshot struct {
	ThreadID    ThreadID                      `json:"thread_id"`
	Provider    string                        `json:"provider,omitempty"`
	Model       string                        `json:"model,omitempty"`
	Policy      config.ContextPolicy          `json:"policy,omitempty"`
	Usage       *observation.ContextStatus    `json:"usage,omitempty"`
	Compactions []observation.CompactionEvent `json:"compactions,omitempty"`
	UpdatedAt   time.Time                     `json:"updated_at,omitempty"`
}

func (s ThreadContextSnapshot) Validate() error {
	if strings.TrimSpace(string(s.ThreadID)) == "" {
		return errors.New("thread context snapshot requires thread id")
	}
	hasContext := strings.TrimSpace(s.Provider) != "" || strings.TrimSpace(s.Model) != "" || s.Policy.ContextWindowTokens > 0 || s.Usage != nil || len(s.Compactions) > 0
	if hasContext && (strings.TrimSpace(s.Provider) == "" || strings.TrimSpace(s.Model) == "" || s.Policy.ContextWindowTokens <= 0 || s.UpdatedAt.IsZero()) {
		return errors.New("thread context snapshot requires model and policy")
	}
	if s.Usage != nil {
		if err := s.Usage.Validate(); err != nil {
			return err
		}
		if strings.TrimSpace(s.Usage.RunID) == "" || strings.TrimSpace(s.Usage.TurnID) == "" || s.Usage.ThreadID != string(s.ThreadID) {
			return errors.New("thread context usage identity mismatch")
		}
		if s.Usage.Provider != s.Provider || s.Usage.Model != s.Model {
			return errors.New("thread context usage model identity mismatch")
		}
	}
	for _, compact := range s.Compactions {
		if err := compact.Validate(); err != nil {
			return err
		}
		if compact.ThreadID != string(s.ThreadID) || strings.TrimSpace(compact.RunID) == "" || strings.TrimSpace(compact.OperationID) == "" || strings.TrimSpace(compact.RequestID) == "" {
			return errors.New("thread context compaction identity mismatch")
		}
	}
	return nil
}

type SubAgentActivityTimelineResult struct {
	Timeline    observation.ActivityTimeline `json:"activity_timeline"`
	GeneratedAt time.Time                    `json:"generated_at"`
}

type ThreadDetailEvents struct {
	Events       []ThreadDetailEvent `json:"events"`
	NextOrdinal  int64               `json:"next_ordinal,omitempty"`
	HasMore      bool                `json:"has_more,omitempty"`
	RetainedFrom int64               `json:"retained_from,omitempty"`
	GeneratedAt  time.Time           `json:"generated_at"`
}

type ApprovalQueue struct {
	RootThreadID      ThreadID         `json:"root_thread_id"`
	Generation        int64            `json:"generation"`
	Revision          int64            `json:"revision"`
	CurrentApprovalID string           `json:"current_approval_id,omitempty"`
	Items             []ApprovalRecord `json:"items"`
	GeneratedAt       time.Time        `json:"generated_at"`
}

func (q ApprovalQueue) Validate() error {
	if strings.TrimSpace(string(q.RootThreadID)) == "" || q.Generation < 0 || q.Revision < 0 {
		return errors.New("approval queue authority is invalid")
	}
	if q.GeneratedAt.IsZero() {
		return errors.New("approval queue requires generated time")
	}
	for index, approval := range q.Items {
		if err := approval.Validate(); err != nil {
			return fmt.Errorf("approval queue item %d: %w", index, err)
		}
		if approval.RootThreadID != q.RootThreadID {
			return fmt.Errorf("approval queue item %d root identity mismatch", index)
		}
		if approval.State != string(sessiontree.ApprovalRequested) && approval.State != string(sessiontree.ApprovalDecisionSubmitted) {
			return fmt.Errorf("approval queue item %d is not queue-visible", index)
		}
	}
	if len(q.Items) == 0 && q.CurrentApprovalID != "" {
		return errors.New("empty approval queue has a current approval")
	}
	if len(q.Items) > 0 && q.CurrentApprovalID != q.Items[0].ApprovalID {
		return errors.New("approval queue current item is not first")
	}
	return nil
}

type ApprovalDecisionReceipt struct {
	DecisionID             string           `json:"decision_id"`
	ApprovalID             string           `json:"approval_id"`
	RootThreadID           ThreadID         `json:"root_thread_id"`
	Decision               ApprovalDecision `json:"decision"`
	State                  string           `json:"state"`
	Reason                 string           `json:"reason,omitempty"`
	AuthorizationProofHash string           `json:"authorization_proof_hash,omitempty"`
	QueueGeneration        int64            `json:"queue_generation"`
	QueueRevision          int64            `json:"queue_revision"`
	ApprovalRevision       int64            `json:"approval_revision"`
	SubmittedAt            time.Time        `json:"submitted_at"`
	ResolvedAt             time.Time        `json:"resolved_at,omitempty"`
}

func (r ApprovalDecisionReceipt) Validate() error {
	if strings.TrimSpace(r.DecisionID) == "" || strings.TrimSpace(r.ApprovalID) == "" || strings.TrimSpace(string(r.RootThreadID)) == "" {
		return errors.New("approval decision receipt identity is incomplete")
	}
	if r.Decision != ApprovalDecisionApprove && r.Decision != ApprovalDecisionReject {
		return errors.New("approval decision receipt decision is invalid")
	}
	if r.QueueGeneration <= 0 || r.QueueRevision <= 0 || r.ApprovalRevision <= 0 || r.SubmittedAt.IsZero() {
		return errors.New("approval decision receipt authority is invalid")
	}
	terminal := !r.ResolvedAt.IsZero()
	switch r.State {
	case string(sessiontree.ApprovalDecisionSubmitted):
		if r.Decision != ApprovalDecisionApprove || terminal || r.Reason != "" || r.AuthorizationProofHash != "" {
			return errors.New("submitted approval receipt is invalid")
		}
	case string(sessiontree.ApprovalApproved):
		if r.Decision != ApprovalDecisionApprove || !terminal || strings.TrimSpace(r.AuthorizationProofHash) == "" || r.Reason != "" {
			return errors.New("approved approval receipt is invalid")
		}
	case string(sessiontree.ApprovalRejected):
		if !terminal || strings.TrimSpace(r.Reason) == "" || r.AuthorizationProofHash != "" ||
			(r.Decision == ApprovalDecisionReject && r.Reason != sessiontree.ApprovalReasonUserRejected) ||
			(r.Decision == ApprovalDecisionApprove && r.Reason == sessiontree.ApprovalReasonUserRejected) {
			return errors.New("rejected approval receipt is invalid")
		}
	case string(sessiontree.ApprovalFailed), string(sessiontree.ApprovalTimedOut), string(sessiontree.ApprovalCancelled):
		if r.Decision != ApprovalDecisionApprove || !terminal || strings.TrimSpace(r.Reason) == "" || r.AuthorizationProofHash != "" {
			return errors.New("terminal approval receipt is invalid")
		}
	default:
		return fmt.Errorf("unsupported approval receipt state %q", r.State)
	}
	return nil
}

type ResolveApprovalResult struct {
	Receipt  ApprovalDecisionReceipt `json:"receipt"`
	Queue    ApprovalQueue           `json:"queue"`
	Approval ApprovalRecord          `json:"approval"`
	Replayed bool                    `json:"replayed,omitempty"`
}

func (r ResolveApprovalResult) Validate() error {
	if err := r.Receipt.Validate(); err != nil {
		return fmt.Errorf("approval decision receipt: %w", err)
	}
	if err := r.Queue.Validate(); err != nil {
		return fmt.Errorf("approval queue: %w", err)
	}
	if err := r.Approval.Validate(); err != nil {
		return fmt.Errorf("approval record: %w", err)
	}
	if r.Approval.ApprovalID != r.Receipt.ApprovalID || r.Approval.RootThreadID != r.Receipt.RootThreadID ||
		r.Approval.DecisionID != r.Receipt.DecisionID || r.Approval.State != r.Receipt.State ||
		r.Approval.Reason != r.Receipt.Reason || r.Approval.AuthorizationProofHash != r.Receipt.AuthorizationProofHash ||
		r.Approval.Revision != r.Receipt.ApprovalRevision || !r.Approval.ResolvedAt.Equal(r.Receipt.ResolvedAt) {
		return errors.New("approval result record and receipt disagree")
	}
	if r.Queue.RootThreadID != r.Approval.RootThreadID || r.Queue.Generation < r.Receipt.QueueGeneration || r.Queue.Revision < r.Receipt.QueueRevision {
		return errors.New("approval result queue authority regressed")
	}
	return nil
}

type PendingToolSettlementResult struct {
	Target                 PendingToolSettlementTarget `json:"target"`
	Event                  ThreadDetailEvent           `json:"event"`
	ProjectionAvailability TurnProjectionAvailability  `json:"projection_availability"`
	Projection             *ThreadTurnProjection       `json:"projection,omitempty"`
	ProjectionError        string                      `json:"projection_error,omitempty"`
}

type ApprovalResource struct {
	Kind  string `json:"kind,omitempty"`
	Value string `json:"value,omitempty"`
}

func (r ApprovalResource) Validate() error {
	if strings.TrimSpace(r.Kind) == "" || strings.TrimSpace(r.Value) == "" {
		return errors.New("approval resource requires kind and value")
	}
	return nil
}

type ApprovalRecord struct {
	ApprovalID             string             `json:"approval_id,omitempty"`
	RootThreadID           ThreadID           `json:"root_thread_id,omitempty"`
	ParentThreadID         ThreadID           `json:"parent_thread_id,omitempty"`
	ToolCallID             string             `json:"tool_call_id,omitempty"`
	EffectAttemptID        string             `json:"effect_attempt_id,omitempty"`
	ToolName               string             `json:"tool_name,omitempty"`
	ToolKind               string             `json:"tool_kind,omitempty"`
	RunID                  RunID              `json:"run_id,omitempty"`
	ThreadID               ThreadID           `json:"thread_id,omitempty"`
	TurnID                 TurnID             `json:"turn_id,omitempty"`
	Step                   int                `json:"step,omitempty"`
	BatchIndex             int                `json:"batch_index"`
	BatchSize              int                `json:"batch_size"`
	State                  string             `json:"state,omitempty"`
	Revision               int64              `json:"revision,omitempty"`
	QueueSequence          int64              `json:"queue_sequence,omitempty"`
	DecisionID             string             `json:"decision_id,omitempty"`
	RequestedAt            time.Time          `json:"requested_at,omitempty"`
	UpdatedAt              time.Time          `json:"updated_at,omitempty"`
	ResolvedAt             time.Time          `json:"resolved_at,omitempty"`
	ArgsHash               string             `json:"args_hash,omitempty"`
	RequestFingerprint     string             `json:"request_fingerprint,omitempty"`
	AuthorizationProofHash string             `json:"authorization_proof_hash,omitempty"`
	Resources              []ApprovalResource `json:"resources,omitempty"`
	Effects                []string           `json:"effects,omitempty"`
	Labels                 map[string]string  `json:"labels,omitempty"`
	HostContext            map[string]string  `json:"host_context,omitempty"`
	ReadOnly               bool               `json:"read_only,omitempty"`
	Destructive            bool               `json:"destructive,omitempty"`
	OpenWorld              bool               `json:"open_world,omitempty"`
	Reason                 string             `json:"reason,omitempty"`
}

func (p ApprovalRecord) Validate() error {
	if strings.TrimSpace(p.ApprovalID) == "" || strings.TrimSpace(p.EffectAttemptID) == "" || strings.TrimSpace(p.ToolCallID) == "" ||
		strings.TrimSpace(string(p.RootThreadID)) == "" {
		return errors.New("approval record requires approval and tool call identities")
	}
	if strings.TrimSpace(p.ToolName) == "" || strings.TrimSpace(p.ToolKind) == "" {
		return errors.New("approval record requires tool name and kind")
	}
	if strings.TrimSpace(string(p.RunID)) == "" || strings.TrimSpace(string(p.ThreadID)) == "" || strings.TrimSpace(string(p.TurnID)) == "" {
		return errors.New("approval record requires run, thread, and turn identities")
	}
	if p.Step <= 0 {
		return errors.New("approval record step must be positive")
	}
	if p.BatchSize <= 0 || p.BatchIndex < 0 || p.BatchIndex >= p.BatchSize {
		return errors.New("approval record batch position is invalid")
	}
	if p.Revision <= 0 || p.QueueSequence <= 0 {
		return errors.New("approval record counters are invalid")
	}
	if p.RequestedAt.IsZero() || p.UpdatedAt.IsZero() || p.UpdatedAt.Before(p.RequestedAt) {
		return errors.New("approval record timestamps are invalid")
	}
	switch p.State {
	case string(sessiontree.ApprovalRequested):
		if p.DecisionID != "" || p.Reason != "" || p.AuthorizationProofHash != "" || !p.ResolvedAt.IsZero() {
			return errors.New("requested approval authority is invalid")
		}
	case string(sessiontree.ApprovalDecisionSubmitted):
		if strings.TrimSpace(p.DecisionID) == "" || p.Reason != "" || p.AuthorizationProofHash != "" || !p.ResolvedAt.IsZero() {
			return errors.New("submitted approval authority is invalid")
		}
	case string(sessiontree.ApprovalApproved):
		if strings.TrimSpace(p.DecisionID) == "" || strings.TrimSpace(p.AuthorizationProofHash) == "" || p.Reason != "" || p.ResolvedAt.IsZero() {
			return errors.New("approved approval authority is invalid")
		}
	case string(sessiontree.ApprovalRejected), string(sessiontree.ApprovalFailed), string(sessiontree.ApprovalTimedOut), string(sessiontree.ApprovalCancelled):
		if strings.TrimSpace(p.DecisionID) == "" || strings.TrimSpace(p.Reason) == "" || p.AuthorizationProofHash != "" || p.ResolvedAt.IsZero() {
			return errors.New("terminal approval authority is invalid")
		}
	default:
		return fmt.Errorf("unsupported approval record state %q", p.State)
	}
	if strings.TrimSpace(p.ArgsHash) == "" || strings.TrimSpace(p.RequestFingerprint) == "" {
		return errors.New("approval record requires argument and request fingerprints")
	}
	for index, resource := range p.Resources {
		if err := resource.Validate(); err != nil {
			return fmt.Errorf("approval resource %d: %w", index, err)
		}
	}
	for index, effect := range p.Effects {
		if strings.TrimSpace(effect) == "" {
			return fmt.Errorf("approval effect %d is empty", index)
		}
	}
	return nil
}

type ThreadDetailEventKind string

const (
	ThreadDetailEventUserMessage      ThreadDetailEventKind = "user_message"
	ThreadDetailEventAssistantMessage ThreadDetailEventKind = "assistant_message"
	ThreadDetailEventToolCall         ThreadDetailEventKind = "tool_call"
	ThreadDetailEventToolDispatch     ThreadDetailEventKind = "tool_dispatch"
	ThreadDetailEventToolActivity     ThreadDetailEventKind = "tool_activity"
	ThreadDetailEventToolResult       ThreadDetailEventKind = "tool_result"
	ThreadDetailEventTurnMarker       ThreadDetailEventKind = "turn_marker"
	ThreadDetailEventCompaction       ThreadDetailEventKind = "compaction"
	ThreadDetailEventError            ThreadDetailEventKind = "error"
	ThreadDetailEventApproval         ThreadDetailEventKind = "approval"
	ThreadDetailEventInput            ThreadDetailEventKind = "input"
	ThreadDetailEventCustom           ThreadDetailEventKind = "custom"
)

type ThreadDetailEvent struct {
	ID        string                `json:"id"`
	Ordinal   int64                 `json:"ordinal"`
	ParentID  string                `json:"parent_id,omitempty"`
	ThreadID  ThreadID              `json:"thread_id"`
	TurnID    TurnID                `json:"turn_id,omitempty"`
	RunID     RunID                 `json:"run_id,omitempty"`
	Step      int                   `json:"step,omitempty"`
	Kind      ThreadDetailEventKind `json:"kind"`
	Type      string                `json:"type,omitempty"`
	CreatedAt time.Time             `json:"created_at"`

	Message    *ThreadDetailMessage    `json:"message,omitempty"`
	ToolCall   *ThreadDetailToolCall   `json:"tool_call,omitempty"`
	ToolResult *ThreadDetailToolResult `json:"tool_result,omitempty"`
	Approval   *ThreadDetailApproval   `json:"approval,omitempty"`
	TurnMarker *ThreadDetailTurnMarker `json:"turn_marker,omitempty"`
	Compaction *ThreadDetailCompaction `json:"compaction,omitempty"`
	Error      string                  `json:"error,omitempty"`
	Metadata   map[string]string       `json:"metadata,omitempty"`

	ActivityTimeline *observation.ActivityTimeline `json:"activity_timeline,omitempty"`
}

type ThreadDetailMessage struct {
	Role        string                            `json:"role,omitempty"`
	Kind        string                            `json:"kind,omitempty"`
	Preview     string                            `json:"preview,omitempty"`
	Content     string                            `json:"content,omitempty"`
	Attachments []MessageAttachment               `json:"attachments,omitempty"`
	References  []MessageReference                `json:"references,omitempty"`
	Reasoning   string                            `json:"reasoning,omitempty"`
	Activity    *observation.ActivityPresentation `json:"activity,omitempty"`
}

type ThreadDetailToolCall struct {
	ID            string                     `json:"id,omitempty"`
	Name          string                     `json:"name,omitempty"`
	ArgsPreview   string                     `json:"args_preview,omitempty"`
	ArgsJSON      string                     `json:"args_json,omitempty"`
	ArgsHash      string                     `json:"args_hash,omitempty"`
	ControlSignal *ThreadDetailControlSignal `json:"control_signal,omitempty"`
}

type ThreadDetailControlSignal struct {
	Name        string         `json:"name,omitempty"`
	CallID      string         `json:"call_id,omitempty"`
	Disposition string         `json:"disposition,omitempty"`
	Text        string         `json:"text,omitempty"`
	ArgsHash    string         `json:"args_hash,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type ThreadDetailToolResult struct {
	CallID          string       `json:"call_id,omitempty"`
	ToolName        string       `json:"tool_name,omitempty"`
	EffectAttemptID string       `json:"effect_attempt_id,omitempty"`
	Status          string       `json:"status,omitempty"`
	Preview         string       `json:"preview,omitempty"`
	Content         string       `json:"content,omitempty"`
	Truncated       bool         `json:"truncated,omitempty"`
	OriginalBytes   int          `json:"original_bytes,omitempty"`
	VisibleBytes    int          `json:"visible_bytes,omitempty"`
	OriginalLines   int          `json:"original_lines,omitempty"`
	VisibleLines    int          `json:"visible_lines,omitempty"`
	Strategy        string       `json:"strategy,omitempty"`
	ContentSHA256   string       `json:"content_sha256,omitempty"`
	FullOutput      *ArtifactRef `json:"full_output,omitempty"`
}

type ThreadDetailApproval struct {
	State    string            `json:"state,omitempty"`
	ToolID   string            `json:"tool_id,omitempty"`
	ToolName string            `json:"tool_name,omitempty"`
	ToolKind string            `json:"tool_kind,omitempty"`
	ArgsHash string            `json:"args_hash,omitempty"`
	Reason   string            `json:"reason,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type ThreadDetailTurnMarker struct {
	Status   string            `json:"status,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type ThreadDetailCompaction struct {
	OperationID             string            `json:"operation_id,omitempty"`
	RequestID               string            `json:"request_id,omitempty"`
	Source                  string            `json:"source,omitempty"`
	CompactionID            string            `json:"compaction_id,omitempty"`
	PreviousCompactionID    string            `json:"previous_compaction_id,omitempty"`
	CompactedThroughEntryID string            `json:"compacted_through_entry_id,omitempty"`
	SummarySchemaVersion    string            `json:"summary_schema_version,omitempty"`
	CompactionGeneration    int               `json:"compaction_generation,omitempty"`
	CompactionWindowID      string            `json:"compaction_window_id,omitempty"`
	FirstKeptEntryID        string            `json:"first_kept_entry_id,omitempty"`
	KeptUserEntryIDs        []string          `json:"kept_user_entry_ids,omitempty"`
	Summary                 string            `json:"summary,omitempty"`
	Trigger                 string            `json:"trigger,omitempty"`
	Reason                  string            `json:"reason,omitempty"`
	Phase                   string            `json:"phase,omitempty"`
	TokensBefore            int64             `json:"tokens_before,omitempty"`
	TokensAfterEstimate     int64             `json:"tokens_after_estimate,omitempty"`
	Metadata                map[string]string `json:"metadata,omitempty"`
}

type ArtifactRef struct {
	ID        ArtifactID `json:"id,omitempty"`
	SafeLabel string     `json:"safe_label,omitempty"`
	Kind      string     `json:"kind,omitempty"`
	MIME      string     `json:"mime,omitempty"`
	SizeBytes int64      `json:"size_bytes,omitempty"`
	SHA256    string     `json:"sha256,omitempty"`
}

type ReadArtifactRequest struct {
	ThreadID   ThreadID   `json:"thread_id"`
	ArtifactID ArtifactID `json:"artifact_id"`
}

type ArtifactContent struct {
	Ref  ArtifactRef `json:"ref"`
	Text string      `json:"text"`
}

type RunLabels struct {
	Correlation map[string]string
	Host        map[string]string
}

type ThreadSnapshot struct {
	ID               ThreadID     `json:"id"`
	Title            string       `json:"title,omitempty"`
	TitleStatus      string       `json:"title_status,omitempty"`
	TitleSource      string       `json:"title_source,omitempty"`
	TitleUpdatedAt   time.Time    `json:"title_updated_at,omitempty"`
	TitleError       string       `json:"title_error,omitempty"`
	TitleGeneration  int64        `json:"title_generation,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
	Phase            ThreadPhase  `json:"phase"`
	Status           ThreadStatus `json:"status"`
	LatestTurnID     TurnID       `json:"latest_turn_id,omitempty"`
	LatestRunID      RunID        `json:"latest_run_id,omitempty"`
	ThroughOrdinal   int64        `json:"through_ordinal"`
	WaitingPrompt    string       `json:"waiting_prompt,omitempty"`
	Recoverable      bool         `json:"recoverable"`
	CanAppendMessage bool         `json:"can_append_message"`
	CanRetry         bool         `json:"can_retry"`
}

type ThreadSummary struct {
	ID               ThreadID     `json:"id"`
	Title            string       `json:"title,omitempty"`
	TitleStatus      string       `json:"title_status,omitempty"`
	TitleSource      string       `json:"title_source,omitempty"`
	TitleUpdatedAt   time.Time    `json:"title_updated_at,omitempty"`
	TitleError       string       `json:"title_error,omitempty"`
	TitleGeneration  int64        `json:"title_generation,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
	Phase            ThreadPhase  `json:"phase"`
	Status           ThreadStatus `json:"status"`
	LatestTurnID     TurnID       `json:"latest_turn_id,omitempty"`
	WaitingPrompt    string       `json:"waiting_prompt,omitempty"`
	Recoverable      bool         `json:"recoverable"`
	CanAppendMessage bool         `json:"can_append_message"`
	CanRetry         bool         `json:"can_retry"`
}

type TurnResult struct {
	ThreadID               ThreadID                       `json:"thread_id"`
	TurnID                 TurnID                         `json:"turn_id"`
	RunID                  RunID                          `json:"run_id"`
	Status                 TurnStatus                     `json:"status"`
	Output                 string                         `json:"output,omitempty"`
	Failure                *ThreadTurnFailure             `json:"failure,omitempty"`
	Diagnostics            map[string]string              `json:"diagnostics,omitempty"`
	Metrics                RunMetrics                     `json:"metrics"`
	CompletionReason       observation.CompletionReason   `json:"completion_reason,omitempty"`
	ContinuationReason     observation.ContinuationReason `json:"continuation_reason,omitempty"`
	FinishReason           observation.FinishReason       `json:"finish_reason,omitempty"`
	RawFinishReason        string                         `json:"raw_finish_reason,omitempty"`
	FinishInferred         bool                           `json:"finish_inferred,omitempty"`
	Signal                 *TurnSignal                    `json:"signal,omitempty"`
	ActivityTimeline       observation.ActivityTimeline   `json:"activity_timeline"`
	ProjectionAvailability TurnProjectionAvailability     `json:"projection_availability"`
	Projection             *ThreadTurnProjection          `json:"projection,omitempty"`
	ProjectionError        string                         `json:"projection_error,omitempty"`
	Replayed               bool                           `json:"replayed,omitempty"`
}

type TurnProjectionAvailability string

const (
	TurnProjectionAvailabilityReady       TurnProjectionAvailability = "ready"
	TurnProjectionAvailabilityUnavailable TurnProjectionAvailability = "unavailable"
)

func (a TurnProjectionAvailability) Valid() bool {
	return a == TurnProjectionAvailabilityReady || a == TurnProjectionAvailabilityUnavailable
}

func validateTurnProjectionOutcome(availability TurnProjectionAvailability, projection *ThreadTurnProjection, projectionError string) error {
	if !availability.Valid() {
		return fmt.Errorf("unsupported turn projection availability %q", availability)
	}
	switch availability {
	case TurnProjectionAvailabilityReady:
		if projection == nil {
			return errors.New("ready turn projection is required")
		}
		if strings.TrimSpace(projectionError) != "" {
			return errors.New("ready turn projection must not include an error")
		}
		if err := projection.Validate(); err != nil {
			return fmt.Errorf("invalid ready turn projection: %w", err)
		}
	case TurnProjectionAvailabilityUnavailable:
		if projection != nil {
			return errors.New("unavailable turn projection must not include a projection")
		}
		if strings.TrimSpace(projectionError) == "" {
			return errors.New("unavailable turn projection requires an error")
		}
	}
	return nil
}

func (r TurnResult) Validate() error {
	if strings.TrimSpace(string(r.ThreadID)) == "" || strings.TrimSpace(string(r.TurnID)) == "" || strings.TrimSpace(string(r.RunID)) == "" {
		return errors.New("turn result requires thread, turn, and run identities")
	}
	if !r.Status.Valid() || (!r.Status.IsTerminal() && !(r.Replayed && r.Status == TurnStatusRunning)) {
		return fmt.Errorf("turn result requires terminal status or a replayed running status, got %q", r.Status)
	}
	if err := validateThreadTurnFailureForStatus(r.Status, r.Failure); err != nil {
		return err
	}
	if r.CompletionReason != "" && !r.CompletionReason.Valid() {
		return fmt.Errorf("unsupported turn completion reason %q", r.CompletionReason)
	}
	if r.ContinuationReason != "" && !r.ContinuationReason.Valid() {
		return fmt.Errorf("unsupported turn continuation reason %q", r.ContinuationReason)
	}
	if r.CompletionReason != "" && r.ContinuationReason != "" {
		return errors.New("turn result cannot complete and continue simultaneously")
	}
	if r.FinishReason != "" && !r.FinishReason.Valid() {
		return fmt.Errorf("unsupported turn finish reason %q", r.FinishReason)
	}
	if r.FinishInferred && r.FinishReason == "" {
		return errors.New("inferred turn finish requires finish reason")
	}
	if err := observation.ValidateActivityTimeline(r.ActivityTimeline); err != nil {
		return fmt.Errorf("invalid turn result activity timeline: %w", err)
	}
	if r.ActivityTimeline.ThreadID != string(r.ThreadID) || r.ActivityTimeline.TurnID != string(r.TurnID) || r.ActivityTimeline.RunID != string(r.RunID) || r.ActivityTimeline.TraceID != string(r.RunID) {
		return errors.New("turn result activity timeline identity mismatch")
	}
	if err := validateTurnProjectionOutcome(r.ProjectionAvailability, r.Projection, r.ProjectionError); err != nil {
		return err
	}
	if r.Projection == nil {
		return nil
	}
	if r.Projection.ThreadID != r.ThreadID || r.Projection.TurnID != r.TurnID || r.Projection.RunID != r.RunID {
		return errors.New("turn result projection identity mismatch")
	}
	if r.Projection.Status != r.Status {
		return fmt.Errorf("turn result projection status %q does not match result status %q", r.Projection.Status, r.Status)
	}
	return nil
}

func (r PendingToolSettlementResult) Validate() error {
	if err := validatePendingToolSettlementTarget(r.Target); err != nil {
		return fmt.Errorf("invalid pending tool settlement target: %w", err)
	}
	if r.Event.ThreadID != r.Target.ThreadID || r.Event.TurnID != r.Target.TurnID {
		return errors.New("pending tool settlement event thread identity mismatch")
	}
	if r.Event.Kind != ThreadDetailEventToolResult || r.Event.Type != threadTurnProjectionPendingToolSettlementType || r.Event.ToolResult == nil {
		return errors.New("pending tool settlement result requires a settlement tool result event")
	}
	if strings.TrimSpace(r.Event.ToolResult.CallID) != strings.TrimSpace(r.Target.ToolCallID) ||
		strings.TrimSpace(r.Event.ToolResult.ToolName) != strings.TrimSpace(r.Target.ToolName) ||
		strings.TrimSpace(r.Event.Metadata["run_id"]) != strings.TrimSpace(string(r.Target.RunID)) ||
		strings.TrimSpace(r.Event.Metadata["handle"]) != strings.TrimSpace(r.Target.Handle) {
		return errors.New("pending tool settlement event target mismatch")
	}
	if err := validateTurnProjectionOutcome(r.ProjectionAvailability, r.Projection, r.ProjectionError); err != nil {
		return err
	}
	if r.Projection == nil {
		return nil
	}
	if r.Projection.ThreadID != r.Target.ThreadID || r.Projection.TurnID != r.Target.TurnID || r.Projection.RunID != r.Target.RunID {
		return errors.New("pending tool settlement projection identity mismatch")
	}
	return nil
}

type CompactThreadResult struct {
	ThreadID         ThreadID                     `json:"thread_id"`
	RunID            RunID                        `json:"run_id"`
	RequestID        string                       `json:"request_id"`
	Compaction       observation.CompactionEvent  `json:"compaction"`
	Metrics          RunMetrics                   `json:"metrics"`
	ActivityTimeline observation.ActivityTimeline `json:"activity_timeline"`
	Replayed         bool                         `json:"replayed,omitempty"`
}

func (r CompactThreadResult) Validate() error {
	if strings.TrimSpace(string(r.ThreadID)) == "" || strings.TrimSpace(string(r.RunID)) == "" || strings.TrimSpace(r.RequestID) == "" {
		return errors.New("compact thread result requires thread, run, and request identities")
	}
	if err := r.Compaction.Validate(); err != nil {
		return fmt.Errorf("invalid compact thread result: %w", err)
	}
	if strings.TrimSpace(r.Compaction.ThreadID) != string(r.ThreadID) || strings.TrimSpace(r.Compaction.RunID) != string(r.RunID) || strings.TrimSpace(r.Compaction.RequestID) != strings.TrimSpace(r.RequestID) {
		return errors.New("compact thread result identity mismatch")
	}
	if r.Compaction.TurnID != "" {
		return fmt.Errorf("standalone thread compaction must not include turn id %q", r.Compaction.TurnID)
	}
	if r.Compaction.Status == observation.CompactionStatusRunning {
		return errors.New("compact thread result requires terminal compaction status")
	}
	if strings.TrimSpace(r.Compaction.OperationID) == "" || strings.TrimSpace(r.Compaction.Source) == "" {
		return errors.New("compact thread result requires operation and source identities")
	}
	if err := observation.ValidateActivityTimeline(r.ActivityTimeline); err != nil {
		return fmt.Errorf("invalid compact thread result activity timeline: %w", err)
	}
	if r.ActivityTimeline.ThreadID != string(r.ThreadID) || r.ActivityTimeline.TurnID != "" || r.ActivityTimeline.RunID != string(r.RunID) || r.ActivityTimeline.TraceID != string(r.RunID) {
		return errors.New("compact thread result activity timeline identity mismatch")
	}
	return nil
}

type EventSink interface {
	EmitEvent(Event)
}

type Event struct {
	Type               observation.EventType             `json:"type"`
	TraceID            TraceID                           `json:"trace_id,omitempty"`
	RunID              RunID                             `json:"run_id,omitempty"`
	ThreadID           ThreadID                          `json:"thread_id,omitempty"`
	TurnID             TurnID                            `json:"turn_id,omitempty"`
	Step               int                               `json:"step,omitempty"`
	Provider           string                            `json:"provider,omitempty"`
	Model              string                            `json:"model,omitempty"`
	Message            string                            `json:"message,omitempty"`
	Result             string                            `json:"result,omitempty"`
	Error              string                            `json:"error,omitempty"`
	ToolID             string                            `json:"tool_id,omitempty"`
	ToolName           string                            `json:"tool_name,omitempty"`
	ToolKind           string                            `json:"tool_kind,omitempty"`
	ArgsHash           string                            `json:"args_hash,omitempty"`
	DurationMS         int64                             `json:"duration_ms,omitempty"`
	FinishReason       observation.FinishReason          `json:"finish_reason,omitempty"`
	RawFinishReason    string                            `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool                              `json:"finish_inferred,omitempty"`
	CompletionReason   observation.CompletionReason      `json:"completion_reason,omitempty"`
	ContinuationReason observation.ContinuationReason    `json:"continuation_reason,omitempty"`
	Activity           *observation.ActivityPresentation `json:"activity,omitempty"`
	ActivityTimeline   *observation.ActivityTimeline     `json:"activity_timeline,omitempty"`
	Projection         *ThreadTurnProjection             `json:"projection,omitempty"`
	Stream             *StreamObservation                `json:"stream,omitempty"`
	Committed          *ThreadDetailEvent                `json:"committed,omitempty"`
	ContextStatus      *observation.ContextStatus        `json:"context_status,omitempty"`
	Compaction         *observation.CompactionEvent      `json:"compaction,omitempty"`
	CompactionDebug    *observation.CompactionDebugEvent `json:"compaction_debug,omitempty"`
	Sources            []SourceRef                       `json:"sources,omitempty"`
	Metadata           map[string]any                    `json:"metadata,omitempty"`
	Timestamp          time.Time                         `json:"timestamp,omitempty"`
}

func (e Event) Validate() error {
	if !e.Type.Valid() {
		return fmt.Errorf("unsupported runtime event type %q", e.Type)
	}
	if e.FinishReason != "" && !e.FinishReason.Valid() {
		return fmt.Errorf("unsupported finish reason %q", e.FinishReason)
	}
	if e.CompletionReason != "" && !e.CompletionReason.Valid() {
		return fmt.Errorf("unsupported completion reason %q", e.CompletionReason)
	}
	if e.ContinuationReason != "" && !e.ContinuationReason.Valid() {
		return fmt.Errorf("unsupported continuation reason %q", e.ContinuationReason)
	}
	if e.CompletionReason != "" && e.ContinuationReason != "" {
		return errors.New("runtime event cannot complete and continue simultaneously")
	}
	if e.FinishInferred && e.FinishReason == "" {
		return errors.New("runtime event inferred finish requires finish reason")
	}
	if e.ContextStatus != nil {
		if err := e.ContextStatus.Validate(); err != nil {
			return fmt.Errorf("invalid context status: %w", err)
		}
		if !eventIdentityMatches(e, e.ContextStatus.RunID, e.ContextStatus.ThreadID, e.ContextStatus.TurnID, e.ContextStatus.Step) {
			return errors.New("runtime event context status identity mismatch")
		}
	}
	if e.Compaction != nil {
		if err := e.Compaction.Validate(); err != nil {
			return fmt.Errorf("invalid compaction event: %w", err)
		}
		if !eventIdentityMatches(e, e.Compaction.RunID, e.Compaction.ThreadID, e.Compaction.TurnID, e.Compaction.Step) {
			return errors.New("runtime event compaction identity mismatch")
		}
	}
	if e.CompactionDebug != nil {
		if err := e.CompactionDebug.Validate(); err != nil {
			return fmt.Errorf("invalid compaction debug event: %w", err)
		}
		if !eventIdentityMatches(e, e.CompactionDebug.RunID, e.CompactionDebug.ThreadID, e.CompactionDebug.TurnID, e.CompactionDebug.Step) {
			return errors.New("runtime event compaction debug identity mismatch")
		}
	}
	if e.Stream != nil {
		if err := e.Stream.Validate(); err != nil {
			return fmt.Errorf("invalid stream observation: %w", err)
		}
	}
	if e.ActivityTimeline != nil {
		if err := observation.ValidateActivityTimeline(*e.ActivityTimeline); err != nil {
			return fmt.Errorf("invalid event activity timeline: %w", err)
		}
		if e.ActivityTimeline.RunID != string(e.RunID) || e.ActivityTimeline.ThreadID != string(e.ThreadID) || e.ActivityTimeline.TurnID != string(e.TurnID) {
			return errors.New("runtime event activity timeline identity mismatch")
		}
	}
	if e.Projection != nil {
		if err := e.Projection.Validate(); err != nil {
			return fmt.Errorf("invalid event turn projection: %w", err)
		}
		if e.ThreadID != e.Projection.ThreadID || e.TurnID != e.Projection.TurnID || e.RunID != e.Projection.RunID {
			return errors.New("runtime event projection identity mismatch")
		}
	}
	if e.Type == observation.EventTypeThreadEntryCommitted && e.Committed == nil {
		return errors.New("runtime thread entry committed event requires committed detail")
	}
	if e.Type != observation.EventTypeThreadEntryCommitted && e.Committed != nil {
		return errors.New("runtime committed detail requires thread entry committed event type")
	}
	if e.Committed != nil {
		if e.Committed.ThreadID != e.ThreadID || e.Committed.TurnID != e.TurnID || e.Committed.RunID != e.RunID || e.Committed.Step != e.Step {
			return errors.New("runtime event committed detail identity mismatch")
		}
		if err := validateCommittedUserMessage(*e.Committed); err != nil {
			return err
		}
	}
	return nil
}

func validateCommittedUserMessage(committed ThreadDetailEvent) error {
	if committed.Kind != ThreadDetailEventUserMessage {
		return nil
	}
	if strings.TrimSpace(committed.ID) == "" || strings.TrimSpace(string(committed.ThreadID)) == "" ||
		strings.TrimSpace(string(committed.TurnID)) == "" || strings.TrimSpace(string(committed.RunID)) == "" {
		return errors.New("runtime committed user message requires entry, thread, turn, and run identities")
	}
	if committed.CreatedAt.IsZero() {
		return errors.New("runtime committed user message requires creation time")
	}
	if committed.Message == nil || strings.TrimSpace(committed.Message.Role) != string(session.User) {
		return errors.New("runtime committed user message requires user payload")
	}
	if strings.TrimSpace(committed.Message.Content) == "" && strings.TrimSpace(committed.Message.Preview) == "" && len(committed.Message.Attachments) == 0 && len(committed.Message.References) == 0 {
		return errors.New("runtime committed user message requires preview, content, attachments, or references")
	}
	for index, attachment := range committed.Message.Attachments {
		if err := attachment.Validate(); err != nil {
			return fmt.Errorf("runtime committed user message attachment %d: %w", index, err)
		}
	}
	if err := validateMessageReferences(committed.Message.References); err != nil {
		return fmt.Errorf("runtime committed user message references: %w", err)
	}
	return nil
}

func eventIdentityMatches(e Event, runID, threadID, turnID string, step int) bool {
	return strings.TrimSpace(runID) == string(e.RunID) &&
		strings.TrimSpace(threadID) == string(e.ThreadID) &&
		strings.TrimSpace(turnID) == string(e.TurnID) &&
		step == e.Step
}

type StreamObservationType string

const (
	StreamObservationAssistantDelta   StreamObservationType = "assistant_delta"
	StreamObservationReasoningDelta   StreamObservationType = "reasoning_delta"
	StreamObservationToolCallStart    StreamObservationType = "tool_call_start"
	StreamObservationToolCallDelta    StreamObservationType = "tool_call_delta"
	StreamObservationToolCallEnd      StreamObservationType = "tool_call_end"
	StreamObservationModelRetry       StreamObservationType = "model_retry"
	StreamObservationModelStreamDone  StreamObservationType = "model_stream_done"
	StreamObservationModelStreamAbort StreamObservationType = "model_stream_abort"
)

func (t StreamObservationType) Valid() bool {
	switch t {
	case StreamObservationAssistantDelta,
		StreamObservationReasoningDelta,
		StreamObservationToolCallStart,
		StreamObservationToolCallDelta,
		StreamObservationToolCallEnd,
		StreamObservationModelRetry,
		StreamObservationModelStreamDone,
		StreamObservationModelStreamAbort:
		return true
	default:
		return false
	}
}

// StreamObservation is a provider-neutral, engine-confirmed streaming fact for
// hosts that render live assistant output from Floret runtime events.
type StreamObservation struct {
	Type            StreamObservationType    `json:"type"`
	Text            string                   `json:"text,omitempty"`
	ToolCallStream  *ModelToolCallStream     `json:"tool_call_stream,omitempty"`
	Reason          string                   `json:"reason,omitempty"`
	FinishReason    observation.FinishReason `json:"finish_reason,omitempty"`
	RawFinishReason string                   `json:"raw_finish_reason,omitempty"`
	FinishInferred  bool                     `json:"finish_inferred,omitempty"`
	Attempt         int                      `json:"attempt,omitempty"`
	Labels          RunLabels                `json:"labels,omitempty"`
}

func (s StreamObservation) Validate() error {
	if !s.Type.Valid() {
		return fmt.Errorf("unsupported stream observation type %q", s.Type)
	}
	if s.FinishReason != "" && !s.FinishReason.Valid() {
		return fmt.Errorf("unsupported stream finish reason %q", s.FinishReason)
	}
	if s.FinishInferred && s.FinishReason == "" {
		return errors.New("inferred stream finish requires finish reason")
	}
	return nil
}

type ThreadStatus string

const (
	ThreadStatusIdle        ThreadStatus = "idle"
	ThreadStatusRunning     ThreadStatus = "running"
	ThreadStatusCompleted   ThreadStatus = "completed"
	ThreadStatusWaiting     ThreadStatus = "waiting"
	ThreadStatusFailed      ThreadStatus = "failed"
	ThreadStatusCancelled   ThreadStatus = "cancelled"
	ThreadStatusInterrupted ThreadStatus = "interrupted"
)

type ThreadPhase string

const (
	ThreadPhaseIdle ThreadPhase = "idle"
	ThreadPhaseTurn ThreadPhase = "turn"
)

type TurnStatus string

const (
	TurnStatusRunning     TurnStatus = "running"
	TurnStatusCompleted   TurnStatus = "completed"
	TurnStatusWaiting     TurnStatus = "waiting"
	TurnStatusFailed      TurnStatus = "failed"
	TurnStatusCancelled   TurnStatus = "cancelled"
	TurnStatusInterrupted TurnStatus = "interrupted"
)

func (s TurnStatus) Valid() bool {
	switch s {
	case TurnStatusRunning, TurnStatusCompleted, TurnStatusWaiting, TurnStatusFailed, TurnStatusCancelled, TurnStatusInterrupted:
		return true
	default:
		return false
	}
}

func (s TurnStatus) IsTerminal() bool {
	switch s {
	case TurnStatusCompleted, TurnStatusWaiting, TurnStatusFailed, TurnStatusCancelled, TurnStatusInterrupted:
		return true
	default:
		return false
	}
}

type ThreadTurnFailureCode string

const (
	ThreadTurnFailureCancelled                ThreadTurnFailureCode = "cancelled"
	ThreadTurnFailureInterrupted              ThreadTurnFailureCode = "interrupted"
	ThreadTurnFailureProvider                 ThreadTurnFailureCode = "provider"
	ThreadTurnFailureToolDispatch             ThreadTurnFailureCode = "tool_dispatch"
	ThreadTurnFailureEffectOutcomeUnknown     ThreadTurnFailureCode = "effect_outcome_unknown"
	ThreadTurnFailureAuthorizationUnavailable ThreadTurnFailureCode = "authorization_unavailable"
	ThreadTurnFailureAuthorizationContract    ThreadTurnFailureCode = "authorization_contract"
	ThreadTurnFailureStorage                  ThreadTurnFailureCode = "storage"
	ThreadTurnFailureEngineContract           ThreadTurnFailureCode = "engine_contract"
	ThreadTurnFailureLegacyUnclassified       ThreadTurnFailureCode = "legacy_unclassified"
)

func (c ThreadTurnFailureCode) Valid() bool {
	switch c {
	case ThreadTurnFailureCancelled,
		ThreadTurnFailureInterrupted,
		ThreadTurnFailureProvider,
		ThreadTurnFailureToolDispatch,
		ThreadTurnFailureEffectOutcomeUnknown,
		ThreadTurnFailureAuthorizationUnavailable,
		ThreadTurnFailureAuthorizationContract,
		ThreadTurnFailureStorage,
		ThreadTurnFailureEngineContract,
		ThreadTurnFailureLegacyUnclassified:
		return true
	default:
		return false
	}
}

type ThreadTurnFailure struct {
	Code    ThreadTurnFailureCode `json:"code"`
	Message string                `json:"message"`
}

func (f ThreadTurnFailure) Validate() error {
	if !f.Code.Valid() {
		return fmt.Errorf("unsupported thread turn failure code %q", f.Code)
	}
	if strings.TrimSpace(f.Message) == "" {
		return errors.New("thread turn failure requires a message")
	}
	return nil
}

func validateThreadTurnFailureForStatus(status TurnStatus, failure *ThreadTurnFailure) error {
	requiresFailure := status == TurnStatusFailed || status == TurnStatusCancelled || status == TurnStatusInterrupted
	if failure == nil {
		if requiresFailure {
			return fmt.Errorf("turn status %q requires a failure", status)
		}
		return nil
	}
	if !requiresFailure {
		return fmt.Errorf("turn status %q must not include a failure", status)
	}
	if err := failure.Validate(); err != nil {
		return err
	}
	if status == TurnStatusCancelled && failure.Code != ThreadTurnFailureCancelled {
		return errors.New("cancelled turn requires cancelled failure code")
	}
	if status == TurnStatusInterrupted && failure.Code != ThreadTurnFailureInterrupted {
		return errors.New("interrupted turn requires interrupted failure code")
	}
	if status == TurnStatusFailed && (failure.Code == ThreadTurnFailureCancelled || failure.Code == ThreadTurnFailureInterrupted) {
		return errors.New("failed turn cannot use cancelled or interrupted failure code")
	}
	return nil
}

type Store struct {
	self              *Store
	repo              sessiontree.Repo
	prompt            cache.Store
	forkOperations    storage.ForkOperationStore
	agentTodos        sessiontree.AgentTodoStateRepo
	rootAuthority     sessiontree.RootAuthorityRepo
	deleteCleanup     func(context.Context, []string) error
	threadAuthorityMu sync.Mutex
	turnExecutionMu   sync.Mutex
	turnExecutions    map[string]sessiontree.TurnLease
	bootstrapMu       sync.Mutex
	bootstrapIssued   bool
	titleRecoveryMu   sync.Mutex
	titleRecoveryDone bool
	close             func() error
	lifetimeMu        sync.Mutex
	lifetimeCond      *sync.Cond
	lifetimeState     storeLifetimeState
	activeOperations  int
	backgroundErr     error
	closeInProgress   bool
	lifetimeCtx       context.Context
	lifetimeCancel    context.CancelFunc
}

type storeLifetimeState string

const (
	storeLifetimeOpen    storeLifetimeState = "open"
	storeLifetimeClosing storeLifetimeState = "closing"
	storeLifetimeClosed  storeLifetimeState = "closed"
)

func NewMemoryStore() *Store {
	repo := sessiontree.NewMemoryRepo()
	prompt := cache.NewMemoryStore()
	forkOperations := storage.NewMemoryForkOperationStore(repo)
	store := &Store{
		repo:           repo,
		prompt:         prompt,
		forkOperations: forkOperations,
		agentTodos:     repo,
		rootAuthority:  repo,
		deleteCleanup: func(ctx context.Context, threadIDs []string) error {
			if err := prompt.DeletePromptScopes(ctx, threadIDs...); err != nil {
				return err
			}
			return nil
		},
	}
	store.self = store
	store.initLifetime()
	return store
}

func OpenSQLiteStore(path string) (*Store, error) {
	sqliteStore, err := sqlite.Open(path)
	if err != nil {
		var unsupported *storage.UnsupportedStoreSchemaError
		if errors.As(err, &unsupported) {
			return nil, &UnsupportedStoreSchemaError{
				ObservedVersion: unsupported.ObservedVersion, ObservedFingerprint: unsupported.ObservedFingerprint,
				CurrentVersion: unsupported.CurrentVersion, CurrentFingerprint: unsupported.CurrentFingerprint,
				PredecessorVersion: unsupported.PredecessorVersion, PredecessorFingerprint: unsupported.PredecessorFingerprint,
			}
		}
		var mismatch *storage.StoreLeasePolicyMismatchError
		if errors.As(err, &mismatch) {
			return nil, &StoreLeasePolicyMismatchError{
				Configured: StoreLeasePolicy{
					TTL: mismatch.Configured.TTL, RenewInterval: mismatch.Configured.RenewInterval,
					ClockSkewAllowance: mismatch.Configured.ClockSkewAllowance,
				},
				Persisted: StoreLeasePolicy{
					TTL: mismatch.Persisted.TTL, RenewInterval: mismatch.Persisted.RenewInterval,
					ClockSkewAllowance: mismatch.Persisted.ClockSkewAllowance,
				},
			}
		}
		if errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
			return nil, fmt.Errorf("%w: %w", ErrAuthorityCorrupt, err)
		}
		if errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
			return nil, fmt.Errorf("%w: %w", ErrThreadAuthorityInvariant, err)
		}
		return nil, err
	}
	store := &Store{
		repo:           sqliteStore,
		prompt:         sqliteStore,
		forkOperations: sqliteStore,
		agentTodos:     sqliteStore,
		rootAuthority:  sqliteStore,
		deleteCleanup:  func(context.Context, []string) error { return nil },
		close:          sqliteStore.Close,
	}
	store.self = store
	store.initLifetime()
	return store, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if err := s.validateIdentity(); err != nil {
		return err
	}
	s.lifetimeMu.Lock()
	s.initLifetimeLocked()
	if s.lifetimeState == storeLifetimeClosed {
		err := s.backgroundErr
		s.lifetimeMu.Unlock()
		return err
	}
	if s.lifetimeState == storeLifetimeOpen {
		s.lifetimeState = storeLifetimeClosing
		if s.lifetimeCancel != nil {
			s.lifetimeCancel()
		}
	}
	for s.closeInProgress {
		s.lifetimeCond.Wait()
		if s.lifetimeState == storeLifetimeClosed {
			err := s.backgroundErr
			s.lifetimeMu.Unlock()
			return err
		}
	}
	for s.activeOperations > 0 {
		s.lifetimeCond.Wait()
	}
	backgroundErr := s.backgroundErr
	s.closeInProgress = true
	s.lifetimeMu.Unlock()

	var closeErr error
	if s.close != nil {
		closeErr = s.close()
	}
	err := errors.Join(backgroundErr, closeErr)

	s.lifetimeMu.Lock()
	s.closeInProgress = false
	if closeErr == nil {
		s.lifetimeState = storeLifetimeClosed
	}
	s.lifetimeCond.Broadcast()
	s.lifetimeMu.Unlock()
	return err
}

func (s *Store) reportBackgroundError(err error) {
	if s == nil || err == nil {
		return
	}
	s.lifetimeMu.Lock()
	s.backgroundErr = errors.Join(s.backgroundErr, err)
	s.lifetimeMu.Unlock()
}

func (s *Store) validate() error {
	if s == nil {
		return errors.New("runtime store is required")
	}
	if err := s.validateIdentity(); err != nil {
		return err
	}
	if err := s.validateOpen(); err != nil {
		return err
	}
	if s.repo == nil || s.prompt == nil || s.forkOperations == nil || s.agentTodos == nil || s.rootAuthority == nil || s.deleteCleanup == nil {
		return errors.New("runtime store must be created with runtime.NewMemoryStore or runtime.OpenSQLiteStore")
	}
	if _, ok := s.repo.(sessiontree.ProviderStateStore); !ok {
		return ErrUnsupportedStoreCapability
	}
	return nil
}

func (s *Store) initLifetime() {
	s.lifetimeMu.Lock()
	defer s.lifetimeMu.Unlock()
	s.initLifetimeLocked()
}

func (s *Store) initLifetimeLocked() {
	if s.lifetimeCond == nil {
		s.lifetimeCond = sync.NewCond(&s.lifetimeMu)
	}
	if s.lifetimeState == "" {
		s.lifetimeState = storeLifetimeOpen
	}
	if s.lifetimeCtx == nil {
		s.lifetimeCtx, s.lifetimeCancel = context.WithCancel(context.Background())
	}
}

func (s *Store) validateOpen() error {
	if s == nil {
		return errors.New("runtime store is required")
	}
	s.lifetimeMu.Lock()
	defer s.lifetimeMu.Unlock()
	s.initLifetimeLocked()
	if s.lifetimeState != storeLifetimeOpen {
		return ErrStoreClosed
	}
	return nil
}

func (s *Store) beginOperation() (func(), error) {
	if err := s.validateIdentity(); err != nil {
		return nil, err
	}
	s.lifetimeMu.Lock()
	s.initLifetimeLocked()
	if s.lifetimeState != storeLifetimeOpen {
		s.lifetimeMu.Unlock()
		return nil, ErrStoreClosed
	}
	s.activeOperations++
	s.lifetimeMu.Unlock()
	return func() {
		s.lifetimeMu.Lock()
		s.activeOperations--
		if s.activeOperations == 0 {
			s.lifetimeCond.Broadcast()
		}
		s.lifetimeMu.Unlock()
	}, nil
}

func (s *Store) beginOperationContext(ctx context.Context) (context.Context, func(), error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	operationCtx, cancel := context.WithCancel(ctx)
	s.lifetimeMu.Lock()
	lifetimeCtx := s.lifetimeCtx
	s.lifetimeMu.Unlock()
	stopLifetimeCancel := context.AfterFunc(lifetimeCtx, cancel)
	var once sync.Once
	finish := func() {
		once.Do(func() {
			stopLifetimeCancel()
			cancel()
			done()
		})
	}
	return operationCtx, finish, nil
}

func (s *Store) recoverPendingAutomaticThreadTitles(harness *agentharness.AgentHarness) error {
	if s == nil || harness == nil {
		return errors.New("automatic title recovery requires store and harness")
	}
	s.titleRecoveryMu.Lock()
	defer s.titleRecoveryMu.Unlock()
	if s.titleRecoveryDone {
		return nil
	}
	ctx, finish, err := s.beginOperationContext(context.Background())
	if err != nil {
		return err
	}
	defer finish()
	if err := harness.RecoverPendingAutomaticThreadTitles(ctx); err != nil {
		return err
	}
	s.titleRecoveryDone = true
	return nil
}

func (s *Store) registerTurnExecution(lease sessiontree.TurnLease) error {
	if err := lease.Validate(); err != nil || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return sessiontree.ErrStaleAuthority
	}
	s.turnExecutionMu.Lock()
	defer s.turnExecutionMu.Unlock()
	if s.turnExecutions == nil {
		s.turnExecutions = make(map[string]sessiontree.TurnLease)
	}
	if current, ok := s.turnExecutions[lease.ThreadID]; ok && !sessiontree.SameTurnLease(current, lease) {
		return sessiontree.ErrThreadAuthorityBusy
	}
	s.turnExecutions[lease.ThreadID] = lease
	return nil
}

func (s *Store) renewTurnExecution(previous, renewed sessiontree.TurnLease) error {
	s.turnExecutionMu.Lock()
	defer s.turnExecutionMu.Unlock()
	current, ok := s.turnExecutions[previous.ThreadID]
	if !ok || !sessiontree.SameTurnLease(current, previous) ||
		previous.ThreadID != renewed.ThreadID || previous.OwnerID != renewed.OwnerID ||
		previous.Generation != renewed.Generation || renewed.Heartbeat <= previous.Heartbeat {
		return sessiontree.ErrStaleAuthority
	}
	s.turnExecutions[renewed.ThreadID] = renewed
	return nil
}

func (s *Store) unregisterTurnExecution(lease sessiontree.TurnLease) {
	s.turnExecutionMu.Lock()
	if current, ok := s.turnExecutions[lease.ThreadID]; ok && sessiontree.SameTurnLease(current, lease) {
		delete(s.turnExecutions, lease.ThreadID)
	}
	s.turnExecutionMu.Unlock()
}

func (s *Store) activeTurnExecution(threadID string) (sessiontree.TurnLease, bool) {
	s.turnExecutionMu.Lock()
	defer s.turnExecutionMu.Unlock()
	lease, ok := s.turnExecutions[strings.TrimSpace(threadID)]
	return lease, ok
}

func (s *Store) turnExecutionRegistry() *agentharness.TurnExecutionRegistry {
	return &agentharness.TurnExecutionRegistry{
		Register: s.registerTurnExecution, Renew: s.renewTurnExecution,
		Unregister: s.unregisterTurnExecution, Active: s.activeTurnExecution,
	}
}

func (s *Store) beginLifetimeOperationContext() (context.Context, func(), error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, nil, err
	}
	s.lifetimeMu.Lock()
	lifetimeCtx := s.lifetimeCtx
	s.lifetimeMu.Unlock()
	operationCtx, cancel := context.WithCancel(lifetimeCtx)
	var once sync.Once
	finish := func() {
		once.Do(func() {
			cancel()
			done()
		})
	}
	return operationCtx, finish, nil
}

func (s *Store) validateIdentity() error {
	if s == nil {
		return errors.New("runtime store is required")
	}
	if s.self != nil && s.self != s {
		return errors.New("runtime store must not be copied")
	}
	return nil
}

func (s *Store) deleteThreadData(ctx context.Context, threadID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	result, err := s.rootAuthority.DeleteRootTree(ctx, threadID)
	if err != nil {
		return err
	}
	if err := s.deleteCleanup(ctx, result.ThreadIDs); err != nil {
		return &CommittedCleanupError{ThreadID: ThreadID(threadID), Err: err}
	}
	return nil
}

func cleanRuntimeIDs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (s *Store) threadTreeIDs(ctx context.Context, threadID string) ([]string, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, errors.New("thread id is required")
	}
	threads, err := sessiontree.ListThreads(ctx, s.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return nil, err
	}
	return sessiontree.ThreadAuthorityTreeIDs(threads, threadID)
}

func newProviderHost(opts providerHostOptions) (*providerHost, error) {
	titleMode, err := normalizeThreadTitleMode(opts.ThreadTitleMode)
	if err != nil {
		return nil, err
	}
	cfg, provider, err := resolveHostConfigAndProvider(opts)
	if err != nil {
		return nil, err
	}
	store := opts.Store
	if store == nil {
		return nil, errors.New("runtime store is required")
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	harness, err := newHarnessWithProvider(cfg, provider, harnessOptions{
		Store:                   store,
		Tools:                   opts.Tools,
		EffectAuthorizationGate: opts.EffectAuthorizationGate,
		Sink:                    newRuntimeEventSink(opts.Sink),
		SinkPolicy:              runtimeHarnessSinkPolicy(),
		ToolSurfaceProvider:     runtimeToolSurfaceProvider(opts.ToolSurfaceProvider),
		NewID:                   opts.IDGenerator,
		LoopLimits:              opts.LoopLimits,
		SubAgentRunTimeout:      opts.SubAgentRunTimeout,
		Capabilities:            opts.Capabilities,
		ThreadTitleMode:         titleMode,
		StateCompatibilityKey:   runtimeStateCompatibilityKey(cfg, opts),
	})
	if err != nil {
		return nil, err
	}
	return &providerHost{
		cfg:                       cfg,
		store:                     store,
		sink:                      opts.Sink,
		harness:                   harness,
		supportsOpaqueAttachments: opts.ModelGateway != nil,
	}, nil
}

func resolveHostConfigAndProvider(opts providerHostOptions) (config.Config, provider.Provider, error) {
	if opts.ModelGateway != nil {
		identity, err := normalizeModelGatewayIdentity(opts.ModelGatewayIdentity)
		if err != nil {
			return config.Config{}, nil, err
		}
		cfg, err := resolveModelGatewayHostConfig(opts.Config, identity)
		if err != nil {
			return config.Config{}, nil, err
		}
		modelProvider, err := projectedModelProvider(cfg, opts.ModelGateway, identity)
		if err != nil {
			return config.Config{}, nil, err
		}
		return cfg, modelProvider, nil
	}
	cfg, err := config.Resolve(opts.Config, nil)
	if err != nil {
		return config.Config{}, nil, err
	}
	modelProvider, err := projectedModelProvider(cfg, nil, ModelGatewayIdentity{})
	if err != nil {
		return config.Config{}, nil, err
	}
	return cfg, modelProvider, nil
}

func runtimeHarnessSinkPolicy() event.SinkPolicy {
	return event.SinkPolicy{AllowRaw: true, Redactor: event.SafePathRefsText}
}

func runtimeHostError(err error) error {
	var committedEffect *agentharness.CommittedEffectError
	if errors.As(err, &committedEffect) {
		return &CommittedEffectError{EffectAttemptID: committedEffect.EffectAttemptID, Err: runtimeHostError(committedEffect.Err)}
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, agentharness.ErrActiveTurn), errors.Is(err, sessiontree.ErrActiveTurn):
		return &AuthorityBusyError{Kind: AuthorityBusyTurn, Err: err}
	case errors.Is(err, sessiontree.ErrThreadAuthorityBusy):
		return &AuthorityBusyError{Kind: AuthorityBusyAuthority, Err: err}
	case errors.Is(err, sessiontree.ErrThreadDeleted):
		return fmt.Errorf("%w: %w", ErrThreadDeleted, err)
	case errors.Is(err, sessiontree.ErrSubAgentClosing):
		return fmt.Errorf("%w: %w", ErrSubAgentClosing, err)
	case errors.Is(err, sessiontree.ErrStaleAuthority):
		return fmt.Errorf("%w: %w", ErrStaleAuthority, err)
	case errors.Is(err, sessiontree.ErrRecoveryTargetResolved):
		return fmt.Errorf("%w: %w", ErrRecoveryTargetResolved, err)
	case errors.Is(err, sessiontree.ErrRequestConflict):
		return &RequestConflictError{Operation: "authority", Err: err}
	case errors.Is(err, sessiontree.ErrAuthorityCorrupt):
		return fmt.Errorf("%w: %w", ErrAuthorityCorrupt, err)
	case errors.Is(err, sessiontree.ErrEffectOutcomeUnknown):
		return fmt.Errorf("%w: %w", ErrEffectOutcomeUnknown, err)
	case errors.Is(err, agentharness.ErrEffectUnauthorized):
		return fmt.Errorf("%w: %w", ErrEffectUnauthorized, err)
	case errors.Is(err, agentharness.ErrAuthorizationUnavailable):
		return fmt.Errorf("%w: %w", ErrAuthorizationUnavailable, err)
	case errors.Is(err, agentharness.ErrInvalidAuthorizationProof):
		return fmt.Errorf("%w: %w", ErrInvalidAuthorizationProof, err)
	case errors.Is(err, agentharness.ErrEffectDispatchConsumed):
		return fmt.Errorf("%w: %w", ErrEffectDispatchConsumed, err)
	case errors.Is(err, agentharness.ErrAuthorizationContract):
		return fmt.Errorf("%w: %w", ErrAuthorizationContract, err)
	case errors.Is(err, agentharness.ErrNoRetryTarget):
		return fmt.Errorf("%w: %w", ErrNoRetryTarget, err)
	case errors.Is(err, agentharness.ErrPendingToolSettlementTargetTurnNotFound):
		return fmt.Errorf("%w: %w", ErrTurnNotFound, err)
	case errors.Is(err, agentharness.ErrPendingToolSettlementTargetRunNotFound):
		return fmt.Errorf("%w: %w", ErrRunNotFound, err)
	case errors.Is(err, agentharness.ErrPendingToolSettlementTargetToolNotFound):
		return fmt.Errorf("%w: %w", ErrPendingToolNotFound, err)
	case errors.Is(err, agentharness.ErrPendingToolSettlementTargetNotActive):
		return fmt.Errorf("%w: %w", ErrPendingToolNotActive, err)
	case errors.Is(err, agentharness.ErrPendingToolSettlementConflict):
		return fmt.Errorf("%w: %w", ErrPendingToolSettlementConflict, err)
	case errors.Is(err, agentharness.ErrSubAgentNotFound):
		return fmt.Errorf("%w: %w", ErrSubAgentNotFound, err)
	case errors.Is(err, agentharness.ErrSubAgentClosed):
		return fmt.Errorf("%w: %w", ErrSubAgentClosed, err)
	case errors.Is(err, sessiontree.ErrThreadClosed):
		return fmt.Errorf("%w: %w", ErrSubAgentClosed, err)
	case errors.Is(err, agentharness.ErrForkOperationConflict):
		return fmt.Errorf("%w: %w", ErrForkOperationConflict, err)
	case errors.Is(err, sessiontree.ErrForkDestinationConflict):
		return fmt.Errorf("%w: %w", ErrForkDestinationConflict, err)
	case errors.Is(err, sessiontree.ErrAgentTodoVersionConflict):
		return fmt.Errorf("%w: %w", ErrAgentTodoVersionConflict, err)
	case errors.Is(err, agentharness.ErrJournalInvariant),
		errors.Is(err, sessiontree.ErrEntryNotFound),
		errors.Is(err, sessiontree.ErrInvalidParent):
		return fmt.Errorf("%w: %w", ErrJournalInvariant, err)
	case errors.Is(err, sessiontree.ErrInvalidThreadAuthority):
		return fmt.Errorf("%w: %w", ErrThreadAuthorityInvariant, err)
	case errors.Is(err, sessiontree.ErrArtifactNotFound):
		return fmt.Errorf("%w: %w", ErrArtifactNotFound, err)
	case errors.Is(err, sessiontree.ErrSubAgentNotFound):
		return fmt.Errorf("%w: %w", ErrSubAgentNotFound, err)
	case errors.Is(err, sessiontree.ErrSubAgentParentRequired):
		return fmt.Errorf("%w: %w", ErrSubAgentParentRequired, err)
	case errors.Is(err, sessiontree.ErrUnsupportedStoreCapability):
		return fmt.Errorf("%w: %w", ErrUnsupportedStoreCapability, err)
	case errors.Is(err, sessiontree.ErrThreadNotFound):
		return fmt.Errorf("%w: %w", ErrThreadNotFound, err)
	default:
		return err
	}
}

func (h *ThreadCreateHost) CreateThread(ctx context.Context, req CreateThreadRequest) (ThreadSummary, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadSummary{}, err
	}
	defer done()
	requestedThreadID := ThreadID(strings.TrimSpace(string(req.ThreadID)))
	if requestedThreadID != "" && requestedThreadID != h.threadID {
		return ThreadSummary{}, fmt.Errorf("thread create host is bound to thread %q, got %q", h.threadID, requestedThreadID)
	}
	requestedIntentID := CreateIntentID(strings.TrimSpace(string(req.CreateIntentID)))
	if requestedIntentID != "" && requestedIntentID != h.createIntentID {
		return ThreadSummary{}, fmt.Errorf("thread create host is bound to create intent %q, got %q", h.createIntentID, requestedIntentID)
	}
	req.ThreadID = h.threadID
	req.CreateIntentID = h.createIntentID
	h.store.threadAuthorityMu.Lock()
	defer h.store.threadAuthorityMu.Unlock()
	created, err := h.store.rootAuthority.CreateRoot(ctx, sessiontree.CreateRootRequest{
		ThreadID:        string(req.ThreadID),
		CreateIntentID:  string(req.CreateIntentID),
		ContractVersion: "1",
		Meta:            sessiontree.ThreadMeta{ID: string(req.ThreadID)},
	})
	if err != nil {
		return ThreadSummary{}, requestConflictError(runtimeHostError(err), "root_create", string(req.CreateIntentID))
	}
	thread, err := h.harness.BindCreatedRoot(created.Thread, created.Replayed)
	if err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	if !created.Replayed {
		return ThreadSummary{
			ID: ThreadID(created.Thread.ID), Title: created.Thread.Title,
			TitleStatus: string(created.Thread.TitleStatus), TitleSource: string(created.Thread.TitleSource),
			TitleUpdatedAt: created.Thread.TitleUpdatedAt, TitleError: created.Thread.TitleError,
			CreatedAt: created.Thread.CreatedAt, UpdatedAt: created.Thread.UpdatedAt,
			Phase: ThreadPhaseIdle, Status: ThreadStatusIdle, CanAppendMessage: true,
		}, nil
	}
	summary, err := thread.Summary(ctx)
	if err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	return threadSummary(summary), nil
}

func (h *ThreadTitleHost) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (ThreadSnapshot, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, req.ThreadID, "thread title host"); err != nil {
		return ThreadSnapshot{}, err
	}
	return setThreadTitle(ctx, h.harness, req)
}

func setThreadTitle(ctx context.Context, harness *agentharness.AgentHarness, req SetThreadTitleRequest) (ThreadSnapshot, error) {
	snapshot, err := harness.SetThreadTitle(ctx, string(req.ThreadID), req.Title)
	if err != nil {
		return ThreadSnapshot{}, runtimeHostError(err)
	}
	return threadSnapshot(snapshot), nil
}

func (h *ThreadForkHost) ForkThread(ctx context.Context, req ForkThreadRequest) (ForkThreadResult, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ForkThreadResult{}, err
	}
	defer done()
	h.store.threadAuthorityMu.Lock()
	defer h.store.threadAuthorityMu.Unlock()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, req.SourceThreadID, "thread fork host"); err != nil {
		return ForkThreadResult{}, err
	}
	result, err := forkThread(ctx, h.harness, req)
	return result, requestConflictError(err, "fork", string(req.OperationID))
}

func forkThread(ctx context.Context, harness *agentharness.AgentHarness, req ForkThreadRequest) (ForkThreadResult, error) {
	if strings.TrimSpace(string(req.OperationID)) == "" {
		return ForkThreadResult{}, errors.New("fork operation id is required")
	}
	if strings.TrimSpace(string(req.SourceThreadID)) == "" {
		return ForkThreadResult{}, errors.New("source thread id is required")
	}
	if strings.TrimSpace(string(req.DestinationThreadID)) == "" {
		return ForkThreadResult{}, errors.New("destination thread id is required")
	}
	if strings.TrimSpace(string(req.SourceThreadID)) == strings.TrimSpace(string(req.DestinationThreadID)) {
		return ForkThreadResult{}, errors.New("fork destination must differ from source")
	}
	result, err := harness.ForkThreadWithResult(ctx, agentharness.ForkOptions{
		OperationID:    string(req.OperationID),
		SourceThreadID: string(req.SourceThreadID),
		NewThreadID:    string(req.DestinationThreadID),
	})
	if err != nil {
		return ForkThreadResult{}, runtimeHostError(err)
	}
	return forkThreadResult(result), nil
}

func (h *providerHost) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	return readThreadByID(ctx, h.harness, threadID)
}

func (h *ThreadReadHost) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, threadID, "thread read host"); err != nil {
		return ThreadSnapshot{}, err
	}
	return readThreadByID(ctx, h.harness, threadID)
}

func readThreadByID(ctx context.Context, harness *agentharness.AgentHarness, threadID ThreadID) (ThreadSnapshot, error) {
	snapshot, err := harness.ReadThread(ctx, string(threadID))
	if err != nil {
		return ThreadSnapshot{}, runtimeHostError(err)
	}
	return threadSnapshot(snapshot), nil
}

func (h *providerHost) ListThreadDetailEvents(ctx context.Context, req ListThreadDetailEventsRequest) (ThreadDetailEvents, error) {
	return listThreadDetailEvents(ctx, h.harness, req)
}

func (h *ThreadReadHost) ListThreadDetailEvents(ctx context.Context, req ListThreadDetailEventsRequest) (ThreadDetailEvents, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadDetailEvents{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, req.ThreadID, "thread read host"); err != nil {
		return ThreadDetailEvents{}, err
	}
	return listThreadDetailEvents(ctx, h.harness, req)
}

func listThreadDetailEvents(ctx context.Context, harness *agentharness.AgentHarness, req ListThreadDetailEventsRequest) (ThreadDetailEvents, error) {
	detail, err := harness.ListThreadDetailEvents(ctx, agentharness.ListThreadDetailEventsOptions{
		ThreadID:     string(req.ThreadID),
		AfterOrdinal: req.AfterOrdinal,
		Limit:        req.Limit,
		IncludeRaw:   req.IncludeRaw,
	})
	if err != nil {
		return ThreadDetailEvents{}, runtimeHostError(err)
	}
	return ThreadDetailEvents{
		Events:       threadDetailEvents(detail.Events),
		NextOrdinal:  detail.NextOrdinal,
		HasMore:      detail.HasMore,
		RetainedFrom: detail.RetainedFrom,
		GeneratedAt:  detail.GeneratedAt,
	}, nil
}

func (h *providerHost) ReadThreadContext(ctx context.Context, threadID ThreadID) (ThreadContextSnapshot, error) {
	return readThreadContext(ctx, h.harness, threadID)
}

func (h *ThreadReadHost) ReadThreadContext(ctx context.Context, threadID ThreadID) (ThreadContextSnapshot, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadContextSnapshot{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, threadID, "thread read host"); err != nil {
		return ThreadContextSnapshot{}, err
	}
	return readThreadContext(ctx, h.harness, threadID)
}

func readThreadContext(ctx context.Context, harness *agentharness.AgentHarness, threadID ThreadID) (ThreadContextSnapshot, error) {
	contextSnapshot, err := harness.ReadThreadContext(ctx, string(threadID))
	if err != nil {
		return ThreadContextSnapshot{}, runtimeHostError(err)
	}
	out := subAgentDetailContext(string(threadID), contextSnapshot)
	if err := out.Validate(); err != nil {
		return ThreadContextSnapshot{}, err
	}
	return out, nil
}

func (h *providerHost) ReadThreadAgentTodos(ctx context.Context, threadID ThreadID) (ThreadAgentTodoState, error) {
	return readThreadAgentTodos(ctx, h.store, threadID)
}

func (h *ThreadReadHost) ReadThreadAgentTodos(ctx context.Context, threadID ThreadID) (ThreadAgentTodoState, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadAgentTodoState{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, threadID, "thread read host"); err != nil {
		return ThreadAgentTodoState{}, err
	}
	return readThreadAgentTodos(ctx, h.store, threadID)
}

func readThreadAgentTodos(ctx context.Context, store *Store, threadID ThreadID) (ThreadAgentTodoState, error) {
	if strings.TrimSpace(string(threadID)) == "" {
		return ThreadAgentTodoState{}, errors.New("thread id is required")
	}
	state, err := store.agentTodos.ReadAgentTodoState(ctx, string(threadID))
	if err != nil {
		return ThreadAgentTodoState{}, runtimeHostError(err)
	}
	return threadAgentTodoState(state), nil
}

func (h *providerHost) UpdateThreadAgentTodos(ctx context.Context, req UpdateThreadAgentTodosRequest) (ThreadAgentTodoState, error) {
	return updateThreadAgentTodos(ctx, h.store, req)
}

func updateThreadAgentTodos(ctx context.Context, store *Store, req UpdateThreadAgentTodosRequest) (ThreadAgentTodoState, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return ThreadAgentTodoState{}, errors.New("thread id is required")
	}
	if req.ExpectedVersion < 0 {
		return ThreadAgentTodoState{}, errors.New("expected todo version must be non-negative")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" || strings.TrimSpace(string(req.RunID)) == "" || strings.TrimSpace(req.ToolCallID) == "" {
		return ThreadAgentTodoState{}, errors.New("todo update requires turn, run, and tool call identities")
	}
	if err := validateAgentTodoUpdateIdentity(ctx, store.repo, req); err != nil {
		return ThreadAgentTodoState{}, err
	}
	items := make([]sessiontree.AgentTodoItem, 0, len(req.Items))
	seen := make(map[string]struct{}, len(req.Items))
	for index, item := range req.Items {
		id := strings.TrimSpace(item.ID)
		content := strings.TrimSpace(item.Content)
		if id == "" || content == "" || !item.Status.Valid() {
			return ThreadAgentTodoState{}, fmt.Errorf("todo item %d is invalid", index)
		}
		if _, ok := seen[id]; ok {
			return ThreadAgentTodoState{}, fmt.Errorf("duplicate todo id %q", id)
		}
		seen[id] = struct{}{}
		items = append(items, sessiontree.AgentTodoItem{ID: id, Content: content, Status: sessiontree.AgentTodoStatus(item.Status)})
	}
	state, err := store.agentTodos.CompareAndSwapAgentTodoState(ctx, sessiontree.AgentTodoState{
		ThreadID:          string(req.ThreadID),
		Items:             items,
		UpdatedAt:         time.Now().UTC(),
		UpdatedByTurnID:   string(req.TurnID),
		UpdatedByRunID:    string(req.RunID),
		UpdatedByToolCall: strings.TrimSpace(req.ToolCallID),
	}, req.ExpectedVersion)
	if err != nil {
		return ThreadAgentTodoState{}, runtimeHostError(err)
	}
	return threadAgentTodoState(state), nil
}

func validateAgentTodoUpdateIdentity(ctx context.Context, repo sessiontree.JournalRepo, req UpdateThreadAgentTodosRequest) error {
	meta, err := repo.Thread(ctx, string(req.ThreadID))
	if err != nil {
		return runtimeHostError(err)
	}
	path, err := repo.Path(ctx, string(req.ThreadID), meta.LeafID)
	if err != nil {
		return runtimeHostError(err)
	}
	runFound := false
	toolFound := false
	for _, entry := range path {
		if entry.TurnID != string(req.TurnID) {
			continue
		}
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && strings.TrimSpace(entry.Metadata["run_id"]) == string(req.RunID) {
			runFound = true
		}
		if entry.Type == sessiontree.EntryToolCall && strings.TrimSpace(entry.Message.ToolCallID) == strings.TrimSpace(req.ToolCallID) {
			toolFound = true
		}
	}
	if !runFound {
		return fmt.Errorf("%w: %s", ErrRunNotFound, req.RunID)
	}
	if !toolFound {
		return fmt.Errorf("todo update tool call %q was not found in turn %q", req.ToolCallID, req.TurnID)
	}
	return nil
}

func threadAgentTodoState(in sessiontree.AgentTodoState) ThreadAgentTodoState {
	out := ThreadAgentTodoState{
		ThreadID:          ThreadID(in.ThreadID),
		Version:           in.Version,
		Items:             make([]AgentTodo, 0, len(in.Items)),
		UpdatedAt:         in.UpdatedAt,
		UpdatedByTurnID:   TurnID(in.UpdatedByTurnID),
		UpdatedByRunID:    RunID(in.UpdatedByRunID),
		UpdatedByToolCall: in.UpdatedByToolCall,
	}
	for _, item := range in.Items {
		out.Items = append(out.Items, AgentTodo{ID: item.ID, Content: item.Content, Status: AgentTodoStatus(item.Status)})
	}
	return out
}

func (h *providerHost) ReadApprovalQueue(ctx context.Context, req ReadApprovalQueueRequest) (ApprovalQueue, error) {
	return readApprovalQueue(ctx, h.harness, req)
}

func readApprovalQueue(ctx context.Context, harness *agentharness.AgentHarness, req ReadApprovalQueueRequest) (ApprovalQueue, error) {
	result, err := harness.ReadApprovalQueue(ctx, agentharness.ReadApprovalQueueOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return ApprovalQueue{}, runtimeHostError(err)
	}
	out := approvalQueue(result)
	if err := out.Validate(); err != nil {
		return ApprovalQueue{}, fmt.Errorf("validate approval queue: %w", err)
	}
	return out, nil
}

func (h *providerHost) ResolveApproval(ctx context.Context, req ResolveApprovalRequest) (ResolveApprovalResult, error) {
	if err := req.Validate(); err != nil {
		return ResolveApprovalResult{}, err
	}
	result, err := h.harness.ResolveApproval(ctx, agentharness.ResolveApprovalOptions{
		DecisionID: req.DecisionID, ExpectedRootThreadID: string(req.ExpectedRootThreadID),
		ExpectedGeneration: req.ExpectedGeneration, ExpectedRevision: req.ExpectedRevision,
		ExpectedCurrent: sessiontree.ApprovalIdentity{
			ApprovalID: req.ExpectedCurrent.ApprovalID, ThreadID: string(req.ExpectedCurrent.ThreadID),
			TurnID: string(req.ExpectedCurrent.TurnID), RunID: string(req.ExpectedCurrent.RunID),
			ToolCallID: req.ExpectedCurrent.ToolCallID, EffectAttemptID: req.ExpectedCurrent.EffectAttemptID,
		},
		ExpectedApprovalRevision: req.ExpectedApprovalRevision,
		Decision:                 sessiontree.ApprovalDecision(req.Decision),
	})
	if err != nil {
		return ResolveApprovalResult{}, runtimeHostError(err)
	}
	out := ResolveApprovalResult{
		Receipt: approvalDecisionReceipt(result.Receipt), Queue: approvalQueue(result.Queue),
		Approval: approvalRecord(result.Approval), Replayed: result.Replayed,
	}
	if err := out.Validate(); err != nil {
		return ResolveApprovalResult{}, fmt.Errorf("validate approval result: %w", err)
	}
	return out, nil
}

func (h *providerHost) ReadTurnProjection(ctx context.Context, req ReadTurnProjectionRequest) (ThreadTurnProjection, error) {
	return readTurnProjection(ctx, h.harness, req)
}

func (h *ThreadReadHost) ReadTurnProjection(ctx context.Context, req ReadTurnProjectionRequest) (ThreadTurnProjection, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadTurnProjection{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, req.ThreadID, "thread read host"); err != nil {
		return ThreadTurnProjection{}, err
	}
	return readTurnProjection(ctx, h.harness, req)
}

func readTurnProjection(ctx context.Context, harness *agentharness.AgentHarness, req ReadTurnProjectionRequest) (ThreadTurnProjection, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return ThreadTurnProjection{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" {
		return ThreadTurnProjection{}, errors.New("turn id is required")
	}
	if strings.TrimSpace(string(req.RunID)) == "" {
		return ThreadTurnProjection{}, errors.New("run id is required")
	}
	detail, found, err := harness.ReadTurnDetailEvents(ctx, string(req.ThreadID), string(req.TurnID), string(req.RunID), true)
	if err != nil {
		if errors.Is(err, sessiontree.ErrRequestConflict) {
			return ThreadTurnProjection{}, fmt.Errorf("%w: %s", ErrRunNotFound, req.RunID)
		}
		return ThreadTurnProjection{}, runtimeHostError(err)
	}
	if !found {
		return ThreadTurnProjection{}, ErrTurnNotFound
	}
	events := threadDetailEvents(detail.Events)
	if !threadDetailEventsTurnStartedRunIDMatches(events, req.RunID) {
		return ThreadTurnProjection{}, fmt.Errorf("%w: %s", ErrRunNotFound, req.RunID)
	}
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: req.ThreadID,
		TurnID:   req.TurnID,
		RunID:    req.RunID,
		TraceID:  TraceID(req.RunID),
		Events:   events,
	})
	failure := canonicalTurnFailure(events)
	projection.Status = canonicalTurnStatus(projection.Status, failure)
	if err := validateThreadTurnFailureForStatus(projection.Status, failure); err != nil {
		return ThreadTurnProjection{}, fmt.Errorf("%w: canonical turn failure state is invalid: %v", ErrAuthorityCorrupt, err)
	}
	if err := projection.Validate(); err != nil {
		return ThreadTurnProjection{}, fmt.Errorf("%w: canonical turn projection is invalid: %v", ErrAuthorityCorrupt, err)
	}
	return projection, nil
}

func (h *providerHost) RunTurn(ctx context.Context, req RunTurnRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.RunID)) == "" {
		return TurnResult{}, errors.New("run id is required")
	}
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" {
		return TurnResult{}, errors.New("turn id is required")
	}
	input, err := normalizeTurnInput(req.Input)
	if err != nil {
		return TurnResult{}, err
	}
	supplementalContext := agentHarnessSupplementalContext(req.SupplementalContext)
	if len(input.Attachments) > 0 && !h.supportsOpaqueAttachments {
		return TurnResult{}, errors.New("opaque message attachments require a ModelGateway host")
	}
	completionPolicy, err := engineTurnCompletionPolicy(req.Completion)
	if err != nil {
		return TurnResult{}, err
	}
	signalSpec, err := engineTurnSignalSpec(req.Signals, completionPolicy)
	if err != nil {
		return TurnResult{}, err
	}
	operationCtx, done, err := beginHostOperationContext(h.store, ctx)
	if err != nil {
		return TurnResult{}, err
	}
	defer done()
	ctx = operationCtx
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, runtimeHostError(err)
	}
	activityRecorder := &runtimeActivityEventRecorder{sink: newRuntimeEventSink(h.sink)}
	result, runErr := thread.Run(ctx, input.Text, agentharness.RunOptions{
		RunID:  string(req.RunID),
		TurnID: string(req.TurnID),
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
		CompletionPolicy:         completionPolicy,
		ControlSpec:              signalSpec,
		Reasoning:                projectedReasoningSelection(req.Reasoning, h.cfg.Reasoning),
		MaxInputTokens:           req.Limits.MaxInputTokens,
		MaxTotalTokens:           req.Limits.MaxTotalTokens,
		MaxCostUSD:               req.Limits.MaxCostUSD,
		MaxToolCalls:             req.Limits.MaxToolCalls,
		MaxLengthContinuations:   req.Limits.MaxLengthContinuations,
		MaxStopHookContinuations: req.Limits.MaxStopHookContinuations,
		ManualCompactions:        projectedManualCompactionSource(req.ManualCompactions),
		ToolSurfaceProvider:      runtimeToolSurfaceProvider(req.ToolSurfaceProvider),
		SupplementalContext:      supplementalContext,
		Attachments:              sessionMessageAttachments(input.Attachments),
		References:               sessionMessageReferences(input.References),
		Sink:                     activityRecorder,
	})
	out := turnResult(result, string(req.ThreadID), activityRecorder.Snapshot(), time.Now().UnixMilli())
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	_ = h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out, result.CanonicalEvents)
	return out, runtimeHostError(runErr)
}

func normalizeTurnInput(input TurnInput) (TurnInput, error) {
	input.Attachments = append([]MessageAttachment(nil), input.Attachments...)
	input.References = append([]MessageReference(nil), input.References...)
	for index := range input.Attachments {
		input.Attachments[index].ResourceRef = strings.TrimSpace(input.Attachments[index].ResourceRef)
		input.Attachments[index].Name = strings.TrimSpace(input.Attachments[index].Name)
		input.Attachments[index].MIMEType = strings.TrimSpace(input.Attachments[index].MIMEType)
	}
	if err := input.Validate(); err != nil {
		return TurnInput{}, err
	}
	return input, nil
}

func sessionMessageAttachments(in []MessageAttachment) []session.MessageAttachment {
	if len(in) == 0 {
		return nil
	}
	out := make([]session.MessageAttachment, 0, len(in))
	for _, attachment := range in {
		out = append(out, session.MessageAttachment{
			ResourceRef: attachment.ResourceRef,
			Name:        attachment.Name,
			MIMEType:    attachment.MIMEType,
			SizeBytes:   attachment.SizeBytes,
		})
	}
	return out
}

func sessionMessageReferences(in []MessageReference) []session.MessageReference {
	if len(in) == 0 {
		return nil
	}
	out := make([]session.MessageReference, 0, len(in))
	for _, reference := range in {
		out = append(out, session.MessageReference{
			ReferenceID: reference.ReferenceID,
			Kind:        session.MessageReferenceKind(reference.Kind),
			Label:       reference.Label,
			Text:        reference.Text,
			ResourceRef: reference.ResourceRef,
			Truncated:   reference.Truncated,
		})
	}
	return out
}

func (h *providerHost) RetryTurn(ctx context.Context, req RetryTurnRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, runtimeHostError(err)
	}
	result, runErr := thread.Retry(ctx, agentharness.RetryOptions{
		Reason: req.Reason,
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
	})
	out := turnResult(result, string(req.ThreadID), nil, time.Now().UnixMilli())
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	_ = h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out, result.CanonicalEvents)
	return out, runtimeHostError(runErr)
}

func (h *providerHost) CompactThread(ctx context.Context, req CompactThreadRequest) (CompactThreadResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return CompactThreadResult{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(req.RequestID) == "" {
		return CompactThreadResult{}, errors.New("manual compaction request id is required")
	}
	if strings.TrimSpace(req.Source) == "" {
		return CompactThreadResult{}, errors.New("manual compaction source is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return CompactThreadResult{}, runtimeHostError(err)
	}
	activityRecorder := &runtimeActivityEventRecorder{sink: newRuntimeEventSink(h.sink)}
	result, compactErr := thread.Compact(ctx, agentharness.CompactOptions{
		RequestID:              req.RequestID,
		Source:                 req.Source,
		Labels:                 engineLabels(req.Labels),
		Reasoning:              projectedReasoningSelection(req.Reasoning, h.cfg.Reasoning),
		MaxInputTokens:         req.Limits.MaxInputTokens,
		MaxTotalTokens:         req.Limits.MaxTotalTokens,
		MaxCostUSD:             req.Limits.MaxCostUSD,
		MaxToolCalls:           req.Limits.MaxToolCalls,
		MaxLengthContinuations: req.Limits.MaxLengthContinuations,
		Sink:                   activityRecorder,
	})
	events := activityRecorder.Snapshot()
	compactions := observation.CompactionEventsFromEvents(events)
	terminalCompactions := make([]observation.CompactionEvent, 0, 1)
	for _, compact := range compactions {
		if compact.Status != observation.CompactionStatusRunning {
			terminalCompactions = append(terminalCompactions, compact)
		}
	}
	if len(terminalCompactions) == 0 {
		if result.Replayed || result.OperationID != "" {
			status := observation.CompactionStatusCompacted
			phase := observation.CompactionPhaseComplete
			if compactErr != nil {
				status = observation.CompactionStatusFailed
				phase = observation.CompactionPhaseFailed
			}
			errorText := ""
			if compactErr != nil {
				errorText = compactErr.Error()
			}
			terminalCompactions = append(terminalCompactions, observation.CompactionEvent{
				RunID: result.RunID, ThreadID: string(req.ThreadID), OperationID: result.OperationID,
				RequestID: result.RequestID, Source: result.Source, Trigger: string(compaction.TriggerManual),
				Reason: string(compaction.ReasonManual), Phase: phase, Status: status,
				Error: errorText, ObservedAt: time.Now(),
			})
		} else if compactErr != nil {
			return CompactThreadResult{}, runtimeHostError(compactErr)
		} else {
			return CompactThreadResult{}, errors.New("compact thread completed without a terminal compaction event")
		}
	}
	out := CompactThreadResult{
		ThreadID:   req.ThreadID,
		RunID:      RunID(result.RunID),
		RequestID:  strings.TrimSpace(req.RequestID),
		Compaction: terminalCompactions[len(terminalCompactions)-1],
		Metrics:    runtimeMetrics(result.Metrics),
		Replayed:   result.Replayed,
		ActivityTimeline: observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    result.RunID,
			ThreadID: string(req.ThreadID),
			TurnID:   "",
			TraceID:  result.RunID,
		}, events, time.Now().UnixMilli()),
	}
	if err := out.Validate(); err != nil {
		return CompactThreadResult{}, err
	}
	return out, runtimeHostError(compactErr)
}

func (h *providerHost) CompletePendingTool(ctx context.Context, req PendingToolCompletionRequest) (PendingToolCompletionResult, error) {
	requestID := strings.TrimSpace(req.CompletionRequestID)
	if requestID == "" {
		return PendingToolCompletionResult{}, errors.New("completion request id is required")
	}
	if err := validatePendingToolSettlementTarget(req.Target); err != nil {
		return PendingToolCompletionResult{}, err
	}
	if strings.TrimSpace(string(req.ContinuationTurnID)) == "" {
		return PendingToolCompletionResult{}, errors.New("continuation turn id is required")
	}
	if strings.TrimSpace(string(req.ContinuationRunID)) == "" {
		return PendingToolCompletionResult{}, errors.New("continuation run id is required")
	}
	input, err := normalizeTurnInput(req.Input)
	if err != nil {
		return PendingToolCompletionResult{}, err
	}
	if err := rejectReferenceOnlyInputWithoutSupplemental(input, "pending tool completion"); err != nil {
		return PendingToolCompletionResult{}, err
	}
	if len(input.Attachments) > 0 && !h.supportsOpaqueAttachments {
		return PendingToolCompletionResult{}, errors.New("opaque message attachments require a ModelGateway host")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.Target.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return PendingToolCompletionResult{}, runtimeHostError(err)
	}
	result, runErr := thread.CompletePendingTool(ctx, agentharness.PendingToolCompletion{
		CompletionRequestID: requestID,
		Target: sessiontree.PendingToolSettlementTarget{
			ThreadID: string(req.Target.ThreadID), TurnID: string(req.Target.TurnID), RunID: string(req.Target.RunID),
			ToolCallID: req.Target.ToolCallID, ToolName: req.Target.ToolName, Handle: req.Target.Handle,
			EffectAttemptID: req.Target.EffectAttemptID,
		},
		ContinuationTurnID: string(req.ContinuationTurnID), ContinuationRunID: string(req.ContinuationRunID),
		Status: pendingToolCompletionStatus(req.Status), Summary: req.Summary, Output: req.Output,
		Input: session.Message{Role: session.User, Content: input.Text, Attachments: sessionMessageAttachments(input.Attachments), References: sessionMessageReferences(input.References)},
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
	})
	out := PendingToolCompletionResult{
		CompletionRequestID: requestID, ThreadID: req.Target.ThreadID,
		TurnID: req.ContinuationTurnID, RunID: req.ContinuationRunID, Replayed: result.Replayed,
	}
	if result.AdmissionRunning {
		out.Status = TurnStatusRunning
		return out, runtimeHostError(runErr)
	}
	turn := turnResult(result, string(req.Target.ThreadID), nil, time.Now().UnixMilli())
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	_ = h.attachThreadTurnProjection(projectionCtx, string(req.Target.ThreadID), &turn, result.CanonicalEvents)
	out.Status = turn.Status
	out.Turn = &turn
	return out, runtimeHostError(runErr)
}

func (h *PendingToolRecoveryHost) SettlePendingTool(ctx context.Context, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	ctx, done, err := beginHostOperationContext(h.store, ctx)
	if err != nil {
		return PendingToolSettlementResult{}, err
	}
	defer done()
	if h == nil || h.harness == nil {
		return PendingToolSettlementResult{}, errors.New("pending tool recovery host is invalid")
	}
	if err := validatePendingToolSettlementRequest(req); err != nil {
		return PendingToolSettlementResult{}, err
	}
	if (h.threadID == "") == (h.parentThreadID == "") {
		return PendingToolSettlementResult{}, errors.New("pending tool recovery host authority is invalid")
	}
	if h.threadID != "" {
		if err := validateBoundThreadID(h.threadID, req.Target.ThreadID, "pending tool recovery host"); err != nil {
			return PendingToolSettlementResult{}, err
		}
		if err := validateRootThreadAuthority(ctx, h.store, req.Target.ThreadID); err != nil {
			return PendingToolSettlementResult{}, err
		}
	} else {
		if err := validateSubAgentSettlementAuthority(ctx, h.harness, h.parentThreadID, req.Target.ThreadID); err != nil {
			return PendingToolSettlementResult{}, err
		}
	}
	result, err := settlePendingToolRecovery(ctx, h.harness, req)
	return result, requestConflictError(err, "pending_tool_settlement", req.Target.ToolCallID)
}

func (h *TurnExecutionHost) SettlePendingTool(ctx context.Context, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return PendingToolSettlementResult{}, err
	}
	defer done()
	if h == nil || h.host == nil || h.host.harness == nil {
		return PendingToolSettlementResult{}, errors.New("turn execution host is invalid")
	}
	if err := validateBoundThreadID(h.threadID, req.Target.ThreadID, "turn execution host"); err != nil {
		return PendingToolSettlementResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.Target.ThreadID); err != nil {
		return PendingToolSettlementResult{}, err
	}
	result, err := settlePendingToolActive(ctx, h.host.harness, req)
	return result, requestConflictError(err, "pending_tool_settlement", req.Target.ToolCallID)
}

func (h *SubAgentHost) SettlePendingTool(ctx context.Context, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return PendingToolSettlementResult{}, err
	}
	defer done()
	if h == nil || h.host == nil || h.host.harness == nil {
		return PendingToolSettlementResult{}, errors.New("subagent host is invalid")
	}
	if err := validateSubAgentSettlementAuthority(ctx, h.host.harness, h.parentThreadID, req.Target.ThreadID); err != nil {
		return PendingToolSettlementResult{}, err
	}
	result, err := settlePendingToolActive(ctx, h.host.harness, req)
	return result, requestConflictError(err, "pending_tool_settlement", req.Target.ToolCallID)
}

func validateSubAgentSettlementAuthority(ctx context.Context, harness *agentharness.AgentHarness, parentThreadID, childThreadID ThreadID) error {
	if err := harness.ValidateSubAgentAuthority(ctx, string(parentThreadID), string(childThreadID)); err != nil {
		return runtimeHostError(err)
	}
	return nil
}

func settlePendingToolRecovery(ctx context.Context, harness *agentharness.AgentHarness, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	if err := validatePendingToolSettlementRequest(req); err != nil {
		return PendingToolSettlementResult{}, err
	}
	thread, err := harness.ResumeThread(ctx, string(req.Target.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	return settlePendingToolOnThread(ctx, harness, thread, req, sessiontree.TurnLease{})
}

func settlePendingToolActive(ctx context.Context, harness *agentharness.AgentHarness, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	if err := validatePendingToolSettlementRequest(req); err != nil {
		return PendingToolSettlementResult{}, err
	}
	thread, lease, active, err := harness.OwnedActiveThread(ctx, string(req.Target.ThreadID), string(req.Target.TurnID))
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	if !active {
		return PendingToolSettlementResult{}, ErrThreadNotActive
	}
	return settlePendingToolOnThread(ctx, harness, thread, req, lease)
}

func validatePendingToolSettlementRequest(req PendingToolSettlementRequest) error {
	return validatePendingToolSettlementTarget(req.Target)
}

func validatePendingToolSettlementTarget(target PendingToolSettlementTarget) error {
	if strings.TrimSpace(string(target.ThreadID)) == "" {
		return errors.New("thread id is required")
	}
	if strings.TrimSpace(string(target.TurnID)) == "" {
		return errors.New("turn id is required")
	}
	if strings.TrimSpace(string(target.RunID)) == "" {
		return errors.New("run id is required")
	}
	if strings.TrimSpace(target.ToolCallID) == "" {
		return errors.New("tool call id is required")
	}
	if strings.TrimSpace(target.ToolName) == "" {
		return errors.New("tool name is required")
	}
	if strings.TrimSpace(target.Handle) == "" {
		return errors.New("handle is required")
	}
	return nil
}

func settlePendingToolOnThread(ctx context.Context, harness *agentharness.AgentHarness, thread *agentharness.Thread, req PendingToolSettlementRequest, activeLease sessiontree.TurnLease) (PendingToolSettlementResult, error) {
	settlement := agentharness.PendingToolSettlement{
		TurnID:          string(req.Target.TurnID),
		RunID:           string(req.Target.RunID),
		ToolCallID:      req.Target.ToolCallID,
		ToolName:        req.Target.ToolName,
		Handle:          req.Target.Handle,
		EffectAttemptID: req.Target.EffectAttemptID,
		Status:          pendingToolSettlementStatus(req.Status),
		Summary:         req.Summary,
		Output:          req.Output,
		Activity:        observation.CloneActivityPresentation(req.Activity),
	}
	var event agentharness.SubAgentDetailEvent
	var err error
	if strings.TrimSpace(activeLease.OwnerID) == "" {
		event, err = thread.SettlePendingTool(ctx, settlement)
	} else {
		event, err = thread.SettlePendingToolActive(ctx, settlement, activeLease)
	}
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	out := PendingToolSettlementResult{
		Target: req.Target,
		Event:  threadDetailEvent(event),
	}
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	detail, found, err := harness.ReadTurnDetailEvents(projectionCtx, string(req.Target.ThreadID), string(req.Target.TurnID), string(req.Target.RunID), true)
	if err != nil {
		out.ProjectionAvailability = TurnProjectionAvailabilityUnavailable
		out.ProjectionError = runtimeHostError(err).Error()
		return out, nil
	}
	if !found {
		out.ProjectionAvailability = TurnProjectionAvailabilityUnavailable
		out.ProjectionError = runtimeHostError(sessiontree.ErrAuthorityCorrupt).Error()
		return out, nil
	}
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: req.Target.ThreadID,
		TurnID:   req.Target.TurnID,
		RunID:    req.Target.RunID,
		TraceID:  TraceID(req.Target.RunID),
		Events:   threadDetailEvents(detail.Events),
	})
	out.ProjectionAvailability = TurnProjectionAvailabilityReady
	out.Projection = &projection
	return out, nil
}

func (h *providerHost) SpawnSubAgent(ctx context.Context, req SpawnSubAgentRequest) (SubAgentSnapshot, error) {
	input, err := normalizeTurnInput(TurnInput{Text: req.Message, Attachments: req.Attachments, References: req.References})
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if err := rejectReferenceOnlyInputWithoutSupplemental(input, "subagent spawn"); err != nil {
		return SubAgentSnapshot{}, err
	}
	snapshot, err := h.harness.SpawnSubAgent(ctx, agentharness.SpawnSubAgentOptions{
		PublicationID:   req.PublicationID,
		ParentThreadID:  string(req.ParentThreadID),
		ParentTurnID:    string(req.ParentTurnID),
		ThreadID:        string(req.ThreadID),
		TaskName:        req.TaskName,
		TaskDescription: req.TaskDescription,
		Message:         input.Text,
		Attachments:     sessionMessageAttachments(input.Attachments),
		References:      sessionMessageReferences(input.References),
		HostProfileRef:  req.HostProfileRef,
		ForkMode:        agentharness.SubAgentForkMode(req.ForkMode),
		Labels:          engineLabels(req.Labels),
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func (h *providerHost) SendSubAgentInput(ctx context.Context, req SendSubAgentInputRequest) (SubAgentSnapshot, error) {
	input, err := normalizeTurnInput(TurnInput{Text: req.Message, Attachments: req.Attachments, References: req.References})
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if err := rejectReferenceOnlyInputWithoutSupplemental(input, "subagent input"); err != nil {
		return SubAgentSnapshot{}, err
	}
	snapshot, err := h.harness.SendSubAgentInput(ctx, agentharness.SendSubAgentInputOptions{
		InputRequestID: req.InputRequestID,
		ParentThreadID: string(req.ParentThreadID),
		ChildThreadID:  string(req.ChildThreadID),
		Message:        input.Text,
		Attachments:    sessionMessageAttachments(input.Attachments),
		References:     sessionMessageReferences(input.References),
		Interrupt:      req.Interrupt,
		Labels:         engineLabels(req.Labels),
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func (h *providerHost) PublishSubAgentPendingToolCompletion(ctx context.Context, req PublishSubAgentPendingToolCompletionRequest) (SubAgentSnapshot, error) {
	if strings.TrimSpace(req.InputRequestID) == "" {
		return SubAgentSnapshot{}, errors.New("subagent pending tool completion input request id is required")
	}
	if strings.TrimSpace(string(req.ParentThreadID)) == "" || strings.TrimSpace(string(req.ChildThreadID)) == "" {
		return SubAgentSnapshot{}, errors.New("subagent pending tool completion requires parent and child thread identities")
	}
	if err := validatePendingToolSettlementTarget(req.Target); err != nil {
		return SubAgentSnapshot{}, err
	}
	if req.Target.ThreadID != req.ChildThreadID {
		return SubAgentSnapshot{}, errors.New("subagent pending tool completion target thread identity mismatch")
	}
	input, err := normalizeTurnInput(req.Input)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if err := rejectReferenceOnlyInputWithoutSupplemental(input, "subagent pending tool completion"); err != nil {
		return SubAgentSnapshot{}, err
	}
	snapshot, err := h.harness.PublishSubAgentPendingToolCompletion(ctx, agentharness.PublishSubAgentPendingToolCompletionOptions{
		InputRequestID: req.InputRequestID, ParentThreadID: string(req.ParentThreadID), ChildThreadID: string(req.ChildThreadID),
		Target: sessiontree.PendingToolSettlementTarget{
			ThreadID: string(req.Target.ThreadID), TurnID: string(req.Target.TurnID), RunID: string(req.Target.RunID),
			ToolCallID: req.Target.ToolCallID, ToolName: req.Target.ToolName, Handle: req.Target.Handle,
			EffectAttemptID: req.Target.EffectAttemptID,
		},
		Status: pendingToolCompletionStatus(req.Status), Summary: req.Summary, Output: req.Output,
		Message: input.Text, Attachments: sessionMessageAttachments(input.Attachments), References: sessionMessageReferences(input.References), Labels: engineLabels(req.Labels),
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func rejectReferenceOnlyInputWithoutSupplemental(input TurnInput, operation string) error {
	if strings.TrimSpace(input.Text) == "" && len(input.Attachments) == 0 && len(input.References) > 0 {
		return fmt.Errorf("%s does not support reference-only input", operation)
	}
	return nil
}

func (h *providerHost) WaitSubAgents(ctx context.Context, req WaitSubAgentsRequest) (WaitSubAgentsResult, error) {
	result, err := h.harness.WaitSubAgents(ctx, agentharness.WaitSubAgentsOptions{
		ParentThreadID: string(req.ParentThreadID),
		ChildThreadIDs: threadIDStrings(req.ChildThreadIDs),
		Timeout:        req.Timeout,
	})
	if err != nil {
		return WaitSubAgentsResult{}, runtimeHostError(err)
	}
	return waitSubAgentsResult(result), nil
}

func (h *providerHost) ListSubAgents(ctx context.Context, parentThreadID ThreadID) ([]SubAgentSnapshot, error) {
	return listSubAgents(ctx, h.harness, parentThreadID)
}

func (h *SubAgentReadHost) ListSubAgents(ctx context.Context, parentThreadID ThreadID) ([]SubAgentSnapshot, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, parentThreadID, "subagent read host parent"); err != nil {
		return nil, err
	}
	return listSubAgents(ctx, h.harness, parentThreadID)
}

func listSubAgents(ctx context.Context, harness *agentharness.AgentHarness, parentThreadID ThreadID) ([]SubAgentSnapshot, error) {
	snapshots, err := harness.ListSubAgents(ctx, string(parentThreadID))
	if err != nil {
		return nil, runtimeHostError(err)
	}
	out := make([]SubAgentSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, subAgentSnapshot(snapshot))
	}
	return out, nil
}

func (h *providerHost) CloseSubAgent(ctx context.Context, req CloseSubAgentRequest) (SubAgentSnapshot, error) {
	snapshot, err := h.harness.CloseSubAgent(ctx, agentharness.CloseSubAgentOptions{
		CloseOperationID: req.CloseOperationID,
		ParentThreadID:   string(req.ParentThreadID),
		ChildThreadID:    string(req.ChildThreadID),
		Reason:           req.Reason,
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func (h *providerHost) ListSubAgentActivityTimeline(ctx context.Context, req ListSubAgentActivityTimelineRequest) (SubAgentActivityTimelineResult, error) {
	return listSubAgentActivityTimeline(ctx, h.harness, req)
}

func (h *SubAgentReadHost) ListSubAgentActivityTimeline(ctx context.Context, req ListSubAgentActivityTimelineRequest) (SubAgentActivityTimelineResult, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return SubAgentActivityTimelineResult{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent read host parent"); err != nil {
		return SubAgentActivityTimelineResult{}, err
	}
	return listSubAgentActivityTimeline(ctx, h.harness, req)
}

func listSubAgentActivityTimeline(ctx context.Context, harness *agentharness.AgentHarness, req ListSubAgentActivityTimelineRequest) (SubAgentActivityTimelineResult, error) {
	snapshots, err := harness.ListSubAgents(ctx, string(req.ParentThreadID))
	if err != nil {
		return SubAgentActivityTimelineResult{}, runtimeHostError(err)
	}
	generatedAt := time.Now()
	return SubAgentActivityTimelineResult{
		Timeline:    subAgentActivityTimeline(req.Meta, snapshots, generatedAt),
		GeneratedAt: generatedAt,
	}, nil
}

func (h *providerHost) ReadSubAgentDetail(ctx context.Context, req ReadSubAgentDetailRequest) (SubAgentDetail, error) {
	return readSubAgentDetail(ctx, h.harness, req)
}

func (h *SubAgentReadHost) ReadSubAgentDetail(ctx context.Context, req ReadSubAgentDetailRequest) (SubAgentDetail, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return SubAgentDetail{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent read host parent"); err != nil {
		return SubAgentDetail{}, err
	}
	return readSubAgentDetail(ctx, h.harness, req)
}

func validateRootThreadAuthority(ctx context.Context, store *Store, threadID ThreadID) error {
	if strings.TrimSpace(string(threadID)) == "" {
		return errors.New("thread id is required")
	}
	snapshot, err := inspectThreadAuthority(ctx, store, threadID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(snapshot.Thread.ParentThreadID) != "" {
		return fmt.Errorf("%w: %s", ErrSubAgentParentRequired, threadID)
	}
	return validateLiveThreadLifecycle(snapshot.Thread)
}

func readSubAgentDetail(ctx context.Context, harness *agentharness.AgentHarness, req ReadSubAgentDetailRequest) (SubAgentDetail, error) {
	detail, err := harness.ReadSubAgentDetail(ctx, agentharness.ReadSubAgentDetailOptions{
		ParentThreadID: string(req.ParentThreadID),
		ChildThreadID:  string(req.ChildThreadID),
		AfterOrdinal:   req.AfterOrdinal,
		Limit:          req.Limit,
		IncludeRaw:     req.IncludeRaw,
	})
	if err != nil {
		return SubAgentDetail{}, runtimeHostError(err)
	}
	return subAgentDetail(detail), nil
}

func (h *ThreadDeleteHost) DeleteThread(ctx context.Context, threadID ThreadID) error {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return err
	}
	defer done()
	h.store.threadAuthorityMu.Lock()
	defer h.store.threadAuthorityMu.Unlock()
	if err := validateBoundThreadID(h.threadID, threadID, "thread delete host"); err != nil {
		return err
	}
	return deleteThread(ctx, h.store, threadID)
}

func deleteThread(ctx context.Context, store *Store, threadID ThreadID) error {
	id := strings.TrimSpace(string(threadID))
	if id == "" {
		return errors.New("thread id is required")
	}
	return runtimeHostError(store.deleteThreadData(ctx, id))
}

func pendingToolCompletionStatus(status PendingToolCompletionStatus) agentharness.PendingToolCompletionStatus {
	switch status {
	case PendingToolCompletionCompleted:
		return agentharness.PendingToolCompleted
	case PendingToolCompletionFailed:
		return agentharness.PendingToolFailed
	case PendingToolCompletionCanceled:
		return agentharness.PendingToolCanceled
	default:
		return agentharness.PendingToolCompletionStatus(status)
	}
}

func pendingToolSettlementStatus(status PendingToolSettlementStatus) agentharness.PendingToolSettlementStatus {
	switch status {
	case PendingToolSettlementCompleted:
		return agentharness.PendingToolSettledCompleted
	case PendingToolSettlementFailed:
		return agentharness.PendingToolSettledFailed
	case PendingToolSettlementCanceled:
		return agentharness.PendingToolSettledCanceled
	default:
		return agentharness.PendingToolSettlementStatus(status)
	}
}

func readThread(ctx context.Context, thread *agentharness.Thread) (ThreadSnapshot, error) {
	snapshot, err := thread.Read(ctx)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return threadSnapshot(snapshot), nil
}

func threadSnapshot(in agentharness.ThreadSnapshot) ThreadSnapshot {
	out := ThreadSnapshot{
		ID:               ThreadID(in.ID),
		Title:            in.Title,
		TitleStatus:      in.TitleStatus,
		TitleSource:      in.TitleSource,
		TitleUpdatedAt:   in.TitleUpdatedAt,
		TitleError:       in.TitleError,
		TitleGeneration:  in.TitleGeneration,
		CreatedAt:        in.CreatedAt,
		UpdatedAt:        in.UpdatedAt,
		Phase:            ThreadPhase(in.Phase),
		Status:           ThreadStatus(in.Status),
		LatestTurnID:     TurnID(in.LatestTurnID),
		LatestRunID:      RunID(in.LatestRunID),
		ThroughOrdinal:   in.ThroughOrdinal,
		WaitingPrompt:    in.WaitingPrompt,
		Recoverable:      in.Recoverable,
		CanAppendMessage: in.CanAppendMessage,
		CanRetry:         in.CanRetry,
	}
	return out
}

func threadSummary(in agentharness.ThreadSummary) ThreadSummary {
	return ThreadSummary{
		ID:               ThreadID(in.ID),
		Title:            in.Title,
		TitleStatus:      in.TitleStatus,
		TitleSource:      in.TitleSource,
		TitleUpdatedAt:   in.TitleUpdatedAt,
		TitleError:       in.TitleError,
		TitleGeneration:  in.TitleGeneration,
		CreatedAt:        in.CreatedAt,
		UpdatedAt:        in.UpdatedAt,
		Phase:            ThreadPhase(in.Phase),
		Status:           ThreadStatus(in.Status),
		LatestTurnID:     TurnID(in.LatestTurnID),
		WaitingPrompt:    in.WaitingPrompt,
		Recoverable:      in.Recoverable,
		CanAppendMessage: in.CanAppendMessage,
		CanRetry:         in.CanRetry,
	}
}

func forkThreadResult(in agentharness.ForkResult) ForkThreadResult {
	return ForkThreadResult{
		OperationID: ForkOperationID(in.OperationID),
		Thread:      threadSummary(in.Summary),
	}
}

func approvalQueue(in agentharness.ApprovalQueueSnapshot) ApprovalQueue {
	return ApprovalQueue{
		RootThreadID: ThreadID(in.RootThreadID), Generation: in.Generation, Revision: in.Revision,
		CurrentApprovalID: in.CurrentApprovalID, Items: approvalRecordList(in.Approvals), GeneratedAt: in.GeneratedAt,
	}
}

func approvalDecisionReceipt(in sessiontree.ApprovalDecisionReceipt) ApprovalDecisionReceipt {
	return ApprovalDecisionReceipt{
		DecisionID: in.DecisionID, ApprovalID: in.ApprovalID, RootThreadID: ThreadID(in.RootThreadID),
		Decision: ApprovalDecision(in.Decision), State: string(in.State), Reason: in.Reason,
		AuthorizationProofHash: in.AuthorizationProofHash, QueueGeneration: in.QueueGeneration,
		QueueRevision: in.QueueRevision, ApprovalRevision: in.ApprovalRevision,
		SubmittedAt: in.SubmittedAt, ResolvedAt: in.ResolvedAt,
	}
}

func approvalRecordList(in []agentharness.ApprovalRecord) []ApprovalRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]ApprovalRecord, 0, len(in))
	for _, approval := range in {
		out = append(out, approvalRecord(approval))
	}
	return out
}

func approvalRecord(in agentharness.ApprovalRecord) ApprovalRecord {
	return ApprovalRecord{
		ApprovalID: in.ApprovalID, RootThreadID: ThreadID(in.RootThreadID), ParentThreadID: ThreadID(in.ParentThreadID),
		ToolCallID: in.ToolCallID, EffectAttemptID: in.EffectAttemptID, ToolName: in.ToolName, ToolKind: in.ToolKind,
		RunID: RunID(in.RunID), ThreadID: ThreadID(in.ThreadID), TurnID: TurnID(in.TurnID),
		Step: in.Step, BatchIndex: in.BatchIndex, BatchSize: in.BatchSize,
		State: in.State, Revision: in.Revision, QueueSequence: in.QueueSequence, DecisionID: in.DecisionID,
		RequestedAt:            in.RequestedAt,
		UpdatedAt:              in.UpdatedAt,
		ResolvedAt:             in.ResolvedAt,
		ArgsHash:               in.ArgsHash,
		RequestFingerprint:     in.RequestFingerprint,
		AuthorizationProofHash: in.AuthorizationProofHash,
		Resources:              approvalResources(in.Resources),
		Effects:                append([]string(nil), in.Effects...),
		Labels:                 cloneStringMap(in.Labels),
		HostContext:            cloneStringMap(in.HostContext),
		ReadOnly:               in.ReadOnly,
		Destructive:            in.Destructive,
		OpenWorld:              in.OpenWorld,
		Reason:                 in.Reason,
	}
}

func approvalResources(in []agentharness.ApprovalResource) []ApprovalResource {
	if len(in) == 0 {
		return nil
	}
	out := make([]ApprovalResource, 0, len(in))
	for _, resource := range in {
		out = append(out, ApprovalResource{Kind: resource.Kind, Value: resource.Value})
	}
	return out
}

func turnResult(in agentharness.TurnResult, threadID string, events []observation.Event, nowUnixMS int64) TurnResult {
	status := TurnStatus(in.Status)
	if in.AdmissionRunning {
		status = TurnStatusRunning
	}
	out := TurnResult{
		ThreadID:           ThreadID(threadID),
		TurnID:             TurnID(in.ID),
		RunID:              RunID(in.RunID),
		Status:             status,
		Output:             in.Output,
		Diagnostics:        cloneStringMap(in.Diagnostics),
		Metrics:            runtimeMetrics(in.Metrics),
		CompletionReason:   observation.CompletionReason(in.CompletionReason),
		ContinuationReason: observation.ContinuationReason(in.ContinuationReason),
		FinishReason:       observation.FinishReason(in.FinishReason),
		RawFinishReason:    in.RawFinishReason,
		FinishInferred:     in.FinishInferred,
		Signal:             runtimeTurnSignal(in.ControlSignal),
		ActivityTimeline: observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    in.RunID,
			ThreadID: threadID,
			TurnID:   in.ID,
			TraceID:  in.RunID,
		}, events, nowUnixMS),
		Replayed: in.Replayed,
	}
	if in.Err != nil {
		out.Failure = &ThreadTurnFailure{
			Code:    ThreadTurnFailureCode(strings.TrimSpace(in.FailureCode)),
			Message: in.Err.Error(),
		}
	}
	return out
}

func (h *providerHost) attachThreadTurnProjection(ctx context.Context, threadID string, result *TurnResult, canonicalEvents []agentharness.SubAgentDetailEvent) error {
	if result == nil {
		return errors.New("turn result is required")
	}
	if h == nil || strings.TrimSpace(threadID) == "" || strings.TrimSpace(string(result.TurnID)) == "" {
		return markTurnProjectionUnavailable(result, errors.New("turn projection identity is incomplete"))
	}
	var events []ThreadDetailEvent
	if len(canonicalEvents) > 0 {
		events = threadDetailEvents(canonicalEvents)
	} else {
		detail, found, err := h.harness.ReadTurnDetailEvents(ctx, threadID, string(result.TurnID), string(result.RunID), true)
		if err != nil {
			return markTurnProjectionUnavailable(result, runtimeHostError(err))
		}
		if !found {
			return markTurnProjectionUnavailable(result, runtimeHostError(sessiontree.ErrAuthorityCorrupt))
		}
		events = threadDetailEvents(detail.Events)
	}
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: ThreadID(threadID),
		TurnID:   result.TurnID,
		RunID:    result.RunID,
		TraceID:  TraceID(result.RunID),
		Events:   events,
	})
	failure := canonicalTurnFailure(events)
	status := canonicalTurnStatus(projection.Status, failure)
	if err := validateThreadTurnFailureForStatus(status, failure); err != nil {
		return markTurnProjectionUnavailable(result, fmt.Errorf("%w: canonical turn failure state is invalid: %v", ErrAuthorityCorrupt, err))
	}
	projection.Status = status
	if err := projection.Validate(); err != nil {
		return markTurnProjectionUnavailable(result, fmt.Errorf("%w: canonical turn projection is invalid: %v", ErrAuthorityCorrupt, err))
	}
	result.ProjectionAvailability = TurnProjectionAvailabilityReady
	result.Projection = &projection
	result.ProjectionError = ""
	result.Failure = failure
	result.Status = status
	return nil
}

func markTurnProjectionUnavailable(result *TurnResult, err error) error {
	if err == nil {
		err = errors.New("turn projection is unavailable")
	}
	result.ProjectionAvailability = TurnProjectionAvailabilityUnavailable
	result.Projection = nil
	result.ProjectionError = err.Error()
	result.Failure = nil
	return err
}

func runtimeTerminalProjectionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func threadDetailEventsTurnStartedRunIDMatches(events []ThreadDetailEvent, runID RunID) bool {
	want := strings.TrimSpace(string(runID))
	if want == "" {
		return false
	}
	for _, ev := range events {
		if ev.TurnMarker == nil {
			continue
		}
		if strings.TrimSpace(ev.TurnMarker.Status) != string(sessiontree.TurnStarted) {
			continue
		}
		if strings.TrimSpace(ev.TurnMarker.Metadata["run_id"]) == want {
			return true
		}
	}
	return false
}

func waitSubAgentsResult(in agentharness.WaitSubAgentsResult) WaitSubAgentsResult {
	out := WaitSubAgentsResult{TimedOut: in.TimedOut, Snapshots: make([]SubAgentSnapshot, 0, len(in.Snapshots))}
	for _, snapshot := range in.Snapshots {
		out.Snapshots = append(out.Snapshots, subAgentSnapshot(snapshot))
	}
	return out
}

func subAgentSnapshot(in agentharness.SubAgentSnapshot) SubAgentSnapshot {
	return SubAgentSnapshot{
		ThreadID:        ThreadID(in.ThreadID),
		Path:            in.Path,
		TaskName:        in.TaskName,
		TaskDescription: in.TaskDescription,
		ParentThreadID:  ThreadID(in.ParentThreadID),
		ParentTurnID:    TurnID(in.ParentTurnID),
		HostProfileRef:  in.HostProfileRef,
		ForkMode:        SubAgentForkMode(in.ForkMode),
		Status:          SubAgentStatus(in.Status),
		LatestTurnID:    TurnID(in.LatestTurnID),
		LastMessage:     in.LastMessage,
		WaitingPrompt:   in.WaitingPrompt,
		QueuedInputs:    in.QueuedInputs,
		CreatedAt:       in.CreatedAt,
		UpdatedAt:       in.UpdatedAt,
		Closed:          in.Closed,
		CanSendInput:    in.CanSendInput,
		CanInterrupt:    in.CanInterrupt,
		CanClose:        in.CanClose,
	}
}

func subAgentActivityTimeline(meta observation.ActivityRunMeta, snapshots []agentharness.SubAgentSnapshot, generatedAt time.Time) observation.ActivityTimeline {
	timeline := observation.ActivityTimeline{
		SchemaVersion: observation.ActivityTimelineSchemaVersion,
		RunID:         strings.TrimSpace(meta.RunID),
		ThreadID:      strings.TrimSpace(meta.ThreadID),
		TurnID:        strings.TrimSpace(meta.TurnID),
		TraceID:       strings.TrimSpace(meta.TraceID),
		Summary: observation.ActivitySummary{
			Status:   observation.ActivityStatusSuccess,
			Severity: observation.ActivitySeverityQuiet,
		},
		Items: []observation.ActivityItem{},
	}
	if timeline.ThreadID == "" {
		timeline.ThreadID = strings.TrimSpace(string(metaThreadID(meta, snapshots)))
	}
	items := make([]agentharness.SubAgentSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if strings.TrimSpace(snapshot.ThreadID) == "" {
			continue
		}
		items = append(items, snapshot)
	}
	sort.SliceStable(items, func(i, j int) bool {
		leftTerminal := subAgentActivityTerminal(items[i].Status)
		rightTerminal := subAgentActivityTerminal(items[j].Status)
		if leftTerminal != rightTerminal {
			return !leftTerminal
		}
		if !items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].UpdatedAt.After(items[j].UpdatedAt)
		}
		return strings.TrimSpace(items[i].ThreadID) < strings.TrimSpace(items[j].ThreadID)
	})
	counts := observation.ActivityCounts{}
	nowMS := generatedAt.UnixMilli()
	for _, snapshot := range items {
		status, severity, attention := subAgentActivityState(snapshot.Status)
		noteSubAgentActivityCount(&counts, status)
		startedAt := activityTimeUnixMS(snapshot.CreatedAt, nowMS)
		endedAt := int64(0)
		if subAgentActivityTerminal(snapshot.Status) {
			endedAt = activityTimeUnixMS(snapshot.UpdatedAt, nowMS)
			if endedAt < startedAt {
				endedAt = startedAt
			}
		}
		title := firstRuntimeNonEmpty(strings.TrimSpace(snapshot.TaskName), strings.TrimSpace(snapshot.Path), strings.TrimSpace(snapshot.ThreadID), "Subagent")
		description := strings.TrimSpace(snapshot.TaskDescription)
		timeline.Items = append(timeline.Items, observation.ActivityItem{
			ItemID:           "subagent:" + stableSubAgentActivityHash(snapshot.ThreadID),
			ToolID:           "subagents",
			ToolName:         "subagents",
			Kind:             observation.ActivityKindControl,
			Status:           status,
			Severity:         severity,
			NeedsAttention:   len(attention) > 0,
			AttentionReasons: attention,
			RequiresApproval: false,
			StartedAtUnixMS:  startedAt,
			EndedAtUnixMS:    endedAt,
			Label:            title,
			Description:      description,
			Payload:          subAgentActivityPayload(snapshot),
		})
	}
	timeline.Summary.TotalItems = len(timeline.Items)
	timeline.Summary.Counts = counts
	timeline.Summary.Status, timeline.Summary.Severity, timeline.Summary.NeedsAttention, timeline.Summary.AttentionReasons = subAgentActivitySummaryState(counts)
	return timeline
}

func metaThreadID(meta observation.ActivityRunMeta, snapshots []agentharness.SubAgentSnapshot) ThreadID {
	if strings.TrimSpace(meta.ThreadID) != "" {
		return ThreadID(meta.ThreadID)
	}
	for _, snapshot := range snapshots {
		if strings.TrimSpace(snapshot.ParentThreadID) != "" {
			return ThreadID(snapshot.ParentThreadID)
		}
	}
	return ""
}

func subAgentActivityPayload(snapshot agentharness.SubAgentSnapshot) map[string]any {
	title := firstRuntimeNonEmpty(strings.TrimSpace(snapshot.TaskName), strings.TrimSpace(snapshot.Path), strings.TrimSpace(snapshot.ThreadID))
	return map[string]any{
		"thread_id":        strings.TrimSpace(snapshot.ThreadID),
		"path":             strings.TrimSpace(snapshot.Path),
		"task_name":        strings.TrimSpace(snapshot.TaskName),
		"task_description": strings.TrimSpace(snapshot.TaskDescription),
		"title":            title,
		"host_profile_ref": strings.TrimSpace(snapshot.HostProfileRef),
		"fork_mode":        strings.TrimSpace(string(snapshot.ForkMode)),
		"status":           strings.TrimSpace(string(snapshot.Status)),
		"last_message":     strings.TrimSpace(snapshot.LastMessage),
		"waiting_prompt":   strings.TrimSpace(snapshot.WaitingPrompt),
		"queued_inputs":    snapshot.QueuedInputs,
		"parent_thread_id": strings.TrimSpace(snapshot.ParentThreadID),
		"parent_turn_id":   strings.TrimSpace(snapshot.ParentTurnID),
		"latest_turn_id":   strings.TrimSpace(snapshot.LatestTurnID),
		"created_at_ms":    activityTimeUnixMS(snapshot.CreatedAt, 0),
		"updated_at_ms":    activityTimeUnixMS(snapshot.UpdatedAt, 0),
		"closed":           snapshot.Closed,
		"can_send_input":   snapshot.CanSendInput,
		"can_interrupt":    snapshot.CanInterrupt,
		"can_close":        snapshot.CanClose,
	}
}

func subAgentActivityState(status agentharness.SubAgentStatus) (observation.ActivityStatus, observation.ActivitySeverity, []observation.ActivityAttentionReason) {
	switch status {
	case agentharness.SubAgentStatusIdle:
		return observation.ActivityStatusPending, observation.ActivitySeverityQuiet, nil
	case agentharness.SubAgentStatusRunning:
		return observation.ActivityStatusRunning, observation.ActivitySeverityNormal, []observation.ActivityAttentionReason{observation.ActivityAttentionRunning}
	case agentharness.SubAgentStatusWaiting, agentharness.SubAgentStatusInterrupted:
		return observation.ActivityStatusWaiting, observation.ActivitySeverityBlocking, []observation.ActivityAttentionReason{observation.ActivityAttentionWaiting}
	case agentharness.SubAgentStatusCompleted:
		return observation.ActivityStatusSuccess, observation.ActivitySeverityNormal, nil
	case agentharness.SubAgentStatusFailed:
		return observation.ActivityStatusError, observation.ActivitySeverityError, []observation.ActivityAttentionReason{observation.ActivityAttentionError}
	case agentharness.SubAgentStatusCancelled, agentharness.SubAgentStatusClosed:
		return observation.ActivityStatusCanceled, observation.ActivitySeverityWarning, nil
	default:
		return observation.ActivityStatusPending, observation.ActivitySeverityQuiet, nil
	}
}

func noteSubAgentActivityCount(counts *observation.ActivityCounts, status observation.ActivityStatus) {
	if counts == nil {
		return
	}
	switch status {
	case observation.ActivityStatusPending:
		counts.Pending++
	case observation.ActivityStatusRunning:
		counts.Running++
	case observation.ActivityStatusWaiting:
		counts.Waiting++
	case observation.ActivityStatusSuccess:
		counts.Success++
	case observation.ActivityStatusError:
		counts.Error++
	case observation.ActivityStatusCanceled:
		counts.Canceled++
	}
}

func subAgentActivitySummaryState(counts observation.ActivityCounts) (observation.ActivityStatus, observation.ActivitySeverity, bool, []observation.ActivityAttentionReason) {
	if counts.Error > 0 {
		return observation.ActivityStatusError, observation.ActivitySeverityError, true, []observation.ActivityAttentionReason{observation.ActivityAttentionError}
	}
	if counts.Waiting > 0 {
		return observation.ActivityStatusWaiting, observation.ActivitySeverityBlocking, true, []observation.ActivityAttentionReason{observation.ActivityAttentionWaiting}
	}
	if counts.Running > 0 {
		return observation.ActivityStatusRunning, observation.ActivitySeverityNormal, true, []observation.ActivityAttentionReason{observation.ActivityAttentionRunning}
	}
	if counts.Pending > 0 {
		return observation.ActivityStatusPending, observation.ActivitySeverityQuiet, false, nil
	}
	if counts.Canceled > 0 && counts.Success == 0 {
		return observation.ActivityStatusCanceled, observation.ActivitySeverityWarning, false, nil
	}
	return observation.ActivityStatusSuccess, observation.ActivitySeverityNormal, false, nil
}

func subAgentActivityTerminal(status agentharness.SubAgentStatus) bool {
	switch status {
	case agentharness.SubAgentStatusCompleted, agentharness.SubAgentStatusFailed, agentharness.SubAgentStatusCancelled, agentharness.SubAgentStatusClosed:
		return true
	default:
		return false
	}
}

func activityTimeUnixMS(value time.Time, fallback int64) int64 {
	if value.IsZero() {
		return fallback
	}
	return value.UnixMilli()
}

func stableSubAgentActivityHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func firstRuntimeNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func subAgentDetail(in agentharness.SubAgentDetail) SubAgentDetail {
	return SubAgentDetail{
		Snapshot:         subAgentSnapshot(in.Snapshot),
		Events:           subAgentThreadDetailEvents(in.Events),
		ActivityTimeline: cloneRuntimeActivityTimeline(in.ActivityTimeline),
		Context:          subAgentDetailContext(in.Snapshot.ThreadID, in.Context),
		NextOrdinal:      in.NextOrdinal,
		HasMore:          in.HasMore,
		RetainedFrom:     in.RetainedFrom,
		GeneratedAt:      in.GeneratedAt,
	}
}

func subAgentDetailContext(threadID string, in agentharness.ThreadContextSnapshot) ThreadContextSnapshot {
	return ThreadContextSnapshot{
		ThreadID: ThreadID(threadID),
		Provider: in.Model.Provider,
		Model:    in.Model.Model,
		Policy: config.ContextPolicy{
			ContextWindowTokens:  in.Policy.ContextWindowTokens,
			MaxOutputTokens:      in.Policy.MaxOutputTokens,
			ReservedOutputTokens: in.Policy.ReservedOutputTokens,
		},
		Usage:       cloneContextStatus(in.Usage),
		Compactions: subAgentDetailContextCompactions(in.Compactions),
		UpdatedAt:   in.UpdatedAt,
	}
}

func subAgentDetailContextCompactions(in []agentharness.ThreadContextCompaction) []observation.CompactionEvent {
	if len(in) == 0 {
		return nil
	}
	out := make([]observation.CompactionEvent, 0, len(in))
	for _, compact := range in {
		out = append(out, observation.CompactionEvent{
			RunID:               compact.RunID,
			ThreadID:            compact.ThreadID,
			TurnID:              compact.TurnID,
			Step:                compact.Step,
			OperationID:         compact.OperationID,
			RequestID:           compact.RequestID,
			Phase:               observation.CompactionPhase(compact.Phase),
			Status:              observation.CompactionStatus(compact.Status),
			Trigger:             compact.Trigger,
			Reason:              compact.Reason,
			Source:              compact.Source,
			TokensBefore:        compact.TokensBefore,
			TokensAfterEstimate: compact.TokensAfterEstimate,
			Error:               compact.Error,
			ObservedAt:          compact.ObservedAt,
		})
	}
	return out
}

func cloneContextStatus(in *observation.ContextStatus) *observation.ContextStatus {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func threadDetailEvents(in []agentharness.SubAgentDetailEvent) []ThreadDetailEvent {
	out := make([]ThreadDetailEvent, 0, len(in))
	for _, ev := range in {
		out = append(out, threadDetailEvent(ev))
	}
	return out
}

func subAgentThreadDetailEvents(in []agentharness.SubAgentDetailEvent) []ThreadDetailEvent {
	out := threadDetailEvents(in)
	for index := range out {
		out[index].ActivityTimeline = nil
	}
	return out
}

func threadDetailEvent(in agentharness.SubAgentDetailEvent) ThreadDetailEvent {
	return ThreadDetailEvent{
		ID:         in.ID,
		Ordinal:    in.Ordinal,
		ParentID:   in.ParentID,
		ThreadID:   ThreadID(in.ThreadID),
		TurnID:     TurnID(in.TurnID),
		Kind:       ThreadDetailEventKind(in.Kind),
		Type:       in.Type,
		CreatedAt:  in.CreatedAt,
		Message:    threadDetailMessage(in.Message),
		ToolCall:   threadDetailToolCall(in.ToolCall),
		ToolResult: threadDetailToolResult(in.ToolResult),
		Approval:   threadDetailApproval(in.Approval),
		TurnMarker: threadDetailTurnMarker(in.TurnMarker),
		Compaction: threadDetailCompaction(in.Compaction),
		Error:      in.Error,
		Metadata:   cloneStringMap(in.Metadata),

		ActivityTimeline: observation.CloneActivityTimeline(in.ActivityTimeline),
	}
}

func threadDetailMessage(in *agentharness.SubAgentDetailMessage) *ThreadDetailMessage {
	if in == nil {
		return nil
	}
	return &ThreadDetailMessage{
		Role:        in.Role,
		Kind:        in.Kind,
		Preview:     in.Preview,
		Content:     in.Content,
		Attachments: runtimeMessageAttachments(in.Attachments),
		References:  runtimeMessageReferences(in.References),
		Reasoning:   in.Reasoning,
		Activity:    cloneActivityPresentation(in.Activity),
	}
}

func threadDetailToolCall(in *agentharness.SubAgentDetailToolCall) *ThreadDetailToolCall {
	if in == nil {
		return nil
	}
	out := &ThreadDetailToolCall{ID: in.ID, Name: in.Name, ArgsPreview: in.ArgsPreview, ArgsJSON: in.ArgsJSON, ArgsHash: in.ArgsHash}
	if in.ControlSignal != nil {
		out.ControlSignal = &ThreadDetailControlSignal{
			Name:        in.ControlSignal.Name,
			CallID:      in.ControlSignal.CallID,
			Disposition: in.ControlSignal.Disposition,
			Text:        in.ControlSignal.Text,
			ArgsHash:    in.ControlSignal.ArgsHash,
			Payload:     cloneAnyMap(in.ControlSignal.Payload),
		}
	}
	return out
}

func threadDetailToolResult(in *agentharness.SubAgentDetailToolResult) *ThreadDetailToolResult {
	if in == nil {
		return nil
	}
	out := &ThreadDetailToolResult{
		CallID:          in.CallID,
		ToolName:        in.ToolName,
		EffectAttemptID: in.EffectAttemptID,
		Status:          in.Status,
		Preview:         in.Preview,
		Content:         in.Content,
		Truncated:       in.Truncated,
		OriginalBytes:   in.OriginalBytes,
		VisibleBytes:    in.VisibleBytes,
		OriginalLines:   in.OriginalLines,
		VisibleLines:    in.VisibleLines,
		Strategy:        in.Strategy,
		ContentSHA256:   in.ContentSHA256,
	}
	if in.FullOutput != nil {
		out.FullOutput = &ArtifactRef{
			ID:        ArtifactID(in.FullOutput.ID),
			SafeLabel: in.FullOutput.SafeLabel,
			Kind:      in.FullOutput.Kind,
			MIME:      in.FullOutput.MIME,
			SizeBytes: in.FullOutput.SizeBytes,
			SHA256:    in.FullOutput.SHA256,
		}
	}
	return out
}

func threadDetailApproval(in *agentharness.SubAgentDetailApproval) *ThreadDetailApproval {
	if in == nil {
		return nil
	}
	return &ThreadDetailApproval{
		State:    in.State,
		ToolID:   in.ToolID,
		ToolName: in.ToolName,
		ToolKind: in.ToolKind,
		ArgsHash: in.ArgsHash,
		Reason:   in.Reason,
		Metadata: cloneStringMap(in.Metadata),
	}
}

func threadDetailTurnMarker(in *agentharness.SubAgentDetailTurnMarker) *ThreadDetailTurnMarker {
	if in == nil {
		return nil
	}
	return &ThreadDetailTurnMarker{Status: in.Status, Metadata: cloneStringMap(in.Metadata)}
}

func threadDetailCompaction(in *agentharness.SubAgentDetailCompaction) *ThreadDetailCompaction {
	if in == nil {
		return nil
	}
	return &ThreadDetailCompaction{
		OperationID:             in.OperationID,
		RequestID:               in.RequestID,
		Source:                  in.Source,
		CompactionID:            in.CompactionID,
		PreviousCompactionID:    in.PreviousCompactionID,
		CompactedThroughEntryID: in.CompactedThroughEntryID,
		SummarySchemaVersion:    in.SummarySchemaVersion,
		CompactionGeneration:    in.CompactionGeneration,
		CompactionWindowID:      in.CompactionWindowID,
		FirstKeptEntryID:        in.FirstKeptEntryID,
		KeptUserEntryIDs:        append([]string(nil), in.KeptUserEntryIDs...),
		Summary:                 in.Summary,
		Trigger:                 in.Trigger,
		Reason:                  in.Reason,
		Phase:                   in.Phase,
		TokensBefore:            in.TokensBefore,
		TokensAfterEstimate:     in.TokensAfterEstimate,
		Metadata:                safeStringMetadata(in.Metadata),
	}
}

func cloneThreadDetailEvents(in []ThreadDetailEvent) []ThreadDetailEvent {
	if len(in) == 0 {
		return nil
	}
	out := make([]ThreadDetailEvent, 0, len(in))
	for _, ev := range in {
		out = append(out, cloneThreadDetailEvent(ev))
	}
	return out
}

func cloneThreadDetailEvent(in ThreadDetailEvent) ThreadDetailEvent {
	return ThreadDetailEvent{
		ID:               in.ID,
		Ordinal:          in.Ordinal,
		ParentID:         in.ParentID,
		ThreadID:         in.ThreadID,
		TurnID:           in.TurnID,
		Kind:             in.Kind,
		Type:             in.Type,
		CreatedAt:        in.CreatedAt,
		Message:          cloneThreadDetailMessage(in.Message),
		ToolCall:         cloneThreadDetailToolCall(in.ToolCall),
		ToolResult:       cloneThreadDetailToolResult(in.ToolResult),
		Approval:         cloneThreadDetailApproval(in.Approval),
		TurnMarker:       cloneThreadDetailTurnMarker(in.TurnMarker),
		Compaction:       cloneThreadDetailCompaction(in.Compaction),
		Error:            in.Error,
		Metadata:         cloneStringMap(in.Metadata),
		ActivityTimeline: observation.CloneActivityTimeline(in.ActivityTimeline),
	}
}

func cloneThreadDetailMessage(in *ThreadDetailMessage) *ThreadDetailMessage {
	if in == nil {
		return nil
	}
	return &ThreadDetailMessage{
		Role:        in.Role,
		Kind:        in.Kind,
		Preview:     in.Preview,
		Content:     in.Content,
		Attachments: append([]MessageAttachment(nil), in.Attachments...),
		References:  append([]MessageReference(nil), in.References...),
		Reasoning:   in.Reasoning,
		Activity:    cloneActivityPresentation(in.Activity),
	}
}

func cloneThreadDetailToolCall(in *ThreadDetailToolCall) *ThreadDetailToolCall {
	if in == nil {
		return nil
	}
	out := *in
	if in.ControlSignal != nil {
		signal := *in.ControlSignal
		signal.Payload = cloneAnyMap(in.ControlSignal.Payload)
		out.ControlSignal = &signal
	}
	return &out
}

func cloneThreadDetailToolResult(in *ThreadDetailToolResult) *ThreadDetailToolResult {
	if in == nil {
		return nil
	}
	out := *in
	out.FullOutput = cloneArtifactRef(in.FullOutput)
	return &out
}

func cloneThreadDetailApproval(in *ThreadDetailApproval) *ThreadDetailApproval {
	if in == nil {
		return nil
	}
	out := *in
	out.Metadata = cloneStringMap(in.Metadata)
	return &out
}

func cloneThreadDetailTurnMarker(in *ThreadDetailTurnMarker) *ThreadDetailTurnMarker {
	if in == nil {
		return nil
	}
	out := *in
	out.Metadata = cloneStringMap(in.Metadata)
	return &out
}

func cloneThreadDetailCompaction(in *ThreadDetailCompaction) *ThreadDetailCompaction {
	if in == nil {
		return nil
	}
	out := *in
	out.KeptUserEntryIDs = append([]string(nil), in.KeptUserEntryIDs...)
	out.Metadata = cloneStringMap(in.Metadata)
	return &out
}

func cloneArtifactRef(in *ArtifactRef) *ArtifactRef {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneThreadTurnProjectionPtr(in *ThreadTurnProjection) *ThreadTurnProjection {
	if in == nil {
		return nil
	}
	out := *in
	out.Segments = make([]ThreadTurnProjectionSegment, 0, len(in.Segments))
	for _, segment := range in.Segments {
		out.Segments = append(out.Segments, cloneThreadTurnProjectionSegment(segment))
	}
	return &out
}

func cloneThreadTurnProjectionSegment(in ThreadTurnProjectionSegment) ThreadTurnProjectionSegment {
	out := in
	out.ActivityTimeline = observation.CloneActivityTimeline(in.ActivityTimeline)
	if in.Signal != nil {
		signal := *in.Signal
		signal.Payload = cloneAnyMap(in.Signal.Payload)
		out.Signal = &signal
	}
	out.EventIDs = append([]string(nil), in.EventIDs...)
	return out
}

func threadIDStrings(ids []ThreadID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, string(id))
	}
	return out
}

type harnessOptions struct {
	Store                   *Store
	Tools                   *tools.Registry
	EffectAuthorizationGate EffectAuthorizationGate
	Sink                    event.Sink
	SinkPolicy              event.SinkPolicy
	Title                   agentharness.TitleGenerator
	ThreadTitleMode         ThreadTitleMode
	NewID                   func(string) string
	LoopLimits              LoopLimits
	SubAgentRunTimeout      time.Duration
	Capabilities            CapabilityOptions
	ToolSurfaceProvider     engine.ToolSurfaceProvider
	StateCompatibilityKey   string
}

func newHarnessWithProvider(cfg config.Config, p provider.Provider, opts harnessOptions) (*agentharness.AgentHarness, error) {
	cfg = config.ResolvePrompt(cfg)
	store := opts.Store
	if store == nil {
		return nil, errors.New("runtime store is required")
	}
	registry := opts.Tools
	if registry == nil {
		registry = tools.NewRegistry()
	}
	capabilities := mergeCapabilityOptions(cfg, opts.Capabilities)
	effectivePrompt, err := applyCapabilities(registry, cfg.SystemPrompt, capabilities, opts.Sink)
	if err != nil {
		return nil, err
	}
	cacheRetention, err := config.PromptCacheRetention(cfg)
	if err != nil {
		return nil, err
	}
	turnPolicy := agentharness.TurnPolicy{
		ContextPolicy:  configbridge.ContextPolicy(cfg.ContextPolicy),
		Reasoning:      configbridge.ReasoningSelection(cfg.Reasoning),
		CacheRetention: configbridge.CacheRetention(cacheRetention),
	}
	loopLimits := agentharness.LoopLimits{
		MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
		NoProgressLimit:         cfg.NoProgressLimit,
		DuplicateToolLimit:      cfg.DuplicateToolLimit,
		WallTime:                cfg.WallTime,
	}
	if opts.LoopLimits.MaxEmptyProviderRetries > 0 {
		loopLimits.MaxEmptyProviderRetries = opts.LoopLimits.MaxEmptyProviderRetries
	}
	if opts.LoopLimits.NoProgressLimit > 0 {
		loopLimits.NoProgressLimit = opts.LoopLimits.NoProgressLimit
	}
	if opts.LoopLimits.DuplicateToolLimit > 0 {
		loopLimits.DuplicateToolLimit = opts.LoopLimits.DuplicateToolLimit
	}
	if opts.LoopLimits.WallTime > 0 {
		loopLimits.WallTime = opts.LoopLimits.WallTime
	}
	model, _ := catalog.FindModel(cfg.Provider, cfg.Model)
	titleGenerator := opts.Title
	if titleGenerator == nil && opts.ThreadTitleMode == ThreadTitleModeProvider {
		titleGenerator = agentharness.ProviderTitleGenerator{
			Provider:     p,
			ProviderName: cfg.Provider,
			Model:        cfg.Model,
			Reasoning:    model.Reasoning,
		}
	}
	harness := agentharness.New(agentharness.Options{
		Provider:                 p,
		ProviderName:             cfg.Provider,
		Model:                    cfg.Model,
		SystemPrompt:             effectivePrompt,
		Tools:                    registry,
		PromptStore:              store.prompt,
		Repo:                     store.repo,
		ForkOperations:           store.forkOperations,
		StateCompatibilityKey:    opts.StateCompatibilityKey,
		Sink:                     opts.Sink,
		SinkPolicy:               opts.SinkPolicy,
		EffectAuthorizationGate:  runtimeEffectAuthorizationGate(opts.EffectAuthorizationGate),
		ToolSurfaceProvider:      opts.ToolSurfaceProvider,
		TitleGenerator:           titleGenerator,
		CompactionPrompt:         compaction.PromptOptions{},
		Reasoning:                model.Reasoning,
		TurnPolicy:               turnPolicy,
		LoopLimits:               loopLimits,
		SubAgentRunTimeout:       opts.SubAgentRunTimeout,
		BeginBackgroundExecution: store.beginLifetimeOperationContext,
		ReportBackgroundError:    store.reportBackgroundError,
		TurnExecutions:           store.turnExecutionRegistry(),
		NewID:                    opts.NewID,
	})
	if err := store.recoverPendingAutomaticThreadTitles(harness); err != nil {
		return nil, err
	}
	return harness, nil
}

func runtimeStateCompatibilityKey(cfg config.Config, opts providerHostOptions) string {
	if opts.ModelGateway != nil {
		return strings.TrimSpace(opts.ModelGatewayIdentity.StateCompatibilityKey)
	}
	raw := strings.Join([]string{
		strings.TrimSpace(cfg.Provider),
		strings.TrimSpace(cfg.Model),
		strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/"),
	}, "\x00")
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func mergeCapabilityOptions(cfg config.Config, explicit CapabilityOptions) CapabilityOptions {
	out := explicit
	if !out.SkillsEnabled {
		out.SkillsEnabled = cfg.SkillsEnabled
	}
	if out.SkillPromptBudgetBytes <= 0 {
		out.SkillPromptBudgetBytes = cfg.SkillPromptBudgetBytes
	}
	if len(out.SkillSources) == 0 {
		out.SkillSources = append([]string(nil), cfg.SkillSources...)
	}
	return out
}

func applyCapabilities(registry *tools.Registry, basePrompt string, capability CapabilityOptions, sink event.Sink) (string, error) {
	if !capability.SkillsEnabled {
		return basePrompt, nil
	}
	sources := make([]skills.Source, 0, len(capability.SkillSources))
	for _, root := range capability.SkillSources {
		sources = append(sources, skills.Source{Root: root, Kind: skills.SourceConfig, Enabled: true})
	}
	catalog, err := skills.Discover(sources)
	if err != nil {
		return "", err
	}
	emitSkillDiagnostics(sink, catalog.Diagnostics)
	for _, skill := range catalog.Skills {
		emitSkillEvent(sink, event.SkillDetected, map[string]any{
			"skill_id":     skill.Name,
			"source_kind":  string(skill.SourceInfo.Kind),
			"source_label": skill.SourceInfo.DisplayLabel,
			"content_hash": skill.ContentHash,
		})
	}
	prompt, promptDiagnostics := skills.BuildPrompt(catalog.Skills, skills.PromptOptions{MaxBytes: capability.SkillPromptBudgetBytes})
	emitSkillDiagnostics(sink, promptDiagnostics)
	if prompt != "" {
		emitSkillEvent(sink, event.SkillDisclosureApplied, map[string]any{
			"skill_count":   len(catalog.Skills),
			"prompt_bytes":  len(prompt),
			"prompt_sha256": event.StableHash(prompt),
		})
		basePrompt = appendPromptMaterial(basePrompt, prompt)
	}
	if len(catalog.Skills) == 0 {
		return basePrompt, nil
	}
	tool, err := skills.DefineSkillTool(catalog.Skills, skills.ToolOptions{
		OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 64 * 1024, Strategy: tools.OutputHead, PreserveFull: true},
		OnLoad: func(load skills.SkillLoad) {
			emitSkillEvent(sink, event.SkillLoaded, map[string]any{
				"skill_id":     load.Name,
				"source_kind":  string(load.SourceKind),
				"content_hash": load.ContentHash,
				"bytes":        load.Bytes,
			})
		},
	})
	if err != nil {
		return "", err
	}
	if err := registry.Register(tool); err != nil {
		return "", err
	}
	return basePrompt, nil
}

func appendPromptMaterial(base, addition string) string {
	base = strings.TrimRight(base, "\n")
	addition = strings.TrimSpace(addition)
	if addition == "" {
		return base
	}
	if base == "" {
		return addition
	}
	return base + "\n\n" + addition
}

func emitSkillDiagnostics(sink event.Sink, diagnostics []skills.Diagnostic) {
	for _, diagnostic := range diagnostics {
		emitSkillEvent(sink, event.SkillBlocked, map[string]any{
			"failure_category": diagnostic.Kind,
			"skill_id":         diagnostic.SkillName,
			"source_kind":      string(diagnostic.SourceKind),
			"path":             diagnostic.Path,
			"message":          diagnostic.Message,
			"next_action":      "Fix or remove the downstream skill source entry.",
		})
	}
}

func emitSkillEvent(sink event.Sink, typ event.Type, metadata map[string]any) {
	if sink == nil {
		return
	}
	sink.Emit(event.Event{Type: typ, Metadata: metadata})
}

type runtimeEventSink struct {
	mu         *sync.Mutex
	sink       EventSink
	projection *runtimeLiveProjectionRecorder
}

func newRuntimeEventSink(sink EventSink) runtimeEventSink {
	if sink == nil {
		return runtimeEventSink{}
	}
	return runtimeEventSink{
		mu:         &sync.Mutex{},
		sink:       sink,
		projection: &runtimeLiveProjectionRecorder{},
	}
}

func (s runtimeEventSink) Emit(ev event.Event) {
	s.EmitWithActivityTimeline(ev, nil)
}

func (s runtimeEventSink) EmitWithActivityTimeline(ev event.Event, timeline *observation.ActivityTimeline) {
	if s.sink == nil {
		return
	}
	if s.mu != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	out := runtimeEvent(ev)
	out.ActivityTimeline = observation.CloneActivityTimeline(timeline)
	if s.projection != nil {
		out.Projection = s.projection.project(out)
	}
	s.sink.EmitEvent(out)
}

func runtimeEvent(ev event.Event) Event {
	contextStatus := runtimeContextStatus(ev)
	sanitized := event.Sanitize(ev)
	committed := runtimeCommittedEvent(ev, sanitized)
	compactionEvent := runtimeCompactionEventWithError(ev, sanitized, sanitized.Err)
	compactionDebugEvent := runtimeCompactionDebugEventWithError(ev, sanitized, sanitized.Err)
	stream := runtimeStreamObservation(ev, sanitized.Metadata)
	ev = sanitized
	return Event{
		Type:               ev.Type,
		TraceID:            TraceID(ev.TraceID),
		RunID:              RunID(ev.RunID),
		ThreadID:           ThreadID(ev.ThreadID),
		TurnID:             TurnID(ev.TurnID),
		Step:               ev.Step,
		Provider:           ev.Provider,
		Model:              ev.Model,
		Message:            ev.Message,
		Result:             ev.Result,
		Error:              ev.Err,
		ToolID:             ev.ToolID,
		ToolName:           ev.ToolName,
		ToolKind:           ev.ToolKind,
		ArgsHash:           ev.ArgsHash,
		DurationMS:         ev.Duration,
		FinishReason:       observation.FinishReason(ev.FinishReason),
		RawFinishReason:    ev.RawFinishReason,
		FinishInferred:     ev.FinishInferred,
		CompletionReason:   observation.CompletionReason(ev.CompletionReason),
		ContinuationReason: observation.ContinuationReason(ev.ContinuationReason),
		Activity:           cloneActivityPresentation(ev.Activity),
		Stream:             stream,
		Committed:          committed,
		ContextStatus:      contextStatus,
		Compaction:         compactionEvent,
		CompactionDebug:    compactionDebugEvent,
		Sources:            runtimeSourceRefs(ev.Sources),
		Metadata:           safeMetadata(ev.Metadata),
		Timestamp:          ev.Timestamp,
	}
}

func runtimeCommittedEvent(raw, sanitized event.Event) *ThreadDetailEvent {
	if sanitized.Type != event.ThreadEntryCommitted {
		return nil
	}
	meta, _ := raw.Metadata.(map[string]any)
	detail, ok := raw.Payload.(agentharness.SubAgentDetailEvent)
	if !ok {
		return nil
	}
	out := threadDetailEvent(detail)
	out.RunID = RunID(sanitized.RunID)
	out.Step = sanitized.Step
	if out.Ordinal == 0 {
		out.Ordinal = int64FromMetadata(meta, "ordinal")
	}
	if out.CreatedAt.IsZero() {
		out.CreatedAt = sanitized.Timestamp
	}
	return &out
}

type runtimeLiveProjectionRecorder struct {
	eventsByTurn map[string][]ThreadDetailEvent
}

func (r *runtimeLiveProjectionRecorder) project(ev Event) *ThreadTurnProjection {
	if r == nil || ev.Committed == nil {
		return nil
	}
	threadID := strings.TrimSpace(string(ev.ThreadID))
	turnID := strings.TrimSpace(string(ev.TurnID))
	runID := strings.TrimSpace(string(ev.RunID))
	if threadID == "" || turnID == "" || runID == "" {
		return nil
	}
	if r.eventsByTurn == nil {
		r.eventsByTurn = map[string][]ThreadDetailEvent{}
	}
	key := runtimeLiveProjectionTurnKey(threadID, turnID, runID)
	events := append(r.eventsByTurn[key], cloneThreadDetailEvent(*ev.Committed))
	r.eventsByTurn[key] = events
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: ThreadID(threadID),
		TurnID:   TurnID(turnID),
		RunID:    RunID(runID),
		TraceID:  TraceID(runID),
		Events:   cloneThreadDetailEvents(events),
	})
	return cloneThreadTurnProjectionPtr(&projection)
}

func runtimeLiveProjectionTurnKey(threadID string, turnID string, runID string) string {
	return threadID + "\x00" + turnID + "\x00" + runID
}

func runtimeContextStatus(ev event.Event) *observation.ContextStatus {
	switch ev.Type {
	case event.ProviderRequest:
		meta, ok := ev.Metadata.(map[string]any)
		if !ok {
			return nil
		}
		pressure, ok := meta["context_pressure"].(contextpolicy.ContextPressure)
		if !ok {
			return nil
		}
		estimate, _ := meta["request_estimate"].(contextpolicy.RequestEstimate)
		status := observation.ContextStatusFromRequest(observation.RequestObservation{
			RunID:             ev.RunID,
			ThreadID:          ev.ThreadID,
			TurnID:            ev.TurnID,
			Step:              ev.Step,
			RequestID:         stringFromMetadata(meta, "request_id"),
			LogicalRequestID:  stringFromMetadata(meta, "logical_request_id"),
			Attempt:           intFromMetadata(meta, "attempt"),
			Provider:          ev.Provider,
			Model:             ev.Model,
			ObservedAt:        ev.Timestamp,
			RequestEstimate:   configbridge.RequestEstimate(estimate),
			ProjectedPressure: configbridge.PublicContextPressure(pressure),
		})
		return &status
	case event.ProviderUsage:
		status, ok := ev.Metadata.(engine.ProviderUsageContextStatus)
		if !ok || status.Phase != engine.ProviderUsagePhaseFinalContextStatus {
			return nil
		}
		out, ok := observation.ContextStatusFromProviderUsage(observation.ProviderUsageObservation{
			RunID:            ev.RunID,
			ThreadID:         ev.ThreadID,
			TurnID:           ev.TurnID,
			Step:             ev.Step,
			RequestID:        status.RequestID,
			LogicalRequestID: status.LogicalRequestID,
			Attempt:          status.Attempt,
			Provider:         ev.Provider,
			Model:            ev.Model,
			ObservedAt:       ev.Timestamp,
			Usage:            observationProviderUsage(status.Usage),
			RequestEstimate:  configbridge.RequestEstimate(status.RequestEstimate),
			ContextPressure:  configbridge.PublicContextPressure(status.ContextPressure),
		})
		if !ok {
			return nil
		}
		return &out
	default:
		return nil
	}
}

func runtimeCompactionEvent(ev event.Event) *observation.CompactionEvent {
	sanitized := event.Sanitize(ev)
	return runtimeCompactionEventWithError(ev, sanitized, sanitized.Err)
}

func runtimeCompactionEventWithError(raw, sanitized event.Event, sanitizedError string) *observation.CompactionEvent {
	if sanitized.Type != event.ContextCompact {
		return nil
	}
	meta, ok := sanitized.Metadata.(map[string]any)
	if !ok {
		return nil
	}
	rawMeta, _ := raw.Metadata.(map[string]any)
	phase := observation.CompactionPhase(stringFromMetadata(meta, "phase"))
	if !phase.Valid() || (sanitizedError != "" && phase != observation.CompactionPhaseFailed && phase != observation.CompactionPhaseCancelled) {
		return nil
	}
	out := observation.CompactionEvent{
		RunID:               sanitized.RunID,
		ThreadID:            sanitized.ThreadID,
		TurnID:              sanitized.TurnID,
		Step:                sanitized.Step,
		OperationID:         stringFromMetadata(meta, "operation_id"),
		RequestID:           stringFromMetadata(meta, "request_id"),
		Phase:               phase,
		Status:              observation.CompactionStatusRunning,
		Trigger:             stringFromMetadata(meta, "trigger"),
		Reason:              stringFromMetadata(meta, "reason"),
		Source:              stringFromMetadata(meta, "source"),
		TokensBefore:        int64FromMetadata(meta, "tokens_before"),
		TokensAfterEstimate: int64FromMetadata(meta, "tokens_after_estimate"),
		Error:               sanitizedError,
		ObservedAt:          sanitized.Timestamp,
	}
	switch phase {
	case observation.CompactionPhaseStart:
		out.Status = observation.CompactionStatusRunning
	case observation.CompactionPhaseComplete:
		out.Status = observation.CompactionStatusCompacted
	case observation.CompactionPhaseFailed:
		out.Status = observation.CompactionStatusFailed
	case observation.CompactionPhaseCancelled:
		out.Status = observation.CompactionStatusCancelled
	case observation.CompactionPhaseNoop:
		out.Status = observation.CompactionStatusNoop
	default:
		return nil
	}
	if pressure, ok := rawMeta["before_pressure"].(contextpolicy.ContextPressure); ok {
		out.BeforePressure = configbridge.PublicContextPressure(pressure)
	}
	if usage, ok := rawMeta["message_context_before"].(contextpolicy.Usage); ok {
		out.ContextBefore = configbridge.PublicContextUsage(usage)
		out.TokensBefore = usage.InputTokens
	}
	if usage, ok := rawMeta["context_before"].(contextpolicy.Usage); ok {
		out.ContextBefore = configbridge.PublicContextUsage(usage)
		if out.TokensBefore == 0 {
			out.TokensBefore = usage.InputTokens
		}
	}
	if usage, ok := rawMeta["context_after"].(contextpolicy.Usage); ok {
		out.ContextAfter = configbridge.PublicContextUsage(usage)
	}
	return &out
}

func runtimeCompactionDebugEvent(ev event.Event) *observation.CompactionDebugEvent {
	sanitized := event.Sanitize(ev)
	return runtimeCompactionDebugEventWithError(ev, sanitized, sanitized.Err)
}

func runtimeCompactionDebugEventWithError(raw, sanitized event.Event, sanitizedError string) *observation.CompactionDebugEvent {
	if sanitized.Type != event.ContextCompactDebug {
		return nil
	}
	meta, ok := sanitized.Metadata.(map[string]any)
	if !ok {
		return nil
	}
	rawMeta, _ := raw.Metadata.(map[string]any)
	stage := observation.CompactionDebugStage(stringFromMetadata(meta, "stage"))
	status := observation.CompactionDebugStatus(stringFromMetadata(meta, "status"))
	if !stage.Valid() || !status.Valid() {
		return nil
	}
	out := observation.CompactionDebugEvent{
		RunID:                            sanitized.RunID,
		ThreadID:                         sanitized.ThreadID,
		TurnID:                           sanitized.TurnID,
		Step:                             sanitized.Step,
		OperationID:                      stringFromMetadata(meta, "operation_id"),
		RequestID:                        stringFromMetadata(meta, "request_id"),
		Stage:                            stage,
		Status:                           status,
		Trigger:                          stringFromMetadata(meta, "trigger"),
		Reason:                           stringFromMetadata(meta, "reason"),
		Source:                           stringFromMetadata(meta, "source"),
		CompactionConvergenceAttempt:     intFromMetadata(meta, "compaction_convergence_attempt"),
		HistoryMessageCount:              intFromMetadata(meta, "history_message_count"),
		ActiveMessageCount:               intFromMetadata(meta, "active_message_count"),
		TokensBefore:                     int64FromMetadata(meta, "tokens_before"),
		TokensAfterEstimate:              int64FromMetadata(meta, "tokens_after_estimate"),
		HardLimitExceeded:                boolFromAnyMetadata(meta, "hard_limit_exceeded"),
		FixedInputTokens:                 int64FromMetadata(meta, "fixed_input_tokens"),
		ReducibleInputTokens:             int64FromMetadata(meta, "reducible_input_tokens"),
		RequestSafeLimit:                 int64FromMetadata(meta, "request_safe_limit"),
		CompactedContextTargetTokens:     int64FromMetadata(meta, "compacted_context_target_tokens"),
		NextCompactedContextTargetTokens: int64FromMetadata(meta, "next_compacted_context_target_tokens"),
		ConsecutiveFailures:              intFromMetadata(meta, "consecutive_failures"),
		DurationMS:                       sanitized.Duration,
		ProviderStateKind:                stringFromMetadata(meta, "provider_state_kind"),
		NextAction:                       stringFromMetadata(meta, "next_action"),
		Error:                            sanitizedError,
		ObservedAt:                       sanitized.Timestamp,
	}
	if duration := int64FromMetadata(meta, "duration_ms"); duration > 0 {
		out.DurationMS = duration
	}
	if pressure, ok := rawMeta["before_pressure"].(contextpolicy.ContextPressure); ok {
		out.BeforePressure = configbridge.PublicContextPressure(pressure)
	}
	if pressure, ok := rawMeta["validated_context_pressure"].(contextpolicy.ContextPressure); ok {
		out.ValidatedContextPressure = configbridge.PublicContextPressure(pressure)
		if !out.HardLimitExceeded {
			out.HardLimitExceeded = pressure.HardLimitExceeded
		}
	}
	if estimate, ok := rawMeta["request_estimate"].(contextpolicy.RequestEstimate); ok {
		out.RequestEstimate = configbridge.RequestEstimate(estimate)
	}
	if usage, ok := rawMeta["context_before"].(contextpolicy.Usage); ok {
		out.ContextBefore = configbridge.PublicContextUsage(usage)
		if out.TokensBefore == 0 {
			out.TokensBefore = usage.InputTokens
		}
	}
	if usage, ok := rawMeta["message_context_before"].(contextpolicy.Usage); ok {
		out.ContextBefore = configbridge.PublicContextUsage(usage)
		if out.TokensBefore == 0 {
			out.TokensBefore = usage.InputTokens
		}
	}
	if usage, ok := rawMeta["context_after"].(contextpolicy.Usage); ok {
		out.ContextAfter = configbridge.PublicContextUsage(usage)
	}
	return &out
}

func observationProviderUsage(in provider.Usage) observation.ProviderUsage {
	in = in.Normalized()
	return observation.ProviderUsage{
		InputTokens:       in.InputTokens,
		OutputTokens:      in.OutputTokens,
		ReasoningTokens:   in.ReasoningTokens,
		CacheReadTokens:   in.CacheReadTokens,
		CacheWriteTokens:  in.CacheWriteTokens,
		TotalTokens:       in.TotalTokens,
		WindowInputTokens: in.WindowInputTokens,
		CostUSD:           in.CostUSD,
		Source:            string(in.Source),
		Available:         in.Available,
	}
}

func runtimeStreamObservation(ev event.Event, safeMetadata any) *StreamObservation {
	var streamType StreamObservationType
	var text string
	var reason string
	var toolCallStream *ModelToolCallStream
	switch ev.Type {
	case event.ProviderDelta:
		streamType = StreamObservationAssistantDelta
		text = ev.Message
	case event.ProviderReasoning:
		streamType = StreamObservationReasoningDelta
		text = ev.Message
	case event.ProviderToolCallStart:
		streamType = StreamObservationToolCallStart
		toolCallStream = runtimeModelToolCallStream(ev)
	case event.ProviderToolCallDelta:
		streamType = StreamObservationToolCallDelta
		toolCallStream = runtimeModelToolCallStream(ev)
	case event.ProviderToolCallEnd:
		streamType = StreamObservationToolCallEnd
		toolCallStream = runtimeModelToolCallStream(ev)
	case event.ProviderRetry:
		streamType = StreamObservationModelRetry
		reason = ev.Message
	case event.ProviderFinish:
		streamType = StreamObservationModelStreamDone
		reason = ev.Message
	case event.RunEnd:
		switch ev.Message {
		case string(engine.Failed), string(engine.Cancelled):
			streamType = StreamObservationModelStreamAbort
			reason = ev.Err
		default:
			return nil
		}
	default:
		return nil
	}
	out := &StreamObservation{
		Type:            streamType,
		Text:            text,
		ToolCallStream:  toolCallStream,
		Reason:          reason,
		FinishReason:    observation.FinishReason(ev.FinishReason),
		RawFinishReason: ev.RawFinishReason,
		FinishInferred:  ev.FinishInferred,
		Attempt:         streamAttemptFromMetadata(safeMetadata),
		Labels:          streamLabelsFromMetadata(safeMetadata),
	}
	if out.Reason == "" && ev.Err != "" {
		out.Reason = ev.Err
	}
	return out
}

func runtimeModelToolCallStream(ev event.Event) *ModelToolCallStream {
	id := strings.TrimSpace(ev.ToolID)
	name := strings.TrimSpace(ev.ToolName)
	if id == "" && name == "" {
		return nil
	}
	return &ModelToolCallStream{
		ID:   id,
		Name: name,
	}
}

func runtimeSourceRefs(in []event.SourceRef) []SourceRef {
	out := make([]SourceRef, 0, len(in))
	for _, ref := range in {
		if strings.TrimSpace(ref.Title) == "" && strings.TrimSpace(ref.URL) == "" {
			continue
		}
		out = append(out, SourceRef{
			Title: strings.TrimSpace(ref.Title),
			URL:   strings.TrimSpace(ref.URL),
		})
	}
	return out
}

func streamAttemptFromMetadata(metadata any) int {
	values, ok := metadata.(map[string]any)
	if !ok {
		return 0
	}
	switch v := values["attempt"].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func streamLabelsFromMetadata(metadata any) RunLabels {
	values, ok := metadata.(map[string]any)
	if !ok {
		return RunLabels{}
	}
	rawLabels, ok := values["labels"]
	if !ok {
		return RunLabels{}
	}
	labels := metadataStringMap(rawLabels)
	if len(labels) == 0 {
		return RunLabels{}
	}
	out := RunLabels{}
	for key, value := range labels {
		if strings.HasPrefix(key, "correlation.") {
			if out.Correlation == nil {
				out.Correlation = map[string]string{}
			}
			out.Correlation[strings.TrimPrefix(key, "correlation.")] = value
		}
	}
	return out
}

func metadataStringMap(value any) map[string]string {
	switch v := value.(type) {
	case map[string]string:
		return v
	case map[string]any:
		out := make(map[string]string, len(v))
		for key, item := range v {
			text, ok := item.(string)
			if ok {
				out[key] = text
			}
		}
		return out
	default:
		return nil
	}
}

func stringFromMetadata(meta map[string]any, key string) string {
	switch v := meta[key].(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		if v == nil {
			return ""
		}
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func intFromMetadata(meta map[string]any, key string) int {
	switch v := meta[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	default:
		return 0
	}
}

func int64FromMetadata(meta map[string]any, key string) int64 {
	switch v := meta[key].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}

func boolFromAnyMetadata(meta map[string]any, key string) bool {
	switch v := meta[key].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func runtimeObservationEvent(ev event.Event) observation.Event {
	sanitized := event.Sanitize(ev)
	return observation.Event{
		Type:               sanitized.Type,
		TraceID:            sanitized.TraceID,
		RunID:              sanitized.RunID,
		ThreadID:           sanitized.ThreadID,
		TurnID:             sanitized.TurnID,
		Step:               sanitized.Step,
		Provider:           sanitized.Provider,
		Model:              sanitized.Model,
		Message:            sanitized.Message,
		Result:             sanitized.Result,
		Error:              sanitized.Err,
		ToolID:             sanitized.ToolID,
		ToolName:           sanitized.ToolName,
		ToolKind:           sanitized.ToolKind,
		ArgsHash:           sanitized.ArgsHash,
		DurationMS:         sanitized.Duration,
		FinishReason:       observation.FinishReason(sanitized.FinishReason),
		RawFinishReason:    sanitized.RawFinishReason,
		FinishInferred:     sanitized.FinishInferred,
		CompletionReason:   observation.CompletionReason(sanitized.CompletionReason),
		ContinuationReason: observation.ContinuationReason(sanitized.ContinuationReason),
		Activity:           cloneActivityPresentation(sanitized.Activity),
		Compaction:         runtimeCompactionEventWithError(ev, sanitized, sanitized.Err),
		CompactionDebug:    runtimeCompactionDebugEventWithError(ev, sanitized, sanitized.Err),
		Metadata:           safeMetadata(sanitized.Metadata),
		ObservedAt:         sanitized.Timestamp,
	}
}

func cloneActivityPresentation(in *observation.ActivityPresentation) *observation.ActivityPresentation {
	if in == nil {
		return nil
	}
	out := *in
	out.Chips = append([]observation.ActivityChip(nil), in.Chips...)
	out.TargetRefs = append([]observation.ActivityTargetRef(nil), in.TargetRefs...)
	out.Payload = cloneAnyMap(in.Payload)
	return &out
}

func cloneRuntimeActivityTimeline(in observation.ActivityTimeline) observation.ActivityTimeline {
	cloned := observation.CloneActivityTimeline(&in)
	if cloned == nil {
		return observation.ActivityTimeline{}
	}
	return *cloned
}

type runtimeActivityEventRecorder struct {
	mu     sync.Mutex
	events []observation.Event
	sink   runtimeEventSink
}

func (r *runtimeActivityEventRecorder) Emit(ev event.Event) {
	observed := runtimeObservationEvent(ev)
	var timeline *observation.ActivityTimeline
	r.mu.Lock()
	r.events = append(r.events, observed)
	if runtimeActivityTimelineEvent(ev.Type) {
		built := observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    observed.RunID,
			ThreadID: observed.ThreadID,
			TurnID:   observed.TurnID,
			TraceID:  observed.TraceID,
		}, r.events, time.Now().UnixMilli())
		if len(built.Items) > 0 {
			timeline = &built
		}
	}
	r.mu.Unlock()
	r.sink.EmitWithActivityTimeline(ev, timeline)
}

func (r *runtimeActivityEventRecorder) Snapshot() []observation.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observation.Event(nil), r.events...)
}

func runtimeActivityTimelineEvent(typ event.Type) bool {
	switch typ {
	case event.ToolCall,
		event.ToolDispatchStarted,
		event.ToolActivityUpdated,
		event.ToolResult,
		event.ToolApprovalRequested,
		event.ToolApprovalApproved,
		event.ToolApprovalRejected,
		event.ToolApprovalTimedOut,
		event.ToolApprovalCanceled,
		event.HostedToolCall,
		event.HostedToolResult,
		event.ControlSignal,
		event.BudgetExceeded,
		event.RunEnd:
		return true
	default:
		return false
	}
}

func safeMetadata(in any) map[string]any {
	values, ok := in.(map[string]any)
	if !ok || len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		switch key {
		case "approval_id":
			if hash := stableRuntimeMetadataHash(value); hash != "" {
				out["approval_id_hash"] = hash
			}
			continue
		case "resources",
			"compaction_id",
			"previous_compaction_id",
			"compaction_generation",
			"compaction_window_id",
			"first_kept_entry_id",
			"kept_user_entry_ids",
			"compacted_through_entry_id",
			"summary_schema_version",
			"compaction_phase",
			"provider_ledger_key",
			"provider_request_ledger_key",
			"prompt_cache_segment_key",
			"checkpoint_pointer":
			continue
		}
		out[key] = safeMetadataValue(value)
	}
	return out
}

func safeStringMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		switch key {
		case "compaction_id",
			"previous_compaction_id",
			"compaction_generation",
			"compaction_window_id",
			"first_kept_entry_id",
			"kept_user_entry_ids",
			"compacted_through_entry_id",
			"summary_schema_version",
			"compaction_phase",
			"provider_ledger_key",
			"provider_request_ledger_key",
			"provider_response_ledger_key",
			"prompt_cache_key",
			"prompt_cache_segment_id",
			"checkpoint_payload",
			"checkpoint_pointer":
			continue
		default:
			out[key] = event.SafePathRefsText(value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func stableRuntimeMetadataHash(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])[:16]
}

func safeMetadataValue(value any) any {
	switch v := value.(type) {
	case nil, string, bool, int, int64, float64:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
