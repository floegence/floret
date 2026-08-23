package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v5/config"
	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/internal/agentharness"
	"github.com/floegence/floret/v5/internal/configbridge"
	"github.com/floegence/floret/v5/internal/engine"
	"github.com/floegence/floret/v5/internal/event"
	"github.com/floegence/floret/v5/internal/provider"
	"github.com/floegence/floret/v5/internal/provider/cache"
	"github.com/floegence/floret/v5/internal/provider/catalog"
	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/session/compaction"
	"github.com/floegence/floret/v5/internal/session/contextpolicy"
	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/internal/storage"
	"github.com/floegence/floret/v5/internal/tools/skills"
	"github.com/floegence/floret/v5/observation"
	publicprovider "github.com/floegence/floret/v5/provider"
	"github.com/floegence/floret/v5/storage/spi"
	"github.com/floegence/floret/v5/tools"
)

var (
	// ErrHostClosed reports that Host shutdown has started.
	ErrHostClosed = errors.New("floret host is closed")
	// ErrRevisionUnavailable reports a thread revision outside retained durable history.
	ErrRevisionUnavailable = errors.New("floret thread revision is unavailable")
	// ErrSubscriptionStale reports a subscription that must be recreated from a new snapshot.
	ErrSubscriptionStale = errors.New("floret subscription is stale")
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
	// ErrStoreClosed reports that the store has started closing.
	ErrStoreClosed = ErrHostClosed
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
	ThreadID identity.ThreadID
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

// ContractError identifies a corrupt public result contract. Contract names
// the root DTO or projection without exposing internal store records.
type ContractError struct {
	Contract string
	Err      error
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

func (e *ContractError) Error() string {
	if e == nil {
		return ErrAuthorityCorrupt.Error()
	}
	name := strings.TrimSpace(e.Contract)
	if name == "" {
		name = "public result"
	}
	if e.Err == nil {
		return fmt.Sprintf("%s: invalid %s", ErrAuthorityCorrupt, name)
	}
	return fmt.Sprintf("%s: invalid %s: %v", ErrAuthorityCorrupt, name, e.Err)
}

func (e *ContractError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *ContractError) Is(target error) bool {
	return target == ErrAuthorityCorrupt || e != nil && errors.Is(e.Err, target)
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
	cfg                       runtimeConfig
	store                     *runtimeStore
	sink                      runtimeEventSink
	harness                   *agentharness.AgentHarness
	supportsOpaqueAttachments bool
}

type runtimeConfig struct {
	Provider                string
	Model                   string
	PromptCacheRetention    string
	SystemPrompt            string
	ContextPolicy           config.ContextPolicy
	Reasoning               config.ReasoningSelection
	SkillsEnabled           bool
	SkillSources            []string
	SkillPromptBudgetBytes  int
	MaxOutputTokensSet      bool
	MaxEmptyProviderRetries int
	NoProgressLimit         int
	DuplicateToolLimit      int
	WallTime                time.Duration
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
	Config                   runtimeConfig
	modelGateway             modelGateway
	modelGatewayIdentity     modelGatewayIdentity
	modelGatewayCapabilities modelGatewayCapabilities
	store                    *runtimeStore
	Tools                    *tools.Registry
	EffectAuthorizationGate  EffectAuthorizationGate
	Sink                     EventSink
	ToolSurfaceProvider      ToolSurfaceProvider
	IDGenerator              func(string) string
	LoopLimits               LoopLimits
	SubAgentRunTimeout       time.Duration
	Capabilities             CapabilityOptions
	ThreadTitleMode          ThreadTitleMode
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
// host's modelGateway implementation.
type MessageAttachment struct {
	ResourceRef string                      `json:"resource_ref"`
	Name        string                      `json:"name"`
	MIMEType    string                      `json:"mime_type"`
	SizeBytes   int64                       `json:"size_bytes,omitempty"`
	TextStats   *MessageAttachmentTextStats `json:"text_stats,omitempty"`
}

type MessageAttachmentTextStats struct {
	UnicodeCodePointCount int64 `json:"unicode_code_points"`
	LogicalLineCount      int64 `json:"logical_lines"`
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
	MaxMessageAttachmentsPerTurn          = 32
	MaxMessageAttachmentResourceRefBytes  = 16 * 1024
	MaxMessageAttachmentNameRunes         = 1024
	MaxMessageAttachmentMIMETypeBytes     = 512
	MaxMessageAttachmentSizeBytes         = 64 * 1024 * 1024
	MaxMessageAttachmentsTotalSizeBytes   = 256 * 1024 * 1024
	MaxMessageAttachmentsPayloadBytes     = 512 * 1024
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
	EffectAttemptID    string            `json:"effect_attempt_id"`
	RequestFingerprint string            `json:"request_fingerprint"`
	ThreadID           identity.ThreadID `json:"thread_id"`
	TurnID             identity.TurnID   `json:"turn_id"`
	RunID              identity.RunID    `json:"run_id"`
	ToolCallID         string            `json:"tool_call_id"`
	ToolName           string            `json:"tool_name"`
	Arguments          string            `json:"arguments,omitempty"`
	ArgumentHash       string            `json:"argument_hash"`
	Step               int               `json:"step"`
	BatchIndex         int               `json:"batch_index"`
	BatchSize          int               `json:"batch_size"`
	Labels             map[string]string `json:"labels,omitempty"`
	HostContext        map[string]string `json:"host_context,omitempty"`
	// Activity is detached tool-authored display data. It is never authority.
	Activity    *tools.ActivityPresentation `json:"activity,omitempty"`
	Resources   []tools.ResourceRef         `json:"resources,omitempty"`
	Effects     []tools.Effect              `json:"effects,omitempty"`
	Permission  tools.PermissionSpec        `json:"permission"`
	ReadOnly    bool                        `json:"read_only"`
	Destructive bool                        `json:"destructive"`
	OpenWorld   bool                        `json:"open_world"`
}

type EffectAuthorizationProof struct {
	EffectAttemptID    string            `json:"effect_attempt_id"`
	RequestFingerprint string            `json:"request_fingerprint"`
	ThreadID           identity.ThreadID `json:"thread_id"`
	TurnID             identity.TurnID   `json:"turn_id"`
	RunID              identity.RunID    `json:"run_id"`
	ToolCallID         string            `json:"tool_call_id"`
	PolicyRevision     string            `json:"policy_revision"`
	ApprovalID         string            `json:"approval_id,omitempty"`
	AuditReference     string            `json:"audit_reference"`
	AuditHash          string            `json:"audit_hash"`
	AuthorizedAt       time.Time         `json:"authorized_at"`
}

type EffectDispatchResult struct {
	result agentharness.EffectDispatchResult
}

// AuthorizedEffect invokes one prepared effect under the host-selected
// execution context. Floret additionally bounds that context by the active turn.
type AuthorizedEffect func(context.Context, EffectAuthorizationProof) (EffectDispatchResult, error)

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
			ThreadID: identity.ThreadID(req.ThreadID), TurnID: identity.TurnID(req.TurnID), RunID: identity.RunID(req.RunID),
			ToolCallID: req.ToolCallID, ToolName: req.ToolName, Arguments: req.Arguments, ArgumentHash: req.ArgumentHash,
			Step: req.Step, BatchIndex: req.BatchIndex, BatchSize: req.BatchSize,
			Labels: cloneStringMap(req.Labels), HostContext: cloneStringMap(req.HostContext),
			Activity:  tools.CloneActivityPresentation(req.Activity),
			Resources: append([]tools.ResourceRef(nil), req.Resources...), Effects: append([]tools.Effect(nil), req.Effects...),
			Permission: req.Permission, ReadOnly: req.ReadOnly, Destructive: req.Destructive, OpenWorld: req.OpenWorld,
		}, func(dispatchCtx context.Context, proof EffectAuthorizationProof) (EffectDispatchResult, error) {
			internalResult, err := effect(dispatchCtx, agentharness.EffectAuthorizationProof{
				EffectAttemptID: proof.EffectAttemptID, RequestFingerprint: proof.RequestFingerprint,
				ThreadID: string(proof.ThreadID), TurnID: string(proof.TurnID), RunID: string(proof.RunID), ToolCallID: proof.ToolCallID,
				PolicyRevision: proof.PolicyRevision, ApprovalID: proof.ApprovalID,
				AuditReference: proof.AuditReference, AuditHash: proof.AuditHash, AuthorizedAt: proof.AuthorizedAt,
			})
			return EffectDispatchResult{result: internalResult}, runtimeHostError(err)
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
	return sessionMessageAttachment(a).Validate()
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
	if err := session.ValidateMessageAttachments(sessionMessageAttachments(i.Attachments)); err != nil {
		return fmt.Errorf("turn input: %w", err)
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

type runTurnRequest struct {
	LogicalRequestID            identity.LogicalRequestID
	RunID                       identity.RunID
	ThreadID                    identity.ThreadID
	TurnID                      identity.TurnID
	Input                       TurnInput
	SupplementalContext         []TurnSupplementalContextItem
	Labels                      RunLabels
	Completion                  TurnCompletionPolicy
	Signals                     TurnSignalSpec
	Limits                      TurnLimits
	Reasoning                   config.ReasoningSelection
	ManualCompactions           ManualCompactionSource
	ToolSurfaceProvider         ToolSurfaceProvider
	PromotedQueueID             string
	PromotionRequestKey         string
	PromotionRequestFingerprint string
	InputFingerprint            string
}

// Validate checks the provider-independent request contract before admission.
// Host execution repeats this validation and additionally checks bound
// authority and provider-specific capabilities.
func (r runTurnRequest) Validate() error {
	_, err := validateRunTurnRequest(r)
	return err
}

type RunLabels struct {
	Correlation map[string]string `json:"correlation,omitempty"`
	Host        map[string]string `json:"host,omitempty"`
}

type TurnResult struct {
	ThreadID           identity.ThreadID              `json:"thread_id"`
	TurnID             identity.TurnID                `json:"turn_id"`
	RunID              identity.RunID                 `json:"-"`
	Status             TurnStatus                     `json:"status"`
	Output             string                         `json:"output,omitempty"`
	Failure            *ThreadTurnFailure             `json:"failure,omitempty"`
	Diagnostics        map[string]string              `json:"diagnostics,omitempty"`
	Metrics            RunMetrics                     `json:"metrics"`
	CompletionReason   observation.CompletionReason   `json:"completion_reason,omitempty"`
	ContinuationReason observation.ContinuationReason `json:"continuation_reason,omitempty"`
	FinishReason       observation.FinishReason       `json:"finish_reason,omitempty"`
	RawFinishReason    string                         `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool                           `json:"finish_inferred,omitempty"`
	Signal             *TurnSignal                    `json:"signal,omitempty"`
	ActivityTimeline   observation.ActivityTimeline   `json:"activity_timeline"`
	Replayed           bool                           `json:"replayed,omitempty"`
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
	if r.ActivityTimeline.ThreadID.String() != string(r.ThreadID) || r.ActivityTimeline.TurnID.String() != string(r.TurnID) || r.ActivityTimeline.RunID.String() != string(r.RunID) {
		return errors.New("turn result activity timeline identity mismatch")
	}
	return nil
}

type EventSink interface {
	EmitEvent(Event)
}

type Event struct {
	Type               observation.EventType             `json:"type"`
	TraceID            identity.TraceID                  `json:"trace_id,omitempty"`
	RunID              identity.RunID                    `json:"run_id,omitempty"`
	ThreadID           identity.ThreadID                 `json:"thread_id,omitempty"`
	TurnID             identity.TurnID                   `json:"turn_id,omitempty"`
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
	Activity           *tools.ActivityPresentation       `json:"activity,omitempty"`
	ActivityTimeline   *observation.ActivityTimeline     `json:"activity_timeline,omitempty"`
	Stream             *StreamObservation                `json:"stream,omitempty"`
	ContextStatus      *observation.ContextStatus        `json:"context_status,omitempty"`
	Compaction         *observation.CompactionEvent      `json:"compaction,omitempty"`
	CompactionDebug    *observation.CompactionDebugEvent `json:"compaction_debug,omitempty"`
	Sources            []publicprovider.Source           `json:"sources,omitempty"`
	Metadata           map[string]any                    `json:"metadata,omitempty"`
	Timestamp          time.Time                         `json:"timestamp,omitempty"`
	committed          *agentharness.ThreadDetailEvent
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
		if !eventIdentityMatches(e, e.ContextStatus.RunID.String(), e.ContextStatus.ThreadID.String(), e.ContextStatus.TurnID.String(), e.ContextStatus.Step) {
			return errors.New("runtime event context status identity mismatch")
		}
	}
	if e.Compaction != nil {
		if err := e.Compaction.Validate(); err != nil {
			return fmt.Errorf("invalid compaction event: %w", err)
		}
		if !eventIdentityMatches(e, e.Compaction.RunID.String(), e.Compaction.ThreadID.String(), e.Compaction.TurnID.String(), e.Compaction.Step) {
			return errors.New("runtime event compaction identity mismatch")
		}
	}
	if e.CompactionDebug != nil {
		if err := e.CompactionDebug.Validate(); err != nil {
			return fmt.Errorf("invalid compaction debug event: %w", err)
		}
		if !eventIdentityMatches(e, e.CompactionDebug.RunID.String(), e.CompactionDebug.ThreadID.String(), e.CompactionDebug.TurnID.String(), e.CompactionDebug.Step) {
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
		if e.ActivityTimeline.RunID.String() != string(e.RunID) || e.ActivityTimeline.ThreadID.String() != string(e.ThreadID) || e.ActivityTimeline.TurnID.String() != string(e.TurnID) {
			return errors.New("runtime event activity timeline identity mismatch")
		}
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
	Type             StreamObservationType     `json:"type"`
	Text             string                    `json:"text,omitempty"`
	ToolCallStream   *ToolCallStream           `json:"tool_call_stream,omitempty"`
	Reason           string                    `json:"reason,omitempty"`
	FinishReason     observation.FinishReason  `json:"finish_reason,omitempty"`
	RawFinishReason  string                    `json:"raw_finish_reason,omitempty"`
	FinishInferred   bool                      `json:"finish_inferred,omitempty"`
	Attempt          int                       `json:"attempt,omitempty"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id,omitempty"`
	AttemptID        string                    `json:"attempt_id,omitempty"`
	AttemptEpoch     int                       `json:"attempt_epoch,omitempty"`
	Labels           RunLabels                 `json:"labels,omitempty"`
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
	ThreadTurnFailureControlError             ThreadTurnFailureCode = "control_error"
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
		ThreadTurnFailureControlError,
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

type runtimeStore struct {
	self              *runtimeStore
	repo              sessiontree.Repo
	prompt            cache.Store
	agentTodos        sessiontree.AgentTodoStateRepo
	rootAuthority     rootTreeDeleter
	deleteCleanup     func(context.Context, []string) error
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

type rootTreeDeleter interface {
	DeleteRootTree(context.Context, string) (sessiontree.DeleteRootTreeResult, error)
}

type storeLifetimeState string

const (
	storeLifetimeOpen    storeLifetimeState = "open"
	storeLifetimeClosing storeLifetimeState = "closing"
	storeLifetimeClosed  storeLifetimeState = "closed"
)

func newMemoryStore() *runtimeStore {
	repo := sessiontree.NewMemoryRepo()
	prompt := cache.NewMemoryStore()
	store := &runtimeStore{
		repo:          repo,
		prompt:        prompt,
		agentTodos:    repo,
		rootAuthority: repo,
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

func newBackendRuntimeStore(ctx context.Context, backend spi.Backend) (*runtimeStore, error) {
	kernel, err := storage.NewBackendKernel(ctx, backend, time.Now)
	if err != nil {
		return nil, err
	}
	store := &runtimeStore{
		repo: kernel, prompt: kernel,
		agentTodos: kernel, rootAuthority: kernel,
		deleteCleanup: func(context.Context, []string) error { return nil },
	}
	store.close = backend.Close
	store.self = store
	store.initLifetime()
	return store, nil
}

func newBackendRuntimeStoreWithKernel(backend spi.Backend, kernel *storage.BackendKernel) (*runtimeStore, error) {
	if backend == nil || kernel == nil {
		return nil, errors.New("runtime store requires backend and kernel")
	}
	store := &runtimeStore{
		repo: kernel, prompt: kernel,
		agentTodos: kernel, rootAuthority: kernel,
		deleteCleanup: func(context.Context, []string) error { return nil },
	}
	store.close = backend.Close
	store.self = store
	store.initLifetime()
	return store, nil
}

func (s *runtimeStore) Close() error {
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

	var checkpointErr error
	if checkpoint, ok := s.repo.(interface{ Checkpoint(context.Context) error }); ok {
		checkpointErr = checkpoint.Checkpoint(context.Background())
	}
	var closeErr error
	if s.close != nil {
		closeErr = s.close()
	}
	err := errors.Join(backgroundErr, checkpointErr, closeErr)

	s.lifetimeMu.Lock()
	s.closeInProgress = false
	if closeErr == nil {
		s.lifetimeState = storeLifetimeClosed
	}
	s.lifetimeCond.Broadcast()
	s.lifetimeMu.Unlock()
	return err
}

func (s *runtimeStore) reportBackgroundError(err error) {
	if s == nil || err == nil {
		return
	}
	s.lifetimeMu.Lock()
	s.backgroundErr = errors.Join(s.backgroundErr, err)
	s.lifetimeMu.Unlock()
}

func (s *runtimeStore) validate() error {
	if s == nil {
		return errors.New("runtime store is required")
	}
	if err := s.validateIdentity(); err != nil {
		return err
	}
	if err := s.validateOpen(); err != nil {
		return err
	}
	if s.repo == nil || s.prompt == nil || s.agentTodos == nil || s.rootAuthority == nil || s.deleteCleanup == nil {
		return errors.New("runtime store must be created with runtime.newMemoryStore or runtime.openSQLiteStore")
	}
	if _, ok := s.repo.(sessiontree.ProviderStateStore); !ok {
		return ErrUnsupportedStoreCapability
	}
	return nil
}

func (s *runtimeStore) initLifetime() {
	s.lifetimeMu.Lock()
	defer s.lifetimeMu.Unlock()
	s.initLifetimeLocked()
}

func (s *runtimeStore) initLifetimeLocked() {
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

func (s *runtimeStore) validateOpen() error {
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

func (s *runtimeStore) beginOperation() (func(), error) {
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

func (s *runtimeStore) beginOperationContext(ctx context.Context) (context.Context, func(), error) {
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

func (s *runtimeStore) recoverPendingAutomaticThreadTitles(harness *agentharness.AgentHarness) error {
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

func (s *runtimeStore) beginLifetimeOperationContext() (context.Context, func(), error) {
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

func (s *runtimeStore) validateIdentity() error {
	if s == nil {
		return errors.New("runtime store is required")
	}
	if s.self != nil && s.self != s {
		return errors.New("runtime store must not be copied")
	}
	return nil
}

func (s *runtimeStore) deleteThreadData(ctx context.Context, threadID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	result, err := s.rootAuthority.DeleteRootTree(ctx, threadID)
	if err != nil {
		return err
	}
	if err := s.deleteCleanup(ctx, result.ThreadIDs); err != nil {
		return &CommittedCleanupError{ThreadID: identity.ThreadID(threadID), Err: err}
	}
	return nil
}

func newProviderHost(opts providerHostOptions) (*providerHost, error) {
	if err := opts.modelGatewayCapabilities.validate(opts.modelGateway); err != nil {
		return nil, err
	}
	titleMode, err := normalizeThreadTitleMode(opts.ThreadTitleMode)
	if err != nil {
		return nil, err
	}
	cfg, provider, err := resolveHostConfigAndProvider(opts)
	if err != nil {
		return nil, err
	}
	store := opts.store
	if store == nil {
		return nil, errors.New("runtime store is required")
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	runtimeSink := newRuntimeEventSink(opts.Sink)
	harness, err := newHarnessWithProvider(cfg, provider, harnessOptions{
		store:                    store,
		Tools:                    opts.Tools,
		EffectAuthorizationGate:  opts.EffectAuthorizationGate,
		Sink:                     runtimeSink,
		SinkPolicy:               runtimeHarnessSinkPolicy(),
		ToolSurfaceProvider:      runtimeToolSurfaceProvider(opts.ToolSurfaceProvider),
		NewID:                    opts.IDGenerator,
		LoopLimits:               opts.LoopLimits,
		SubAgentRunTimeout:       opts.SubAgentRunTimeout,
		Capabilities:             opts.Capabilities,
		ThreadTitleMode:          titleMode,
		modelGatewayCapabilities: opts.modelGatewayCapabilities,
		StateCompatibilityKey:    runtimeStateCompatibilityKey(cfg, opts),
	})
	if err != nil {
		return nil, err
	}
	return &providerHost{
		cfg:                       cfg,
		store:                     store,
		sink:                      runtimeSink,
		harness:                   harness,
		supportsOpaqueAttachments: opts.modelGateway != nil,
	}, nil
}

func resolveHostConfigAndProvider(opts providerHostOptions) (runtimeConfig, provider.Provider, error) {
	if opts.modelGateway == nil {
		return runtimeConfig{}, nil, errors.New("provider gateway is required")
	}
	identity, err := normalizeModelGatewayIdentity(opts.modelGatewayIdentity)
	if err != nil {
		return runtimeConfig{}, nil, err
	}
	cfg, err := resolveModelGatewayHostConfig(opts.Config, identity)
	if err != nil {
		return runtimeConfig{}, nil, err
	}
	modelProvider, err := projectedModelProvider(cfg, opts.modelGateway, identity, opts.modelGatewayCapabilities)
	if err != nil {
		return runtimeConfig{}, nil, err
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
	case errors.Is(err, sessiontree.ErrActiveTurn):
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
	case errors.Is(err, sessiontree.ErrCanonicalTurnNotFound):
		return fmt.Errorf("%w: %w", ErrTurnNotFound, err)
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
	case errors.Is(err, sessiontree.ErrThreadClosed):
		return fmt.Errorf("%w: %w", ErrSubAgentClosed, err)
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

func beginHostOperationContext(store *runtimeStore, ctx context.Context) (context.Context, func(), error) {
	if store == nil {
		return nil, nil, errors.New("runtime store is required")
	}
	return store.beginOperationContext(ctx)
}

func invalidPublicResult(name string, err error) error {
	return &ContractError{Contract: strings.TrimSpace(name), Err: err}
}

func cloneMessageAttachments(attachments []MessageAttachment) []MessageAttachment {
	if attachments == nil {
		return nil
	}
	result := make([]MessageAttachment, len(attachments))
	for index, attachment := range attachments {
		if attachment.TextStats != nil {
			stats := *attachment.TextStats
			attachment.TextStats = &stats
		}
		result[index] = attachment
	}
	return result
}

func (h *providerHost) ResumeInput(ctx context.Context, threadID identity.ThreadID, req resumeInputRequest) (TurnResult, error) {
	if h == nil || h.harness == nil {
		return TurnResult{}, errors.New("provider host is required")
	}
	if strings.TrimSpace(string(threadID)) == "" || strings.TrimSpace(string(req.TurnID)) == "" || strings.TrimSpace(string(req.RunID)) == "" || strings.TrimSpace(req.Answer) == "" {
		return TurnResult{}, errors.New("waiting turn resume request is incomplete")
	}
	thread, err := h.harness.ResumeWaitingThread(ctx, string(threadID))
	if err != nil {
		return TurnResult{}, runtimeHostError(err)
	}
	completion, err := engineTurnCompletionPolicy(req.Options.Completion)
	if err != nil {
		return TurnResult{}, err
	}
	signals, err := engineTurnSignalSpec(req.Options.Signals, completion)
	if err != nil {
		return TurnResult{}, err
	}
	result, runErr := thread.ResumeInput(ctx, string(req.TurnID), string(req.WaitingRunID), req.Answer, agentharness.RunOptions{
		LogicalRequestID: string(req.Options.LogicalRequestID),
		RunID:            string(req.RunID), TurnID: string(req.TurnID),
		CompletionPolicy: completion,
		ControlSpec:      signals,
		Reasoning:        projectedReasoningSelection(req.Options.Reasoning, h.cfg.Reasoning),
		MaxInputTokens:   req.Options.Limits.MaxInputTokens, MaxTotalTokens: req.Options.Limits.MaxTotalTokens,
		MaxCostUSD: req.Options.Limits.MaxCostUSD, MaxToolCalls: req.Options.Limits.MaxToolCalls,
		MaxLengthContinuations: req.Options.Limits.MaxLengthContinuations, MaxStopHookContinuations: req.Options.Limits.MaxStopHookContinuations,
		ManualCompactions:   projectedManualCompactionSource(req.Options.ManualCompactions),
		ToolSurfaceProvider: runtimeToolSurfaceProvider(req.Options.ToolSurfaceProvider),
		SupplementalContext: agentHarnessSupplementalContext(req.Options.SupplementalContext),
	})
	return turnResult(result, string(threadID), nil, time.Now().UnixMilli()), runtimeHostError(runErr)
}

type acceptedTurn struct {
	ThreadID    identity.ThreadID
	TurnID      identity.TurnID
	RunID       identity.RunID
	UserEntryID string
	BaseLeafID  string
	Replayed    bool
}

func (h *providerHost) ExecuteAcceptedTurn(ctx context.Context, accepted acceptedTurn, req runTurnRequest) (TurnResult, error) {
	validated, err := validateRunTurnRequest(req)
	if err != nil {
		return TurnResult{}, err
	}
	input := validated.input
	supplementalContext := agentHarnessSupplementalContext(validated.supplementalContext)
	if len(input.Attachments) > 0 && !h.supportsOpaqueAttachments {
		return TurnResult{}, errors.New("opaque message attachments require a modelGateway host")
	}
	operationCtx, done, err := beginHostOperationContext(h.store, ctx)
	if err != nil {
		return TurnResult{}, err
	}
	defer done()
	thread, err := h.harness.ResumeThread(operationCtx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, runtimeHostError(err)
	}
	activityRecorder := &runtimeActivityEventRecorder{sink: h.sink}
	result, runErr := thread.ExecuteAccepted(operationCtx, agentharness.AcceptedTurn{
		ThreadID: string(accepted.ThreadID), TurnID: string(accepted.TurnID), RunID: string(accepted.RunID),
		UserEntryID: accepted.UserEntryID, BaseLeafID: accepted.BaseLeafID, Replayed: accepted.Replayed,
	}, input.Text, agentharness.RunOptions{
		LogicalRequestID: string(req.LogicalRequestID),
		RunID:            string(req.RunID), TurnID: string(req.TurnID),
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
		CompletionPolicy:         validated.completionPolicy,
		ControlSpec:              validated.signalSpec,
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
	if err := out.Validate(); err != nil {
		if (out.ThreadID == "" || out.TurnID == "" || out.RunID == "") && runErr != nil {
			return out, runtimeHostError(runErr)
		}
		return TurnResult{}, invalidPublicResult("turn result", err)
	}
	return out, runtimeHostError(runErr)
}

type validatedRunTurnRequest struct {
	input               TurnInput
	supplementalContext []TurnSupplementalContextItem
	completionPolicy    engine.CompletionPolicy
	signalSpec          engine.ControlSpec
}

func validateRunTurnRequest(req runTurnRequest) (validatedRunTurnRequest, error) {
	if strings.TrimSpace(string(req.RunID)) == "" {
		return validatedRunTurnRequest{}, errors.New("run id is required")
	}
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return validatedRunTurnRequest{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" {
		return validatedRunTurnRequest{}, errors.New("turn id is required")
	}
	input, err := normalizeTurnInput(req.Input)
	if err != nil {
		return validatedRunTurnRequest{}, err
	}
	supplementalContext, err := normalizeTurnSupplementalContext(req.SupplementalContext)
	if err != nil {
		return validatedRunTurnRequest{}, err
	}
	completionPolicy, err := engineTurnCompletionPolicy(req.Completion)
	if err != nil {
		return validatedRunTurnRequest{}, err
	}
	signalSpec, err := engineTurnSignalSpec(req.Signals, completionPolicy)
	if err != nil {
		return validatedRunTurnRequest{}, err
	}
	return validatedRunTurnRequest{
		input: input, supplementalContext: supplementalContext,
		completionPolicy: completionPolicy, signalSpec: signalSpec,
	}, nil
}

func normalizeTurnInput(input TurnInput) (TurnInput, error) {
	input.Attachments = cloneMessageAttachments(input.Attachments)
	input.References = append([]MessageReference(nil), input.References...)
	if err := input.Validate(); err != nil {
		return TurnInput{}, err
	}
	return input, nil
}

func normalizeTurnSupplementalContext(items []TurnSupplementalContextItem) ([]TurnSupplementalContextItem, error) {
	if len(items) == 0 {
		return nil, nil
	}
	engineItems := make([]engine.TurnSupplementalContextItem, 0, len(items))
	for _, item := range items {
		engineItems = append(engineItems, engine.TurnSupplementalContextItem{
			Kind: item.Kind, Title: item.Title, Text: item.Text, Metadata: cloneStringMap(item.Metadata),
			Sensitive: item.Sensitive, Truncated: item.Truncated,
		})
	}
	normalized, err := engine.NormalizeAndValidateTurnSupplementalContext(engineItems)
	if err != nil {
		return nil, err
	}
	out := make([]TurnSupplementalContextItem, 0, len(normalized))
	for _, item := range normalized {
		out = append(out, TurnSupplementalContextItem{
			Kind: item.Kind, Title: item.Title, Text: item.Text, Metadata: cloneStringMap(item.Metadata),
			Sensitive: item.Sensitive, Truncated: item.Truncated,
		})
	}
	return out, nil
}

func cloneTurnSupplementalContext(items []TurnSupplementalContextItem) []TurnSupplementalContextItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]TurnSupplementalContextItem, len(items))
	for index, item := range items {
		out[index] = item
		out[index].Metadata = cloneStringMap(item.Metadata)
	}
	return out
}

func sessionMessageAttachments(in []MessageAttachment) []session.MessageAttachment {
	if len(in) == 0 {
		return nil
	}
	out := make([]session.MessageAttachment, 0, len(in))
	for _, attachment := range in {
		out = append(out, sessionMessageAttachment(attachment))
	}
	return out
}

func sessionMessageAttachment(attachment MessageAttachment) session.MessageAttachment {
	var textStats *session.MessageAttachmentTextStats
	if attachment.TextStats != nil {
		textStats = &session.MessageAttachmentTextStats{
			UnicodeCodePointCount: attachment.TextStats.UnicodeCodePointCount,
			LogicalLineCount:      attachment.TextStats.LogicalLineCount,
		}
	}
	return session.MessageAttachment{
		ResourceRef: attachment.ResourceRef,
		Name:        attachment.Name,
		MIMEType:    attachment.MIMEType,
		SizeBytes:   attachment.SizeBytes,
		TextStats:   textStats,
	}
}

func runtimeMessageAttachments(in []session.MessageAttachment) []MessageAttachment {
	if len(in) == 0 {
		return nil
	}
	out := make([]MessageAttachment, 0, len(in))
	for _, attachment := range in {
		var textStats *MessageAttachmentTextStats
		if attachment.TextStats != nil {
			textStats = &MessageAttachmentTextStats{
				UnicodeCodePointCount: attachment.TextStats.UnicodeCodePointCount,
				LogicalLineCount:      attachment.TextStats.LogicalLineCount,
			}
		}
		out = append(out, MessageAttachment{
			ResourceRef: attachment.ResourceRef, Name: attachment.Name, MIMEType: attachment.MIMEType,
			SizeBytes: attachment.SizeBytes, TextStats: textStats,
		})
	}
	return out
}

func runtimeMessageReferences(in []session.MessageReference) []MessageReference {
	if len(in) == 0 {
		return nil
	}
	out := make([]MessageReference, 0, len(in))
	for _, reference := range in {
		out = append(out, MessageReference{
			ReferenceID: reference.ReferenceID, Kind: MessageReferenceKind(reference.Kind), Label: reference.Label,
			Text: reference.Text, ResourceRef: reference.ResourceRef, Truncated: reference.Truncated,
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

func turnResult(in agentharness.TurnResult, threadID string, events []observation.Event, nowUnixMS int64) TurnResult {
	status := TurnStatus(in.Status)
	if in.AdmissionRunning {
		status = TurnStatusRunning
	}
	out := TurnResult{
		ThreadID:           identity.ThreadID(threadID),
		TurnID:             identity.TurnID(in.ID),
		RunID:              identity.RunID(in.RunID),
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
			RunID:    identity.RunID(in.RunID),
			ThreadID: identity.ThreadID(threadID),
			TurnID:   identity.TurnID(in.ID),
			TraceID:  identity.TraceID(in.RunID),
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

type harnessOptions struct {
	store                    *runtimeStore
	Tools                    *tools.Registry
	EffectAuthorizationGate  EffectAuthorizationGate
	Sink                     event.Sink
	SinkPolicy               event.SinkPolicy
	Title                    agentharness.TitleGenerator
	ThreadTitleMode          ThreadTitleMode
	modelGatewayCapabilities modelGatewayCapabilities
	NewID                    func(string) string
	LoopLimits               LoopLimits
	SubAgentRunTimeout       time.Duration
	Capabilities             CapabilityOptions
	ToolSurfaceProvider      engine.ToolSurfaceProvider
	StateCompatibilityKey    string
}

func newHarnessWithProvider(cfg runtimeConfig, p provider.Provider, opts harnessOptions) (*agentharness.AgentHarness, error) {
	store := opts.store
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
	cacheRetention := strings.TrimSpace(cfg.PromptCacheRetention)
	if cacheRetention == "" {
		cacheRetention = "in_memory"
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
	if opts.modelGatewayCapabilities.Reasoning != nil {
		model.Reasoning = configbridge.ReasoningCapability(*opts.modelGatewayCapabilities.Reasoning)
	}
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
		NewID:                    opts.NewID,
	})
	if err := store.recoverPendingAutomaticThreadTitles(harness); err != nil {
		return nil, err
	}
	return harness, nil
}

func runtimeStateCompatibilityKey(cfg runtimeConfig, opts providerHostOptions) string {
	return strings.TrimSpace(opts.modelGatewayIdentity.StateCompatibilityKey)
}

func mergeCapabilityOptions(cfg runtimeConfig, explicit CapabilityOptions) CapabilityOptions {
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
	mu   *sync.Mutex
	sink EventSink
}

func newRuntimeEventSink(sink EventSink) runtimeEventSink {
	if sink == nil {
		return runtimeEventSink{}
	}
	return runtimeEventSink{
		mu:   &sync.Mutex{},
		sink: sink,
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
	s.sink.EmitEvent(out)
}

func runtimeEvent(ev event.Event) Event {
	contextStatus := runtimeContextStatus(ev)
	sanitized := event.Sanitize(ev)
	committed := runtimeCommittedEvent(ev)
	compactionEvent := runtimeCompactionEventWithError(ev, sanitized, sanitized.Err)
	compactionDebugEvent := runtimeCompactionDebugEventWithError(ev, sanitized, sanitized.Err)
	stream := runtimeStreamObservation(ev, sanitized.Metadata)
	ev = sanitized
	return Event{
		Type:               ev.Type,
		TraceID:            identity.TraceID(ev.TraceID),
		RunID:              identity.RunID(ev.RunID),
		ThreadID:           identity.ThreadID(ev.ThreadID),
		TurnID:             identity.TurnID(ev.TurnID),
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
		ContextStatus:      contextStatus,
		Compaction:         compactionEvent,
		CompactionDebug:    compactionDebugEvent,
		Sources:            runtimeSourceRefs(ev.Sources),
		Metadata:           safeMetadata(ev.Metadata),
		Timestamp:          ev.Timestamp,
		committed:          committed,
	}
}

func runtimeCommittedEvent(raw event.Event) *agentharness.ThreadDetailEvent {
	if raw.Type != event.ThreadEntryCommitted {
		return nil
	}
	detail, ok := raw.Payload.(agentharness.ThreadDetailEvent)
	if !ok {
		return nil
	}
	return &detail
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
			RunID:             identity.RunID(ev.RunID),
			ThreadID:          identity.ThreadID(ev.ThreadID),
			TurnID:            identity.TurnID(ev.TurnID),
			Step:              ev.Step,
			RequestID:         stringFromMetadata(meta, "request_id"),
			LogicalRequestID:  identity.LogicalRequestID(stringFromMetadata(meta, "logical_request_id")),
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
			RunID:            identity.RunID(ev.RunID),
			ThreadID:         identity.ThreadID(ev.ThreadID),
			TurnID:           identity.TurnID(ev.TurnID),
			Step:             ev.Step,
			RequestID:        status.RequestID,
			LogicalRequestID: identity.LogicalRequestID(status.LogicalRequestID),
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
		RunID:               identity.RunID(sanitized.RunID),
		ThreadID:            identity.ThreadID(sanitized.ThreadID),
		TurnID:              identity.TurnID(sanitized.TurnID),
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
		RunID:                            identity.RunID(sanitized.RunID),
		ThreadID:                         identity.ThreadID(sanitized.ThreadID),
		TurnID:                           identity.TurnID(sanitized.TurnID),
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
	var toolCallStream *ToolCallStream
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
		Type:             streamType,
		Text:             text,
		ToolCallStream:   toolCallStream,
		Reason:           reason,
		FinishReason:     observation.FinishReason(ev.FinishReason),
		RawFinishReason:  ev.RawFinishReason,
		FinishInferred:   ev.FinishInferred,
		Attempt:          streamAttemptFromMetadata(safeMetadata),
		LogicalRequestID: identity.LogicalRequestID(streamStringFromMetadata(safeMetadata, "logical_request_id")),
		AttemptID:        streamStringFromMetadata(safeMetadata, "attempt_id"),
		AttemptEpoch:     streamIntFromMetadata(safeMetadata, "attempt_epoch"),
		Labels:           streamLabelsFromMetadata(safeMetadata),
	}
	if out.Reason == "" && ev.Err != "" {
		out.Reason = ev.Err
	}
	return out
}

func runtimeModelToolCallStream(ev event.Event) *ToolCallStream {
	id := strings.TrimSpace(ev.ToolID)
	name := strings.TrimSpace(ev.ToolName)
	if id == "" && name == "" {
		return nil
	}
	return &ToolCallStream{
		ID:   id,
		Name: name,
	}
}

func runtimeSourceRefs(in []event.SourceRef) []publicprovider.Source {
	out := make([]publicprovider.Source, 0, len(in))
	for _, ref := range in {
		if strings.TrimSpace(ref.Title) == "" && strings.TrimSpace(ref.URL) == "" {
			continue
		}
		out = append(out, publicprovider.Source{
			Title: strings.TrimSpace(ref.Title),
			URL:   strings.TrimSpace(ref.URL),
		})
	}
	return out
}

func streamAttemptFromMetadata(metadata any) int {
	return streamIntFromMetadata(metadata, "attempt")
}

func streamIntFromMetadata(metadata any, key string) int {
	values, ok := metadata.(map[string]any)
	if !ok {
		return 0
	}
	switch v := values[key].(type) {
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

func streamStringFromMetadata(metadata any, key string) string {
	values, ok := metadata.(map[string]any)
	if !ok {
		return ""
	}
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
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
		TraceID:            identity.TraceID(sanitized.TraceID),
		RunID:              identity.RunID(sanitized.RunID),
		ThreadID:           identity.ThreadID(sanitized.ThreadID),
		TurnID:             identity.TurnID(sanitized.TurnID),
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

func cloneActivityPresentation(in *tools.ActivityPresentation) *tools.ActivityPresentation {
	return tools.CloneActivityPresentation(in)
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
