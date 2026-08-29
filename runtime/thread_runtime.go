package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/internal/agentharness"
	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/sessionlifecycle"
	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/observation"
	"github.com/floegence/floret/v5/tools"
)

// AgentRequest identifies one typed execution whose provider and tool surface
// must be resolved outside the thread lock. Run identities remain internal.
type AgentRequest struct {
	ThreadID        identity.ThreadID
	TurnID          identity.TurnID
	RequestKey      string
	Input           UserInput
	RetrySource     identity.TurnID
	InteractionID   string
	EffectAttemptID string
}

// AgentFactory resolves provider and tool capability outside the thread lock.
type AgentFactory interface {
	Agent(context.Context, AgentRequest) (*Agent, error)
}

// AgentFactoryFunc adapts a function to AgentFactory.
type AgentFactoryFunc func(context.Context, AgentRequest) (*Agent, error)

func (factory AgentFactoryFunc) Agent(ctx context.Context, request AgentRequest) (*Agent, error) {
	return factory(ctx, request)
}

// UserInput is one user-authored thread input.
type UserInput = TurnInput

// RequestKey is the stable idempotency identity of one transport mutation.
type RequestKey string

// ThreadScope is a host-neutral root or direct-child inventory filter.
type ThreadScope struct {
	ParentID *identity.ThreadID `json:"parent_id,omitempty"`
}

type CreateThreadInput struct {
	ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
	ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
	TaskName        string            `json:"task_name,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	HostProfileRef  string            `json:"host_profile_ref,omitempty"`
	ForkMode        string            `json:"fork_mode,omitempty"`
	RequestKey      RequestKey        `json:"request_key"`
}

type ForkThreadInput struct {
	SourceThreadID  identity.ThreadID `json:"source_thread_id"`
	SourceEntryID   string            `json:"source_entry_id,omitempty"`
	ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
	ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
	TaskName        string            `json:"task_name,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	HostProfileRef  string            `json:"host_profile_ref,omitempty"`
	ForkMode        string            `json:"fork_mode,omitempty"`
	RequestKey      RequestKey        `json:"request_key"`
}

type DeleteThreadInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

type SetTitleInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	Title      string            `json:"title"`
	RequestKey RequestKey        `json:"request_key"`
}

type SendInput struct {
	ThreadID            identity.ThreadID             `json:"thread_id"`
	Input               UserInput                     `json:"input"`
	SupplementalContext []TurnSupplementalContextItem `json:"supplemental_context,omitempty"`
	RequestKey          RequestKey                    `json:"request_key"`
}

type RespondInput struct {
	ThreadID      identity.ThreadID   `json:"thread_id"`
	InteractionID string              `json:"interaction_id"`
	Answers       []InteractionAnswer `json:"answers"`
	RequestKey    RequestKey          `json:"request_key"`
}

type CancelInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

type RetryInput struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	SourceTurnID identity.TurnID   `json:"source_turn_id"`
	RequestKey   RequestKey        `json:"request_key"`
}

type RetryEffectInput struct {
	ThreadID               identity.ThreadID `json:"thread_id"`
	EffectAttemptID        string            `json:"effect_attempt_id"`
	ToolCallID             string            `json:"tool_call_id"`
	AcknowledgeUnknownRisk bool              `json:"acknowledge_unknown_risk"`
	RequestKey             RequestKey        `json:"request_key"`
}

type ReorderQueueInput struct {
	ThreadID       identity.ThreadID `json:"thread_id"`
	OrderedItemIDs []string          `json:"ordered_item_ids"`
	RequestKey     RequestKey        `json:"request_key"`
}

type DeleteQueuedInput struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	QueueItemID string            `json:"queue_item_id"`
	RequestKey  RequestKey        `json:"request_key"`
}

type PromoteQueuedInput = DeleteQueuedInput

type ImportedPendingInput struct {
	RequestKey          RequestKey                    `json:"request_key"`
	Input               UserInput                     `json:"input"`
	SupplementalContext []TurnSupplementalContextItem `json:"supplemental_context,omitempty"`
}

type ImportPendingInputsInput struct {
	ThreadID identity.ThreadID      `json:"thread_id"`
	Items    []ImportedPendingInput `json:"items"`
}

type ImportResult struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	Imported int               `json:"imported"`
	View     ThreadView        `json:"view"`
}

type HistoryPage struct {
	Items   []ThreadItem `json:"items"`
	Before  string       `json:"before,omitempty"`
	HasMore bool         `json:"has_more,omitempty"`
}

type ThreadSummary struct {
	ID              identity.ThreadID  `json:"id"`
	ParentThreadID  identity.ThreadID  `json:"parent_thread_id,omitempty"`
	ParentTurnID    identity.TurnID    `json:"parent_turn_id,omitempty"`
	TaskName        string             `json:"task_name,omitempty"`
	TaskDescription string             `json:"task_description,omitempty"`
	HostProfileRef  string             `json:"host_profile_ref,omitempty"`
	ForkMode        string             `json:"fork_mode,omitempty"`
	Title           string             `json:"title,omitempty"`
	TitleStatus     ThreadTitleStatus  `json:"title_status,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
	Activity        ThreadActivity     `json:"activity"`
	Attention       AttentionSummary   `json:"attention"`
	LastOutcome     *TurnOutcome       `json:"last_outcome,omitempty"`
	TurnID          identity.TurnID    `json:"turn_id,omitempty"`
	RunID           identity.RunID     `json:"run_id,omitempty"`
	RunProgress     *ThreadRunProgress `json:"run_progress,omitempty"`
	QueueCount      int                `json:"queue_count,omitempty"`
	Failure         *ThreadTurnFailure `json:"failure,omitempty"`
	// Deprecated: use Failure for terminal failures. Error remains a text
	// mirror for v5 wire compatibility and transient runtime diagnostics.
	Error           string             `json:"error,omitempty"`
	PendingInput    *InputPresentation `json:"pending_input,omitempty"`
	LastItemAt      time.Time          `json:"last_item_at,omitempty"`
	LastItemPreview string             `json:"last_item_preview,omitempty"`
}

// ThreadTitleStatus is presentation metadata derived from the canonical
// thread journal. It does not own thread lifecycle state.
type ThreadTitleStatus string

const (
	ThreadTitleStatusUnset   ThreadTitleStatus = ""
	ThreadTitleStatusPending ThreadTitleStatus = "pending"
	ThreadTitleStatusReady   ThreadTitleStatus = "ready"
	ThreadTitleStatusFailed  ThreadTitleStatus = "failed"
)

// ThreadActivity reports whether a thread owns an accepted turn. RunProgress
// separately reports transient progress while its current run is advancing.
type ThreadActivity string

const (
	ThreadActivityIdle   ThreadActivity = "idle"
	ThreadActivityActive ThreadActivity = "active"
)

// ThreadRunPhase identifies one provider-neutral active-run phase.
type ThreadRunPhase string

const (
	ThreadRunPhasePreparing       ThreadRunPhase = "preparing"
	ThreadRunPhaseWaitingResponse ThreadRunPhase = "waiting_response"
	ThreadRunPhaseStreaming       ThreadRunPhase = "streaming"
	ThreadRunPhaseRetrying        ThreadRunPhase = "retrying"
	ThreadRunPhaseFinalizing      ThreadRunPhase = "finalizing"
	ThreadRunPhaseToolExecution   ThreadRunPhase = "tool_execution"
)

// ThreadRunProgress is the process-local presentation of an advancing run.
// It is absent while the thread is idle, terminal, or waiting for interaction.
type ThreadRunProgress struct {
	Phase ThreadRunPhase `json:"phase"`
}

// TurnOutcome is a terminal outcome rendered on one turn, never as thread failure.
type TurnOutcome string

const (
	TurnOutcomeCompleted   TurnOutcome = "completed"
	TurnOutcomeFailed      TurnOutcome = "failed"
	TurnOutcomeCancelled   TurnOutcome = "cancelled"
	TurnOutcomeInterrupted TurnOutcome = "interrupted"
)

type AttentionSummary struct {
	ApprovalCount int `json:"approval_count"`
	InputCount    int `json:"input_count"`
}

// ThreadItemKind identifies one stable current-view item.
type ThreadItemKind string

const (
	ThreadItemUser ThreadItemKind = "user"
	// ThreadItemThinking is one ordered reasoning segment in a thread view.
	ThreadItemThinking    ThreadItemKind = "thinking"
	ThreadItemAssistant   ThreadItemKind = "assistant"
	ThreadItemTool        ThreadItemKind = "tool"
	ThreadItemInteraction ThreadItemKind = "interaction"
)

// ThreadItem is a directly renderable current-view item.
type ThreadItem struct {
	ID     string          `json:"id"`
	TurnID identity.TurnID `json:"turn_id,omitempty"`
	// Ordinal is the stable one-based position in the thread presentation.
	Ordinal uint64         `json:"ordinal"`
	Kind    ThreadItemKind `json:"kind"`
	Text    string         `json:"text,omitempty"`
	// Live reports that this segment may still grow in place.
	Live        bool                      `json:"live,omitempty"`
	CreatedAt   time.Time                 `json:"created_at,omitempty"`
	Attachments []MessageAttachment       `json:"attachments,omitempty"`
	References  []MessageReference        `json:"references,omitempty"`
	Activity    *observation.ActivityItem `json:"activity,omitempty"`
	Interaction *ThreadInteraction        `json:"interaction,omitempty"`
}

type ThreadInteractionKind string

const (
	ThreadInteractionApproval    ThreadInteractionKind = "approval"
	ThreadInteractionInput       ThreadInteractionKind = "input"
	ThreadInteractionEffectRetry ThreadInteractionKind = "effect_retry"
)

// ThreadInteraction is an unresolved or resolved action embedded in one item.
type ThreadInteraction struct {
	ID          string                `json:"id"`
	TurnID      identity.TurnID       `json:"turn_id"`
	Kind        ThreadInteractionKind `json:"kind"`
	runID       identity.RunID
	ToolCallID  string                   `json:"tool_call_id,omitempty"`
	Resolved    bool                     `json:"resolved,omitempty"`
	Approved    *bool                    `json:"approved,omitempty"`
	Approval    *ApprovalPresentation    `json:"approval,omitempty"`
	Input       *InputPresentation       `json:"input,omitempty"`
	EffectRetry *EffectRetryPresentation `json:"effect_retry,omitempty"`
	Resolution  *InteractionResolution   `json:"resolution,omitempty"`
}

type EffectRetryPresentation struct {
	EffectAttemptID string `json:"effect_attempt_id"`
	ToolCallID      string `json:"tool_call_id"`
	ToolName        string `json:"tool_name"`
}

type ApprovalPresentation struct {
	Label       string   `json:"label"`
	Description string   `json:"description,omitempty"`
	Command     string   `json:"command,omitempty"`
	Effects     []string `json:"effects,omitempty"`
	Targets     []string `json:"targets,omitempty"`
	Risk        string   `json:"risk,omitempty"`
	ToolName    string   `json:"tool_name"`
	ToolCallID  string   `json:"tool_call_id"`
}

type InputPresentation struct {
	Summary   string          `json:"summary"`
	Questions []InputQuestion `json:"questions"`
}

type InputQuestion struct {
	ID         string   `json:"id"`
	Prompt     string   `json:"prompt"`
	Kind       string   `json:"kind"`
	Options    []string `json:"options,omitempty"`
	WriteLabel string   `json:"write_label,omitempty"`
	Secret     bool     `json:"secret,omitempty"`
}

type InteractionResolution struct {
	Accepted bool              `json:"accepted"`
	Redacted bool              `json:"redacted,omitempty"`
	Outcome  string            `json:"outcome,omitempty"`
	Approved *bool             `json:"approved,omitempty"`
	Input    map[string]string `json:"input,omitempty"`
	At       time.Time         `json:"at"`
}

// QueuedInput is one accepted input waiting behind the active turn.
type QueuedInput struct {
	ID                  string                        `json:"id"`
	RequestKey          string                        `json:"request_key"`
	Input               UserInput                     `json:"input"`
	SupplementalContext []TurnSupplementalContextItem `json:"supplemental_context,omitempty"`
	CreatedAt           time.Time                     `json:"created_at"`
}

// InteractionAnswer resolves one user or approval interaction.
type InteractionAnswer struct {
	InteractionID string            `json:"interaction_id,omitempty"`
	Approved      *bool             `json:"approved,omitempty"`
	Input         map[string]string `json:"input,omitempty"`
}

// ThreadView is the complete, replaceable presentation for one thread. Its
// version is process-local notification ordering, not a durable journal cursor.
type ThreadView struct {
	ThreadID    identity.ThreadID  `json:"thread_id"`
	ViewVersion uint64             `json:"view_version"`
	Activity    ThreadActivity     `json:"activity"`
	Attention   AttentionSummary   `json:"attention"`
	LastOutcome *TurnOutcome       `json:"last_outcome,omitempty"`
	Failure     *ThreadTurnFailure `json:"failure,omitempty"`
	// Deprecated: use Failure for terminal failures. Error remains a text
	// mirror for v5 wire compatibility and transient runtime diagnostics.
	Error        string              `json:"error,omitempty"`
	TurnID       identity.TurnID     `json:"turn_id,omitempty"`
	RunID        identity.RunID      `json:"run_id,omitempty"`
	RunProgress  *ThreadRunProgress  `json:"run_progress,omitempty"`
	Items        []ThreadItem        `json:"items,omitempty"`
	Queue        []QueuedInput       `json:"queue,omitempty"`
	Interactions []ThreadInteraction `json:"interactions,omitempty"`
	// Deprecated: derive active assistant content from Items. This field is
	// retained for v5 wire compatibility and has no independent lifecycle.
	AssistantDraft string `json:"assistant_draft,omitempty"`
	// Deprecated: derive active thinking content from Items. This field is
	// retained for v5 wire compatibility and has no independent lifecycle.
	ThinkingDraft string `json:"thinking_draft,omitempty"`
}

// ThreadContextSnapshot is the canonical context and compaction projection for
// one thread. Compactions contain one latest lifecycle record per operation.
type ThreadContextSnapshot struct {
	Model       ThreadContextModel         `json:"model,omitempty"`
	Policy      ThreadContextPolicy        `json:"policy,omitempty"`
	Usage       *observation.ContextStatus `json:"usage,omitempty"`
	UsageTotals *ThreadTokenUsageTotals    `json:"usage_totals,omitempty"`
	Compactions []ThreadContextCompaction  `json:"compactions,omitempty"`
	UpdatedAt   time.Time                  `json:"updated_at,omitempty"`
}

// ThreadTokenUsageTotals contains disjoint token totals from canonical final
// provider usage records across one thread.
type ThreadTokenUsageTotals struct {
	InputTokens      int64 `json:"input_tokens,omitempty"`
	OutputTokens     int64 `json:"output_tokens,omitempty"`
	CacheReadTokens  int64 `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int64 `json:"cache_write_tokens,omitempty"`
}

type ThreadContextModel struct {
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
}

type ThreadContextPolicy struct {
	ContextWindowTokens  int64 `json:"context_window_tokens,omitempty"`
	MaxOutputTokens      int64 `json:"max_output_tokens,omitempty"`
	ReservedOutputTokens int64 `json:"reserved_output_tokens,omitempty"`
}

type ThreadContextCompaction struct {
	RunID               identity.RunID    `json:"run_id,omitempty"`
	ThreadID            identity.ThreadID `json:"thread_id,omitempty"`
	TurnID              identity.TurnID   `json:"turn_id,omitempty"`
	Step                int               `json:"step,omitempty"`
	OperationID         string            `json:"operation_id,omitempty"`
	RequestID           string            `json:"request_id,omitempty"`
	Phase               string            `json:"phase,omitempty"`
	Status              string            `json:"status,omitempty"`
	Trigger             string            `json:"trigger,omitempty"`
	Reason              string            `json:"reason,omitempty"`
	Source              string            `json:"source,omitempty"`
	TokensBefore        int64             `json:"tokens_before,omitempty"`
	TokensAfterEstimate int64             `json:"tokens_after_estimate,omitempty"`
	Error               string            `json:"error,omitempty"`
	ObservedAt          time.Time         `json:"observed_at,omitempty"`
}

// ThreadContextReader exposes canonical context lifecycle facts without
// widening the lifecycle mutation surface of ThreadService.
type ThreadContextReader interface {
	Context(context.Context, identity.ThreadID) (ThreadContextSnapshot, error)
}

// ThreadService is the only provider-backed thread lifecycle boundary.
type ThreadService interface {
	Create(context.Context, CreateThreadInput) (ThreadView, error)
	Fork(context.Context, ForkThreadInput) (ThreadView, error)
	Delete(context.Context, DeleteThreadInput) error
	SetTitle(context.Context, SetTitleInput) (ThreadView, error)
	List(context.Context, ThreadScope) ([]ThreadSummary, error)
	View(context.Context, identity.ThreadID) (ThreadView, error)
	History(context.Context, identity.ThreadID, string, int) (HistoryPage, error)
	Send(context.Context, SendInput) (ThreadView, error)
	Respond(context.Context, RespondInput) (ThreadView, error)
	Cancel(context.Context, CancelInput) (ThreadView, error)
	Retry(context.Context, RetryInput) (ThreadView, error)
	RetryEffect(context.Context, RetryEffectInput) (ThreadView, error)
	ReorderQueue(context.Context, ReorderQueueInput) (ThreadView, error)
	DeleteQueued(context.Context, DeleteQueuedInput) (ThreadView, error)
	PromoteQueued(context.Context, PromoteQueuedInput) (ThreadView, error)
	ImportPendingInputs(context.Context, ImportPendingInputsInput) (ImportResult, error)
	Subscribe(context.Context) (*WorkspaceSubscription, error)
}

var _ ThreadContextReader = (*threadRuntimeService)(nil)

type threadRuntimeService struct {
	host           *Host
	factory        AgentFactory
	mu             sync.Mutex
	subscribers    map[*WorkspaceSubscription]struct{}
	published      map[string]uint64
	cancellationMu sync.Mutex
	cancellations  []threadCancellationDiagnostic
	runtimesMu     sync.Mutex
	runtimes       map[string]*threadRuntimeState
	closed         bool
}

type threadRuntimeRequest struct {
	fingerprint string
}

type preparedThreadExecution struct {
	agent  *Agent
	runner *turnRunnerHandle
	err    error
}

// WorkspaceSubscription receives replaceable current views.
type WorkspaceSubscription struct {
	mu      sync.Mutex
	views   chan ThreadView
	done    chan struct{}
	closed  bool
	service *threadRuntimeService
}

func (subscription *WorkspaceSubscription) Next(ctx context.Context) (ThreadView, error) {
	if subscription == nil {
		return ThreadView{}, errors.New("thread runtime subscription is required")
	}
	select {
	case <-ctx.Done():
		return ThreadView{}, ctx.Err()
	case <-subscription.done:
		return ThreadView{}, ErrHostClosed
	case view := <-subscription.views:
		return view, nil
	}
}

func (subscription *WorkspaceSubscription) Close() {
	if subscription == nil {
		return
	}
	subscription.mu.Lock()
	if subscription.closed {
		subscription.mu.Unlock()
		return
	}
	subscription.closed = true
	close(subscription.done)
	service := subscription.service
	subscription.mu.Unlock()
	if service != nil {
		service.mu.Lock()
		delete(service.subscribers, subscription)
		service.mu.Unlock()
	}
}

func (subscription *WorkspaceSubscription) closeLocked() {
	if subscription.closed {
		return
	}
	subscription.closed = true
	close(subscription.done)
}

// ThreadRuntime returns the Host-owned typed runtime service.
func (host *Host) ThreadService(factory AgentFactory) (ThreadService, error) {
	if factory == nil {
		return nil, errors.New("thread runtime requires an Agent factory")
	}
	if host == nil {
		return nil, errors.New("runtime Host is required")
	}
	host.closeMu.Lock()
	defer host.closeMu.Unlock()
	if host.closing || host.closed {
		return nil, ErrHostClosed
	}
	host.threadRuntimeMu.Lock()
	defer host.threadRuntimeMu.Unlock()
	if host.threadRuntime != nil {
		if host.threadRuntime.factory == nil {
			host.threadRuntime.factory = factory
		}
		return host.threadRuntime, nil
	}
	host.threadRuntime = &threadRuntimeService{
		host: host, factory: factory,
		subscribers: make(map[*WorkspaceSubscription]struct{}),
		published:   make(map[string]uint64),
		runtimes:    make(map[string]*threadRuntimeState),
	}
	return host.threadRuntime, nil
}

func cleanRequestKey(key RequestKey) (string, error) {
	value := strings.TrimSpace(string(key))
	if value == "" {
		return "", errors.New("request key is required")
	}
	return value, nil
}

func (host *Host) nextThreadID() (identity.ThreadID, error) {
	host.idMu.Lock()
	defer host.idMu.Unlock()
	value, err := host.idSource.NewThreadID()
	if err != nil {
		return "", err
	}
	return identity.ParseThreadID(value.String())
}

func (host *Host) nextTurnRunIDs() (identity.TurnID, identity.RunID, error) {
	host.idMu.Lock()
	defer host.idMu.Unlock()
	turnID, err := host.idSource.NewTurnID()
	if err != nil {
		return "", "", err
	}
	turnID, err = identity.ParseTurnID(turnID.String())
	if err != nil {
		return "", "", err
	}
	runID, err := host.idSource.NewRunID()
	if err != nil {
		return "", "", err
	}
	runID, err = identity.ParseRunID(runID.String())
	return turnID, runID, err
}

func (service *threadRuntimeService) Create(ctx context.Context, in CreateThreadInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(struct {
		ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
		ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
		TaskName        string            `json:"task_name,omitempty"`
		TaskDescription string            `json:"task_description,omitempty"`
		HostProfileRef  string            `json:"host_profile_ref,omitempty"`
		ForkMode        string            `json:"fork_mode,omitempty"`
	}{in.ParentThreadID, in.ParentTurnID, strings.TrimSpace(in.TaskName), strings.TrimSpace(in.TaskDescription), strings.TrimSpace(in.HostProfileRef), strings.TrimSpace(in.ForkMode)})
	if err != nil {
		return ThreadView{}, err
	}
	service.host.mutationMu.Lock()
	defer service.host.mutationMu.Unlock()
	origins, ok := service.host.store.repo.(sessiontree.ThreadOriginRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	origin, originErr := origins.ThreadOrigin(ctx, key)
	if originErr == nil {
		if origin.Tombstone != nil {
			if origin.Tombstone.OriginFingerprint != fingerprint || origin.Tombstone.ParentThreadID != in.ParentThreadID.String() {
				return ThreadView{}, ErrRequestConflict
			}
			return ThreadView{}, sessiontree.ErrThreadDeleted
		}
		if origin.Thread == nil || origin.Thread.OriginFingerprint != fingerprint || origin.Thread.ParentThreadID != in.ParentThreadID.String() {
			return ThreadView{}, ErrRequestConflict
		}
		return service.View(ctx, identity.ThreadID(origin.Thread.ID))
	}
	if !errors.Is(originErr, sessiontree.ErrThreadNotFound) {
		return ThreadView{}, runtimeHostError(originErr)
	}
	threadID, err := service.host.nextThreadID()
	if err != nil {
		return ThreadView{}, err
	}
	_, err = service.host.store.repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: threadID.String(), ParentThreadID: in.ParentThreadID.String(), ParentTurnID: in.ParentTurnID.String(),
		TaskName: strings.TrimSpace(in.TaskName), TaskDescription: strings.TrimSpace(in.TaskDescription),
		HostProfileRef: strings.TrimSpace(in.HostProfileRef), ForkMode: strings.TrimSpace(in.ForkMode),
		OriginRequestKey: key, OriginFingerprint: fingerprint,
	})
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	return service.View(ctx, threadID)
}

func (service *threadRuntimeService) Fork(ctx context.Context, in ForkThreadInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := identity.ParseThreadID(in.SourceThreadID.String()); err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(struct {
		Source          identity.ThreadID `json:"source"`
		Entry           string            `json:"entry,omitempty"`
		ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
		ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
		TaskName        string            `json:"task_name,omitempty"`
		TaskDescription string            `json:"task_description,omitempty"`
		HostProfileRef  string            `json:"host_profile_ref,omitempty"`
		ForkMode        string            `json:"fork_mode,omitempty"`
	}{in.SourceThreadID, strings.TrimSpace(in.SourceEntryID), in.ParentThreadID, in.ParentTurnID, strings.TrimSpace(in.TaskName), strings.TrimSpace(in.TaskDescription), strings.TrimSpace(in.HostProfileRef), strings.TrimSpace(in.ForkMode)})
	if err != nil {
		return ThreadView{}, err
	}
	service.host.mutationMu.Lock()
	defer service.host.mutationMu.Unlock()
	origins, ok := service.host.store.repo.(sessiontree.ThreadOriginRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	origin, originErr := origins.ThreadOrigin(ctx, key)
	if originErr == nil {
		if origin.Tombstone != nil {
			if origin.Tombstone.OriginFingerprint != fingerprint || origin.Tombstone.ForkedFromThreadID != in.SourceThreadID.String() {
				return ThreadView{}, ErrRequestConflict
			}
			return ThreadView{}, sessiontree.ErrThreadDeleted
		}
		if origin.Thread == nil || origin.Thread.OriginFingerprint != fingerprint || origin.Thread.ForkedFromThreadID != in.SourceThreadID.String() {
			return ThreadView{}, ErrRequestConflict
		}
		return service.View(ctx, identity.ThreadID(origin.Thread.ID))
	}
	if !errors.Is(originErr, sessiontree.ErrThreadNotFound) {
		return ThreadView{}, runtimeHostError(originErr)
	}
	source, err := service.host.store.repo.Thread(ctx, in.SourceThreadID.String())
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	entryID := strings.TrimSpace(in.SourceEntryID)
	if entryID == "" {
		entryID = source.LeafID
	}
	threadID, err := service.host.nextThreadID()
	if err != nil {
		return ThreadView{}, err
	}
	_, err = service.host.store.repo.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: in.SourceThreadID.String(), EntryID: entryID, EntryIDPinned: true,
		NewThreadID: threadID.String(), OriginRequestKey: key, OriginFingerprint: fingerprint,
		DestinationMeta: &sessiontree.ForkDestinationMeta{
			ParentThreadID: in.ParentThreadID.String(), ParentTurnID: in.ParentTurnID.String(),
			TaskName: strings.TrimSpace(in.TaskName), TaskDescription: strings.TrimSpace(in.TaskDescription),
			HostProfileRef: strings.TrimSpace(in.HostProfileRef), ForkMode: strings.TrimSpace(in.ForkMode),
		},
	})
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	return service.View(ctx, threadID)
}

func (service *threadRuntimeService) Delete(ctx context.Context, in DeleteThreadInput) error {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return err
	}
	fingerprint, err := stableFingerprint(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
	}{in.ThreadID})
	if err != nil {
		return err
	}
	repo, ok := service.host.store.repo.(sessiontree.ThreadDeleteRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	service.host.mutationMu.Lock()
	defer service.host.mutationMu.Unlock()
	if _, threadErr := service.host.store.repo.Thread(ctx, in.ThreadID.String()); threadErr != nil {
		_, err = repo.DeleteRootTreeWithRequest(ctx, in.ThreadID.String(), key, fingerprint)
		return runtimeHostError(err)
	}
	threadIDs, err := service.subtreeThreadIDs(ctx, in.ThreadID)
	if err != nil {
		return err
	}
	drains, err := service.fenceThreadRuntimes(threadIDs)
	if err != nil {
		return err
	}
	if err := waitThreadRuntimeDrains(ctx, drains); err != nil {
		service.releaseDeleteFences(drains)
		return err
	}
	result, err := repo.DeleteRootTreeWithRequest(ctx, in.ThreadID.String(), key, fingerprint)
	if err != nil {
		service.releaseDeleteFences(drains)
		return runtimeHostError(err)
	}
	deleted := make(map[string]struct{}, len(result.ThreadIDs))
	for _, threadID := range result.ThreadIDs {
		deleted[threadID] = struct{}{}
	}
	for _, drain := range drains {
		drain.actor.mu.Lock()
		_, committed := deleted[drain.actor.threadID]
		drain.actor.deleted = committed
		drain.actor.deleting = false
		drain.actor.mu.Unlock()
	}
	return nil
}

type threadRuntimeDrain struct {
	actor         *threadRuntimeState
	executionDone <-chan struct{}
	effectsDone   <-chan struct{}
}

func (service *threadRuntimeService) subtreeThreadIDs(ctx context.Context, rootThreadID identity.ThreadID) ([]identity.ThreadID, error) {
	metas, err := sessiontree.ListThreads(ctx, service.host.store.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return nil, runtimeHostError(err)
	}
	children := make(map[string][]string)
	found := false
	for _, meta := range metas {
		if meta.ID == rootThreadID.String() {
			found = true
		}
		children[meta.ParentThreadID] = append(children[meta.ParentThreadID], meta.ID)
	}
	if !found {
		return nil, ErrThreadNotFound
	}
	ids := []string{rootThreadID.String()}
	for index := 0; index < len(ids); index++ {
		ids = append(ids, children[ids[index]]...)
	}
	sort.Strings(ids)
	out := make([]identity.ThreadID, len(ids))
	for index, threadID := range ids {
		out[index] = identity.ThreadID(threadID)
	}
	return out, nil
}

func (service *threadRuntimeService) fenceThreadRuntimes(threadIDs []identity.ThreadID) ([]threadRuntimeDrain, error) {
	drains := make([]threadRuntimeDrain, len(threadIDs))
	for index, threadID := range threadIDs {
		drains[index].actor = service.runtime(threadID)
		drains[index].actor.mu.Lock()
	}
	for _, drain := range drains {
		if drain.actor.closed {
			for _, locked := range drains {
				locked.actor.mu.Unlock()
			}
			return nil, ErrHostClosed
		}
	}
	for index := range drains {
		actor := drains[index].actor
		actor.deleting = true
		drains[index].executionDone = actor.state.executionDone
		drains[index].effectsDone = actor.state.effectsDone
	}
	for _, drain := range drains {
		if drain.actor.state.cancel != nil {
			service.recordCancellation(identity.ThreadID(drain.actor.threadID), drain.actor.state.turnID, drain.actor.state.runID, "thread_delete", "thread deleted")
			drain.actor.state.cancel()
		}
		for _, cancelRetry := range drain.actor.state.effectRetryCancels {
			cancelRetry()
		}
	}
	for _, drain := range drains {
		drain.actor.mu.Unlock()
	}
	return drains, nil
}

func waitThreadRuntimeDrains(ctx context.Context, drains []threadRuntimeDrain) error {
	for _, drain := range drains {
		for _, done := range []<-chan struct{}{drain.executionDone, drain.effectsDone} {
			if done == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
			}
		}
	}
	return nil
}

func (service *threadRuntimeService) releaseDeleteFences(drains []threadRuntimeDrain) {
	for _, drain := range drains {
		drain.actor.mu.Lock()
		if !drain.actor.deleted {
			drain.actor.deleting = false
		}
		drain.actor.mu.Unlock()
	}
}

func (service *threadRuntimeService) SetTitle(ctx context.Context, in SetTitleInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	title := strings.TrimSpace(in.Title)
	if title == "" {
		return ThreadView{}, errors.New("thread title is required")
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	titles, ok := service.host.store.repo.(sessiontree.ThreadTitleAuthorityRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	fingerprint, err := stableFingerprint(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		Title    string            `json:"title"`
	}{in.ThreadID, title})
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := titles.SetThreadTitle(ctx, sessiontree.SetThreadTitleRequest{
		ThreadID: in.ThreadID.String(), Title: title, RequestKey: key, RequestFingerprint: fingerprint, Now: time.Now().UTC(),
	}); err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	return service.View(ctx, in.ThreadID)
}

func (service *threadRuntimeService) List(ctx context.Context, scope ThreadScope) ([]ThreadSummary, error) {
	if scope.ParentID == nil {
		inventory, ok := service.host.store.repo.(interface {
			ListRootThreadInventory(context.Context, sessiontree.ListThreadsOptions) ([]sessiontree.RootThreadInventoryItem, error)
		})
		if !ok {
			return nil, ErrUnsupportedStoreCapability
		}
		items, err := inventory.ListRootThreadInventory(ctx, sessiontree.ListThreadsOptions{IncludeArchived: false})
		if err != nil {
			return nil, runtimeHostError(err)
		}
		out := make([]ThreadSummary, 0, len(items))
		for _, item := range items {
			summary, err := threadSummaryFromCanonicalPath(item.Meta, item.Path)
			if err != nil {
				return nil, err
			}
			service.applyActiveThreadSummary(&summary)
			out = append(out, summary)
		}
		return out, nil
	}
	metas, err := sessiontree.ListThreads(ctx, service.host.store.repo, sessiontree.ListThreadsOptions{IncludeArchived: false})
	if err != nil {
		return nil, runtimeHostError(err)
	}
	out := make([]ThreadSummary, 0, len(metas))
	for _, meta := range metas {
		if meta.ParentThreadID != scope.ParentID.String() {
			continue
		}
		path, err := service.host.store.repo.Path(ctx, meta.ID, meta.LeafID)
		if err != nil {
			return nil, runtimeHostError(err)
		}
		summary, err := threadSummaryFromCanonicalPath(meta, path)
		if err != nil {
			return nil, err
		}
		service.applyActiveThreadSummary(&summary)
		out = append(out, summary)
	}
	return out, nil
}

func threadSummaryFromCanonicalPath(meta sessiontree.ThreadMeta, path []sessiontree.Entry) (ThreadSummary, error) {
	turnID, runID, activity, outcome, failure := threadRuntimeLifecycleFromEntries(meta, path)
	queueCount, err := canonicalQueueCountFromEntries(path)
	if err != nil {
		return ThreadSummary{}, err
	}
	summary := ThreadSummary{
		ID: identity.ThreadID(meta.ID), ParentThreadID: identity.ThreadID(meta.ParentThreadID), ParentTurnID: identity.TurnID(meta.ParentTurnID),
		TaskName: meta.TaskName, TaskDescription: meta.TaskDescription, HostProfileRef: meta.HostProfileRef, ForkMode: meta.ForkMode,
		Title: meta.Title, TitleStatus: ThreadTitleStatus(meta.TitleStatus), CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
		Activity: activity, LastOutcome: outcome, TurnID: turnID, RunID: runID, QueueCount: queueCount,
		Failure: cloneThreadTurnFailure(failure), Error: threadTurnFailureMessage(failure),
	}
	interactions := make(map[string]ThreadInteraction)
	interactionOrder := make([]string, 0)
	for _, entry := range path {
		switch entry.Type {
		case sessiontree.EntryToolCall, sessiontree.EntryToolResult:
			if entry.Message.Kind != "control_signal" || entry.Message.ControlSignal == nil || entry.Message.ControlSignal.Disposition != string(SignalWaiting) {
				continue
			}
			signal := entry.Message.ControlSignal
			if _, found := interactions[signal.CallID]; !found {
				interactionOrder = append(interactionOrder, signal.CallID)
			}
			interactions[signal.CallID] = ThreadInteraction{
				ID: signal.CallID, TurnID: identity.TurnID(entry.TurnID), Kind: ThreadInteractionInput,
				Input: inputPresentationFromControlSignal(signal.OutputText, signal.Payload),
			}
		case sessiontree.EntryInteractionAsked:
			var interaction ThreadInteraction
			if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
				return ThreadSummary{}, fmt.Errorf("decode interaction %q: %w", entry.ID, err)
			}
			if strings.TrimSpace(interaction.ID) == "" {
				return ThreadSummary{}, ErrAuthorityCorrupt
			}
			if _, found := interactions[interaction.ID]; !found {
				interactionOrder = append(interactionOrder, interaction.ID)
			}
			interactions[interaction.ID] = interaction
		case sessiontree.EntryInteractionDone:
			interactionID := strings.TrimPrefix(entry.ID, "interaction-resolved:")
			interaction, found := interactions[interactionID]
			if found {
				interaction.Resolved = true
				interactions[interactionID] = interaction
			}
		}
		if entry.Type == sessiontree.EntryUserMessage || entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Kind != "control_signal" {
			if text := strings.TrimSpace(entry.Message.Content); text != "" {
				summary.LastItemAt = entry.CreatedAt
				summary.LastItemPreview = truncateThreadSummaryPreview(text, 160)
			}
		}
	}
	for _, interactionID := range interactionOrder {
		interaction := interactions[interactionID]
		if interaction.Resolved {
			continue
		}
		switch interaction.Kind {
		case ThreadInteractionApproval:
			summary.Attention.ApprovalCount++
		case ThreadInteractionInput:
			summary.Attention.InputCount++
			if interaction.Input != nil {
				input := *interaction.Input
				input.Questions = append([]InputQuestion(nil), interaction.Input.Questions...)
				summary.PendingInput = &input
			}
		}
	}
	if summary.Activity == ThreadActivityActive && summary.Attention.ApprovalCount == 0 && summary.Attention.InputCount == 0 {
		summary.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
	}
	return summary, nil
}

func canonicalQueueCountFromEntries(entries []sessiontree.Entry) (int, error) {
	queue := make([]string, 0)
	for _, entry := range entries {
		switch entry.Type {
		case sessiontree.EntryQueueAdded:
			var item struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(entry.Payload, &item); err != nil {
				return 0, fmt.Errorf("decode canonical queue item %q: %w", entry.ID, err)
			}
			if item.ID == "" {
				item.ID = entry.ID
			}
			if !slices.Contains(queue, item.ID) {
				queue = append(queue, item.ID)
			}
		case sessiontree.EntryQueueReordered:
			var ids []string
			if err := json.Unmarshal(entry.Payload, &ids); err != nil {
				return 0, fmt.Errorf("decode canonical queue order %q: %w", entry.ID, err)
			}
			if len(ids) != len(queue) {
				return 0, ErrAuthorityCorrupt
			}
			for _, id := range ids {
				if !slices.Contains(queue, id) {
					return 0, ErrAuthorityCorrupt
				}
			}
			queue = append(queue[:0], ids...)
		case sessiontree.EntryQueueDeleted, sessiontree.EntryQueuePromoted:
			var id string
			if err := json.Unmarshal(entry.Payload, &id); err != nil {
				return 0, fmt.Errorf("decode canonical queue removal %q: %w", entry.ID, err)
			}
			for index := range queue {
				if queue[index] == id {
					queue = append(queue[:index], queue[index+1:]...)
					break
				}
			}
		}
	}
	return len(queue), nil
}

func threadSummaryFromView(meta sessiontree.ThreadMeta, view ThreadView) ThreadSummary {
	failure := cloneThreadTurnFailure(view.Failure)
	errorText := strings.TrimSpace(view.Error)
	if failure != nil {
		errorText = threadTurnFailureMessage(failure)
	}
	summary := ThreadSummary{
		ID: identity.ThreadID(meta.ID), ParentThreadID: identity.ThreadID(meta.ParentThreadID), ParentTurnID: identity.TurnID(meta.ParentTurnID),
		TaskName: meta.TaskName, TaskDescription: meta.TaskDescription, HostProfileRef: meta.HostProfileRef, ForkMode: meta.ForkMode,
		Title: meta.Title, TitleStatus: ThreadTitleStatus(meta.TitleStatus), CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
		Activity: view.Activity, Attention: view.Attention, LastOutcome: view.LastOutcome, TurnID: view.TurnID,
		RunID: view.RunID, RunProgress: cloneThreadRunProgress(view.RunProgress),
		QueueCount: len(view.Queue), Failure: failure, Error: errorText,
	}
	for index := len(view.Interactions) - 1; index >= 0; index-- {
		interaction := view.Interactions[index]
		if !interaction.Resolved && interaction.Kind == ThreadInteractionInput && interaction.Input != nil {
			input := *interaction.Input
			input.Questions = append([]InputQuestion(nil), interaction.Input.Questions...)
			summary.PendingInput = &input
			break
		}
	}
	for index := len(view.Items) - 1; index >= 0; index-- {
		item := view.Items[index]
		if summary.LastItemAt.IsZero() && !item.CreatedAt.IsZero() {
			summary.LastItemAt = item.CreatedAt
		}
		if text := strings.TrimSpace(item.Text); text != "" {
			summary.LastItemPreview = truncateThreadSummaryPreview(text, 160)
			if !item.CreatedAt.IsZero() {
				summary.LastItemAt = item.CreatedAt
			}
			break
		}
	}
	return summary
}

func (service *threadRuntimeService) applyActiveThreadSummary(summary *ThreadSummary) {
	if service == nil || summary == nil {
		return
	}
	service.runtimesMu.Lock()
	actor := service.runtimes[summary.ID.String()]
	service.runtimesMu.Unlock()
	if actor == nil {
		return
	}
	view := service.currentView(actor)
	if view.ViewVersion == 0 {
		return
	}
	meta := sessiontree.ThreadMeta{
		ID: summary.ID.String(), ParentThreadID: summary.ParentThreadID.String(), ParentTurnID: summary.ParentTurnID.String(),
		TaskName: summary.TaskName, TaskDescription: summary.TaskDescription, HostProfileRef: summary.HostProfileRef, ForkMode: summary.ForkMode,
		Title: summary.Title, TitleStatus: sessiontree.ThreadTitleStatus(summary.TitleStatus), CreatedAt: summary.CreatedAt, UpdatedAt: summary.UpdatedAt,
	}
	*summary = threadSummaryFromView(meta, view)
}

func truncateThreadSummaryPreview(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func (service *threadRuntimeService) History(ctx context.Context, threadID identity.ThreadID, before string, limit int) (HistoryPage, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return HistoryPage{}, runtimeHostError(err)
	}
	entries, err := service.host.store.repo.Path(ctx, threadID.String(), meta.LeafID)
	if err != nil {
		return HistoryPage{}, runtimeHostError(err)
	}
	items, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil {
		return HistoryPage{}, err
	}
	end := len(items)
	cursor := strings.TrimSpace(before)
	if cursor != "" {
		end = threadItemIndexByID(items, cursor)
		if end < 0 {
			return HistoryPage{}, runtimeHostError(sessiontree.ErrEntryNotFound)
		}
	}
	start := end - limit
	if start < 0 {
		start = 0
	}
	result := HistoryPage{Items: cloneThreadItems(items[start:end]), HasMore: start > 0}
	if result.HasMore {
		result.Before = items[start].ID
	}
	return result, nil
}

func (service *threadRuntimeService) Context(ctx context.Context, threadID identity.ThreadID) (ThreadContextSnapshot, error) {
	if service == nil || service.host == nil || service.host.store == nil {
		return ThreadContextSnapshot{}, ErrHostClosed
	}
	contextSnapshot, err := agentharness.New(agentharness.Options{Repo: service.host.store.repo}).ReadThreadContext(ctx, threadID.String())
	if err != nil {
		return ThreadContextSnapshot{}, runtimeHostError(err)
	}
	compactions := make([]ThreadContextCompaction, 0, len(contextSnapshot.Compactions))
	for _, compaction := range contextSnapshot.Compactions {
		compactions = append(compactions, ThreadContextCompaction{
			RunID: identity.RunID(compaction.RunID), ThreadID: identity.ThreadID(compaction.ThreadID), TurnID: identity.TurnID(compaction.TurnID),
			Step: compaction.Step, OperationID: compaction.OperationID, RequestID: compaction.RequestID,
			Phase: compaction.Phase, Status: compaction.Status, Trigger: compaction.Trigger,
			Reason: compaction.Reason, Source: compaction.Source, TokensBefore: compaction.TokensBefore,
			TokensAfterEstimate: compaction.TokensAfterEstimate, Error: compaction.Error, ObservedAt: compaction.ObservedAt,
		})
	}
	return ThreadContextSnapshot{
		Model: ThreadContextModel{Provider: contextSnapshot.Model.Provider, Model: contextSnapshot.Model.Model},
		Policy: ThreadContextPolicy{
			ContextWindowTokens:  contextSnapshot.Policy.ContextWindowTokens,
			MaxOutputTokens:      contextSnapshot.Policy.MaxOutputTokens,
			ReservedOutputTokens: contextSnapshot.Policy.ReservedOutputTokens,
		},
		Usage: contextSnapshot.Usage, UsageTotals: threadTokenUsageTotals(contextSnapshot.UsageTotals),
		Compactions: compactions, UpdatedAt: contextSnapshot.UpdatedAt,
	}, nil
}

func threadTokenUsageTotals(in *agentharness.ThreadTokenUsageTotals) *ThreadTokenUsageTotals {
	if in == nil {
		return nil
	}
	return &ThreadTokenUsageTotals{
		InputTokens: in.InputTokens, OutputTokens: in.OutputTokens,
		CacheReadTokens: in.CacheReadTokens, CacheWriteTokens: in.CacheWriteTokens,
	}
}

func (service *threadRuntimeService) Send(ctx context.Context, in SendInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	supplemental, err := normalizeTurnSupplementalContext(in.SupplementalContext)
	if err != nil {
		return ThreadView{}, err
	}
	return service.send(ctx, in.ThreadID, in.Input, supplemental, key)
}

func (service *threadRuntimeService) Cancel(ctx context.Context, in CancelInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	return service.cancel(ctx, in.ThreadID, key)
}

func (service *threadRuntimeService) Retry(ctx context.Context, in RetryInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	return service.retry(ctx, in.ThreadID, in.SourceTurnID, key)
}

func (service *threadRuntimeService) Respond(ctx context.Context, in RespondInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if len(in.Answers) == 0 {
		return ThreadView{}, errors.New("interaction answers are required")
	}
	answers := in.Answers
	if in.InteractionID != "" && len(answers) == 1 && answers[0].InteractionID == "" {
		answers = append([]InteractionAnswer(nil), answers...)
		answers[0].InteractionID = in.InteractionID
	}
	return service.respond(ctx, in.ThreadID, answers, key)
}

func (service *threadRuntimeService) ReorderQueue(ctx context.Context, in ReorderQueueInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(in.ThreadID)
	var reordered []QueuedInput
	fingerprint, _ := stableFingerprint(in.OrderedItemIDs)
	if err := actor.apply(ctx, func() error {
		current := append([]QueuedInput(nil), actor.state.view.Queue...)
		if len(current) != len(in.OrderedItemIDs) {
			return ErrRequestConflict
		}
		byID := make(map[string]QueuedInput, len(current))
		for _, item := range current {
			byID[item.ID] = item
		}
		reordered = make([]QueuedInput, 0, len(current))
		for _, id := range in.OrderedItemIDs {
			item, ok := byID[strings.TrimSpace(id)]
			if !ok {
				return ErrRequestConflict
			}
			delete(byID, item.ID)
			reordered = append(reordered, item)
		}
		if len(byID) != 0 {
			return ErrRequestConflict
		}
		if err := service.appendQueueFact(ctx, in.ThreadID, sessiontree.EntryQueueReordered, "queue-reorder:"+key, key, fingerprint, in.OrderedItemIDs); err != nil {
			return err
		}
		actor.state.view.Queue = reordered
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	view := service.currentView(actor)
	service.publish(view)
	return view, nil
}

func (service *threadRuntimeService) DeleteQueued(ctx context.Context, in DeleteQueuedInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	queueID := strings.TrimSpace(in.QueueItemID)
	actor := service.runtime(in.ThreadID)
	fingerprint, _ := stableFingerprint(queueID)
	if err := actor.apply(ctx, func() error {
		for index, item := range actor.state.view.Queue {
			if item.ID == queueID {
				if err := service.appendQueueFact(ctx, in.ThreadID, sessiontree.EntryQueueDeleted, "queue-delete:"+key, key, fingerprint, queueID); err != nil {
					return err
				}
				actor.state.view.Queue = append(actor.state.view.Queue[:index], actor.state.view.Queue[index+1:]...)
				actor.state.view.ViewVersion++
				return nil
			}
		}
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	view := service.currentView(actor)
	service.publish(view)
	return view, nil
}

func (service *threadRuntimeService) PromoteQueued(ctx context.Context, in PromoteQueuedInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	queueID := strings.TrimSpace(in.QueueItemID)
	fingerprint, err := stableFingerprint(struct {
		QueueItemID string `json:"queue_item_id"`
	}{QueueItemID: queueID})
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(in.ThreadID)
	var target QueuedInput
	var replayed bool
	err = actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[key]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "promote queue", RequestID: key, Err: ErrRequestConflict}
			}
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		for _, item := range actor.state.view.Queue {
			if item.ID == queueID {
				target = item
				return nil
			}
		}
		return ErrRequestConflict
	})
	if err != nil {
		return ThreadView{}, err
	}
	if replayed {
		return service.currentView(actor), nil
	}
	result, err := service.startAccepted(ctx, actor, in.ThreadID, target.Input, target.SupplementalContext, target.RequestKey, target.ID, key)
	if err != nil {
		return ThreadView{}, err
	}
	_ = actor.apply(context.Background(), func() error {
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
		return nil
	})
	return result, nil
}

func (service *threadRuntimeService) ImportPendingInputs(ctx context.Context, in ImportPendingInputsInput) (ImportResult, error) {
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ImportResult{}, err
	}
	actor := service.runtime(in.ThreadID)
	imported := 0
	for _, item := range in.Items {
		key, err := cleanRequestKey(item.RequestKey)
		if err != nil {
			return ImportResult{}, err
		}
		if err := item.Input.Validate(); err != nil {
			return ImportResult{}, err
		}
		supplemental, err := normalizeTurnSupplementalContext(item.SupplementalContext)
		if err != nil {
			return ImportResult{}, err
		}
		queued := QueuedInput{ID: "queue:" + key, RequestKey: key, Input: item.Input, SupplementalContext: supplemental, CreatedAt: time.Now().UTC()}
		fingerprint, _ := stableFingerprint(item.Input)
		var replayed bool
		err = actor.apply(ctx, func() error {
			if existing, ok := actor.state.requestKeys[key]; ok {
				if existing.fingerprint != fingerprint {
					return &RequestConflictError{Operation: "import pending input", RequestID: key, Err: ErrRequestConflict}
				}
				replayed = true
				return nil
			}
			if err := service.appendQueueFact(ctx, in.ThreadID, sessiontree.EntryQueueAdded, queued.ID, key, fingerprint, queued); err != nil {
				return err
			}
			if actor.state.requestKeys == nil {
				actor.state.requestKeys = make(map[string]threadRuntimeRequest)
			}
			actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
			return nil
		})
		if err != nil {
			return ImportResult{}, err
		}
		if replayed {
			continue
		}
		imported++
	}
	queue, err := hydrateCanonicalQueue(ctx, service.host.store.repo, in.ThreadID)
	if err != nil {
		return ImportResult{}, err
	}
	_ = actor.apply(ctx, func() error { actor.state.view.Queue = queue; actor.state.view.ViewVersion++; return nil })
	view := service.currentView(actor)
	service.publish(view)
	return ImportResult{ThreadID: in.ThreadID, Imported: imported, View: view}, nil
}

func (service *threadRuntimeService) RetryEffect(ctx context.Context, in RetryEffectInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if !in.AcknowledgeUnknownRisk || strings.TrimSpace(in.EffectAttemptID) == "" || strings.TrimSpace(in.ToolCallID) == "" {
		return ThreadView{}, errors.New("retry effect requires an unknown-risk acknowledgement and exact effect identity")
	}
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	reader, ok := service.host.store.repo.(sessiontree.EffectAttemptReader)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	source, err := reader.EffectAttempt(ctx, in.ThreadID.String(), strings.TrimSpace(in.EffectAttemptID))
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	if (source.State != sessiontree.EffectAttemptUnknown && source.State != sessiontree.EffectAttemptRetrying) || source.Invocation.ToolCallID != strings.TrimSpace(in.ToolCallID) {
		return ThreadView{}, ErrRequestConflict
	}
	normalizedInput := in
	normalizedInput.RequestKey = RequestKey(key)
	normalizedInput.EffectAttemptID = strings.TrimSpace(normalizedInput.EffectAttemptID)
	normalizedInput.ToolCallID = strings.TrimSpace(normalizedInput.ToolCallID)
	fingerprint, _ := stableFingerprint(normalizedInput)
	actor := service.runtime(in.ThreadID)
	var result ThreadView
	var replayed bool
	// Request-key idempotency is checked before consulting the source
	// authority. A completed source attempt is still a successful replay of
	// the original command, not a new conflicting retry.
	err = actor.apply(ctx, func() error {
		if existing, found := actor.state.requestKeys[key]; found {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "retry effect", RequestID: key, Err: ErrRequestConflict}
			}
			replayed = true
			result = cloneThreadRuntimeView(actor.state.view)
		}
		return nil
	})
	if err != nil || replayed {
		return result, err
	}
	claimedLocally, err := actor.claimEffectRetrySource(strings.TrimSpace(in.EffectAttemptID))
	if err != nil {
		return ThreadView{}, err
	}
	if !claimedLocally {
		return ThreadView{}, ErrRequestConflict
	}
	if err := actor.claimEffectDispatch(); err != nil {
		actor.releaseEffectRetrySource(strings.TrimSpace(in.EffectAttemptID))
		return ThreadView{}, err
	}
	releaseEffect := func() {
		actor.releaseEffectDispatch()
		actor.releaseEffectRetrySource(strings.TrimSpace(in.EffectAttemptID))
	}
	operationCtx, finishOperation, err := service.host.store.beginLifetimeOperationContext()
	if err != nil {
		releaseEffect()
		return ThreadView{}, err
	}
	retryCtx, retryCancel := context.WithCancel(operationCtx)
	actor.mu.Lock()
	if actor.deleting || actor.deleted || actor.closed {
		actor.mu.Unlock()
		retryCancel()
		releaseEffect()
		finishOperation()
		return ThreadView{}, ErrThreadDeleted
	}
	actor.state.effectRetryEpoch++
	retryEpoch := actor.state.effectRetryEpoch
	if actor.state.effectRetryCancels == nil {
		actor.state.effectRetryCancels = make(map[uint64]context.CancelFunc)
	}
	actor.state.effectRetryCancels[retryEpoch] = retryCancel
	actor.mu.Unlock()
	finishRetry := func() {
		actor.mu.Lock()
		if actor.state.effectRetryCancels != nil {
			delete(actor.state.effectRetryCancels, retryEpoch)
		}
		actor.mu.Unlock()
		retryCancel()
		releaseEffect()
		finishOperation()
	}
	claimer, ok := service.host.store.repo.(sessiontree.EffectRetryRepo)
	if !ok {
		finishRetry()
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	claimed, err := claimer.ClaimEffectRetry(ctx, sessiontree.ClaimEffectRetryRequest{
		EffectAttemptID: strings.TrimSpace(in.EffectAttemptID), ToolCallID: strings.TrimSpace(in.ToolCallID),
		RequestKey: key, RequestFingerprint: fingerprint, Now: time.Now().UTC(),
	})
	if err != nil {
		finishRetry()
		return ThreadView{}, runtimeHostError(err)
	}
	source = claimed.Attempt
	err = actor.apply(ctx, func() error {
		if existing, found := actor.state.requestKeys[key]; found {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "retry effect", RequestID: key, Err: ErrRequestConflict}
			}
			replayed = true
			result = cloneThreadRuntimeView(actor.state.view)
			return nil
		}
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
		actor.state.view.ViewVersion++
		result = cloneThreadRuntimeView(actor.state.view)
		return nil
	})
	if err != nil || replayed {
		finishRetry()
		return result, err
	}
	service.publish(result)
	if claimed.Replayed {
		// The durable claim predates this process. The in-process source fence
		// above makes this the sole dispatcher after restart; a concurrent local
		// request returned before reaching the claim.
	}
	go service.runEffectRetry(retryCtx, finishRetry, actor, in.ThreadID, source, key)
	return result, nil
}

func (service *threadRuntimeService) runEffectRetry(ctx context.Context, finish func(), actor *threadRuntimeState, threadID identity.ThreadID, source sessiontree.EffectAttempt, requestKey string) {
	defer finish()
	agent, err := service.factory.Agent(ctx, AgentRequest{
		ThreadID: threadID, TurnID: identity.TurnID(source.Invocation.TurnID), RequestKey: requestKey,
		EffectAttemptID: source.EffectAttemptID,
	})
	if err == nil && agent == nil {
		err = errors.New("Agent factory returned nil")
	}
	if err != nil {
		service.effectRetryFailed(actor, err)
		return
	}
	runner, err := service.host.turnRunner(ctx, threadID, service.executionAgent(actor, agent))
	if err != nil {
		service.effectRetryFailed(actor, err)
		return
	}
	if _, err := runner.RetryUnknownEffect(ctx, source.EffectAttemptID, requestKey); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			service.effectRetryFailed(actor, err)
		}
		return
	}
	active := false
	_ = actor.apply(context.Background(), func() error {
		active = actor.state.view.Activity == ThreadActivityActive &&
			actor.state.turnID.String() == source.Invocation.TurnID && actor.state.runID.String() == source.Invocation.RunID
		return nil
	})
	if !active {
		return
	}
	service.refreshCanonical(threadID, identity.TurnID(source.Invocation.TurnID))
	service.redispatchAcceptedTurn(ctx, threadID, identity.TurnID(source.Invocation.TurnID), identity.RunID(source.Invocation.RunID))
}

func (service *threadRuntimeService) effectRetryFailed(actor *threadRuntimeState, err error) {
	if service != nil && service.host != nil && service.host.store != nil {
		service.host.store.reportBackgroundError(err)
	}
	_ = actor.apply(context.Background(), func() error {
		actor.state.view.Error = err.Error()
		actor.state.view.ViewVersion++
		service.publish(cloneThreadRuntimeView(actor.state.view))
		return nil
	})
}

func (service *threadRuntimeService) currentView(actor *threadRuntimeState) ThreadView {
	var view ThreadView
	_ = actor.apply(context.Background(), func() error { view = cloneThreadRuntimeView(actor.state.view); return nil })
	return view
}

func (host *Host) threadRuntimeForFence() *threadRuntimeService {
	host.threadRuntimeMu.Lock()
	defer host.threadRuntimeMu.Unlock()
	if host.threadRuntime == nil {
		host.threadRuntime = &threadRuntimeService{
			host: host, subscribers: make(map[*WorkspaceSubscription]struct{}), published: make(map[string]uint64), runtimes: make(map[string]*threadRuntimeState),
		}
	}
	return host.threadRuntime
}

func (service *threadRuntimeService) runtime(threadID identity.ThreadID) *threadRuntimeState {
	service.runtimesMu.Lock()
	defer service.runtimesMu.Unlock()
	if runtime := service.runtimes[threadID.String()]; runtime != nil {
		return runtime
	}
	runtime := &threadRuntimeState{threadID: threadID.String()}
	if service.closed {
		runtime.closed = true
		return runtime
	}
	service.runtimes[threadID.String()] = runtime
	return runtime
}

func (service *threadRuntimeService) close() {
	if service == nil {
		return
	}
	service.mu.Lock()
	subscriptions := make([]*WorkspaceSubscription, 0, len(service.subscribers))
	for subscription := range service.subscribers {
		subscriptions = append(subscriptions, subscription)
	}
	service.subscribers = make(map[*WorkspaceSubscription]struct{})
	service.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.closeLocked()
		subscription.mu.Unlock()
	}
	service.runtimesMu.Lock()
	service.closed = true
	runtimes := make([]*threadRuntimeState, 0, len(service.runtimes))
	for _, runtime := range service.runtimes {
		runtimes = append(runtimes, runtime)
	}
	service.runtimesMu.Unlock()
	drains := make([]threadRuntimeDrain, 0, len(runtimes))
	for _, runtime := range runtimes {
		runtime.mu.Lock()
		runtime.closed = true
		drains = append(drains, threadRuntimeDrain{
			actor: runtime, executionDone: runtime.state.executionDone, effectsDone: runtime.state.effectsDone,
		})
		if runtime.state.cancel != nil {
			service.recordCancellation(identity.ThreadID(runtime.threadID), runtime.state.turnID, runtime.state.runID, "runtime_shutdown", "runtime shutdown")
			runtime.state.cancel()
		}
		for _, cancelRetry := range runtime.state.effectRetryCancels {
			cancelRetry()
		}
		runtime.mu.Unlock()
	}
	_ = waitThreadRuntimeDrains(context.Background(), drains)
}

func (service *threadRuntimeService) View(ctx context.Context, threadID identity.ThreadID) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	var view ThreadView
	if err := actor.apply(ctx, func() error {
		if actor.state.view.ViewVersion > 0 {
			view = cloneThreadRuntimeView(actor.state.view)
		}
		return nil
	}); err != nil || view.ViewVersion > 0 {
		return view, err
	}
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	canonical := ThreadView{ThreadID: threadID, Activity: ThreadActivityIdle}
	canonical.Items, canonical.Interactions, err = hydrateThreadRuntimeItems(ctx, service.host.store.repo, threadID)
	if err != nil {
		return ThreadView{}, err
	}
	canonical.Queue, err = hydrateCanonicalQueue(ctx, service.host.store.repo, threadID)
	if err != nil {
		return ThreadView{}, err
	}
	startHydration := false
	err = actor.apply(ctx, func() error {
		if actor.state.view.ViewVersion > 0 {
			view = cloneThreadRuntimeView(actor.state.view)
			return nil
		}
		canonical.ViewVersion = 1
		canonical.ThreadID = threadID
		var runID identity.RunID
		canonical.TurnID, runID, canonical.Activity, canonical.LastOutcome, canonical.Failure = hydrateThreadRuntimeLifecycle(ctx, service.host.store.repo, meta)
		canonical.RunID = runID
		if canonical.Activity == ThreadActivityActive && !threadRuntimeViewNeedsAttention(canonical) {
			canonical.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
		}
		canonical.Error = threadTurnFailureMessage(canonical.Failure)
		actor.state.view = canonical
		actor.state.turnID = canonical.TurnID
		actor.state.runID = runID
		actor.state.requestKeys = hydrateThreadRequestKeys(ctx, service.host.store.repo, meta)
		if canonical.Activity == ThreadActivityActive && !actor.state.hydrationStarted {
			actor.state.hydrationStarted = true
			startHydration = true
		}
		view = cloneThreadRuntimeView(canonical)
		return nil
	})
	if err == nil && startHydration {
		go service.recoverHydratedThread(threadID)
	}
	return view, err
}

func stableFingerprint(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("sha256:%x", digest[:]), nil
}

func hydrateThreadRequestKeys(ctx context.Context, repo sessiontree.JournalRepo, meta sessiontree.ThreadMeta) map[string]threadRuntimeRequest {
	keys := make(map[string]threadRuntimeRequest)
	if strings.TrimSpace(meta.LeafID) == "" {
		return keys
	}
	page, err := repo.PathPage(ctx, meta.ID, meta.LeafID, "", 256)
	if err != nil {
		return keys
	}
	for _, entry := range page.Entries {
		key := strings.TrimSpace(entry.RequestKey)
		if key == "" {
			continue
		}
		fingerprint := strings.TrimSpace(entry.RequestFingerprint)
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			if sourceTurnID := strings.TrimSpace(entry.Metadata[sessiontree.RetrySourceTurnIDMetadataKey]); sourceTurnID != "" {
				fingerprint, _ = stableFingerprint(identity.TurnID(sourceTurnID))
			}
		}
		keys[key] = threadRuntimeRequest{fingerprint: fingerprint}
	}
	return keys
}

func (service *threadRuntimeService) recoverHydratedThread(threadID identity.ThreadID) {
	ctx := context.Background()
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return
	}
	entries, err := service.host.store.repo.Entries(ctx, threadID.String())
	if err != nil {
		return
	}
	turnID, runID, activity, _, _ := hydrateThreadRuntimeLifecycle(ctx, service.host.store.repo, meta)
	if activity != ThreadActivityActive || turnID == "" || runID == "" {
		return
	}
	if turnHasCancelRequest(entries, turnID) {
		service.finishUnloadedCancellation(service.runtime(threadID), threadID, turnID, runID)
		return
	}
	if service.recoverClaimedEffectRetries(ctx, threadID, turnID, entries) {
		return
	}
	blockedByUnknown := service.recoverEffectAttempts(ctx, threadID, turnID, entries)
	if blockedByUnknown {
		service.refreshCanonical(threadID, turnID)
		return
	}
	view := service.currentView(service.runtime(threadID))
	for _, interaction := range view.Interactions {
		if interaction.TurnID != turnID {
			continue
		}
		if !interaction.Resolved {
			return
		}
		if interaction.Kind != ThreadInteractionInput || interaction.Resolution == nil {
			continue
		}
		if interaction.Resolution.Redacted {
			service.restoreSecretInteraction(ctx, threadID, interaction, meta.LeafID)
			return
		}
		service.continueCanonicalInput(service.runtime(threadID), threadID, interaction, InteractionAnswer{
			InteractionID: interaction.ID, Input: maps.Clone(interaction.Resolution.Input),
		})
		return
	}
	service.redispatchAcceptedTurn(ctx, threadID, turnID, runID)
}

func (service *threadRuntimeService) recoverClaimedEffectRetries(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, entries []sessiontree.Entry) bool {
	latest := make(map[string]sessiontree.EffectAttempt)
	for _, entry := range entries {
		if entry.TurnID != turnID.String() || entry.Type != sessiontree.EntryEffectAttempt {
			continue
		}
		attempt, err := sessiontree.DecodeCanonicalEffectAttempt(entry)
		if err == nil {
			latest[attempt.EffectAttemptID] = attempt
		}
	}
	resolved := settledRetrySources(latest)
	children := make(map[string][]sessiontree.EffectAttempt)
	for _, attempt := range latest {
		if sourceID := strings.TrimSpace(attempt.Invocation.SourceEffectAttemptID); sourceID != "" {
			children[sourceID] = append(children[sourceID], attempt)
		}
	}
	started := false
	for _, attempt := range latest {
		if attempt.State != sessiontree.EffectAttemptRetrying || strings.TrimSpace(attempt.RetryRequestKey) == "" {
			continue
		}
		if _, settled := resolved[attempt.EffectAttemptID]; settled {
			continue
		}
		ready := len(children[attempt.EffectAttemptID]) == 0
		for _, child := range children[attempt.EffectAttemptID] {
			if child.State == sessiontree.EffectAttemptPrepared {
				ready = true
			}
		}
		if !ready {
			continue
		}
		started = true
		_, err := service.RetryEffect(ctx, RetryEffectInput{
			ThreadID: threadID, EffectAttemptID: attempt.EffectAttemptID,
			ToolCallID: attempt.Invocation.ToolCallID, AcknowledgeUnknownRisk: true,
			RequestKey: RequestKey(attempt.RetryRequestKey),
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			service.host.store.reportBackgroundError(err)
		}
	}
	return started
}

func turnHasCancelRequest(entries []sessiontree.Entry, turnID identity.TurnID) bool {
	for _, entry := range entries {
		if entry.TurnID == turnID.String() && entry.Type == sessiontree.EntryCancelRequested {
			return true
		}
	}
	return false
}

func (service *threadRuntimeService) recoverEffectAttempts(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, entries []sessiontree.Entry) bool {
	latest := make(map[string]sessiontree.EffectAttempt)
	for _, entry := range entries {
		if entry.TurnID != turnID.String() || entry.Type != sessiontree.EntryEffectAttempt {
			continue
		}
		attempt, err := sessiontree.DecodeCanonicalEffectAttempt(entry)
		if err == nil {
			latest[attempt.EffectAttemptID] = attempt
		}
	}
	resolved := settledRetrySources(latest)
	authority, ok := service.host.store.repo.(sessiontree.EffectAttemptRepo)
	blocked := false
	for _, attempt := range latest {
		switch attempt.State {
		case sessiontree.EffectAttemptDispatching:
			blocked = true
			if ok {
				_, _ = authority.MarkEffectUnknown(ctx, sessiontree.MarkEffectUnknownRequest{
					EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: attempt.RequestFingerprint,
					OutcomeFingerprint: sessiontree.StableHash(attempt.EffectAttemptID + "\x00runtime-restarted"), Now: time.Now().UTC(),
				})
			}
		case sessiontree.EffectAttemptRetrying:
			if _, settled := resolved[attempt.EffectAttemptID]; !settled {
				blocked = true
			}
		case sessiontree.EffectAttemptUnknown:
			if _, settled := resolved[attempt.EffectAttemptID]; !settled {
				blocked = true
			}
		}
	}
	return blocked
}

func settledRetrySources(attempts map[string]sessiontree.EffectAttempt) map[string]struct{} {
	resolved := make(map[string]struct{})
	for {
		changed := false
		for _, attempt := range attempts {
			sourceID := strings.TrimSpace(attempt.Invocation.SourceEffectAttemptID)
			if sourceID == "" || (!effectAttemptSettlesRetry(attempt.State) && !hasResolvedRetrySource(resolved, attempt.EffectAttemptID)) {
				continue
			}
			if _, found := resolved[sourceID]; !found {
				resolved[sourceID] = struct{}{}
				changed = true
			}
		}
		if !changed {
			return resolved
		}
	}
}

func hasResolvedRetrySource(resolved map[string]struct{}, attemptID string) bool {
	_, found := resolved[strings.TrimSpace(attemptID)]
	return found
}

func (service *threadRuntimeService) restoreSecretInteraction(ctx context.Context, threadID identity.ThreadID, source ThreadInteraction, leafID string) {
	restored := source
	restored.ID = "secret-retry:" + source.ID + ":" + sessiontree.StableHash(leafID)[:12]
	restored.Resolved = false
	restored.Resolution = nil
	payload, err := json.Marshal(restored)
	if err != nil {
		return
	}
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return
	}
	_, err = writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{
		{ID: "runtime-restarted:" + sessiontree.StableHash(leafID)[:24], ThreadID: threadID.String(), TurnID: source.TurnID.String(), Type: sessiontree.EntryRuntimeRestarted},
		{ID: "interaction-requested:" + restored.ID, ThreadID: threadID.String(), TurnID: source.TurnID.String(), Type: sessiontree.EntryInteractionAsked, Payload: payload},
	})
	if err == nil {
		service.refreshCanonical(threadID, source.TurnID)
	}
}

func (service *threadRuntimeService) redispatchAcceptedTurn(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) {
	journal, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return
	}
	canonical, found, err := journal.ReadAcceptedTurn(ctx, threadID.String(), turnID.String(), runID.String())
	if err != nil || !found || canonical.Terminal != nil {
		return
	}
	input := turnInputFromSessionMessage(canonical.UserMessage.Message)
	requestKey := strings.TrimSpace(canonical.TurnStarted.RequestKey)
	var retrySourceTurnID identity.TurnID
	var retrySourceEntryID string
	if reader, ok := service.host.store.repo.(sessiontree.CanonicalTurnReadRepo); ok {
		read, readErr := reader.ReadCanonicalTurn(ctx, threadID.String(), turnID.String())
		if readErr != nil {
			return
		}
		if read.Turn.RetrySource != nil {
			retrySourceTurnID = identity.TurnID(read.Turn.RetrySource.TurnID)
			retrySourceEntryID = strings.TrimSpace(read.Turn.RetrySource.EntryID)
		}
	}
	agent, err := service.factory.Agent(ctx, AgentRequest{
		ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input,
		RetrySource: retrySourceTurnID,
	})
	if err != nil || agent == nil {
		return
	}
	runner, err := service.host.turnRunner(ctx, threadID, service.executionAgent(service.runtime(threadID), agent))
	if err != nil {
		return
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	actor := service.runtime(threadID)
	_ = actor.apply(ctx, func() error {
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		actor.state.executionDone = done
		actor.state.agent = agent
		return nil
	})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		RetrySourceTurnID: retrySourceTurnID, RetrySourceEntryID: retrySourceEntryID,
		Signals: TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	accepted := acceptedTurn{ThreadID: threadID, TurnID: turnID, RunID: runID, UserEntryID: canonical.UserMessage.ID, BaseLeafID: canonical.BaseLeafID, Replayed: true}
	go service.executeAcceptedSend(executionCtx, actor, runner, accepted, request, nil, done)
}

func turnInputFromSessionMessage(message session.Message) UserInput {
	input := UserInput{Text: message.Content}
	for _, attachment := range message.Attachments {
		item := MessageAttachment{ResourceRef: attachment.ResourceRef, Name: attachment.Name, MIMEType: attachment.MIMEType, SizeBytes: attachment.SizeBytes}
		if attachment.TextStats != nil {
			item.TextStats = &MessageAttachmentTextStats{UnicodeCodePointCount: attachment.TextStats.UnicodeCodePointCount, LogicalLineCount: attachment.TextStats.LogicalLineCount}
		}
		input.Attachments = append(input.Attachments, item)
	}
	for _, reference := range message.References {
		input.References = append(input.References, MessageReference{ReferenceID: reference.ReferenceID, Kind: MessageReferenceKind(reference.Kind), Label: reference.Label, Text: reference.Text, ResourceRef: reference.ResourceRef, Truncated: reference.Truncated})
	}
	return input
}

func (service *threadRuntimeService) send(ctx context.Context, threadID identity.ThreadID, input UserInput, supplemental []TurnSupplementalContextItem, requestKey string) (ThreadView, error) {
	if err := input.Validate(); err != nil {
		return ThreadView{}, err
	}
	requestKey = strings.TrimSpace(requestKey)
	if requestKey == "" {
		return ThreadView{}, errors.New("send request key is required")
	}
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(input)
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	canonical, err := service.View(ctx, threadID)
	if err != nil {
		return ThreadView{}, err
	}
	for _, entryID := range []string{"user:" + requestKey, "queue:" + requestKey} {
		entry, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entryID)
		if readErr != nil {
			if errors.Is(readErr, sessiontree.ErrEntryNotFound) {
				continue
			}
			return ThreadView{}, runtimeHostError(readErr)
		}
		if entry.RequestKey != requestKey || entry.RequestFingerprint != fingerprint {
			return ThreadView{}, &RequestConflictError{Operation: "send", RequestID: requestKey, Err: ErrRequestConflict}
		}
		return canonical, nil
	}
	var replay ThreadView
	var found bool
	if err := actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[requestKey]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "send", RequestID: requestKey, Err: ErrRequestConflict}
			}
			replay, found = cloneThreadRuntimeView(actor.state.view), true
		}
		return nil
	}); err != nil || found {
		return replay, err
	}
	var active bool
	if err := actor.apply(ctx, func() error {
		active = actor.state.view.Activity == ThreadActivityActive
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	if active {
		queued := QueuedInput{ID: "queue:" + requestKey, RequestKey: requestKey, Input: input, SupplementalContext: cloneTurnSupplementalContext(supplemental), CreatedAt: time.Now().UTC()}
		var replayedQueue bool
		err := actor.apply(ctx, func() error {
			if existing, ok := actor.state.requestKeys[requestKey]; ok {
				if existing.fingerprint != fingerprint {
					return &RequestConflictError{Operation: "send", RequestID: requestKey, Err: ErrRequestConflict}
				}
				replayedQueue = true
				return nil
			}
			for _, existing := range actor.state.view.Queue {
				if existing.ID == queued.ID {
					replayedQueue = true
					return nil
				}
			}
			if err := service.appendQueueFact(ctx, threadID, sessiontree.EntryQueueAdded, queued.ID, requestKey, fingerprint, queued); err != nil {
				return err
			}
			actor.state.view.ViewVersion++
			actor.state.view.Queue = append(actor.state.view.Queue, queued)
			result := cloneThreadRuntimeView(actor.state.view)
			if actor.state.requestKeys == nil {
				actor.state.requestKeys = make(map[string]threadRuntimeRequest)
			}
			actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
			replay = result
			return nil
		})
		if err != nil {
			return ThreadView{}, err
		}
		if replayedQueue {
			return service.currentView(actor), nil
		}
		service.publish(replay)
		return replay, nil
	}
	turnID, runID, err := service.host.nextTurnRunIDs()
	if err != nil {
		return ThreadView{}, err
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	executionDone := make(chan struct{})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		SupplementalContext: cloneTurnSupplementalContext(supplemental),
		InputFingerprint:    fingerprint,
		Signals:             TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	var result ThreadView
	var accepted acceptedTurn
	var replayed bool
	var previousExecution <-chan struct{}
	err = actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[requestKey]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "send", RequestID: requestKey, Err: ErrRequestConflict}
			}
			result = cloneThreadRuntimeView(actor.state.view)
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		var acceptErr error
		accepted, acceptErr = service.acceptCanonicalTurn(ctx, threadID, request)
		if acceptErr != nil {
			return acceptErr
		}
		_ = canonical
		actor.state.turnID, actor.state.runID = turnID, runID
		actor.state.logicalRequestID = identity.LogicalRequestID(requestKey)
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		previousExecution = actor.state.executionDone
		actor.state.executionDone = executionDone
		actor.state.view.ViewVersion++
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.TurnID = turnID
		actor.state.view.RunID = runID
		actor.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
		actor.state.view.LastOutcome = nil
		actor.state.view.Failure = nil
		actor.state.view.Error = ""
		actor.state.openTextSegmentID = ""
		actor.state.openTextKind = ""
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: "user:" + requestKey, TurnID: turnID, Kind: ThreadItemUser, Text: input.Text, CreatedAt: time.Now().UTC(), Attachments: cloneMessageAttachments(input.Attachments), References: append([]MessageReference(nil), input.References...)})
		result = cloneThreadRuntimeView(actor.state.view)
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		return nil
	})
	if err != nil {
		cancel()
		return ThreadView{}, err
	}
	if replayed {
		cancel()
		return result, nil
	}
	service.publish(result)
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{
		ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input,
	})
	go service.executePreparedSend(executionCtx, actor, prepared, accepted, request, previousExecution, executionDone)
	return result, nil
}

func (service *threadRuntimeService) acceptAndExecutePreparedSend(
	ctx context.Context,
	actor *threadRuntimeState,
	prepared <-chan preparedThreadExecution,
	threadID identity.ThreadID,
	request turnExecutionRequest,
	previousExecution <-chan struct{},
	executionDone chan<- struct{},
	requestKey string,
	retry bool,
) {
	accepted, err := service.acceptCanonicalTurn(ctx, threadID, request)
	if err != nil {
		close(executionDone)
		if retry {
			service.rollbackProvisionalRetry(actor, request.TurnID, request.RunID, requestKey)
		} else {
			service.rollbackProvisionalSend(actor, request.TurnID, request.RunID, requestKey)
		}
		return
	}
	service.executePreparedSend(ctx, actor, prepared, accepted, request, previousExecution, executionDone)
}

func (service *threadRuntimeService) prepareExecution(ctx context.Context, actor *threadRuntimeState, request AgentRequest) <-chan preparedThreadExecution {
	ready := make(chan preparedThreadExecution, 1)
	go func() {
		prepared := preparedThreadExecution{}
		prepared.agent, prepared.err = service.factory.Agent(ctx, request)
		if prepared.err == nil && prepared.agent == nil {
			prepared.err = errors.New("Agent factory returned nil")
		}
		if prepared.err == nil {
			prepared.runner, prepared.err = service.host.turnRunner(ctx, request.ThreadID, service.executionAgent(actor, prepared.agent))
		}
		ready <- prepared
	}()
	return ready
}

func (service *threadRuntimeService) acceptCanonicalTurn(ctx context.Context, threadID identity.ThreadID, request turnExecutionRequest) (acceptedTurn, error) {
	journal, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return acceptedTurn{}, ErrUnsupportedStoreCapability
	}
	canonicalInput := session.Message{
		Role: session.User, Content: request.Input.Text,
		Attachments: sessionMessageAttachments(request.Input.Attachments),
		References:  sessionMessageReferences(request.Input.References),
	}
	if request.RetrySourceEntryID != "" {
		canonicalInput = session.Message{}
	}
	req := sessiontree.AcceptTurnRequest{
		ThreadID: threadID.String(), TurnID: request.TurnID.String(), RunID: request.RunID.String(),
		LogicalRequestID:            request.LogicalRequestID.String(),
		Input:                       canonicalInput,
		PromotedQueueID:             request.PromotedQueueID,
		PromotionRequestKey:         request.PromotionRequestKey,
		PromotionRequestFingerprint: request.PromotionRequestFingerprint,
		InputRequestFingerprint:     request.InputFingerprint,
		RetrySourceTurnID:           request.RetrySourceTurnID.String(),
		RetrySourceEntryID:          request.RetrySourceEntryID,
		Now:                         time.Now().UTC(),
	}
	fingerprint, err := sessiontree.TurnAcceptanceRequestFingerprint(req)
	if err != nil {
		return acceptedTurn{}, err
	}
	req.RequestFingerprint = fingerprint
	accepted, err := journal.AcceptTurn(ctx, req)
	if err != nil {
		return acceptedTurn{}, runtimeHostError(err)
	}
	return acceptedTurn{
		ThreadID: identity.ThreadID(req.ThreadID), TurnID: request.TurnID, RunID: request.RunID,
		UserEntryID: accepted.UserMessage.ID, BaseLeafID: accepted.BaseLeafID, Replayed: accepted.Replayed,
	}, nil
}

func agentManualCompactions(agent *Agent) ManualCompactionSource {
	if agent == nil {
		return nil
	}
	return agent.manualCompactions
}

func (service *threadRuntimeService) rollbackProvisionalSend(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, requestKey string) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID {
			return nil
		}
		for index := len(actor.state.view.Items) - 1; index >= 0; index-- {
			if actor.state.view.Items[index].ID == "user:"+requestKey {
				actor.state.view.Items = append(actor.state.view.Items[:index], actor.state.view.Items[index+1:]...)
				break
			}
		}
		delete(actor.state.requestKeys, requestKey)
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.TurnID = ""
		actor.state.view.RunID = ""
		actor.state.view.RunProgress = nil
		actor.state.view.ViewVersion++
		service.publish(cloneThreadRuntimeView(actor.state.view))
		return nil
	})
}

func (service *threadRuntimeService) appendQueueFact(ctx context.Context, threadID identity.ThreadID, kind sessiontree.EntryType, entryID, requestKey, fingerprint string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	entry := sessiontree.Entry{
		ID: entryID, ThreadID: threadID.String(), Type: kind, RequestKey: requestKey,
		RequestFingerprint: fingerprint, Payload: raw,
	}
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	_, appendErr := writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{entry})
	if appendErr == nil {
		return nil
	}
	existing, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entryID)
	if readErr != nil {
		return runtimeHostError(appendErr)
	}
	if existing.Type != kind || existing.RequestKey != requestKey || existing.RequestFingerprint != fingerprint || string(existing.Payload) != string(raw) {
		return ErrRequestConflict
	}
	return nil
}

func (service *threadRuntimeService) executePreparedSend(
	ctx context.Context,
	actor *threadRuntimeState,
	prepared <-chan preparedThreadExecution,
	accepted acceptedTurn,
	request turnExecutionRequest,
	previousExecution <-chan struct{},
	executionDone chan<- struct{},
) {
	select {
	case <-ctx.Done():
		service.recordCancellation(accepted.ThreadID, request.TurnID, request.RunID, "execution_context", ctx.Err().Error())
		close(executionDone)
		service.finishUnloadedCancellation(actor, accepted.ThreadID, request.TurnID, request.RunID)
		return
	case execution := <-prepared:
		if execution.err != nil {
			close(executionDone)
			service.finishUnloadedTerminal(actor, accepted.ThreadID, request.TurnID, request.RunID, TurnResult{}, execution.err)
			return
		}
		request.ManualCompactions = agentManualCompactions(execution.agent)
		_ = actor.apply(context.Background(), func() error {
			if actor.state.turnID == request.TurnID && actor.state.runID == request.RunID {
				actor.state.agent = execution.agent
			}
			return nil
		})
		service.executeAcceptedSend(ctx, actor, execution.runner, accepted, request, previousExecution, executionDone)
	}
}

func (service *threadRuntimeService) executeAcceptedSend(ctx context.Context, actor *threadRuntimeState, runner *turnRunnerHandle, accepted acceptedTurn, request turnExecutionRequest, previousExecution <-chan struct{}, executionDone chan<- struct{}) {
	defer close(executionDone)
	if previousExecution != nil {
		select {
		case <-ctx.Done():
			service.recordCancellation(accepted.ThreadID, request.TurnID, request.RunID, "execution_context", ctx.Err().Error())
			service.finishUnloadedTerminal(actor, accepted.ThreadID, request.TurnID, request.RunID, TurnResult{Status: TurnStatusCancelled}, ctx.Err())
			return
		case <-previousExecution:
		}
	}
	completed, err := runner.ExecuteAccepted(ctx, acceptedTurnExecutionRequest{
		Accepted: accepted, LogicalRequestID: request.LogicalRequestID, RunID: request.RunID, TurnID: request.TurnID,
		Input: request.Input, SupplementalContext: request.SupplementalContext, Labels: request.Labels,
		Completion: request.Completion, Signals: request.Signals, Limits: request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
	})
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		service.recordCancellation(accepted.ThreadID, request.TurnID, request.RunID, "execution_context", err.Error())
	}
	service.finishSend(actor, request.TurnID, request.RunID, completed, err)
}

func (service *threadRuntimeService) finishSend(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, completed TurnResult, runErr error) {
	settleCanonical := completed.Status != TurnStatusWaiting
	var outcome TurnOutcome
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID || actor.state.view.Activity != ThreadActivityActive {
			return nil
		}
		if actor.state.cancelOwner == "run:"+runID.String() {
			actor.state.cancel = nil
			actor.state.cancelOwner = ""
		}
		actor.state.view.ViewVersion++
		actor.finishLiveTextSegment()
		actor.state.view.AssistantDraft = ""
		actor.state.view.ThinkingDraft = ""
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.RunProgress = nil
		outcome = TurnOutcomeCompleted
		if completed.Status == TurnStatusWaiting {
			actor.state.view.Activity = ThreadActivityActive
			actor.state.view.LastOutcome = nil
		} else if completed.Status == TurnStatusCancelled || completed.Status == TurnStatusInterrupted || errors.Is(runErr, context.Canceled) {
			outcome = TurnOutcomeCancelled
		} else if runErr != nil || completed.Status == TurnStatusFailed {
			outcome = TurnOutcomeFailed
		}
		actor.state.view.Failure = nil
		actor.state.view.Error = ""
		if outcome == TurnOutcomeFailed {
			if completed.Failure != nil {
				actor.state.view.Failure = cloneThreadTurnFailure(completed.Failure)
				actor.state.view.Error = threadTurnFailureMessage(actor.state.view.Failure)
			}
			if actor.state.view.Error == "" && runErr != nil {
				actor.state.view.Failure = &ThreadTurnFailure{Code: ThreadTurnFailureEngineContract, Message: strings.TrimSpace(runErr.Error())}
				actor.state.view.Error = threadTurnFailureMessage(actor.state.view.Failure)
			}
		}
		if completed.Status != TurnStatusWaiting {
			actor.state.view.LastOutcome = &outcome
			actor.state.view.Items = settleTerminalToolSegments(actor.state.view.Items, turnID, completed.ActivityTimeline)
		}
		return nil
	})
	if !settleCanonical {
		service.publish(service.currentView(actor))
		return
	}
	// TurnResult.Output is the run-level aggregate, not a display segment.
	// The canonical journal owns terminal text and preserves each ordered
	// assistant segment without creating a second item at settlement. Publish
	// only after the journal projection has been applied, so a terminal view
	// cannot temporarily claim success while containing only the user item.
	refreshErr := service.refreshCanonical(identity.ThreadID(actor.threadID), turnID)
	current := service.currentView(actor)
	if refreshErr != nil && !errors.Is(refreshErr, errCanonicalRefreshDiscarded) && outcome == TurnOutcomeCompleted {
		_ = actor.apply(context.Background(), func() error {
			if actor.state.turnID != turnID || actor.state.runID != runID {
				return nil
			}
			actor.state.view.ViewVersion++
			failed := TurnOutcomeFailed
			actor.state.view.LastOutcome = &failed
			actor.state.view.Error = "The completed response could not be loaded."
			return nil
		})
		current = service.currentView(actor)
	} else if outcome == TurnOutcomeCompleted && !hasTerminalPresentation(current.Items, turnID) {
		_ = actor.apply(context.Background(), func() error {
			if actor.state.turnID != turnID || actor.state.runID != runID {
				return nil
			}
			actor.state.view.ViewVersion++
			failed := TurnOutcomeFailed
			actor.state.view.LastOutcome = &failed
			actor.state.view.Error = "The turn completed without a visible response."
			return nil
		})
		current = service.currentView(actor)
	}
	service.publish(current)
	if completed.Status == TurnStatusCompleted && runErr == nil {
		service.startNextQueued(actor)
	}
}

func hasTerminalPresentation(items []ThreadItem, turnID identity.TurnID) bool {
	for _, item := range items {
		if item.TurnID != turnID {
			continue
		}
		switch item.Kind {
		case ThreadItemAssistant:
			if strings.TrimSpace(item.Text) != "" {
				return true
			}
		case ThreadItemTool:
			if item.Activity != nil && item.Activity.Status != observation.ActivityStatusRunning {
				return true
			}
		case ThreadItemInteraction:
			if item.Interaction != nil && item.Interaction.Resolved {
				return true
			}
		}
	}
	return false
}

func threadItemIndexByID(items []ThreadItem, id string) int {
	if id == "" {
		return -1
	}
	for index := range items {
		if items[index].ID == id {
			return index
		}
	}
	return -1
}

func appendThreadItem(items []ThreadItem, item ThreadItem) []ThreadItem {
	item.Ordinal = uint64(len(items) + 1)
	return append(items, item)
}

type threadRuntimeEventSink struct {
	service *threadRuntimeService
}

type combinedThreadEventSink struct {
	primary EventSink
	runtime threadRuntimeEventSink
}

func (sink combinedThreadEventSink) EmitEvent(event Event) {
	if sink.primary != nil {
		sink.primary.EmitEvent(event)
	}
	sink.runtime.EmitEvent(event)
}

func (service *threadRuntimeService) executionAgent(_ *threadRuntimeState, agent *Agent) *Agent {
	if agent == nil {
		return nil
	}
	copy := *agent
	copy.eventSink = combinedThreadEventSink{primary: agent.eventSink, runtime: threadRuntimeEventSink{service: service}}
	copy.effectAuthorization = threadRuntimeEffectGate{service: service, downstream: agent.effectAuthorization}
	return &copy
}

type threadRuntimeEffectGate struct {
	service    *threadRuntimeService
	downstream EffectAuthorizationGate
}

func (gate threadRuntimeEffectGate) Dispatch(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	if gate.service == nil {
		return EffectDispatchResult{}, ErrAuthorizationUnavailable
	}
	if request.Permission.Mode == tools.PermissionDeny {
		return EffectDispatchResult{}, ErrEffectUnauthorized
	}
	if request.Permission.Mode != tools.PermissionAsk {
		if gate.downstream == nil {
			return EffectDispatchResult{}, ErrAuthorizationUnavailable
		}
		return gate.dispatch(ctx, request, effect)
	}
	threadInteraction := ThreadInteraction{
		ID: "approval:" + request.EffectAttemptID, TurnID: request.TurnID, runID: request.RunID,
		Kind: ThreadInteractionApproval, ToolCallID: request.ToolCallID,
		Approval: approvalPresentation(request),
	}
	actor := gate.service.runtime(request.ThreadID)
	waiter, err := gate.service.requestInteraction(ctx, actor, request.ThreadID, threadInteraction)
	if err != nil {
		return EffectDispatchResult{}, err
	}
	select {
	case <-ctx.Done():
		return EffectDispatchResult{}, ctx.Err()
	case resolution := <-waiter.resolution:
		if resolution.Approved == nil || !*resolution.Approved {
			return EffectDispatchResult{}, ErrEffectUnauthorized
		}
	}
	if gate.downstream == nil {
		return EffectDispatchResult{}, ErrAuthorizationUnavailable
	}
	return gate.dispatch(ctx, request, effect)
}

func (gate threadRuntimeEffectGate) dispatch(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	actor := gate.service.runtime(request.ThreadID)
	if err := actor.claimEffectDispatch(); err != nil {
		return EffectDispatchResult{}, err
	}
	defer actor.releaseEffectDispatch()
	return gate.downstream.Dispatch(ctx, request, effect)
}

func approvalPresentation(request EffectAuthorizationRequest) *ApprovalPresentation {
	label := strings.TrimSpace(request.ToolName)
	description := ""
	command := ""
	if activity := request.Activity; activity != nil {
		if activityLabel := strings.TrimSpace(activity.Label); activityLabel != "" {
			label = activityLabel
		}
		description = strings.TrimSpace(activity.Description)
		if activity.Renderer == tools.ActivityRendererTerminal {
			if payload, ok := activity.Payload.(tools.TerminalActivityPayload); ok {
				command = strings.TrimSpace(payload.Command)
			}
		}
	}
	return &ApprovalPresentation{
		Label: label, Description: description, Command: command,
		Effects: effectNames(request.Effects), Targets: effectTargets(request.Resources),
		ToolName: request.ToolName, ToolCallID: request.ToolCallID,
	}
}

func effectNames(effects []tools.Effect) []string {
	out := make([]string, 0, len(effects))
	for _, effect := range effects {
		out = append(out, string(effect))
	}
	return out
}

func effectTargets(resources []tools.ResourceRef) []string {
	out := make([]string, 0, len(resources))
	for _, resource := range resources {
		out = append(out, resource.Kind+":"+resource.Value)
	}
	return out
}

func (service *threadRuntimeService) requestInteraction(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, interaction ThreadInteraction) (*pendingThreadInteraction, error) {
	if strings.TrimSpace(interaction.ID) == "" {
		return nil, errors.New("interaction identity is required")
	}
	payload, err := json.Marshal(interaction)
	if err != nil {
		return nil, err
	}
	fingerprint, err := stableFingerprint(interaction)
	if err != nil {
		return nil, err
	}
	entry := sessiontree.Entry{
		ID: "interaction-requested:" + interaction.ID, ThreadID: threadID.String(), TurnID: interaction.TurnID.String(), RunID: interaction.runID.String(),
		Type: sessiontree.EntryInteractionAsked, RequestKey: interaction.ID, RequestFingerprint: fingerprint, Payload: payload,
	}
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	if _, err := writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{entry}); err != nil {
		existing, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entry.ID)
		if readErr != nil || existing.RequestFingerprint != fingerprint || string(existing.Payload) != string(payload) {
			return nil, runtimeHostError(err)
		}
	}
	canonicalItems, _, canonicalErr := hydrateThreadRuntimeItems(ctx, service.host.store.repo, threadID)
	waiter := &pendingThreadInteraction{resolution: make(chan InteractionResolution, 1)}
	err = actor.apply(ctx, func() error {
		if canonicalErr == nil {
			if reconciled, ok := reconcileCanonicalThreadItems(actor.state.view.Items, canonicalItems, false); ok {
				actor.state.view.Items = reconciled
			}
		}
		if actor.state.pendingInteractions == nil {
			actor.state.pendingInteractions = make(map[string]*pendingThreadInteraction)
		}
		if existing := actor.state.pendingInteractions[interaction.ID]; existing != nil {
			waiter = existing
			return nil
		}
		actor.state.pendingInteractions[interaction.ID] = waiter
		for _, current := range actor.state.view.Interactions {
			if current.ID == interaction.ID {
				return nil
			}
		}
		actor.state.view.Interactions = append(actor.state.view.Interactions, interaction)
		copy := interaction
		if interaction.Kind == ThreadInteractionApproval {
			for itemIndex := range actor.state.view.Items {
				item := &actor.state.view.Items[itemIndex]
				if item.Kind == ThreadItemTool && item.Activity != nil && item.Activity.ToolID == interaction.ToolCallID {
					item.Interaction = &copy
					break
				}
			}
		} else {
			itemID := "interaction:" + interaction.ID
			if itemIndex := threadItemIndexByID(actor.state.view.Items, itemID); itemIndex >= 0 {
				actor.state.view.Items[itemIndex].Interaction = &copy
			} else {
				actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: itemID, TurnID: interaction.TurnID, Kind: ThreadItemInteraction, Interaction: &copy})
			}
		}
		actor.state.view.ViewVersion++
		return nil
	})
	if err != nil {
		return nil, err
	}
	service.publish(service.currentView(actor))
	return waiter, nil
}

func (sink threadRuntimeEventSink) EmitEvent(event Event) {
	if sink.service == nil || event.ThreadID == "" {
		return
	}
	actor := sink.service.runtime(event.ThreadID)
	var current ThreadView
	changed := false
	_ = actor.apply(context.Background(), func() error {
		if !actor.acceptLiveEvent(event) {
			return nil
		}
		progress, progressEvent := threadRunProgressForEvent(event)
		progressChanged := progressEvent && !sameThreadRunProgress(actor.state.view.RunProgress, progress)
		if progressChanged {
			actor.state.view.RunProgress = cloneThreadRunProgress(progress)
		}
		if event.Stream != nil || progressChanged {
			actor.state.view.ViewVersion++
			current = cloneThreadRuntimeView(actor.state.view)
			changed = true
		}
		return nil
	})
	if changed {
		sink.service.publish(current)
	}
	if event.Type == observation.EventTypeThreadEntryCommitted && event.committed != nil && event.committed.ToolCall != nil && event.committed.ToolCall.ControlSignal != nil {
		signal := event.committed.ToolCall.ControlSignal
		if signal.Disposition == string(SignalWaiting) && strings.TrimSpace(signal.CallID) != "" {
			interaction := ThreadInteraction{ID: signal.CallID, TurnID: event.TurnID, runID: event.RunID, Kind: ThreadInteractionInput, Input: inputPresentationFromControlSignal(signal.Text, signal.Payload)}
			go func() {
				_, _ = sink.service.requestInteraction(context.Background(), actor, event.ThreadID, interaction)
			}()
		}
	}
	if event.committed != nil || event.Type == observation.EventTypeToolApprovalRequested || event.Type == observation.EventTypeToolApprovalApproved || event.Type == observation.EventTypeToolApprovalRejected || event.Type == observation.EventTypeControlSignal {
		go sink.service.refreshCanonical(event.ThreadID, event.TurnID)
	}
}

func threadRunProgressForEvent(event Event) (*ThreadRunProgress, bool) {
	var phase ThreadRunPhase
	switch event.Type {
	case observation.EventTypeStepStart:
		phase = ThreadRunPhasePreparing
	case observation.EventTypeProviderRequest:
		phase = ThreadRunPhaseWaitingResponse
	case observation.EventTypeProviderDelta,
		observation.EventTypeProviderReasoning,
		observation.EventTypeProviderToolCallStart,
		observation.EventTypeProviderToolCallDelta,
		observation.EventTypeProviderToolCallEnd:
		phase = ThreadRunPhaseStreaming
	case observation.EventTypeProviderRetry:
		phase = ThreadRunPhaseRetrying
	case observation.EventTypeProviderFinish:
		phase = ThreadRunPhaseFinalizing
	case observation.EventTypeToolDispatchStarted,
		observation.EventTypeHostedToolCall,
		observation.EventTypeMCPToolCall:
		phase = ThreadRunPhaseToolExecution
	case observation.EventTypeToolApprovalRequested,
		observation.EventTypeControlSignal,
		observation.EventTypeRunEnd:
		return nil, true
	default:
		return nil, false
	}
	return &ThreadRunProgress{Phase: phase}, true
}

func sameThreadRunProgress(left, right *ThreadRunProgress) bool {
	return left == nil && right == nil || left != nil && right != nil && left.Phase == right.Phase
}

var errCanonicalRefreshDiscarded = errors.New("canonical refresh snapshot discarded")

func (service *threadRuntimeService) refreshCanonical(threadID identity.ThreadID, turnID identity.TurnID) error {
	if _, err := service.ensureThread(context.Background(), threadID); err != nil {
		return fmt.Errorf("canonical refresh ensure thread: %w", err)
	}
	actor := service.runtime(threadID)
	baseVersion := service.currentView(actor).ViewVersion
	items, interactions, err := hydrateThreadRuntimeItems(context.Background(), service.host.store.repo, threadID)
	if err != nil {
		return fmt.Errorf("canonical refresh hydrate items: %w", err)
	}
	var current ThreadView
	changed := false
	var refreshErr error
	_ = actor.apply(context.Background(), func() error {
		if actor.state.view.ViewVersion != baseVersion {
			refreshErr = fmt.Errorf("%w: view version changed", errCanonicalRefreshDiscarded)
			return nil
		}
		if turnID != "" && actor.state.turnID != "" && actor.state.turnID != turnID {
			refreshErr = fmt.Errorf("%w: turn changed", errCanonicalRefreshDiscarded)
			return nil
		}
		terminal := actor.state.view.Activity == ThreadActivityIdle && actor.state.view.LastOutcome != nil
		reconciled, ok := reconcileCanonicalThreadItems(actor.state.view.Items, items, terminal)
		if !ok {
			refreshErr = fmt.Errorf("%w: item identity or ordinal conflict", errCanonicalRefreshDiscarded)
			return nil
		}
		actor.state.view.ViewVersion++
		actor.state.view.Items = reconciled
		actor.state.view.Interactions = mergeThreadInteractions(actor.state.view.Interactions, interactions)
		applyThreadInteractionsToItems(actor.state.view.Items, actor.state.view.Interactions)
		current = cloneThreadRuntimeView(actor.state.view)
		changed = true
		return nil
	})
	if changed {
		service.publish(current)
	}
	return refreshErr
}

func reconcileCanonicalThreadItems(current, canonical []ThreadItem, terminal bool) ([]ThreadItem, bool) {
	if len(current) == 0 {
		return cloneThreadItems(canonical), true
	}
	if terminal {
		// Once a turn is terminal, the journal is the complete presentation
		// authority. Do not retain stream-only items or stale text.
		return cloneThreadItems(canonical), true
	}
	byOrdinal := make(map[uint64]ThreadItem, len(canonical))
	for _, item := range canonical {
		if item.Ordinal == 0 {
			return nil, false
		}
		byOrdinal[item.Ordinal] = item
	}
	maxOrdinal := uint64(len(canonical))
	for _, item := range current {
		if item.Ordinal == 0 {
			return nil, false
		}
		if item.Ordinal > maxOrdinal {
			maxOrdinal = item.Ordinal
		}
		if committed, exists := byOrdinal[item.Ordinal]; exists {
			if committed.ID != item.ID {
				return nil, false
			}
			canonicalTerminalTool := committed.Kind == ThreadItemTool && committed.Activity != nil && committed.Activity.Status != observation.ActivityStatusRunning
			if item.Live && !canonicalTerminalTool {
				committed.Live = true
				if item.Kind == ThreadItemThinking || item.Kind == ThreadItemAssistant {
					committed.Text = item.Text
				}
			}
			byOrdinal[item.Ordinal] = committed
			continue
		}
		byOrdinal[item.Ordinal] = item
	}
	result := make([]ThreadItem, 0, maxOrdinal)
	for ordinal := uint64(1); ordinal <= maxOrdinal; ordinal++ {
		item, exists := byOrdinal[ordinal]
		if !exists {
			return nil, false
		}
		result = append(result, item)
	}
	return cloneThreadItems(result), true
}

func mergeThreadInteractions(current, canonical []ThreadInteraction) []ThreadInteraction {
	merged := cloneThreadInteractions(canonical)
	index := make(map[string]int, len(merged))
	for itemIndex := range merged {
		index[merged[itemIndex].ID] = itemIndex
	}
	for _, local := range current {
		itemIndex, found := index[local.ID]
		if !found {
			index[local.ID] = len(merged)
			merged = append(merged, local)
			continue
		}
		persisted := merged[itemIndex]
		if local.Resolved && !persisted.Resolved || local.runID != "" && persisted.runID == "" {
			merged[itemIndex] = local
		}
	}
	return merged
}

func applyThreadInteractionsToItems(items []ThreadItem, interactions []ThreadInteraction) {
	byID := make(map[string]ThreadInteraction, len(interactions))
	byTool := make(map[string]ThreadInteraction, len(interactions))
	for _, interaction := range interactions {
		byID[interaction.ID] = interaction
		if interaction.ToolCallID != "" && (interaction.Kind == ThreadInteractionApproval || interaction.Kind == ThreadInteractionEffectRetry) {
			byTool[interaction.TurnID.String()+":"+interaction.ToolCallID] = interaction
		}
	}
	for index := range items {
		if items[index].Interaction != nil {
			if interaction, found := byID[items[index].Interaction.ID]; found {
				copy := interaction
				items[index].Interaction = &copy
			}
			continue
		}
		if items[index].Kind != ThreadItemTool || items[index].Activity == nil {
			continue
		}
		if interaction, found := byTool[items[index].TurnID.String()+":"+items[index].Activity.ToolID]; found {
			copy := interaction
			items[index].Interaction = &copy
		}
	}
}

func (service *threadRuntimeService) cancel(ctx context.Context, threadID identity.ThreadID, requestKey string) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	current := service.currentView(actor)
	if current.Activity != ThreadActivityActive {
		return current, nil
	}
	turnID, runID := current.TurnID, identity.RunID("")
	if err := actor.apply(ctx, func() error {
		turnID, runID = actor.state.turnID, actor.state.runID
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		TurnID   identity.TurnID   `json:"turn_id"`
	}{threadID, turnID})
	if err != nil {
		return ThreadView{}, err
	}
	return service.settleCancellation(ctx, actor, threadID, turnID, runID, cancellationRequest{
		EntryID: "cancel:" + requestKey, RequestKey: requestKey, RequestFingerprint: fingerprint,
	}, "user_stop", "user requested stop")
}

type cancellationRequest struct {
	EntryID            string
	RequestKey         string
	RequestFingerprint string
}

func (service *threadRuntimeService) settleCancellation(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, request cancellationRequest, source, reason string) (ThreadView, error) {
	repo, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	resolution := InteractionResolution{Accepted: false, Outcome: "cancelled"}
	resolutionPayload, err := json.Marshal(resolution)
	if err != nil {
		return ThreadView{}, err
	}
	terminalID := stableCancellationEntryID(threadID, turnID, runID)
	metadata := map[string]string{
		"run_id": runID.String(), "outcome": "cancelled",
		sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureCancelled,
	}
	outcomePayload, err := json.Marshal(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		TurnID   identity.TurnID   `json:"turn_id"`
		RunID    identity.RunID    `json:"run_id"`
		Terminal string            `json:"terminal"`
		Status   string            `json:"status"`
	}{ThreadID: threadID, TurnID: turnID, RunID: runID, Terminal: terminalID, Status: "cancelled"})
	if err != nil {
		return ThreadView{}, err
	}
	var runCancel context.CancelFunc
	var retryCancels []context.CancelFunc
	var waiters []chan InteractionResolution
	err = actor.apply(ctx, func() error {
		if actor.state.view.Activity != ThreadActivityActive || actor.state.turnID != turnID || actor.state.runID != runID {
			return nil
		}
		if _, err := repo.CancelTurn(ctx, sessiontree.CancelTurnRequest{
			ThreadID: threadID.String(), TurnID: turnID.String(), RunID: runID.String(),
			CancelEntryID: request.EntryID, TerminalEntryID: terminalID,
			RequestKey: request.RequestKey, RequestFingerprint: request.RequestFingerprint,
			OutcomeFingerprint:           sessiontree.StableHash(string(outcomePayload)),
			InteractionResolutionPayload: resolutionPayload, Metadata: metadata,
			ClearProviderState: true, Now: time.Now().UTC(),
		}); err != nil {
			return err
		}
		for _, interaction := range actor.state.view.Interactions {
			if interaction.Resolved || interaction.TurnID != turnID {
				continue
			}
			resolveThreadInteractionCanonical(&actor.state.view, interaction.ID, resolution)
			if waiter := actor.state.pendingInteractions[interaction.ID]; waiter != nil {
				waiters = append(waiters, waiter.resolution)
				delete(actor.state.pendingInteractions, interaction.ID)
			}
		}
		actor.state.view.ViewVersion++
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[request.RequestKey] = threadRuntimeRequest{fingerprint: request.RequestFingerprint}
		runCancel = actor.state.cancel
		for _, cancelRetry := range actor.state.effectRetryCancels {
			retryCancels = append(retryCancels, cancelRetry)
		}
		return nil
	})
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	if runCancel != nil {
		service.recordCancellation(threadID, turnID, runID, source, reason)
		runCancel()
	}
	for _, cancelRetry := range retryCancels {
		cancelRetry()
	}
	for _, waiter := range waiters {
		waiter <- resolution
	}
	service.finishSend(actor, turnID, runID, TurnResult{Status: TurnStatusCancelled}, context.Canceled)
	return service.currentView(actor), nil
}

func (service *threadRuntimeService) finishUnloadedCancellation(actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := cancellationRequest{}
	entries, err := service.host.store.repo.Entries(ctx, threadID.String())
	if err == nil {
		for index := len(entries) - 1; index >= 0; index-- {
			entry := entries[index]
			if entry.Type == sessiontree.EntryCancelRequested && entry.TurnID == turnID.String() && entry.RunID == runID.String() {
				request = cancellationRequest{EntryID: entry.ID, RequestKey: entry.RequestKey, RequestFingerprint: entry.RequestFingerprint}
				break
			}
		}
	}
	if request.EntryID == "" {
		request.RequestKey = "runtime-cancel:" + turnID.String()
		request.RequestFingerprint, _ = stableFingerprint(struct {
			ThreadID identity.ThreadID `json:"thread_id"`
			TurnID   identity.TurnID   `json:"turn_id"`
		}{threadID, turnID})
		request.EntryID = "cancel:" + request.RequestKey
	}
	if _, err := service.settleCancellation(ctx, actor, threadID, turnID, runID, request, "execution_context", "execution context cancelled"); err != nil && !errors.Is(err, sessiontree.ErrStaleAuthority) {
		service.host.store.reportBackgroundError(err)
	}
}

// finishUnloadedTerminal is used when no agent-harness runner remains to
// persist the terminal marker. Canonical settlement must precede the live
// actor transition so a preparation failure cannot look complete in memory
// while the journal still contains an active turn.
func (service *threadRuntimeService) finishUnloadedTerminal(actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, completed TurnResult, runErr error) {
	if completed.Status == TurnStatusCancelled || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		service.finishUnloadedCancellation(actor, threadID, turnID, runID)
		return
	}
	repo, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		service.host.store.reportBackgroundError(ErrUnsupportedStoreCapability)
		return
	}
	status := sessiontree.TurnFailed
	failureCode := sessiontree.TurnFailureEngineContract
	outcome := "failed"
	failureMessage := ""
	if runErr != nil {
		failureMessage = strings.TrimSpace(runErr.Error())
	}
	if failureMessage == "" {
		failureMessage = "thread runtime execution preparation failed"
	}
	terminalID := stableTerminalEntryID(threadID, turnID, runID)
	metadata := map[string]string{
		"run_id": runID.String(), "outcome": outcome,
		sessiontree.TurnFailureCodeMetadataKey: failureCode,
	}
	payload, err := json.Marshal(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		TurnID   identity.TurnID   `json:"turn_id"`
		RunID    identity.RunID    `json:"run_id"`
		Terminal string            `json:"terminal"`
		Status   string            `json:"status"`
		Error    string            `json:"error,omitempty"`
	}{ThreadID: threadID, TurnID: turnID, RunID: runID, Terminal: terminalID, Status: outcome, Error: failureMessage})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		ThreadID: threadID.String(), TurnID: turnID.String(), RunID: runID.String(),
		TerminalEntryID: terminalID, Status: status, Metadata: metadata, FailureMessage: failureMessage,
		OutcomeFingerprint: sessiontree.StableHash(string(payload)), Now: time.Now().UTC(), ClearProviderState: true,
	})
	if err != nil {
		// A runner may have committed its terminal before this fallback
		// reaches the journal. Those stale/conflicting claims are an expected
		// race, not a new background failure.
		if !errors.Is(err, sessiontree.ErrStaleAuthority) && !errors.Is(err, sessiontree.ErrRequestConflict) {
			service.host.store.reportBackgroundError(err)
		}
		return
	}
	service.finishSend(actor, turnID, runID, completed, runErr)
}

func stableCancellationEntryID(threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) string {
	hash := sessiontree.StableHash(strings.Join([]string{threadID.String(), turnID.String(), runID.String(), "cancelled"}, "\x00"))
	return "terminal-cancelled-" + hash[:24]
}

func stableTerminalEntryID(threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) string {
	hash := sessiontree.StableHash(strings.Join([]string{threadID.String(), turnID.String(), runID.String(), "terminal"}, "\x00"))
	return "terminal-" + hash[:24]
}

func (service *threadRuntimeService) respond(ctx context.Context, threadID identity.ThreadID, answers []InteractionAnswer, requestKey string) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	current := service.currentView(actor)
	byID := make(map[string]ThreadInteraction, len(current.Interactions))
	for _, interaction := range current.Interactions {
		byID[interaction.ID] = interaction
	}
	entries := make([]sessiontree.Entry, 0, len(answers))
	resolutions := make(map[string]InteractionResolution, len(answers))
	pendingAnswers := make([]InteractionAnswer, 0, len(answers))
	for _, answer := range answers {
		id := strings.TrimSpace(answer.InteractionID)
		interaction, ok := byID[id]
		if id == "" || !ok {
			return ThreadView{}, ErrRequestConflict
		}
		if interaction.Kind == ThreadInteractionApproval && answer.Approved == nil {
			return ThreadView{}, errors.New("approval answer is required")
		}
		if interaction.Kind == ThreadInteractionInput && len(answer.Input) == 0 {
			return ThreadView{}, errors.New("input answer is required")
		}
		resolution, resolutionErr := canonicalInteractionResolution(interaction, answer)
		if resolutionErr != nil {
			return ThreadView{}, resolutionErr
		}
		if interaction.Resolved {
			if interaction.Resolution == nil || !sameInteractionResolution(*interaction.Resolution, resolution) {
				return ThreadView{}, ErrRequestConflict
			}
			continue
		}
		resolutions[id] = resolution
		pendingAnswers = append(pendingAnswers, answer)
		payload, marshalErr := json.Marshal(resolution)
		if marshalErr != nil {
			return ThreadView{}, marshalErr
		}
		entries = append(entries, sessiontree.Entry{
			ID: "interaction-resolved:" + id, ThreadID: threadID.String(), TurnID: interaction.TurnID.String(),
			RunID: interaction.runID.String(),
			Type:  sessiontree.EntryInteractionDone, RequestKey: requestKey, Payload: payload,
		})
	}
	if len(entries) == 0 {
		return current, nil
	}
	canonicalBatch := make([]struct {
		InteractionID string                `json:"interaction_id"`
		Resolution    InteractionResolution `json:"resolution"`
	}, 0, len(pendingAnswers))
	for _, answer := range pendingAnswers {
		canonicalBatch = append(canonicalBatch, struct {
			InteractionID string                `json:"interaction_id"`
			Resolution    InteractionResolution `json:"resolution"`
		}{InteractionID: strings.TrimSpace(answer.InteractionID), Resolution: resolutions[strings.TrimSpace(answer.InteractionID)]})
	}
	answerFingerprint, err := stableFingerprint(canonicalBatch)
	if err != nil {
		return ThreadView{}, err
	}
	for index := range entries {
		entries[index].RequestFingerprint = answerFingerprint
	}
	now := time.Now().UTC()
	replayedRespond := false
	if err := actor.apply(ctx, func() error {
		if existing, found := actor.state.requestKeys[requestKey]; found {
			if existing.fingerprint != answerFingerprint {
				return &RequestConflictError{Operation: "respond", RequestID: requestKey, Err: ErrRequestConflict}
			}
			replayedRespond = true
			return nil
		}
		for interactionID := range resolutions {
			for _, interaction := range actor.state.view.Interactions {
				if interaction.ID != interactionID {
					continue
				}
				if interaction.Resolved {
					return ErrRequestConflict
				}
			}
		}
		if err := service.appendRuntimeFactsError(ctx, threadID, entries); err != nil {
			return err
		}
		for id, resolution := range resolutions {
			resolution.At = now
			resolveThreadInteractionCanonical(&actor.state.view, id, resolution)
		}
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: answerFingerprint}
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	if replayedRespond {
		return service.currentView(actor), nil
	}
	view := service.currentView(actor)
	service.publish(view)
	go service.resumeCanonicalInteractions(actor, threadID, resolutions, pendingAnswers, byID)
	return view, nil
}

func (service *threadRuntimeService) appendRuntimeFactsError(ctx context.Context, threadID identity.ThreadID, entries []sessiontree.Entry) error {
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	if _, err := writer.AppendRuntimeFacts(ctx, threadID.String(), entries); err == nil {
		return nil
	} else {
		for _, entry := range entries {
			existing, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entry.ID)
			if readErr != nil || existing.Type != entry.Type || existing.RequestKey != entry.RequestKey || existing.RequestFingerprint != entry.RequestFingerprint || string(existing.Payload) != string(entry.Payload) {
				return runtimeHostError(err)
			}
		}
	}
	return nil
}

func (service *threadRuntimeService) resumeCanonicalInteractions(actor *threadRuntimeState, threadID identity.ThreadID, resolutions map[string]InteractionResolution, answers []InteractionAnswer, interactions map[string]ThreadInteraction) {
	resumedLocally := make(map[string]bool, len(resolutions))
	_ = actor.apply(context.Background(), func() error {
		for id, resolution := range resolutions {
			if pending := actor.state.pendingInteractions[id]; pending != nil {
				pending.resolution <- resolution
				delete(actor.state.pendingInteractions, id)
				resumedLocally[id] = true
			}
		}
		return nil
	})
	recoverApproval := false
	for _, answer := range answers {
		interaction := interactions[answer.InteractionID]
		if interaction.Kind == ThreadInteractionInput {
			service.continueCanonicalInput(actor, threadID, interaction, answer)
		} else if interaction.Kind == ThreadInteractionApproval && !resumedLocally[interaction.ID] {
			recoverApproval = true
		}
	}
	if recoverApproval {
		service.recoverHydratedThread(threadID)
	}
}

func canonicalInteractionResolution(interaction ThreadInteraction, answer InteractionAnswer) (InteractionResolution, error) {
	resolution := InteractionResolution{Accepted: true, Approved: answer.Approved}
	if interaction.Kind != ThreadInteractionInput {
		return resolution, nil
	}
	if interaction.Input == nil {
		return InteractionResolution{}, ErrAuthorityCorrupt
	}
	questions := make(map[string]InputQuestion, len(interaction.Input.Questions))
	for _, question := range interaction.Input.Questions {
		questions[strings.TrimSpace(question.ID)] = question
	}
	public := make(map[string]string)
	for rawID, value := range answer.Input {
		id := strings.TrimSpace(rawID)
		question, ok := questions[id]
		if !ok || id == "" {
			return InteractionResolution{}, fmt.Errorf("%w: input answer targets unknown question %q", ErrRequestConflict, rawID)
		}
		if question.Secret {
			resolution.Redacted = true
			continue
		}
		public[id] = value
	}
	if len(public) != 0 {
		resolution.Input = public
	}
	return resolution, nil
}

func sameInteractionResolution(left, right InteractionResolution) bool {
	if left.Accepted != right.Accepted || left.Redacted != right.Redacted || left.Outcome != right.Outcome || !sameOptionalBool(left.Approved, right.Approved) {
		return false
	}
	return maps.Equal(left.Input, right.Input)
}

func sameOptionalBool(left, right *bool) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func resolveThreadInteractionCanonical(view *ThreadView, interactionID string, resolution InteractionResolution) {
	for index := range view.Interactions {
		if view.Interactions[index].ID != interactionID {
			continue
		}
		view.Interactions[index].Resolved = true
		copy := resolution
		view.Interactions[index].Resolution = &copy
		if resolution.Approved != nil {
			approved := *resolution.Approved
			view.Interactions[index].Approved = &approved
		}
		for itemIndex := range view.Items {
			if view.Items[itemIndex].Interaction != nil && view.Items[itemIndex].Interaction.ID == interactionID {
				resolved := view.Interactions[index]
				view.Items[itemIndex].Interaction = &resolved
			}
		}
		return
	}
}

func (service *threadRuntimeService) continueCanonicalInput(actor *threadRuntimeState, threadID identity.ThreadID, interaction ThreadInteraction, answer InteractionAnswer) {
	payload, err := json.Marshal(answer.Input)
	if err != nil {
		return
	}
	waitingRunID := interaction.runID
	if waitingRunID == "" {
		_ = actor.apply(context.Background(), func() error {
			if actor.state.turnID == interaction.TurnID {
				waitingRunID = actor.state.runID
			}
			return nil
		})
	}
	runID, err := service.host.idSource.NewRunID()
	if err != nil {
		return
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	executionDone := make(chan struct{})
	var previousExecution <-chan struct{}
	claimed := false
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != interaction.TurnID || actor.state.runID != waitingRunID || actor.state.view.Activity != ThreadActivityActive {
			return nil
		}
		actor.state.runID = runID
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.RunID = runID
		actor.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
		actor.state.view.ViewVersion++
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		previousExecution = actor.state.executionDone
		actor.state.executionDone = executionDone
		claimed = true
		return nil
	})
	if !claimed {
		cancel()
		close(executionDone)
		return
	}
	defer cancel()
	defer close(executionDone)
	if previousExecution != nil {
		select {
		case <-executionCtx.Done():
			return
		case <-previousExecution:
		}
	}
	continuationKey := "continue-input:" + interaction.ID
	agent, err := service.factory.Agent(executionCtx, AgentRequest{
		ThreadID: threadID, TurnID: interaction.TurnID, RequestKey: continuationKey,
		Input: UserInput{Text: string(payload)}, InteractionID: interaction.ID,
	})
	if err != nil || agent == nil {
		if err == nil {
			err = errors.New("thread runtime agent is unavailable")
		}
		service.finishUnloadedTerminal(actor, threadID, interaction.TurnID, runID, TurnResult{}, err)
		return
	}
	runner, err := service.host.turnRunner(executionCtx, threadID, service.executionAgent(actor, agent))
	if err != nil {
		service.finishUnloadedTerminal(actor, threadID, interaction.TurnID, runID, TurnResult{}, err)
		return
	}
	result, runErr := runner.ResumeInput(executionCtx, resumeInputRequest{
		TurnID: interaction.TurnID, WaitingRunID: waitingRunID, RunID: runID, Answer: string(payload),
		Options: turnExecutionRequest{
			LogicalRequestID: identity.LogicalRequestID(continuationKey), TurnID: interaction.TurnID, RunID: runID,
			Signals:           TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
			ManualCompactions: agent.manualCompactions,
		},
	})
	service.finishSend(actor, interaction.TurnID, runID, result, runErr)
}

func (service *threadRuntimeService) retry(ctx context.Context, threadID identity.ThreadID, sourceTurnID identity.TurnID, requestKey string) (ThreadView, error) {
	if _, err := identity.ParseTurnID(sourceTurnID.String()); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	sourceEntryID, sourceInput, err := service.retrySource(ctx, threadID, sourceTurnID)
	if err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(sourceTurnID)
	if err != nil {
		return ThreadView{}, err
	}
	turnID, runID := retryExecutionIDs(threadID, requestKey)
	executionCtx, cancel := context.WithCancel(context.Background())
	executionDone := make(chan struct{})
	actor := service.runtime(threadID)
	var result ThreadView
	var previousExecution <-chan struct{}
	var replayed bool
	err = actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[requestKey]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "retry", RequestID: requestKey, Err: ErrRequestConflict}
			}
			result = cloneThreadRuntimeView(actor.state.view)
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		actor.state.turnID = turnID
		actor.state.runID = runID
		actor.state.logicalRequestID = identity.LogicalRequestID(requestKey)
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		previousExecution = actor.state.executionDone
		actor.state.executionDone = executionDone
		actor.state.view.ViewVersion++
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.TurnID = turnID
		actor.state.view.RunID = runID
		actor.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
		actor.state.view.LastOutcome = nil
		actor.state.view.Failure = nil
		actor.state.view.Error = ""
		actor.finishLiveTextSegment()
		actor.state.view.AssistantDraft = ""
		actor.state.view.ThinkingDraft = ""
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		result = cloneThreadRuntimeView(actor.state.view)
		return nil
	})
	if err != nil {
		cancel()
		return ThreadView{}, err
	}
	if replayed {
		cancel()
		return result, nil
	}
	service.publish(result)
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: sourceInput, RetrySource: sourceTurnID})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID,
		Input: sourceInput, RetrySourceTurnID: sourceTurnID, RetrySourceEntryID: sourceEntryID,
		Signals: TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	go service.acceptAndExecutePreparedSend(executionCtx, actor, prepared, threadID, request, previousExecution, executionDone, requestKey, true)
	return result, nil
}

func retryExecutionIDs(threadID identity.ThreadID, requestKey string) (identity.TurnID, identity.RunID) {
	hash := sessiontree.StableHash(threadID.String() + "\x00retry\x00" + strings.TrimSpace(requestKey))
	return identity.TurnID("retry-turn-" + hash), identity.RunID("retry-run-" + hash)
}

func (service *threadRuntimeService) retrySource(ctx context.Context, threadID identity.ThreadID, sourceTurnID identity.TurnID) (string, UserInput, error) {
	reader, ok := service.host.store.repo.(sessiontree.CanonicalTurnReadRepo)
	if !ok {
		return "", UserInput{}, ErrUnsupportedStoreCapability
	}
	read, err := reader.ReadCanonicalTurn(ctx, threadID.String(), sourceTurnID.String())
	if err != nil {
		return "", UserInput{}, runtimeHostError(err)
	}
	sourceEntryID := ""
	if read.Turn.RetrySource != nil {
		if strings.TrimSpace(read.Turn.RetrySource.EntryID) == "" {
			return "", UserInput{}, ErrAuthorityCorrupt
		}
		sourceEntryID = read.Turn.RetrySource.EntryID
	} else {
		for _, item := range read.Turn.Entries {
			if item.Entry.Type == sessiontree.EntryUserMessage && item.Entry.Message.Role == session.User {
				sourceEntryID = item.Entry.ID
				break
			}
		}
	}
	if sourceEntryID == "" {
		return "", UserInput{}, errors.New("retry source turn has no canonical user input")
	}
	entry, err := service.host.store.repo.Entry(ctx, threadID.String(), sourceEntryID)
	if err != nil {
		return "", UserInput{}, runtimeHostError(err)
	}
	if entry.Type != sessiontree.EntryUserMessage || entry.Message.Role != session.User {
		return "", UserInput{}, ErrAuthorityCorrupt
	}
	return sourceEntryID, turnInputFromSessionMessage(entry.Message), nil
}

func (service *threadRuntimeService) rollbackProvisionalRetry(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, requestKey string) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID {
			return nil
		}
		delete(actor.state.requestKeys, requestKey)
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.TurnID = ""
		actor.state.view.RunID = ""
		actor.state.view.RunProgress = nil
		actor.state.view.ViewVersion++
		service.publish(cloneThreadRuntimeView(actor.state.view))
		return nil
	})
}

func (service *threadRuntimeService) Subscribe(ctx context.Context) (*WorkspaceSubscription, error) {
	if err := service.host.available(); err != nil {
		return nil, err
	}
	subscription := &WorkspaceSubscription{views: make(chan ThreadView, 64), done: make(chan struct{}), service: service}
	service.mu.Lock()
	service.subscribers[subscription] = struct{}{}
	service.mu.Unlock()
	go func() {
		<-ctx.Done()
		service.mu.Lock()
		delete(service.subscribers, subscription)
		service.mu.Unlock()
		subscription.Close()
	}()
	return subscription, nil
}

func (service *threadRuntimeService) publish(current ThreadView) {
	service.mu.Lock()
	defer service.mu.Unlock()
	threadID := current.ThreadID.String()
	if current.ViewVersion == 0 || current.ViewVersion <= service.published[threadID] {
		return
	}
	service.published[threadID] = current.ViewVersion
	for subscriber := range service.subscribers {
		view := cloneThreadRuntimeView(current)
		subscriber.mu.Lock()
		if subscriber.closed {
			subscriber.mu.Unlock()
			continue
		}
		select {
		case subscriber.views <- view:
		default:
			select {
			case <-subscriber.views:
			default:
			}
			select {
			case subscriber.views <- view:
			default:
			}
		}
		subscriber.mu.Unlock()
	}
}

func (service *threadRuntimeService) startNextQueued(actor *threadRuntimeState) {
	var next QueuedInput
	var threadID identity.ThreadID
	_ = actor.apply(context.Background(), func() error {
		if actor.state.view.Activity == ThreadActivityActive || len(actor.state.view.Queue) == 0 {
			return nil
		}
		next = actor.state.view.Queue[0]
		threadID = actor.state.view.ThreadID
		return nil
	})
	if next.RequestKey == "" || threadID == "" {
		return
	}
	go func() {
		if _, err := service.startAccepted(context.Background(), actor, threadID, next.Input, next.SupplementalContext, next.RequestKey, next.ID, "auto-promote:"+next.ID); err != nil {
			_ = actor.apply(context.Background(), func() error {
				actor.state.view.ViewVersion++
				actor.state.view.Activity = ThreadActivityIdle
				outcome := TurnOutcomeFailed
				actor.state.view.LastOutcome = &outcome
				service.publish(actor.state.view)
				return nil
			})
		}
	}()
}

func (service *threadRuntimeService) startAccepted(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, input UserInput, supplemental []TurnSupplementalContextItem, requestKey, promotedQueueID, promotionRequestKey string) (ThreadView, error) {
	inputFingerprint, err := stableFingerprint(input)
	if err != nil {
		return ThreadView{}, err
	}
	promotionFingerprint := ""
	if promotedQueueID != "" {
		promotionFingerprint, err = stableFingerprint(struct {
			QueueItemID string `json:"queue_item_id"`
		}{QueueItemID: promotedQueueID})
		if err != nil {
			return ThreadView{}, err
		}
	}
	turnID, runID, err := service.host.nextTurnRunIDs()
	if err != nil {
		return ThreadView{}, err
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	executionDone := make(chan struct{})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		SupplementalContext:         cloneTurnSupplementalContext(supplemental),
		InputFingerprint:            inputFingerprint,
		PromotedQueueID:             promotedQueueID,
		PromotionRequestKey:         promotionRequestKey,
		PromotionRequestFingerprint: promotionFingerprint,
		Signals:                     TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	var accepted acceptedTurn
	var result ThreadView
	var previousExecution <-chan struct{}
	err = actor.apply(ctx, func() error {
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		if promotedQueueID != "" {
			found := false
			for _, item := range actor.state.view.Queue {
				if item.ID == promotedQueueID {
					found = true
					break
				}
			}
			if !found {
				return ErrRequestConflict
			}
		}
		var acceptErr error
		accepted, acceptErr = service.acceptCanonicalTurn(ctx, threadID, request)
		if acceptErr != nil {
			return acceptErr
		}
		actor.state.turnID, actor.state.runID = turnID, runID
		actor.state.logicalRequestID = identity.LogicalRequestID(requestKey)
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		previousExecution = actor.state.executionDone
		actor.state.executionDone = executionDone
		actor.state.view.ViewVersion++
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.TurnID = turnID
		actor.state.view.RunID = runID
		actor.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
		actor.state.view.LastOutcome = nil
		actor.state.view.Failure = nil
		actor.state.view.Error = ""
		if promotedQueueID != "" {
			for index := range actor.state.view.Queue {
				if actor.state.view.Queue[index].ID == promotedQueueID {
					actor.state.view.Queue = append(actor.state.view.Queue[:index], actor.state.view.Queue[index+1:]...)
					break
				}
			}
		}
		actor.state.openTextSegmentID = ""
		actor.state.openTextKind = ""
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: "user:" + requestKey, TurnID: turnID, Kind: ThreadItemUser, Text: input.Text, CreatedAt: time.Now().UTC(), Attachments: cloneMessageAttachments(input.Attachments), References: append([]MessageReference(nil), input.References...)})
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: inputFingerprint}
		if promotionRequestKey != "" {
			actor.state.requestKeys[promotionRequestKey] = threadRuntimeRequest{fingerprint: promotionFingerprint}
		}
		result = cloneThreadRuntimeView(actor.state.view)
		return nil
	})
	if err != nil {
		cancel()
		return ThreadView{}, err
	}
	service.publish(result)
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{
		ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input,
	})
	go service.executePreparedSend(executionCtx, actor, prepared, accepted, request, previousExecution, executionDone)
	return result, nil
}

func cloneThreadRuntimeView(view ThreadView) ThreadView {
	view.AssistantDraft = liveThreadText(view.Items, ThreadItemAssistant)
	view.ThinkingDraft = liveThreadText(view.Items, ThreadItemThinking)
	view.Attention = AttentionSummary{}
	for _, interaction := range view.Interactions {
		if interaction.Resolved {
			continue
		}
		if interaction.Kind == ThreadInteractionApproval {
			view.Attention.ApprovalCount++
		}
		if interaction.Kind == ThreadInteractionInput {
			view.Attention.InputCount++
		}
	}
	view.Items = cloneThreadItems(view.Items)
	view.Queue = append([]QueuedInput(nil), view.Queue...)
	for index := range view.Queue {
		view.Queue[index].Input.Attachments = cloneMessageAttachments(view.Queue[index].Input.Attachments)
		view.Queue[index].Input.References = append([]MessageReference(nil), view.Queue[index].Input.References...)
		view.Queue[index].SupplementalContext = cloneTurnSupplementalContext(view.Queue[index].SupplementalContext)
	}
	view.Interactions = cloneThreadInteractions(view.Interactions)
	view.Failure = cloneThreadTurnFailure(view.Failure)
	view.RunProgress = cloneThreadRunProgress(view.RunProgress)
	return view
}

func cloneThreadRunProgress(progress *ThreadRunProgress) *ThreadRunProgress {
	if progress == nil {
		return nil
	}
	copy := *progress
	return &copy
}

func threadRuntimeViewNeedsAttention(view ThreadView) bool {
	for _, interaction := range view.Interactions {
		if interaction.Resolved {
			continue
		}
		if interaction.Kind == ThreadInteractionApproval || interaction.Kind == ThreadInteractionInput {
			return true
		}
	}
	return false
}

func liveThreadText(items []ThreadItem, kind ThreadItemKind) string {
	for index := len(items) - 1; index >= 0; index-- {
		if items[index].Kind == kind && items[index].Live {
			return items[index].Text
		}
	}
	return ""
}

func cloneThreadItems(items []ThreadItem) []ThreadItem {
	out := make([]ThreadItem, len(items))
	for index, item := range items {
		out[index] = item
		out[index].Attachments = cloneMessageAttachments(item.Attachments)
		out[index].References = append([]MessageReference(nil), item.References...)
		if item.Activity != nil {
			timeline := observation.CloneActivityTimeline(&observation.ActivityTimeline{Items: []observation.ActivityItem{*item.Activity}})
			out[index].Activity = &timeline.Items[0]
		}
		if item.Interaction != nil {
			interaction := *item.Interaction
			out[index].Interaction = &interaction
		}
	}
	return out
}

func cloneThreadInteractions(items []ThreadInteraction) []ThreadInteraction {
	return append([]ThreadInteraction(nil), items...)
}

func hydrateCanonicalQueue(ctx context.Context, repo sessiontree.JournalRepo, threadID identity.ThreadID) ([]QueuedInput, error) {
	entries, err := repo.Entries(ctx, threadID.String())
	if err != nil {
		return nil, runtimeHostError(err)
	}
	return canonicalQueueFromEntries(entries)
}

func canonicalQueueFromEntries(entries []sessiontree.Entry) ([]QueuedInput, error) {
	queue := make([]QueuedInput, 0)
	for _, entry := range entries {
		switch entry.Type {
		case sessiontree.EntryQueueAdded:
			var item QueuedInput
			if err := json.Unmarshal(entry.Payload, &item); err != nil {
				return nil, fmt.Errorf("decode canonical queue item %q: %w", entry.ID, err)
			}
			if item.ID == "" {
				item.ID = entry.ID
			}
			if !queuedInputExists(queue, item.ID) {
				queue = append(queue, item)
			}
		case sessiontree.EntryQueueReordered:
			var ids []string
			if err := json.Unmarshal(entry.Payload, &ids); err != nil {
				return nil, fmt.Errorf("decode canonical queue order %q: %w", entry.ID, err)
			}
			byID := make(map[string]QueuedInput, len(queue))
			for _, item := range queue {
				byID[item.ID] = item
			}
			next := make([]QueuedInput, 0, len(queue))
			for _, id := range ids {
				if item, ok := byID[id]; ok {
					next = append(next, item)
					delete(byID, id)
				}
			}
			if len(byID) != 0 {
				return nil, ErrAuthorityCorrupt
			}
			queue = next
		case sessiontree.EntryQueueDeleted, sessiontree.EntryQueuePromoted:
			var id string
			if err := json.Unmarshal(entry.Payload, &id); err != nil {
				return nil, fmt.Errorf("decode canonical queue removal %q: %w", entry.ID, err)
			}
			for index := range queue {
				if queue[index].ID == id {
					queue = append(queue[:index], queue[index+1:]...)
					break
				}
			}
		}
	}
	return queue, nil
}

func queuedInputExists(queue []QueuedInput, id string) bool {
	for _, item := range queue {
		if item.ID == id {
			return true
		}
	}
	return false
}

func hydrateThreadRuntimeItems(ctx context.Context, repo sessiontree.JournalRepo, threadID identity.ThreadID) ([]ThreadItem, []ThreadInteraction, error) {
	meta, err := repo.Thread(ctx, threadID.String())
	if err != nil {
		return nil, nil, runtimeHostError(err)
	}
	entries, err := repo.Path(ctx, threadID.String(), meta.LeafID)
	if err != nil {
		return nil, nil, runtimeHostError(err)
	}
	return threadRuntimeItemsFromEntries(entries)
}

func (service *threadRuntimeService) ensureThread(ctx context.Context, threadID identity.ThreadID) (sessiontree.ThreadMeta, error) {
	if err := service.host.available(); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if tombstones, ok := service.host.store.repo.(sessiontree.ThreadTombstoneRepo); ok {
				if _, tombstoneErr := tombstones.ThreadTombstone(ctx, threadID.String()); tombstoneErr == nil {
					return sessiontree.ThreadMeta{}, runtimeHostError(sessiontree.ErrThreadDeleted)
				}
			}
		}
		return sessiontree.ThreadMeta{}, runtimeHostError(err)
	}
	return meta, nil
}

func threadRuntimeItemsFromEntries(entries []sessiontree.Entry) ([]ThreadItem, []ThreadInteraction, error) {
	items := make([]ThreadItem, 0, len(entries))
	interactions := make([]ThreadInteraction, 0)
	terminalTurns := make(map[string]struct{})
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryTurnMarker {
			continue
		}
		switch entry.TurnStatus {
		case sessiontree.TurnCompleted, sessiontree.TurnFailed, sessiontree.TurnAborted:
			terminalTurns[entry.TurnID] = struct{}{}
		}
	}
	interactionIndex := make(map[string]int)
	itemIndex := make(map[string]int)
	toolItemIndex := make(map[string]int)
	thinkingCounts := make(map[identity.TurnID]int)
	assistantCounts := make(map[identity.TurnID]int)
	reasoningOpen := make(map[identity.TurnID]bool)
	lastReasoning := make(map[identity.TurnID]string)
	appendReasoning := func(turnID identity.TurnID, text string, createdAt time.Time) {
		if text == "" || reasoningOpen[turnID] && lastReasoning[turnID] == text {
			return
		}
		thinkingCounts[turnID]++
		items = appendThreadItem(items, ThreadItem{
			ID:     "thinking:" + turnID.String() + ":" + strconv.Itoa(thinkingCounts[turnID]),
			TurnID: turnID, Kind: ThreadItemThinking, Text: text, CreatedAt: createdAt,
		})
		reasoningOpen[turnID] = true
		lastReasoning[turnID] = text
	}
	for _, entry := range entries {
		turnID := identity.TurnID(entry.TurnID)
		switch entry.Type {
		case sessiontree.EntryUserMessage:
			items = appendThreadItem(items, ThreadItem{ID: entry.ID, TurnID: turnID, Kind: ThreadItemUser, Text: entry.Message.Content, CreatedAt: entry.CreatedAt, Attachments: runtimeMessageAttachments(entry.Message.Attachments), References: runtimeMessageReferences(entry.Message.References)})
		case sessiontree.EntryAssistantMessage:
			if entry.Message.Kind != "control_signal" {
				appendReasoning(turnID, entry.Message.Reasoning, entry.CreatedAt)
				if len(items) > 0 {
					previous := &items[len(items)-1]
					if previous.Kind == ThreadItemAssistant && previous.TurnID == turnID {
						previous.Text += entry.Message.Content
						continue
					}
				}
				assistantCounts[turnID]++
				items = appendThreadItem(items, ThreadItem{ID: "assistant:" + turnID.String() + ":" + strconv.Itoa(assistantCounts[turnID]), TurnID: turnID, Kind: ThreadItemAssistant, Text: entry.Message.Content, CreatedAt: entry.CreatedAt})
			}
		case sessiontree.EntryToolCall, sessiontree.EntryToolResult:
			if entry.Message.Kind == "control_signal" && entry.Message.ControlSignal != nil {
				signal := entry.Message.ControlSignal
				if signal.Disposition == string(SignalWaiting) {
					interaction := ThreadInteraction{ID: signal.CallID, TurnID: turnID, runID: identity.RunID(entry.RunID), Kind: ThreadInteractionInput, Input: inputPresentationFromControlSignal(signal.OutputText, signal.Payload)}
					interactionIndex[interaction.ID] = len(interactions)
					interactions = append(interactions, interaction)
					copy := interaction
					itemIndex[interaction.ID] = len(items)
					items = appendThreadItem(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: turnID, Kind: ThreadItemInteraction, Interaction: &copy})
				}
				continue
			}
			if entry.Type == sessiontree.EntryToolCall {
				appendReasoning(turnID, entry.Message.Reasoning, entry.CreatedAt)
			}
			activity := activityItemFromCanonicalEntry(entry)
			toolKey := turnID.String() + ":" + entry.Message.ToolCallID
			if previous, found := toolItemIndex[toolKey]; found && entry.Type == sessiontree.EntryToolResult {
				activity.Presentation = tools.MergeActivityPresentations(items[previous].Activity.Presentation, activity.Presentation)
				items[previous].Activity = &activity
				items[previous].Live = false
				reasoningOpen[turnID] = false
				continue
			}
			toolItemIndex[toolKey] = len(items)
			items = appendThreadItem(items, ThreadItem{ID: threadToolSegmentID(turnID, entry.Message.ToolCallID), TurnID: turnID, Kind: ThreadItemTool, Activity: &activity})
		case sessiontree.EntryEffectAttempt:
			attempt, decodeErr := sessiontree.DecodeCanonicalEffectAttempt(entry)
			if decodeErr != nil {
				return nil, nil, decodeErr
			}
			if sourceID := strings.TrimSpace(attempt.Invocation.SourceEffectAttemptID); sourceID != "" && effectAttemptSettlesRetry(attempt.State) {
				interactionID := "effect-retry:" + sourceID
				if index, found := interactionIndex[interactionID]; found {
					interactions[index].Resolved = true
					resolution := InteractionResolution{Accepted: true, Outcome: string(attempt.State), At: attempt.UpdatedAt}
					interactions[index].Resolution = &resolution
					if item, exists := itemIndex[interactionID]; exists {
						copy := interactions[index]
						items[item].Interaction = &copy
					}
				}
			}
			if attempt.State != sessiontree.EffectAttemptUnknown {
				continue
			}
			if _, terminal := terminalTurns[attempt.Invocation.TurnID]; terminal {
				continue
			}
			interaction := ThreadInteraction{
				ID: "effect-retry:" + attempt.EffectAttemptID, TurnID: identity.TurnID(attempt.Invocation.TurnID),
				runID: identity.RunID(attempt.Invocation.RunID), Kind: ThreadInteractionEffectRetry,
				ToolCallID:  attempt.Invocation.ToolCallID,
				EffectRetry: &EffectRetryPresentation{EffectAttemptID: attempt.EffectAttemptID, ToolCallID: attempt.Invocation.ToolCallID, ToolName: attempt.Invocation.ToolName},
			}
			interactionIndex[interaction.ID] = len(interactions)
			interactions = append(interactions, interaction)
			copy := interaction
			if toolIndex, exists := toolItemIndex[interaction.TurnID.String()+":"+interaction.ToolCallID]; exists {
				itemIndex[interaction.ID] = toolIndex
				items[toolIndex].Interaction = &copy
			}
		case sessiontree.EntryInteractionAsked:
			var interaction ThreadInteraction
			if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
				return nil, nil, fmt.Errorf("decode interaction %q: %w", entry.ID, err)
			}
			interaction.runID = identity.RunID(entry.RunID)
			if interaction.ID == "" {
				return nil, nil, ErrAuthorityCorrupt
			}
			if previous, ok := interactionIndex[interaction.ID]; ok {
				interactions[previous] = interaction
				if index, exists := itemIndex[interaction.ID]; exists {
					copy := interaction
					items[index].Interaction = &copy
				}
				continue
			}
			interactionIndex[interaction.ID] = len(interactions)
			interactions = append(interactions, interaction)
			copy := interaction
			if interaction.Kind == ThreadInteractionApproval {
				if toolIndex, exists := toolItemIndex[interaction.TurnID.String()+":"+interaction.ToolCallID]; exists {
					itemIndex[interaction.ID] = toolIndex
					items[toolIndex].Interaction = &copy
				}
				continue
			}
			itemIndex[interaction.ID] = len(items)
			items = appendThreadItem(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: interaction.TurnID, Kind: ThreadItemInteraction, Interaction: &copy})
		case sessiontree.EntryInteractionDone:
			interactionID := strings.TrimPrefix(entry.ID, "interaction-resolved:")
			index, ok := interactionIndex[interactionID]
			if !ok {
				continue
			}
			var resolution InteractionResolution
			if err := json.Unmarshal(entry.Payload, &resolution); err != nil {
				return nil, nil, fmt.Errorf("decode interaction resolution %q: %w", entry.ID, err)
			}
			resolution.At = entry.CreatedAt
			interactions[index].Resolved = true
			interactions[index].Resolution = &resolution
			if resolution.Approved != nil {
				approved := *resolution.Approved
				interactions[index].Approved = &approved
			}
			if index, exists := itemIndex[interactionID]; exists {
				copy := interactions[interactionIndex[interactionID]]
				items[index].Interaction = &copy
			}
		}
	}
	for interactionPosition := range interactions {
		interaction := interactions[interactionPosition]
		if interaction.Kind != ThreadInteractionApproval && interaction.Kind != ThreadInteractionEffectRetry {
			continue
		}
		if toolIndex, exists := toolItemIndex[interaction.TurnID.String()+":"+interaction.ToolCallID]; exists {
			copy := interaction
			items[toolIndex].Interaction = &copy
		}
	}
	return items, interactions, nil
}

func effectAttemptSettlesRetry(state sessiontree.EffectAttemptState) bool {
	switch state {
	case sessiontree.EffectAttemptCompleted, sessiontree.EffectAttemptFailed, sessiontree.EffectAttemptRejected, sessiontree.EffectAttemptCancelled:
		return true
	default:
		return false
	}
}

func activityItemFromCanonicalEntry(entry sessiontree.Entry) observation.ActivityItem {
	status := observation.ActivityStatusRunning
	if entry.Type == sessiontree.EntryToolResult {
		status = observation.ActivityStatusSuccess
		if entry.Message.ToolResult != nil {
			switch strings.ToLower(strings.TrimSpace(entry.Message.ToolResult.Status)) {
			case "error":
				status = observation.ActivityStatusError
			case "declined":
				status = observation.ActivityStatusDeclined
			case "canceled", "cancelled":
				status = observation.ActivityStatusCanceled
			}
		}
	}
	return observation.ActivityItem{ItemID: entry.ID, ToolID: entry.Message.ToolCallID, ToolName: entry.Message.ToolName, Kind: observation.ActivityKindTool, Status: status, Presentation: tools.CloneActivityPresentation(entry.Message.Activity)}
}

func threadToolSegmentID(turnID identity.TurnID, toolCallID string) string {
	return "tool:" + turnID.String() + ":" + strings.TrimSpace(toolCallID)
}

func inputPresentationFromControlSignal(text string, payload map[string]any) *InputPresentation {
	presentation := &InputPresentation{Summary: strings.TrimSpace(text)}
	if payload == nil {
		return presentation
	}
	if summary := strings.TrimSpace(fmt.Sprint(payload["summary"])); summary != "" && summary != "<nil>" {
		presentation.Summary = summary
	}
	if questions, ok := payload["questions"].([]any); ok {
		for _, raw := range questions {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			question := InputQuestion{
				ID:         optionalString(item["id"]),
				Prompt:     optionalString(item["question"]),
				Kind:       optionalString(item["response_mode"]),
				WriteLabel: optionalString(item["write_label"]),
				Secret:     item["is_secret"] == true,
			}
			if choices, ok := item["choices"].([]any); ok {
				for _, rawChoice := range choices {
					choice, ok := rawChoice.(map[string]any)
					if !ok {
						continue
					}
					label := optionalString(choice["label"])
					if label == "" {
						label = optionalString(choice["choice_id"])
					}
					if label != "" {
						question.Options = append(question.Options, label)
					}
				}
			}
			presentation.Questions = append(presentation.Questions, question)
		}
	}
	return presentation
}

func optionalString(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func hydrateThreadRuntimeLifecycle(ctx context.Context, repo sessiontree.JournalRepo, meta sessiontree.ThreadMeta) (identity.TurnID, identity.RunID, ThreadActivity, *TurnOutcome, *ThreadTurnFailure) {
	activity := ThreadActivityIdle
	if strings.TrimSpace(meta.LeafID) == "" {
		return "", "", activity, nil, nil
	}
	path, err := repo.Path(ctx, meta.ID, meta.LeafID)
	if err != nil {
		return "", "", activity, nil, nil
	}
	return threadRuntimeLifecycleFromEntries(meta, path)
}

func threadRuntimeLifecycleFromEntries(meta sessiontree.ThreadMeta, path []sessiontree.Entry) (identity.TurnID, identity.RunID, ThreadActivity, *TurnOutcome, *ThreadTurnFailure) {
	activity := ThreadActivityIdle
	lifecycle := sessionlifecycle.Derive(path, sessionlifecycle.PhaseTurn)
	var turnID identity.TurnID = identity.TurnID(lifecycle.LatestTurnID())
	var runID identity.RunID
	failureMessage := ""
	failureCode := ThreadTurnFailureCode("")
	for _, entry := range path {
		if entry.TurnID != turnID.String() {
			continue
		}
		if value := strings.TrimSpace(entry.RunID); value != "" {
			runID = identity.RunID(value)
		} else if value := strings.TrimSpace(entry.Metadata["run_id"]); value != "" {
			runID = identity.RunID(value)
		}
		if entry.Type == sessiontree.EntryRunFailure {
			failureMessage = strings.TrimSpace(entry.Error)
		}
		if failureMessage == "" {
			failureMessage = strings.TrimSpace(entry.Metadata["failure_reason"])
		}
		if value := strings.TrimSpace(entry.Metadata[sessiontree.TurnFailureCodeMetadataKey]); value != "" {
			failureCode = ThreadTurnFailureCode(value)
		}
	}
	var outcome *TurnOutcome
	switch lifecycle.Status() {
	case sessionlifecycle.StatusRunning, sessionlifecycle.StatusWaiting:
		activity = ThreadActivityActive
	case sessionlifecycle.StatusCompleted:
		value := TurnOutcomeCompleted
		outcome = &value
	case sessionlifecycle.StatusFailed:
		value := TurnOutcomeFailed
		outcome = &value
	case sessionlifecycle.StatusInterrupted:
		value := TurnOutcomeInterrupted
		outcome = &value
	case sessionlifecycle.StatusCancelled:
		value := TurnOutcomeCancelled
		outcome = &value
	}
	var failure *ThreadTurnFailure
	if failureMessage != "" {
		if !failureCode.Valid() {
			failureCode = ThreadTurnFailureLegacyUnclassified
		}
		failure = &ThreadTurnFailure{Code: failureCode, Message: failureMessage}
	}
	return turnID, runID, activity, outcome, failure
}

func cloneThreadTurnFailure(failure *ThreadTurnFailure) *ThreadTurnFailure {
	if failure == nil {
		return nil
	}
	cloned := *failure
	return &cloned
}

func threadTurnFailureMessage(failure *ThreadTurnFailure) string {
	if failure == nil {
		return ""
	}
	return strings.TrimSpace(failure.Message)
}
