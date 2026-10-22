package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/agentharness"
	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/control"
	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/internal/providerattempt"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	publicprovider "github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

// TurnKind distinguishes conversation, context-only, and Goal continuation turns.
// The zero value retains the ordinary conversation contract.
type TurnKind string

const TurnKindContextCompaction TurnKind = "context_compaction"

// TurnKindGoalContinuation prepares context and continues an unfinished Goal
// without admitting a user message.
const TurnKindGoalContinuation TurnKind = "goal_continuation"

// CompactContextInput admits idle-only compaction with optional Goal continuation.
type CompactContextInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
	// ContinueGoal continues an active Goal after successful compaction or noop.
	ContinueGoal bool `json:"continue_goal,omitempty"`
}

// ThreadContextCompactor is an optional capability of the typed thread service.
// Acceptance returns immediately; execution is observed through the workspace
// subscription and canonical context snapshot.
type ThreadContextCompactor interface {
	CompactContext(context.Context, CompactContextInput) (ThreadView, error)
}

// AgentRequest identifies an admitted execution resolved outside the thread lock.
// TurnKind tells the factory whether this execution has canonical user input.
type AgentRequest struct {
	TurnKind           TurnKind
	ThreadID           identity.ThreadID
	TurnID             identity.TurnID
	RequestKey         string
	Input              UserInput
	CanonicalTurnInput UserInput
	RetrySource        identity.TurnID
	InteractionID      string
	EffectAttemptID    string
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
	// SourceItemID pins a committed assistant item on the active path.
	// It is mutually exclusive with SourceEntryID; unknown items are rejected.
	SourceItemID    string            `json:"source_item_id,omitempty"`
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

// CancelMode selects when an accepted stop seals in-flight effects.
type CancelMode string

const (
	CancelModeImmediate CancelMode = "immediate"
	CancelModeGraceful  CancelMode = "graceful"
)

// ThreadCancellation is a canonical stop request for the indicated execution.
// Active views with this fact are stopping; terminal views retain its provenance.
type ThreadCancellation struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	Source      string            `json:"source"`
	Mode        CancelMode        `json:"mode"`
	TurnID      identity.TurnID   `json:"turn_id"`
	RunID       identity.RunID    `json:"run_id"`
	RequestedAt time.Time         `json:"requested_at"`
}

type CancelInput struct {
	// IncludeDescendants also admits a stop for every existing child in the
	// selected subtree. Independent threads and pending inputs are preserved.
	IncludeDescendants bool `json:"include_descendants,omitempty"`
	// Mode defaults to immediate. Graceful stops admit immediately and allow
	// in-flight tools up to five seconds to commit their confirmed results.
	Mode       CancelMode        `json:"mode,omitempty"`
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

type RetryInput struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	SourceTurnID identity.TurnID   `json:"source_turn_id"`
	RequestKey   RequestKey        `json:"request_key"`
}

type ReorderQueueInput struct {
	ThreadID       identity.ThreadID `json:"thread_id"`
	OrderedItemIDs []string          `json:"ordered_item_ids"`
	RequestKey     RequestKey        `json:"request_key"`
}

// DeleteQueuedInput removes pending input. ExpectedInputSHA256 optionally requires
// the complete pending input to match the lowercase SHA-256 hex digest of its
// encoding/json representation. Missing, started or changed input then returns
// ErrRequestConflict; exact retries of a committed deletion succeed.
type DeleteQueuedInput struct {
	ThreadID            identity.ThreadID `json:"thread_id"`
	QueueItemID         string            `json:"queue_item_id"`
	RequestKey          RequestKey        `json:"request_key"`
	ExpectedInputSHA256 string            `json:"expected_input_sha256,omitempty"`
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
	Turns   []ThreadTurn `json:"turns"`
	Items   []ThreadItem `json:"items"`
	Before  string       `json:"before,omitempty"`
	HasMore bool         `json:"has_more,omitempty"`
}

type ThreadSummary struct {
	TurnKind        TurnKind            `json:"turn_kind,omitempty"`
	Cancellation    *ThreadCancellation `json:"cancellation,omitempty"`
	ID              identity.ThreadID   `json:"id"`
	ParentThreadID  identity.ThreadID   `json:"parent_thread_id,omitempty"`
	ParentTurnID    identity.TurnID     `json:"parent_turn_id,omitempty"`
	TaskName        string              `json:"task_name,omitempty"`
	TaskDescription string              `json:"task_description,omitempty"`
	HostProfileRef  string              `json:"host_profile_ref,omitempty"`
	ForkMode        string              `json:"fork_mode,omitempty"`
	Title           string              `json:"title,omitempty"`
	TitleStatus     ThreadTitleStatus   `json:"title_status,omitempty"`
	// TitleGeneration orders the complete Title/TitleStatus snapshot independently
	// of runtime view versions and activity timestamps. Zero means unset. A new
	// automatic attempt or manual title advances the generation; within one
	// generation, pending may settle to ready or failed, never the reverse.
	TitleGeneration int64              `json:"title_generation"`
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
	Transport *observation.ProviderTransportProgress `json:"transport,omitempty"`
	Request   *ProviderRequestProgress               `json:"request,omitempty"`
	Retry     *ProviderRetryProgress                 `json:"retry,omitempty"`
	Phase     ThreadRunPhase                         `json:"phase"`
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
	ThreadItemGoal     ThreadItemKind = "goal"
	ThreadItemUser     ThreadItemKind = "user"
	ThreadItemSteering ThreadItemKind = "steering"
	// ThreadItemThinking is one ordered reasoning segment in a thread view.
	ThreadItemThinking    ThreadItemKind = "thinking"
	ThreadItemAssistant   ThreadItemKind = "assistant"
	ThreadItemTool        ThreadItemKind = "tool"
	ThreadItemInteraction ThreadItemKind = "interaction"
)

// ThreadItem is a directly renderable current-view item.
type ThreadItem struct {
	Goal             *ThreadGoalChange       `json:"goal,omitempty"`
	Steering         *ThreadSteering         `json:"steering,omitempty"`
	Sources          []publicprovider.Source `json:"sources,omitempty"`
	contentSourceID  string
	sourceEntryIDs   []string
	activityEntryIDs []string
	activityOmitted  bool
	textOmitted      bool
	liveBody         outputbody.Reader
	textBytes        int
	controlTextBytes int
	controlTextStart int
	textNonSpace     bool
	canonicalText    bool
	textLeafID       string
	textSummary      string
	textSummaryTail  bool
	ID               string          `json:"id"`
	TurnID           identity.TurnID `json:"turn_id,omitempty"`
	RunID            identity.RunID  `json:"run_id,omitempty"`
	// CanonicalOrdinal is the one-based source position on the queried Path.
	// Zero denotes an uncommitted live item, not a timestamp or durable cursor.
	CanonicalOrdinal uint64 `json:"canonical_ordinal,omitempty"`
	// Ordinal is the stable one-based position in the thread presentation.
	Ordinal uint64          `json:"ordinal"`
	Kind    ThreadItemKind  `json:"kind"`
	Text    string          `json:"text,omitempty"`
	Body    *ThreadItemBody `json:"body,omitempty"`
	// Live reports that this segment may still grow in place.
	Live        bool                      `json:"live,omitempty"`
	CreatedAt   time.Time                 `json:"created_at,omitempty"`
	Attachments []MessageAttachment       `json:"attachments,omitempty"`
	References  []MessageReference        `json:"references,omitempty"`
	Context     []MessageContextItem      `json:"context,omitempty"`
	Activity    *observation.ActivityItem `json:"activity,omitempty"`
	// ActivityBodies contains ordered canonical presentations omitted by a bounded
	// window. Read and decode each complete field, then merge in this order.
	ActivityBodies []ThreadActivityBody `json:"activity_bodies,omitempty"`
	Interaction    *ThreadInteraction   `json:"interaction,omitempty"`
}

type ThreadInteractionKind string

const (
	ThreadInteractionApproval ThreadInteractionKind = "approval"
	ThreadInteractionInput    ThreadInteractionKind = "input"
)

// ThreadInteraction is an unresolved or resolved action embedded in one item.
type ThreadInteraction struct {
	toolRequestedInput bool

	ID         string                 `json:"id"`
	TurnID     identity.TurnID        `json:"turn_id"`
	RunID      identity.RunID         `json:"run_id"`
	Kind       ThreadInteractionKind  `json:"kind"`
	ToolCallID string                 `json:"tool_call_id,omitempty"`
	Resolved   bool                   `json:"resolved,omitempty"`
	Approved   *bool                  `json:"approved,omitempty"`
	Approval   *ApprovalPresentation  `json:"approval,omitempty"`
	Input      *InputPresentation     `json:"input,omitempty"`
	Resolution *InteractionResolution `json:"resolution,omitempty"`
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
	// ReasonCode, RequiredFromUser and EvidenceRefs preserve validated ask_user
	// intent. Evidence references express association, never access or permission.
	ReasonCode       string          `json:"reason_code,omitempty"`
	RequiredFromUser []string        `json:"required_from_user,omitempty"`
	EvidenceRefs     []string        `json:"evidence_refs,omitempty"`
	Summary          string          `json:"summary"`
	Questions        []InputQuestion `json:"questions"`
}

// InputChoice preserves display metadata alongside the established v7 answer value.
// ChoiceID identifies the original option; clients submit Value through Respond.
type InputChoice struct {
	ChoiceID    string `json:"choice_id,omitempty"`
	Value       string `json:"value"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type InputQuestion struct {
	ID     string `json:"id"`
	Prompt string `json:"prompt"`
	Kind   string `json:"kind"`
	// Options retains the v7 answer values, in the same order as Choices when present.
	Options          []string `json:"options,omitempty"`
	WriteLabel       string   `json:"write_label,omitempty"`
	Secret           bool     `json:"secret,omitempty"`
	Header           string   `json:"header,omitempty"`
	WritePlaceholder string   `json:"write_placeholder,omitempty"`
	// ChoicesExhaustive is absent when the source did not declare it.
	ChoicesExhaustive *bool         `json:"choices_exhaustive,omitempty"`
	Choices           []InputChoice `json:"choices,omitempty"`
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
	// Context is the complete canonical context at this view version, including an empty snapshot.
	Context *ThreadContextSnapshot `json:"context,omitempty"`
	Goal    *ThreadGoal            `json:"goal,omitempty"`
	// ParentEvents is a bounded, explicitly paginated receipt view, separate from streamed item ordinals.
	ParentEvents     *ParentEventPage        `json:"parent_events,omitempty"`
	ToolCallPreviews []ThreadToolCallPreview `json:"tool_call_previews,omitempty"`
	Turns            []ThreadTurn            `json:"turns"`
	TurnKind         TurnKind                `json:"turn_kind,omitempty"`
	Cancellation     *ThreadCancellation     `json:"cancellation,omitempty"`
	RestoredInputs   []RestoredInput         `json:"restored_inputs,omitempty"`
	ThreadID         identity.ThreadID       `json:"thread_id"`
	ViewVersion      uint64                  `json:"view_version"`
	Activity         ThreadActivity          `json:"activity"`
	Attention        AttentionSummary        `json:"attention"`
	LastOutcome      *TurnOutcome            `json:"last_outcome,omitempty"`
	Failure          *ThreadTurnFailure      `json:"failure,omitempty"`
	TurnID           identity.TurnID         `json:"turn_id,omitempty"`
	RunID            identity.RunID          `json:"run_id,omitempty"`
	RunProgress      *ThreadRunProgress      `json:"run_progress,omitempty"`
	Items            []ThreadItem            `json:"items,omitempty"`
	Queue            []QueuedInput           `json:"queue,omitempty"`
	Interactions     []ThreadInteraction     `json:"interactions,omitempty"`
}

// ThreadContextSnapshot is the canonical context and compaction projection for
// one thread. Compactions contain one latest lifecycle record per operation.
type ThreadContextSnapshot struct {
	ModelContext *ThreadModelContext        `json:"model_context,omitempty"`
	ContextUsage *ThreadContextUsage        `json:"context_usage,omitempty"`
	Model        ThreadContextModel         `json:"model,omitempty"`
	Policy       ThreadContextPolicy        `json:"policy,omitempty"`
	Usage        *observation.ContextStatus `json:"usage,omitempty"`
	UsageTotals  *ThreadTokenUsageTotals    `json:"usage_totals,omitempty"`
	Compactions  []ThreadContextCompaction  `json:"compactions,omitempty"`
	UpdatedAt    time.Time                  `json:"updated_at,omitempty"`
}

// ThreadContextUsage separates model-confirmed input usage from the current
// request estimate. Confirmed describes a completed provider request, not a
// count of all tokens currently visible in the conversation. Nil samples mean
// unavailable; a non-nil empty snapshot explicitly clears obsolete samples.
type ThreadContextUsage struct {
	Confirmed *observation.ContextStatus `json:"confirmed,omitempty"`
	Estimate  *observation.ContextStatus `json:"estimate,omitempty"`
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
	ModelMaxOutputTokens int64 `json:"model_max_output_tokens,omitempty"`
	ContextWindowTokens  int64 `json:"context_window_tokens,omitempty"`
	MaxOutputTokens      int64 `json:"max_output_tokens,omitempty"`
	ReservedOutputTokens int64 `json:"reserved_output_tokens,omitempty"`
}

type ThreadContextCompaction struct {
	Decision *observation.CompactionDecision `json:"decision,omitempty"`
	// CanonicalOrdinal is the first lifecycle entry's one-based position on the current Path.
	// Terminal updates retain this position; a fork projects its own Path order.
	CanonicalOrdinal    uint64            `json:"canonical_ordinal"`
	AfterItemID         string            `json:"after_item_id,omitempty"`
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
	ReorderQueue(context.Context, ReorderQueueInput) (ThreadView, error)
	DeleteQueued(context.Context, DeleteQueuedInput) (ThreadView, error)
	PromoteQueued(context.Context, PromoteQueuedInput) (ThreadView, error)
	ImportPendingInputs(context.Context, ImportPendingInputsInput) (ImportResult, error)
	Subscribe(context.Context) (*WorkspaceSubscription, error)
}

var _ ThreadContextReader = (*threadRuntimeService)(nil)

type threadRuntimeService struct {
	host                *Host
	factory             AgentFactory
	mu                  sync.Mutex
	subscribers         map[*WorkspaceSubscription]struct{}
	updateSubscribers   map[*ThreadUpdateSubscription]struct{}
	subscriptionsClosed bool
	published           map[string]uint64
	cancellationMu      sync.Mutex
	cancellations       []threadCancellationDiagnostic
	runtimesMu          sync.Mutex
	runtimes            map[string]*threadRuntimeState
	closed              bool
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
	mu       sync.Mutex
	views    chan ThreadView
	done     chan struct{}
	closed   bool
	closeErr error
	changed  chan struct{}
	wakeOnly bool
	service  *threadRuntimeService
}

func (subscription *WorkspaceSubscription) Next(ctx context.Context) (ThreadView, error) {
	if subscription == nil {
		return ThreadView{}, errors.New("thread runtime subscription is required")
	}
	select {
	case <-ctx.Done():
		return ThreadView{}, ctx.Err()
	case <-subscription.done:
		subscription.mu.Lock()
		err := subscription.closeErr
		subscription.mu.Unlock()
		return ThreadView{}, err
	case view := <-subscription.views:
		return cloneThreadRuntimeView(view), nil
	}
}

func (subscription *WorkspaceSubscription) Close() { subscription.closeWithError(ErrHostClosed) }
func (subscription *WorkspaceSubscription) closeWithError(err error) {
	if subscription == nil {
		return
	}
	subscription.mu.Lock()
	subscription.closeLockedWithError(err)
	service := subscription.service
	subscription.mu.Unlock()
	if service != nil {
		service.mu.Lock()
		delete(service.subscribers, subscription)
		service.mu.Unlock()
	}
}
func (subscription *WorkspaceSubscription) closeLocked() {
	subscription.closeLockedWithError(ErrHostClosed)
}
func (subscription *WorkspaceSubscription) closeLockedWithError(err error) {
	if subscription.closed {
		return
	}
	subscription.closed = true
	subscription.closeErr = err
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
	view, err := service.createCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) createCommand(ctx context.Context, in CreateThreadInput, mode threadResultMode) (ThreadView, error) {
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
	if err := ctx.Err(); err != nil {
		return ThreadView{}, err
	}
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
		return service.completedCommandResult(ctx, identity.ThreadID(origin.Thread.ID), mode)
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
	return service.completedCommandResult(ctx, threadID, mode)
}

func (service *threadRuntimeService) Fork(ctx context.Context, in ForkThreadInput) (ThreadView, error) {
	view, err := service.forkCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) forkCommand(ctx context.Context, in ForkThreadInput, mode threadResultMode) (ThreadView, error) {
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
		Item            string            `json:"item,omitempty"`
		ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
		ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
		TaskName        string            `json:"task_name,omitempty"`
		TaskDescription string            `json:"task_description,omitempty"`
		HostProfileRef  string            `json:"host_profile_ref,omitempty"`
		ForkMode        string            `json:"fork_mode,omitempty"`
	}{in.SourceThreadID, strings.TrimSpace(in.SourceEntryID), strings.TrimSpace(in.SourceItemID), in.ParentThreadID, in.ParentTurnID, strings.TrimSpace(in.TaskName), strings.TrimSpace(in.TaskDescription), strings.TrimSpace(in.HostProfileRef), strings.TrimSpace(in.ForkMode)})
	if err != nil {
		return ThreadView{}, err
	}
	service.host.mutationMu.Lock()
	defer service.host.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ThreadView{}, err
	}
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
		return service.completedCommandResult(ctx, identity.ThreadID(origin.Thread.ID), mode)
	}
	if !errors.Is(originErr, sessiontree.ErrThreadNotFound) {
		return ThreadView{}, runtimeHostError(originErr)
	}
	source, err := service.host.store.repo.Thread(ctx, in.SourceThreadID.String())
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	entryID := strings.TrimSpace(in.SourceEntryID)
	if itemID := strings.TrimSpace(in.SourceItemID); itemID != "" {
		if entryID != "" {
			return ThreadView{}, fmt.Errorf("source_item_id and source_entry_id are mutually exclusive")
		}
		path, err := service.host.store.repo.Path(ctx, source.ID, source.LeafID)
		if err != nil {
			return ThreadView{}, runtimeHostError(err)
		}
		items, _, err := threadRuntimeItemsFromEntries(path)
		if err != nil {
			return ThreadView{}, err
		}
		turns, err := threadRuntimeTurnsFromEntries(path, items)
		if err != nil {
			return ThreadView{}, err
		}
		for _, item := range items {
			if item.ID == itemID && item.Kind == ThreadItemAssistant && len(item.sourceEntryIDs) > 0 {
				entryID = item.sourceEntryIDs[len(item.sourceEntryIDs)-1]
				// The last final item includes its canonical completion boundary.
				// Earlier items retain their exact cut without later answer content.
				for _, turn := range turns {
					if turn.TurnID != item.TurnID || turn.RunID != item.RunID || !turn.FinalBoundaryComplete || len(turn.FinalItemIDs) == 0 || turn.FinalItemIDs[len(turn.FinalItemIDs)-1] != item.ID {
						continue
					}
					for _, entry := range path {
						if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnCompleted && entry.TurnID == item.TurnID.String() && entry.RunID == item.RunID.String() {
							entryID = entry.ID
						}
					}
				}
				break
			}
		}
		if entryID == "" {
			return ThreadView{}, fmt.Errorf("source_item_id is not a committed assistant item")
		}
	}
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
	return service.completedCommandResult(ctx, threadID, mode)
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
		if committed {
			drain.actor.releaseDeletedState()
		}
		drain.actor.deleted = committed
		drain.actor.deleting = false
		drain.actor.mu.Unlock()
	}
	service.runtimesMu.Lock()
	for _, drain := range drains {
		if _, committed := deleted[drain.actor.threadID]; committed && service.runtimes[drain.actor.threadID] == drain.actor {
			delete(service.runtimes, drain.actor.threadID)
		}
	}
	service.runtimesMu.Unlock()
	service.mu.Lock()
	for threadID := range deleted {
		delete(service.published, threadID)
	}
	for subscription := range service.subscribers {
		select {
		case subscription.changed <- struct{}{}:
		default:
		}
	}
	service.mu.Unlock()
	service.invalidateUpdates()
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
			drain.actor.state.cancel(context.Canceled)
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
	view, err := service.setTitleCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) setTitleCommand(ctx context.Context, in SetTitleInput, mode threadResultMode) (ThreadView, error) {
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
	return service.completedCommandResult(ctx, in.ThreadID, mode)
}

func (service *threadRuntimeService) List(ctx context.Context, scope ThreadScope) ([]ThreadSummary, error) {

	metas, err := sessiontree.ListThreads(ctx, service.host.store.repo, sessiontree.ListThreadsOptions{IncludeArchived: false, RootOnly: scope.ParentID == nil})
	if err != nil {
		return nil, runtimeHostError(err)
	}
	out := make([]ThreadSummary, 0, len(metas))
	for _, meta := range metas {
		if scope.ParentID != nil && meta.ParentThreadID != scope.ParentID.String() {
			continue
		}
		if summary, found, err := service.activeThreadSummary(ctx, meta); err != nil {
			return nil, err
		} else if found {
			out = append(out, summary)
			continue
		}
		summary, err := service.coldThreadSummary(ctx, meta)
		if err != nil {
			return nil, err
		}
		out = append(out, summary)
	}
	return out, nil
}

type canonicalQueueCountProjection struct{ ids []string }

func canonicalQueueCountFromEntries(entries []sessiontree.Entry) (int, error) {
	projection := canonicalQueueCountProjection{}
	for _, entry := range entries {
		if err := projection.observe(entry); err != nil {
			return 0, err
		}
	}
	return len(projection.ids), nil
}
func (projection *canonicalQueueCountProjection) observe(entry sessiontree.Entry) error {
	switch entry.Type {
	case sessiontree.EntryQueueAdded:
		var item struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(entry.Payload, &item); err != nil {
			return fmt.Errorf("decode canonical queue item %q: %w", entry.ID, err)
		}
		if item.ID == "" {
			item.ID = entry.ID
		}
		if !slices.Contains(projection.ids, item.ID) {
			projection.ids = append(projection.ids, item.ID)
		}
	case sessiontree.EntryQueueReordered:
		var ids []string
		if err := json.Unmarshal(entry.Payload, &ids); err != nil {
			return fmt.Errorf("decode canonical queue order %q: %w", entry.ID, err)
		}
		if len(ids) != len(projection.ids) {
			return ErrAuthorityCorrupt
		}
		for _, id := range ids {
			if !slices.Contains(projection.ids, id) {
				return ErrAuthorityCorrupt
			}
		}
		projection.ids = append(projection.ids[:0], ids...)
	case sessiontree.EntryQueueDeleted, sessiontree.EntryQueuePromoted:
		var id string
		if err := json.Unmarshal(entry.Payload, &id); err != nil {
			return fmt.Errorf("decode canonical queue removal %q: %w", entry.ID, err)
		}
		for index := range projection.ids {
			if projection.ids[index] == id {
				projection.ids = append(projection.ids[:index], projection.ids[index+1:]...)
				break
			}
		}
	}

	return nil
}

func threadSummaryFromView(meta sessiontree.ThreadMeta, view ThreadView) ThreadSummary {
	failure := cloneThreadTurnFailure(view.Failure)
	summary := ThreadSummary{
		TurnKind:     view.TurnKind,
		Cancellation: cloneThreadCancellation(view.Cancellation),
		ID:           identity.ThreadID(meta.ID), ParentThreadID: identity.ThreadID(meta.ParentThreadID), ParentTurnID: identity.TurnID(meta.ParentTurnID),
		TaskName: meta.TaskName, TaskDescription: meta.TaskDescription, HostProfileRef: meta.HostProfileRef, ForkMode: meta.ForkMode,
		Title: meta.Title, TitleStatus: ThreadTitleStatus(meta.TitleStatus), TitleGeneration: meta.TitleGeneration, CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
		Activity: view.Activity, Attention: view.Attention, LastOutcome: view.LastOutcome, TurnID: view.TurnID,
		RunID: view.RunID, RunProgress: cloneThreadRunProgress(view.RunProgress),
		QueueCount: len(view.Queue), Failure: failure,
	}
	for index := len(view.Interactions) - 1; index >= 0; index-- {
		interaction := view.Interactions[index]
		if !interaction.Resolved && interaction.Kind == ThreadInteractionInput && interaction.Input != nil {
			summary.PendingInput = cloneInputPresentation(interaction.Input)
			break
		}
	}
	for index := len(view.Items) - 1; index >= 0; index-- {
		item := view.Items[index]
		if summary.LastItemAt.IsZero() && !item.CreatedAt.IsZero() {
			summary.LastItemAt = item.CreatedAt
		}
		text := strings.TrimSpace(item.Text)
		if item.textOmitted {
			text = item.textSummary
			if !item.textSummaryTail {
				text = strings.TrimRightFunc(text, unicode.IsSpace)
			}
		}
		if text != "" {
			summary.LastItemPreview = truncateThreadSummaryPreview(text, 160)
			if !item.CreatedAt.IsZero() {
				summary.LastItemAt = item.CreatedAt
			}
			break
		}
	}
	return summary
}

// activeThreadSummary reads only the existing actor. Listing must not decode
// canonical history before replacing it with these newer in-process facts.
func (service *threadRuntimeService) activeThreadSummary(ctx context.Context, meta sessiontree.ThreadMeta) (ThreadSummary, bool, error) {
	service.runtimesMu.Lock()
	actor := service.runtimes[meta.ID]
	service.runtimesMu.Unlock()
	if actor == nil {
		return ThreadSummary{}, false, nil
	}
	var summary ThreadSummary
	found := false
	err := actor.apply(ctx, func() error {
		if actor.state.contextErr != nil {
			return actor.state.contextErr
		}
		if actor.state.view.ViewVersion > 0 {
			summary = actor.threadSummary(meta)
			found = true
		}
		return nil
	})
	return summary, found, err
}

func truncateThreadSummaryPreview(value string, limit int) string {
	count := 0
	for offset := range value {
		if count == limit {
			return value[:offset]
		}
		count++
	}
	return value
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
	turns, err := threadRuntimeTurnsFromEntries(entries, items)
	if err != nil {
		return HistoryPage{}, err
	}
	result := HistoryPage{Turns: turns, Items: cloneThreadItems(items[start:end]), HasMore: start > 0}
	if result.HasMore {
		result.Before = items[start].ID
	}
	return result, nil
}

func (service *threadRuntimeService) Context(ctx context.Context, threadID identity.ThreadID) (ThreadContextSnapshot, error) {
	if service == nil || service.host == nil || service.host.store == nil {
		return ThreadContextSnapshot{}, ErrHostClosed
	}
	view, err := service.viewResult(ctx, threadID, threadFullResult)
	if err != nil {
		return ThreadContextSnapshot{}, err
	}
	return *view.Context, nil
}

func (service *threadRuntimeService) projectThreadContext(ctx context.Context, threadID identity.ThreadID, path []sessiontree.Entry) (ThreadContextSnapshot, error) {
	contextSnapshot, err := agentharness.ProjectThreadContext(path)
	if err != nil {
		return ThreadContextSnapshot{}, runtimeHostError(err)
	}
	anchors := make(map[string]string)
	if _, _, err := threadRuntimeItemsWithAnchors(path, anchors); err != nil {
		return ThreadContextSnapshot{}, err
	}
	ordinals := make(map[string]uint64)
	for i, entry := range path {
		if sessiontree.ThreadContextEntryKind(entry) != sessiontree.ThreadContextCompactionEntryKind {
			continue
		}
		compact, err := sessiontree.DecodeThreadContextCompactionEntry(entry)
		if err != nil {
			return ThreadContextSnapshot{}, err
		}
		if _, exists := ordinals[compact.OperationID]; !exists {
			ordinals[compact.OperationID] = uint64(i + 1)
		}
	}
	modelContext, err := service.readModelContext(ctx, threadID, path)
	if err != nil {
		return ThreadContextSnapshot{}, runtimeHostError(err)
	}
	return runtimeCanonicalContext(contextSnapshot, modelContext, anchors, ordinals), nil
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
	view, err := service.sendCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) sendCommand(ctx context.Context, in SendInput, mode threadResultMode) (ThreadView, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	supplemental, err := normalizeTurnSupplementalContext(in.SupplementalContext)
	if err != nil {
		return ThreadView{}, err
	}
	return service.sendResult(ctx, in.ThreadID, in.Input, supplemental, key, mode)
}

func (service *threadRuntimeService) Cancel(ctx context.Context, in CancelInput) (ThreadView, error) {
	view, err := service.cancelCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) cancelCommand(ctx context.Context, in CancelInput, mode threadResultMode) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if in.Mode != "" && in.Mode != CancelModeImmediate && in.Mode != CancelModeGraceful {
		return ThreadView{}, fmt.Errorf("invalid cancellation mode %q", in.Mode)
	}
	if !in.IncludeDescendants {
		return service.cancelResult(ctx, in.ThreadID, key, in.Mode, mode)
	}
	// Serialize the subtree snapshot and stop admission with child creation.
	// No provider work or effect settlement is awaited under this lock.
	service.host.mutationMu.Lock()
	defer service.host.mutationMu.Unlock()
	ids, err := service.subtreeThreadIDs(ctx, in.ThreadID)
	if err != nil {
		return ThreadView{}, err
	}
	view, err := service.cancelResult(ctx, in.ThreadID, key, in.Mode, mode)
	if err != nil {
		return ThreadView{}, err
	}
	for _, id := range ids {
		if id == in.ThreadID {
			continue
		}
		if _, err := service.cancelResult(ctx, id, key+":child:"+id.String(), in.Mode, threadAckResult); err != nil {
			return ThreadView{}, err
		}
	}
	return view, nil
}

func (service *threadRuntimeService) Retry(ctx context.Context, in RetryInput) (ThreadView, error) {
	view, err := service.retryCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) retryCommand(ctx context.Context, in RetryInput, mode threadResultMode) (ThreadView, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	return service.retryResult(ctx, in.ThreadID, in.SourceTurnID, key, mode)
}

func (service *threadRuntimeService) Respond(ctx context.Context, in RespondInput) (ThreadView, error) {
	view, err := service.respondCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) respondCommand(ctx context.Context, in RespondInput, mode threadResultMode) (ThreadView, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
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
	return service.respondResult(ctx, in.ThreadID, answers, key, mode)
}

func (service *threadRuntimeService) ReorderQueue(ctx context.Context, in ReorderQueueInput) (ThreadView, error) {
	view, err := service.reorderQueueCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) reorderQueueCommand(ctx context.Context, in ReorderQueueInput, mode threadResultMode) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.viewResult(ctx, in.ThreadID, mode); err != nil {
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
		actor.markComponents(threadQueueChanged)
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	view := service.currentResult(actor, mode)
	return view, nil
}

func (service *threadRuntimeService) DeleteQueued(ctx context.Context, in DeleteQueuedInput) (ThreadView, error) {
	view, err := service.deleteQueuedCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) deleteQueuedCommand(ctx context.Context, in DeleteQueuedInput, mode threadResultMode) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.viewResult(ctx, in.ThreadID, mode); err != nil {
		return ThreadView{}, err
	}
	queueID := strings.TrimSpace(in.QueueItemID)
	actor := service.runtime(in.ThreadID)
	fingerprint, _ := stableFingerprint(queueID)
	if in.ExpectedInputSHA256 != "" {
		decoded, err := hex.DecodeString(in.ExpectedInputSHA256)
		if err != nil || len(decoded) != sha256.Size || strings.ToLower(in.ExpectedInputSHA256) != in.ExpectedInputSHA256 {
			return ThreadView{}, errors.New("invalid expected input SHA-256")
		}
		fingerprint, _ = stableFingerprint(struct{ ID, ExpectedInputSHA256 string }{queueID, in.ExpectedInputSHA256})
	}
	if err := actor.apply(ctx, func() error {
		if previous, ok := actor.state.requestKeys[key]; ok && in.ExpectedInputSHA256 != "" {
			if previous.fingerprint != fingerprint {
				return ErrRequestConflict
			}
			return nil
		}
		for index, item := range actor.state.view.Queue {
			if item.ID == queueID {
				if in.ExpectedInputSHA256 != "" {
					currentFingerprint, err := stableFingerprint(item.Input)
					if err != nil {
						return err
					}
					if currentFingerprint != "sha256:"+in.ExpectedInputSHA256 {
						return ErrRequestConflict
					}
				}
				if err := service.appendQueueFact(ctx, in.ThreadID, sessiontree.EntryQueueDeleted, "queue-delete:"+key, key, fingerprint, queueID); err != nil {
					return err
				}
				actor.state.view.Queue = append(actor.state.view.Queue[:index], actor.state.view.Queue[index+1:]...)
				actor.markComponents(threadQueueChanged)
				actor.state.view.ViewVersion++
				actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
				return nil
			}
		}
		if in.ExpectedInputSHA256 != "" {
			return ErrRequestConflict
		}
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	view := service.currentResult(actor, mode)
	return view, nil
}

func (service *threadRuntimeService) PromoteQueued(ctx context.Context, in PromoteQueuedInput) (ThreadView, error) {
	view, err := service.promoteQueuedCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) promoteQueuedCommand(ctx context.Context, in PromoteQueuedInput, mode threadResultMode) (ThreadView, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.viewResult(ctx, in.ThreadID, mode); err != nil {
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
		return service.currentResult(actor, mode), nil
	}
	result, err := service.startAcceptedResult(ctx, actor, in.ThreadID, target.Input, target.SupplementalContext, target.RequestKey, target.ID, key, mode)
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
	if _, err := service.viewResult(ctx, in.ThreadID, threadAckResult); err != nil {
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
	_ = actor.apply(ctx, func() error {
		actor.state.view.Queue = queue
		actor.markComponents(threadQueueChanged)
		actor.state.view.ViewVersion++
		return nil
	})
	view, err := service.completeViewResult(ctx, service.currentView(actor), nil)
	if err != nil {
		return ImportResult{}, err
	}
	return ImportResult{ThreadID: in.ThreadID, Imported: imported, View: view}, nil
}

func (service *threadRuntimeService) currentView(actor *threadRuntimeState) ThreadView {
	return service.currentResult(actor, threadFullResult)
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
	runtime := &threadRuntimeState{threadID: threadID.String(), service: service}
	if service.closed {
		runtime.closed = true
		return runtime
	}
	// Deleted identities live in canonical tombstones, not an unbounded actor
	// cache. Check cold lookups before installing a new lifecycle owner.
	if service.host != nil {
		if _, err := service.ensureThread(context.Background(), threadID); err != nil {
			runtime.lookupErr = err
			return runtime
		}
	}
	service.runtimes[threadID.String()] = runtime
	return runtime
}

func (service *threadRuntimeService) close() error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	service.subscriptionsClosed = true
	subscriptions := make([]*WorkspaceSubscription, 0, len(service.subscribers))
	for subscription := range service.subscribers {
		subscriptions = append(subscriptions, subscription)
	}
	service.subscribers = make(map[*WorkspaceSubscription]struct{})
	updates := make([]*ThreadUpdateSubscription, 0, len(service.updateSubscribers))
	for subscription := range service.updateSubscribers {
		updates = append(updates, subscription)
	}
	service.updateSubscribers = nil
	service.mu.Unlock()
	for _, subscription := range updates {
		subscription.closeWithError(ErrHostClosed)
	}
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
	var closeErr error
	for _, runtime := range runtimes {
		runtime.mu.Lock()
		runtime.stopUpdateTimer()
		drains = append(drains, threadRuntimeDrain{
			actor: runtime, executionDone: runtime.state.executionDone, effectsDone: runtime.state.effectsDone,
		})
		turnID, runID, cancel := runtime.state.turnID, runtime.state.runID, runtime.state.cancel
		if runtime.deleted || runtime.deleting || runtime.state.view.Activity != ThreadActivityActive {
			cancel = nil
		}
		for _, interaction := range runtime.state.view.Interactions {
			if !interaction.Resolved && interaction.Kind == ThreadInteractionInput && len(runtime.state.pendingInteractions) == 0 {
				// Input requests are durable pauses. The runner may still be
				// committing its waiting marker after publishing the question;
				// join that settlement without turning it into a cancellation.
				cancel = nil
				break
			}
		}
		runtime.mu.Unlock()
		// Admission is already fenced by Host.Shutdown. Settle the exact
		// in-memory execution before closing its actor, so cancellation also
		// resolves approvals and uncertain effects in canonical storage.
		// Durable input waits survive reopening.
		if cancel != nil {
			key := "shutdown:" + runID.String()
			fingerprint, _ := stableFingerprint(struct {
				ThreadID string         `json:"thread_id"`
				RunID    identity.RunID `json:"run_id"`
			}{runtime.threadID, runID})
			_, err := service.settleCancellation(context.Background(), runtime, identity.ThreadID(runtime.threadID), turnID, runID,
				cancellationRequest{EntryID: "cancel:" + key, RequestKey: key, RequestFingerprint: fingerprint}, "runtime_shutdown", "runtime shutdown")
			closeErr = errors.Join(closeErr, err)
			cancel(context.Canceled)
		}
	}
	closeErr = errors.Join(closeErr, waitThreadRuntimeDrains(context.Background(), drains))
	for _, runtime := range runtimes {
		runtime.mu.Lock()
		runtime.stopUpdateTimer()
		runtime.closed = true
		runtime.mu.Unlock()
	}
	return closeErr
}

func (service *threadRuntimeService) View(ctx context.Context, threadID identity.ThreadID) (ThreadView, error) {
	view, err := service.viewResult(ctx, threadID, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) viewResult(ctx context.Context, threadID identity.ThreadID, mode threadResultMode) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	var view ThreadView
	if err := actor.apply(ctx, func() error {
		if actor.state.contextErr != nil {
			canonical, err := service.loadCanonicalThreadView(ctx, threadID)
			if err != nil {
				return err
			}
			if err := actor.installCanonicalView(canonical); err != nil {
				return err
			}
		}
		service.flushUpdateLocked(actor)
		if actor.state.view.ViewVersion > 0 {
			view = threadResult(actor.state.view, mode)
		}
		return nil
	}); err != nil || view.ViewVersion > 0 {
		return view, err
	}
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	canonical, err := service.loadRetainedThreadView(ctx, threadID, meta)
	if err != nil {
		return ThreadView{}, err
	}
	canonical.Queue, canonical.RestoredInputs, err = hydrateThreadQueues(ctx, service.host.store.repo, threadID)
	if err != nil {
		return ThreadView{}, err
	}
	startHydration := false
	err = actor.apply(ctx, func() error {
		service.flushUpdateLocked(actor)
		if actor.state.view.ViewVersion > 0 {
			view = threadResult(actor.state.view, mode)
			return nil
		}
		canonical.ViewVersion = 1
		canonical.ThreadID = threadID
		if canonical.Activity == ThreadActivityActive && !threadRuntimeViewNeedsAttention(canonical.ThreadView) {
			canonical.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
		}
		actor.state.view = canonical.ThreadView
		actor.interactionIndex = nil
		actor.canonical = canonical.projection
		actor.canonical.items.releaseText()
		actor.retainItems()
		actor.invalidateItemIndex()
		actor.markComponents(threadContextChanged | threadGoalChanged | threadParentEventsChanged | threadTurnsChanged | threadQueueChanged | threadInteractionsChanged | threadPreviewsChanged | threadRestoredInputsChanged)
		for _, item := range canonical.Items {
			actor.markItem(item.ID, ThreadItemAdded)
		}
		actor.state.turnID = canonical.TurnID
		actor.state.runID = canonical.RunID
		actor.state.requestKeys = hydrateThreadRequestKeys(ctx, service.host.store.repo, meta)
		actor.state.steering = actor.canonical.pendingSteering(canonical.TurnID)
		if canonical.Activity == ThreadActivityActive && !actor.state.hydrationStarted && service.host.requireExecution() == nil {
			actor.state.hydrationStarted = true
			startHydration = true
		}
		view = threadResult(canonical.ThreadView, mode)
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
	var entries []sessiontree.Entry
	if reader, ok := repo.(sessiontree.PathMetadataReader); ok {
		page, err := reader.PathMetadataPage(ctx, meta.ID, meta.LeafID, "", 256)
		if err != nil {
			return keys
		}
		entries = make([]sessiontree.Entry, len(page.Entries))
		for i, metadata := range page.Entries {
			entries[i] = metadata.Entry
		}
	} else {
		page, err := repo.PathPage(ctx, meta.ID, meta.LeafID, "", 256)
		if err != nil {
			return keys
		}
		entries = page.Entries
	}
	for _, entry := range entries {
		key := strings.TrimSpace(entry.RequestKey)
		if key == "" {
			continue
		}
		fingerprint := strings.TrimSpace(entry.RequestFingerprint)
		if entry.Metadata[sessiontree.TurnKindMetadataKey] != "" {
			intent := TurnKindContextCompaction
			if entry.Metadata[sessiontree.ContinueGoalMetadataKey] == "1" {
				intent = TurnKindGoalContinuation
			}
			fingerprint, _ = stableFingerprint(intent)
		}
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
	if service.host.requireExecution() != nil {
		return
	}
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
	if service.failRecoveredUnknownEffectTurn(ctx, threadID, turnID, runID, entries) {
		service.refreshCanonical(threadID, turnID)
		return
	}
	actor := service.runtime(threadID)
	pendingSteering := false
	_ = actor.apply(ctx, func() error { pendingSteering = len(actor.state.steering) > 0; return nil })
	if pendingSteering {
		service.continueCanonicalInput(actor, threadID, ThreadInteraction{ID: "recovered-steering:" + runID.String(), TurnID: turnID, RunID: runID}, InteractionResolution{}, nil, true)
		return
	}
	view := service.currentView(actor)
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
		service.continueCanonicalInput(service.runtime(threadID), threadID, interaction, *interaction.Resolution, nil)
		return
	}
	service.redispatchAcceptedTurn(ctx, threadID, turnID, runID)
}

func (service *threadRuntimeService) failRecoveredUnknownEffectTurn(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, entries []sessiontree.Entry) bool {
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
	unknown := false
	for _, attempt := range latest {
		switch attempt.State {
		case sessiontree.EffectAttemptDispatching, sessiontree.EffectAttemptUnknown:
			unknown = true
		}
	}
	if !unknown {
		return false
	}
	repo, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		service.host.store.reportBackgroundError(ErrUnsupportedStoreCapability)
		return true
	}
	if _, err := repo.FailUnknownEffectTurn(ctx, sessiontree.FailUnknownEffectTurnRequest{
		ThreadID: threadID.String(), TurnID: turnID.String(), RunID: runID.String(), Now: time.Now().UTC(),
	}); err != nil && !errors.Is(err, sessiontree.ErrStaleAuthority) {
		service.host.store.reportBackgroundError(err)
	}
	return true
}

func turnHasCancelRequest(entries []sessiontree.Entry, turnID identity.TurnID) bool {
	for _, entry := range entries {
		if entry.TurnID == turnID.String() && entry.Type == sessiontree.EntryCancelRequested {
			return true
		}
	}
	return false
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
		{ID: "runtime-restarted:" + sessiontree.StableHash(leafID)[:24], ThreadID: threadID.String(), TurnID: source.TurnID.String(), RunID: source.RunID.String(), Type: sessiontree.EntryRuntimeRestarted},
		{ID: "interaction-requested:" + restored.ID, ThreadID: threadID.String(), TurnID: source.TurnID.String(), RunID: source.RunID.String(), Type: sessiontree.EntryInteractionAsked, Payload: payload},
	})
	if err == nil {
		service.refreshCanonical(threadID, source.TurnID)
	}
}

func (service *threadRuntimeService) redispatchAcceptedTurn(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) {
	journal, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		service.finishUnloadedTerminal(ctx, service.runtime(threadID), threadID, turnID, runID, TurnResult{}, ErrUnsupportedStoreCapability)
		return
	}
	canonical, found, err := journal.ReadAcceptedTurn(ctx, threadID.String(), turnID.String(), runID.String())
	if err != nil || !found {
		if err == nil {
			err = ErrAuthorityCorrupt
		}
		service.finishUnloadedTerminal(ctx, service.runtime(threadID), threadID, turnID, runID, TurnResult{}, err)
		return
	}
	if canonical.Terminal != nil {
		return
	}
	kind := TurnKind(canonical.TurnStarted.Metadata[sessiontree.TurnKindMetadataKey])
	input := turnInputFromSessionMessage(canonical.UserMessage.Message)
	requestKey := strings.TrimSpace(canonical.TurnStarted.RequestKey)
	var retrySourceTurnID identity.TurnID
	var retrySourceEntryID string
	if reader, ok := service.host.store.repo.(sessiontree.CanonicalTurnReadRepo); ok && kind != TurnKindContextCompaction {
		read, readErr := reader.ReadCanonicalTurn(ctx, threadID.String(), turnID.String())
		if readErr != nil {
			service.finishUnloadedTerminal(ctx, service.runtime(threadID), threadID, turnID, runID, TurnResult{}, readErr)
			return
		}
		if read.Turn.RetrySource != nil {
			retrySourceTurnID = identity.TurnID(read.Turn.RetrySource.TurnID)
			retrySourceEntryID = strings.TrimSpace(read.Turn.RetrySource.EntryID)
		}
	}
	canonicalInput := input
	if retrySourceEntryID != "" && kind == "" {
		_, canonicalInput, err = service.retrySource(ctx, threadID, retrySourceTurnID)
		if err != nil {
			service.finishUnloadedTerminal(ctx, service.runtime(threadID), threadID, turnID, runID, TurnResult{}, err)
			return
		}
	}
	agent, err := service.factory.Agent(ctx, AgentRequest{
		TurnKind: kind, ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input, CanonicalTurnInput: canonicalInput,
		RetrySource: retrySourceTurnID,
	})
	if err != nil || agent == nil {
		if err == nil {
			err = errors.New("Agent factory returned nil")
		}
		service.finishUnloadedTerminal(ctx, service.runtime(threadID), threadID, turnID, runID, TurnResult{}, err)
		return
	}
	runner, err := service.host.turnRunner(ctx, threadID, service.executionAgent(service.runtime(threadID), agent))
	if err != nil {
		service.finishUnloadedTerminal(ctx, service.runtime(threadID), threadID, turnID, runID, TurnResult{}, err)
		return
	}
	executionCtx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	actor := service.runtime(threadID)
	if err := actor.apply(ctx, func() error {
		if _, beginErr := actor.beginRun(beginThreadRunInput{
			turnID: turnID, runID: runID, logicalRequestID: identity.LogicalRequestID(requestKey),
			cancel: cancel, executionDone: done, turnKind: kind,
		}); beginErr != nil {
			return beginErr
		}
		actor.state.agent = agent
		return nil
	}); err != nil {
		cancel(context.Canceled)
		close(done)
		return
	}
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		TurnKind: kind, RetrySourceTurnID: retrySourceTurnID, RetrySourceEntryID: retrySourceEntryID,
	}
	applyAgentExecutionPolicy(&request, agent)
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
	input.Context = runtimeMessageContext(message.Context)
	input.ModelContextSnapshot = runtimeModelContext(message.ModelContextSnapshot)
	for _, reference := range message.References {
		input.References = append(input.References, MessageReference{ReferenceID: reference.ReferenceID, Kind: MessageReferenceKind(reference.Kind), Label: reference.Label, Text: reference.Text, ResourceRef: reference.ResourceRef, Truncated: reference.Truncated, ReferencedThreadID: identity.ThreadID(reference.ReferencedThreadID)})
	}
	return input
}

func (service *threadRuntimeService) CompactContext(ctx context.Context, in CompactContextInput) (ThreadView, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	kind := TurnKindContextCompaction
	if in.ContinueGoal {
		kind = TurnKindGoalContinuation
	}
	view, err := service.admitThreadExecution(ctx, in.ThreadID, UserInput{}, nil, key, kind)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) sendResult(ctx context.Context, threadID identity.ThreadID, input UserInput, supplemental []TurnSupplementalContextItem, requestKey string, mode threadResultMode) (ThreadView, error) {
	normalized, err := normalizeTurnInput(input)
	if err != nil {
		return ThreadView{}, err
	}
	return service.admitThreadExecutionResult(ctx, threadID, normalized, supplemental, requestKey, "", mode)
}

func (service *threadRuntimeService) admitThreadExecution(ctx context.Context, threadID identity.ThreadID, input UserInput, supplemental []TurnSupplementalContextItem, requestKey string, kind TurnKind) (ThreadView, error) {
	return service.admitThreadExecutionResult(ctx, threadID, input, supplemental, requestKey, kind, threadFullResult)
}

func (service *threadRuntimeService) admitThreadExecutionResult(ctx context.Context, threadID identity.ThreadID, input UserInput, supplemental []TurnSupplementalContextItem, requestKey string, kind TurnKind, mode threadResultMode) (ThreadView, error) {
	requestKey = strings.TrimSpace(requestKey)
	if requestKey == "" {
		return ThreadView{}, errors.New("send request key is required")
	}
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(input)
	if kind != "" {
		fingerprint, err = stableFingerprint(kind)
	}
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	canonical, err := service.viewResult(ctx, threadID, mode)
	if err != nil {
		return ThreadView{}, err
	}
	if entry, found, readErr := service.host.store.repo.FindTurnRequest(ctx, threadID.String(), requestKey); readErr != nil {
		return ThreadView{}, readErr
	} else if found {
		storedKind := TurnKind(entry.Metadata[sessiontree.TurnKindMetadataKey])
		storedIntent := storedKind
		if entry.Metadata[sessiontree.ContinueGoalMetadataKey] == "1" {
			storedIntent = TurnKindGoalContinuation
		}
		if storedIntent != kind {
			return ThreadView{}, &RequestConflictError{Operation: "context_compaction", RequestID: requestKey, Err: ErrRequestConflict}
		}
		if kind != "" {
			return canonical, nil
		}
	}
	if entry, found, readErr := service.lookupSendEntry(ctx, threadID, requestKey); readErr != nil {
		return ThreadView{}, readErr
	} else if found {
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
			replay, found = threadResult(actor.state.view, mode), true
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
	if active && kind != "" {
		return ThreadView{}, ErrThreadBusy
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
			actor.markComponents(threadQueueChanged)
			result := threadResult(actor.state.view, mode)
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
			return service.currentResult(actor, mode), nil
		}
		return replay, nil
	}
	turnID, runID, err := service.host.nextTurnRunIDs()
	if err != nil {
		return ThreadView{}, err
	}
	executionCtx, cancel := context.WithCancelCause(context.Background())
	executionDone := make(chan struct{})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		SupplementalContext: cloneTurnSupplementalContext(supplemental),
		InputFingerprint:    fingerprint, TurnKind: kind, ContinueGoal: kind == TurnKindGoalContinuation,
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
			result = threadResult(actor.state.view, mode)
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		if kind != "" {
			if len(actor.state.view.Queue) != 0 {
				return ErrThreadBusy
			}
			for _, interaction := range actor.state.view.Interactions {
				if !interaction.Resolved {
					return ErrThreadBusy
				}
			}
		}
		var acceptErr error
		accepted, acceptErr = service.acceptCanonicalTurn(ctx, threadID, request)
		if acceptErr != nil {
			return acceptErr
		}
		kind = accepted.TurnKind
		request.TurnKind = kind
		_ = canonical
		previousExecution, acceptErr = actor.beginRun(beginThreadRunInput{
			turnID: turnID, runID: runID, logicalRequestID: identity.LogicalRequestID(requestKey),
			cancel: cancel, executionDone: executionDone, clearOutcome: true, turnKind: kind,
		})
		if acceptErr != nil {
			return acceptErr
		}
		if kind == "" {
			actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: "user:" + requestKey, TurnID: turnID, RunID: runID, Kind: ThreadItemUser, Text: input.Text, CreatedAt: time.Now().UTC(), Attachments: cloneMessageAttachments(input.Attachments), References: append([]MessageReference(nil), input.References...), Context: append([]MessageContextItem(nil), input.Context...)})
			actor.markItem("user:"+requestKey, ThreadItemAdded)
		}
		if err := service.advanceCanonical(context.Background(), actor); err != nil {
			actor.state.contextErr = err
		}
		result = threadResult(actor.state.view, mode)
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		return nil
	})
	if err != nil {
		cancel(context.Canceled)
		return ThreadView{}, err
	}
	if replayed {
		cancel(context.Canceled)
		return result, nil
	}
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{
		ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input, CanonicalTurnInput: input, TurnKind: kind,
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
	if err := service.host.requireExecution(); err != nil {
		ready <- preparedThreadExecution{err: err}
		return ready
	}
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
	req, err := canonicalTurnRequest(threadID, request)
	if err != nil {
		return acceptedTurn{}, err
	}
	accepted, err := journal.AcceptTurn(ctx, req)
	if err != nil {
		return acceptedTurn{}, runtimeHostError(err)
	}
	return acceptedTurn{TurnKind: TurnKind(accepted.TurnStarted.Metadata[sessiontree.TurnKindMetadataKey]), ThreadID: threadID, TurnID: request.TurnID, RunID: request.RunID, UserEntryID: accepted.UserMessage.ID, BaseLeafID: accepted.BaseLeafID, Replayed: accepted.Replayed}, nil
}

func canonicalTurnRequest(threadID identity.ThreadID, request turnExecutionRequest) (sessiontree.AcceptTurnRequest, error) {
	canonicalInput := session.Message{
		Role: session.User, Content: request.Input.Text,
		Attachments:          sessionMessageAttachments(request.Input.Attachments),
		References:           sessionMessageReferences(request.Input.References),
		Context:              sessionMessageContext(request.Input.Context),
		ModelContextSnapshot: sessionModelContext(request.Input.ModelContextSnapshot),
	}
	if request.RetrySourceEntryID != "" || request.TurnKind != "" {
		canonicalInput = session.Message{}
	}
	req := sessiontree.AcceptTurnRequest{
		TurnKind: string(request.TurnKind), ContinueGoal: request.ContinueGoal,
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
		return sessiontree.AcceptTurnRequest{}, err
	}
	req.RequestFingerprint = fingerprint
	return req, nil
}

func agentManualCompactions(agent *Agent) ManualCompactionSource {
	if agent == nil {
		return nil
	}
	return agent.manualCompactions
}

func applyAgentExecutionPolicy(request *turnExecutionRequest, agent *Agent) {
	if request == nil {
		return
	}
	request.Signals = TurnSignalSpec{
		Definitions: CoreControlDefinitions(),
		Project:     ProjectCoreControlSignal,
	}
}

func (service *threadRuntimeService) rollbackProvisionalSend(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, requestKey string) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID {
			return nil
		}
		for index := len(actor.state.view.Items) - 1; index >= 0; index-- {
			if actor.state.view.Items[index].ID == "user:"+requestKey {
				actor.state.view.Items = append(actor.state.view.Items[:index], actor.state.view.Items[index+1:]...)
				actor.markItem("user:"+requestKey, ThreadItemRemoved)
				actor.invalidateItemIndex()
				actor.mutations.order = true
				break
			}
		}
		delete(actor.state.requestKeys, requestKey)
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.TurnID = ""
		actor.state.view.RunID = ""
		actor.state.view.RunProgress = nil
		actor.state.view.ToolCallPreviews = nil
		actor.markComponents(threadPreviewsChanged)
		actor.state.view.ViewVersion++
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
			service.finishUnloadedTerminal(ctx, actor, accepted.ThreadID, request.TurnID, request.RunID, TurnResult{}, execution.err)
			return
		}
		request.ManualCompactions = agentManualCompactions(execution.agent)
		applyAgentExecutionPolicy(&request, execution.agent)
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
			service.finishUnloadedTerminal(ctx, actor, accepted.ThreadID, request.TurnID, request.RunID, TurnResult{Status: TurnStatusCancelled}, ctx.Err())
			return
		case <-previousExecution:
		}
	}
	completed, err := runner.ExecuteAccepted(ctx, acceptedTurnExecutionRequest{
		Accepted: accepted, LogicalRequestID: request.LogicalRequestID, RunID: request.RunID, TurnID: request.TurnID, TurnKind: request.TurnKind,
		Input: request.Input, SupplementalContext: request.SupplementalContext,
		Signals: request.Signals, Limits: request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
	})
	if completed.Status == "" && err != nil {
		// Resume or accepted-turn validation can fail before the harness owns
		// finalization. Settle canonical admission before publishing actor idle.
		service.finishUnloadedTerminal(ctx, actor, accepted.ThreadID, request.TurnID, request.RunID, completed, err)
		return
	}
	if completed.Status == TurnStatusCancelled && ctx.Err() != nil {
		service.recordCancellation(accepted.ThreadID, request.TurnID, request.RunID, "execution_context", ctx.Err().Error())
	}
	service.finishSend(actor, request.TurnID, request.RunID, completed, err)
}

func (service *threadRuntimeService) finishSend(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, completed TurnResult, runErr error) {
	var graceful *agentharness.GracefulCancellation
	if completed.Status == TurnStatusCancelled && errors.As(runErr, &graceful) {
		// The stop owner seals the terminal after this execution drains.
		return
	}
	// Keep the execution active until its complete canonical terminal snapshot
	// can be installed in one actor transition. View and Send read this state
	// directly; delaying publication alone cannot prevent partial settlement.
	read, refreshErr := service.readTerminalCanonical(context.Background(), actor)
	var canonical canonicalThreadView
	startQueue := false
	changed := false
	for {
		retry := false
		_ = actor.apply(context.Background(), func() error {
			if actor.state.turnID != turnID || actor.state.runID != runID || actor.state.view.Activity != ThreadActivityActive {
				return nil
			}
			if refreshErr == nil && !read.current(actor) {
				retry = true
				return nil
			}
			if refreshErr == nil {
				canonical, refreshErr = service.installTerminalCanonical(context.Background(), actor, read)
			}
			if actor.state.cancelOwner == "run:"+runID.String() {
				actor.state.cancel = nil
				actor.state.cancelOwner = ""
			}
			actor.state.contextErr = refreshErr
			actor.state.view.ViewVersion++
			actor.finishLiveTextSegment()
			actor.state.view.Activity = ThreadActivityIdle
			if completed.Status != TurnStatusWaiting {
				actor.state.steering = nil
				actor.state.executionSealed = false
			}
			actor.state.view.RunProgress = nil
			actor.state.view.ToolCallPreviews = nil
			actor.markComponents(threadPreviewsChanged)
			outcome := TurnOutcomeCompleted
			if completed.Status == TurnStatusWaiting {
				actor.state.executionSealed = false
				actor.state.view.Activity = ThreadActivityActive
				actor.state.view.LastOutcome = nil
				actor.normalizeRunProgress()
			} else if completed.Status == TurnStatusCancelled || completed.Status == TurnStatusInterrupted {
				outcome = TurnOutcomeCancelled
			} else if runErr != nil || completed.Status == TurnStatusFailed {
				outcome = TurnOutcomeFailed
			}
			actor.state.view.Failure = nil
			if outcome == TurnOutcomeFailed {
				actor.state.view.Failure = cloneThreadTurnFailure(completed.Failure)
				if actor.state.view.Failure == nil && runErr != nil {
					actor.state.view.Failure = &ThreadTurnFailure{Code: ThreadTurnFailureEngineContract, Message: strings.TrimSpace(runErr.Error())}
				}
			}
			if completed.Status != TurnStatusWaiting {
				actor.state.view.LastOutcome = &outcome
				if refreshErr == nil {
					if canonical.Activity == ThreadActivityIdle && canonical.LastOutcome != nil && canonical.TurnID == turnID && canonical.RunID == runID {
						actor.state.view.LastOutcome = canonical.LastOutcome
						actor.state.view.Failure = canonical.Failure
						actor.state.view.Cancellation = canonical.Cancellation
					}
				} else {
					actor.replaceItems(settleTerminalToolSegments(cloneThreadItems(actor.state.view.Items), turnID, completed.ActivityTimeline))
				}
				if outcome == TurnOutcomeCompleted && refreshErr == nil && actor.state.view.TurnKind != TurnKindContextCompaction && !(actor.state.view.TurnKind == TurnKindGoalContinuation && actor.state.view.Goal != nil && actor.state.view.Goal.Status != "active") && !actor.hasTerminalPresentation(turnID) {
					failed := TurnOutcomeFailed
					actor.state.view.LastOutcome = &failed
					message := "The turn completed without a visible response."
					actor.state.view.Failure = &ThreadTurnFailure{Code: ThreadTurnFailureEngineContract, Message: message}
				}
			}
			view := actor.state.view
			startQueue = view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCompleted && view.Cancellation == nil
			changed = true
			return nil
		})
		if !retry {
			break
		}
		read, refreshErr = service.readTerminalCanonical(context.Background(), actor)
	}
	if !changed {
		return
	}
	if completed.Status == TurnStatusCompleted && runErr == nil && startQueue {
		service.startNextQueued(actor)
	}
}

func hasTerminalPresentation(items []ThreadItem, turnID identity.TurnID) bool {
	for _, item := range items {
		if item.TurnID == turnID && threadItemTerminalPresentation(item) {
			return true
		}
	}
	return false
}

func (actor *threadRuntimeState) hasTerminalPresentation(turnID identity.TurnID) bool {
	if actor.canonical != nil {
		return actor.canonical.terminalCounts[turnID] > 0
	}
	return hasTerminalPresentation(actor.state.view.Items, turnID)
}

func threadItemTerminalPresentation(item ThreadItem) bool {
	switch item.Kind {
	case ThreadItemAssistant:
		return threadItemHasText(item)
	case ThreadItemTool:
		return item.Activity != nil && item.Activity.Status != observation.ActivityStatusRunning
	case ThreadItemInteraction:
		return item.Interaction != nil && item.Interaction.Resolved
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

func (service *threadRuntimeService) executionAgent(actor *threadRuntimeState, agent *Agent) *Agent {
	if agent == nil {
		return nil
	}
	copy := *agent
	copy.steering = service.steeringControl(actor)
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
	if err := gate.service.host.requireExecution(); err != nil {
		return EffectDispatchResult{}, err
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
		ID: "approval:" + request.EffectAttemptID, TurnID: request.TurnID, RunID: request.RunID,
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
	if strings.TrimSpace(interaction.ID) == "" || interaction.TurnID == "" || interaction.RunID == "" {
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
		ID: "interaction-requested:" + interaction.ID, ThreadID: threadID.String(), TurnID: interaction.TurnID.String(), RunID: interaction.RunID.String(),
		Type: sessiontree.EntryInteractionAsked, RequestKey: interaction.ID, RequestFingerprint: fingerprint, Payload: payload,
	}
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	// Reserve before the durable write so a canonical refresh and response
	// cannot overtake the local caller that is waiting for the resolution.
	waiter := &pendingThreadInteraction{resolution: make(chan InteractionResolution, 1)}
	reservedWaiter := false
	if err := actor.apply(ctx, func() error {
		if actor.state.pendingInteractions == nil {
			actor.state.pendingInteractions = make(map[string]*pendingThreadInteraction)
		}
		if existing := actor.state.pendingInteractions[interaction.ID]; existing != nil {
			waiter = existing
			return nil
		}
		actor.state.pendingInteractions[interaction.ID] = waiter
		reservedWaiter = true
		return nil
	}); err != nil {
		return nil, err
	}
	releaseReservedWaiter := func() {
		if !reservedWaiter {
			return
		}
		_ = actor.apply(context.Background(), func() error {
			if actor.state.pendingInteractions[interaction.ID] == waiter {
				delete(actor.state.pendingInteractions, interaction.ID)
			}
			return nil
		})
	}
	if _, err := writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{entry}); err != nil {
		existing, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entry.ID)
		if readErr != nil || existing.RequestFingerprint != fingerprint || string(existing.Payload) != string(payload) {
			releaseReservedWaiter()
			return nil, runtimeHostError(err)
		}
	}
	err = actor.apply(ctx, func() error {
		if err := service.advanceCanonical(ctx, actor); err != nil {
			return err
		}
		actor.ensureInteractionIndex()
		if index, found := actor.interactionIndex[interaction.ID]; found {
			current := actor.state.view.Interactions[index]
			if current.Resolved {
				if current.Resolution == nil {
					return ErrAuthorityCorrupt
				}
				progress := cloneThreadRunProgress(actor.state.view.RunProgress)
				actor.normalizeRunProgress()
				if !sameThreadRunProgress(progress, actor.state.view.RunProgress) {
					actor.state.view.ViewVersion++
				}
				if reservedWaiter && actor.state.pendingInteractions[interaction.ID] == waiter {
					delete(actor.state.pendingInteractions, interaction.ID)
					waiter.resolution <- *current.Resolution
				}
			} else if actor.state.view.RunProgress != nil {
				actor.state.view.RunProgress = nil
				actor.state.view.ViewVersion++
			}
			return nil
		}
		return fmt.Errorf("%w: committed interaction is absent from canonical projection", ErrAuthorityCorrupt)
	})
	if err != nil {
		releaseReservedWaiter()
		return nil, err
	}
	return waiter, nil
}

func (sink threadRuntimeEventSink) EmitEvent(event Event) {
	if sink.service == nil || event.ThreadID == "" {
		return
	}
	actor := sink.service.runtime(event.ThreadID)
	_ = actor.apply(context.Background(), func() error {
		if !actor.acceptLiveEvent(event) {
			return nil
		}
		canonicalChanged := event.contextCommitted || event.committed != nil
		if canonicalChanged {
			if err := sink.service.advanceCanonical(context.Background(), actor); err != nil {
				actor.state.contextErr = err
				return nil
			}
		}
		activityChanged := actor.applyToolActivity(event.ActivityTimeline, event.Timestamp)
		if len(event.activityItems) > 0 {
			activityChanged = actor.applyActivityItems(event.ThreadID, event.TurnID, event.RunID, event.activityItems, event.Timestamp) || activityChanged
		}
		actor.orderAttemptOutput(event.outputOrder)
		previewChanged := actor.applyArgumentPreview(event)
		progress, progressEvent := threadRunProgressForEvent(event)
		if progressEvent {
			progress = mergeRequestProgress(actor.state.view.RunProgress, progress, event)
		}
		progressChanged := progressEvent && actor.state.view.Attention == (AttentionSummary{}) && !sameThreadRunProgress(actor.state.view.RunProgress, progress)
		if progressChanged {
			actor.state.view.RunProgress = cloneThreadRunProgress(progress)
		}
		if event.Stream != nil || progressChanged || activityChanged || previewChanged {
			actor.state.view.ViewVersion++
		}
		return nil
	})
}

func threadRunProgressForEvent(event Event) (*ThreadRunProgress, bool) {
	if (event.Type == observation.EventTypeProviderDelta || event.Type == observation.EventTypeProviderReasoning) && (event.Stream != nil && event.Stream.Text == "") {
		return nil, false
	}
	var phase ThreadRunPhase
	switch event.Type {
	case observation.EventTypeStepStart:
		phase = ThreadRunPhasePreparing
	case observation.EventTypeProviderTransport, observation.EventTypeProviderRequest, observation.EventTypeHostedToolResult:
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
	default:
		return nil, false
	}
	return &ThreadRunProgress{Phase: phase, Retry: cloneProviderRetryProgress(event.Retry)}, true
}

func sameThreadRunProgress(left, right *ThreadRunProgress) bool {
	return left == nil && right == nil || left != nil && right != nil && reflect.DeepEqual(left, right)
}

var errCanonicalRefreshDiscarded = errors.New("canonical refresh snapshot discarded")

func (service *threadRuntimeService) refreshCanonical(threadID identity.ThreadID, turnID identity.TurnID) error {
	if _, err := service.ensureThread(context.Background(), threadID); err != nil {
		return fmt.Errorf("canonical refresh ensure thread: %w", err)
	}
	actor := service.runtime(threadID)
	var baseVersion uint64
	_ = actor.apply(context.Background(), func() error { baseVersion = actor.state.view.ViewVersion; return nil })
	canonical, err := service.loadCanonicalThreadView(context.Background(), threadID)
	if err != nil {
		return err
	}
	var refreshErr error
	_ = actor.apply(context.Background(), func() error {
		if actor.state.view.ViewVersion != baseVersion {
			previous := actor.state.view.Goal
			if canonical.Goal != nil && (previous == nil || canonical.Goal.Version > previous.Version) {
				actor.state.view.Goal = canonical.Goal
				actor.markComponents(threadGoalChanged)
				actor.state.view.ViewVersion++
			}
			refreshErr = fmt.Errorf("%w: view version changed", errCanonicalRefreshDiscarded)
			return nil
		}
		if turnID != "" && actor.state.turnID != "" && actor.state.turnID != turnID {
			refreshErr = fmt.Errorf("%w: turn changed", errCanonicalRefreshDiscarded)
			return nil
		}
		if err := actor.installCanonicalView(canonical); err != nil {
			refreshErr = err
			return nil
		}
		return nil
	})
	return refreshErr
}

// installCanonicalView runs under the actor and retains only observations that
// have not yet reached the canonical journal.
func (actor *threadRuntimeState) installCanonicalView(canonical canonicalThreadView) error {
	terminal := actor.state.view.Activity == ThreadActivityIdle && actor.state.view.LastOutcome != nil
	if terminal && canonical.Activity == ThreadActivityIdle && canonical.LastOutcome != nil && actor.state.turnID == canonical.TurnID && actor.state.runID == canonical.RunID {
		actor.state.view.LastOutcome = canonical.LastOutcome
		actor.state.view.Failure = cloneThreadTurnFailure(canonical.Failure)
		actor.state.view.Cancellation = canonical.Cancellation
	}
	reconciled, ok := reconcileCanonicalThreadItems(actor.state.view.Items, canonical.Items, terminal, actor.state.attemptDraftIDs)
	if !ok {
		return fmt.Errorf("%w: item identity or canonical order conflict", errCanonicalRefreshDiscarded)
	}
	actor.state.view.ViewVersion++
	actor.replaceItems(reconciled)
	actor.state.view.ParentEvents = canonical.ParentEvents
	actor.state.view.Goal = canonical.Goal
	actor.state.view.Context = canonical.Context
	actor.markComponents(threadContextChanged | threadGoalChanged | threadParentEventsChanged | threadTurnsChanged | threadInteractionsChanged | threadPreviewsChanged)
	actor.state.contextErr = nil
	actor.canonical = canonical.projection
	if actor.canonical != nil {
		actor.canonical.items.releaseText()
	}
	actor.retainItems()
	actor.pruneArgumentPreviews()
	actor.state.view.Turns = canonical.Turns
	actor.replaceInteractions(mergeThreadInteractions(actor.state.view.Interactions, canonical.Interactions))
	applyThreadInteractionsToItems(actor.state.view.Items, actor.state.view.Interactions)
	actor.normalizeRunProgress()
	return nil
}

func (service *threadRuntimeService) loadCanonicalThreadView(ctx context.Context, threadID identity.ThreadID) (canonicalThreadView, error) {
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return canonicalThreadView{}, fmt.Errorf("canonical refresh thread: %w", err)
	}
	entries, err := service.host.store.repo.Path(ctx, threadID.String(), meta.LeafID)
	if err != nil {
		return canonicalThreadView{}, fmt.Errorf("canonical refresh path: %w", err)
	}
	projection := newThreadCanonicalProjection()
	if err := projection.append(entries); err != nil {
		return canonicalThreadView{}, err
	}
	canonical, err := projection.snapshot(ctx, service, threadID, entries)
	if err != nil {
		return canonicalThreadView{}, err
	}
	canonical.ParentEvents, err = service.parentEventView(ctx, threadID)
	if err != nil {
		return canonicalThreadView{}, err
	}
	canonical.TurnID, canonical.RunID, canonical.Activity, canonical.LastOutcome, canonical.Failure = threadRuntimeLifecycleFromEntries(meta, entries)
	canonical.TurnKind = turnKindFromEntries(entries, canonical.TurnID)
	canonical.Cancellation = threadCancellationForTurn(entries, canonical.TurnID)
	return canonical, nil
}

func reconcileCanonicalThreadItems(current, canonical []ThreadItem, terminal bool, allowedDrafts map[string]bool) ([]ThreadItem, bool) {
	if len(current) == 0 {
		return cloneThreadItems(canonical), true
	}
	if terminal {
		// Once a turn is terminal, the journal is the complete presentation
		// authority. Do not retain stream-only items or stale text.
		return cloneThreadItems(canonical), true
	}
	byID := make(map[string]int, len(canonical))
	result := cloneThreadItems(canonical)
	for index, item := range canonical {
		if item.ID == "" || item.Ordinal != uint64(index+1) {
			return nil, false
		}
		if _, exists := byID[item.ID]; exists {
			return nil, false
		}
		byID[item.ID] = index
	}
	seen := make(map[string]bool, len(current))
	lastCommitted := -1
	for index, item := range current {
		if item.ID == "" || item.Ordinal != uint64(index+1) || seen[item.ID] {
			return nil, false
		}
		seen[item.ID] = true
		if position, exists := byID[item.ID]; exists {
			committed := result[position]
			if position <= lastCommitted || item.Kind != committed.Kind || item.TurnID != committed.TurnID || item.RunID != committed.RunID ||
				item.CanonicalOrdinal != 0 && item.CanonicalOrdinal != committed.CanonicalOrdinal || item.contentSourceID != committed.contentSourceID {
				return nil, false
			}
			lastCommitted = position
			// A journal call is durable input, not a newer observation of an
			// executing tool. Keep live output and settled results until the
			// corresponding canonical result or terminal turn is available.
			if item.Kind == ThreadItemTool && item.Activity != nil && committed.Activity != nil &&
				(threadToolActivityTerminal(item.Activity.Status) || item.Live && !threadToolActivityTerminal(committed.Activity.Status)) {
				committed.Activity, committed.Live = item.Activity, item.Live
			}
			result[position] = committed
			continue
		}
		// A missing persisted identity means the snapshot is stale or belongs
		// to another path. Only stream-only observations may survive its tail.
		if item.CanonicalOrdinal != 0 {
			return nil, false
		}
		if (item.Kind == ThreadItemAssistant || item.Kind == ThreadItemThinking) && !allowedDrafts[item.ID] {
			continue
		}
		result = append(result, item)
	}
	for index := range result {
		result[index].Ordinal = uint64(index + 1)
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
		if local.Resolved && !persisted.Resolved || local.RunID != "" && persisted.RunID == "" {
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
		if interaction.ToolCallID != "" && interaction.Kind == ThreadInteractionApproval {
			byTool[threadToolKey(interaction.TurnID, interaction.RunID, interaction.ToolCallID)] = interaction
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
		if interaction, found := byTool[threadToolKey(items[index].TurnID, items[index].RunID, items[index].Activity.ToolID)]; found {
			copy := interaction
			items[index].Interaction = &copy
		}
	}
}

func (service *threadRuntimeService) cancelResult(ctx context.Context, threadID identity.ThreadID, requestKey string, mode CancelMode, resultMode threadResultMode) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	current := service.currentResult(actor, resultMode)
	// A hydrated actor owns Stop even if its next presentation cannot be assembled.
	if current.ViewVersion == 0 {
		var err error
		current, err = service.viewResult(ctx, threadID, resultMode)
		if err != nil {
			return ThreadView{}, err
		}
	}
	if current.Activity != ThreadActivityActive {
		return current, nil
	}
	turnID := current.TurnID
	fingerprint, err := stableFingerprint(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		TurnID   identity.TurnID   `json:"turn_id"`
	}{threadID, turnID})
	if err != nil {
		return ThreadView{}, err
	}
	if mode == CancelModeGraceful {
		return service.requestGracefulCancellationResult(ctx, actor, threadID, turnID, requestKey, resultMode)
	}
	return service.settleCancellationResult(ctx, actor, threadID, turnID, "", cancellationRequest{
		EntryID: "cancel:" + requestKey, RequestKey: requestKey, RequestFingerprint: fingerprint,
	}, "user_stop", "user requested stop", resultMode)
}

type cancellationRequest struct {
	EntryID            string
	RequestKey         string
	RequestFingerprint string
}

func (service *threadRuntimeService) settleCancellation(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, request cancellationRequest, source, reason string) (ThreadView, error) {
	return service.settleCancellationResult(ctx, actor, threadID, turnID, runID, request, source, reason, threadFullResult)
}

func (service *threadRuntimeService) settleCancellationResult(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, request cancellationRequest, source, reason string, mode threadResultMode) (ThreadView, error) {
	repo, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	resolution := InteractionResolution{Accepted: false, Outcome: "cancelled"}
	resolutionPayload, err := json.Marshal(resolution)
	if err != nil {
		return ThreadView{}, err
	}
	var runCancel context.CancelCauseFunc
	var waiters []chan InteractionResolution
	unknownEffectFailure := false
	err = actor.apply(ctx, func() error {
		if actor.state.view.Activity != ThreadActivityActive || actor.state.turnID != turnID || runID != "" && actor.state.runID != runID {
			return nil
		}
		// A user stop binds the current run of the selected turn under the same
		// lock as cancellation. An answered interaction may have advanced it.
		// Execution-context cancellation still supplies an exact run identity.
		runID = actor.state.runID
		entries, err := service.host.store.repo.Entries(ctx, threadID.String())
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Type == sessiontree.EntryCancelRequested && entry.TurnID == turnID.String() && entry.RunID == runID.String() {
				request = cancellationRequest{EntryID: entry.ID, RequestKey: entry.RequestKey, RequestFingerprint: entry.RequestFingerprint}
				break
			}
		}
		terminalID := stableCancellationEntryID(threadID, turnID, runID)
		metadata := map[string]string{
			"run_id": runID.String(), "outcome": "cancelled",
			sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureCancelled,
		}
		if source == "snapshot_restore" {
			metadata[restoredFromBackupMetadata] = "true"
		}
		outcomePayload, err := json.Marshal(struct {
			ThreadID identity.ThreadID `json:"thread_id"`
			TurnID   identity.TurnID   `json:"turn_id"`
			RunID    identity.RunID    `json:"run_id"`
			Terminal string            `json:"terminal"`
			Status   string            `json:"status"`
		}{ThreadID: threadID, TurnID: turnID, RunID: runID, Terminal: terminalID, Status: "cancelled"})
		if err != nil {
			return err
		}
		result, err := repo.CancelTurn(ctx, sessiontree.CancelTurnRequest{
			ThreadID: threadID.String(), TurnID: turnID.String(), RunID: runID.String(),
			CancelEntryID: request.EntryID, TerminalEntryID: terminalID,
			RequestKey: request.RequestKey, RequestFingerprint: request.RequestFingerprint,
			OutcomeFingerprint:           sessiontree.StableHash(string(outcomePayload)),
			InteractionResolutionPayload: resolutionPayload, Metadata: metadata,
			CancellationMetadata:  map[string]string{"cancellation_source": source, "cancellation_mode": string(CancelModeImmediate)},
			PreserveProviderState: true, Now: time.Now().UTC(),
		})
		if err != nil {
			return err
		}
		if cancellation := threadCancellationForTurn([]sessiontree.Entry{result.CancelRequest}, turnID); cancellation != nil {
			actor.state.view.Cancellation = cancellation
		}
		unknownEffectFailure = result.Terminal.TurnStatus == sessiontree.TurnFailed &&
			strings.TrimSpace(result.Terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey]) == sessiontree.TurnFailureEffectOutcomeUnknown
		if unknownEffectFailure {
			resolution = InteractionResolution{Accepted: false, Outcome: "failed"}
		}
		for _, interaction := range actor.state.view.Interactions {
			if interaction.Resolved || interaction.TurnID != turnID {
				continue
			}
			actor.resolveInteraction(interaction.ID, resolution)
			if waiter := actor.state.pendingInteractions[interaction.ID]; waiter != nil {
				waiters = append(waiters, waiter.resolution)
				delete(actor.state.pendingInteractions, interaction.ID)
			}
		}
		actor.normalizeRunProgress()
		actor.state.view.ViewVersion++
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[request.RequestKey] = threadRuntimeRequest{fingerprint: request.RequestFingerprint}
		runCancel = actor.state.cancel
		return nil
	})
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	if runCancel != nil {
		service.recordCancellation(threadID, turnID, runID, source, reason)
		runCancel(context.Canceled)
	}
	for _, waiter := range waiters {
		waiter <- resolution
	}
	if unknownEffectFailure {
		service.finishSend(actor, turnID, runID, TurnResult{
			Status: TurnStatusFailed,
			Failure: &ThreadTurnFailure{
				Code:    ThreadTurnFailureEffectOutcomeUnknown,
				Message: sessiontree.EffectOutcomeUnknownFailureMessage,
			},
		}, ErrEffectOutcomeUnknown)
	} else {
		service.finishSend(actor, turnID, runID, TurnResult{Status: TurnStatusCancelled}, context.Canceled)
	}
	return service.currentResult(actor, mode), nil
}

func (service *threadRuntimeService) finishUnloadedCancellation(actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := "runtime-cancel:" + turnID.String()
	fingerprint, _ := stableFingerprint(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		TurnID   identity.TurnID   `json:"turn_id"`
	}{threadID, turnID})
	request := cancellationRequest{EntryID: "cancel:" + key, RequestKey: key, RequestFingerprint: fingerprint}
	if _, err := service.settleCancellation(ctx, actor, threadID, turnID, runID, request, "execution_context", "execution context cancelled"); err != nil && !errors.Is(err, sessiontree.ErrStaleAuthority) {
		service.host.store.reportBackgroundError(err)
	}
}

// finishUnloadedTerminal is used when no agent-harness runner remains to
// persist the terminal marker. Canonical settlement must precede the live
// actor transition so a preparation failure cannot look complete in memory
// while the journal still contains an active turn.
func (service *threadRuntimeService) finishUnloadedTerminal(executionCtx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, completed TurnResult, runErr error) {
	if completed.Status == TurnStatusCancelled || executionCtx.Err() != nil && (errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)) {
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
	hash := sessiontree.StableHash(strings.Join([]string{threadID.String(), turnID.String(), runID.String(), "preparation_failed"}, "\x00"))
	terminalID := "terminal-preparation-failed-" + hash[:24]
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

func (service *threadRuntimeService) respondResult(ctx context.Context, threadID identity.ThreadID, answers []InteractionAnswer, requestKey string, mode threadResultMode) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.viewResult(ctx, threadID, mode); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	var current ThreadView
	var interactions []ThreadInteraction
	if err := actor.apply(ctx, func() error {
		service.flushUpdateLocked(actor)
		current = threadResult(actor.state.view, mode)
		interactions = cloneThreadInteractions(actor.state.view.Interactions)
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	byID := make(map[string]ThreadInteraction, len(interactions))
	for _, interaction := range interactions {
		byID[interaction.ID] = interaction
	}
	entries := make([]sessiontree.Entry, 0, len(answers))
	resolutions := make(map[string]InteractionResolution, len(answers))
	secretInputs := make(map[string]map[string]string, len(answers))
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
		resolution, secretInput, resolutionErr := canonicalInteractionResolution(interaction, answer)
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
		if len(secretInput) != 0 {
			secretInputs[id] = secretInput
		}
		pendingAnswers = append(pendingAnswers, answer)
		payload, marshalErr := json.Marshal(resolution)
		if marshalErr != nil {
			return ThreadView{}, marshalErr
		}
		entries = append(entries, sessiontree.Entry{
			ID: "interaction-resolved:" + id, ThreadID: threadID.String(), TurnID: interaction.TurnID.String(),
			RunID: interaction.RunID.String(),
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
	resumedLocally := make(map[string]bool, len(resolutions))
	type waiterDelivery struct {
		waiter     chan InteractionResolution
		resolution InteractionResolution
	}
	waiters := make([]waiterDelivery, 0, len(resolutions))
	if err := actor.apply(ctx, func() error {
		if existing, found := actor.state.requestKeys[requestKey]; found {
			if existing.fingerprint != answerFingerprint {
				return &RequestConflictError{Operation: "respond", RequestID: requestKey, Err: ErrRequestConflict}
			}
			replayedRespond = true
			return nil
		}
		if actor.state.view.Cancellation != nil || len(actor.state.steering) > 0 {
			return ErrRequestConflict
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
			actor.resolveInteraction(id, resolution)
			if pending := actor.state.pendingInteractions[id]; pending != nil {
				delete(actor.state.pendingInteractions, id)
				resumedLocally[id] = true
				waiters = append(waiters, waiterDelivery{waiter: pending.resolution, resolution: resolution})
			}
		}
		actor.normalizeRunProgress()
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
		return service.currentResult(actor, mode), nil
	}
	for _, delivery := range waiters {
		delivery.waiter <- delivery.resolution
	}
	view := service.currentResult(actor, mode)
	go service.resumeCanonicalInteractions(actor, threadID, resolutions, secretInputs, pendingAnswers, byID, resumedLocally)
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

func (service *threadRuntimeService) resumeCanonicalInteractions(actor *threadRuntimeState, threadID identity.ThreadID, resolutions map[string]InteractionResolution, secretInputs map[string]map[string]string, answers []InteractionAnswer, interactions map[string]ThreadInteraction, resumedLocally map[string]bool) {
	recoverApproval := false
	for _, answer := range answers {
		interactionID := strings.TrimSpace(answer.InteractionID)
		interaction := interactions[interactionID]
		if interaction.Kind == ThreadInteractionInput {
			service.continueCanonicalInput(actor, threadID, interaction, resolutions[interactionID], secretInputs[interactionID])
		} else if interaction.Kind == ThreadInteractionApproval && !resumedLocally[interaction.ID] {
			recoverApproval = true
		}
	}
	if recoverApproval {
		service.recoverHydratedThread(threadID)
	}
}

func canonicalInteractionResolution(interaction ThreadInteraction, answer InteractionAnswer) (InteractionResolution, map[string]string, error) {
	resolution := InteractionResolution{Accepted: true, Approved: answer.Approved}
	if interaction.Kind != ThreadInteractionInput {
		return resolution, nil, nil
	}
	if interaction.Input == nil {
		return InteractionResolution{}, nil, ErrAuthorityCorrupt
	}
	questions := make(map[string]InputQuestion, len(interaction.Input.Questions))
	for _, question := range interaction.Input.Questions {
		questions[strings.TrimSpace(question.ID)] = question
	}
	public := make(map[string]string)
	secret := make(map[string]string)
	for rawID, value := range answer.Input {
		id := strings.TrimSpace(rawID)
		question, ok := questions[id]
		if !ok || id == "" {
			return InteractionResolution{}, nil, fmt.Errorf("%w: input answer targets unknown question %q", ErrRequestConflict, rawID)
		}
		if question.Secret {
			resolution.Redacted = true
			secret[id] = value
			continue
		}
		public[id] = value
	}
	if interaction.toolRequestedInput {
		if len(public) != len(questions) || len(answer.Input) != len(questions) {
			return InteractionResolution{}, nil, fmt.Errorf("%w: every tool input question requires an answer", ErrRequestConflict)
		}
		for id, question := range questions {
			value := public[id]
			if strings.TrimSpace(value) == "" || (question.Kind == "select" && !slices.Contains(question.Options, value)) {
				return InteractionResolution{}, nil, fmt.Errorf("%w: invalid answer for tool input question %q", ErrRequestConflict, id)
			}
		}
	}
	if len(public) != 0 {
		resolution.Input = public
	}
	return resolution, secret, nil
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

func (service *threadRuntimeService) continueCanonicalInput(actor *threadRuntimeState, threadID identity.ThreadID, interaction ThreadInteraction, resolution InteractionResolution, secretInput map[string]string, steering ...bool) {
	payload, err := json.Marshal(resolution)
	if err != nil {
		return
	}
	var supplemental []TurnSupplementalContextItem
	if len(secretInput) != 0 {
		secretPayload, marshalErr := json.Marshal(secretInput)
		if marshalErr != nil {
			return
		}
		supplemental = []TurnSupplementalContextItem{{
			Kind: "user_answer", Title: "Secret answers to the pending question", Text: string(secretPayload), Sensitive: true,
		}}
	}
	waitingRunID := interaction.RunID
	continuationKey := "continue-input:" + interaction.ID
	isSteering := len(steering) > 0 && steering[0]
	if isSteering {
		continuationKey = "continue-steering:" + interaction.ID
	}
	runID, err := service.host.idSource.NewRunID()
	if err != nil {
		return
	}
	executionCtx, cancel := context.WithCancelCause(context.Background())
	executionDone := make(chan struct{})
	// The waiting execution owns finalization and cancellation until it drains.
	// Publishing a successor earlier lets Stop cancel a Run that has no
	// canonical marker while its predecessor can still commit a terminal.
	var previousExecution <-chan struct{}
	if err := actor.apply(context.Background(), func() error {
		previousExecution = actor.state.executionDone
		return nil
	}); err != nil {
		cancel(context.Canceled)
		return
	}
	if previousExecution != nil {
		<-previousExecution
	}
	claimed := false
	claimErr := actor.apply(context.Background(), func() error {
		if actor.state.turnID != interaction.TurnID || actor.state.runID != waitingRunID || actor.state.view.Activity != ThreadActivityActive || actor.state.view.Cancellation != nil {
			return nil
		}
		for _, pending := range actor.state.view.Interactions {
			if pending.Kind == ThreadInteractionInput && !pending.Resolved && !isSteering {
				return nil
			}
		}

		if isSteering {
			// Resolve suspended interactions before installing a continuation Run.
			if err := service.resolveSteeringInteractions(context.Background(), actor); err != nil {
				return err
			}
		}
		reason := "input_resume"
		if isSteering {
			reason = "steering_resume"
		}
		if err := service.appendRuntimeFactsError(context.Background(), threadID, []sessiontree.Entry{{
			ID: "continue-run:" + runID.String(), Type: sessiontree.EntryTurnMarker,
			ThreadID: threadID.String(), TurnID: interaction.TurnID.String(), RunID: runID.String(),
			TurnStatus: sessiontree.TurnSavePoint, Metadata: map[string]string{"reason": reason},
		}}); err != nil {
			return err
		}
		pendingSteering := slices.Clone(actor.state.steering)
		kind := actor.state.view.TurnKind
		var beginErr error
		_, beginErr = actor.beginRun(beginThreadRunInput{
			turnID: interaction.TurnID, runID: runID, logicalRequestID: identity.LogicalRequestID(continuationKey),
			cancel: cancel, executionDone: executionDone, turnKind: kind,
		})
		if beginErr != nil {
			return beginErr
		}
		actor.state.steering = pendingSteering
		claimed = true
		return nil
	})
	if !claimed {
		close(executionDone)
		if claimErr != nil {
			service.finishUnloadedTerminal(executionCtx, actor, threadID, interaction.TurnID, waitingRunID, TurnResult{}, claimErr)
		}
		cancel(context.Canceled)
		return
	}
	defer cancel(context.Canceled)
	defer close(executionDone)
	_, canonicalInput, canonicalErr := service.retrySource(executionCtx, threadID, interaction.TurnID)
	kind := service.currentView(actor).TurnKind
	if canonicalErr != nil {
		service.finishUnloadedTerminal(executionCtx, actor, threadID, interaction.TurnID, runID, TurnResult{}, canonicalErr)
		return
	}
	agent, err := service.factory.Agent(executionCtx, AgentRequest{
		TurnKind: kind, ThreadID: threadID, TurnID: interaction.TurnID, RequestKey: continuationKey,
		Input: UserInput{Text: string(payload)}, CanonicalTurnInput: canonicalInput, InteractionID: interaction.ID,
	})
	if err != nil || agent == nil {
		if err == nil {
			err = errors.New("thread runtime agent is unavailable")
		}
		service.finishUnloadedTerminal(executionCtx, actor, threadID, interaction.TurnID, runID, TurnResult{}, err)
		return
	}
	runner, err := service.host.turnRunner(executionCtx, threadID, service.executionAgent(actor, agent))
	if err != nil {
		service.finishUnloadedTerminal(executionCtx, actor, threadID, interaction.TurnID, runID, TurnResult{}, err)
		return
	}
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(continuationKey), TurnID: interaction.TurnID, RunID: runID,
		ManualCompactions: agent.manualCompactions, SupplementalContext: supplemental,
	}
	applyAgentExecutionPolicy(&request, agent)
	service.executeInputResume(executionCtx, actor, runner, threadID, resumeInputRequest{
		TurnID: interaction.TurnID, WaitingRunID: waitingRunID, RunID: runID, Answer: string(payload), Steering: isSteering,
		Options: request,
	})
}

func (service *threadRuntimeService) executeInputResume(ctx context.Context, actor *threadRuntimeState, runner *turnRunnerHandle, threadID identity.ThreadID, request resumeInputRequest) {
	result, runErr := runner.ResumeInput(ctx, request)
	if result.Status == "" && runErr != nil {
		service.finishUnloadedTerminal(ctx, actor, threadID, request.TurnID, request.RunID, result, runErr)
		return
	}
	service.finishSend(actor, request.TurnID, request.RunID, result, runErr)
}

func (service *threadRuntimeService) retryResult(ctx context.Context, threadID identity.ThreadID, sourceTurnID identity.TurnID, requestKey string, mode threadResultMode) (ThreadView, error) {
	if _, err := identity.ParseTurnID(sourceTurnID.String()); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.viewResult(ctx, threadID, mode); err != nil {
		return ThreadView{}, err
	}
	sourceEntryID, sourceInput, err := service.retrySource(ctx, threadID, sourceTurnID)
	if err != nil {
		return ThreadView{}, err
	}
	sourceEntry, err := service.host.store.repo.Entry(ctx, threadID.String(), sourceEntryID)
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	read, err := service.host.store.repo.(sessiontree.CanonicalTurnReadRepo).ReadCanonicalTurn(ctx, threadID.String(), sourceTurnID.String())
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	kind := TurnKind(read.Turn.Entries[0].Entry.Metadata[sessiontree.TurnKindMetadataKey])
	fingerprint, err := stableFingerprint(sourceTurnID)
	if err != nil {
		return ThreadView{}, err
	}
	turnID, runID := retryExecutionIDs(threadID, requestKey)
	executionCtx, cancel := context.WithCancelCause(context.Background())
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
			result = threadResult(actor.state.view, mode)
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		var beginErr error
		previousExecution, beginErr = actor.beginRun(beginThreadRunInput{
			turnID: turnID, runID: runID, logicalRequestID: identity.LogicalRequestID(requestKey),
			cancel: cancel, executionDone: executionDone, clearOutcome: true, turnKind: kind,
		})
		if beginErr != nil {
			return beginErr
		}
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		result = threadResult(actor.state.view, mode)
		return nil
	})
	if err != nil {
		cancel(context.Canceled)
		return ThreadView{}, err
	}
	if replayed {
		cancel(context.Canceled)
		return result, nil
	}
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{TurnKind: kind, ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: sourceInput, CanonicalTurnInput: sourceInput, RetrySource: sourceTurnID})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID,
		TurnKind: kind, Input: sourceInput, RetrySourceTurnID: identity.TurnID(sourceEntry.TurnID), RetrySourceEntryID: sourceEntryID,
	}
	go service.acceptAndExecutePreparedSend(executionCtx, actor, prepared, threadID, request, previousExecution, executionDone, requestKey, true)
	return result, nil
}

func retryExecutionIDs(threadID identity.ThreadID, requestKey string) (identity.TurnID, identity.RunID) {
	hash := sessiontree.StableHash(threadID.String() + "\x00retry\x00" + strings.TrimSpace(requestKey))
	return identity.TurnID("retry-turn-" + hash), identity.RunID("retry-run-" + hash)
}

func (service *threadRuntimeService) retrySource(ctx context.Context, threadID identity.ThreadID, sourceTurnID identity.TurnID) (string, UserInput, error) {
	entries, err := service.host.store.repo.Entries(ctx, threadID.String())
	if err != nil {
		return "", UserInput{}, runtimeHostError(err)
	}
	for _, entry := range entries {
		if entry.TurnID == sourceTurnID.String() && entry.Metadata[sessiontree.TurnKindMetadataKey] == sessiontree.TurnKindContextCompaction {
			return "", UserInput{}, ErrNoRetryTarget
		}
		if entry.TurnID != sourceTurnID.String() {
			continue
		}
		if entry.Metadata[restoredFromBackupMetadata] == "true" {
			return "", UserInput{}, ErrRestoredTurn
		}
		if entry.Metadata[sessiontree.TurnFailureCodeMetadataKey] == sessiontree.TurnFailureEffectOutcomeUnknown {
			return "", UserInput{}, ErrEffectOutcomeUnknown
		}
	}
	reader, ok := service.host.store.repo.(sessiontree.CanonicalTurnReadRepo)
	if !ok {
		return "", UserInput{}, ErrUnsupportedStoreCapability
	}
	read, err := reader.ReadCanonicalTurn(ctx, threadID.String(), sourceTurnID.String())
	if err != nil {
		return "", UserInput{}, runtimeHostError(err)
	}
	sourceEntryID := retryContinuationEntryID(read.Turn)
	if sourceEntryID == "" {
		return "", UserInput{}, ErrAuthorityCorrupt
	}
	if read.Turn.Entries[0].Entry.Metadata[sessiontree.TurnKindMetadataKey] == sessiontree.TurnKindGoalContinuation {
		return sourceEntryID, UserInput{}, nil
	}
	path, err := service.host.store.repo.Path(ctx, threadID.String(), sourceEntryID)
	if err != nil {
		return "", UserInput{}, runtimeHostError(err)
	}
	for index := len(path) - 1; index >= 0; index-- {
		entry := path[index]
		if entry.Type == sessiontree.EntryUserMessage && entry.Message.Role == session.User {
			return sourceEntryID, turnInputFromSessionMessage(entry.Message), nil
		}
	}
	return "", UserInput{}, errors.New("retry source turn has no canonical user input")
}

func retryContinuationEntryID(turn sessiontree.CanonicalTurn) string {
	sourceEntryID := ""
	if len(turn.Entries) > 0 && turn.Entries[0].Entry.Metadata[sessiontree.TurnKindMetadataKey] == sessiontree.TurnKindGoalContinuation {
		sourceEntryID = turn.StartedEntryID
	}
	if turn.RetrySource != nil {
		sourceEntryID = turn.RetrySource.EntryID
	} else {
		for _, item := range turn.Entries {
			if item.Entry.Type == sessiontree.EntryUserMessage && item.Entry.Message.Role == session.User {
				sourceEntryID = item.Entry.ID
				break
			}
		}
	}
	failed := false
	uncertain := false
	lastUncertain := -1
	for index, item := range turn.Entries {
		if item.Entry.Message.ToolResult != nil && item.Entry.Message.ToolResult.EffectOutcome == tools.EffectOutcomeUncertain {
			uncertain = true
			lastUncertain = index
		}
		failed = failed || item.Entry.Type == sessiontree.EntryTurnMarker && item.Entry.TurnStatus == sessiontree.TurnFailed
	}
	if failed || uncertain {
		// Use the complete batch boundary after returned uncertainty. If the
		// process stopped before its save point, retain the admitted path rather
		// than moving back across a tool result whose effect is unconfirmed.
		if uncertain && len(turn.Entries) > 0 {
			sourceEntryID = turn.Entries[len(turn.Entries)-1].Entry.ID
		}
		for index := len(turn.Entries) - 1; index >= 0; index-- {
			entry := turn.Entries[index].Entry
			if index > lastUncertain && entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnSavePoint {
				sourceEntryID = entry.ParentID
				break
			}
		}
	}
	return sourceEntryID
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
		actor.state.view.ToolCallPreviews = nil
		actor.markComponents(threadPreviewsChanged)
		actor.state.view.ViewVersion++
		return nil
	})
}

func (service *threadRuntimeService) Subscribe(ctx context.Context) (*WorkspaceSubscription, error) {
	return service.subscribe(ctx, false)
}

func (service *threadRuntimeService) subscribe(ctx context.Context, wakeOnly bool) (*WorkspaceSubscription, error) {
	if err := service.host.available(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subscription := &WorkspaceSubscription{views: make(chan ThreadView, 64), done: make(chan struct{}), changed: make(chan struct{}, 1), service: service, wakeOnly: wakeOnly}
	service.mu.Lock()
	if service.subscriptionsClosed {
		service.mu.Unlock()
		return nil, ErrHostClosed
	}
	service.subscribers[subscription] = struct{}{}
	service.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			subscription.closeWithError(ctx.Err())
		case <-subscription.done:
		}
	}()
	return subscription, nil
}

func (service *threadRuntimeService) startNextQueued(actor *threadRuntimeState) {
	if service.host.requireExecution() != nil {
		return
	}
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
				return nil
			})
		}
	}()
}

func (service *threadRuntimeService) startAccepted(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, input UserInput, supplemental []TurnSupplementalContextItem, requestKey, promotedQueueID, promotionRequestKey string, steeringQueued ...*SteerQueuedInput) (ThreadView, error) {
	return service.startAcceptedResult(ctx, actor, threadID, input, supplemental, requestKey, promotedQueueID, promotionRequestKey, threadFullResult, steeringQueued...)
}

func (service *threadRuntimeService) startAcceptedResult(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, input UserInput, supplemental []TurnSupplementalContextItem, requestKey, promotedQueueID, promotionRequestKey string, mode threadResultMode, steeringQueued ...*SteerQueuedInput) (ThreadView, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
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
	if len(steeringQueued) > 0 {
		promotionFingerprint, err = stableFingerprint(*steeringQueued[0])
		if err != nil {
			return ThreadView{}, err
		}
	}
	turnID, runID, err := service.host.nextTurnRunIDs()
	if err != nil {
		return ThreadView{}, err
	}
	executionCtx, cancel := context.WithCancelCause(context.Background())
	executionDone := make(chan struct{})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		SupplementalContext:         cloneTurnSupplementalContext(supplemental),
		InputFingerprint:            inputFingerprint,
		PromotedQueueID:             promotedQueueID,
		PromotionRequestKey:         promotionRequestKey,
		PromotionRequestFingerprint: promotionFingerprint,
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
					if len(steeringQueued) > 0 {
						expected, _ := stableFingerprint(steeringQueued[0].ExpectedInput)
						actual, _ := stableFingerprint(item.Input)
						if expected != actual || len(item.SupplementalContext) != 0 {
							return ErrRequestConflict
						}
					}
					// Resolve the pending input while admitting it, so an edit that
					// won the actor lock cannot be replaced by a captured old copy.
					input = item.Input
					supplemental = item.SupplementalContext
					inputFingerprint, err = stableFingerprint(input)
					if err != nil {
						return err
					}
					request.Input = input
					request.SupplementalContext = cloneTurnSupplementalContext(supplemental)
					request.InputFingerprint = inputFingerprint
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
		var beginErr error
		previousExecution, beginErr = actor.beginRun(beginThreadRunInput{
			turnID: turnID, runID: runID, logicalRequestID: identity.LogicalRequestID(requestKey),
			cancel: cancel, executionDone: executionDone, clearOutcome: true,
		})
		if beginErr != nil {
			return beginErr
		}
		if promotedQueueID != "" {
			for index := range actor.state.view.Queue {
				if actor.state.view.Queue[index].ID == promotedQueueID {
					actor.state.view.Queue = append(actor.state.view.Queue[:index], actor.state.view.Queue[index+1:]...)
					actor.markComponents(threadQueueChanged)
					break
				}
			}
		}
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: "user:" + requestKey, TurnID: turnID, RunID: runID, Kind: ThreadItemUser, Text: input.Text, CreatedAt: time.Now().UTC(), Attachments: cloneMessageAttachments(input.Attachments), References: append([]MessageReference(nil), input.References...), Context: append([]MessageContextItem(nil), input.Context...)})
		actor.markItem("user:"+requestKey, ThreadItemAdded)
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: inputFingerprint}
		if promotionRequestKey != "" {
			actor.state.requestKeys[promotionRequestKey] = threadRuntimeRequest{fingerprint: promotionFingerprint}
		}
		if err := service.advanceCanonical(context.Background(), actor); err != nil {
			actor.state.contextErr = err
		}
		result = threadResult(actor.state.view, mode)
		return nil
	})
	if err != nil {
		cancel(context.Canceled)
		return ThreadView{}, err
	}
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{
		ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input, CanonicalTurnInput: input,
	})
	go service.executePreparedSend(executionCtx, actor, prepared, accepted, request, previousExecution, executionDone)
	return result, nil
}

func cloneThreadRuntimeView(view ThreadView) ThreadView {
	view.Items = cloneThreadItems(view.Items)
	return cloneThreadRuntimeFields(view)
}

func cloneThreadRuntimeFields(view ThreadView) ThreadView {
	view.Context = cloneThreadContextSnapshot(view.Context)
	if view.Goal != nil {
		copy := *view.Goal
		view.Goal = &copy
	}
	if view.ParentEvents != nil {
		page := *view.ParentEvents
		page.Events = make([]ParentEvent, len(view.ParentEvents.Events))
		copy(page.Events, view.ParentEvents.Events)
		view.ParentEvents = &page
	}
	view.Cancellation = cloneThreadCancellation(view.Cancellation)
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
	view.Turns = cloneThreadTurns(view.Turns)
	view.Queue = append([]QueuedInput(nil), view.Queue...)
	for index := range view.Queue {
		view.Queue[index].Input.Attachments = cloneMessageAttachments(view.Queue[index].Input.Attachments)
		view.Queue[index].Input.References = append([]MessageReference(nil), view.Queue[index].Input.References...)
		view.Queue[index].Input.Context = append([]MessageContextItem(nil), view.Queue[index].Input.Context...)
		view.Queue[index].Input.ModelContextSnapshot = runtimeModelContext(sessionModelContext(view.Queue[index].Input.ModelContextSnapshot))
		view.Queue[index].SupplementalContext = cloneTurnSupplementalContext(view.Queue[index].SupplementalContext)
	}
	if view.RestoredInputs != nil {
		items := make([]RestoredInput, len(view.RestoredInputs))
		for i, item := range view.RestoredInputs {
			items[i] = item
			items[i].Input.Attachments = cloneMessageAttachments(item.Input.Attachments)
			items[i].Input.References = append([]MessageReference(nil), item.Input.References...)
			items[i].Input.Context = append([]MessageContextItem(nil), item.Input.Context...)
			items[i].Input.ModelContextSnapshot = runtimeModelContext(sessionModelContext(item.Input.ModelContextSnapshot))
		}
		view.RestoredInputs = items
	}
	view.Interactions = cloneThreadInteractions(view.Interactions)
	view.Failure = cloneThreadTurnFailure(view.Failure)
	view.ToolCallPreviews = cloneToolCallPreviews(view.ToolCallPreviews)
	view.RunProgress = cloneThreadRunProgress(view.RunProgress)
	return view
}

func cloneThreadRunProgress(progress *ThreadRunProgress) *ThreadRunProgress {
	if progress == nil {
		return nil
	}
	copy := *progress
	copy.Retry = cloneProviderRetryProgress(progress.Retry)
	copy.Transport = cloneProviderTransportProgress(progress.Transport)
	if progress.Request != nil {
		request := *progress.Request
		if request.LastProgressAt != nil {
			at := *request.LastProgressAt
			request.LastProgressAt = &at
		}
		if request.FirstOutputAt != nil {
			at := *request.FirstOutputAt
			request.FirstOutputAt = &at
		}
		copy.Request = &request
	}
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

func cloneThreadItems(items []ThreadItem) []ThreadItem {
	out := make([]ThreadItem, len(items))
	for index, item := range items {
		out[index] = item
		if item.Body != nil {
			body := *item.Body
			out[index].Body = &body
		}
		if item.Goal != nil {
			copy := *item.Goal
			out[index].Goal = &copy
		}
		if item.Steering != nil {
			steering := *item.Steering
			out[index].Steering = &steering
		}
		out[index].Sources = runtimeSourceRefs(citation.Clone(providerSourceRefs(item.Sources)))
		out[index].sourceEntryIDs = append([]string(nil), item.sourceEntryIDs...)
		out[index].activityEntryIDs = append([]string(nil), item.activityEntryIDs...)
		out[index].ActivityBodies = append([]ThreadActivityBody(nil), item.ActivityBodies...)
		out[index].Attachments = cloneMessageAttachments(item.Attachments)
		out[index].References = append([]MessageReference(nil), item.References...)
		out[index].Context = append([]MessageContextItem(nil), item.Context...)
		if item.Activity != nil {
			timeline := observation.CloneActivityTimeline(&observation.ActivityTimeline{Items: []observation.ActivityItem{*item.Activity}})
			out[index].Activity = &timeline.Items[0]
		}
		if item.Interaction != nil {
			interaction := cloneThreadInteraction(*item.Interaction)
			out[index].Interaction = &interaction
		}
	}
	return out
}

func cloneThreadInteractions(items []ThreadInteraction) []ThreadInteraction {
	out := slices.Clone(items)
	for index := range out {
		out[index] = cloneThreadInteraction(out[index])
	}
	return out
}

func cloneThreadInteraction(item ThreadInteraction) ThreadInteraction {
	item.Input = cloneInputPresentation(item.Input)
	if item.Approval != nil {
		approval := *item.Approval
		approval.Effects = slices.Clone(approval.Effects)
		approval.Targets = slices.Clone(approval.Targets)
		item.Approval = &approval
	}
	if item.Approved != nil {
		approved := *item.Approved
		item.Approved = &approved
	}
	if item.Resolution != nil {
		resolution := *item.Resolution
		resolution.Input = maps.Clone(resolution.Input)
		if resolution.Approved != nil {
			approved := *resolution.Approved
			resolution.Approved = &approved
		}
		item.Resolution = &resolution
	}
	return item
}

func cloneInputPresentation(input *InputPresentation) *InputPresentation {
	if input == nil {
		return nil
	}
	out := *input
	out.RequiredFromUser = slices.Clone(input.RequiredFromUser)
	out.EvidenceRefs = slices.Clone(input.EvidenceRefs)
	out.Questions = slices.Clone(input.Questions)
	for index := range out.Questions {
		question := &out.Questions[index]
		question.Options = slices.Clone(question.Options)
		question.Choices = slices.Clone(question.Choices)
		if question.ChoicesExhaustive != nil {
			exhaustive := *question.ChoicesExhaustive
			question.ChoicesExhaustive = &exhaustive
		}
	}
	return &out
}

func hydrateCanonicalQueue(ctx context.Context, repo sessiontree.JournalRepo, threadID identity.ThreadID) ([]QueuedInput, error) {
	queue, _, err := hydrateThreadQueues(ctx, repo, threadID)
	return queue, err
}

func canonicalQueueFromEntries(entries []sessiontree.Entry) ([]QueuedInput, error) {
	return appendCanonicalQueue(make([]QueuedInput, 0), entries)
}

func appendCanonicalQueue(queue []QueuedInput, entries []sessiontree.Entry) ([]QueuedInput, error) {
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
	return threadRuntimeItemsWithAnchors(entries, nil)
}

func threadRuntimeItemsWithAnchors(entries []sessiontree.Entry, anchors map[string]string) ([]ThreadItem, []ThreadInteraction, error) {
	projection := newThreadItemProjection()
	if err := projection.append(entries, anchors); err != nil {
		return nil, nil, err
	}
	items, interactions := projection.snapshot()
	return items, interactions, nil
}

func threadRuntimeEntryIdentity(entry sessiontree.Entry) (identity.TurnID, identity.RunID, error) {
	turnID := identity.TurnID(strings.TrimSpace(entry.TurnID))
	runID := identity.RunID(strings.TrimSpace(entry.RunID))
	if turnID == "" || runID == "" {
		return "", "", ErrAuthorityCorrupt
	}
	return turnID, runID, nil
}

func threadExecutionKey(turnID identity.TurnID, runID identity.RunID) string {
	return turnID.String() + ":" + runID.String()
}

func threadToolKey(turnID identity.TurnID, runID identity.RunID, toolCallID string) string {
	return threadExecutionKey(turnID, runID) + ":" + strings.TrimSpace(toolCallID)
}

func canonicalToolDispatchIndex(items []ThreadItem, byInvocation map[string]int, entry sessiontree.Entry) (int, error) {
	callID := entry.Message.ToolCallID
	if callID == "" {
		callID = entry.Metadata["tool_id"]
	}
	if entry.RunID != "" {
		if index, found := byInvocation[threadToolKey(identity.TurnID(entry.TurnID), identity.RunID(entry.RunID), callID)]; found {
			return index, nil
		}
		return 0, ErrAuthorityCorrupt
	}
	// An observation can supply timing only for one canonical invocation;
	// its metadata never establishes or advances execution identity.
	index := -1
	for i, item := range items {
		if item.TurnID.String() == entry.TurnID && item.Activity != nil && item.Activity.Kind == observation.ActivityKindTool && item.Activity.ToolID == callID {
			if index != -1 {
				return 0, ErrAuthorityCorrupt
			}
			index = i
		}
	}
	if index == -1 {
		return 0, ErrAuthorityCorrupt
	}
	return index, nil
}

func activityItemFromCanonicalEntry(entry sessiontree.Entry, occurredAt time.Time) observation.ActivityItem {
	status := observation.ActivityStatusRunning
	endedAt := int64(0)
	if entry.Type == sessiontree.EntryToolResult {
		status = observation.ActivityStatusSuccess
		endedAt = occurredAt.UnixMilli()
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
	presentation := tools.CloneActivityPresentation(entry.Message.Activity)
	if presentation == nil {
		label := strings.TrimSpace(entry.Message.ToolName)
		if label == "" {
			label = "Tool activity"
		}
		presentation = &tools.ActivityPresentation{
			Label:    label,
			Renderer: tools.ActivityRendererStructured,
			Payload: tools.StructuredActivityPayload{
				Status:      string(status),
				Operation:   "tool",
				DisplayName: label,
				Summary:     "Historical tool activity",
			},
		}
	}
	return observation.ActivityItem{ItemID: entry.ID, ToolID: entry.Message.ToolCallID, ToolName: entry.Message.ToolName, Kind: observation.ActivityKindTool, Status: status, Presentation: presentation, EndedAtUnixMS: endedAt}
}

func threadToolSegmentID(turnID identity.TurnID, toolCallID string) string {
	return "tool:" + turnID.String() + ":" + strings.TrimSpace(toolCallID)
}

type threadControlIdentity struct {
	turnID string
	runID  string
}

func waitingInputFromControlSignal(signal *session.ControlSignalView) (*InputPresentation, controlstate.Classification, error) {
	classification := controlstate.Classify(signal)
	switch classification {
	case controlstate.WaitingInput:
		presentation := inputPresentationFromControlSignal(signal.OutputText, signal.Payload)
		if len(presentation.Questions) == 0 {
			return nil, controlstate.Invalid, ErrAuthorityCorrupt
		}
		return presentation, classification, nil
	case controlstate.Invalid:
		return nil, classification, fmt.Errorf("%w: invalid waiting ask_user control signal %q", ErrAuthorityCorrupt, strings.TrimSpace(signal.CallID))
	default:
		return nil, classification, nil
	}
}

func inputPresentationFromControlSignal(text string, payload map[string]any) *InputPresentation {
	presentation := &InputPresentation{Summary: strings.TrimSpace(text)}
	if payload == nil {
		return presentation
	}
	payload, _ = control.NormalizeAskUserPayload(payload)
	presentation.ReasonCode = optionalString(payload["reason_code"])
	for _, field := range []struct {
		name string
		out  *[]string
	}{{"required_from_user", &presentation.RequiredFromUser}, {"evidence_refs", &presentation.EvidenceRefs}} {
		if values, ok := payload[field.name].([]any); ok {
			for _, value := range values {
				if text, ok := value.(string); ok {
					*field.out = append(*field.out, text)
				}
			}
		}
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
				ID:               optionalString(item["id"]),
				Header:           optionalString(item["header"]),
				WritePlaceholder: optionalString(item["write_placeholder"]),
				Prompt:           optionalString(item["question"]),
				Kind:             optionalString(item["response_mode"]),
				WriteLabel:       optionalString(item["write_label"]),
				Secret:           item["is_secret"] == true,
			}
			if exhaustive, ok := item["choices_exhaustive"].(bool); ok {
				question.ChoicesExhaustive = &exhaustive
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
						question.Choices = append(question.Choices, InputChoice{
							ChoiceID: optionalString(choice["choice_id"]),
							Value:    label, Label: label, Description: optionalString(choice["description"]),
						})
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
	projection := threadLifecycleProjection{}
	for _, entry := range path {
		projection.observe(entry)
	}
	return projection.snapshot()
}

func cloneThreadTurnFailure(failure *ThreadTurnFailure) *ThreadTurnFailure {
	if failure == nil {
		return nil
	}
	cloned := *failure
	cloned.Reason = classifyThreadTurnFailureReason(cloned.Code, cloned.Message)
	cloned.ProviderTimeout = providerattempt.CloneTimeoutDetails(failure.ProviderTimeout)
	return &cloned
}

func toolInputInteraction(entry sessiontree.Entry, ready map[string]bool) (*ThreadInteraction, error) {
	if !ready[entry.ID] {
		return nil, nil
	}
	request := entry.Message.ToolResult.InputRequired
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("%w: invalid tool input request", ErrAuthorityCorrupt)
	}
	turnID, runID, err := threadRuntimeEntryIdentity(entry)
	if err != nil {
		return nil, err
	}
	input := &InputPresentation{Summary: request.Summary}
	for _, q := range request.Questions {
		input.Questions = append(input.Questions, InputQuestion{ID: q.ID, Prompt: q.Prompt, Kind: q.Kind, Options: append([]string(nil), q.Options...), WriteLabel: q.WriteLabel})
	}
	return &ThreadInteraction{toolRequestedInput: true, ID: sessiontree.ToolInputInteractionID(entry.ID), TurnID: turnID, RunID: runID, Kind: ThreadInteractionInput, ToolCallID: entry.Message.ToolCallID, Input: input}, nil
}

func turnKindFromEntries(entries []sessiontree.Entry, turnID identity.TurnID) TurnKind {
	for _, entry := range entries {
		if entry.TurnID == turnID.String() && entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			return TurnKind(entry.Metadata[sessiontree.TurnKindMetadataKey])
		}
	}
	return ""
}

func (sink combinedThreadEventSink) commitProviderOutput(ctx context.Context, commit agentharness.ProviderOutputCommitted) error {
	return sink.runtime.commitProviderOutput(ctx, commit)
}

func (sink threadRuntimeEventSink) commitProviderOutput(ctx context.Context, commit agentharness.ProviderOutputCommitted) error {
	if sink.service == nil || commit.ThreadID == "" {
		return errors.New("response commit requires a thread service")
	}
	threadID := identity.ThreadID(commit.ThreadID)
	actor := sink.service.runtime(threadID)
	committedSources := make(map[string]bool)
	for _, entry := range commit.Entries {
		if entry.ThreadID != commit.ThreadID || entry.TurnID != commit.TurnID || entry.RunID != commit.RunID {
			return errors.New("response commit identity mismatch")
		}
		if entry.Type == sessiontree.EntryCustom && entry.Metadata["kind"] == sessiontree.HostedToolEntryKind {
			observed, err := sessiontree.HostedToolObservation(entry)
			if err != nil {
				return err
			}
			committedSources["hosted:"+threadToolKey(observed.TurnID, observed.RunID, observed.ToolID)] = true
		}
		for _, source := range []string{entry.Message.ContentSourceID, entry.Message.ReasoningSourceID} {
			if source != "" {
				committedSources[source] = true
			}
		}
	}
	err := actor.apply(ctx, func() error {
		if identity.TurnID(commit.TurnID) != actor.state.turnID || identity.RunID(commit.RunID) != actor.state.runID {
			return ErrStaleAuthority
		}
		if err := sink.service.advanceCanonical(ctx, actor); err != nil {
			return err
		}

		// Only this committed response relinquishes draft ownership.
		for source := range committedSources {
			delete(actor.state.attemptDraftIDs, source)
		}
		if committedSources[actor.state.openTextSegmentID] {
			actor.finishLiveTextSegment()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
