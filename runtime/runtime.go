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
	"github.com/floegence/floret/internal/session/artifact"
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
type ForkOperationID string
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
	// ErrThreadNotFound reports that a durable thread requested through Host was not found.
	ErrThreadNotFound = errors.New("floret thread not found")
	// ErrTurnNotFound reports that a durable turn requested through Host was not found.
	ErrTurnNotFound = errors.New("floret turn not found")
	// ErrRunNotFound reports that a durable run requested through Host was not found.
	ErrRunNotFound = errors.New("floret run not found")
	// ErrSubAgentNotFound reports that a parent-scoped child thread requested through Host was not found.
	ErrSubAgentNotFound = errors.New("floret subagent not found")
	// ErrForkOperationConflict reports that an operation ID was reused with a different fork request.
	ErrForkOperationConflict = errors.New("floret fork operation conflicts with existing request")
	// ErrForkDestinationConflict reports that a planned destination is owned by another operation or node.
	ErrForkDestinationConflict = errors.New("floret fork destination conflicts with operation plan")
	// ErrForkOperationTargetMissing reports that a completed operation no longer has every marked target.
	ErrForkOperationTargetMissing = errors.New("floret fork operation target is missing")
	// ErrAgentTodoVersionConflict reports that a todo update was based on a stale canonical version.
	ErrAgentTodoVersionConflict = errors.New("floret agent todo version conflict")
	// ErrJournalInvariant reports an ambiguous active path that Floret refuses to repair heuristically.
	ErrJournalInvariant = errors.New("floret thread journal invariant violated")
)

// Host is the concrete public facade for provider-backed durable conversations.
// Callers that need substitution should declare a local interface containing
// only the methods used by that responsibility.
type Host struct {
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

type HostOptions struct {
	Config               config.Config
	ModelGateway         ModelGateway
	ModelGatewayIdentity ModelGatewayIdentity
	Runtime              *HostRuntime
	Tools                *tools.Registry
	Approver             tools.Approver
	Sink                 EventSink
	ToolSurfaceProvider  ToolSurfaceProvider
	IDGenerator          func(string) string
	LoopLimits           LoopLimits
	SubAgentRunTimeout   time.Duration
	Capabilities         CapabilityOptions
	ThreadTitleMode      ThreadTitleMode
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
	ThreadID ThreadID
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
	Turns       []ForkedTurnRef `json:"turns,omitempty"`
}

type ForkedTurnRef struct {
	SourceTurnID      TurnID    `json:"source_turn_id,omitempty"`
	SourceRunID       RunID     `json:"source_run_id,omitempty"`
	DestinationTurnID TurnID    `json:"destination_turn_id,omitempty"`
	DestinationRunID  RunID     `json:"destination_run_id,omitempty"`
	CreatedAt         time.Time `json:"created_at,omitempty"`
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
}

func (i TurnInput) Validate() error {
	if strings.TrimSpace(i.Text) == "" && len(i.Attachments) == 0 {
		return errors.New("turn input requires text or attachments")
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
	return nil
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
	ThreadID   ThreadID
	TurnID     TurnID
	RunID      RunID
	ToolCallID string
	ToolName   string
	Handle     string
	Status     PendingToolCompletionStatus
	Summary    string
	Output     string
	Labels     RunLabels
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
	ThreadID   ThreadID `json:"thread_id"`
	TurnID     TurnID   `json:"turn_id"`
	RunID      RunID    `json:"run_id"`
	ToolCallID string   `json:"tool_call_id"`
	ToolName   string   `json:"tool_name"`
	Handle     string   `json:"handle"`
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
	SubAgentStatusClosed      SubAgentStatus = "closed"
)

type SubAgentForkMode string

const (
	SubAgentForkNone     SubAgentForkMode = "none"
	SubAgentForkFullPath SubAgentForkMode = "full_path"
)

type SpawnSubAgentRequest struct {
	ParentThreadID  ThreadID
	ParentTurnID    TurnID
	ThreadID        ThreadID
	TaskName        string
	TaskDescription string
	Message         string
	HostProfileRef  string
	ForkMode        SubAgentForkMode
	Labels          RunLabels
}

type SendSubAgentInputRequest struct {
	ParentThreadID ThreadID
	ChildThreadID  ThreadID
	Message        string
	Interrupt      bool
	Labels         RunLabels
}

type WaitSubAgentsRequest struct {
	ParentThreadID ThreadID
	ChildThreadIDs []ThreadID
	Timeout        time.Duration
}

type CloseSubAgentRequest struct {
	ParentThreadID ThreadID
	ChildThreadID  ThreadID
	Reason         string
}

type CloseSubAgentsRequest struct {
	ParentThreadID ThreadID
	Reason         string
}

type ReadSubAgentDetailRequest struct {
	ParentThreadID ThreadID
	ChildThreadID  ThreadID
	AfterOrdinal   int64
	Limit          int
	IncludeRaw     bool
}

type ListSubAgentDetailEventsRequest struct {
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

type ListPendingApprovalsRequest struct {
	ThreadID ThreadID
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

type CloseSubAgentsResult struct {
	Snapshots []SubAgentSnapshot `json:"snapshots"`
	Closed    int                `json:"closed,omitempty"`
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

type SubAgentDetailEvents struct {
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

type PendingApprovals struct {
	ThreadID    ThreadID          `json:"thread_id"`
	Approvals   []PendingApproval `json:"approvals"`
	GeneratedAt time.Time         `json:"generated_at"`
}

func (p PendingApprovals) Validate() error {
	if strings.TrimSpace(string(p.ThreadID)) == "" {
		return errors.New("pending approvals require thread id")
	}
	if p.GeneratedAt.IsZero() {
		return errors.New("pending approvals require generated time")
	}
	for index, approval := range p.Approvals {
		if err := approval.Validate(); err != nil {
			return fmt.Errorf("pending approval %d: %w", index, err)
		}
		if approval.ThreadID != p.ThreadID {
			return fmt.Errorf("pending approval %d thread identity mismatch", index)
		}
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

type PendingApprovalResource struct {
	Kind  string `json:"kind,omitempty"`
	Value string `json:"value,omitempty"`
}

func (r PendingApprovalResource) Validate() error {
	if strings.TrimSpace(r.Kind) == "" || strings.TrimSpace(r.Value) == "" {
		return errors.New("pending approval resource requires kind and value")
	}
	return nil
}

type PendingApproval struct {
	ApprovalID  string                    `json:"approval_id,omitempty"`
	ToolCallID  string                    `json:"tool_call_id,omitempty"`
	ToolName    string                    `json:"tool_name,omitempty"`
	ToolKind    string                    `json:"tool_kind,omitempty"`
	RunID       RunID                     `json:"run_id,omitempty"`
	ThreadID    ThreadID                  `json:"thread_id,omitempty"`
	TurnID      TurnID                    `json:"turn_id,omitempty"`
	Step        int                       `json:"step,omitempty"`
	BatchIndex  int                       `json:"batch_index"`
	BatchSize   int                       `json:"batch_size"`
	State       string                    `json:"state,omitempty"`
	Revision    int64                     `json:"revision,omitempty"`
	Epoch       int64                     `json:"epoch,omitempty"`
	RequestedAt time.Time                 `json:"requested_at,omitempty"`
	ResolvedAt  time.Time                 `json:"resolved_at,omitempty"`
	ArgsHash    string                    `json:"args_hash,omitempty"`
	Resources   []PendingApprovalResource `json:"resources,omitempty"`
	Effects     []string                  `json:"effects,omitempty"`
	Labels      map[string]string         `json:"labels,omitempty"`
	HostContext map[string]string         `json:"host_context,omitempty"`
	ReadOnly    bool                      `json:"read_only,omitempty"`
	Destructive bool                      `json:"destructive,omitempty"`
	OpenWorld   bool                      `json:"open_world,omitempty"`
	Reason      string                    `json:"reason,omitempty"`
}

func (p PendingApproval) Validate() error {
	if strings.TrimSpace(p.ApprovalID) == "" || strings.TrimSpace(p.ToolCallID) == "" {
		return errors.New("pending approval requires approval and tool call identities")
	}
	if strings.TrimSpace(p.ToolName) == "" || strings.TrimSpace(p.ToolKind) == "" {
		return errors.New("pending approval requires tool name and kind")
	}
	if strings.TrimSpace(string(p.RunID)) == "" || strings.TrimSpace(string(p.ThreadID)) == "" || strings.TrimSpace(string(p.TurnID)) == "" {
		return errors.New("pending approval requires run, thread, and turn identities")
	}
	if p.Step <= 0 {
		return errors.New("pending approval step must be positive")
	}
	if p.BatchSize <= 0 || p.BatchIndex < 0 || p.BatchIndex >= p.BatchSize {
		return errors.New("pending approval batch position is invalid")
	}
	if p.State != "requested" || p.Revision <= 0 || p.Epoch <= 0 {
		return errors.New("pending approval lifecycle state is invalid")
	}
	if p.RequestedAt.IsZero() || !p.ResolvedAt.IsZero() {
		return errors.New("pending approval timestamps are invalid")
	}
	if strings.TrimSpace(p.ArgsHash) == "" {
		return errors.New("pending approval requires args hash")
	}
	for index, resource := range p.Resources {
		if err := resource.Validate(); err != nil {
			return fmt.Errorf("pending approval resource %d: %w", index, err)
		}
	}
	for index, effect := range p.Effects {
		if strings.TrimSpace(effect) == "" {
			return fmt.Errorf("pending approval effect %d is empty", index)
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
	CallID        string       `json:"call_id,omitempty"`
	ToolName      string       `json:"tool_name,omitempty"`
	Status        string       `json:"status,omitempty"`
	Preview       string       `json:"preview,omitempty"`
	Content       string       `json:"content,omitempty"`
	Truncated     bool         `json:"truncated,omitempty"`
	OriginalBytes int          `json:"original_bytes,omitempty"`
	VisibleBytes  int          `json:"visible_bytes,omitempty"`
	OriginalLines int          `json:"original_lines,omitempty"`
	VisibleLines  int          `json:"visible_lines,omitempty"`
	Strategy      string       `json:"strategy,omitempty"`
	ContentSHA256 string       `json:"content_sha256,omitempty"`
	FullOutput    *ArtifactRef `json:"full_output,omitempty"`
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
	ID        string `json:"id,omitempty"`
	SafeLabel string `json:"safe_label,omitempty"`
	URL       string `json:"url,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
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
	Error                  string                         `json:"error,omitempty"`
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
	PendingApprovals       []PendingApproval              `json:"pending_approvals,omitempty"`
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
	if !r.Status.Valid() || !r.Status.IsTerminal() {
		return fmt.Errorf("turn result requires terminal status, got %q", r.Status)
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
	if e.Committed != nil {
		if e.Committed.ThreadID != e.ThreadID || e.Committed.TurnID != e.TurnID || e.Committed.RunID != e.RunID || e.Committed.Step != e.Step {
			return errors.New("runtime event committed detail identity mismatch")
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
	TurnStatusRunning   TurnStatus = "running"
	TurnStatusCompleted TurnStatus = "completed"
	TurnStatusWaiting   TurnStatus = "waiting"
	TurnStatusFailed    TurnStatus = "failed"
	TurnStatusCancelled TurnStatus = "cancelled"
)

func (s TurnStatus) Valid() bool {
	switch s {
	case TurnStatusRunning, TurnStatusCompleted, TurnStatusWaiting, TurnStatusFailed, TurnStatusCancelled:
		return true
	default:
		return false
	}
}

func (s TurnStatus) IsTerminal() bool {
	switch s {
	case TurnStatusCompleted, TurnStatusWaiting, TurnStatusFailed, TurnStatusCancelled:
		return true
	default:
		return false
	}
}

type Store struct {
	repo           sessiontree.Repo
	prompt         cache.Store
	artifacts      artifact.Store
	forkOperations storage.ForkOperationStore
	providerStates storage.ProviderStateStore
	agentTodos     sessiontree.AgentTodoStateRepo
	deleteData     func(context.Context, storage.DeleteThreadTreeDataRequest) error
	close          func() error
}

func NewMemoryStore() *Store {
	repo := sessiontree.NewMemoryRepo()
	prompt := cache.NewMemoryStore()
	artifacts := artifact.NewMemoryStore()
	forkOperations := storage.NewMemoryForkOperationStore()
	providerStates := storage.NewMemoryProviderStateStore()
	return &Store{
		repo:           repo,
		prompt:         prompt,
		artifacts:      artifacts,
		forkOperations: forkOperations,
		providerStates: providerStates,
		agentTodos:     repo,
		deleteData: func(ctx context.Context, req storage.DeleteThreadTreeDataRequest) error {
			threadIDs := cleanRuntimeIDs(append([]string{req.RootThreadID}, req.ThreadIDs...))
			for i := len(threadIDs) - 1; i >= 0; i-- {
				if err := repo.DeleteThread(ctx, threadIDs[i]); err != nil {
					return err
				}
			}
			if err := prompt.DeletePromptScopes(ctx, req.PromptScopeIDs...); err != nil {
				return err
			}
			for _, threadID := range threadIDs {
				if err := providerStates.DeleteProviderState(ctx, threadID); err != nil {
					return err
				}
				if err := artifacts.DeleteThreadArtifacts(ctx, threadID); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func OpenSQLiteStore(path string) (*Store, error) {
	sqliteStore, err := sqlite.Open(path)
	if err != nil {
		return nil, err
	}
	return &Store{
		repo:           sqliteStore,
		prompt:         sqliteStore,
		artifacts:      sqliteStore,
		forkOperations: sqliteStore,
		providerStates: sqliteStore,
		agentTodos:     sqliteStore,
		deleteData: func(ctx context.Context, req storage.DeleteThreadTreeDataRequest) error {
			return sqliteStore.DeleteThreadTreeData(ctx, req)
		},
		close: sqliteStore.Close,
	}, nil
}

func (s *Store) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close()
}

func (s *Store) validate() error {
	if s == nil {
		return errors.New("runtime store is required")
	}
	if s.repo == nil || s.prompt == nil || s.artifacts == nil || s.forkOperations == nil || s.providerStates == nil || s.agentTodos == nil || s.deleteData == nil {
		return errors.New("runtime store must be created with runtime.NewMemoryStore or runtime.OpenSQLiteStore")
	}
	return nil
}

func (s *Store) deleteThreadData(ctx context.Context, threadID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	threadIDs, err := s.threadTreeIDs(ctx, threadID)
	if err != nil {
		return err
	}
	return s.deleteData(ctx, storage.DeleteThreadTreeDataRequest{
		RootThreadID:   threadID,
		ThreadIDs:      threadIDs,
		PromptScopeIDs: append([]string(nil), threadIDs...),
	})
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
	if _, err := s.repo.Thread(ctx, threadID); err != nil {
		return nil, err
	}
	threads, err := sessiontree.ListThreads(ctx, s.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return nil, err
	}
	children := map[string][]string{}
	for _, meta := range threads {
		parentID := strings.TrimSpace(meta.ParentThreadID)
		id := strings.TrimSpace(meta.ID)
		if parentID == "" || id == "" {
			continue
		}
		children[parentID] = append(children[parentID], id)
	}
	out := []string{}
	seen := map[string]bool{}
	var walk func(string)
	walk = func(id string) {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
		for _, childID := range children[id] {
			walk(childID)
		}
	}
	walk(threadID)
	return out, nil
}

func NewHost(opts HostOptions) (*Host, error) {
	titleMode, err := normalizeThreadTitleMode(opts.ThreadTitleMode)
	if err != nil {
		return nil, err
	}
	cfg, provider, err := resolveHostConfigAndProvider(opts)
	if err != nil {
		return nil, err
	}
	if opts.Runtime == nil || opts.Runtime.store == nil {
		return nil, errors.New("host runtime is required")
	}
	store := opts.Runtime.store
	if err := store.validate(); err != nil {
		return nil, err
	}
	harness, err := newHarnessWithProvider(cfg, provider, harnessOptions{
		Store:                 store,
		Tools:                 opts.Tools,
		Approver:              opts.Approver,
		Sink:                  newRuntimeEventSink(opts.Sink),
		SinkPolicy:            runtimeHarnessSinkPolicy(),
		ToolSurfaceProvider:   runtimeToolSurfaceProvider(opts.ToolSurfaceProvider),
		NewID:                 opts.IDGenerator,
		LoopLimits:            opts.LoopLimits,
		SubAgentRunTimeout:    opts.SubAgentRunTimeout,
		Capabilities:          opts.Capabilities,
		ThreadTitleMode:       titleMode,
		StateCompatibilityKey: runtimeStateCompatibilityKey(cfg, opts),
	})
	if err != nil {
		return nil, err
	}
	return &Host{
		cfg:                       cfg,
		store:                     store,
		sink:                      opts.Sink,
		harness:                   harness,
		supportsOpaqueAttachments: opts.ModelGateway != nil,
	}, nil
}

func resolveHostConfigAndProvider(opts HostOptions) (config.Config, provider.Provider, error) {
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
	switch {
	case err == nil:
		return nil
	case errors.Is(err, agentharness.ErrPendingToolSettlementTargetTurnNotFound):
		return fmt.Errorf("%w: %w", ErrTurnNotFound, err)
	case errors.Is(err, agentharness.ErrPendingToolSettlementTargetRunNotFound):
		return fmt.Errorf("%w: %w", ErrRunNotFound, err)
	case errors.Is(err, agentharness.ErrSubAgentNotFound):
		return fmt.Errorf("%w: %w", ErrSubAgentNotFound, err)
	case errors.Is(err, agentharness.ErrForkOperationConflict):
		return fmt.Errorf("%w: %w", ErrForkOperationConflict, err)
	case errors.Is(err, agentharness.ErrForkOperationTargetMissing):
		return fmt.Errorf("%w: %w", ErrForkOperationTargetMissing, err)
	case errors.Is(err, sessiontree.ErrForkDestinationConflict):
		return fmt.Errorf("%w: %w", ErrForkDestinationConflict, err)
	case errors.Is(err, sessiontree.ErrAgentTodoVersionConflict):
		return fmt.Errorf("%w: %w", ErrAgentTodoVersionConflict, err)
	case errors.Is(err, agentharness.ErrJournalInvariant):
		return fmt.Errorf("%w: %w", ErrJournalInvariant, err)
	case errors.Is(err, sessiontree.ErrThreadNotFound):
		return fmt.Errorf("%w: %w", ErrThreadNotFound, err)
	default:
		return err
	}
}

func (h *ThreadCreateHost) CreateThread(ctx context.Context, req CreateThreadRequest) (ThreadSummary, error) {
	return createThread(ctx, h.harness, req)
}

func createThread(ctx context.Context, harness *agentharness.AgentHarness, req CreateThreadRequest) (ThreadSummary, error) {
	summary, err := harness.CreateThread(ctx, agentharness.StartThreadOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	return threadSummary(summary), nil
}

func (h *ThreadTitleHost) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (ThreadSnapshot, error) {
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
	return forkThread(ctx, h.harness, req)
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
		OperationID:           string(req.OperationID),
		SourceThreadID:        string(req.SourceThreadID),
		NewThreadID:           string(req.DestinationThreadID),
		RewriteTurnIdentities: true,
	})
	if err != nil {
		return ForkThreadResult{}, runtimeHostError(err)
	}
	return forkThreadResult(result), nil
}

func (h *Host) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	return readThreadByID(ctx, h.harness, threadID)
}

func (h *ThreadReadHost) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	return readThreadByID(ctx, h.harness, threadID)
}

func readThreadByID(ctx context.Context, harness *agentharness.AgentHarness, threadID ThreadID) (ThreadSnapshot, error) {
	snapshot, err := harness.ReadThread(ctx, string(threadID))
	if err != nil {
		return ThreadSnapshot{}, runtimeHostError(err)
	}
	return threadSnapshot(snapshot), nil
}

func (h *Host) ListThreadDetailEvents(ctx context.Context, req ListThreadDetailEventsRequest) (ThreadDetailEvents, error) {
	return listThreadDetailEvents(ctx, h.harness, req)
}

func (h *ThreadReadHost) ListThreadDetailEvents(ctx context.Context, req ListThreadDetailEventsRequest) (ThreadDetailEvents, error) {
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

func (h *Host) ReadThreadContext(ctx context.Context, threadID ThreadID) (ThreadContextSnapshot, error) {
	return readThreadContext(ctx, h.harness, threadID)
}

func (h *ThreadReadHost) ReadThreadContext(ctx context.Context, threadID ThreadID) (ThreadContextSnapshot, error) {
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

func (h *Host) ReadThreadAgentTodos(ctx context.Context, threadID ThreadID) (ThreadAgentTodoState, error) {
	return readThreadAgentTodos(ctx, h.store, threadID)
}

func (h *ThreadReadHost) ReadThreadAgentTodos(ctx context.Context, threadID ThreadID) (ThreadAgentTodoState, error) {
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

func (h *Host) UpdateThreadAgentTodos(ctx context.Context, req UpdateThreadAgentTodosRequest) (ThreadAgentTodoState, error) {
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

func validateAgentTodoUpdateIdentity(ctx context.Context, repo sessiontree.Repo, req UpdateThreadAgentTodosRequest) error {
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

func (h *Host) ListPendingApprovals(ctx context.Context, req ListPendingApprovalsRequest) (PendingApprovals, error) {
	return listPendingApprovals(ctx, h.harness, req)
}

func listPendingApprovals(ctx context.Context, harness *agentharness.AgentHarness, req ListPendingApprovalsRequest) (PendingApprovals, error) {
	result, err := harness.ListPendingApprovals(ctx, agentharness.ListPendingApprovalsOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return PendingApprovals{}, runtimeHostError(err)
	}
	out := pendingApprovals(result)
	if err := out.Validate(); err != nil {
		return PendingApprovals{}, fmt.Errorf("validate pending approvals: %w", err)
	}
	return out, nil
}

func (h *Host) ReadTurnProjection(ctx context.Context, req ReadTurnProjectionRequest) (ThreadTurnProjection, error) {
	return readTurnProjection(ctx, h.harness, req)
}

func (h *ThreadReadHost) ReadTurnProjection(ctx context.Context, req ReadTurnProjectionRequest) (ThreadTurnProjection, error) {
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
	events, err := listRawThreadDetailEventsForTurn(ctx, harness, string(req.ThreadID), string(req.TurnID))
	if err != nil {
		return ThreadTurnProjection{}, runtimeHostError(err)
	}
	if len(events) == 0 {
		return ThreadTurnProjection{}, ErrTurnNotFound
	}
	if !threadDetailEventsTurnStartedRunIDMatches(events, req.RunID) {
		return ThreadTurnProjection{}, fmt.Errorf("%w: %s", ErrRunNotFound, req.RunID)
	}
	return ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: req.ThreadID,
		TurnID:   req.TurnID,
		RunID:    req.RunID,
		TraceID:  TraceID(req.RunID),
		Events:   events,
	}), nil
}

func (h *Host) RunTurn(ctx context.Context, req RunTurnRequest) (TurnResult, error) {
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
		SupplementalContext:      agentHarnessSupplementalContext(req.SupplementalContext),
		Attachments:              sessionMessageAttachments(input.Attachments),
		Sink:                     activityRecorder,
	})
	out := turnResult(result, string(req.ThreadID), activityRecorder.Snapshot(), time.Now().UnixMilli())
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out)
	return out, runtimeHostError(runErr)
}

func normalizeTurnInput(input TurnInput) (TurnInput, error) {
	input.Attachments = append([]MessageAttachment(nil), input.Attachments...)
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

func (h *Host) RetryTurn(ctx context.Context, req RetryTurnRequest) (TurnResult, error) {
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
	h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out)
	return out, runtimeHostError(runErr)
}

func (h *Host) CompactThread(ctx context.Context, req CompactThreadRequest) (CompactThreadResult, error) {
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
		if compactErr != nil {
			return CompactThreadResult{}, runtimeHostError(compactErr)
		}
		return CompactThreadResult{}, errors.New("compact thread completed without a terminal compaction event")
	}
	out := CompactThreadResult{
		ThreadID:   req.ThreadID,
		RunID:      RunID(result.RunID),
		RequestID:  strings.TrimSpace(req.RequestID),
		Compaction: terminalCompactions[len(terminalCompactions)-1],
		Metrics:    runtimeMetrics(result.Metrics),
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

func (h *Host) CompletePendingTool(ctx context.Context, req PendingToolCompletionRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.RunID)) == "" {
		return TurnResult{}, errors.New("run id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, runtimeHostError(err)
	}
	result, runErr := thread.CompletePendingTool(ctx, agentharness.PendingToolCompletion{
		TurnID:     string(req.TurnID),
		RunID:      string(req.RunID),
		ToolCallID: req.ToolCallID,
		ToolName:   req.ToolName,
		Handle:     req.Handle,
		Status:     pendingToolCompletionStatus(req.Status),
		Summary:    req.Summary,
		Output:     req.Output,
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
	})
	out := turnResult(result, string(req.ThreadID), nil, time.Now().UnixMilli())
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out)
	return out, runtimeHostError(runErr)
}

func (h *Host) SettlePendingTool(ctx context.Context, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	if thread, ok := h.harness.ActiveThread(string(req.Target.ThreadID)); ok {
		if err := validatePendingToolSettlementRequest(req); err != nil {
			return PendingToolSettlementResult{}, err
		}
		return settlePendingToolOnThread(ctx, h.harness, thread, req)
	}
	return settlePendingTool(ctx, h.harness, req)
}

func (h *PendingToolSettlementHost) SettlePendingTool(ctx context.Context, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	return settlePendingTool(ctx, h.harness, req)
}

func settlePendingTool(ctx context.Context, harness *agentharness.AgentHarness, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	if err := validatePendingToolSettlementRequest(req); err != nil {
		return PendingToolSettlementResult{}, err
	}
	thread, err := harness.ResumeThread(ctx, string(req.Target.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	return settlePendingToolOnThread(ctx, harness, thread, req)
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

func settlePendingToolOnThread(ctx context.Context, harness *agentharness.AgentHarness, thread *agentharness.Thread, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	event, err := thread.SettlePendingTool(ctx, agentharness.PendingToolSettlement{
		TurnID:     string(req.Target.TurnID),
		RunID:      string(req.Target.RunID),
		ToolCallID: req.Target.ToolCallID,
		ToolName:   req.Target.ToolName,
		Handle:     req.Target.Handle,
		Status:     pendingToolSettlementStatus(req.Status),
		Summary:    req.Summary,
		Output:     req.Output,
		Activity:   observation.CloneActivityPresentation(req.Activity),
	})
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	out := PendingToolSettlementResult{
		Target: req.Target,
		Event:  threadDetailEvent(event),
	}
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	events, err := listRawThreadDetailEventsForTurn(projectionCtx, harness, string(req.Target.ThreadID), string(req.Target.TurnID))
	if err != nil {
		out.ProjectionAvailability = TurnProjectionAvailabilityUnavailable
		out.ProjectionError = runtimeHostError(err).Error()
		return out, nil
	}
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: req.Target.ThreadID,
		TurnID:   req.Target.TurnID,
		RunID:    req.Target.RunID,
		TraceID:  TraceID(req.Target.RunID),
		Events:   events,
	})
	out.ProjectionAvailability = TurnProjectionAvailabilityReady
	out.Projection = &projection
	return out, nil
}

func (h *Host) SpawnSubAgent(ctx context.Context, req SpawnSubAgentRequest) (SubAgentSnapshot, error) {
	snapshot, err := h.harness.SpawnSubAgent(ctx, agentharness.SpawnSubAgentOptions{
		ParentThreadID:  string(req.ParentThreadID),
		ParentTurnID:    string(req.ParentTurnID),
		ThreadID:        string(req.ThreadID),
		TaskName:        req.TaskName,
		TaskDescription: req.TaskDescription,
		Message:         req.Message,
		HostProfileRef:  req.HostProfileRef,
		ForkMode:        agentharness.SubAgentForkMode(req.ForkMode),
		Labels:          engineLabels(req.Labels),
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func (h *Host) SendSubAgentInput(ctx context.Context, req SendSubAgentInputRequest) (SubAgentSnapshot, error) {
	snapshot, err := h.harness.SendSubAgentInput(ctx, agentharness.SendSubAgentInputOptions{
		ParentThreadID: string(req.ParentThreadID),
		ChildThreadID:  string(req.ChildThreadID),
		Message:        req.Message,
		Interrupt:      req.Interrupt,
		Labels:         engineLabels(req.Labels),
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func (h *Host) WaitSubAgents(ctx context.Context, req WaitSubAgentsRequest) (WaitSubAgentsResult, error) {
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

func (h *Host) ListSubAgents(ctx context.Context, parentThreadID ThreadID) ([]SubAgentSnapshot, error) {
	return listSubAgents(ctx, h.harness, parentThreadID)
}

func (h *ThreadReadHost) ListSubAgents(ctx context.Context, parentThreadID ThreadID) ([]SubAgentSnapshot, error) {
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

func (h *Host) CloseSubAgent(ctx context.Context, req CloseSubAgentRequest) (SubAgentSnapshot, error) {
	snapshot, err := h.harness.CloseSubAgent(ctx, agentharness.CloseSubAgentOptions{
		ParentThreadID: string(req.ParentThreadID),
		ChildThreadID:  string(req.ChildThreadID),
		Reason:         req.Reason,
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func (h *SubAgentMaintenanceHost) CloseSubAgents(ctx context.Context, req CloseSubAgentsRequest) (CloseSubAgentsResult, error) {
	return closeSubAgents(ctx, h.harness, req)
}

func closeSubAgents(ctx context.Context, harness *agentharness.AgentHarness, req CloseSubAgentsRequest) (CloseSubAgentsResult, error) {
	result, err := harness.CloseSubAgents(ctx, agentharness.CloseSubAgentsOptions{
		ParentThreadID: string(req.ParentThreadID),
		Reason:         req.Reason,
	})
	if err != nil {
		return CloseSubAgentsResult{}, runtimeHostError(err)
	}
	out := CloseSubAgentsResult{Closed: result.Closed, Snapshots: make([]SubAgentSnapshot, 0, len(result.Snapshots))}
	for _, snapshot := range result.Snapshots {
		out.Snapshots = append(out.Snapshots, subAgentSnapshot(snapshot))
	}
	return out, nil
}

func (h *Host) ListSubAgentActivityTimeline(ctx context.Context, req ListSubAgentActivityTimelineRequest) (SubAgentActivityTimelineResult, error) {
	return listSubAgentActivityTimeline(ctx, h.harness, req)
}

func (h *ThreadReadHost) ListSubAgentActivityTimeline(ctx context.Context, req ListSubAgentActivityTimelineRequest) (SubAgentActivityTimelineResult, error) {
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

func (h *Host) ReadSubAgentDetail(ctx context.Context, req ReadSubAgentDetailRequest) (SubAgentDetail, error) {
	return readSubAgentDetail(ctx, h.harness, req)
}

func (h *ThreadReadHost) ReadSubAgentDetail(ctx context.Context, req ReadSubAgentDetailRequest) (SubAgentDetail, error) {
	return readSubAgentDetail(ctx, h.harness, req)
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

func (h *Host) ListSubAgentDetailEvents(ctx context.Context, req ListSubAgentDetailEventsRequest) (SubAgentDetailEvents, error) {
	return listSubAgentDetailEvents(ctx, h.harness, req)
}

func (h *ThreadReadHost) ListSubAgentDetailEvents(ctx context.Context, req ListSubAgentDetailEventsRequest) (SubAgentDetailEvents, error) {
	return listSubAgentDetailEvents(ctx, h.harness, req)
}

func listSubAgentDetailEvents(ctx context.Context, harness *agentharness.AgentHarness, req ListSubAgentDetailEventsRequest) (SubAgentDetailEvents, error) {
	detail, err := harness.ReadSubAgentDetail(ctx, agentharness.ReadSubAgentDetailOptions{
		ParentThreadID: string(req.ParentThreadID),
		ChildThreadID:  string(req.ChildThreadID),
		AfterOrdinal:   req.AfterOrdinal,
		Limit:          req.Limit,
		IncludeRaw:     req.IncludeRaw,
	})
	if err != nil {
		return SubAgentDetailEvents{}, runtimeHostError(err)
	}
	return SubAgentDetailEvents{
		Events:           subAgentThreadDetailEvents(detail.Events),
		ActivityTimeline: cloneRuntimeActivityTimeline(detail.ActivityTimeline),
		Context:          subAgentDetailContext(detail.Snapshot.ThreadID, detail.Context),
		NextOrdinal:      detail.NextOrdinal,
		HasMore:          detail.HasMore,
		RetainedFrom:     detail.RetainedFrom,
		GeneratedAt:      detail.GeneratedAt,
	}, nil
}

func (h *ThreadDeleteHost) DeleteThread(ctx context.Context, threadID ThreadID) error {
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
	out := ForkThreadResult{
		OperationID: ForkOperationID(in.OperationID),
		Thread:      threadSummary(in.Summary),
		Turns:       make([]ForkedTurnRef, 0, len(in.Turns)),
	}
	for _, ref := range in.Turns {
		out.Turns = append(out.Turns, ForkedTurnRef{
			SourceTurnID:      TurnID(ref.SourceTurnID),
			SourceRunID:       RunID(ref.SourceRunID),
			DestinationTurnID: TurnID(ref.DestinationTurnID),
			DestinationRunID:  RunID(ref.DestinationRunID),
			CreatedAt:         ref.CreatedAt,
		})
	}
	return out
}

func pendingApprovals(in agentharness.PendingApprovals) PendingApprovals {
	return PendingApprovals{
		ThreadID:    ThreadID(in.ThreadID),
		Approvals:   pendingApprovalList(in.Approvals),
		GeneratedAt: in.GeneratedAt,
	}
}

func pendingApprovalList(in []agentharness.PendingApproval) []PendingApproval {
	if len(in) == 0 {
		return nil
	}
	out := make([]PendingApproval, 0, len(in))
	for _, approval := range in {
		out = append(out, pendingApproval(approval))
	}
	return out
}

func pendingApproval(in agentharness.PendingApproval) PendingApproval {
	return PendingApproval{
		ApprovalID:  in.ApprovalID,
		ToolCallID:  in.ToolCallID,
		ToolName:    in.ToolName,
		ToolKind:    in.ToolKind,
		RunID:       RunID(in.RunID),
		ThreadID:    ThreadID(in.ThreadID),
		TurnID:      TurnID(in.TurnID),
		Step:        in.Step,
		BatchIndex:  in.BatchIndex,
		BatchSize:   in.BatchSize,
		State:       in.State,
		Revision:    in.Revision,
		Epoch:       in.Epoch,
		RequestedAt: in.RequestedAt,
		ResolvedAt:  in.ResolvedAt,
		ArgsHash:    in.ArgsHash,
		Resources:   pendingApprovalResources(in.Resources),
		Effects:     append([]string(nil), in.Effects...),
		Labels:      cloneStringMap(in.Labels),
		HostContext: cloneStringMap(in.HostContext),
		ReadOnly:    in.ReadOnly,
		Destructive: in.Destructive,
		OpenWorld:   in.OpenWorld,
		Reason:      in.Reason,
	}
}

func pendingApprovalResources(in []agentharness.PendingApprovalResource) []PendingApprovalResource {
	if len(in) == 0 {
		return nil
	}
	out := make([]PendingApprovalResource, 0, len(in))
	for _, resource := range in {
		out = append(out, PendingApprovalResource{Kind: resource.Kind, Value: resource.Value})
	}
	return out
}

func turnResult(in agentharness.TurnResult, threadID string, events []observation.Event, nowUnixMS int64) TurnResult {
	out := TurnResult{
		ThreadID:           ThreadID(threadID),
		TurnID:             TurnID(in.ID),
		RunID:              RunID(in.RunID),
		Status:             TurnStatus(in.Status),
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
		PendingApprovals: pendingApprovalList(in.PendingApprovals),
	}
	if in.Err != nil {
		out.Error = in.Err.Error()
	}
	return out
}

func (h *Host) attachThreadTurnProjection(ctx context.Context, threadID string, result *TurnResult) {
	if result == nil {
		return
	}
	if h == nil || strings.TrimSpace(threadID) == "" || strings.TrimSpace(string(result.TurnID)) == "" {
		result.ProjectionAvailability = TurnProjectionAvailabilityUnavailable
		result.ProjectionError = "turn projection identity is incomplete"
		return
	}
	events, err := listRawThreadDetailEventsForTurn(ctx, h.harness, threadID, string(result.TurnID))
	if err != nil {
		result.ProjectionAvailability = TurnProjectionAvailabilityUnavailable
		result.ProjectionError = runtimeHostError(err).Error()
		return
	}
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: ThreadID(threadID),
		TurnID:   result.TurnID,
		RunID:    result.RunID,
		TraceID:  TraceID(result.RunID),
		Events:   events,
	})
	result.ProjectionAvailability = TurnProjectionAvailabilityReady
	result.Projection = &projection
	result.ProjectionError = ""
}

func runtimeTerminalProjectionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func listRawThreadDetailEventsForTurn(ctx context.Context, harness *agentharness.AgentHarness, threadID string, turnID string) ([]ThreadDetailEvent, error) {
	var out []ThreadDetailEvent
	var afterOrdinal int64
	for {
		detail, err := harness.ListThreadDetailEvents(ctx, agentharness.ListThreadDetailEventsOptions{
			ThreadID:     threadID,
			AfterOrdinal: afterOrdinal,
			Limit:        agentharness.MaxThreadDetailEventLimit,
			IncludeRaw:   true,
		})
		if err != nil {
			return nil, err
		}
		for _, ev := range threadDetailEvents(detail.Events) {
			if strings.TrimSpace(string(ev.TurnID)) == strings.TrimSpace(turnID) {
				out = append(out, ev)
			}
		}
		if !detail.HasMore {
			return out, nil
		}
		if detail.NextOrdinal <= afterOrdinal {
			return nil, fmt.Errorf("thread detail pagination did not advance after ordinal %d", afterOrdinal)
		}
		afterOrdinal = detail.NextOrdinal
	}
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
		CallID:        in.CallID,
		ToolName:      in.ToolName,
		Status:        in.Status,
		Preview:       in.Preview,
		Content:       in.Content,
		Truncated:     in.Truncated,
		OriginalBytes: in.OriginalBytes,
		VisibleBytes:  in.VisibleBytes,
		OriginalLines: in.OriginalLines,
		VisibleLines:  in.VisibleLines,
		Strategy:      in.Strategy,
		ContentSHA256: in.ContentSHA256,
	}
	if in.FullOutput != nil {
		out.FullOutput = &ArtifactRef{
			ID:        in.FullOutput.ID,
			SafeLabel: in.FullOutput.SafeLabel,
			URL:       in.FullOutput.URL,
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
	Store                 *Store
	Tools                 *tools.Registry
	Approver              tools.Approver
	Sink                  event.Sink
	SinkPolicy            event.SinkPolicy
	Title                 agentharness.TitleGenerator
	ThreadTitleMode       ThreadTitleMode
	NewID                 func(string) string
	LoopLimits            LoopLimits
	SubAgentRunTimeout    time.Duration
	Capabilities          CapabilityOptions
	ToolSurfaceProvider   engine.ToolSurfaceProvider
	StateCompatibilityKey string
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
	return agentharness.New(agentharness.Options{
		Provider:              p,
		ProviderName:          cfg.Provider,
		Model:                 cfg.Model,
		SystemPrompt:          effectivePrompt,
		Tools:                 registry,
		PromptStore:           store.prompt,
		Repo:                  store.repo,
		ForkOperations:        store.forkOperations,
		ProviderStates:        store.providerStates,
		StateCompatibilityKey: opts.StateCompatibilityKey,
		Sink:                  opts.Sink,
		SinkPolicy:            opts.SinkPolicy,
		Approver:              opts.Approver,
		ToolSurfaceProvider:   opts.ToolSurfaceProvider,
		TitleGenerator:        titleGenerator,
		CompactionPrompt:      compaction.PromptOptions{},
		Artifacts:             store.artifacts,
		Reasoning:             model.Reasoning,
		TurnPolicy:            turnPolicy,
		LoopLimits:            loopLimits,
		SubAgentRunTimeout:    opts.SubAgentRunTimeout,
		NewID:                 opts.NewID,
	}), nil
}

func runtimeStateCompatibilityKey(cfg config.Config, opts HostOptions) string {
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

type engineHelperOptions struct {
	RunID         string
	PromptScopeID string
	PromptStore   cache.Store
}

func newEngineWithProvider(cfg config.Config, p provider.Provider, store session.TranscriptStore, registry *tools.Registry, opts engineHelperOptions) (*engine.Engine, error) {
	cfg = config.ResolvePrompt(cfg)
	if store == nil {
		store = session.NewMemoryStore()
	}
	if registry == nil {
		registry = tools.NewRegistry()
	}
	promptStore := opts.PromptStore
	if promptStore == nil {
		promptStore = cache.NewMemoryStore()
	}
	cacheRetention, err := config.PromptCacheRetention(cfg)
	if err != nil {
		return nil, err
	}
	return engine.New(engine.Config{
		Provider:     p,
		Store:        store,
		Prompt:       promptStore,
		Artifacts:    artifact.NewMemoryStore(),
		SystemPrompt: cfg.SystemPrompt,
		Tools:        registry,
		Options: engine.Options{
			RunID:                   opts.RunID,
			TraceID:                 opts.RunID,
			PromptScopeID:           opts.PromptScopeID,
			ProviderName:            cfg.Provider,
			Model:                   cfg.Model,
			CacheRetention:          configbridge.CacheRetention(cacheRetention),
			ContextPolicy:           configbridge.ContextPolicy(cfg.ContextPolicy),
			Reasoning:               configbridge.ReasoningSelection(cfg.Reasoning),
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
		},
	})
}
