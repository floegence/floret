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
type PromptScopeID string
type TraceID string

var (
	// ErrThreadNotFound reports that a durable thread requested through Host was not found.
	ErrThreadNotFound = errors.New("floret thread not found")
	// ErrTurnNotFound reports that a durable turn requested through Host was not found.
	ErrTurnNotFound = errors.New("floret turn not found")
	// ErrRunNotFound reports that a durable run requested through Host was not found.
	ErrRunNotFound = errors.New("floret run not found")
	// ErrSubAgentNotFound reports that a parent-scoped child thread requested through Host was not found.
	ErrSubAgentNotFound = errors.New("floret subagent not found")
)

type Host interface {
	StartThread(context.Context, StartThreadRequest) (ThreadSnapshot, error)
	EnsureThread(context.Context, EnsureThreadRequest) (ThreadSummary, error)
	ReadThread(context.Context, ThreadID) (ThreadSnapshot, error)
	ListThreadDetailEvents(context.Context, ListThreadDetailEventsRequest) (ThreadDetailEvents, error)
	ListPendingApprovals(context.Context, ListPendingApprovalsRequest) (PendingApprovals, error)
	ReadTurnProjection(context.Context, ReadTurnProjectionRequest) (ThreadTurnProjection, error)
	RunTurn(context.Context, RunTurnRequest) (TurnResult, error)
	RetryTurn(context.Context, RetryTurnRequest) (TurnResult, error)
	CompactThread(context.Context, CompactThreadRequest) (CompactThreadResult, error)
	CompletePendingTool(context.Context, PendingToolCompletionRequest) (TurnResult, error)
	SettlePendingTool(context.Context, PendingToolSettlementRequest) (PendingToolSettlementResult, error)
	SpawnSubAgent(context.Context, SpawnSubAgentRequest) (SubAgentSnapshot, error)
	SendSubAgentInput(context.Context, SendSubAgentInputRequest) (SubAgentSnapshot, error)
	WaitSubAgents(context.Context, WaitSubAgentsRequest) (WaitSubAgentsResult, error)
	ListSubAgents(context.Context, ThreadID) ([]SubAgentSnapshot, error)
	CloseSubAgent(context.Context, CloseSubAgentRequest) (SubAgentSnapshot, error)
	CloseSubAgents(context.Context, CloseSubAgentsRequest) (CloseSubAgentsResult, error)
	ListSubAgentActivityTimeline(context.Context, ListSubAgentActivityTimelineRequest) (SubAgentActivityTimelineResult, error)
	ReadSubAgentDetail(context.Context, ReadSubAgentDetailRequest) (SubAgentDetail, error)
	ListSubAgentDetailEvents(context.Context, ListSubAgentDetailEventsRequest) (SubAgentDetailEvents, error)
	DeleteThread(context.Context, ThreadID) error
	Close() error
}

// LifecycleHost exposes provider-free thread lifecycle operations over a Floret store.
type LifecycleHost interface {
	EnsureThread(context.Context, EnsureThreadRequest) (ThreadSummary, error)
	CloseSubAgents(context.Context, CloseSubAgentsRequest) (CloseSubAgentsResult, error)
	DeleteThread(context.Context, ThreadID) error
	Close() error
}

type HostOptions struct {
	Config              config.Config
	ModelGateway        ModelGateway
	Store               *Store
	Tools               *tools.Registry
	Approver            tools.Approver
	Sink                EventSink
	ToolSurfaceProvider ToolSurfaceProvider
	IDGenerator         func(string) string
	LoopLimits          LoopLimits
	SubAgentRunTimeout  time.Duration
	Capabilities        CapabilityOptions
}

// LifecycleHostOptions configures NewLifecycleHost. If Store is nil, the host uses an ephemeral memory store.
type LifecycleHostOptions struct {
	Store *Store
	Sink  EventSink
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

type StartThreadRequest struct {
	ThreadID ThreadID
}

type EnsureThreadRequest struct {
	ThreadID ThreadID
}

type RunTurnRequest struct {
	RunID                 RunID
	ThreadID              ThreadID
	TurnID                TurnID
	Input                 string
	Labels                RunLabels
	PreviousProviderState *ModelState
	Completion            TurnCompletionPolicy
	Signals               TurnSignalSpec
	Limits                TurnLimits
	Reasoning             ReasoningSelection
	ManualCompactions     ManualCompactionSource
	ToolSurfaceProvider   ToolSurfaceProvider
}

type RetryTurnRequest struct {
	ThreadID ThreadID
	Reason   string
	Labels   RunLabels
}

type CompactThreadRequest struct {
	ThreadID              ThreadID
	RequestID             string
	Source                string
	Labels                RunLabels
	PreviousProviderState *ModelState
	Limits                TurnLimits
	Reasoning             ReasoningSelection
}

// ReadTurnProjectionRequest identifies a durable hosted turn projection to rebuild from Floret detail.
// RunID is required and must match the execution identity recorded for the turn.
type ReadTurnProjectionRequest struct {
	ThreadID ThreadID
	TurnID   TurnID
	RunID    RunID
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

// PendingToolSettlementRequest records a host-owned pending tool outcome as a
// detail/activity event only. It does not resume the provider loop.
type PendingToolSettlementRequest struct {
	ThreadID   ThreadID
	TurnID     TurnID
	RunID      RunID
	ToolCallID string
	ToolName   string
	Handle     string
	Status     PendingToolSettlementStatus
	Summary    string
	Output     string
	Activity   *observation.ActivityPresentation
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
	ParentThreadID ThreadID
	ParentTurnID   TurnID
	ThreadID       ThreadID
	TaskName       string
	Message        string
	HostProfileRef string
	ForkMode       SubAgentForkMode
	Labels         RunLabels
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
	ThreadID       ThreadID         `json:"thread_id"`
	Path           string           `json:"path"`
	TaskName       string           `json:"task_name"`
	ParentThreadID ThreadID         `json:"parent_thread_id"`
	ParentTurnID   TurnID           `json:"parent_turn_id,omitempty"`
	HostProfileRef string           `json:"host_profile_ref,omitempty"`
	ForkMode       SubAgentForkMode `json:"fork_mode,omitempty"`
	Status         SubAgentStatus   `json:"status"`
	LatestTurnID   TurnID           `json:"latest_turn_id,omitempty"`
	LastMessage    string           `json:"last_message,omitempty"`
	WaitingPrompt  string           `json:"waiting_prompt,omitempty"`
	QueuedInputs   int              `json:"queued_inputs,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
	Closed         bool             `json:"closed,omitempty"`
	CanSendInput   bool             `json:"can_send_input"`
	CanInterrupt   bool             `json:"can_interrupt"`
	CanClose       bool             `json:"can_close"`
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
	Snapshot     SubAgentSnapshot      `json:"snapshot"`
	Events       []SubAgentDetailEvent `json:"events"`
	NextOrdinal  int64                 `json:"next_ordinal,omitempty"`
	HasMore      bool                  `json:"has_more,omitempty"`
	RetainedFrom int64                 `json:"retained_from,omitempty"`
	GeneratedAt  time.Time             `json:"generated_at"`
}

type SubAgentDetailEvents struct {
	Events       []SubAgentDetailEvent `json:"events"`
	NextOrdinal  int64                 `json:"next_ordinal,omitempty"`
	HasMore      bool                  `json:"has_more,omitempty"`
	RetainedFrom int64                 `json:"retained_from,omitempty"`
	GeneratedAt  time.Time             `json:"generated_at"`
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

type PendingToolSettlementResult struct {
	ThreadID   ThreadID             `json:"thread_id"`
	TurnID     TurnID               `json:"turn_id"`
	RunID      RunID                `json:"run_id,omitempty"`
	Event      ThreadDetailEvent    `json:"event"`
	Projection ThreadTurnProjection `json:"projection"`
}

type PendingApprovalResource struct {
	Kind  string `json:"kind,omitempty"`
	Value string `json:"value,omitempty"`
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

type SubAgentDetailEventKind string

const (
	SubAgentDetailEventUserMessage      SubAgentDetailEventKind = "user_message"
	SubAgentDetailEventAssistantMessage SubAgentDetailEventKind = "assistant_message"
	SubAgentDetailEventToolCall         SubAgentDetailEventKind = "tool_call"
	SubAgentDetailEventToolDispatch     SubAgentDetailEventKind = "tool_dispatch"
	SubAgentDetailEventToolActivity     SubAgentDetailEventKind = "tool_activity"
	SubAgentDetailEventToolResult       SubAgentDetailEventKind = "tool_result"
	SubAgentDetailEventTurnMarker       SubAgentDetailEventKind = "turn_marker"
	SubAgentDetailEventCompaction       SubAgentDetailEventKind = "compaction"
	SubAgentDetailEventError            SubAgentDetailEventKind = "error"
	SubAgentDetailEventApproval         SubAgentDetailEventKind = "approval"
	SubAgentDetailEventInput            SubAgentDetailEventKind = "input"
	SubAgentDetailEventCustom           SubAgentDetailEventKind = "custom"
)

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

type SubAgentDetailEvent struct {
	ID        string                  `json:"id"`
	Ordinal   int64                   `json:"ordinal"`
	ParentID  string                  `json:"parent_id,omitempty"`
	ThreadID  ThreadID                `json:"thread_id"`
	TurnID    TurnID                  `json:"turn_id,omitempty"`
	Kind      SubAgentDetailEventKind `json:"kind"`
	Type      string                  `json:"type,omitempty"`
	CreatedAt time.Time               `json:"created_at"`

	Message    *SubAgentDetailMessage    `json:"message,omitempty"`
	ToolCall   *SubAgentDetailToolCall   `json:"tool_call,omitempty"`
	ToolResult *SubAgentDetailToolResult `json:"tool_result,omitempty"`
	Approval   *SubAgentDetailApproval   `json:"approval,omitempty"`
	TurnMarker *SubAgentDetailTurnMarker `json:"turn_marker,omitempty"`
	Compaction *SubAgentDetailCompaction `json:"compaction,omitempty"`
	Error      string                    `json:"error,omitempty"`
	Metadata   map[string]string         `json:"metadata,omitempty"`

	ActivityTimeline *observation.ActivityTimeline `json:"activity_timeline,omitempty"`
}

type ThreadDetailEvent struct {
	ID        string                `json:"id"`
	Ordinal   int64                 `json:"ordinal"`
	ParentID  string                `json:"parent_id,omitempty"`
	ThreadID  ThreadID              `json:"thread_id"`
	TurnID    TurnID                `json:"turn_id,omitempty"`
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
	Role      string                            `json:"role,omitempty"`
	Kind      string                            `json:"kind,omitempty"`
	Preview   string                            `json:"preview,omitempty"`
	Content   string                            `json:"content,omitempty"`
	Reasoning string                            `json:"reasoning,omitempty"`
	Activity  *observation.ActivityPresentation `json:"activity,omitempty"`
}

type ThreadDetailToolCall struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	ArgsPreview string `json:"args_preview,omitempty"`
	ArgsJSON    string `json:"args_json,omitempty"`
	ArgsHash    string `json:"args_hash,omitempty"`
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

type SubAgentDetailMessage struct {
	Role      string `json:"role,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Preview   string `json:"preview,omitempty"`
	Content   string `json:"content,omitempty"`
	Reasoning string `json:"reasoning,omitempty"`
}

type SubAgentDetailToolCall struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	ArgsPreview string `json:"args_preview,omitempty"`
	ArgsJSON    string `json:"args_json,omitempty"`
	ArgsHash    string `json:"args_hash,omitempty"`
}

type SubAgentDetailToolResult struct {
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

type SubAgentDetailApproval struct {
	State    string            `json:"state,omitempty"`
	ToolID   string            `json:"tool_id,omitempty"`
	ToolName string            `json:"tool_name,omitempty"`
	ToolKind string            `json:"tool_kind,omitempty"`
	ArgsHash string            `json:"args_hash,omitempty"`
	Reason   string            `json:"reason,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type SubAgentDetailTurnMarker struct {
	Status   string            `json:"status,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type SubAgentDetailCompaction struct {
	Trigger             string            `json:"trigger,omitempty"`
	Reason              string            `json:"reason,omitempty"`
	Phase               string            `json:"phase,omitempty"`
	TokensBefore        int64             `json:"tokens_before,omitempty"`
	TokensAfterEstimate int64             `json:"tokens_after_estimate,omitempty"`
	Metadata            map[string]string `json:"metadata,omitempty"`
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
	ID               ThreadID        `json:"id"`
	Title            string          `json:"title,omitempty"`
	TitleStatus      string          `json:"title_status,omitempty"`
	TitleSource      string          `json:"title_source,omitempty"`
	TitleUpdatedAt   time.Time       `json:"title_updated_at,omitempty"`
	TitleError       string          `json:"title_error,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	Phase            ThreadPhase     `json:"phase"`
	Status           ThreadStatus    `json:"status"`
	LatestTurnID     TurnID          `json:"latest_turn_id,omitempty"`
	WaitingPrompt    string          `json:"waiting_prompt,omitempty"`
	Recoverable      bool            `json:"recoverable"`
	CanAppendMessage bool            `json:"can_append_message"`
	CanRetry         bool            `json:"can_retry"`
	Messages         []ThreadMessage `json:"messages"`
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

type ThreadMessage struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	TurnID    TurnID    `json:"turn_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type TurnResult struct {
	ID                 TurnID                       `json:"id"`
	RunID              RunID                        `json:"run_id,omitempty"`
	Status             TurnStatus                   `json:"status"`
	Output             string                       `json:"output,omitempty"`
	Error              string                       `json:"error,omitempty"`
	Diagnostics        map[string]string            `json:"diagnostics,omitempty"`
	Metrics            RunMetrics                   `json:"metrics"`
	CompletionReason   string                       `json:"completion_reason,omitempty"`
	ContinuationReason string                       `json:"continuation_reason,omitempty"`
	FinishReason       string                       `json:"finish_reason,omitempty"`
	RawFinishReason    string                       `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool                         `json:"finish_inferred,omitempty"`
	ProviderState      *ModelState                  `json:"provider_state,omitempty"`
	Signal             *TurnSignal                  `json:"signal,omitempty"`
	ActivityTimeline   observation.ActivityTimeline `json:"activity_timeline"`
	Projection         ThreadTurnProjection         `json:"projection,omitempty"`
	PendingApprovals   []PendingApproval            `json:"pending_approvals,omitempty"`
}

type CompactThreadResult struct {
	ThreadID         ThreadID                     `json:"thread_id,omitempty"`
	Status           string                       `json:"status"`
	Error            string                       `json:"error,omitempty"`
	Diagnostics      map[string]string            `json:"diagnostics,omitempty"`
	Metrics          RunMetrics                   `json:"metrics"`
	ProviderState    *ModelState                  `json:"provider_state,omitempty"`
	ActivityTimeline observation.ActivityTimeline `json:"activity_timeline"`
}

type EventSink interface {
	EmitEvent(Event)
}

type Event struct {
	Type             string                            `json:"type"`
	TraceID          TraceID                           `json:"trace_id,omitempty"`
	RunID            RunID                             `json:"run_id,omitempty"`
	ThreadID         ThreadID                          `json:"thread_id,omitempty"`
	TurnID           TurnID                            `json:"turn_id,omitempty"`
	Step             int                               `json:"step,omitempty"`
	Provider         string                            `json:"provider,omitempty"`
	Model            string                            `json:"model,omitempty"`
	Message          string                            `json:"message,omitempty"`
	Result           string                            `json:"result,omitempty"`
	Error            string                            `json:"error,omitempty"`
	ToolID           string                            `json:"tool_id,omitempty"`
	ToolName         string                            `json:"tool_name,omitempty"`
	ToolKind         string                            `json:"tool_kind,omitempty"`
	ArgsHash         string                            `json:"args_hash,omitempty"`
	DurationMS       int64                             `json:"duration_ms,omitempty"`
	FinishReason     string                            `json:"finish_reason,omitempty"`
	Activity         *observation.ActivityPresentation `json:"activity,omitempty"`
	ActivityTimeline *observation.ActivityTimeline     `json:"activity_timeline,omitempty"`
	Stream           *StreamObservation                `json:"stream,omitempty"`
	Committed        *ThreadDetailEvent                `json:"committed,omitempty"`
	ContextStatus    *observation.ContextStatus        `json:"context_status,omitempty"`
	Compaction       *observation.CompactionEvent      `json:"compaction,omitempty"`
	CompactionDebug  *observation.CompactionDebugEvent `json:"compaction_debug,omitempty"`
	Sources          []SourceRef                       `json:"sources,omitempty"`
	Metadata         map[string]any                    `json:"metadata,omitempty"`
	Timestamp        time.Time                         `json:"timestamp,omitempty"`
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

// StreamObservation is a provider-neutral, engine-confirmed streaming fact for
// hosts that render live assistant output from Floret runtime events.
type StreamObservation struct {
	Type            StreamObservationType `json:"type"`
	Text            string                `json:"text,omitempty"`
	ToolCallStream  *ModelToolCallStream  `json:"tool_call_stream,omitempty"`
	Reason          string                `json:"reason,omitempty"`
	FinishReason    string                `json:"finish_reason,omitempty"`
	RawFinishReason string                `json:"raw_finish_reason,omitempty"`
	FinishInferred  bool                  `json:"finish_inferred,omitempty"`
	Attempt         int                   `json:"attempt,omitempty"`
	Labels          RunLabels             `json:"labels,omitempty"`
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
	TurnStatusCompleted TurnStatus = "completed"
	TurnStatusWaiting   TurnStatus = "waiting"
	TurnStatusFailed    TurnStatus = "failed"
	TurnStatusCancelled TurnStatus = "cancelled"
)

type Store struct {
	repo       sessiontree.Repo
	prompt     cache.Store
	artifacts  artifact.Store
	deleteData func(context.Context, string) error
	close      func() error
}

func NewMemoryStore() *Store {
	repo := sessiontree.NewMemoryRepo()
	prompt := cache.NewMemoryStore()
	artifacts := artifact.NewMemoryStore()
	return &Store{
		repo:      repo,
		prompt:    prompt,
		artifacts: artifacts,
		deleteData: func(ctx context.Context, threadID string) error {
			if err := repo.DeleteThread(ctx, threadID); err != nil {
				return err
			}
			if err := prompt.DeletePromptScopes(ctx, threadID); err != nil {
				return err
			}
			return artifacts.DeleteThreadArtifacts(ctx, threadID)
		},
	}
}

func OpenSQLiteStore(path string) (*Store, error) {
	sqliteStore, err := sqlite.Open(path)
	if err != nil {
		return nil, err
	}
	return &Store{
		repo:      sqliteStore,
		prompt:    sqliteStore,
		artifacts: sqliteStore,
		deleteData: func(ctx context.Context, threadID string) error {
			return sqliteStore.DeleteThreadData(ctx, storage.DeleteThreadDataRequest{ThreadID: threadID, PromptScopeIDs: []string{threadID}})
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
	if s.repo == nil || s.prompt == nil || s.artifacts == nil || s.deleteData == nil {
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
	for i := len(threadIDs) - 1; i >= 0; i-- {
		if err := s.deleteData(ctx, threadIDs[i]); err != nil {
			return err
		}
	}
	return nil
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

type host struct {
	cfg     config.Config
	store   *Store
	sink    EventSink
	harness *agentharness.AgentHarness
}

func NewHost(opts HostOptions) (Host, error) {
	cfg, err := config.Resolve(opts.Config, nil)
	if err != nil {
		return nil, err
	}
	provider, err := projectedModelProvider(cfg, opts.ModelGateway)
	if err != nil {
		return nil, err
	}
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	harness, err := newHarnessWithProvider(cfg, provider, harnessOptions{
		Store:               store,
		Tools:               opts.Tools,
		Approver:            opts.Approver,
		Sink:                newRuntimeEventSink(opts.Sink),
		SinkPolicy:          runtimeHarnessSinkPolicy(),
		ToolSurfaceProvider: runtimeToolSurfaceProvider(opts.ToolSurfaceProvider),
		NewID:               opts.IDGenerator,
		LoopLimits:          opts.LoopLimits,
		SubAgentRunTimeout:  opts.SubAgentRunTimeout,
		Capabilities:        opts.Capabilities,
	})
	if err != nil {
		return nil, err
	}
	return &host{cfg: cfg, store: store, sink: opts.Sink, harness: harness}, nil
}

// NewLifecycleHost creates a provider-free host for thread lifecycle maintenance.
// It does not configure model transport, tools, or provider loop execution.
func NewLifecycleHost(opts LifecycleHostOptions) (LifecycleHost, error) {
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	harness := agentharness.New(agentharness.Options{
		Repo:        store.repo,
		PromptStore: store.prompt,
		Artifacts:   store.artifacts,
		Sink:        newRuntimeEventSink(opts.Sink),
		SinkPolicy:  runtimeHarnessSinkPolicy(),
	})
	return &host{store: store, sink: opts.Sink, harness: harness}, nil
}

func runtimeHarnessSinkPolicy() event.SinkPolicy {
	return event.SinkPolicy{AllowRaw: true, Redactor: event.SafePathRefsText}
}

func runtimeHostError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, agentharness.ErrSubAgentNotFound):
		return fmt.Errorf("%w: %w", ErrSubAgentNotFound, err)
	case errors.Is(err, sessiontree.ErrThreadNotFound):
		return fmt.Errorf("%w: %w", ErrThreadNotFound, err)
	default:
		return err
	}
}

func (h *host) StartThread(ctx context.Context, req StartThreadRequest) (ThreadSnapshot, error) {
	thread, err := h.harness.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return ThreadSnapshot{}, runtimeHostError(err)
	}
	return readThread(ctx, thread)
}

func (h *host) EnsureThread(ctx context.Context, req EnsureThreadRequest) (ThreadSummary, error) {
	summary, err := h.harness.EnsureThread(ctx, agentharness.StartThreadOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	return threadSummary(summary), nil
}

func (h *host) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	thread, err := h.harness.ResumeThread(ctx, string(threadID), agentharness.ResumeOptions{})
	if err != nil {
		return ThreadSnapshot{}, runtimeHostError(err)
	}
	return readThread(ctx, thread)
}

func (h *host) ListThreadDetailEvents(ctx context.Context, req ListThreadDetailEventsRequest) (ThreadDetailEvents, error) {
	detail, err := h.harness.ListThreadDetailEvents(ctx, agentharness.ListThreadDetailEventsOptions{
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

func (h *host) ListPendingApprovals(ctx context.Context, req ListPendingApprovalsRequest) (PendingApprovals, error) {
	result, err := h.harness.ListPendingApprovals(ctx, agentharness.ListPendingApprovalsOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return PendingApprovals{}, runtimeHostError(err)
	}
	return pendingApprovals(result), nil
}

func (h *host) ReadTurnProjection(ctx context.Context, req ReadTurnProjectionRequest) (ThreadTurnProjection, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return ThreadTurnProjection{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" {
		return ThreadTurnProjection{}, errors.New("turn id is required")
	}
	if strings.TrimSpace(string(req.RunID)) == "" {
		return ThreadTurnProjection{}, errors.New("run id is required")
	}
	events, err := h.listRawThreadDetailEventsForTurn(ctx, string(req.ThreadID), string(req.TurnID))
	if err != nil {
		return ThreadTurnProjection{}, runtimeHostError(err)
	}
	if len(events) == 0 {
		return ThreadTurnProjection{}, ErrTurnNotFound
	}
	if !threadDetailEventsContainRunID(events, req.RunID) {
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

func (h *host) RunTurn(ctx context.Context, req RunTurnRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.RunID)) == "" {
		return TurnResult{}, errors.New("run id is required")
	}
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" {
		return TurnResult{}, errors.New("turn id is required")
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
	result, runErr := thread.Run(ctx, req.Input, agentharness.RunOptions{
		RunID:  string(req.RunID),
		TurnID: string(req.TurnID),
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
		CompletionPolicy:         completionPolicy,
		ControlSpec:              signalSpec,
		Reasoning:                projectedReasoningSelection(req.Reasoning, h.cfg.Reasoning),
		MaxTotalTokens:           req.Limits.MaxTotalTokens,
		MaxCostUSD:               req.Limits.MaxCostUSD,
		MaxToolCalls:             req.Limits.MaxToolCalls,
		MaxLengthContinuations:   req.Limits.MaxLengthContinuations,
		MaxStopHookContinuations: req.Limits.MaxStopHookContinuations,
		PreviousProviderState:    providerState(req.PreviousProviderState),
		ManualCompactions:        projectedManualCompactionSource(req.ManualCompactions),
		ToolSurfaceProvider:      runtimeToolSurfaceProvider(req.ToolSurfaceProvider),
		Sink:                     activityRecorder,
	})
	out := turnResult(result, string(req.ThreadID), activityRecorder.Snapshot(), time.Now().UnixMilli())
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	projectionErr := h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out)
	if runErr == nil && projectionErr != nil {
		runErr = projectionErr
	}
	return out, runtimeHostError(runErr)
}

func (h *host) RetryTurn(ctx context.Context, req RetryTurnRequest) (TurnResult, error) {
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
	projectionErr := h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out)
	if runErr == nil && projectionErr != nil {
		runErr = projectionErr
	}
	return out, runtimeHostError(runErr)
}

func (h *host) CompactThread(ctx context.Context, req CompactThreadRequest) (CompactThreadResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return CompactThreadResult{}, errors.New("thread id is required")
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
		MaxTotalTokens:         req.Limits.MaxTotalTokens,
		MaxCostUSD:             req.Limits.MaxCostUSD,
		MaxToolCalls:           req.Limits.MaxToolCalls,
		MaxLengthContinuations: req.Limits.MaxLengthContinuations,
		PreviousProviderState:  providerState(req.PreviousProviderState),
		Sink:                   activityRecorder,
	})
	out := CompactThreadResult{
		ThreadID:      req.ThreadID,
		Status:        string(result.Status),
		Diagnostics:   cloneStringMap(result.Diagnostics),
		Metrics:       runtimeMetrics(result.Metrics),
		ProviderState: modelState(result.ProviderState),
		ActivityTimeline: observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    "",
			ThreadID: string(req.ThreadID),
			TurnID:   "",
			TraceID:  "",
		}, activityRecorder.Snapshot(), time.Now().UnixMilli()),
	}
	if result.Err != nil {
		out.Error = result.Err.Error()
	}
	return out, runtimeHostError(compactErr)
}

func (h *host) CompletePendingTool(ctx context.Context, req PendingToolCompletionRequest) (TurnResult, error) {
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
	projectionErr := h.attachThreadTurnProjection(projectionCtx, string(req.ThreadID), &out)
	if runErr == nil && projectionErr != nil {
		runErr = projectionErr
	}
	return out, runtimeHostError(runErr)
}

func (h *host) SettlePendingTool(ctx context.Context, req PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return PendingToolSettlementResult{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" {
		return PendingToolSettlementResult{}, errors.New("turn id is required")
	}
	if strings.TrimSpace(string(req.RunID)) == "" {
		return PendingToolSettlementResult{}, errors.New("run id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	event, err := thread.SettlePendingTool(ctx, agentharness.PendingToolSettlement{
		TurnID:     string(req.TurnID),
		RunID:      string(req.RunID),
		ToolCallID: req.ToolCallID,
		ToolName:   req.ToolName,
		Handle:     req.Handle,
		Status:     pendingToolSettlementStatus(req.Status),
		Summary:    req.Summary,
		Output:     req.Output,
		Activity:   observation.CloneActivityPresentation(req.Activity),
	})
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	out := PendingToolSettlementResult{
		ThreadID: req.ThreadID,
		TurnID:   req.TurnID,
		RunID:    req.RunID,
		Event:    threadDetailEvent(event),
	}
	projectionCtx, cancelProjection := runtimeTerminalProjectionContext(ctx)
	defer cancelProjection()
	events, err := h.listRawThreadDetailEventsForTurn(projectionCtx, string(req.ThreadID), string(req.TurnID))
	if err != nil {
		return PendingToolSettlementResult{}, runtimeHostError(err)
	}
	out.Projection = ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: req.ThreadID,
		TurnID:   req.TurnID,
		RunID:    req.RunID,
		TraceID:  TraceID(req.RunID),
		Events:   events,
	})
	return out, nil
}

func (h *host) SpawnSubAgent(ctx context.Context, req SpawnSubAgentRequest) (SubAgentSnapshot, error) {
	snapshot, err := h.harness.SpawnSubAgent(ctx, agentharness.SpawnSubAgentOptions{
		ParentThreadID: string(req.ParentThreadID),
		ParentTurnID:   string(req.ParentTurnID),
		ThreadID:       string(req.ThreadID),
		TaskName:       req.TaskName,
		Message:        req.Message,
		HostProfileRef: req.HostProfileRef,
		ForkMode:       agentharness.SubAgentForkMode(req.ForkMode),
		Labels:         engineLabels(req.Labels),
	})
	if err != nil {
		return SubAgentSnapshot{}, runtimeHostError(err)
	}
	return subAgentSnapshot(snapshot), nil
}

func (h *host) SendSubAgentInput(ctx context.Context, req SendSubAgentInputRequest) (SubAgentSnapshot, error) {
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

func (h *host) WaitSubAgents(ctx context.Context, req WaitSubAgentsRequest) (WaitSubAgentsResult, error) {
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

func (h *host) ListSubAgents(ctx context.Context, parentThreadID ThreadID) ([]SubAgentSnapshot, error) {
	snapshots, err := h.harness.ListSubAgents(ctx, string(parentThreadID))
	if err != nil {
		return nil, runtimeHostError(err)
	}
	out := make([]SubAgentSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, subAgentSnapshot(snapshot))
	}
	return out, nil
}

func (h *host) CloseSubAgent(ctx context.Context, req CloseSubAgentRequest) (SubAgentSnapshot, error) {
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

func (h *host) CloseSubAgents(ctx context.Context, req CloseSubAgentsRequest) (CloseSubAgentsResult, error) {
	result, err := h.harness.CloseSubAgents(ctx, agentharness.CloseSubAgentsOptions{
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

func (h *host) ListSubAgentActivityTimeline(ctx context.Context, req ListSubAgentActivityTimelineRequest) (SubAgentActivityTimelineResult, error) {
	snapshots, err := h.harness.ListSubAgents(ctx, string(req.ParentThreadID))
	if err != nil {
		return SubAgentActivityTimelineResult{}, runtimeHostError(err)
	}
	generatedAt := time.Now()
	return SubAgentActivityTimelineResult{
		Timeline:    subAgentActivityTimeline(req.Meta, snapshots, generatedAt),
		GeneratedAt: generatedAt,
	}, nil
}

func (h *host) ReadSubAgentDetail(ctx context.Context, req ReadSubAgentDetailRequest) (SubAgentDetail, error) {
	detail, err := h.harness.ReadSubAgentDetail(ctx, agentharness.ReadSubAgentDetailOptions{
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

func (h *host) ListSubAgentDetailEvents(ctx context.Context, req ListSubAgentDetailEventsRequest) (SubAgentDetailEvents, error) {
	detail, err := h.harness.ReadSubAgentDetail(ctx, agentharness.ReadSubAgentDetailOptions{
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
		Events:       subAgentDetailEvents(detail.Events),
		NextOrdinal:  detail.NextOrdinal,
		HasMore:      detail.HasMore,
		RetainedFrom: detail.RetainedFrom,
		GeneratedAt:  detail.GeneratedAt,
	}, nil
}

func (h *host) DeleteThread(ctx context.Context, threadID ThreadID) error {
	id := strings.TrimSpace(string(threadID))
	if id == "" {
		return errors.New("thread id is required")
	}
	return runtimeHostError(h.store.deleteThreadData(ctx, id))
}

func (h *host) Close() error {
	return h.store.Close()
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
		WaitingPrompt:    in.WaitingPrompt,
		Recoverable:      in.Recoverable,
		CanAppendMessage: in.CanAppendMessage,
		CanRetry:         in.CanRetry,
		Messages:         make([]ThreadMessage, 0, len(in.Messages)),
	}
	for _, msg := range in.Messages {
		out.Messages = append(out.Messages, ThreadMessage{
			Role:      string(msg.Role),
			Content:   msg.Content,
			TurnID:    TurnID(msg.TurnID),
			CreatedAt: msg.CreatedAt,
		})
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
		ID:                 TurnID(in.ID),
		RunID:              RunID(in.RunID),
		Status:             TurnStatus(in.Status),
		Output:             in.Output,
		Diagnostics:        cloneStringMap(in.Diagnostics),
		Metrics:            runtimeMetrics(in.Metrics),
		CompletionReason:   string(in.CompletionReason),
		ContinuationReason: string(in.ContinuationReason),
		FinishReason:       string(in.FinishReason),
		RawFinishReason:    in.RawFinishReason,
		FinishInferred:     in.FinishInferred,
		ProviderState:      modelState(in.ProviderState),
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

func (h *host) attachThreadTurnProjection(ctx context.Context, threadID string, result *TurnResult) error {
	if h == nil || result == nil || strings.TrimSpace(threadID) == "" || strings.TrimSpace(string(result.ID)) == "" {
		return nil
	}
	events, err := h.listRawThreadDetailEventsForTurn(ctx, threadID, string(result.ID))
	if err != nil {
		return err
	}
	result.Projection = ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: ThreadID(threadID),
		TurnID:   result.ID,
		RunID:    result.RunID,
		TraceID:  TraceID(result.RunID),
		Events:   events,
	})
	return nil
}

func runtimeTerminalProjectionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func (h *host) listRawThreadDetailEventsForTurn(ctx context.Context, threadID string, turnID string) ([]ThreadDetailEvent, error) {
	var out []ThreadDetailEvent
	var afterOrdinal int64
	for {
		detail, err := h.harness.ListThreadDetailEvents(ctx, agentharness.ListThreadDetailEventsOptions{
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

func threadDetailEventsContainRunID(events []ThreadDetailEvent, runID RunID) bool {
	want := strings.TrimSpace(string(runID))
	if want == "" {
		return false
	}
	for _, ev := range events {
		for _, got := range []string{
			threadDetailMetadataRunID(ev.Metadata),
			threadDetailTurnMarkerRunID(ev.TurnMarker),
		} {
			if strings.TrimSpace(got) == want {
				return true
			}
		}
	}
	return false
}

func threadDetailMetadataRunID(metadata map[string]string) string {
	for _, key := range []string{"run_id", "pending_tool_settlement_run_id"} {
		if value := strings.TrimSpace(metadata[key]); value != "" {
			return value
		}
	}
	return ""
}

func threadDetailTurnMarkerRunID(marker *ThreadDetailTurnMarker) string {
	if marker == nil {
		return ""
	}
	return strings.TrimSpace(marker.Metadata["run_id"])
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
		ThreadID:       ThreadID(in.ThreadID),
		Path:           in.Path,
		TaskName:       in.TaskName,
		ParentThreadID: ThreadID(in.ParentThreadID),
		ParentTurnID:   TurnID(in.ParentTurnID),
		HostProfileRef: in.HostProfileRef,
		ForkMode:       SubAgentForkMode(in.ForkMode),
		Status:         SubAgentStatus(in.Status),
		LatestTurnID:   TurnID(in.LatestTurnID),
		LastMessage:    in.LastMessage,
		WaitingPrompt:  in.WaitingPrompt,
		QueuedInputs:   in.QueuedInputs,
		CreatedAt:      in.CreatedAt,
		UpdatedAt:      in.UpdatedAt,
		Closed:         in.Closed,
		CanSendInput:   in.CanSendInput,
		CanInterrupt:   in.CanInterrupt,
		CanClose:       in.CanClose,
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
		description := firstRuntimeNonEmpty(strings.TrimSpace(snapshot.LastMessage), strings.TrimSpace(string(snapshot.Status)))
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
		"subagent_id":      strings.TrimSpace(snapshot.ThreadID),
		"thread_id":        strings.TrimSpace(snapshot.ThreadID),
		"path":             strings.TrimSpace(snapshot.Path),
		"task_name":        strings.TrimSpace(snapshot.TaskName),
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
		Snapshot:     subAgentSnapshot(in.Snapshot),
		Events:       subAgentDetailEvents(in.Events),
		NextOrdinal:  in.NextOrdinal,
		HasMore:      in.HasMore,
		RetainedFrom: in.RetainedFrom,
		GeneratedAt:  in.GeneratedAt,
	}
}

func subAgentDetailEvents(in []agentharness.SubAgentDetailEvent) []SubAgentDetailEvent {
	out := make([]SubAgentDetailEvent, 0, len(in))
	for _, ev := range in {
		out = append(out, subAgentDetailEvent(ev))
	}
	return out
}

func threadDetailEvents(in []agentharness.SubAgentDetailEvent) []ThreadDetailEvent {
	out := make([]ThreadDetailEvent, 0, len(in))
	for _, ev := range in {
		out = append(out, threadDetailEvent(ev))
	}
	return out
}

func subAgentDetailEvent(in agentharness.SubAgentDetailEvent) SubAgentDetailEvent {
	return SubAgentDetailEvent{
		ID:         in.ID,
		Ordinal:    in.Ordinal,
		ParentID:   in.ParentID,
		ThreadID:   ThreadID(in.ThreadID),
		TurnID:     TurnID(in.TurnID),
		Kind:       SubAgentDetailEventKind(in.Kind),
		Type:       in.Type,
		CreatedAt:  in.CreatedAt,
		Message:    subAgentDetailMessage(in.Message),
		ToolCall:   subAgentDetailToolCall(in.ToolCall),
		ToolResult: subAgentDetailToolResult(in.ToolResult),
		Approval:   subAgentDetailApproval(in.Approval),
		TurnMarker: subAgentDetailTurnMarker(in.TurnMarker),
		Compaction: subAgentDetailCompaction(in.Compaction),
		Error:      in.Error,
		Metadata:   cloneStringMap(in.Metadata),

		ActivityTimeline: observation.CloneActivityTimeline(in.ActivityTimeline),
	}
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
		Role:      in.Role,
		Kind:      in.Kind,
		Preview:   in.Preview,
		Content:   in.Content,
		Reasoning: in.Reasoning,
		Activity:  cloneActivityPresentation(in.Activity),
	}
}

func threadDetailToolCall(in *agentharness.SubAgentDetailToolCall) *ThreadDetailToolCall {
	if in == nil {
		return nil
	}
	return &ThreadDetailToolCall{ID: in.ID, Name: in.Name, ArgsPreview: in.ArgsPreview, ArgsJSON: in.ArgsJSON, ArgsHash: in.ArgsHash}
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
		Metadata:                cloneStringMap(in.Metadata),
	}
}

func subAgentDetailMessage(in *agentharness.SubAgentDetailMessage) *SubAgentDetailMessage {
	if in == nil {
		return nil
	}
	return &SubAgentDetailMessage{Role: in.Role, Kind: in.Kind, Preview: in.Preview, Content: in.Content, Reasoning: in.Reasoning}
}

func subAgentDetailToolCall(in *agentharness.SubAgentDetailToolCall) *SubAgentDetailToolCall {
	if in == nil {
		return nil
	}
	return &SubAgentDetailToolCall{ID: in.ID, Name: in.Name, ArgsPreview: in.ArgsPreview, ArgsJSON: in.ArgsJSON, ArgsHash: in.ArgsHash}
}

func subAgentDetailToolResult(in *agentharness.SubAgentDetailToolResult) *SubAgentDetailToolResult {
	if in == nil {
		return nil
	}
	out := &SubAgentDetailToolResult{
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

func subAgentDetailApproval(in *agentharness.SubAgentDetailApproval) *SubAgentDetailApproval {
	if in == nil {
		return nil
	}
	return &SubAgentDetailApproval{
		State:    in.State,
		ToolID:   in.ToolID,
		ToolName: in.ToolName,
		ToolKind: in.ToolKind,
		ArgsHash: in.ArgsHash,
		Reason:   in.Reason,
		Metadata: cloneStringMap(in.Metadata),
	}
}

func subAgentDetailTurnMarker(in *agentharness.SubAgentDetailTurnMarker) *SubAgentDetailTurnMarker {
	if in == nil {
		return nil
	}
	return &SubAgentDetailTurnMarker{Status: in.Status, Metadata: cloneStringMap(in.Metadata)}
}

func subAgentDetailCompaction(in *agentharness.SubAgentDetailCompaction) *SubAgentDetailCompaction {
	if in == nil {
		return nil
	}
	return &SubAgentDetailCompaction{
		Trigger:             in.Trigger,
		Reason:              in.Reason,
		Phase:               in.Phase,
		TokensBefore:        in.TokensBefore,
		TokensAfterEstimate: in.TokensAfterEstimate,
		Metadata:            safeStringMetadata(in.Metadata),
	}
}

func threadIDStrings(ids []ThreadID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, string(id))
	}
	return out
}

type harnessOptions struct {
	Store               *Store
	Tools               *tools.Registry
	Approver            tools.Approver
	Sink                event.Sink
	SinkPolicy          event.SinkPolicy
	Title               agentharness.TitleGenerator
	NewID               func(string) string
	LoopLimits          LoopLimits
	SubAgentRunTimeout  time.Duration
	Capabilities        CapabilityOptions
	ToolSurfaceProvider engine.ToolSurfaceProvider
}

func newHarnessWithProvider(cfg config.Config, p provider.Provider, opts harnessOptions) (*agentharness.AgentHarness, error) {
	cfg = config.ResolvePrompt(cfg)
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
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
	return agentharness.New(agentharness.Options{
		Provider:            p,
		ProviderName:        cfg.Provider,
		Model:               cfg.Model,
		SystemPrompt:        effectivePrompt,
		Tools:               registry,
		PromptStore:         store.prompt,
		Repo:                store.repo,
		Sink:                opts.Sink,
		SinkPolicy:          opts.SinkPolicy,
		Approver:            opts.Approver,
		ToolSurfaceProvider: opts.ToolSurfaceProvider,
		TitleGenerator:      opts.Title,
		CompactionPrompt:    compaction.PromptOptions{},
		Artifacts:           store.artifacts,
		Reasoning:           model.Reasoning,
		TurnPolicy:          turnPolicy,
		LoopLimits:          loopLimits,
		SubAgentRunTimeout:  opts.SubAgentRunTimeout,
		NewID:               opts.NewID,
	}), nil
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
	mu   *sync.Mutex
	sink EventSink
}

func newRuntimeEventSink(sink EventSink) runtimeEventSink {
	if sink == nil {
		return runtimeEventSink{}
	}
	return runtimeEventSink{mu: &sync.Mutex{}, sink: sink}
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
	committed := runtimeCommittedEvent(ev, sanitized)
	compactionEvent := runtimeCompactionEventWithError(ev, sanitized, sanitized.Err)
	compactionDebugEvent := runtimeCompactionDebugEventWithError(ev, sanitized, sanitized.Err)
	stream := runtimeStreamObservation(ev, sanitized.Metadata)
	ev = sanitized
	return Event{
		Type:            string(ev.Type),
		TraceID:         TraceID(ev.TraceID),
		RunID:           RunID(ev.RunID),
		ThreadID:        ThreadID(ev.ThreadID),
		TurnID:          TurnID(ev.TurnID),
		Step:            ev.Step,
		Provider:        ev.Provider,
		Model:           ev.Model,
		Message:         ev.Message,
		Result:          ev.Result,
		Error:           ev.Err,
		ToolID:          ev.ToolID,
		ToolName:        ev.ToolName,
		ToolKind:        ev.ToolKind,
		ArgsHash:        ev.ArgsHash,
		DurationMS:      ev.Duration,
		FinishReason:    ev.FinishReason,
		Activity:        cloneActivityPresentation(ev.Activity),
		Stream:          stream,
		Committed:       committed,
		ContextStatus:   contextStatus,
		Compaction:      compactionEvent,
		CompactionDebug: compactionDebugEvent,
		Sources:         runtimeSourceRefs(ev.Sources),
		Metadata:        safeMetadata(ev.Metadata),
		Timestamp:       ev.Timestamp,
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
	if out.Ordinal == 0 {
		out.Ordinal = int64FromMetadata(meta, "ordinal")
	}
	if out.CreatedAt.IsZero() {
		out.CreatedAt = sanitized.Timestamp
	}
	return &out
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
	phase := stringFromMetadata(meta, "phase")
	if phase == "" || (sanitizedError != "" && phase != observation.CompactionPhaseFailed && phase != observation.CompactionPhaseCancelled) {
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
	stage := stringFromMetadata(meta, "stage")
	status := stringFromMetadata(meta, "status")
	if stage == "" || status == "" {
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
		FinishReason:    ev.FinishReason,
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
		Type:            string(sanitized.Type),
		TraceID:         sanitized.TraceID,
		RunID:           sanitized.RunID,
		ThreadID:        sanitized.ThreadID,
		TurnID:          sanitized.TurnID,
		Step:            sanitized.Step,
		Provider:        sanitized.Provider,
		Model:           sanitized.Model,
		Message:         sanitized.Message,
		Result:          sanitized.Result,
		Error:           sanitized.Err,
		ToolID:          sanitized.ToolID,
		ToolName:        sanitized.ToolName,
		ToolKind:        sanitized.ToolKind,
		ArgsHash:        sanitized.ArgsHash,
		DurationMS:      sanitized.Duration,
		FinishReason:    sanitized.FinishReason,
		Activity:        cloneActivityPresentation(sanitized.Activity),
		Compaction:      runtimeCompactionEventWithError(ev, sanitized, sanitized.Err),
		CompactionDebug: runtimeCompactionDebugEventWithError(ev, sanitized, sanitized.Err),
		Metadata:        safeMetadata(sanitized.Metadata),
		ObservedAt:      sanitized.Timestamp,
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
