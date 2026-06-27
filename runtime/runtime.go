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

type Host interface {
	StartThread(context.Context, StartThreadRequest) (ThreadSnapshot, error)
	ReadThread(context.Context, ThreadID) (ThreadSnapshot, error)
	RunTurn(context.Context, RunTurnRequest) (TurnResult, error)
	RetryTurn(context.Context, RetryTurnRequest) (TurnResult, error)
	CompactThread(context.Context, CompactThreadRequest) (CompactThreadResult, error)
	CompletePendingTool(context.Context, PendingToolCompletionRequest) (TurnResult, error)
	SpawnSubAgent(context.Context, SpawnSubAgentRequest) (SubAgentSnapshot, error)
	SendSubAgentInput(context.Context, SendSubAgentInputRequest) (SubAgentSnapshot, error)
	WaitSubAgents(context.Context, WaitSubAgentsRequest) (WaitSubAgentsResult, error)
	ListSubAgents(context.Context, ThreadID) ([]SubAgentSnapshot, error)
	CloseSubAgent(context.Context, CloseSubAgentRequest) (SubAgentSnapshot, error)
	ReadSubAgentDetail(context.Context, ReadSubAgentDetailRequest) (SubAgentDetail, error)
	ListSubAgentDetailEvents(context.Context, ListSubAgentDetailEventsRequest) (SubAgentDetailEvents, error)
	DeleteThread(context.Context, ThreadID) error
	Close() error
}

type HostOptions struct {
	Config             config.Config
	ModelGateway       ModelGateway
	Store              *Store
	Tools              *tools.Registry
	Approver           tools.Approver
	Sink               EventSink
	IDGenerator        func(string) string
	LoopLimits         LoopLimits
	SubAgentRunTimeout time.Duration
	Capabilities       CapabilityOptions
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

type RunTurnRequest struct {
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

type SubAgentSnapshot struct {
	ThreadID       ThreadID       `json:"thread_id"`
	Path           string         `json:"path"`
	TaskName       string         `json:"task_name"`
	ParentThreadID ThreadID       `json:"parent_thread_id"`
	ParentTurnID   TurnID         `json:"parent_turn_id,omitempty"`
	HostProfileRef string         `json:"host_profile_ref,omitempty"`
	Status         SubAgentStatus `json:"status"`
	LatestTurnID   TurnID         `json:"latest_turn_id,omitempty"`
	LastMessage    string         `json:"last_message,omitempty"`
	WaitingPrompt  string         `json:"waiting_prompt,omitempty"`
	QueuedInputs   int            `json:"queued_inputs,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	Closed         bool           `json:"closed,omitempty"`
	CanSendInput   bool           `json:"can_send_input"`
	CanInterrupt   bool           `json:"can_interrupt"`
	CanClose       bool           `json:"can_close"`
}

type WaitSubAgentsResult struct {
	Snapshots []SubAgentSnapshot `json:"snapshots"`
	TimedOut  bool               `json:"timed_out,omitempty"`
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

type SubAgentDetailEventKind string

const (
	SubAgentDetailEventUserMessage      SubAgentDetailEventKind = "user_message"
	SubAgentDetailEventAssistantMessage SubAgentDetailEventKind = "assistant_message"
	SubAgentDetailEventToolCall         SubAgentDetailEventKind = "tool_call"
	SubAgentDetailEventToolResult       SubAgentDetailEventKind = "tool_result"
	SubAgentDetailEventTurnMarker       SubAgentDetailEventKind = "turn_marker"
	SubAgentDetailEventCompaction       SubAgentDetailEventKind = "compaction"
	SubAgentDetailEventError            SubAgentDetailEventKind = "error"
	SubAgentDetailEventApproval         SubAgentDetailEventKind = "approval"
	SubAgentDetailEventInput            SubAgentDetailEventKind = "input"
	SubAgentDetailEventCustom           SubAgentDetailEventKind = "custom"
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
}

type SubAgentDetailMessage struct {
	Role      string `json:"role,omitempty"`
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

type ThreadMessage struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	TurnID    TurnID    `json:"turn_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type TurnResult struct {
	ID                 TurnID                       `json:"id"`
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
	Type            string                            `json:"type"`
	TraceID         TraceID                           `json:"trace_id,omitempty"`
	RunID           RunID                             `json:"run_id,omitempty"`
	ThreadID        ThreadID                          `json:"thread_id,omitempty"`
	TurnID          TurnID                            `json:"turn_id,omitempty"`
	Step            int                               `json:"step,omitempty"`
	Provider        string                            `json:"provider,omitempty"`
	Model           string                            `json:"model,omitempty"`
	Message         string                            `json:"message,omitempty"`
	Result          string                            `json:"result,omitempty"`
	Error           string                            `json:"error,omitempty"`
	ToolID          string                            `json:"tool_id,omitempty"`
	ToolName        string                            `json:"tool_name,omitempty"`
	ToolKind        string                            `json:"tool_kind,omitempty"`
	ArgsHash        string                            `json:"args_hash,omitempty"`
	DurationMS      int64                             `json:"duration_ms,omitempty"`
	FinishReason    string                            `json:"finish_reason,omitempty"`
	Activity        *observation.ActivityPresentation `json:"activity,omitempty"`
	Stream          *StreamObservation                `json:"stream,omitempty"`
	ContextStatus   *observation.ContextStatus        `json:"context_status,omitempty"`
	Compaction      *observation.CompactionEvent      `json:"compaction,omitempty"`
	CompactionDebug *observation.CompactionDebugEvent `json:"compaction_debug,omitempty"`
	Sources         []SourceRef                       `json:"sources,omitempty"`
	Metadata        map[string]any                    `json:"metadata,omitempty"`
	Timestamp       time.Time                         `json:"timestamp,omitempty"`
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
	return s.deleteData(ctx, threadID)
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
		Store:              store,
		Tools:              opts.Tools,
		Approver:           opts.Approver,
		Sink:               newRuntimeEventSink(opts.Sink),
		NewID:              opts.IDGenerator,
		LoopLimits:         opts.LoopLimits,
		SubAgentRunTimeout: opts.SubAgentRunTimeout,
		Capabilities:       opts.Capabilities,
	})
	if err != nil {
		return nil, err
	}
	return &host{cfg: cfg, store: store, sink: opts.Sink, harness: harness}, nil
}

func (h *host) StartThread(ctx context.Context, req StartThreadRequest) (ThreadSnapshot, error) {
	thread, err := h.harness.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return readThread(ctx, thread)
}

func (h *host) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	thread, err := h.harness.ResumeThread(ctx, string(threadID), agentharness.ResumeOptions{})
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return readThread(ctx, thread)
}

func (h *host) RunTurn(ctx context.Context, req RunTurnRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
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
		return TurnResult{}, err
	}
	activityRecorder := &runtimeActivityEventRecorder{sink: newRuntimeEventSink(h.sink)}
	result, runErr := thread.Run(ctx, req.Input, agentharness.RunOptions{
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
		Sink:                     activityRecorder,
	})
	return turnResult(result, activityRecorder.Snapshot(), time.Now().UnixMilli()), runErr
}

func (h *host) RetryTurn(ctx context.Context, req RetryTurnRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, err
	}
	result, runErr := thread.Retry(ctx, agentharness.RetryOptions{
		Reason: req.Reason,
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
	})
	return turnResult(result, nil, time.Now().UnixMilli()), runErr
}

func (h *host) CompactThread(ctx context.Context, req CompactThreadRequest) (CompactThreadResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return CompactThreadResult{}, errors.New("thread id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return CompactThreadResult{}, err
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
	return out, compactErr
}

func (h *host) CompletePendingTool(ctx context.Context, req PendingToolCompletionRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, err
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
	return turnResult(result, nil, time.Now().UnixMilli()), runErr
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
		return SubAgentSnapshot{}, err
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
		return SubAgentSnapshot{}, err
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
		return WaitSubAgentsResult{}, err
	}
	return waitSubAgentsResult(result), nil
}

func (h *host) ListSubAgents(ctx context.Context, parentThreadID ThreadID) ([]SubAgentSnapshot, error) {
	snapshots, err := h.harness.ListSubAgents(ctx, string(parentThreadID))
	if err != nil {
		return nil, err
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
	})
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	return subAgentSnapshot(snapshot), nil
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
		return SubAgentDetail{}, err
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
		return SubAgentDetailEvents{}, err
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
	return h.store.deleteThreadData(ctx, id)
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

func turnResult(in agentharness.TurnResult, events []observation.Event, nowUnixMS int64) TurnResult {
	out := TurnResult{
		ID:                 TurnID(in.ID),
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
			RunID:    in.ID,
			ThreadID: "",
			TurnID:   in.ID,
			TraceID:  in.ID,
		}, events, nowUnixMS),
	}
	if in.Err != nil {
		out.Error = in.Err.Error()
	}
	return out
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
	}
}

func subAgentDetailMessage(in *agentharness.SubAgentDetailMessage) *SubAgentDetailMessage {
	if in == nil {
		return nil
	}
	return &SubAgentDetailMessage{Role: in.Role, Preview: in.Preview, Content: in.Content, Reasoning: in.Reasoning}
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
	Store              *Store
	Tools              *tools.Registry
	Approver           tools.Approver
	Sink               event.Sink
	Title              agentharness.TitleGenerator
	NewID              func(string) string
	LoopLimits         LoopLimits
	SubAgentRunTimeout time.Duration
	Capabilities       CapabilityOptions
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
		Provider:           p,
		ProviderName:       cfg.Provider,
		Model:              cfg.Model,
		SystemPrompt:       effectivePrompt,
		Tools:              registry,
		PromptStore:        store.prompt,
		Repo:               store.repo,
		Sink:               opts.Sink,
		Approver:           opts.Approver,
		TitleGenerator:     opts.Title,
		CompactionPrompt:   compaction.PromptOptions{},
		Artifacts:          store.artifacts,
		Reasoning:          model.Reasoning,
		TurnPolicy:         turnPolicy,
		LoopLimits:         loopLimits,
		SubAgentRunTimeout: opts.SubAgentRunTimeout,
		NewID:              opts.NewID,
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
	if s.sink == nil {
		return
	}
	if s.mu != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	s.sink.EmitEvent(runtimeEvent(ev))
}

func runtimeEvent(ev event.Event) Event {
	contextStatus := runtimeContextStatus(ev)
	sanitized := event.Sanitize(ev)
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
		ContextStatus:   contextStatus,
		Compaction:      compactionEvent,
		CompactionDebug: compactionDebugEvent,
		Sources:         runtimeSourceRefs(ev.Sources),
		Metadata:        safeMetadata(ev.Metadata),
		Timestamp:       ev.Timestamp,
	}
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
	sink   event.Sink
}

func (r *runtimeActivityEventRecorder) Emit(ev event.Event) {
	r.mu.Lock()
	r.events = append(r.events, runtimeObservationEvent(ev))
	r.mu.Unlock()
	if r.sink != nil {
		r.sink.Emit(ev)
	}
}

func (r *runtimeActivityEventRecorder) Snapshot() []observation.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observation.Event(nil), r.events...)
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
