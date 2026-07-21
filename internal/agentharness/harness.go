package agentharness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/engine"
	enginecompaction "github.com/floegence/floret/internal/engine/compaction"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

var (
	ErrActiveTurn                              = errors.New("thread already has an active turn")
	ErrNoRetryTarget                           = errors.New("thread has no retryable turn")
	ErrPendingToolSettlementTargetTurnNotFound = errors.New("pending tool settlement target turn was not found")
	ErrPendingToolSettlementTargetRunNotFound  = errors.New("pending tool settlement target run was not found")
	ErrPendingToolSettlementTargetToolNotFound = errors.New("pending tool settlement target tool call was not found")
	ErrPendingToolSettlementTargetNotActive    = errors.New("pending tool settlement target is not an active pending tool result")
	ErrPendingToolSettlementConflict           = errors.New("pending tool settlement conflicts with existing settlement")
	ErrForkOperationConflict                   = errors.New("fork operation conflicts with existing request")
	ErrJournalInvariant                        = errors.New("thread journal invariant violated")
)

const (
	threadPhaseIdle       = sessionlifecycle.PhaseIdle
	threadPhaseTurn       = sessionlifecycle.PhaseTurn
	staleTurnLeaseTimeout = 24 * time.Hour
)

type HarnessEventType string

const (
	EventThreadStarted     HarnessEventType = "thread_started"
	EventThreadForked      HarnessEventType = "thread_forked"
	EventTurnStarted       HarnessEventType = "turn_started"
	EventTurnCompleted     HarnessEventType = "turn_completed"
	EventTurnFailed        HarnessEventType = "turn_failed"
	EventTurnAborted       HarnessEventType = "turn_aborted"
	EventEntryAppended     HarnessEventType = "entry_appended"
	EventRetryStarted      HarnessEventType = "retry_started"
	EventTitlePending      HarnessEventType = "thread_title_pending"
	EventTitleUpdated      HarnessEventType = "thread_title_updated"
	EventTitleFailed       HarnessEventType = "thread_title_failed"
	EventSubAgentSpawned   HarnessEventType = "subagent_spawned"
	EventSubAgentInput     HarnessEventType = "subagent_input"
	EventSubAgentClosed    HarnessEventType = "subagent_closed"
	EventSubAgentCompleted HarnessEventType = "subagent_completed"
)

type HarnessEvent struct {
	Type      HarnessEventType  `json:"type"`
	RunID     string            `json:"run_id,omitempty"`
	ThreadID  string            `json:"thread_id,omitempty"`
	TurnID    string            `json:"turn_id,omitempty"`
	EntryID   string            `json:"entry_id,omitempty"`
	ParentID  string            `json:"parent_id,omitempty"`
	Message   string            `json:"message,omitempty"`
	Status    string            `json:"status,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

type HarnessSink interface {
	EmitHarness(HarnessEvent)
}

type Options struct {
	Provider                 provider.Provider
	ProviderName             string
	Model                    string
	SystemPrompt             string
	Tools                    *tools.Registry
	PromptStore              cache.Store
	Repo                     sessiontree.JournalRepo
	ForkOperations           storage.ForkOperationStore
	StateCompatibilityKey    string
	Sink                     event.Sink
	SinkPolicy               event.SinkPolicy
	HarnessSink              HarnessSink
	EffectAuthorizationGate  EffectAuthorizationGate
	ToolSurfaceProvider      engine.ToolSurfaceProvider
	StopHook                 engine.StopHook
	CompactionGenerator      compaction.SummaryGenerator
	CompactionPrompt         compaction.PromptOptions
	CompactionPromptIdentity string
	TitleGenerator           TitleGenerator
	Reasoning                provider.ReasoningCapability
	TurnPolicy               TurnPolicy
	LoopLimits               LoopLimits
	SubAgentRunTimeout       time.Duration
	AutomaticTitleTimeout    time.Duration
	BeginBackgroundExecution func() (context.Context, func(), error)
	ReportBackgroundError    func(error)
	TurnExecutions           *TurnExecutionRegistry
	NewID                    func(string) string
	Now                      func() time.Time
}

type TurnPolicy struct {
	ContextPolicy         contextpolicy.Policy
	Reasoning             provider.ReasoningSelection
	CacheRetention        cache.Retention
	HostedToolDefinitions []provider.HostedToolDefinition
	CompletionPolicy      engine.CompletionPolicy
}

type LoopLimits struct {
	MaxEmptyProviderRetries  int
	NoProgressLimit          int
	DuplicateToolLimit       int
	WallTime                 time.Duration
	MaxInputTokens           int64
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	MaxLengthContinuations   int
	MaxStopHookContinuations int
}

type AgentHarness struct {
	mu                          sync.Mutex
	subagentSpawnMu             sync.Mutex
	options                     Options
	effectFinalizationTimeout   time.Duration
	effectOutcomeFingerprinter  func(tools.Result, session.Message, *artifact.FullOutput) (string, error)
	effectFinalizerRegistration func(error)
	threads                     map[string]*Thread
	subagents                   map[string]*subagentController
	subagentUpdates             chan struct{}
}

type ResumeOptions struct{}

type ForkOptions struct {
	SourceThreadID string
	EntryID        string
	Position       sessiontree.ForkPosition
	NewThreadID    string
	OperationID    string
}

type ForkResult struct {
	OperationID string
	Thread      *Thread
	Summary     ThreadSummary
}

type RunOptions struct {
	RunID                    string
	TurnID                   string
	AdmittedInputID          string
	AdmissionCommitted       bool
	AdmissionBaseLeafID      string
	Labels                   engine.RunLabels
	TerminalMetadata         map[string]string
	DeadlineMetadata         map[string]string
	CompletionPolicy         engine.CompletionPolicy
	ControlSpec              engine.ControlSpec
	Reasoning                provider.ReasoningSelection
	MaxInputTokens           int64
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	MaxLengthContinuations   int
	MaxStopHookContinuations int
	ManualCompactions        engine.ManualCompactionSource
	ToolSurfaceProvider      engine.ToolSurfaceProvider
	SupplementalContext      []engine.TurnSupplementalContextItem
	Attachments              []session.MessageAttachment
	References               []session.MessageReference
	Sink                     event.Sink
}

type CompactOptions struct {
	RequestID              string
	Source                 string
	Labels                 engine.RunLabels
	Reasoning              provider.ReasoningSelection
	MaxInputTokens         int64
	MaxTotalTokens         int64
	MaxCostUSD             float64
	MaxToolCalls           int
	MaxLengthContinuations int
	Sink                   event.Sink
}

type RetryOptions struct {
	Reason string
	Labels engine.RunLabels
}

type PendingToolCompletionStatus string

const (
	PendingToolCompleted PendingToolCompletionStatus = "completed"
	PendingToolFailed    PendingToolCompletionStatus = "failed"
	PendingToolCanceled  PendingToolCompletionStatus = "canceled"
)

type PendingToolCompletion struct {
	CompletionRequestID string
	Target              sessiontree.PendingToolSettlementTarget
	ContinuationTurnID  string
	ContinuationRunID   string
	Status              PendingToolCompletionStatus
	Summary             string
	Output              string
	Input               session.Message
	Labels              engine.RunLabels
}

type PendingToolSettlementStatus string

const (
	PendingToolSettledCompleted PendingToolSettlementStatus = "completed"
	PendingToolSettledFailed    PendingToolSettlementStatus = "failed"
	PendingToolSettledCanceled  PendingToolSettlementStatus = "canceled"
)

type PendingToolSettlement struct {
	TurnID          string
	RunID           string
	ToolCallID      string
	ToolName        string
	Handle          string
	EffectAttemptID string
	Status          PendingToolSettlementStatus
	Summary         string
	Output          string
	Activity        *observation.ActivityPresentation
}

type ThreadSnapshot struct {
	ID               string          `json:"id"`
	Title            string          `json:"title,omitempty"`
	TitleStatus      string          `json:"title_status,omitempty"`
	TitleSource      string          `json:"title_source,omitempty"`
	TitleUpdatedAt   time.Time       `json:"title_updated_at,omitempty"`
	TitleError       string          `json:"title_error,omitempty"`
	TitleGeneration  int64           `json:"title_generation,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	Phase            string          `json:"phase"`
	Status           string          `json:"status"`
	LatestTurnID     string          `json:"latest_turn_id,omitempty"`
	LatestRunID      string          `json:"latest_run_id,omitempty"`
	ThroughOrdinal   int64           `json:"through_ordinal"`
	WaitingPrompt    string          `json:"waiting_prompt,omitempty"`
	Recoverable      bool            `json:"recoverable"`
	CanAppendMessage bool            `json:"can_append_message"`
	CanRetry         bool            `json:"can_retry"`
	Messages         []ThreadMessage `json:"messages"`
}

type ThreadSummary struct {
	ID               string    `json:"id"`
	Title            string    `json:"title,omitempty"`
	TitleStatus      string    `json:"title_status,omitempty"`
	TitleSource      string    `json:"title_source,omitempty"`
	TitleUpdatedAt   time.Time `json:"title_updated_at,omitempty"`
	TitleError       string    `json:"title_error,omitempty"`
	TitleGeneration  int64     `json:"title_generation,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	Phase            string    `json:"phase"`
	Status           string    `json:"status"`
	LatestTurnID     string    `json:"latest_turn_id,omitempty"`
	WaitingPrompt    string    `json:"waiting_prompt,omitempty"`
	Recoverable      bool      `json:"recoverable"`
	CanAppendMessage bool      `json:"can_append_message"`
	CanRetry         bool      `json:"can_retry"`
}

type ThreadOverview struct {
	Thread     ThreadSnapshot
	LatestTurn ThreadDetailEvents
}

type ThreadMessage struct {
	Role        session.Role                `json:"role"`
	Content     string                      `json:"content"`
	Attachments []session.MessageAttachment `json:"attachments,omitempty"`
	References  []session.MessageReference  `json:"references,omitempty"`
	TurnID      string                      `json:"turn_id,omitempty"`
	CreatedAt   time.Time                   `json:"created_at"`
}

type ThreadJournalSnapshot struct {
	Meta    sessiontree.ThreadMeta `json:"meta"`
	Path    []sessiontree.Entry    `json:"path"`
	Entries []sessiontree.Entry    `json:"entries"`
	Context []session.Message      `json:"context"`
	Phase   string                 `json:"phase"`
}

type TurnResult struct {
	ID                 string
	RunID              string
	Status             engine.Status
	Output             string
	Err                error
	FailureCode        string
	Diagnostics        map[string]string
	Metrics            engine.RunMetrics
	CompletionReason   engine.CompletionReason
	ContinuationReason engine.ContinuationReason
	FinishReason       provider.FinishReason
	RawFinishReason    string
	FinishInferred     bool
	ControlSignal      *engine.ControlSignal
	CanonicalEvents    []SubAgentDetailEvent
	Replayed           bool
	AdmissionRunning   bool
}

type CompactResult struct {
	RunID       string
	OperationID string
	RequestID   string
	Source      string
	Status      engine.Status
	Err         error
	Diagnostics map[string]string
	Metrics     engine.RunMetrics
	Entry       *sessiontree.Entry
	Replayed    bool
}

func (h *AgentHarness) providerState(ctx context.Context, threadID, leafEntryID string) (*provider.State, error) {
	if strings.TrimSpace(h.options.StateCompatibilityKey) == "" {
		return nil, nil
	}
	providerStates, ok := h.options.Repo.(sessiontree.ProviderStateReader)
	if !ok {
		return nil, errors.New("session tree repo does not support provider state authority")
	}
	record, err := providerStates.ProviderState(ctx, threadID)
	if errors.Is(err, sessiontree.ErrProviderStateNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(record.LeafEntryID) != strings.TrimSpace(leafEntryID) || strings.TrimSpace(record.CompatibilityKey) != strings.TrimSpace(h.options.StateCompatibilityKey) {
		return nil, nil
	}
	if strings.TrimSpace(record.State.Kind) == "" || strings.TrimSpace(record.State.ID) == "" {
		return nil, errors.New("stored provider state is incomplete")
	}
	return provider.CloneState(&record.State), nil
}

type ReadApprovalQueueOptions struct {
	ThreadID string
}

type ApprovalQueueSnapshot struct {
	RootThreadID      string           `json:"root_thread_id"`
	Generation        int64            `json:"generation"`
	Revision          int64            `json:"revision"`
	CurrentApprovalID string           `json:"current_approval_id,omitempty"`
	Approvals         []ApprovalRecord `json:"approvals"`
	GeneratedAt       time.Time        `json:"generated_at"`
}

type ApprovalResource struct {
	Kind  string `json:"kind,omitempty"`
	Value string `json:"value,omitempty"`
}

type ApprovalRecord struct {
	ApprovalID             string             `json:"approval_id,omitempty"`
	RootThreadID           string             `json:"root_thread_id,omitempty"`
	ParentThreadID         string             `json:"parent_thread_id,omitempty"`
	ToolCallID             string             `json:"tool_call_id,omitempty"`
	EffectAttemptID        string             `json:"effect_attempt_id,omitempty"`
	ToolName               string             `json:"tool_name,omitempty"`
	ToolKind               string             `json:"tool_kind,omitempty"`
	RunID                  string             `json:"run_id,omitempty"`
	ThreadID               string             `json:"thread_id,omitempty"`
	TurnID                 string             `json:"turn_id,omitempty"`
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

type Thread struct {
	harness          *AgentHarness
	id               string
	mu               sync.Mutex
	authorityMu      sync.RWMutex
	effectFinalizeMu sync.Mutex
	effectFinalizers map[string]func(context.Context, engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error)
	active           bool
	activeLease      sessiontree.TurnLease
	phase            string
}

func New(options Options) *AgentHarness {
	if options.Repo == nil {
		options.Repo = sessiontree.NewMemoryRepo()
	}
	if options.PromptStore == nil {
		options.PromptStore = cache.NewMemoryStore()
	}
	if options.Tools == nil {
		options.Tools = tools.NewRegistry()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &AgentHarness{
		options:                    options,
		effectOutcomeFingerprinter: effectOutcomeFingerprint,
		threads:                    map[string]*Thread{},
		subagents:                  map[string]*subagentController{},
		subagentUpdates:            make(chan struct{}),
	}
}

func (h *AgentHarness) ResumeThread(ctx context.Context, id string, _ ResumeOptions) (*Thread, error) {
	if inspector, ok := h.options.Repo.(sessiontree.ThreadAuthorityInspectionRepo); ok {
		if _, err := inspector.InspectThreadAuthority(ctx, id); err != nil {
			return nil, err
		}
	}
	meta, err := h.options.Repo.Thread(ctx, id)
	if err != nil {
		return nil, err
	}
	return h.threadForResume(meta.ID), nil
}

// BindCreatedRoot attaches the harness cache to a root already committed by
// the storage authority kernel. It never creates or repairs canonical state.
func (h *AgentHarness) BindCreatedRoot(meta sessiontree.ThreadMeta, replayed bool) (*Thread, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness is not configured")
	}
	if strings.TrimSpace(meta.ID) == "" || strings.TrimSpace(meta.ParentThreadID) != "" {
		return nil, sessiontree.ErrInvalidThreadAuthority
	}
	thread := h.cacheThread(meta.ID)
	if !replayed {
		h.emit(HarnessEvent{Type: EventThreadStarted, ThreadID: meta.ID})
	}
	return thread, nil
}

// OwnedActiveThread returns a cached thread only when its in-process owner and
// the durable turn lease are the same exact authority generation.
func (h *AgentHarness) OwnedActiveThread(ctx context.Context, id, turnID string) (*Thread, sessiontree.TurnLease, bool, error) {
	if h == nil {
		return nil, sessiontree.TurnLease{}, false, nil
	}
	id = strings.TrimSpace(id)
	turnID = strings.TrimSpace(turnID)
	if id == "" || turnID == "" {
		return nil, sessiontree.TurnLease{}, false, nil
	}
	h.mu.Lock()
	thread := h.threads[id]
	h.mu.Unlock()
	if thread == nil {
		return nil, sessiontree.TurnLease{}, false, nil
	}
	localLease, ok := thread.ownedActiveTurnLease(turnID)
	if !ok {
		return nil, sessiontree.TurnLease{}, false, nil
	}
	durableLease, active, err := h.activeTurnLease(ctx, id)
	if err != nil {
		return nil, sessiontree.TurnLease{}, false, err
	}
	if !active || !sameTurnLease(localLease, durableLease) {
		return nil, sessiontree.TurnLease{}, false, nil
	}
	return thread, localLease, true, nil
}

type RecoverInterruptedTurnOptions struct {
	ThreadID       string
	ParentThreadID string
	ExpectedLease  sessiontree.TurnLease
}

type RecoverInterruptedTurnResult struct {
	ThreadID string
	TurnID   string
	RunID    string
	Status   sessiontree.TurnMarkerStatus
	Replayed bool
	Terminal sessiontree.Entry
}

func (h *AgentHarness) RecoverInterruptedTurn(ctx context.Context, opts RecoverInterruptedTurnOptions) (RecoverInterruptedTurnResult, error) {
	if h == nil || h.options.Repo == nil {
		return RecoverInterruptedTurnResult{}, errors.New("agent harness is not configured")
	}
	threadID := strings.TrimSpace(opts.ThreadID)
	if threadID == "" || opts.ExpectedLease.ThreadID != threadID || opts.ExpectedLease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return RecoverInterruptedTurnResult{}, sessiontree.ErrInvalidThreadAuthority
	}
	repo, ok := h.options.Repo.(sessiontree.InterruptedTurnRecoveryRepo)
	if !ok {
		return RecoverInterruptedTurnResult{}, errors.New("session tree repo does not support atomic interrupted turn recovery")
	}
	result, err := repo.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{
		ExpectedLease: opts.ExpectedLease, ParentThreadID: strings.TrimSpace(opts.ParentThreadID),
		Now: h.now(),
	})
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	if !result.Replayed {
		for _, entry := range result.ToolResults {
			h.emitEntryCommitted(entry, result.RunID)
		}
		if result.Failure != nil {
			h.emitEntryCommitted(*result.Failure, result.RunID)
		}
		h.emitEntryCommitted(result.Terminal, result.RunID)
		h.emit(HarnessEvent{
			Type: EventTurnAborted, RunID: result.RunID, ThreadID: threadID, TurnID: opts.ExpectedLease.TurnID,
			Status: string(result.Status), Message: sessiontree.InterruptedTurnFailureMessage,
		})
	}
	return RecoverInterruptedTurnResult{
		ThreadID: threadID, TurnID: opts.ExpectedLease.TurnID, RunID: result.RunID,
		Status: result.Status, Replayed: result.Replayed, Terminal: result.Terminal,
	}, nil
}

func unfinishedTurns(path []sessiontree.Entry) []string {
	started := make(map[string]bool)
	terminal := make(map[string]bool)
	order := make([]string, 0)
	for _, entry := range path {
		if entry.Type != sessiontree.EntryTurnMarker || strings.TrimSpace(entry.TurnID) == "" {
			continue
		}
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.TurnStatus == sessiontree.TurnStarted && !started[turnID] {
			started[turnID] = true
			order = append(order, turnID)
		}
		if isTerminalTurnMarker(entry.TurnStatus) {
			terminal[turnID] = true
		}
	}
	out := make([]string, 0, len(order))
	for _, turnID := range order {
		if !terminal[turnID] {
			out = append(out, turnID)
		}
	}
	return out
}

func isTerminalTurnMarker(status sessiontree.TurnMarkerStatus) bool {
	switch status {
	case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
		return true
	default:
		return false
	}
}

func (h *AgentHarness) closeInterruptedTurnToolCalls(ctx context.Context, threadID, turnID string, path []sessiontree.Entry) error {
	calls := unresolvedToolCallsForTurn(path, turnID)
	if len(calls) == 0 {
		return nil
	}
	for _, call := range calls {
		if _, err := sessiontree.AppendMessage(ctx, h.options.Repo, threadID, turnID, interruptedTurnClosureToolResult(call.Message)); err != nil {
			return err
		}
	}
	_, err := sessiontree.AppendTurnMarker(ctx, h.options.Repo, threadID, turnID, sessiontree.TurnSavePoint, map[string]string{"reason": "interrupted_tool_result_batch"})
	return err
}

func unresolvedToolCallsForTurn(path []sessiontree.Entry, turnID string) []sessiontree.Entry {
	results := map[string]struct{}{}
	for _, entry := range path {
		if entry.TurnID != turnID || entry.Type != sessiontree.EntryToolResult || entry.Message.Role != session.Tool {
			continue
		}
		if callID := strings.TrimSpace(entry.Message.ToolCallID); callID != "" {
			results[callID] = struct{}{}
		}
	}
	var calls []sessiontree.Entry
	seen := map[string]struct{}{}
	for _, entry := range path {
		if entry.TurnID != turnID || entry.Type != sessiontree.EntryToolCall || entry.Message.Role != session.Assistant {
			continue
		}
		if entry.Message.Kind != session.MessageKindNormal {
			continue
		}
		callID := strings.TrimSpace(entry.Message.ToolCallID)
		if callID == "" {
			continue
		}
		if _, ok := results[callID]; ok {
			continue
		}
		if _, ok := seen[callID]; ok {
			continue
		}
		seen[callID] = struct{}{}
		calls = append(calls, entry)
	}
	return calls
}

func interruptedTurnClosureToolResult(call session.Message) session.Message {
	result := terminalTurnClosureToolResult(call, engine.Cancelled, nil)
	result.Content = "Tool call did not complete because the turn was interrupted."
	return result
}

func (h *AgentHarness) ForkThread(ctx context.Context, opts ForkOptions) (*Thread, error) {
	result, err := h.ForkThreadWithResult(ctx, opts)
	if err != nil {
		return nil, err
	}
	return result.Thread, nil
}

func (h *AgentHarness) ForkThreadWithResult(ctx context.Context, opts ForkOptions) (ForkResult, error) {
	if strings.TrimSpace(opts.OperationID) == "" {
		return ForkResult{}, errors.New("fork operation id is required")
	}
	return h.forkThreadReplayable(ctx, opts)
}

func rewriteForkContextEntry(entry sessiontree.Entry, identity sessiontree.ForkEntryIdentity) (sessiontree.Entry, error) {
	if entry.Type != sessiontree.EntryCustom {
		return entry, nil
	}
	switch entry.Metadata[subAgentDetailKindKey] {
	case subAgentContextStatusEntryKind:
		status, err := subAgentDetailContextStatus(entry.Metadata)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		status.ThreadID = identity.DestinationThreadID
		status.TurnID = rewriteForkContextID(status.TurnID, identity.TurnIDMap)
		status.RunID = rewriteForkContextID(status.RunID, identity.RunIDMap)
		entry.Metadata[subAgentContextStatusKey] = mustSubAgentMetadataJSON(status)
	case subAgentContextCompactionEntryKind:
		compact, err := subAgentDetailContextCompaction(entry.Metadata)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		compact.ThreadID = identity.DestinationThreadID
		compact.TurnID = rewriteForkContextID(compact.TurnID, identity.TurnIDMap)
		compact.RunID = rewriteForkContextID(compact.RunID, identity.RunIDMap)
		entry.Metadata[subAgentContextCompactionKey] = mustSubAgentMetadataJSON(compact)
	}
	return entry, nil
}

func rewriteForkContextID(value string, rewrites map[string]string) string {
	value = strings.TrimSpace(value)
	if next := strings.TrimSpace(rewrites[value]); next != "" {
		return next
	}
	return value
}

func (h *AgentHarness) forkSourcePath(ctx context.Context, opts ForkOptions) ([]sessiontree.Entry, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness repo is required")
	}
	position := opts.Position
	if position == "" {
		position = sessiontree.ForkAt
	}
	sourceMeta, err := h.options.Repo.Thread(ctx, opts.SourceThreadID)
	if err != nil {
		return nil, err
	}
	targetID := strings.TrimSpace(opts.EntryID)
	if targetID == "" {
		targetID = sourceMeta.LeafID
	}
	if position == sessiontree.ForkBefore {
		entry, err := h.options.Repo.Entry(ctx, opts.SourceThreadID, targetID)
		if err != nil {
			return nil, err
		}
		targetID = entry.ParentID
	}
	return h.options.Repo.Path(ctx, opts.SourceThreadID, targetID)
}

func forkMappedID(value string, ids map[string]string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(ids) == 0 {
		return value
	}
	if next := strings.TrimSpace(ids[value]); next != "" {
		return next
	}
	return value
}

func (h *AgentHarness) cacheThread(id string) *Thread {
	h.mu.Lock()
	defer h.mu.Unlock()
	if thread, ok := h.threads[id]; ok {
		return thread
	}
	thread := &Thread{harness: h, id: id, phase: threadPhaseIdle}
	h.threads[id] = thread
	return thread
}

func (h *AgentHarness) threadForResume(id string) *Thread {
	h.mu.Lock()
	defer h.mu.Unlock()
	if thread, ok := h.threads[id]; ok {
		return thread
	}
	return &Thread{harness: h, id: id, phase: threadPhaseIdle}
}

func (h *AgentHarness) cacheThreadAfterCommit(thread *Thread) {
	if h == nil || thread == nil {
		return
	}
	h.mu.Lock()
	h.threads[thread.id] = thread
	h.mu.Unlock()
}

func (h *AgentHarness) nextID(prefix string) string {
	if h.options.NewID != nil {
		return h.options.NewID(prefix)
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		panic(fmt.Sprintf("generate agent harness identity: %v", err))
	}
	return strings.TrimSpace(prefix) + "-" + hex.EncodeToString(entropy[:])
}

func (h *AgentHarness) now() time.Time {
	if h.options.Now == nil {
		return time.Now()
	}
	return h.options.Now()
}

func (h *AgentHarness) emit(ev HarnessEvent) {
	ev.Timestamp = h.now()
	if h.options.HarnessSink != nil {
		h.options.HarnessSink.EmitHarness(ev)
	}
	if h.options.Sink != nil {
		var eventType event.Type
		switch ev.Type {
		case EventTitlePending:
			eventType = event.ThreadTitlePending
		case EventTitleUpdated:
			eventType = event.ThreadTitleUpdated
		case EventTitleFailed:
			eventType = event.ThreadTitleFailed
		}
		if eventType != "" {
			h.options.Sink.Emit(event.Event{
				Type:      eventType,
				RunID:     ev.RunID,
				ThreadID:  ev.ThreadID,
				TurnID:    ev.TurnID,
				Message:   ev.Message,
				Metadata:  cloneStringMap(ev.Metadata),
				Timestamp: ev.Timestamp,
			})
		}
	}
}

func (h *AgentHarness) emitEntryCommitted(entry sessiontree.Entry, runID string) {
	if h == nil || h.options.Sink == nil || strings.TrimSpace(entry.ID) == "" {
		return
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		runID = strings.TrimSpace(entry.Metadata["run_id"])
	}
	ordinal := h.threadEntryOrdinal(entry)
	detail, ok := h.subAgentDetailEvent(entry, ordinal, false, subAgentDetailActivityContext{})
	if !ok {
		return
	}
	if detail.Kind == SubAgentDetailEventAssistantMessage && detail.Message != nil {
		detail.Message.Content = entry.Message.Content
		detail.Message.Reasoning = entry.Message.Reasoning
		if detail.Metadata != nil {
			delete(detail.Metadata, subAgentDetailRawOmitted)
		}
	}
	metadata := map[string]any{
		"entry_id":   entry.ID,
		"parent_id":  entry.ParentID,
		"entry_type": string(entry.Type),
		"created_at": entry.CreatedAt.Format(time.RFC3339Nano),
	}
	if ordinal > 0 {
		metadata["ordinal"] = ordinal
	}
	if entry.TurnStatus != "" {
		metadata["turn_status"] = string(entry.TurnStatus)
	}
	h.options.Sink.Emit(event.SanitizeWithPolicy(event.Event{
		Type:      event.ThreadEntryCommitted,
		RunID:     runID,
		ThreadID:  entry.ThreadID,
		TurnID:    entry.TurnID,
		Message:   entry.Message.Content,
		ToolID:    entry.Message.ToolCallID,
		ToolName:  entry.Message.ToolName,
		Args:      entry.Message.ToolArgs,
		Result:    entry.Message.Content,
		Err:       entry.Error,
		Metadata:  metadata,
		Payload:   detail,
		Timestamp: entry.CreatedAt,
	}, h.options.SinkPolicy))
}

func (h *AgentHarness) threadEntryOrdinal(entry sessiontree.Entry) int64 {
	if h == nil || h.options.Repo == nil || strings.TrimSpace(entry.ThreadID) == "" || strings.TrimSpace(entry.ID) == "" {
		return 0
	}
	stored, err := h.options.Repo.Entry(context.Background(), entry.ThreadID, entry.ID)
	if err != nil {
		return 0
	}
	if stored.ID != entry.ID || stored.ThreadID != entry.ThreadID || stored.PathDepth <= 0 {
		return 0
	}
	return stored.PathDepth
}

func (h *AgentHarness) activeTurnLease(ctx context.Context, threadID string) (sessiontree.TurnLease, bool, error) {
	repo, ok := h.options.Repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		return sessiontree.TurnLease{}, false, nil
	}
	return repo.ActiveTurnLease(ctx, threadID)
}

func (h *AgentHarness) admitTurn(ctx context.Context, req sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error) {
	repo, ok := h.options.Repo.(sessiontree.TurnAuthorityRepo)
	if !ok {
		return sessiontree.AdmitTurnResult{}, errors.New("session tree repo does not support atomic turn admission")
	}
	fingerprint, err := sessiontree.TurnAdmissionRequestFingerprint(req)
	if err != nil {
		return sessiontree.AdmitTurnResult{}, err
	}
	req.RequestFingerprint = fingerprint
	req.Now = h.now()
	result, err := repo.AdmitTurn(ctx, req)
	if errors.Is(err, sessiontree.ErrActiveTurn) || errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		return sessiontree.AdmitTurnResult{}, ErrActiveTurn
	}
	if errors.Is(err, sessiontree.ErrThreadClosed) || errors.Is(err, sessiontree.ErrSubAgentClosing) {
		return sessiontree.AdmitTurnResult{}, ErrSubAgentClosed
	}
	return result, err
}

func (t *Thread) startLeaseRenewal(ctx context.Context, initial sessiontree.TurnLease) (context.Context, func() error, error) {
	repo, ok := t.harness.options.Repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		return nil, nil, errors.New("session tree repo does not support authority lease renewal")
	}
	policyRepo, ok := t.harness.options.Repo.(sessiontree.LeasePolicyRepo)
	if !ok {
		return nil, nil, errors.New("session tree repo does not expose its authority lease policy")
	}
	policy := policyRepo.AuthorityLeasePolicy()
	if err := policy.Validate(); err != nil {
		return nil, nil, err
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	renewalCtx, cancelRenewal := context.WithCancel(context.WithoutCancel(ctx))
	stop := make(chan struct{})
	done := make(chan struct{})
	var renewalMu sync.Mutex
	var renewalErr error
	go func() {
		defer close(done)
		ticker := time.NewTicker(policy.RenewInterval)
		defer ticker.Stop()
		proof := initial
		for {
			select {
			case <-stop:
				return
			case <-renewalCtx.Done():
				return
			case <-ticker.C:
				t.authorityMu.RLock()
				if !t.hasActiveLease(proof) {
					t.authorityMu.RUnlock()
					return
				}
				renewed, err := repo.RenewTurnLease(renewalCtx, proof)
				if err == nil {
					err = sessiontree.UpdateTurnLeaseContext(runCtx, proof, renewed)
				}
				if err == nil {
					err = t.replaceActiveLease(proof, renewed)
				}
				if err != nil {
					t.authorityMu.RUnlock()
					renewalMu.Lock()
					renewalErr = err
					renewalMu.Unlock()
					cancelRun()
					return
				}
				proof = renewed
				t.authorityMu.RUnlock()
			}
		}
	}()
	var stopOnce sync.Once
	stopRenewal := func() error {
		stopOnce.Do(func() {
			close(stop)
			cancelRenewal()
		})
		<-done
		cancelRun()
		renewalMu.Lock()
		defer renewalMu.Unlock()
		return renewalErr
	}
	return runCtx, stopRenewal, nil
}

func (t *Thread) ID() string {
	return t.id
}

func (t *Thread) Read(ctx context.Context) (ThreadSnapshot, error) {
	journal, err := t.Journal(ctx)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return threadSnapshotFromJournal(journal), nil
}

func threadSnapshotFromJournal(journal ThreadJournalSnapshot) ThreadSnapshot {
	lifecycle := sessionlifecycle.Derive(journal.Path, journal.Phase)
	return ThreadSnapshot{
		ID:               journal.Meta.ID,
		Title:            journal.Meta.Title,
		TitleStatus:      string(journal.Meta.TitleStatus),
		TitleSource:      string(journal.Meta.TitleSource),
		TitleUpdatedAt:   journal.Meta.TitleUpdatedAt,
		TitleError:       journal.Meta.TitleError,
		TitleGeneration:  journal.Meta.TitleGeneration,
		CreatedAt:        journal.Meta.CreatedAt,
		UpdatedAt:        journal.Meta.UpdatedAt,
		Phase:            lifecycle.Phase(),
		Status:           lifecycle.Status(),
		LatestTurnID:     lifecycle.LatestTurnID(),
		LatestRunID:      latestThreadRunID(journal.Path),
		ThroughOrdinal:   int64(len(journal.Path)),
		WaitingPrompt:    lifecycle.WaitingPrompt(),
		Recoverable:      lifecycle.Recoverable(),
		CanAppendMessage: lifecycle.CanAppendMessage(),
		CanRetry:         retryTarget(journal.Path).Entry.ID != "",
		Messages:         threadMessages(journal.Path),
	}
}

func (h *AgentHarness) ReadThread(ctx context.Context, id string) (ThreadSnapshot, error) {
	if h == nil {
		return ThreadSnapshot{}, errors.New("agent harness is nil")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ThreadSnapshot{}, errors.New("thread id is required")
	}
	return h.cacheThread(id).Read(ctx)
}

func (h *AgentHarness) SetThreadTitle(ctx context.Context, id, rawTitle string) (ThreadSnapshot, error) {
	if h == nil {
		return ThreadSnapshot{}, errors.New("agent harness is nil")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ThreadSnapshot{}, errors.New("thread id is required")
	}
	title, err := normalizeHostThreadTitle(rawTitle)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	authority, ok := h.options.Repo.(sessiontree.ThreadTitleAuthorityRepo)
	if !ok {
		return ThreadSnapshot{}, errors.New("session tree repo does not support atomic thread title mutation")
	}
	result, err := authority.SetThreadTitle(ctx, sessiontree.SetThreadTitleRequest{
		ThreadID: id, Title: title, Now: h.now(),
	})
	if err != nil {
		return ThreadSnapshot{}, err
	}
	if result.Changed {
		h.emit(HarnessEvent{Type: EventTitleUpdated, ThreadID: id, Message: title, Metadata: map[string]string{"source": string(sessiontree.ThreadTitleSourceHost)}})
	}
	return h.ReadThread(ctx, id)
}

func (h *AgentHarness) ReadThreadOverview(ctx context.Context, id string) (ThreadOverview, error) {
	if h == nil {
		return ThreadOverview{}, errors.New("agent harness is nil")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ThreadOverview{}, errors.New("thread id is required")
	}
	journal, err := h.cacheThread(id).Journal(ctx)
	if err != nil {
		return ThreadOverview{}, err
	}
	latest, err := h.latestThreadDetailEventsFromPath(journal.Path, true)
	if err != nil {
		return ThreadOverview{}, err
	}
	return ThreadOverview{Thread: threadSnapshotFromJournal(journal), LatestTurn: latest}, nil
}

func latestThreadRunID(path []sessiontree.Entry) string {
	for index := len(path) - 1; index >= 0; index-- {
		entry := path[index]
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			return strings.TrimSpace(entry.Metadata["run_id"])
		}
	}
	return ""
}

func (t *Thread) Summary(ctx context.Context) (ThreadSummary, error) {
	meta, err := t.harness.options.Repo.Thread(ctx, t.id)
	if err != nil {
		return ThreadSummary{}, err
	}
	path, err := t.harness.options.Repo.Path(ctx, t.id, meta.LeafID)
	if err != nil {
		return ThreadSummary{}, err
	}
	t.mu.Lock()
	phase := t.phase
	t.mu.Unlock()
	phase, err = t.canonicalThreadPhase(ctx, phase)
	if err != nil {
		return ThreadSummary{}, err
	}
	lifecycle := sessionlifecycle.Derive(path, phase)
	return ThreadSummary{
		ID:               meta.ID,
		Title:            meta.Title,
		TitleStatus:      string(meta.TitleStatus),
		TitleSource:      string(meta.TitleSource),
		TitleUpdatedAt:   meta.TitleUpdatedAt,
		TitleError:       meta.TitleError,
		TitleGeneration:  meta.TitleGeneration,
		CreatedAt:        meta.CreatedAt,
		UpdatedAt:        meta.UpdatedAt,
		Phase:            lifecycle.Phase(),
		Status:           lifecycle.Status(),
		LatestTurnID:     lifecycle.LatestTurnID(),
		WaitingPrompt:    lifecycle.WaitingPrompt(),
		Recoverable:      lifecycle.Recoverable(),
		CanAppendMessage: lifecycle.CanAppendMessage(),
		CanRetry:         retryTarget(path).Entry.ID != "",
	}, nil
}

func (t *Thread) Journal(ctx context.Context) (ThreadJournalSnapshot, error) {
	meta, err := t.harness.options.Repo.Thread(ctx, t.id)
	if err != nil {
		return ThreadJournalSnapshot{}, err
	}
	path, err := t.harness.options.Repo.Path(ctx, t.id, meta.LeafID)
	if err != nil {
		return ThreadJournalSnapshot{}, err
	}
	entries, err := t.harness.options.Repo.Entries(ctx, t.id)
	if err != nil {
		return ThreadJournalSnapshot{}, err
	}
	t.mu.Lock()
	phase := t.phase
	t.mu.Unlock()
	phase, err = t.canonicalThreadPhase(ctx, phase)
	if err != nil {
		return ThreadJournalSnapshot{}, err
	}
	contextMessages, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
	if err != nil {
		return ThreadJournalSnapshot{}, err
	}
	return ThreadJournalSnapshot{
		Meta:    meta,
		Path:    path,
		Entries: entries,
		Context: contextMessages,
		Phase:   phase,
	}, nil
}

func (t *Thread) canonicalThreadPhase(ctx context.Context, localPhase string) (string, error) {
	registry := t.harness.options.TurnExecutions
	if registry == nil || !registry.validate() {
		return localPhase, nil
	}
	inspector, ok := t.harness.options.Repo.(sessiontree.ThreadAuthorityInspectionRepo)
	if !ok {
		return localPhase, nil
	}
	snapshot, err := inspector.InspectThreadAuthority(ctx, t.id)
	if err != nil {
		return "", err
	}
	if snapshot.Lease != nil && snapshot.Lease.Purpose == sessiontree.TurnLeasePurposeTurn &&
		snapshot.Lease.Fresh(t.harness.now().UTC()) && snapshot.ClaimOperationID == "" {
		if local, ok := t.ownedActiveTurnLease(snapshot.Lease.TurnID); ok && sessiontree.SameTurnLease(local, *snapshot.Lease) {
			return threadPhaseTurn, nil
		}
		if active, ok := registry.Active(t.id); ok && sessiontree.SameTurnLease(active, *snapshot.Lease) {
			return threadPhaseTurn, nil
		}
	}
	if localPhase == threadPhaseTurn {
		return threadPhaseIdle, nil
	}
	return localPhase, nil
}

func threadMessages(path []sessiontree.Entry) []ThreadMessage {
	out := make([]ThreadMessage, 0)
	for _, entry := range path {
		switch entry.Type {
		case sessiontree.EntryUserMessage, sessiontree.EntryAssistantMessage:
			if entry.Message.Content == "" && len(entry.Message.Attachments) == 0 && len(entry.Message.References) == 0 {
				continue
			}
			out = append(out, ThreadMessage{
				Role:        entry.Message.Role,
				Content:     entry.Message.Content,
				Attachments: append([]session.MessageAttachment(nil), entry.Message.Attachments...),
				References:  append([]session.MessageReference(nil), entry.Message.References...),
				TurnID:      entry.TurnID,
				CreatedAt:   entry.CreatedAt,
			})
		}
	}
	return out
}

func (t *Thread) Run(ctx context.Context, input string, opts RunOptions) (TurnResult, error) {
	return t.run(ctx, input, opts, nil)
}

func (t *Thread) Retry(ctx context.Context, opts RetryOptions) (TurnResult, error) {
	if err := t.enterTurn(); err != nil {
		return TurnResult{}, err
	}
	defer t.leaveTurn()
	snap, err := t.Journal(ctx)
	if err != nil {
		return TurnResult{}, err
	}
	target := retryTarget(snap.Path)
	if target.Entry.ID == "" {
		return TurnResult{}, ErrNoRetryTarget
	}
	turnID := t.harness.nextID("turn")
	runID := t.harness.nextID("run")
	admission, err := t.harness.admitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: t.id, TurnID: turnID, RunID: runID, OwnerID: t.harness.nextID("lease"),
		RetrySourceTurnID: target.Entry.TurnID, RetrySourceEntryID: target.Entry.ID,
	})
	if err != nil {
		return TurnResult{}, err
	}
	t.harness.cacheThreadAfterCommit(t)
	lease := admission.Lease
	if err := t.bindActiveLease(lease); err != nil {
		return t.finalizeTurnStartupFailure(ctx, lease, turnID, runID, "local_owner_bind_error", err)
	}
	ctx = sessiontree.ContextWithTurnLease(ctx, lease)
	runCtx, stopRenewal, err := t.startLeaseRenewal(ctx, lease)
	if err != nil {
		return t.finalizeTurnStartupFailure(ctx, lease, turnID, runID, "lease_renewal_start_error", err)
	}
	defer func() {
		if current, ok := sessiontree.TurnLeaseFromContext(ctx); ok {
			t.clearActiveLease(current)
		}
	}()
	if admission.BoundaryTerminal.ID != "" {
		t.harness.emitEntryCommitted(admission.BoundaryTerminal, runID)
	}
	t.harness.emitEntryCommitted(admission.TurnStarted, runID)
	t.harness.emit(HarnessEvent{Type: EventTurnStarted, RunID: runID, ThreadID: t.id, TurnID: turnID})
	t.harness.emit(HarnessEvent{Type: EventRetryStarted, ThreadID: t.id, EntryID: target.Entry.ID, Metadata: map[string]string{"reason": opts.Reason, "source": target.Source}})
	result, runErr := t.runLeased(runCtx, "", RunOptions{
		RunID: runID, TurnID: turnID, Labels: opts.Labels,
		AdmissionCommitted: true, AdmissionBaseLeafID: admission.BaseLeafID,
	}, &target.Entry)
	if renewalErr := stopRenewal(); renewalErr != nil && runErr == nil {
		return result, renewalErr
	}
	return result, runErr
}

func (t *Thread) CompletePendingTool(ctx context.Context, completion PendingToolCompletion) (TurnResult, error) {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return TurnResult{}, errors.New("thread is not initialized")
	}
	completion.CompletionRequestID = strings.TrimSpace(completion.CompletionRequestID)
	completion.ContinuationTurnID = strings.TrimSpace(completion.ContinuationTurnID)
	completion.ContinuationRunID = strings.TrimSpace(completion.ContinuationRunID)
	if completion.CompletionRequestID == "" || completion.ContinuationTurnID == "" || completion.ContinuationRunID == "" {
		return TurnResult{}, errors.New("pending tool completion requires completion request, continuation turn, and continuation run identities")
	}
	if strings.TrimSpace(completion.Target.ThreadID) != t.id {
		return TurnResult{}, errors.New("pending tool completion target thread identity mismatch")
	}
	if completion.Input.Role != session.User || (strings.TrimSpace(completion.Input.Content) == "" && len(completion.Input.Attachments) == 0) {
		return TurnResult{}, errors.New("pending tool completion requires structured user continuation input")
	}
	settlement, err := normalizePendingToolSettlement(PendingToolSettlement{
		TurnID: completion.Target.TurnID, RunID: completion.Target.RunID,
		ToolCallID: completion.Target.ToolCallID, ToolName: completion.Target.ToolName, Handle: completion.Target.Handle,
		EffectAttemptID: completion.Target.EffectAttemptID,
		Status:          pendingToolSettlementStatusFromCompletion(completion.Status), Summary: completion.Summary, Output: completion.Output,
	})
	if err != nil {
		return TurnResult{}, err
	}
	settlementEntry, settlementFingerprint, err := pendingToolSettlementAuthorityEntry(t.id, settlement)
	if err != nil {
		return TurnResult{}, err
	}
	fingerprintPayload, err := json.Marshal(struct {
		CompletionRequestID string                                  `json:"completion_request_id"`
		Target              sessiontree.PendingToolSettlementTarget `json:"target"`
		ContinuationTurnID  string                                  `json:"continuation_turn_id"`
		ContinuationRunID   string                                  `json:"continuation_run_id"`
		Status              PendingToolCompletionStatus             `json:"status"`
		Summary             string                                  `json:"summary"`
		Output              string                                  `json:"output"`
		Input               session.Message                         `json:"input"`
		Labels              engine.RunLabels                        `json:"labels"`
	}{
		CompletionRequestID: completion.CompletionRequestID, Target: completion.Target,
		ContinuationTurnID: completion.ContinuationTurnID, ContinuationRunID: completion.ContinuationRunID,
		Status: completion.Status, Summary: settlement.Summary, Output: settlement.Output,
		Input: session.CloneMessage(completion.Input), Labels: completion.Labels,
	})
	if err != nil {
		return TurnResult{}, err
	}
	repo, ok := t.harness.options.Repo.(sessiontree.PendingToolCompletionAuthorityRepo)
	if !ok {
		return TurnResult{}, errors.New("session tree repo does not support atomic pending tool completion")
	}
	request := sessiontree.AdmitPendingToolCompletionRequest{
		CompletionRequestID: completion.CompletionRequestID,
		RequestFingerprint:  sessiontree.StableHash(string(fingerprintPayload)), SettlementFingerprint: settlementFingerprint,
		Target: completion.Target, Settlement: settlementEntry,
		ContinuationTurnID: completion.ContinuationTurnID, ContinuationRunID: completion.ContinuationRunID,
		OwnerID: t.harness.nextID("lease"), Input: session.CloneMessage(completion.Input), Now: t.harness.now(),
	}

	if err := t.enterTurn(); err != nil {
		replayed, found, readErr := repo.ReadPendingToolCompletion(ctx, request)
		if readErr != nil {
			return TurnResult{}, t.pendingToolCompletionError(readErr)
		}
		if !found {
			return TurnResult{}, err
		}
		if !replayed.Replayed {
			return TurnResult{}, sessiontree.ErrAuthorityCorrupt
		}
		return t.turnAdmissionReplayResult(ctx, replayed.Admission, completion.ContinuationTurnID, completion.ContinuationRunID)
	}
	defer t.leaveTurn()
	admitted, err := repo.AdmitPendingToolCompletion(ctx, request)
	if err != nil {
		return TurnResult{}, t.pendingToolCompletionError(err)
	}
	t.harness.cacheThreadAfterCommit(t)
	if admitted.Replayed {
		return t.turnAdmissionReplayResult(ctx, admitted.Admission, completion.ContinuationTurnID, completion.ContinuationRunID)
	}
	lease := admitted.Admission.Lease
	if err := t.bindActiveLease(lease); err != nil {
		return t.finalizeTurnStartupFailure(ctx, lease, completion.ContinuationTurnID, completion.ContinuationRunID, "local_owner_bind_error", err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, lease)
	runCtx, stopRenewal, err := t.startLeaseRenewal(leaseCtx, lease)
	if err != nil {
		return t.finalizeTurnStartupFailure(leaseCtx, lease, completion.ContinuationTurnID, completion.ContinuationRunID, "lease_renewal_start_error", err)
	}
	defer func() {
		if current, ok := sessiontree.TurnLeaseFromContext(leaseCtx); ok {
			t.clearActiveLease(current)
		}
	}()
	if !admitted.SettlementReplayed {
		t.harness.emitEntryCommitted(admitted.Settlement, completion.Target.RunID)
		t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: completion.Target.RunID, ThreadID: t.id,
			TurnID: completion.Target.TurnID, EntryID: admitted.Settlement.ID, ParentID: admitted.Settlement.ParentID, Message: pendingToolSettlementEntryKind})
	}
	t.harness.emitEntryCommitted(admitted.Admission.TurnStarted, completion.ContinuationRunID)
	t.harness.emitEntryCommitted(admitted.Admission.UserMessage, completion.ContinuationRunID)
	t.harness.emit(HarnessEvent{Type: EventTurnStarted, RunID: completion.ContinuationRunID, ThreadID: t.id, TurnID: completion.ContinuationTurnID})
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: completion.ContinuationRunID, ThreadID: t.id,
		TurnID: completion.ContinuationTurnID, EntryID: admitted.Admission.UserMessage.ID, ParentID: admitted.Admission.UserMessage.ParentID})
	result, runErr := t.runLeased(runCtx, completion.Input.Content, RunOptions{
		RunID: completion.ContinuationRunID, TurnID: completion.ContinuationTurnID, Labels: completion.Labels,
		Attachments:        append([]session.MessageAttachment(nil), completion.Input.Attachments...),
		References:         append([]session.MessageReference(nil), completion.Input.References...),
		AdmissionCommitted: true, AdmissionBaseLeafID: admitted.Admission.BaseLeafID,
	}, nil)
	if renewalErr := stopRenewal(); renewalErr != nil && runErr == nil {
		return result, renewalErr
	}
	return result, runErr
}

func pendingToolSettlementStatusFromCompletion(status PendingToolCompletionStatus) PendingToolSettlementStatus {
	switch status {
	case PendingToolCompleted:
		return PendingToolSettledCompleted
	case PendingToolFailed:
		return PendingToolSettledFailed
	case PendingToolCanceled:
		return PendingToolSettledCanceled
	default:
		return PendingToolSettlementStatus(status)
	}
}

func (t *Thread) pendingToolCompletionError(err error) error {
	switch {
	case errors.Is(err, sessiontree.ErrThreadAuthorityBusy), errors.Is(err, sessiontree.ErrActiveTurn):
		return ErrActiveTurn
	case errors.Is(err, sessiontree.ErrPendingToolTurnNotFound):
		return ErrPendingToolSettlementTargetTurnNotFound
	case errors.Is(err, sessiontree.ErrPendingToolRunNotFound):
		return ErrPendingToolSettlementTargetRunNotFound
	case errors.Is(err, sessiontree.ErrPendingToolNotFound):
		return ErrPendingToolSettlementTargetToolNotFound
	case errors.Is(err, sessiontree.ErrPendingToolNotPending):
		return ErrPendingToolSettlementTargetNotActive
	case errors.Is(err, sessiontree.ErrRequestConflict):
		return ErrPendingToolSettlementConflict
	default:
		return err
	}
}

func (t *Thread) turnAdmissionReplayResult(ctx context.Context, admission sessiontree.AdmitTurnResult, turnID, runID string) (TurnResult, error) {
	result := TurnResult{ID: turnID, RunID: runID, Replayed: true, AdmissionRunning: admission.Terminal == nil}
	if admission.Terminal == nil {
		return result, nil
	}
	if err := validateTurnTerminalOutcome(t.id, turnID, runID, admission.Terminal); err != nil {
		return TurnResult{}, fmt.Errorf("%w: %v", sessiontree.ErrAuthorityCorrupt, err)
	}
	canonical, ok := t.harness.options.Repo.(sessiontree.CanonicalTurnRepo)
	if !ok {
		return TurnResult{}, errors.New("session tree repo does not support canonical turn reads")
	}
	entries, found, err := canonical.CanonicalTurnEntries(ctx, t.id, turnID, runID)
	if err != nil {
		return TurnResult{}, err
	}
	if !found {
		return TurnResult{}, sessiontree.ErrAuthorityCorrupt
	}
	result.CanonicalEvents = t.harness.detailEventsForCanonicalEntries(entries, true)
	var output strings.Builder
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryAssistantMessage && strings.TrimSpace(entry.Message.Content) != "" {
			output.WriteString(entry.Message.Content)
		}
	}
	result.Output = output.String()
	switch admission.Terminal.Terminal.TurnStatus {
	case sessiontree.TurnCompleted:
		result.Status = engine.Completed
	case sessiontree.TurnWaiting:
		result.Status = engine.Waiting
	case sessiontree.TurnFailed:
		result.Status = engine.Failed
	case sessiontree.TurnAborted:
		result.Status = engine.Cancelled
	default:
		return TurnResult{}, sessiontree.ErrAuthorityCorrupt
	}
	if admission.Terminal.Failure != nil {
		if strings.TrimSpace(admission.Terminal.Failure.Error) == "" {
			return TurnResult{}, sessiontree.ErrAuthorityCorrupt
		}
		result.Err = errors.New(admission.Terminal.Failure.Error)
	}
	result.FailureCode = strings.TrimSpace(admission.Terminal.Terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey])
	return result, result.Err
}

func (t *Thread) SettlePendingTool(ctx context.Context, settlement PendingToolSettlement) (SubAgentDetailEvent, error) {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return SubAgentDetailEvent{}, errors.New("thread is not initialized")
	}
	normalized, err := normalizePendingToolSettlement(settlement)
	if err != nil {
		return SubAgentDetailEvent{}, err
	}
	return t.settlePendingToolRecovery(ctx, normalized)
}

func (t *Thread) settlePendingToolRecovery(ctx context.Context, settlement PendingToolSettlement) (SubAgentDetailEvent, error) {
	repo, ok := t.harness.options.Repo.(sessiontree.PendingToolRecoveryRepo)
	if !ok {
		return SubAgentDetailEvent{}, errors.New("session tree repo does not support atomic pending tool recovery")
	}
	entry, fingerprint, err := pendingToolSettlementAuthorityEntry(t.id, settlement)
	if err != nil {
		return SubAgentDetailEvent{}, err
	}
	result, err := repo.SettlePendingToolRecovery(ctx, sessiontree.SettlePendingToolRecoveryRequest{
		Target: sessiontree.PendingToolSettlementTarget{
			ThreadID: t.id, TurnID: settlement.TurnID, RunID: settlement.RunID,
			ToolCallID: settlement.ToolCallID, ToolName: settlement.ToolName, Handle: settlement.Handle,
			EffectAttemptID: settlement.EffectAttemptID,
		},
		RequestFingerprint: fingerprint, Settlement: entry, Now: t.harness.now(),
	})
	if err != nil {
		switch {
		case errors.Is(err, sessiontree.ErrThreadAuthorityBusy), errors.Is(err, sessiontree.ErrActiveTurn):
			return SubAgentDetailEvent{}, ErrActiveTurn
		case errors.Is(err, sessiontree.ErrPendingToolTurnNotFound):
			return SubAgentDetailEvent{}, ErrPendingToolSettlementTargetTurnNotFound
		case errors.Is(err, sessiontree.ErrPendingToolRunNotFound):
			return SubAgentDetailEvent{}, ErrPendingToolSettlementTargetRunNotFound
		case errors.Is(err, sessiontree.ErrPendingToolNotFound):
			return SubAgentDetailEvent{}, ErrPendingToolSettlementTargetToolNotFound
		case errors.Is(err, sessiontree.ErrPendingToolNotPending):
			return SubAgentDetailEvent{}, ErrPendingToolSettlementTargetNotActive
		case errors.Is(err, sessiontree.ErrRequestConflict):
			return SubAgentDetailEvent{}, ErrPendingToolSettlementConflict
		default:
			return SubAgentDetailEvent{}, err
		}
	}
	t.harness.cacheThreadAfterCommit(t)
	if !result.Replayed {
		t.harness.emitEntryCommitted(result.Entry, settlement.RunID)
		t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: settlement.RunID, ThreadID: t.id, TurnID: settlement.TurnID, EntryID: result.Entry.ID, ParentID: result.Entry.ParentID, Message: pendingToolSettlementEntryKind})
	}
	journal, err := t.Journal(ctx)
	if err != nil {
		return SubAgentDetailEvent{}, err
	}
	activityContext := subAgentDetailActivityContext{resultCallIDs: subAgentDetailResultCallIDs(journal.Path)}
	event, ok := t.harness.subAgentDetailEvent(result.Entry, t.harness.threadEntryOrdinal(result.Entry), true, activityContext)
	if !ok {
		return SubAgentDetailEvent{}, errors.New("pending tool settlement did not project")
	}
	return event, nil
}

func (t *Thread) SettlePendingToolActive(ctx context.Context, settlement PendingToolSettlement, lease sessiontree.TurnLease) (SubAgentDetailEvent, error) {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return SubAgentDetailEvent{}, errors.New("thread is not initialized")
	}
	normalized, err := normalizePendingToolSettlement(settlement)
	if err != nil {
		return SubAgentDetailEvent{}, err
	}
	if lease.ThreadID != t.id || lease.TurnID != normalized.TurnID || strings.TrimSpace(lease.OwnerID) == "" {
		return SubAgentDetailEvent{}, ErrActiveTurn
	}
	localLease, ok := t.ownedActiveTurnLease(normalized.TurnID)
	if !ok || !sameTurnLease(localLease, lease) {
		return SubAgentDetailEvent{}, ErrActiveTurn
	}
	return t.settlePendingTool(sessiontree.ContextWithTurnLease(ctx, lease), normalized)
}

func (t *Thread) settlePendingTool(ctx context.Context, normalized PendingToolSettlement) (SubAgentDetailEvent, error) {
	journal, err := t.Journal(ctx)
	if err != nil {
		return SubAgentDetailEvent{}, err
	}
	if existing, ok, err := pendingToolSettlementTarget(journal.Path, normalized); err != nil {
		return SubAgentDetailEvent{}, err
	} else if ok {
		activityContext := subAgentDetailActivityContext{resultCallIDs: subAgentDetailResultCallIDs(journal.Path)}
		event, ok := t.harness.subAgentDetailEvent(existing, t.harness.threadEntryOrdinal(existing), true, activityContext)
		if !ok {
			return SubAgentDetailEvent{}, errors.New("pending tool settlement did not project")
		}
		return event, nil
	}
	entry, err := t.appendPendingToolSettlement(ctx, normalized)
	if err != nil {
		return SubAgentDetailEvent{}, err
	}
	activityContext := subAgentDetailActivityContext{resultCallIDs: subAgentDetailResultCallIDs(append(journal.Path, entry))}
	event, ok := t.harness.subAgentDetailEvent(entry, t.harness.threadEntryOrdinal(entry), true, activityContext)
	if !ok {
		return SubAgentDetailEvent{}, errors.New("pending tool settlement did not project")
	}
	return event, nil
}

func pendingToolCompletionPublicToken(value string) bool {
	text := strings.TrimSpace(value)
	if text == "" || len(text) > 240 {
		return false
	}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-', '.', ':', '/', '@':
			continue
		default:
			return false
		}
	}
	return true
}

func normalizePendingToolSettlement(settlement PendingToolSettlement) (PendingToolSettlement, error) {
	settlement.TurnID = strings.TrimSpace(settlement.TurnID)
	if settlement.TurnID == "" {
		return PendingToolSettlement{}, errors.New("pending tool settlement requires turn id")
	}
	settlement.RunID = strings.TrimSpace(settlement.RunID)
	if settlement.RunID == "" {
		return PendingToolSettlement{}, errors.New("pending tool settlement requires run id")
	}
	settlement.ToolCallID = strings.TrimSpace(settlement.ToolCallID)
	if settlement.ToolCallID == "" {
		return PendingToolSettlement{}, errors.New("pending tool settlement requires tool call id")
	}
	settlement.ToolName = strings.TrimSpace(settlement.ToolName)
	if settlement.ToolName == "" {
		return PendingToolSettlement{}, errors.New("pending tool settlement requires tool name")
	}
	settlement.Handle = strings.TrimSpace(settlement.Handle)
	if settlement.Handle == "" {
		return PendingToolSettlement{}, errors.New("pending tool settlement requires handle")
	}
	if !pendingToolCompletionPublicToken(settlement.Handle) {
		return PendingToolSettlement{}, errors.New("pending tool settlement requires token-safe handle")
	}
	settlement.EffectAttemptID = strings.TrimSpace(settlement.EffectAttemptID)
	switch settlement.Status {
	case PendingToolSettledCompleted, PendingToolSettledFailed, PendingToolSettledCanceled:
	default:
		return PendingToolSettlement{}, fmt.Errorf("pending tool settlement returned invalid status %q", settlement.Status)
	}
	settlement.Summary = strings.TrimSpace(settlement.Summary)
	if settlement.Summary == "" {
		return PendingToolSettlement{}, errors.New("pending tool settlement requires summary")
	}
	return settlement, nil
}

func pendingToolSettlementTarget(path []sessiontree.Entry, settlement PendingToolSettlement) (sessiontree.Entry, bool, error) {
	turnFound := false
	runFound := false
	callFound := false
	pendingFound := false
	activeHandleFound := false
	ordinaryResultFound := false
	for index := range path {
		entry := path[index]
		if strings.TrimSpace(entry.TurnID) != settlement.TurnID {
			continue
		}
		turnFound = true
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if runID != "" && runID == settlement.RunID {
				runFound = true
			}
			continue
		}
		if entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentDetailKindKey] == pendingToolSettlementEntryKind {
			if pendingToolSettlementEntryMatches(entry, settlement) {
				if strings.TrimSpace(entry.Metadata[pendingToolSettlementStateKey]) != string(settlement.Status) {
					return sessiontree.Entry{}, false, ErrPendingToolSettlementConflict
				}
				return entry, true, nil
			}
			continue
		}
		if !pendingToolSettlementEntryNamesTool(entry, settlement) {
			continue
		}
		callFound = true
		if entry.Type != sessiontree.EntryToolResult {
			continue
		}
		if entry.Message.ToolResult == nil ||
			strings.TrimSpace(entry.Message.ToolResult.Status) != string(observation.ActivityStatusRunning) {
			ordinaryResultFound = true
			continue
		}
		pendingFound = true
		if pendingHandleFromSessionActivity(entry.Message.Activity) == settlement.Handle &&
			strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) == settlement.EffectAttemptID {
			activeHandleFound = true
		}
	}
	if !turnFound {
		return sessiontree.Entry{}, false, ErrPendingToolSettlementTargetTurnNotFound
	}
	if !runFound {
		return sessiontree.Entry{}, false, ErrPendingToolSettlementTargetRunNotFound
	}
	if !callFound {
		return sessiontree.Entry{}, false, ErrPendingToolSettlementTargetToolNotFound
	}
	if pendingFound && !activeHandleFound {
		return sessiontree.Entry{}, false, ErrPendingToolSettlementTargetNotActive
	}
	if ordinaryResultFound && !pendingFound {
		return sessiontree.Entry{}, false, ErrPendingToolSettlementTargetNotActive
	}
	return sessiontree.Entry{}, false, nil
}

func pendingToolSettlementEntryNamesTool(entry sessiontree.Entry, settlement PendingToolSettlement) bool {
	switch entry.Type {
	case sessiontree.EntryToolCall, sessiontree.EntryToolResult:
	default:
		return false
	}
	return strings.TrimSpace(entry.Message.ToolCallID) == settlement.ToolCallID &&
		strings.TrimSpace(entry.Message.ToolName) == settlement.ToolName
}

func pendingToolSettlementEntryMatches(entry sessiontree.Entry, settlement PendingToolSettlement) bool {
	return strings.TrimSpace(entry.Metadata[pendingToolSettlementRunIDKey]) == settlement.RunID &&
		strings.TrimSpace(entry.Metadata[pendingToolSettlementToolIDKey]) == settlement.ToolCallID &&
		strings.TrimSpace(entry.Metadata[pendingToolSettlementNameKey]) == settlement.ToolName &&
		strings.TrimSpace(entry.Metadata[pendingToolSettlementHandleKey]) == settlement.Handle &&
		strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) == settlement.EffectAttemptID
}

func pendingHandleFromSessionActivity(activity *session.ActivityPresentation) string {
	if activity == nil || activity.Payload == nil {
		return ""
	}
	value, ok := activity.Payload["pending_handle"]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func (t *Thread) Compact(ctx context.Context, opts CompactOptions) (CompactResult, error) {
	requestID := strings.TrimSpace(opts.RequestID)
	if requestID == "" {
		return CompactResult{}, errors.New("manual compaction request id is required")
	}
	if strings.TrimSpace(opts.Source) == "" {
		return CompactResult{}, errors.New("manual compaction source is required")
	}
	if err := t.enterTurn(); err != nil {
		return CompactResult{}, err
	}
	defer t.leaveTurn()
	runID := manualCompactionRunID(t.id, requestID)
	snap, err := t.Journal(ctx)
	if err != nil {
		return CompactResult{Status: engine.Failed, Err: err, Diagnostics: map[string]string{"diagnostic": "snapshot_error"}}, err
	}
	if strings.TrimSpace(snap.Meta.LeafID) == "" {
		return CompactResult{}, errors.New("manual compaction requires a non-empty canonical path")
	}
	promptIdentity, err := t.harness.compactionPromptIdentity()
	if err != nil {
		return CompactResult{}, err
	}
	requestPayload, err := json.Marshal(struct {
		ThreadID               string                      `json:"thread_id"`
		RequestID              string                      `json:"request_id"`
		Source                 string                      `json:"source"`
		RunID                  string                      `json:"run_id"`
		Labels                 engine.RunLabels            `json:"labels"`
		Reasoning              provider.ReasoningSelection `json:"reasoning"`
		MaxInputTokens         int64                       `json:"max_input_tokens"`
		MaxTotalTokens         int64                       `json:"max_total_tokens"`
		MaxCostUSD             float64                     `json:"max_cost_usd"`
		MaxToolCalls           int                         `json:"max_tool_calls"`
		MaxLengthContinuations int                         `json:"max_length_continuations"`
	}{
		ThreadID: t.id, RequestID: requestID, Source: strings.TrimSpace(opts.Source), RunID: runID,
		Labels: opts.Labels, Reasoning: opts.Reasoning, MaxInputTokens: opts.MaxInputTokens,
		MaxTotalTokens: opts.MaxTotalTokens, MaxCostUSD: opts.MaxCostUSD, MaxToolCalls: opts.MaxToolCalls,
		MaxLengthContinuations: opts.MaxLengthContinuations,
	})
	if err != nil {
		return CompactResult{}, err
	}
	requestPayloadHash := sessiontree.StableHash(string(requestPayload))
	authority, ok := t.harness.options.Repo.(sessiontree.CompactionAuthorityRepo)
	if !ok {
		return CompactResult{}, errors.New("session tree repo does not support atomic compaction authority")
	}
	sourceLeafID := snap.Meta.LeafID
	activePathHash := sessiontree.ActivePathHash(snap.Path)
	requestFingerprint := ""
	if existing, found, err := authority.ReadCompaction(ctx, t.id, requestID); err != nil {
		return CompactResult{}, err
	} else if found {
		if existing.Source != strings.TrimSpace(opts.Source) || existing.SummarySchemaVersion != compaction.SummarySchemaVersion ||
			existing.PromptIdentity != promptIdentity || existing.RequestPayloadHash != requestPayloadHash {
			return CompactResult{}, sessiontree.ErrRequestConflict
		}
		sourceLeafID = existing.SourceLeafID
		activePathHash = existing.ActivePathHash
		requestFingerprint = existing.RequestFingerprint
	}
	beginPayload, err := json.Marshal(struct {
		ThreadID, RequestID, Source, SourceLeafID, ActivePathHash, SummarySchemaVersion, PromptIdentity, RequestPayloadHash string
	}{
		ThreadID: t.id, RequestID: requestID, Source: strings.TrimSpace(opts.Source), SourceLeafID: sourceLeafID,
		ActivePathHash: activePathHash, SummarySchemaVersion: compaction.SummarySchemaVersion,
		PromptIdentity: promptIdentity, RequestPayloadHash: requestPayloadHash,
	})
	if err != nil {
		return CompactResult{}, err
	}
	computedRequestFingerprint := sessiontree.StableHash(string(beginPayload))
	if requestFingerprint == "" {
		requestFingerprint = computedRequestFingerprint
	} else if requestFingerprint != computedRequestFingerprint {
		return CompactResult{}, sessiontree.ErrAuthorityCorrupt
	}
	begin, err := authority.BeginCompaction(ctx, sessiontree.BeginCompactionRequest{
		ThreadID: t.id, RequestID: requestID, RequestFingerprint: requestFingerprint,
		Source: strings.TrimSpace(opts.Source), SourceLeafID: sourceLeafID, ActivePathHash: activePathHash,
		SummarySchemaVersion: compaction.SummarySchemaVersion, PromptIdentity: promptIdentity, RequestPayloadHash: requestPayloadHash,
		OwnerID: t.harness.nextID("compaction-owner"), Now: t.harness.now(),
	})
	if err != nil {
		if errors.Is(err, sessiontree.ErrThreadAuthorityBusy) || errors.Is(err, sessiontree.ErrActiveTurn) {
			return CompactResult{}, ErrActiveTurn
		}
		return CompactResult{}, err
	}
	t.harness.cacheThreadAfterCommit(t)
	if begin.Operation.State != sessiontree.CompactionOperationPrepared {
		return t.replayCompactionResult(ctx, runID, begin.Operation)
	}
	if !begin.Owner {
		if !begin.TakeoverEligible {
			return CompactResult{}, ErrActiveTurn
		}
		begin, err = authority.TakeOverCompaction(ctx, sessiontree.TakeOverCompactionRequest{
			ThreadID: t.id, RequestID: requestID, RequestFingerprint: requestFingerprint,
			ExpectedLease: begin.Operation.Lease, OwnerID: t.harness.nextID("compaction-owner"), Now: t.harness.now(),
		})
		if err != nil {
			if errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
				return CompactResult{}, ErrActiveTurn
			}
			return CompactResult{}, err
		}
		if begin.Operation.State != sessiontree.CompactionOperationPrepared {
			return t.replayCompactionResult(ctx, runID, begin.Operation)
		}
	}
	lease := begin.Operation.Lease
	if err := t.bindActiveLease(lease); err != nil {
		return CompactResult{}, err
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, lease)
	renewedCtx, stopRenewal, err := t.startLeaseRenewal(leaseCtx, lease)
	if err != nil {
		return CompactResult{}, err
	}
	ctx = renewedCtx
	previousProviderState, err := t.harness.providerState(ctx, t.id, snap.Meta.LeafID)
	if err != nil {
		_ = stopRenewal()
		return t.finishFailedCompaction(ctx, authority, begin.Operation, requestFingerprint, runID, "provider_state_load_error", err, engine.Failed, engine.RunMetrics{})
	}
	history, err := sessiontree.BuildContextChecked(snap.Path, sessiontree.ContextOptions{})
	if err != nil {
		_ = stopRenewal()
		return t.finishFailedCompaction(ctx, authority, begin.Operation, requestFingerprint, runID, "context_projection_error", err, engine.Failed, engine.RunMetrics{})
	}
	engineOptions := t.harness.engineOptions()
	engineOptions.RunID = runID
	engineOptions.ThreadID = t.id
	engineOptions.TurnID = ""
	engineOptions.TraceID = runID
	engineOptions.PromptScopeID = t.id
	engineOptions.ProviderName = t.harness.options.ProviderName
	engineOptions.Model = t.harness.options.Model
	engineOptions.Labels = opts.Labels
	engineOptions.ContextPolicy = contextpolicy.Normalize(engineOptions.ContextPolicy)
	applyCompactOptions(&engineOptions, opts)
	engineOptions.PreviousProviderState = provider.CloneState(previousProviderState)
	manager := &durableCompactionManager{thread: t, manual: true}
	eng, err := engine.New(engine.Config{
		Provider:     t.harness.options.Provider,
		Tools:        t.harness.options.Tools,
		Prompt:       t.harness.options.PromptStore,
		SystemPrompt: t.harness.options.SystemPrompt,
		StopHook:     t.harness.options.StopHook,
		Compactor:    manager,
		Options:      engineOptions,
	})
	if err != nil {
		_ = stopRenewal()
		return t.finishFailedCompaction(ctx, authority, begin.Operation, requestFingerprint, runID, "engine_config_error", err, engine.Failed, engine.RunMetrics{})
	}
	downstream := t.harness.options.Sink
	if opts.Sink != nil {
		downstream = opts.Sink
	}
	eng.SetSink(downstream)
	result := eng.CompactContext(ctx, engine.RunInput{
		RunID:                 runID,
		ThreadID:              t.id,
		TraceID:               runID,
		PromptScopeID:         t.id,
		Labels:                opts.Labels,
		PreviousProviderState: provider.CloneState(previousProviderState),
		History:               history,
	}, engine.ManualCompactionRequest{RequestID: requestID, Source: strings.TrimSpace(opts.Source)})
	if renewalErr := stopRenewal(); renewalErr != nil && result.Err == nil {
		result.Status, result.Err = engine.Failed, renewalErr
	}
	if result.Err != nil || result.Status != engine.Completed || manager.result == nil {
		cause := result.Err
		code := "provider_failed"
		if cause == nil {
			cause = engine.ErrCompactionNoop
			code = "no_canonical_result"
		}
		if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
			code = "cancelled"
		}
		return t.finishFailedCompaction(ctx, authority, begin.Operation, requestFingerprint, runID, code, cause, result.Status, result.Metrics)
	}
	entry, err := sessiontree.CompactionEntry(t.id, "", *manager.result)
	if err != nil {
		return t.finishFailedCompaction(ctx, authority, begin.Operation, requestFingerprint, runID, "invalid_result", err, engine.Failed, result.Metrics)
	}
	outcomePayload, err := json.Marshal(struct {
		Status engine.Status     `json:"status"`
		Entry  sessiontree.Entry `json:"entry"`
	}{Status: result.Status, Entry: entry})
	if err != nil {
		return CompactResult{}, err
	}
	current, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok {
		return CompactResult{}, sessiontree.ErrStaleAuthority
	}
	persistCtx, cancelPersist := turnFinalizationContext(ctx)
	defer cancelPersist()
	finished, err := authority.FinishCompaction(persistCtx, sessiontree.FinishCompactionRequest{
		Lease: current, RequestID: requestID, RequestFingerprint: requestFingerprint,
		OutcomeFingerprint: sessiontree.StableHash(string(outcomePayload)), Result: &entry, Now: t.harness.now(),
	})
	if err != nil {
		return CompactResult{}, err
	}
	t.clearActiveLease(current)
	if finished.Entry == nil {
		return CompactResult{}, sessiontree.ErrAuthorityCorrupt
	}
	if !finished.Replayed {
		t.harness.emitEntryCommitted(*finished.Entry, runID)
		t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, EntryID: finished.Entry.ID, ParentID: finished.Entry.ParentID, Message: "compaction"})
	}
	return CompactResult{RunID: runID, OperationID: finished.Entry.CompactionOperationID, RequestID: requestID,
		Source: strings.TrimSpace(opts.Source), Status: result.Status, Metrics: result.Metrics, Entry: finished.Entry, Replayed: finished.Replayed}, nil
}

func manualCompactionRunID(threadID, requestID string) string {
	hash := sessiontree.StableHash(strings.TrimSpace(threadID) + "\x00" + strings.TrimSpace(requestID))
	return "compact-" + hash[:24]
}

func (h *AgentHarness) compactionPromptIdentity() (string, error) {
	if h.options.CompactionGenerator != nil {
		identity := strings.TrimSpace(h.options.CompactionPromptIdentity)
		if identity != "" {
			return identity, nil
		}
		if generator, ok := h.options.CompactionGenerator.(compaction.ExtractiveSummaryGenerator); ok {
			payload, err := json.Marshal(struct {
				Kind   string
				Prompt compaction.PromptOptions
			}{Kind: "extractive_v1", Prompt: generator.PromptOptions})
			if err != nil {
				return "", err
			}
			return sessiontree.StableHash(string(payload)), nil
		}
		return "", errors.New("custom compaction generator requires prompt identity")
	}
	payload, err := json.Marshal(struct {
		Provider, Model, StateCompatibilityKey, SystemPrompt string
		Prompt                                               compaction.PromptOptions
	}{
		Provider: h.options.ProviderName, Model: h.options.Model, StateCompatibilityKey: h.options.StateCompatibilityKey,
		SystemPrompt: h.options.SystemPrompt, Prompt: h.options.CompactionPrompt,
	})
	if err != nil {
		return "", err
	}
	return sessiontree.StableHash(string(payload)), nil
}

func (t *Thread) replayCompactionResult(ctx context.Context, runID string, operation sessiontree.CompactionOperation) (CompactResult, error) {
	result := CompactResult{RunID: runID, OperationID: engine.CompactionOperationID(runID, 1, compaction.TriggerManual, compaction.ReasonManual, operation.RequestID),
		RequestID: operation.RequestID, Source: operation.Source, Replayed: true}
	switch operation.State {
	case sessiontree.CompactionOperationCompleted:
		entry, err := t.harness.options.Repo.Entry(ctx, t.id, operation.ResultEntryID)
		if err != nil {
			return CompactResult{}, err
		}
		result.Status = engine.Completed
		result.Entry = &entry
		return result, nil
	case sessiontree.CompactionOperationFailed:
		result.Status = engine.Failed
		result.Err = errors.New(operation.ErrorMessage)
		result.Diagnostics = map[string]string{"diagnostic": operation.ErrorCode}
		return result, result.Err
	default:
		return CompactResult{}, sessiontree.ErrAuthorityCorrupt
	}
}

func (t *Thread) finishFailedCompaction(ctx context.Context, authority sessiontree.CompactionAuthorityRepo,
	operation sessiontree.CompactionOperation, requestFingerprint, runID, code string, cause error, status engine.Status, metrics engine.RunMetrics,
) (CompactResult, error) {
	if cause == nil {
		cause = errors.New("compaction failed without a cause")
	}
	current, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok {
		current = operation.Lease
	}
	outcome := sessiontree.StableHash(strings.Join([]string{operation.RequestID, code, cause.Error(), string(status)}, "\x00"))
	persistCtx, cancel := turnFinalizationContext(ctx)
	defer cancel()
	_, finishErr := authority.FinishCompaction(persistCtx, sessiontree.FinishCompactionRequest{
		Lease: current, RequestID: operation.RequestID, RequestFingerprint: requestFingerprint,
		OutcomeFingerprint: outcome, ErrorCode: code, ErrorMessage: cause.Error(), Now: t.harness.now(),
	})
	if finishErr != nil {
		return CompactResult{}, finishErr
	}
	t.clearActiveLease(current)
	if status == "" || status == engine.Completed {
		status = engine.Failed
	}
	return CompactResult{RunID: runID,
		OperationID: engine.CompactionOperationID(runID, 1, compaction.TriggerManual, compaction.ReasonManual, operation.RequestID),
		RequestID:   operation.RequestID, Source: operation.Source,
		Status: status, Err: cause, Metrics: metrics, Diagnostics: map[string]string{"diagnostic": code}}, cause
}

func (t *Thread) run(ctx context.Context, input string, opts RunOptions, retrySource *sessiontree.Entry) (TurnResult, error) {
	if strings.TrimSpace(input) == "" && len(opts.Attachments) == 0 && len(opts.References) == 0 && retrySource == nil {
		return TurnResult{}, errors.New("input is required")
	}
	if err := t.enterTurn(); err != nil {
		return TurnResult{}, err
	}
	defer t.leaveTurn()
	return t.runEntered(ctx, input, opts, retrySource)
}

func (t *Thread) runEntered(ctx context.Context, input string, opts RunOptions, retrySource *sessiontree.Entry) (TurnResult, error) {
	turnID := opts.TurnID
	if turnID == "" {
		turnID = t.harness.nextID("turn")
	}
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = t.harness.nextID("run")
	}
	authority, ok := t.harness.options.Repo.(sessiontree.TurnAuthorityRepo)
	if !ok {
		return TurnResult{}, errors.New("session tree repo does not support atomic turn admission")
	}
	_, existingAdmission, err := authority.ReadTurnAdmission(ctx, t.id, turnID, runID)
	if err != nil {
		return TurnResult{}, err
	}
	if !existingAdmission {
		normalized, err := engine.NormalizeAndValidateTurnSupplementalContext(opts.SupplementalContext)
		if err != nil {
			return TurnResult{}, err
		}
		opts.SupplementalContext = normalized
		if strings.TrimSpace(input) == "" && len(opts.Attachments) == 0 && len(opts.References) > 0 && len(normalized) == 0 {
			return TurnResult{}, errors.New("reference-only turn input requires renderable supplemental context")
		}
	}
	message := session.Message{
		Role: session.User, Content: input,
		Attachments: append([]session.MessageAttachment(nil), opts.Attachments...),
		References:  append([]session.MessageReference(nil), opts.References...),
	}
	admission, err := t.harness.admitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: t.id, TurnID: turnID, RunID: runID, OwnerID: t.harness.nextID("lease"), Input: message,
	})
	if err != nil {
		return TurnResult{}, err
	}
	if admission.Replayed {
		return t.turnAdmissionReplayResult(ctx, admission, turnID, runID)
	}
	t.harness.cacheThreadAfterCommit(t)
	lease := admission.Lease
	if err := t.bindActiveLease(lease); err != nil {
		return t.finalizeTurnStartupFailure(ctx, lease, turnID, runID, "local_owner_bind_error", err)
	}
	ctx = sessiontree.ContextWithTurnLease(ctx, lease)
	runCtx, stopRenewal, err := t.startLeaseRenewal(ctx, lease)
	if err != nil {
		return t.finalizeTurnStartupFailure(ctx, lease, turnID, runID, "lease_renewal_start_error", err)
	}
	defer func() {
		if current, ok := sessiontree.TurnLeaseFromContext(ctx); ok {
			t.clearActiveLease(current)
		}
	}()
	t.harness.emitEntryCommitted(admission.TurnStarted, runID)
	t.harness.emitEntryCommitted(admission.UserMessage, runID)
	t.harness.emit(HarnessEvent{Type: EventTurnStarted, RunID: runID, ThreadID: t.id, TurnID: turnID})
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: admission.UserMessage.ID, ParentID: admission.UserMessage.ParentID})
	automaticTitleExecution, titleErr := t.startAutomaticTitle(runCtx, turnID, runID, admission.UserMessage.ID, message)
	if titleErr != nil {
		persistCtx, cancelPersist := turnFinalizationContext(runCtx)
		result, runErr := t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(titleErr), titleErr, "automatic_title_begin_error", engine.FailureOriginStorage)
		cancelPersist()
		if renewalErr := stopRenewal(); renewalErr != nil && runErr == nil {
			return result, renewalErr
		}
		return result, runErr
	}
	opts.TurnID = turnID
	opts.RunID = runID
	opts.AdmissionCommitted = true
	opts.AdmissionBaseLeafID = admission.BaseLeafID
	result, runErr := t.runLeased(runCtx, input, opts, retrySource)
	if renewalErr := stopRenewal(); renewalErr != nil && runErr == nil {
		runErr = renewalErr
	}
	automaticTitleExecution.FinishMain(automaticTitleWorkerMustJoin(result, runErr))
	return result, runErr
}

func automaticTitleWorkerMustJoin(result TurnResult, runErr error) bool {
	return runErr != nil || result.Status == engine.Cancelled || result.Status == engine.Failed
}

func (t *Thread) runLeased(ctx context.Context, input string, opts RunOptions, retrySource *sessiontree.Entry) (TurnResult, error) {
	turnID := opts.TurnID
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = t.harness.nextID("run")
	}
	if !opts.AdmissionCommitted {
		return TurnResult{}, errors.New("provider execution requires committed turn admission")
	}
	lease, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok || lease.ThreadID != t.id || lease.TurnID != turnID || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return TurnResult{}, sessiontree.ErrStaleAuthority
	}
	_, err := t.harness.options.Repo.Thread(ctx, t.id)
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "thread_read_error", engine.FailureOriginStorage)
	}
	providerStateLeafID := strings.TrimSpace(opts.AdmissionBaseLeafID)
	previousProviderState, err := t.harness.providerState(ctx, t.id, providerStateLeafID)
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "provider_state_load_error", engine.FailureOriginStorage)
	}
	snap, err := t.Journal(ctx)
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "snapshot_error", engine.FailureOriginStorage)
	}
	historyPath := snap.Path
	if retrySource != nil {
		historyPath, err = t.harness.options.Repo.Path(ctx, t.id, retrySource.ID)
		if err != nil {
			persistCtx, cancelPersist := turnFinalizationContext(ctx)
			defer cancelPersist()
			return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "retry_source_path_error", engine.FailureOriginStorage)
		}
	}
	history, err := sessiontree.BuildContextChecked(historyPath, sessiontree.ContextOptions{})
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "context_projection_error", engine.FailureOriginStorage)
	}
	engineOptions := t.harness.engineOptions()
	engineOptions.RunID = runID
	engineOptions.ThreadID = t.id
	engineOptions.TurnID = turnID
	engineOptions.TraceID = runID
	engineOptions.PromptScopeID = t.id
	engineOptions.ProviderName = t.harness.options.ProviderName
	engineOptions.Model = t.harness.options.Model
	engineOptions.Labels = opts.Labels
	engineOptions.ContextPolicy = contextpolicy.Normalize(engineOptions.ContextPolicy)
	applyRunOptions(&engineOptions, opts)
	engineOptions.EffectBatchPreflight = t.preflightEffectBatch
	engineOptions.EffectDispatcher = t.effectDispatcher()
	engineOptions.EffectResultFinalizer = t.finalizeEffectResult
	engineOptions.ProviderRequestGate = t.enterProviderRequest
	engineOptions.PreviousProviderState = provider.CloneState(previousProviderState)
	if err := t.appendContextPolicyEvent(ctx, turnID, runID, engineOptions.ProviderName, engineOptions.Model, engineOptions.ContextPolicy); err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "append_context_policy_error", engine.FailureOriginStorage)
	}
	eng, err := engine.New(engine.Config{
		Provider:     t.harness.options.Provider,
		Tools:        t.harness.options.Tools,
		Prompt:       t.harness.options.PromptStore,
		SystemPrompt: t.harness.options.SystemPrompt,
		StopHook:     t.harness.options.StopHook,
		Compactor:    &durableCompactionManager{thread: t, turnID: turnID},
		Options:      engineOptions,
	})
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, engine.Failed, err, "engine_config_error", engine.FailureOriginContract)
	}
	downstream := t.harness.options.Sink
	if opts.Sink != nil {
		downstream = opts.Sink
	}
	projection := &turnProjection{thread: t, ctx: ctx, turnID: turnID, runID: runID, downstream: downstream}
	eng.SetSink(projection)
	result := eng.RunTurn(ctx, engine.RunInput{
		RunID:               runID,
		ThreadID:            t.id,
		TurnID:              turnID,
		TraceID:             runID,
		PromptScopeID:       t.id,
		Labels:              opts.Labels,
		History:             history,
		SupplementalContext: engine.CloneTurnSupplementalContext(opts.SupplementalContext),
	})
	if result.Status == engine.Cancelled {
		if cancellation := contextCancellationError(ctx); cancellation != nil {
			result.Err = cancellation
			result.FailureOrigin = engine.FailureOriginCancelled
		}
	}
	persistCtx, cancelPersist := turnFinalizationContext(ctx)
	defer cancelPersist()
	resultFailureCode := ""
	if result.Err != nil {
		var classificationErr error
		resultFailureCode, classificationErr = turnFailureCode(result.Status, result.Err, result.FailureOrigin)
		if classificationErr != nil {
			contractErr := fmt.Errorf("engine returned invalid failure classification: %w", classificationErr)
			return t.finalizeFailedTurn(persistCtx, turnID, runID, engine.Failed, contractErr, "engine_failure_classification_error", engine.FailureOriginContract)
		}
	}
	projection.ctx = persistCtx
	if projection.err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(projection.err), projection.err, "projection_error", engine.FailureOriginStorage)
	}
	if err := projection.FlushForTurnStatus(result.Status, result.Err); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "projection_flush_error", engine.FailureOriginStorage)
	}
	current, err := t.Journal(persistCtx)
	if err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "snapshot_error", engine.FailureOriginStorage)
	}
	if err := t.appendDelta(persistCtx, turnID, runID, history, result.Messages, current.Path); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "append_delta_error", engine.FailureOriginStorage)
	}
	status := markerForStatus(result.Status)
	savePointMetadata := markerMetadata(runID, result)
	savePointMetadata["reason"] = "run_result"
	if entry, err := sessiontree.AppendTurnMarker(persistCtx, t.harness.options.Repo, t.id, turnID, sessiontree.TurnSavePoint, savePointMetadata); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "save_point_error", engine.FailureOriginStorage)
	} else {
		t.harness.emitEntryCommitted(entry, runID)
	}
	terminalMetadata := markerMetadata(runID, result)
	mergeTerminalMetadata(terminalMetadata, opts.TerminalMetadata)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		mergeTerminalMetadata(terminalMetadata, opts.DeadlineMetadata)
	}
	if result.Err != nil {
		terminalMetadata["failure_reason"] = result.Err.Error()
		terminalMetadata[sessiontree.TurnFailureCodeMetadataKey] = resultFailureCode
	}
	if result.Status == engine.Waiting {
		terminalMetadata["interrupt_reason"] = "ask_user"
	}
	terminalEntryID := terminalTurnEntryID(t.id, turnID, runID)
	var stateToSave *provider.State
	if result.ProviderStateFresh {
		stateToSave = result.ProviderState
	}
	failureMessage := ""
	if result.Err != nil {
		failureMessage = result.Err.Error()
	}
	if _, err := t.finishTurn(persistCtx, runID, terminalEntryID, status, terminalMetadata, failureMessage, stateToSave); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "turn_finalization_error", engine.FailureOriginStorage)
	}
	eventType := EventTurnCompleted
	if result.Status == engine.Failed {
		eventType = EventTurnFailed
	}
	if result.Status == engine.Cancelled {
		eventType = EventTurnAborted
	}
	t.harness.emit(HarnessEvent{Type: eventType, RunID: runID, ThreadID: t.id, TurnID: turnID, Status: string(result.Status), Message: result.Output})
	turn := t.turnResultFromEngine(turnID, runID, result, nil)
	turn.FailureCode = strings.TrimSpace(terminalMetadata[sessiontree.TurnFailureCodeMetadataKey])
	return turn, result.Err
}

func (t *Thread) finishTurn(ctx context.Context, runID, terminalEntryID string, status sessiontree.TurnMarkerStatus, metadata map[string]string, failureMessage string, providerState *provider.State) (sessiontree.FinishTurnResult, error) {
	t.authorityMu.RLock()
	defer t.authorityMu.RUnlock()
	lease, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok || lease.ThreadID != t.id || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return sessiontree.FinishTurnResult{}, sessiontree.ErrStaleAuthority
	}
	repo, ok := t.harness.options.Repo.(sessiontree.TurnAuthorityRepo)
	if !ok {
		return sessiontree.FinishTurnResult{}, errors.New("session tree repo does not support atomic turn finish")
	}
	payload, err := json.Marshal(struct {
		ThreadID        string                       `json:"thread_id"`
		TurnID          string                       `json:"turn_id"`
		RunID           string                       `json:"run_id"`
		Generation      int64                        `json:"generation"`
		TerminalEntryID string                       `json:"terminal_entry_id"`
		Status          sessiontree.TurnMarkerStatus `json:"status"`
		Metadata        map[string]string            `json:"metadata,omitempty"`
		FailureMessage  string                       `json:"failure_message,omitempty"`
		ProviderState   *provider.State              `json:"provider_state,omitempty"`
	}{
		ThreadID: lease.ThreadID, TurnID: lease.TurnID, RunID: strings.TrimSpace(runID), Generation: lease.Generation,
		TerminalEntryID: strings.TrimSpace(terminalEntryID), Status: status, Metadata: cloneStringMap(metadata),
		FailureMessage: strings.TrimSpace(failureMessage),
		ProviderState:  provider.CloneState(providerState),
	})
	if err != nil {
		return sessiontree.FinishTurnResult{}, err
	}
	request := sessiontree.FinishTurnRequest{
		Lease: lease, RunID: strings.TrimSpace(runID), TerminalEntryID: strings.TrimSpace(terminalEntryID), Status: status,
		Metadata: cloneStringMap(metadata), FailureMessage: strings.TrimSpace(failureMessage),
		OutcomeFingerprint: sessiontree.StableHash(string(payload)), Now: t.harness.now(),
		ClearProviderState: providerState == nil && strings.TrimSpace(t.harness.options.StateCompatibilityKey) != "",
	}
	if providerState != nil {
		if strings.TrimSpace(t.harness.options.StateCompatibilityKey) == "" {
			return sessiontree.FinishTurnResult{}, errors.New("provider state compatibility key is required")
		}
		request.ProviderState = &sessiontree.ProviderStateRecord{
			ThreadID: t.id, LeafEntryID: strings.TrimSpace(terminalEntryID), CompatibilityKey: strings.TrimSpace(t.harness.options.StateCompatibilityKey),
			State: *provider.CloneState(providerState), CreatedByRunID: strings.TrimSpace(runID), CreatedByTurnID: lease.TurnID, UpdatedAt: t.harness.now(),
		}
	}
	result, err := repo.FinishTurn(ctx, request)
	if err != nil {
		return sessiontree.FinishTurnResult{}, err
	}
	if !result.Replayed {
		if result.Failure != nil {
			t.harness.emitEntryCommitted(*result.Failure, runID)
		}
		t.harness.emitEntryCommitted(result.Terminal, runID)
	}
	t.clearActiveLease(lease)
	return result, nil
}

func terminalTurnEntryID(threadID, turnID, runID string) string {
	hash := sessiontree.StableHash(strings.Join([]string{threadID, turnID, runID, "terminal"}, "\x00"))
	return "terminal-" + hash[:24]
}

func mergeTerminalMetadata(dst, src map[string]string) {
	for key, value := range src {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			dst[key] = value
		}
	}
}

func (t *Thread) finalizeFailedTurn(ctx context.Context, turnID, runID string, status engine.Status, err error, diagnostic string, origin engine.FailureOrigin) (TurnResult, error) {
	if status == "" {
		status = statusForError(err)
	}
	failureCode, classificationErr := turnFailureCode(status, err, origin)
	if classificationErr != nil {
		err = fmt.Errorf("invalid turn failure classification: %w", classificationErr)
		status = engine.Failed
		origin = engine.FailureOriginContract
		failureCode = sessiontree.TurnFailureEngineContract
	}
	result := engine.Result{Status: status, FailureOrigin: origin, Err: err}
	meta, readErr := t.harness.options.Repo.Thread(ctx, t.id)
	if readErr != nil {
		return TurnResult{}, readErr
	}
	path, readErr := t.harness.options.Repo.Path(ctx, t.id, meta.LeafID)
	if readErr != nil {
		return TurnResult{}, readErr
	}
	if closeErr := t.harness.closeInterruptedTurnToolCalls(ctx, t.id, turnID, path); closeErr != nil {
		return TurnResult{}, closeErr
	}
	metadata := markerMetadata(runID, result)
	if err != nil {
		metadata["failure_reason"] = err.Error()
	}
	if diagnostic != "" {
		metadata["diagnostic"] = diagnostic
	}
	metadata[sessiontree.TurnFailureCodeMetadataKey] = failureCode
	failureMessage := ""
	if err != nil {
		failureMessage = err.Error()
	}
	if _, finishErr := t.finishTurn(ctx, runID, terminalTurnEntryID(t.id, turnID, runID), markerForStatus(status), metadata, failureMessage, nil); finishErr != nil {
		return TurnResult{}, finishErr
	}
	eventType := EventTurnFailed
	if status == engine.Cancelled {
		eventType = EventTurnAborted
	}
	t.harness.emit(HarnessEvent{Type: eventType, RunID: runID, ThreadID: t.id, TurnID: turnID, Status: string(status)})
	turn := t.turnResultFromEngine(turnID, runID, result, map[string]string{"diagnostic": diagnostic})
	turn.FailureCode = strings.TrimSpace(metadata[sessiontree.TurnFailureCodeMetadataKey])
	return turn, err
}

func (t *Thread) finalizeTurnStartupFailure(ctx context.Context, lease sessiontree.TurnLease, turnID, runID, diagnostic string, cause error) (TurnResult, error) {
	if cause == nil {
		cause = errors.New("turn execution startup failed")
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, lease)
	persistCtx, cancelPersist := turnFinalizationContext(leaseCtx)
	defer cancelPersist()
	result, err := t.finalizeFailedTurn(persistCtx, turnID, runID, engine.Failed, cause, diagnostic, engine.FailureOriginContract)
	if err != nil && result.Status == "" {
		return result, errors.Join(cause, err)
	}
	return result, err
}

func statusForError(err error) engine.Status {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return engine.Cancelled
	}
	return engine.Failed
}

func (h *AgentHarness) engineOptions() engine.Options {
	engineOptions := engine.Options{}
	policy := h.options.TurnPolicy
	limits := h.options.LoopLimits
	if policy.ContextPolicy.ContextWindowTokens > 0 ||
		policy.ContextPolicy.MaxOutputTokens > 0 ||
		policy.ContextPolicy.ReservedOutputTokens > 0 ||
		policy.ContextPolicy.ReservedSummaryTokens > 0 ||
		policy.ContextPolicy.RecentTailTokens > 0 ||
		policy.ContextPolicy.RecentUserTokens > 0 ||
		policy.ContextPolicy.MaxCompactionFailures > 0 ||
		policy.ContextPolicy.EstimatorSource != "" {
		engineOptions.ContextPolicy = policy.ContextPolicy
	}
	if policy.CacheRetention != "" {
		engineOptions.CacheRetention = policy.CacheRetention
	}
	if !policy.Reasoning.IsZero() {
		engineOptions.Reasoning = policy.Reasoning
	}
	if len(policy.HostedToolDefinitions) > 0 {
		engineOptions.HostedToolDefinitions = append([]provider.HostedToolDefinition(nil), policy.HostedToolDefinitions...)
	}
	if policy.CompletionPolicy != "" {
		engineOptions.CompletionPolicy = policy.CompletionPolicy
	}
	if limits.MaxEmptyProviderRetries > 0 {
		engineOptions.MaxEmptyProviderRetries = limits.MaxEmptyProviderRetries
	}
	if limits.NoProgressLimit > 0 {
		engineOptions.NoProgressLimit = limits.NoProgressLimit
	}
	if limits.DuplicateToolLimit > 0 {
		engineOptions.DuplicateToolLimit = limits.DuplicateToolLimit
	}
	if limits.WallTime > 0 {
		engineOptions.WallTime = limits.WallTime
	}
	if limits.MaxInputTokens > 0 {
		engineOptions.MaxInputTokens = limits.MaxInputTokens
	}
	if limits.MaxTotalTokens > 0 {
		engineOptions.MaxTotalTokens = limits.MaxTotalTokens
	}
	if limits.MaxCostUSD > 0 {
		engineOptions.MaxCostUSD = limits.MaxCostUSD
	}
	if limits.MaxToolCalls > 0 {
		engineOptions.MaxToolCalls = limits.MaxToolCalls
	}
	if limits.MaxLengthContinuations > 0 {
		engineOptions.MaxLengthContinuations = limits.MaxLengthContinuations
	}
	if limits.MaxStopHookContinuations > 0 {
		engineOptions.MaxStopHookContinuations = limits.MaxStopHookContinuations
	}
	if h.options.ToolSurfaceProvider != nil {
		engineOptions.ToolSurfaceProvider = h.options.ToolSurfaceProvider
	}
	return engineOptions
}

func applyRunOptions(dst *engine.Options, opts RunOptions) {
	if dst == nil {
		return
	}
	if opts.CompletionPolicy != "" {
		dst.CompletionPolicy = opts.CompletionPolicy
	}
	if len(opts.ControlSpec.Definitions) > 0 || opts.ControlSpec.Project != nil {
		dst.ControlSpec = opts.ControlSpec
	}
	if !opts.Reasoning.IsZero() {
		dst.Reasoning = opts.Reasoning
	}
	if opts.MaxInputTokens > 0 {
		dst.MaxInputTokens = opts.MaxInputTokens
	}
	if opts.MaxTotalTokens > 0 {
		dst.MaxTotalTokens = opts.MaxTotalTokens
	}
	if opts.MaxCostUSD > 0 {
		dst.MaxCostUSD = opts.MaxCostUSD
	}
	if opts.MaxToolCalls > 0 {
		dst.MaxToolCalls = opts.MaxToolCalls
	}
	if opts.MaxLengthContinuations > 0 {
		dst.MaxLengthContinuations = opts.MaxLengthContinuations
	}
	if opts.MaxStopHookContinuations > 0 {
		dst.MaxStopHookContinuations = opts.MaxStopHookContinuations
	}
	if opts.ManualCompactions != nil {
		dst.ManualCompactions = opts.ManualCompactions
	}
	if opts.ToolSurfaceProvider != nil {
		dst.ToolSurfaceProvider = opts.ToolSurfaceProvider
	}
}

func applyCompactOptions(dst *engine.Options, opts CompactOptions) {
	if dst == nil {
		return
	}
	if !opts.Reasoning.IsZero() {
		dst.Reasoning = opts.Reasoning
	}
	if opts.MaxInputTokens > 0 {
		dst.MaxInputTokens = opts.MaxInputTokens
	}
	if opts.MaxTotalTokens > 0 {
		dst.MaxTotalTokens = opts.MaxTotalTokens
	}
	if opts.MaxCostUSD > 0 {
		dst.MaxCostUSD = opts.MaxCostUSD
	}
	if opts.MaxToolCalls > 0 {
		dst.MaxToolCalls = opts.MaxToolCalls
	}
	if opts.MaxLengthContinuations > 0 {
		dst.MaxLengthContinuations = opts.MaxLengthContinuations
	}
}

func turnResultFromEngine(turnID string, runID string, result engine.Result, diagnostics map[string]string) TurnResult {
	return TurnResult{
		ID:                 turnID,
		RunID:              runID,
		Status:             result.Status,
		Output:             result.Output,
		Err:                result.Err,
		Diagnostics:        diagnostics,
		Metrics:            result.Metrics,
		CompletionReason:   result.CompletionReason,
		ContinuationReason: result.ContinuationReason,
		FinishReason:       result.FinishReason,
		RawFinishReason:    result.RawFinishReason,
		FinishInferred:     result.FinishInferred,
		ControlSignal:      cloneEngineControlSignal(result.ControlSignal),
	}
}

func (t *Thread) turnResultFromEngine(turnID string, runID string, result engine.Result, diagnostics map[string]string) TurnResult {
	return turnResultFromEngine(turnID, runID, result, diagnostics)
}

func cloneEngineControlSignal(in *engine.ControlSignal) *engine.ControlSignal {
	if in == nil {
		return nil
	}
	out := *in
	out.Payload = cloneAnyMap(in.Payload)
	out.Labels = cloneStringMap(in.Labels)
	if in.Activity != nil {
		activity := *in.Activity
		out.Activity = &activity
	}
	return &out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneAny(value)
	}
	return out
}

func cloneAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneAnyMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneAny(item)
		}
		return out
	case []string:
		return append([]string(nil), v...)
	case map[string]string:
		return cloneStringMap(v)
	default:
		return value
	}
}

func turnFinalizationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// IMPORTANT: Turn finalization must outlive caller cancellation long enough to
	// persist the terminal marker; host/UI deadlines must not strand a durable
	// session in a permanently running state.
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func (h *AgentHarness) effectFinalizationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := 5 * time.Second
	if h != nil && h.effectFinalizationTimeout > 0 {
		timeout = h.effectFinalizationTimeout
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (t *Thread) enterTurn() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active {
		return ErrActiveTurn
	}
	t.active = true
	t.phase = threadPhaseTurn
	return nil
}

func (t *Thread) checkIdle(ctx context.Context) error {
	t.mu.Lock()
	active := t.active
	t.mu.Unlock()
	if active {
		return ErrActiveTurn
	}
	if lease, ok, err := t.harness.activeTurnLease(ctx, t.id); err != nil {
		return err
	} else if ok && lease.TurnID != "" {
		return ErrActiveTurn
	}
	return nil
}

func (t *Thread) leaveTurn() {
	t.mu.Lock()
	lease := t.activeLease
	t.active = false
	t.activeLease = sessiontree.TurnLease{}
	t.phase = threadPhaseIdle
	t.mu.Unlock()
	if registry := t.harness.options.TurnExecutions; registry != nil && registry.validate() && lease.Purpose == sessiontree.TurnLeasePurposeTurn && strings.TrimSpace(lease.OwnerID) != "" {
		registry.Unregister(lease)
	}
}

func (t *Thread) bindActiveLease(lease sessiontree.TurnLease) error {
	if strings.TrimSpace(lease.OwnerID) == "" {
		return nil
	}
	purpose, err := lease.Purpose.Normalize()
	if err != nil {
		return err
	}
	lease.Purpose = purpose
	if lease.ThreadID != t.id {
		return ErrActiveTurn
	}
	switch purpose {
	case sessiontree.TurnLeasePurposeTurn:
		if strings.TrimSpace(lease.TurnID) == "" {
			return ErrActiveTurn
		}
	case sessiontree.TurnLeasePurposeMutation:
		if strings.TrimSpace(lease.MutationID) == "" || strings.TrimSpace(lease.MutationKind) == "" {
			return ErrActiveTurn
		}
	default:
		return ErrActiveTurn
	}
	t.mu.Lock()
	if !t.active || strings.TrimSpace(t.activeLease.OwnerID) != "" {
		t.mu.Unlock()
		return ErrActiveTurn
	}
	t.activeLease = lease
	t.mu.Unlock()
	if registry := t.harness.options.TurnExecutions; registry != nil && registry.validate() && lease.Purpose == sessiontree.TurnLeasePurposeTurn {
		if err := registry.Register(lease); err != nil {
			t.mu.Lock()
			if sameTurnLease(t.activeLease, lease) {
				t.activeLease = sessiontree.TurnLease{}
			}
			t.mu.Unlock()
			return err
		}
	}
	return nil
}

func (t *Thread) clearActiveLease(lease sessiontree.TurnLease) {
	if strings.TrimSpace(lease.OwnerID) == "" {
		return
	}
	t.mu.Lock()
	matched := sameTurnLease(t.activeLease, lease)
	if sameTurnLease(t.activeLease, lease) {
		t.activeLease = sessiontree.TurnLease{}
	}
	t.mu.Unlock()
	if registry := t.harness.options.TurnExecutions; matched && registry != nil && registry.validate() && lease.Purpose == sessiontree.TurnLeasePurposeTurn {
		registry.Unregister(lease)
	}
}

func (t *Thread) replaceActiveLease(previous, renewed sessiontree.TurnLease) error {
	t.mu.Lock()
	if !t.active || !sameTurnLease(t.activeLease, previous) {
		t.mu.Unlock()
		return sessiontree.ErrStaleAuthority
	}
	if previous.ThreadID != renewed.ThreadID || previous.Purpose != renewed.Purpose ||
		previous.OwnerID != renewed.OwnerID || previous.Generation != renewed.Generation ||
		renewed.Heartbeat <= previous.Heartbeat {
		t.mu.Unlock()
		return sessiontree.ErrStaleAuthority
	}
	t.activeLease = renewed
	t.mu.Unlock()
	if registry := t.harness.options.TurnExecutions; registry != nil && registry.validate() && previous.Purpose == sessiontree.TurnLeasePurposeTurn {
		if err := registry.Renew(previous, renewed); err != nil {
			t.mu.Lock()
			if sameTurnLease(t.activeLease, renewed) {
				t.activeLease = previous
			}
			t.mu.Unlock()
			return err
		}
	}
	return nil
}

func (t *Thread) hasActiveLease(proof sessiontree.TurnLease) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active && sameTurnLease(t.activeLease, proof)
}

func (t *Thread) activeLeaseSnapshot() sessiontree.TurnLease {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.activeLease
}

func (t *Thread) ownedActiveTurnLease(turnID string) (sessiontree.TurnLease, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	lease := t.activeLease
	purpose, err := lease.Purpose.Normalize()
	if err != nil {
		return sessiontree.TurnLease{}, false
	}
	lease.Purpose = purpose
	if !t.active || purpose != sessiontree.TurnLeasePurposeTurn || lease.ThreadID != t.id || lease.TurnID != turnID || strings.TrimSpace(lease.OwnerID) == "" {
		return sessiontree.TurnLease{}, false
	}
	return lease, true
}

func (t *Thread) enterProviderRequest(ctx context.Context) (func(), error) {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return nil, errors.New("provider request requires an authority-bound thread")
	}
	t.authorityMu.RLock()
	release := func() { t.authorityMu.RUnlock() }
	lease, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok || lease.ThreadID != t.id || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		release()
		return nil, sessiontree.ErrStaleAuthority
	}
	local, ok := t.ownedActiveTurnLease(lease.TurnID)
	if !ok || !sessiontree.SameTurnLease(local, lease) {
		release()
		return nil, sessiontree.ErrStaleAuthority
	}
	inspector, ok := t.harness.options.Repo.(sessiontree.ThreadAuthorityInspectionRepo)
	if !ok {
		release()
		return nil, errors.New("session tree repo does not support provider authority inspection")
	}
	snapshot, err := inspector.InspectThreadAuthority(ctx, t.id)
	if err != nil {
		release()
		return nil, err
	}
	lifecycle, err := snapshot.Thread.CanonicalLifecycle()
	if err != nil {
		release()
		return nil, err
	}
	if lifecycle != sessiontree.ThreadLifecycleOpen {
		release()
		if lifecycle == sessiontree.ThreadLifecycleClosing {
			return nil, sessiontree.ErrSubAgentClosing
		}
		return nil, sessiontree.ErrThreadClosed
	}
	if snapshot.Lease == nil || !sessiontree.SameTurnLease(*snapshot.Lease, lease) || !snapshot.Lease.Fresh(t.harness.now().UTC()) || snapshot.ClaimOperationID != "" {
		release()
		return nil, sessiontree.ErrStaleAuthority
	}
	return release, nil
}

func sameTurnLease(first, second sessiontree.TurnLease) bool {
	return sessiontree.SameTurnLease(first, second)
}

func (t *Thread) appendDelta(ctx context.Context, turnID string, runID string, before, after []session.Message, currentPath []sessiontree.Entry) error {
	start := sharedMessagePrefix(before, after)
	persisted := persistedTurnMessages(currentPath, turnID)
	for _, msg := range after[start:] {
		if nonDurableProjection(msg) {
			continue
		}
		if suffix, ok := persisted.assistantSuffix(msg); ok {
			if strings.TrimSpace(suffix.Content) == "" && strings.TrimSpace(suffix.Reasoning) == "" {
				continue
			}
			msg = suffix
		}
		// IMPORTANT: Realtime turn projection and appendDelta share the durable
		// journal for one turn. appendDelta may only backfill messages that were
		// not already persisted by projection; hiding duplicates in the UI or
		// deduping across turns would corrupt the session history contract.
		if persisted.skip(msg) {
			continue
		}
		if err := t.appendMessage(ctx, turnID, runID, msg); err != nil {
			return err
		}
		persisted.record(msg)
	}
	return nil
}

type durableMessageCounter struct {
	counts map[durableMessageSignature]int
}

type durableMessageSignature struct {
	Role                 session.Role
	Content              string
	Reasoning            string
	ToolCallID           string
	ToolName             string
	ToolArgs             string
	Kind                 session.MessageKind
	ToolResult           string
	CompactionID         string
	CompactionGeneration int
	CompactionWindowID   string
}

func persistedTurnMessages(entries []sessiontree.Entry, turnID string) *durableMessageCounter {
	counter := &durableMessageCounter{counts: map[durableMessageSignature]int{}}
	for _, entry := range entries {
		if entry.TurnID != turnID {
			continue
		}
		switch entry.Type {
		case sessiontree.EntryUserMessage, sessiontree.EntryAssistantMessage, sessiontree.EntryToolCall, sessiontree.EntryToolResult:
			counter.record(entry.Message)
		}
	}
	return counter
}

func (c *durableMessageCounter) skip(msg session.Message) bool {
	if c == nil {
		return false
	}
	key := durableSignature(msg)
	if c.counts[key] <= 0 {
		return false
	}
	c.counts[key]--
	return true
}

func (c *durableMessageCounter) record(msg session.Message) {
	if c == nil {
		return
	}
	c.counts[durableSignature(msg)]++
}

func (c *durableMessageCounter) assistantSuffix(msg session.Message) (session.Message, bool) {
	if c == nil || msg.Role != session.Assistant || msg.ToolCallID != "" || msg.ToolName != "" || msg.ToolArgs != "" || msg.ToolResult != nil || msg.Kind != "" {
		return session.Message{}, false
	}
	content := msg.Content
	reasoning := msg.Reasoning
	for signature, count := range c.counts {
		if count <= 0 || signature.Role != session.Assistant || signature.ToolCallID != "" || signature.ToolName != "" || signature.ToolArgs != "" || signature.ToolResult != "" || signature.Kind != "" {
			continue
		}
		if signature.Content != "" {
			if !strings.HasPrefix(content, signature.Content) {
				continue
			}
			content = strings.TrimPrefix(content, signature.Content)
		}
		if signature.Reasoning != "" {
			if !strings.HasPrefix(reasoning, signature.Reasoning) {
				continue
			}
			reasoning = strings.TrimPrefix(reasoning, signature.Reasoning)
		}
	}
	if content == msg.Content && reasoning == msg.Reasoning {
		return session.Message{}, false
	}
	suffix := msg
	suffix.Content = content
	suffix.Reasoning = reasoning
	return suffix, true
}

func durableSignature(msg session.Message) durableMessageSignature {
	msg.EntryID = ""
	msg.ParentEntryID = ""
	return durableMessageSignature{
		Role:                 msg.Role,
		Content:              msg.Content,
		Reasoning:            msg.Reasoning,
		ToolCallID:           msg.ToolCallID,
		ToolName:             msg.ToolName,
		ToolArgs:             msg.ToolArgs,
		Kind:                 msg.Kind,
		ToolResult:           toolResultSignature(msg.ToolResult),
		CompactionID:         msg.CompactionID,
		CompactionGeneration: msg.CompactionGeneration,
		CompactionWindowID:   msg.CompactionWindowID,
	}
}

func nonDurableProjection(msg session.Message) bool {
	return msg.Kind == session.MessageKindCompactionSummary
}

func (t *Thread) appendMessage(ctx context.Context, turnID string, runID string, msg session.Message) error {
	return t.appendMessageAt(ctx, turnID, runID, msg, time.Time{})
}

func (t *Thread) appendMessageAt(ctx context.Context, turnID string, runID string, msg session.Message, observedAt time.Time) error {
	msg.EntryID = ""
	msg.ParentEntryID = ""
	entry, err := sessiontree.AppendMessageAt(ctx, t.harness.options.Repo, t.id, turnID, msg, observedAt)
	if err != nil {
		return err
	}
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID})
	return nil
}

func (t *Thread) appendApprovalEvent(ctx context.Context, turnID string, runID string, ev event.Event) error {
	entry, err := t.harness.options.Repo.Append(ctx, approvalEventEntry(t.id, turnID, ev), sessiontree.AppendOptions{})
	if err != nil {
		return err
	}
	t.emitCommittedApprovalEvent(entry, runID, ev)
	return nil
}

func approvalEventEntry(threadID, turnID string, ev event.Event) sessiontree.Entry {
	metadata := map[string]string{
		subAgentDetailKindKey:     subAgentApprovalEntryKind,
		subAgentDetailTypeKey:     string(ev.Type),
		subAgentApprovalStateKey:  approvalStateForEvent(ev.Type),
		subAgentApprovalToolIDKey: strings.TrimSpace(ev.ToolID),
		subAgentApprovalNameKey:   strings.TrimSpace(ev.ToolName),
		subAgentApprovalKindKey:   strings.TrimSpace(ev.ToolKind),
		subAgentApprovalArgsKey:   strings.TrimSpace(ev.ArgsHash),
	}
	if strings.TrimSpace(ev.Err) != "" {
		metadata[subAgentApprovalReasonKey] = strings.TrimSpace(ev.Err)
	}
	if values, ok := event.Sanitize(ev).Metadata.(map[string]any); ok {
		for key, value := range values {
			switch key {
			case "approval_id_hash", "effects", "read_only", "destructive", "open_world", "error_present":
				if text := safeApprovalMetadataValue(value); text != "" {
					metadata[key] = text
				}
			}
		}
	}
	return sessiontree.Entry{
		ThreadID: threadID,
		TurnID:   turnID,
		Type:     sessiontree.EntryCustom,
		Message:  session.Message{Activity: sessionActivityPresentation(sanitizeActivityPresentation(ev.Activity))},
		Metadata: metadata,
	}
}

func (t *Thread) emitCommittedApprovalEvent(entry sessiontree.Entry, runID string, ev event.Event) {
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: entry.TurnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: string(ev.Type)})
}

func (t *Thread) appendToolDispatchEvent(ctx context.Context, turnID string, runID string, ev event.Event) error {
	metadata := map[string]string{
		subAgentDetailKindKey: toolDispatchEntryKind,
		subAgentDetailTypeKey: string(event.ToolDispatchStarted),
		toolDispatchToolIDKey: strings.TrimSpace(ev.ToolID),
		toolDispatchNameKey:   strings.TrimSpace(ev.ToolName),
		toolDispatchKindKey:   strings.TrimSpace(ev.ToolKind),
		toolDispatchArgsKey:   strings.TrimSpace(ev.ArgsHash),
	}
	if values, ok := event.Sanitize(ev).Metadata.(map[string]any); ok {
		for key, value := range values {
			switch key {
			case "batch_index", "batch_size", "error_present":
				if text := safeApprovalMetadataValue(value); text != "" {
					metadata[key] = text
				}
			}
		}
	}
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   turnID,
		Type:     sessiontree.EntryCustom,
		Message: session.Message{
			ToolCallID: strings.TrimSpace(ev.ToolID),
			ToolName:   strings.TrimSpace(ev.ToolName),
			Activity:   sessionActivityPresentation(sanitizeActivityPresentation(ev.Activity)),
		},
		Metadata: metadata,
	}, sessiontree.AppendOptions{})
	if err != nil {
		return err
	}
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: string(event.ToolDispatchStarted)})
	return nil
}

func (t *Thread) appendToolActivityEvent(ctx context.Context, turnID string, runID string, ev event.Event) error {
	metadata := map[string]string{
		subAgentDetailKindKey: toolActivityEntryKind,
		subAgentDetailTypeKey: string(event.ToolActivityUpdated),
		toolActivityToolIDKey: strings.TrimSpace(ev.ToolID),
		toolActivityNameKey:   strings.TrimSpace(ev.ToolName),
		toolActivityKindKey:   strings.TrimSpace(ev.ToolKind),
		toolActivityArgsKey:   strings.TrimSpace(ev.ArgsHash),
	}
	if values, ok := event.Sanitize(ev).Metadata.(map[string]any); ok {
		for key, value := range values {
			if text := safeApprovalMetadataValue(value); text != "" {
				metadata[key] = text
			}
		}
	}
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   turnID,
		Type:     sessiontree.EntryCustom,
		Message: session.Message{
			ToolCallID: strings.TrimSpace(ev.ToolID),
			ToolName:   strings.TrimSpace(ev.ToolName),
			Activity:   sessionActivityPresentation(sanitizeActivityPresentation(ev.Activity)),
		},
		Metadata: metadata,
	}, sessiontree.AppendOptions{})
	if err != nil {
		return err
	}
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: string(event.ToolActivityUpdated)})
	return nil
}

func (t *Thread) appendPendingToolSettlement(ctx context.Context, settlement PendingToolSettlement) (sessiontree.Entry, error) {
	entry, _, err := pendingToolSettlementAuthorityEntry(t.id, settlement)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	entry, err = t.harness.options.Repo.Append(ctx, entry, sessiontree.AppendOptions{})
	if err != nil {
		return sessiontree.Entry{}, err
	}
	t.harness.emitEntryCommitted(entry, settlement.RunID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: settlement.RunID, ThreadID: t.id, TurnID: settlement.TurnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: pendingToolSettlementEntryKind})
	return entry, nil
}

func pendingToolSettlementAuthorityEntry(threadID string, settlement PendingToolSettlement) (sessiontree.Entry, string, error) {
	status := pendingToolSettlementActivityStatus(settlement.Status)
	metadata := map[string]string{
		subAgentDetailKindKey:                     pendingToolSettlementEntryKind,
		subAgentDetailTypeKey:                     pendingToolSettlementEntryKind,
		pendingToolSettlementStateKey:             string(settlement.Status),
		pendingToolSettlementToolIDKey:            settlement.ToolCallID,
		pendingToolSettlementNameKey:              settlement.ToolName,
		pendingToolSettlementHandleKey:            settlement.Handle,
		pendingToolSettlementRunIDKey:             settlement.RunID,
		pendingToolSettlementSummaryKey:           settlement.Summary,
		sessiontree.PendingToolEffectAttemptIDKey: settlement.EffectAttemptID,
	}
	if status == string(observation.ActivityStatusCanceled) {
		metadata["tool_result_status"] = string(observation.ActivityStatusCanceled)
	}
	activity := settlement.Activity
	if activity == nil {
		activity = &observation.ActivityPresentation{Label: settlement.Summary}
	}
	sanitizedActivity := sanitizeActivityPresentation(activity)
	payload, err := json.Marshal(struct {
		ThreadID        string                            `json:"thread_id"`
		TurnID          string                            `json:"turn_id"`
		RunID           string                            `json:"run_id"`
		ToolCallID      string                            `json:"tool_call_id"`
		ToolName        string                            `json:"tool_name"`
		Handle          string                            `json:"handle"`
		EffectAttemptID string                            `json:"effect_attempt_id,omitempty"`
		Status          PendingToolSettlementStatus       `json:"status"`
		Summary         string                            `json:"summary"`
		Output          string                            `json:"output"`
		Activity        *observation.ActivityPresentation `json:"activity,omitempty"`
	}{
		ThreadID: strings.TrimSpace(threadID), TurnID: settlement.TurnID, RunID: settlement.RunID,
		ToolCallID: settlement.ToolCallID, ToolName: settlement.ToolName, Handle: settlement.Handle,
		EffectAttemptID: settlement.EffectAttemptID,
		Status:          settlement.Status, Summary: settlement.Summary, Output: settlement.Output, Activity: sanitizedActivity,
	})
	if err != nil {
		return sessiontree.Entry{}, "", err
	}
	fingerprint := sessiontree.StableHash(string(payload))
	metadata[sessiontree.PendingToolSettlementKindKey] = sessiontree.PendingToolSettlementKind
	metadata[sessiontree.PendingToolSettlementFingerprintKey] = fingerprint
	entry := sessiontree.Entry{
		ThreadID: strings.TrimSpace(threadID),
		TurnID:   settlement.TurnID,
		Type:     sessiontree.EntryCustom,
		Message: session.Message{
			Content:    settlement.Output,
			ToolCallID: settlement.ToolCallID,
			ToolName:   settlement.ToolName,
			ToolResult: &session.ToolResultView{Status: status},
			Activity:   sessionActivityPresentation(sanitizedActivity),
		},
		Metadata: metadata,
	}
	return entry, fingerprint, nil
}

func (t *Thread) appendContextPolicyEvent(ctx context.Context, turnID string, runID string, providerName string, modelName string, policy contextpolicy.Policy) error {
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   turnID,
		Type:     sessiontree.EntryCustom,
		Metadata: subAgentContextPolicyMetadata(providerName, modelName, policy),
	}, sessiontree.AppendOptions{})
	if err != nil {
		return err
	}
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: subAgentContextPolicyEntryKind})
	return nil
}

func (t *Thread) appendContextStatusEvent(ctx context.Context, turnID string, runID string, ev event.Event) error {
	status, ok := subAgentContextStatusFromEvent(ev)
	if !ok {
		return nil
	}
	metadata := map[string]string{
		subAgentDetailKindKey:    subAgentContextStatusEntryKind,
		subAgentDetailTypeKey:    subAgentContextStatusEntryKind,
		subAgentContextStatusKey: mustSubAgentMetadataJSON(status),
	}
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   turnID,
		Type:     sessiontree.EntryCustom,
		Metadata: metadata,
	}, sessiontree.AppendOptions{Now: status.ObservedAt})
	if err != nil {
		return err
	}
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: subAgentContextStatusEntryKind})
	return nil
}

func (t *Thread) appendContextCompactionEvent(ctx context.Context, turnID string, runID string, ev event.Event) error {
	compact, ok, err := subAgentContextCompactionFromEvent(ev)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	metadata := map[string]string{
		subAgentDetailKindKey:        subAgentContextCompactionEntryKind,
		subAgentDetailTypeKey:        subAgentContextCompactionEntryKind,
		subAgentContextCompactionKey: mustSubAgentMetadataJSON(compact),
	}
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   turnID,
		Type:     sessiontree.EntryCustom,
		Metadata: metadata,
	}, sessiontree.AppendOptions{Now: compact.ObservedAt})
	if err != nil {
		return err
	}
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: subAgentContextCompactionEntryKind})
	return nil
}

func subAgentContextStatusFromEvent(ev event.Event) (observation.ContextStatus, bool) {
	switch ev.Type {
	case event.ProviderRequest:
		meta, ok := ev.Metadata.(map[string]any)
		if !ok {
			return observation.ContextStatus{}, false
		}
		estimate, ok := meta["request_estimate"].(contextpolicy.RequestEstimate)
		if !ok {
			return observation.ContextStatus{}, false
		}
		pressure, ok := meta["context_pressure"].(contextpolicy.ContextPressure)
		if !ok {
			return observation.ContextStatus{}, false
		}
		return observation.ContextStatusFromRequest(observation.RequestObservation{
			RunID:             ev.RunID,
			ThreadID:          ev.ThreadID,
			TurnID:            ev.TurnID,
			Step:              ev.Step,
			RequestID:         stringFromEventMetadata(meta["request_id"]),
			LogicalRequestID:  stringFromEventMetadata(meta["logical_request_id"]),
			Attempt:           intFromEventMetadata(meta["attempt"]),
			Provider:          ev.Provider,
			Model:             ev.Model,
			ObservedAt:        nonZeroTime(ev.Timestamp, time.Now()),
			RequestEstimate:   configbridge.RequestEstimate(estimate),
			ProjectedPressure: configbridge.PublicContextPressure(pressure),
		}), true
	case event.ProviderUsage:
		status, ok := ev.Metadata.(engine.ProviderUsageContextStatus)
		if !ok || status.Phase != engine.ProviderUsagePhaseFinalContextStatus {
			return observation.ContextStatus{}, false
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
			ObservedAt:       nonZeroTime(ev.Timestamp, time.Now()),
			Usage:            subAgentObservationProviderUsage(status.Usage),
			RequestEstimate:  configbridge.RequestEstimate(status.RequestEstimate),
			ContextPressure:  configbridge.PublicContextPressure(status.ContextPressure),
		})
		return out, ok
	default:
		return observation.ContextStatus{}, false
	}
}

func subAgentContextCompactionFromEvent(ev event.Event) (ThreadContextCompaction, bool, error) {
	if ev.Type != event.ContextCompact {
		return ThreadContextCompaction{}, false, nil
	}
	meta, ok := ev.Metadata.(map[string]any)
	if !ok {
		return ThreadContextCompaction{}, false, errors.New("context compaction event metadata is invalid")
	}
	phase := stringFromEventMetadata(meta["phase"])
	statusByPhase := map[string]string{
		string(observation.CompactionPhaseStart):     string(observation.CompactionStatusRunning),
		string(observation.CompactionPhaseComplete):  string(observation.CompactionStatusCompacted),
		string(observation.CompactionPhaseFailed):    string(observation.CompactionStatusFailed),
		string(observation.CompactionPhaseCancelled): string(observation.CompactionStatusCancelled),
		string(observation.CompactionPhaseNoop):      string(observation.CompactionStatusNoop),
	}
	status, ok := statusByPhase[phase]
	if !ok {
		return ThreadContextCompaction{}, false, fmt.Errorf("unsupported context compaction phase %q", phase)
	}
	operationID := stringFromEventMetadata(meta["operation_id"])
	if strings.TrimSpace(operationID) == "" {
		return ThreadContextCompaction{}, false, errors.New("context compaction operation id is required")
	}
	return ThreadContextCompaction{
		RunID:               ev.RunID,
		ThreadID:            ev.ThreadID,
		TurnID:              ev.TurnID,
		Step:                ev.Step,
		OperationID:         operationID,
		RequestID:           stringFromEventMetadata(meta["request_id"]),
		Phase:               phase,
		Status:              status,
		Trigger:             stringFromEventMetadata(meta["trigger"]),
		Reason:              stringFromEventMetadata(meta["reason"]),
		Source:              stringFromEventMetadata(meta["source"]),
		TokensBefore:        int64FromEventMetadata(meta["tokens_before"]),
		TokensAfterEstimate: int64FromEventMetadata(meta["tokens_after_estimate"]),
		Error:               strings.TrimSpace(ev.Err),
		ObservedAt:          nonZeroTime(ev.Timestamp, time.Now()),
	}, true, nil
}

func subAgentObservationProviderUsage(in provider.Usage) observation.ProviderUsage {
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

func stringFromEventMetadata(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func intFromEventMetadata(value any) int {
	return int(int64FromEventMetadata(value))
}

func int64FromEventMetadata(value any) int64 {
	switch v := value.(type) {
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

func pendingToolSettlementActivityStatus(status PendingToolSettlementStatus) string {
	switch status {
	case PendingToolSettledCompleted:
		return string(observation.ActivityStatusSuccess)
	case PendingToolSettledFailed:
		return string(observation.ActivityStatusError)
	case PendingToolSettledCanceled:
		return string(observation.ActivityStatusCanceled)
	default:
		return string(status)
	}
}

func approvalStateForEvent(typ event.Type) string {
	switch typ {
	case event.ToolApprovalRequested:
		return "requested"
	case event.ToolApprovalApproved:
		return "approved"
	case event.ToolApprovalRejected:
		return "rejected"
	case event.ToolApprovalTimedOut:
		return "timed_out"
	case event.ToolApprovalCanceled:
		return "canceled"
	default:
		return string(typ)
	}
}

func safeApprovalMetadataValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return fmt.Sprintf("%g", v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := safeApprovalMetadataValue(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, ",")
	case []string:
		return strings.Join(v, ",")
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func sharedMessagePrefix(a, b []session.Message) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if !messagesEqualForDelta(a[i], b[i]) {
			return i
		}
	}
	return n
}

func messagesEqualForDelta(a, b session.Message) bool {
	a.EntryID = ""
	a.ParentEntryID = ""
	b.EntryID = ""
	b.ParentEntryID = ""
	return durableSignature(a) == durableSignature(b)
}

func toolResultSignature(view *session.ToolResultView) string {
	if view == nil {
		return ""
	}
	data, err := json.Marshal(view)
	if err != nil {
		return fmt.Sprintf("%#v", view)
	}
	return string(data)
}

func markerForStatus(status engine.Status) sessiontree.TurnMarkerStatus {
	return sessionlifecycle.MarkerForEngineStatus(status)
}

func markerMetadata(runID string, result engine.Result) map[string]string {
	metadata := map[string]string{"run_id": runID}
	if result.CompletionReason != "" {
		metadata["completion_reason"] = string(result.CompletionReason)
	}
	if result.ContinuationReason != "" {
		metadata["continuation_reason"] = string(result.ContinuationReason)
	}
	if result.FinishReason != "" {
		metadata["finish_reason"] = string(result.FinishReason)
		metadata["finish_inferred"] = strconv.FormatBool(result.FinishInferred)
	}
	if result.RawFinishReason != "" {
		metadata["raw_finish_reason"] = result.RawFinishReason
	}
	return metadata
}

type retryTargetResult struct {
	Entry  sessiontree.Entry
	Source string
}

type durableCompactionManager struct {
	thread *Thread
	turnID string
	manual bool
	result *compaction.Result
}

func (m *durableCompactionManager) Compact(ctx context.Context, req engine.CompactionRequest) (compaction.Result, []session.Message, error) {
	if m == nil || m.thread == nil {
		return compaction.Result{}, nil, errors.New("durable compaction manager requires thread")
	}
	snap, err := m.thread.Journal(ctx)
	if err != nil {
		return compaction.Result{}, nil, err
	}
	previous := latestCompactionEntry(snap.Path)
	if strings.TrimSpace(previous.CompactionID) != strings.TrimSpace(req.PreviousCompactionID) ||
		previous.CompactionGeneration != req.PreviousGeneration ||
		strings.TrimSpace(previous.CompactionWindowID) != strings.TrimSpace(req.PreviousWindowID) ||
		strings.TrimSpace(previous.Summary) != strings.TrimSpace(req.PreviousSummary) {
		return compaction.Result{}, nil, errors.New("journal compaction identity does not match active context")
	}
	generator := m.thread.harness.options.CompactionGenerator
	if generator == nil {
		generator = enginecompaction.ProviderSummaryGenerator{
			Provider:      req.Provider,
			ProviderName:  req.ProviderName,
			Model:         req.Model,
			Reasoning:     m.thread.harness.options.Reasoning,
			Policy:        req.Policy,
			PromptOptions: m.thread.harness.options.CompactionPrompt,
		}
	}
	compactionID := m.thread.harness.nextID("compaction")
	if m.manual {
		hash := sessiontree.StableHash(m.thread.id + "\x00" + req.OperationID + "\x00" + req.RequestID)
		compactionID = "compaction-" + hash[:24]
	}
	prep, err := compaction.Prepare(ctx, compaction.Request{
		CompactionID:              compactionID,
		SupplementalAnchorEntryID: req.SupplementalAnchorEntryID,
		OperationID:               req.OperationID,
		RequestID:                 req.RequestID,
		Source:                    req.Source,
		PreviousCompactionID:      req.PreviousCompactionID,
		PreviousGeneration:        req.PreviousGeneration,
		PreviousWindowID:          req.PreviousWindowID,
		PreviousSummary:           req.PreviousSummary,
		History:                   req.History,
		Policy:                    req.Policy,
		Trigger:                   req.Trigger,
		Reason:                    req.Reason,
		Phase:                     req.Phase,
		Step:                      req.Step,
		Details:                   req.Details,
		Now:                       m.thread.harness.now(),
	}, generator)
	if err != nil {
		return compaction.Result{}, nil, err
	}
	return prep.Result, prep.ActiveMessages, nil
}

func (m *durableCompactionManager) CommitCompaction(ctx context.Context, req engine.CompactionCommitRequest) (compaction.Result, []session.Message, error) {
	if m == nil || m.thread == nil {
		return compaction.Result{}, nil, errors.New("durable compaction manager requires thread")
	}
	result := req.Result
	result.Phase = req.Phase
	if m.manual {
		copy := result
		copy.KeptUserEntryIDs = append([]string(nil), result.KeptUserEntryIDs...)
		copy.Details = cloneStringMap(result.Details)
		m.result = &copy
		return copy, session.CloneMessages(req.ActiveMessages), nil
	}
	entry, err := sessiontree.AppendCompaction(ctx, m.thread.harness.options.Repo, m.thread.id, m.turnID, result)
	if err != nil {
		var committed sessiontree.AppendCommittedError
		if !errors.As(err, &committed) {
			return compaction.Result{}, nil, err
		}
	}
	m.thread.harness.emitEntryCommitted(entry, req.RunID)
	result.CompactionID = entry.CompactionID
	result.CompactionGeneration = entry.CompactionGeneration
	result.CompactionWindowID = entry.CompactionWindowID
	active := append([]session.Message(nil), req.ActiveMessages...)
	for i := range active {
		if active[i].Kind != session.MessageKindCompactionSummary {
			continue
		}
		active[i].EntryID = entry.ID
		active[i].ParentEntryID = entry.ParentID
		active[i].CompactionID = entry.CompactionID
		active[i].CompactionGeneration = entry.CompactionGeneration
		active[i].CompactionWindowID = entry.CompactionWindowID
	}
	m.thread.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: req.RunID, ThreadID: m.thread.id, TurnID: m.turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: "compaction"})
	return result, active, nil
}

func latestCompactionEntry(path []sessiontree.Entry) sessiontree.Entry {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i].Type == sessiontree.EntryCompaction {
			return path[i]
		}
	}
	return sessiontree.Entry{}
}

func retryTarget(path []sessiontree.Entry) retryTargetResult {
	failedTurnID := ""
	for i := len(path) - 1; i >= 0; i-- {
		if path[i].Type == sessiontree.EntryRunFailure && path[i].TurnID != "" {
			failedTurnID = path[i].TurnID
			break
		}
	}
	if failedTurnID != "" {
		for i := len(path) - 1; i >= 0; i-- {
			if path[i].TurnID == failedTurnID && path[i].Type == sessiontree.EntryTurnMarker && path[i].TurnStatus == sessiontree.TurnSavePoint {
				if i > 0 {
					candidate := path[i-1]
					if _, eligible, err := sessiontree.RetrySourceHasRetryEligibleDurableInput(path, candidate.TurnID, candidate.ID); err == nil && eligible {
						return retryTargetResult{Entry: candidate, Source: "save_point"}
					}
					return retryTargetResult{}
				}
			}
		}
	}
	for i := len(path) - 1; i >= 0; i-- {
		if path[i].Type != sessiontree.EntryUserMessage {
			continue
		}
		if _, eligible, err := sessiontree.RetrySourceHasRetryEligibleDurableInput(path, path[i].TurnID, path[i].ID); err == nil && eligible {
			return retryTargetResult{Entry: path[i], Source: "user"}
		}
		return retryTargetResult{}
	}
	return retryTargetResult{}
}

type HarnessRecorder struct {
	mu     sync.Mutex
	Events []HarnessEvent
}

func (r *HarnessRecorder) EmitHarness(ev HarnessEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, ev)
}

func (r *HarnessRecorder) Snapshot() []HarnessEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.Events)
}

type turnProjection struct {
	thread           *Thread
	ctx              context.Context
	turnID           string
	runID            string
	downstream       event.Sink
	mu               sync.Mutex
	text             string
	reasoning        string
	pendingCalls     []pendingToolMessage
	pendingResults   []pendingToolMessage
	pendingBatchSize int
	pendingCallsSent bool
	lastCompaction   event.Event
	err              error
}

type pendingToolMessage struct {
	message          session.Message
	observedAt       time.Time
	canonicalEntryID string
}

func (p *turnProjection) Emit(ev event.Event) {
	if p.downstream != nil {
		p.downstream.Emit(event.SanitizeWithPolicy(ev, p.thread.harness.options.SinkPolicy))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return
	}
	switch ev.Type {
	case event.ProviderRequest, event.ProviderUsage:
		p.err = p.thread.appendContextStatusEvent(p.ctx, p.turnID, p.runID, ev)
	case event.ContextCompact:
		p.err = p.thread.appendContextCompactionEvent(p.ctx, p.turnID, p.runID, ev)
		if p.err == nil {
			p.lastCompaction = ev
		}
	case event.ProviderDelta:
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
		p.text += ev.Message
	case event.ProviderReasoning:
		p.reasoning += ev.Message
	case event.ToolCall:
		if err := p.flushPendingAssistantText(false); err != nil {
			p.err = err
			return
		}
		p.pendingCalls = append(p.pendingCalls, pendingToolMessage{
			message:    session.Message{Role: session.Assistant, Content: "tool_call", Reasoning: p.reasoning, ToolCallID: ev.ToolID, ToolName: ev.ToolName, ToolArgs: ev.Args, Activity: sessionActivityPresentation(sanitizeActivityPresentation(ev.Activity))},
			observedAt: ev.Timestamp,
		})
		if size := eventBatchSize(ev.Metadata); size > p.pendingBatchSize {
			p.pendingBatchSize = size
		}
	case event.ToolDispatchStarted:
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
		if err := p.flushPendingAssistantText(true); err != nil {
			p.err = err
			return
		}
		p.err = p.thread.appendToolDispatchEvent(p.ctx, p.turnID, p.runID, ev)
	case event.ToolActivityUpdated:
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
		if err := p.flushPendingAssistantText(true); err != nil {
			p.err = err
			return
		}
		p.err = p.thread.appendToolActivityEvent(p.ctx, p.turnID, p.runID, ev)
	case event.ToolResult:
		if err := p.flushPendingAssistantText(true); err != nil {
			p.err = err
			return
		}
		p.pendingResults = append(p.pendingResults, pendingToolMessage{
			message:          session.Message{Role: session.Tool, Content: ev.Result, ToolCallID: ev.ToolID, ToolName: ev.ToolName, ToolResult: toolResultViewFromEvent(ev), Activity: sessionActivityPresentation(sanitizeActivityPresentation(ev.Activity))},
			observedAt:       ev.Timestamp,
			canonicalEntryID: ev.CanonicalEntryID,
		})
		if size := eventBatchSize(ev.Metadata); size > p.pendingBatchSize {
			p.pendingBatchSize = size
		}
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
	case event.ToolApprovalRequested, event.ToolApprovalApproved, event.ToolApprovalRejected, event.ToolApprovalTimedOut, event.ToolApprovalCanceled:
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
		if err := p.flushPendingAssistantText(true); err != nil {
			p.err = err
			return
		}
		p.err = p.thread.appendApprovalEvent(p.ctx, p.turnID, p.runID, ev)
	case event.ContextContinue:
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
		if err := p.flushPendingAssistantText(true); err != nil {
			p.err = err
			return
		}
		p.err = p.thread.appendMessage(p.ctx, p.turnID, p.runID, session.Message{Role: session.User, Content: ev.Message})
		if p.err != nil {
			return
		}
		metadata := map[string]string{"reason": "context_continue", "continuation_reason": ev.ContinuationReason, "run_id": p.runID}
		if ev.Result != "" {
			metadata["hook_reason"] = ev.Result
		}
		var entry sessiontree.Entry
		entry, p.err = sessiontree.AppendTurnMarker(p.ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, sessiontree.TurnSavePoint, metadata)
		if p.err == nil {
			p.thread.harness.emitEntryCommitted(entry, p.runID)
		}
	case event.StepEnd:
		if ev.ContinuationReason != "" {
			if err := p.flushPendingAssistantText(true); err != nil {
				p.err = err
				return
			}
		}
	}
}

func (p *turnProjection) failCompactionFinalization(cause error) error {
	p.mu.Lock()
	last := p.lastCompaction
	p.mu.Unlock()
	if last.Type != event.ContextCompact {
		return errors.New("compaction finalization failed without a compaction lifecycle event")
	}
	metadata, ok := last.Metadata.(map[string]any)
	if !ok {
		return errors.New("compaction finalization event metadata is invalid")
	}
	metadata = cloneAnyMap(metadata)
	metadata["phase"] = string(observation.CompactionPhaseFailed)
	last.Metadata = metadata
	last.Err = cause.Error()
	last.Result = ""
	last.Timestamp = p.thread.harness.now()
	p.Emit(last)
	return p.Flush()
}

func (p *turnProjection) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	if err := p.flushPendingToolBatch(true); err != nil {
		p.err = err
		return err
	}
	if err := p.flushPendingAssistantText(true); err != nil {
		p.err = err
		return err
	}
	return nil
}

func (p *turnProjection) FlushForTurnStatus(status engine.Status, cause error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	if status == engine.Cancelled || status == engine.Failed {
		if err := p.closePendingToolBatchForTerminalTurn(status, cause); err != nil {
			p.err = err
			return err
		}
	} else if err := p.flushPendingToolBatch(true); err != nil {
		p.err = err
		return err
	}
	if err := p.flushPendingAssistantText(true); err != nil {
		p.err = err
		return err
	}
	return nil
}

func (p *turnProjection) flushPendingAssistantText(resetReasoning bool) error {
	if p.text == "" {
		if resetReasoning {
			p.reasoning = ""
		}
		return nil
	}
	if err := p.thread.appendMessage(p.ctx, p.turnID, p.runID, session.Message{Role: session.Assistant, Content: p.text, Reasoning: p.reasoning}); err != nil {
		return err
	}
	p.text = ""
	if resetReasoning {
		p.reasoning = ""
	}
	return nil
}

func (p *turnProjection) closePendingToolBatchForTerminalTurn(status engine.Status, cause error) error {
	if len(p.pendingCalls) == 0 && len(p.pendingResults) == 0 {
		return nil
	}
	if !p.pendingCallsSent {
		seenCalls := make(map[string]struct{}, len(p.pendingCalls))
		for _, call := range p.pendingCalls {
			if call.message.ToolCallID == "" {
				return errors.New("tool call batch contains empty tool_call_id")
			}
			if _, ok := seenCalls[call.message.ToolCallID]; ok {
				return fmt.Errorf("tool call batch contains duplicate tool_call_id %q", call.message.ToolCallID)
			}
			seenCalls[call.message.ToolCallID] = struct{}{}
		}
		for _, call := range p.pendingCalls {
			if err := p.thread.appendMessageAt(p.ctx, p.turnID, p.runID, call.message, call.observedAt); err != nil {
				return err
			}
		}
		p.pendingCallsSent = true
		p.reasoning = ""
	}

	byID := make(map[string]pendingToolMessage, len(p.pendingResults))
	for _, result := range p.pendingResults {
		if result.message.ToolCallID == "" {
			return errors.New("tool result batch contains empty tool_call_id")
		}
		if _, ok := byID[result.message.ToolCallID]; ok {
			return fmt.Errorf("tool result batch contains duplicate tool_call_id %q", result.message.ToolCallID)
		}
		byID[result.message.ToolCallID] = result
	}

	appended := false
	for _, call := range p.pendingCalls {
		result, ok := byID[call.message.ToolCallID]
		if !ok {
			result = pendingToolMessage{
				message:    terminalTurnClosureToolResult(call.message, status, cause),
				observedAt: p.thread.harness.now(),
			}
		}
		if err := p.thread.appendMessageAt(p.ctx, p.turnID, p.runID, result.message, result.observedAt); err != nil {
			return err
		}
		delete(byID, call.message.ToolCallID)
		appended = true
	}
	for id := range byID {
		return fmt.Errorf("tool result batch references unknown tool_call_id %q", id)
	}
	if appended {
		entry, err := sessiontree.AppendTurnMarker(p.ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, sessiontree.TurnSavePoint, map[string]string{"reason": "tool_result_batch", "run_id": p.runID})
		if err != nil {
			return err
		}
		p.thread.harness.emitEntryCommitted(entry, p.runID)
	}
	p.pendingCalls = nil
	p.pendingResults = nil
	p.pendingBatchSize = 0
	p.pendingCallsSent = false
	return nil
}

func terminalTurnClosureToolResult(call session.Message, status engine.Status, cause error) session.Message {
	resultStatus := string(observation.ActivityStatusCanceled)
	text := "Tool call was canceled before completion."
	if status == engine.Failed {
		resultStatus = string(observation.ActivityStatusError)
		text = "Tool call did not complete before the turn failed."
	}
	if cause != nil && status == engine.Failed {
		if message := strings.TrimSpace(cause.Error()); message != "" {
			text = message
		}
	}
	activity := session.CloneActivityPresentation(call.Activity)
	if activity == nil {
		activity = &session.ActivityPresentation{}
	}
	activity.Payload = cloneSessionActivityPayload(activity.Payload)
	if activity.Payload == nil {
		activity.Payload = map[string]any{}
	}
	activity.Payload["status"] = resultStatus
	return session.Message{
		Role:       session.Tool,
		Content:    text,
		ToolCallID: strings.TrimSpace(call.ToolCallID),
		ToolName:   strings.TrimSpace(call.ToolName),
		ToolResult: &session.ToolResultView{Status: resultStatus},
		Activity:   activity,
	}
}

func cloneSessionActivityPayload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (p *turnProjection) flushPendingToolBatch(force bool) error {
	if len(p.pendingCalls) == 0 && len(p.pendingResults) == 0 {
		return nil
	}
	size := p.pendingBatchSize
	if size <= 0 {
		size = len(p.pendingCalls)
	}
	if !p.pendingCallsSent {
		if len(p.pendingCalls) < size {
			if force {
				return fmt.Errorf("incomplete tool call batch: %d calls, want %d", len(p.pendingCalls), size)
			}
			return nil
		}
		seenCalls := make(map[string]struct{}, len(p.pendingCalls))
		for _, call := range p.pendingCalls {
			if call.message.ToolCallID == "" {
				return errors.New("tool call batch contains empty tool_call_id")
			}
			if _, ok := seenCalls[call.message.ToolCallID]; ok {
				return fmt.Errorf("tool call batch contains duplicate tool_call_id %q", call.message.ToolCallID)
			}
			seenCalls[call.message.ToolCallID] = struct{}{}
		}
		for _, call := range p.pendingCalls {
			if err := p.thread.appendMessageAt(p.ctx, p.turnID, p.runID, call.message, call.observedAt); err != nil {
				return err
			}
		}
		p.pendingCallsSent = true
		p.reasoning = ""
	}
	if len(p.pendingResults) == 0 {
		return nil
	}
	byID := make(map[string]pendingToolMessage, len(p.pendingResults))
	for _, result := range p.pendingResults {
		if result.message.ToolCallID == "" {
			return errors.New("tool result batch contains empty tool_call_id")
		}
		if _, ok := byID[result.message.ToolCallID]; ok {
			return fmt.Errorf("tool result batch contains duplicate tool_call_id %q", result.message.ToolCallID)
		}
		byID[result.message.ToolCallID] = result
	}
	appendedResult := false
	appendedIDs := map[string]struct{}{}
	appendable := 0
	for _, call := range p.pendingCalls {
		result, ok := byID[call.message.ToolCallID]
		if !ok {
			break
		}
		committed, err := p.committedEffectResult(result.message, result.canonicalEntryID)
		if err != nil {
			return err
		}
		if !committed {
			if err := p.thread.appendMessageAt(p.ctx, p.turnID, p.runID, result.message, result.observedAt); err != nil {
				return err
			}
		}
		delete(byID, call.message.ToolCallID)
		appendedIDs[call.message.ToolCallID] = struct{}{}
		appendedResult = true
		appendable++
	}
	for id := range byID {
		known := false
		for _, call := range p.pendingCalls[appendable:] {
			if call.message.ToolCallID == id {
				known = true
				break
			}
		}
		if !known {
			return fmt.Errorf("tool result batch references unknown tool_call_id %q", id)
		}
	}
	remainingCalls := p.pendingCalls[appendable:]
	if force && len(remainingCalls) > 0 {
		return fmt.Errorf("incomplete tool result batch: %d calls, %d results", len(remainingCalls), len(byID))
	}
	if appendedResult && len(remainingCalls) == 0 {
		if entry, err := sessiontree.AppendTurnMarker(p.ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, sessiontree.TurnSavePoint, map[string]string{"reason": "tool_result_batch", "run_id": p.runID}); err != nil {
			return err
		} else {
			p.thread.harness.emitEntryCommitted(entry, p.runID)
		}
	}
	p.pendingCalls = remainingCalls
	remainingResults := p.pendingResults[:0]
	for _, result := range p.pendingResults {
		if _, ok := appendedIDs[result.message.ToolCallID]; ok {
			continue
		}
		remainingResults = append(remainingResults, result)
	}
	p.pendingResults = remainingResults
	if len(p.pendingCalls) == 0 {
		p.pendingBatchSize = 0
		p.pendingCallsSent = false
		p.pendingResults = nil
	}
	return nil
}

func (p *turnProjection) committedEffectResult(message session.Message, canonicalEntryID string) (bool, error) {
	canonicalEntryID = strings.TrimSpace(canonicalEntryID)
	if canonicalEntryID == "" {
		return false, nil
	}
	entry, err := p.thread.harness.options.Repo.Entry(p.ctx, p.thread.id, canonicalEntryID)
	if err != nil {
		if errors.Is(err, sessiontree.ErrEntryNotFound) || errors.Is(err, sessiontree.ErrThreadNotFound) {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		return false, err
	}
	if entry.ID != canonicalEntryID || entry.ThreadID != p.thread.id || entry.TurnID != p.turnID || entry.Type != sessiontree.EntryToolResult || entry.Message.ToolCallID != message.ToolCallID || entry.Message.ToolName != message.ToolName {
		return false, sessiontree.ErrAuthorityCorrupt
	}
	if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
		return false, err
	}
	if durableSignature(entry.Message) != durableSignature(message) {
		return false, sessiontree.ErrAuthorityCorrupt
	}
	return true, nil
}

func eventBatchSize(metadata any) int {
	values, ok := metadata.(map[string]any)
	if !ok {
		return 0
	}
	switch size := values["batch_size"].(type) {
	case int:
		return size
	case int64:
		return int(size)
	case float64:
		return int(size)
	default:
		return 0
	}
}

func toolResultViewFromEvent(ev event.Event) *session.ToolResultView {
	values, _ := ev.Metadata.(map[string]any)
	status := toolResultStatusFromEvent(ev, values)
	if len(values) == 0 && len(ev.Artifacts) == 0 && status == "" {
		return nil
	}
	view := &session.ToolResultView{
		Status:        status,
		Truncated:     metadataBool(values, "truncated"),
		OriginalBytes: metadataInt(values, "original_bytes"),
		VisibleBytes:  metadataInt(values, "visible_bytes"),
		OriginalLines: metadataInt(values, "original_lines"),
		VisibleLines:  metadataInt(values, "visible_lines"),
		Strategy:      metadataString(values, "strategy"),
		ContentSHA256: metadataString(values, "content_sha256"),
	}
	if artifactID := metadataString(values, "artifact_id"); artifactID != "" {
		for _, item := range ev.Artifacts {
			if item.ID != artifactID {
				continue
			}
			ref := artifactRefFromEvent(item)
			if ref.ID != "" || ref.SafeLabel != "" {
				view.FullOutput = &ref
			}
			break
		}
	}
	if emptyToolResultView(view) {
		return nil
	}
	return view
}

func emptyToolResultView(view *session.ToolResultView) bool {
	return view == nil ||
		(view.Status == "" &&
			!view.Truncated &&
			view.OriginalBytes == 0 &&
			view.VisibleBytes == 0 &&
			view.OriginalLines == 0 &&
			view.VisibleLines == 0 &&
			view.Strategy == "" &&
			view.ContentSHA256 == "" &&
			view.FullOutput == nil)
}

func toolResultStatusFromEvent(ev event.Event, values map[string]any) string {
	if metadataBool(values, "pending_tool_result") {
		return string(observation.ActivityStatusRunning)
	}
	if strings.TrimSpace(ev.Err) != "" || metadataBool(values, "error_present") {
		return string(observation.ActivityStatusError)
	}
	return string(observation.ActivityStatusSuccess)
}

func sanitizeActivityPresentation(activity *observation.ActivityPresentation) *observation.ActivityPresentation {
	return event.Sanitize(event.Event{Activity: activity}).Activity
}

func sessionActivityPresentation(in *observation.ActivityPresentation) *session.ActivityPresentation {
	if in == nil {
		return nil
	}
	out := &session.ActivityPresentation{
		Label:       in.Label,
		Description: in.Description,
		Renderer:    string(in.Renderer),
		Chips:       make([]session.ActivityChip, 0, len(in.Chips)),
		TargetRefs:  make([]session.ActivityTargetRef, 0, len(in.TargetRefs)),
		Payload:     cloneActivityPayload(in.Payload),
	}
	for _, chip := range in.Chips {
		out.Chips = append(out.Chips, session.ActivityChip{
			Kind:  chip.Kind,
			Label: chip.Label,
			Value: chip.Value,
			Tone:  chip.Tone,
		})
	}
	for _, ref := range in.TargetRefs {
		out.TargetRefs = append(out.TargetRefs, session.ActivityTargetRef{
			Kind:  ref.Kind,
			Label: ref.Label,
			URI:   ref.URI,
			Path:  ref.Path,
			Line:  ref.Line,
		})
	}
	return out
}

func cloneActivityPayload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneActivityPayloadValue(value)
	}
	return out
}

func cloneActivityPayloadValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneActivityPayload(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneActivityPayloadValue(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return typed
	}
}

func artifactRefFromEvent(in event.Artifact) artifact.Ref {
	return artifact.Ref{
		ID:        in.ID,
		SafeLabel: in.SafeLabel,
		Kind:      in.Kind,
		MIME:      in.MIME,
		SizeBytes: in.SizeBytes,
		SHA256:    in.SHA256,
	}
}

func metadataString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	value, _ := values[key].(string)
	return value
}

func metadataBool(values map[string]any, key string) bool {
	if values == nil {
		return false
	}
	value, _ := values[key].(bool)
	return value
}

func metadataInt(values map[string]any, key string) int {
	if values == nil {
		return 0
	}
	switch value := values[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}
