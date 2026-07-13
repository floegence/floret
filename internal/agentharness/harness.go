package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
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
)

const (
	threadPhaseIdle       = sessionlifecycle.PhaseIdle
	threadPhaseTurn       = sessionlifecycle.PhaseTurn
	staleTurnLeaseTimeout = 24 * time.Hour
)

type HarnessEventType string

const (
	EventThreadStarted     HarnessEventType = "thread_started"
	EventThreadResumed     HarnessEventType = "thread_resumed"
	EventThreadForked      HarnessEventType = "thread_forked"
	EventLeafMoved         HarnessEventType = "leaf_moved"
	EventTurnStarted       HarnessEventType = "turn_started"
	EventTurnCompleted     HarnessEventType = "turn_completed"
	EventTurnFailed        HarnessEventType = "turn_failed"
	EventTurnAborted       HarnessEventType = "turn_aborted"
	EventEntryAppended     HarnessEventType = "entry_appended"
	EventRetryStarted      HarnessEventType = "retry_started"
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
	Provider            provider.Provider
	ProviderName        string
	Model               string
	SystemPrompt        string
	Tools               *tools.Registry
	PromptStore         cache.Store
	Repo                sessiontree.Repo
	Sink                event.Sink
	SinkPolicy          event.SinkPolicy
	HarnessSink         HarnessSink
	Approver            tools.Approver
	ToolSurfaceProvider engine.ToolSurfaceProvider
	StopHook            engine.StopHook
	CompactionGenerator compaction.SummaryGenerator
	CompactionPrompt    compaction.PromptOptions
	TitleGenerator      TitleGenerator
	Artifacts           artifact.Store
	Reasoning           provider.ReasoningCapability
	TurnPolicy          TurnPolicy
	LoopLimits          LoopLimits
	SubAgentRunTimeout  time.Duration
	NewID               func(string) string
	Now                 func() time.Time
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
	mu              sync.Mutex
	subagentSpawnMu sync.Mutex
	options         Options
	threads         map[string]*Thread
	subagents       map[string]*subagentController
	subagentUpdates chan struct{}
	approvals       map[string]map[string]PendingApproval
	seq             int64
}

type StartThreadOptions struct {
	ThreadID string
}

type ResumeOptions struct{}

type ForkOptions struct {
	SourceThreadID         string
	EntryID                string
	Position               sessiontree.ForkPosition
	NewThreadID            string
	RewriteTurnIdentities  bool
	CloneTerminalSubAgents bool
}

type ForkResult struct {
	Thread  *Thread
	Summary ThreadSummary
	Turns   []ForkedTurnRef
}

type ForkedTurnRef struct {
	SourceTurnID      string
	SourceRunID       string
	DestinationTurnID string
	DestinationRunID  string
	CreatedAt         time.Time
}

type MoveOptions struct {
	Summary string
}

type RunOptions struct {
	RunID                    string
	TurnID                   string
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
	PreviousProviderState    *provider.State
	ManualCompactions        engine.ManualCompactionSource
	ToolSurfaceProvider      engine.ToolSurfaceProvider
	SupplementalContext      []engine.TurnSupplementalContextItem
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
	PreviousProviderState  *provider.State
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
	TurnID     string
	RunID      string
	ToolCallID string
	ToolName   string
	Handle     string
	Status     PendingToolCompletionStatus
	Summary    string
	Output     string
	Labels     engine.RunLabels
}

type PendingToolSettlementStatus string

const (
	PendingToolSettledCompleted PendingToolSettlementStatus = "completed"
	PendingToolSettledFailed    PendingToolSettlementStatus = "failed"
	PendingToolSettledCanceled  PendingToolSettlementStatus = "canceled"
)

type PendingToolSettlement struct {
	TurnID     string
	RunID      string
	ToolCallID string
	ToolName   string
	Handle     string
	Status     PendingToolSettlementStatus
	Summary    string
	Output     string
	Activity   *observation.ActivityPresentation
}

type ThreadSnapshot struct {
	ID               string          `json:"id"`
	Title            string          `json:"title,omitempty"`
	TitleStatus      string          `json:"title_status,omitempty"`
	TitleSource      string          `json:"title_source,omitempty"`
	TitleUpdatedAt   time.Time       `json:"title_updated_at,omitempty"`
	TitleError       string          `json:"title_error,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	Phase            string          `json:"phase"`
	Status           string          `json:"status"`
	LatestTurnID     string          `json:"latest_turn_id,omitempty"`
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

type ThreadMessage struct {
	Role      session.Role `json:"role"`
	Content   string       `json:"content"`
	TurnID    string       `json:"turn_id,omitempty"`
	CreatedAt time.Time    `json:"created_at"`
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
	Diagnostics        map[string]string
	Metrics            engine.RunMetrics
	CompletionReason   engine.CompletionReason
	ContinuationReason engine.ContinuationReason
	FinishReason       provider.FinishReason
	RawFinishReason    string
	FinishInferred     bool
	ControlSignal      *engine.ControlSignal
	ProviderState      *provider.State
	PendingApprovals   []PendingApproval
}

type CompactResult struct {
	Status        engine.Status
	Err           error
	Diagnostics   map[string]string
	Metrics       engine.RunMetrics
	ProviderState *provider.State
}

type ListPendingApprovalsOptions struct {
	ThreadID string
}

type PendingApprovals struct {
	ThreadID    string            `json:"thread_id"`
	Approvals   []PendingApproval `json:"approvals"`
	GeneratedAt time.Time         `json:"generated_at"`
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
	RunID       string                    `json:"run_id,omitempty"`
	ThreadID    string                    `json:"thread_id,omitempty"`
	TurnID      string                    `json:"turn_id,omitempty"`
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

type Thread struct {
	harness *AgentHarness
	id      string
	mu      sync.Mutex
	active  bool
	phase   string
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
	if options.Artifacts == nil {
		options.Artifacts = artifact.NewMemoryStore()
	}
	if options.TitleGenerator == nil {
		options.TitleGenerator = ProviderTitleGenerator{
			Provider:     options.Provider,
			ProviderName: options.ProviderName,
			Model:        options.Model,
			Reasoning:    options.Reasoning,
		}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &AgentHarness{
		options:         options,
		threads:         map[string]*Thread{},
		subagents:       map[string]*subagentController{},
		subagentUpdates: make(chan struct{}),
		approvals:       map[string]map[string]PendingApproval{},
	}
}

func (h *AgentHarness) StartThread(ctx context.Context, opts StartThreadOptions) (*Thread, error) {
	meta, err := h.options.Repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: opts.ThreadID, CreatedAt: h.now(), UpdatedAt: h.now()})
	if err != nil {
		return nil, err
	}
	thread := h.cacheThread(meta.ID)
	h.emit(HarnessEvent{Type: EventThreadStarted, ThreadID: meta.ID})
	return thread, nil
}

func (h *AgentHarness) EnsureThread(ctx context.Context, opts StartThreadOptions) (ThreadSummary, error) {
	thread, err := h.StartThread(ctx, opts)
	if errors.Is(err, sessiontree.ErrThreadExists) {
		thread = h.cacheThread(strings.TrimSpace(opts.ThreadID))
	} else if err != nil {
		return ThreadSummary{}, err
	}
	return thread.Summary(ctx)
}

func (h *AgentHarness) ResumeThread(ctx context.Context, id string, _ ResumeOptions) (*Thread, error) {
	meta, err := h.options.Repo.Thread(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := h.reconcileTurnAdmission(ctx, meta.ID); err != nil {
		return nil, err
	}
	if err := h.recoverProviderUnsafeInterruptedPath(ctx, meta.ID); err != nil {
		return nil, err
	}
	meta, err = h.options.Repo.Thread(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	if err := h.markInterruptedTurns(ctx, meta); err != nil {
		return nil, err
	}
	thread := h.cacheThread(meta.ID)
	h.emit(HarnessEvent{Type: EventThreadResumed, ThreadID: meta.ID})
	return thread, nil
}

func (h *AgentHarness) markInterruptedTurns(ctx context.Context, meta sessiontree.ThreadMeta) error {
	path, err := h.options.Repo.Path(ctx, meta.ID, meta.LeafID)
	if err != nil {
		return err
	}
	started := map[string]bool{}
	terminal := map[string]bool{}
	for _, entry := range path {
		if entry.Type != sessiontree.EntryTurnMarker || entry.TurnID == "" {
			continue
		}
		if entry.TurnStatus == sessiontree.TurnStarted {
			started[entry.TurnID] = true
		}
		switch entry.TurnStatus {
		case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
			terminal[entry.TurnID] = true
		}
	}
	for turnID := range started {
		if terminal[turnID] {
			continue
		}
		if err := h.finalizeInterruptedTurn(ctx, meta.ID, turnID, path, interruptedTurnFinalization{
			AppendFailure:  true,
			FailureMessage: interruptedTurnFailureMessage,
			Status:         sessiontree.TurnAborted,
			Metadata:       map[string]string{"recoverable": "true"},
		}); err != nil {
			return err
		}
	}
	return nil
}

const interruptedTurnFailureMessage = "turn interrupted during previous process"

func (h *AgentHarness) recoverProviderUnsafeInterruptedPath(ctx context.Context, threadID string) error {
	for {
		meta, err := h.options.Repo.Thread(ctx, threadID)
		if err != nil {
			return err
		}
		path, err := h.options.Repo.Path(ctx, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		repair, ok := providerUnsafeInterruptedRepair(path)
		if !ok {
			return nil
		}
		if err := h.options.Repo.MoveLeaf(ctx, threadID, repair.anchorEntryID); err != nil {
			return err
		}
		if err := h.finalizeInterruptedTurn(ctx, threadID, repair.turnID, path[:repair.anchorIndex+1], interruptedTurnFinalization{
			AppendFailure:  true,
			FailureMessage: interruptedTurnFailureMessage,
			Status:         sessiontree.TurnAborted,
			Metadata:       map[string]string{"recoverable": "true"},
		}); err != nil {
			return err
		}
	}
}

type turnRecoveryInfo struct {
	Started         bool
	Terminal        bool
	RunFailure      bool
	RunFailureError string
}

type interruptedTurnFinalization struct {
	AppendFailure  bool
	FailureMessage string
	Status         sessiontree.TurnMarkerStatus
	Metadata       map[string]string
}

func (h *AgentHarness) reconcileTurnAdmission(ctx context.Context, threadID string) error {
	lease, ok, err := h.activeTurnLease(ctx, threadID)
	if err != nil || !ok || lease.TurnID == "" {
		return err
	}
	if err := h.recoverProviderUnsafeInterruptedPath(ctx, threadID); err != nil {
		return err
	}
	meta, err := h.options.Repo.Thread(ctx, threadID)
	if err != nil {
		return err
	}
	path, err := h.options.Repo.Path(ctx, threadID, meta.LeafID)
	if err != nil {
		return err
	}
	info := recoveryInfoForTurn(path, lease.TurnID)
	if info.Terminal {
		return h.releaseTurnLease(ctx, lease)
	}
	if info.RunFailure {
		status := terminalStatusForRecoveredFailure(info.RunFailureError)
		metadata := map[string]string{"recoverable": "true"}
		if info.RunFailureError != "" {
			metadata["failure_reason"] = info.RunFailureError
		}
		if err := h.finalizeInterruptedTurn(ctx, threadID, lease.TurnID, path, interruptedTurnFinalization{
			Status:   status,
			Metadata: metadata,
		}); err != nil {
			return err
		}
		return h.releaseTurnLease(ctx, lease)
	}
	cutoff := h.now().Add(-staleTurnLeaseTimeout)
	cleared, stale, clearErr := h.clearExpiredTurnLease(ctx, threadID, cutoff)
	if clearErr != nil {
		return clearErr
	}
	if !stale || cleared.TurnID != lease.TurnID {
		return ErrActiveTurn
	}
	return nil
}

func recoveryInfoForTurn(path []sessiontree.Entry, turnID string) turnRecoveryInfo {
	var info turnRecoveryInfo
	for _, entry := range path {
		if entry.TurnID != turnID {
			continue
		}
		switch entry.Type {
		case sessiontree.EntryRunFailure:
			info.RunFailure = true
			info.RunFailureError = entry.Error
		case sessiontree.EntryTurnMarker:
			if entry.TurnStatus == sessiontree.TurnStarted {
				info.Started = true
			}
			if isTerminalTurnMarker(entry.TurnStatus) {
				info.Terminal = true
			}
		}
	}
	return info
}

func isTerminalTurnMarker(status sessiontree.TurnMarkerStatus) bool {
	switch status {
	case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
		return true
	default:
		return false
	}
}

func terminalStatusForRecoveredFailure(message string) sessiontree.TurnMarkerStatus {
	normalized := strings.ToLower(strings.TrimSpace(message))
	if normalized == "" ||
		strings.Contains(normalized, strings.ToLower(context.Canceled.Error())) ||
		strings.Contains(normalized, strings.ToLower(context.DeadlineExceeded.Error())) ||
		strings.Contains(normalized, "interrupted") ||
		strings.Contains(normalized, "runtime restarted") {
		return sessiontree.TurnAborted
	}
	return sessiontree.TurnFailed
}

func (h *AgentHarness) finalizeInterruptedTurn(ctx context.Context, threadID, turnID string, path []sessiontree.Entry, opts interruptedTurnFinalization) error {
	if err := h.closeInterruptedTurnToolCalls(ctx, threadID, turnID, path); err != nil {
		return err
	}
	if opts.AppendFailure {
		message := strings.TrimSpace(opts.FailureMessage)
		if message == "" {
			message = interruptedTurnFailureMessage
		}
		if _, err := sessiontree.AppendFailure(ctx, h.options.Repo, threadID, turnID, message); err != nil {
			return err
		}
	}
	status := opts.Status
	if status == "" {
		status = sessiontree.TurnAborted
	}
	metadata := cloneStringMap(opts.Metadata)
	if len(metadata) == 0 {
		metadata = map[string]string{"recoverable": "true"}
	}
	_, err := sessiontree.AppendTurnMarker(ctx, h.options.Repo, threadID, turnID, status, metadata)
	return err
}

type interruptedPathRepair struct {
	turnID        string
	anchorEntryID string
	anchorIndex   int
}

type interruptedToolCall struct {
	entry sessiontree.Entry
}

func providerUnsafeInterruptedRepair(path []sessiontree.Entry) (interruptedPathRepair, bool) {
	pending := map[string]interruptedToolCall{}
	pendingOrder := []string{}
	for index, entry := range path {
		switch entry.Type {
		case sessiontree.EntryToolCall:
			if entry.Message.Role == session.Assistant && strings.TrimSpace(entry.Message.ToolCallID) != "" {
				callID := strings.TrimSpace(entry.Message.ToolCallID)
				if _, exists := pending[callID]; !exists {
					pendingOrder = append(pendingOrder, callID)
				}
				pending[callID] = interruptedToolCall{entry: entry}
			}
		case sessiontree.EntryToolResult:
			if entry.Message.Role == session.Tool && strings.TrimSpace(entry.Message.ToolCallID) != "" {
				delete(pending, strings.TrimSpace(entry.Message.ToolCallID))
			}
		case sessiontree.EntryRunFailure:
			if entry.TurnID != "" && hasPendingTurn(pending, entry.TurnID) {
				return repairBeforeInterruptedTerminal(path, pending, pendingOrder, entry.TurnID, index)
			}
		case sessiontree.EntryTurnMarker:
			if isInterruptedTurnMarker(entry.TurnStatus) && entry.TurnID != "" && hasPendingTurn(pending, entry.TurnID) {
				return repairBeforeInterruptedTerminal(path, pending, pendingOrder, entry.TurnID, index)
			}
		case sessiontree.EntryUserMessage:
			if len(pending) == 0 {
				continue
			}
			for _, callID := range pendingOrder {
				call, ok := pending[callID]
				if !ok {
					continue
				}
				anchorIndex := index - 1
				if anchorIndex < 0 {
					anchorIndex = 0
				}
				return interruptedPathRepair{
					turnID:        call.entry.TurnID,
					anchorEntryID: path[anchorIndex].ID,
					anchorIndex:   anchorIndex,
				}, true
			}
		}
	}
	return interruptedPathRepair{}, false
}

func repairBeforeInterruptedTerminal(path []sessiontree.Entry, pending map[string]interruptedToolCall, pendingOrder []string, turnID string, terminalIndex int) (interruptedPathRepair, bool) {
	for _, callID := range pendingOrder {
		call, ok := pending[callID]
		if !ok || call.entry.TurnID != turnID {
			continue
		}
		anchorIndex := terminalIndex - 1
		if anchorIndex < 0 {
			anchorIndex = 0
		}
		return interruptedPathRepair{
			turnID:        call.entry.TurnID,
			anchorEntryID: path[anchorIndex].ID,
			anchorIndex:   anchorIndex,
		}, true
	}
	return interruptedPathRepair{}, false
}

func hasPendingTurn(pending map[string]interruptedToolCall, turnID string) bool {
	for _, call := range pending {
		if call.entry.TurnID == turnID {
			return true
		}
	}
	return false
}

func isInterruptedTurnMarker(status sessiontree.TurnMarkerStatus) bool {
	switch status {
	case sessiontree.TurnFailed, sessiontree.TurnAborted:
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
	turnIDs := map[string]string(nil)
	runIDs := map[string]string(nil)
	turnRefs := []ForkedTurnRef(nil)
	if opts.RewriteTurnIdentities {
		var err error
		turnIDs, runIDs, turnRefs, err = h.forkIdentityRewrite(ctx, opts)
		if err != nil {
			return ForkResult{}, err
		}
	}
	meta, err := h.options.Repo.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: opts.SourceThreadID,
		EntryID:        opts.EntryID,
		Position:       opts.Position,
		NewThreadID:    opts.NewThreadID,
		Now:            h.now(),
		TurnIDMap:      turnIDs,
		RunIDMap:       runIDs,
	})
	if err != nil {
		return ForkResult{}, err
	}
	if opts.CloneTerminalSubAgents {
		if err := h.cloneForkedTerminalSubAgents(ctx, opts.SourceThreadID, meta.ID, turnIDs); err != nil {
			return ForkResult{}, err
		}
	}
	thread := h.cacheThread(meta.ID)
	h.emit(HarnessEvent{Type: EventThreadForked, ThreadID: meta.ID, EntryID: meta.ForkedFromEntryID, Metadata: map[string]string{"source_thread_id": opts.SourceThreadID}})
	summary, err := thread.Summary(ctx)
	if err != nil {
		return ForkResult{}, err
	}
	return ForkResult{Thread: thread, Summary: summary, Turns: turnRefs}, nil
}

func (h *AgentHarness) forkIdentityRewrite(ctx context.Context, opts ForkOptions) (map[string]string, map[string]string, []ForkedTurnRef, error) {
	path, err := h.forkSourcePath(ctx, opts)
	if err != nil {
		return nil, nil, nil, err
	}
	turnIDs := map[string]string{}
	runIDs := map[string]string{}
	refsByTurn := map[string]*ForkedTurnRef{}
	order := make([]string, 0)
	for _, entry := range path {
		sourceTurnID := strings.TrimSpace(entry.TurnID)
		if sourceTurnID == "" {
			continue
		}
		destinationTurnID := turnIDs[sourceTurnID]
		if destinationTurnID == "" {
			destinationTurnID = h.nextID("turn")
			turnIDs[sourceTurnID] = destinationTurnID
		}
		ref := refsByTurn[sourceTurnID]
		if ref == nil {
			ref = &ForkedTurnRef{
				SourceTurnID:      sourceTurnID,
				DestinationTurnID: destinationTurnID,
				CreatedAt:         entry.CreatedAt,
			}
			refsByTurn[sourceTurnID] = ref
			order = append(order, sourceTurnID)
		}
		if ref.CreatedAt.IsZero() || (!entry.CreatedAt.IsZero() && entry.CreatedAt.Before(ref.CreatedAt)) {
			ref.CreatedAt = entry.CreatedAt
		}
		sourceRunID := strings.TrimSpace(entry.Metadata["run_id"])
		if sourceRunID == "" {
			continue
		}
		destinationRunID := runIDs[sourceRunID]
		if destinationRunID == "" {
			destinationRunID = h.nextID("run")
			runIDs[sourceRunID] = destinationRunID
		}
		if ref.SourceRunID == "" {
			ref.SourceRunID = sourceRunID
			ref.DestinationRunID = destinationRunID
		}
	}
	refs := make([]ForkedTurnRef, 0, len(order))
	for _, turnID := range order {
		ref := refsByTurn[turnID]
		if ref == nil {
			continue
		}
		refs = append(refs, *ref)
	}
	return turnIDs, runIDs, refs, nil
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

func (h *AgentHarness) cloneForkedTerminalSubAgents(ctx context.Context, sourceParentThreadID string, destinationParentThreadID string, parentTurnIDs map[string]string) error {
	metas, err := h.childThreadMetas(ctx, sourceParentThreadID)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		snapshot, err := h.subAgentSnapshotFromMeta(ctx, meta)
		if err != nil {
			return err
		}
		if !isTerminalSubAgentStatus(snapshot.Status) {
			continue
		}
		childID, err := h.nextSubAgentThreadID(ctx)
		if err != nil {
			return err
		}
		if _, err := h.ForkThreadWithResult(ctx, ForkOptions{
			SourceThreadID:        meta.ID,
			NewThreadID:           childID,
			RewriteTurnIdentities: true,
		}); err != nil {
			return err
		}
		cloned, err := h.options.Repo.Thread(ctx, childID)
		if err != nil {
			return err
		}
		cloned.ParentThreadID = strings.TrimSpace(destinationParentThreadID)
		cloned.ParentTurnID = forkMappedID(meta.ParentTurnID, parentTurnIDs)
		cloned.TaskName = meta.TaskName
		cloned.TaskDescription = meta.TaskDescription
		cloned.AgentPath = meta.AgentPath
		cloned.HostProfileRef = meta.HostProfileRef
		cloned.ForkMode = meta.ForkMode
		cloned.Closed = meta.Closed
		cloned.Status = meta.Status
		cloned.UpdatedAt = h.now()
		if err := h.options.Repo.UpdateThread(ctx, cloned); err != nil {
			return err
		}
	}
	return nil
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

func (h *AgentHarness) nextID(prefix string) string {
	if h.options.NewID != nil {
		return h.options.NewID(prefix)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	return fmt.Sprintf("%s-%d", prefix, h.seq)
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
		h.options.Sink.Emit(event.Sanitize(event.Event{Type: event.Type(ev.Type), RunID: ev.RunID, ThreadID: ev.ThreadID, TurnID: ev.TurnID, Message: ev.Message, Timestamp: ev.Timestamp}))
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
	entries, err := h.options.Repo.Entries(context.Background(), entry.ThreadID)
	if err != nil {
		return 0
	}
	for index, candidate := range entries {
		if candidate.ID == entry.ID {
			return int64(index + 1)
		}
	}
	return 0
}

func (h *AgentHarness) activeTurnLease(ctx context.Context, threadID string) (sessiontree.TurnLease, bool, error) {
	repo, ok := h.options.Repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		return sessiontree.TurnLease{}, false, nil
	}
	return repo.ActiveTurnLease(ctx, threadID)
}

func (h *AgentHarness) clearExpiredTurnLease(ctx context.Context, threadID string, cutoff time.Time) (sessiontree.TurnLease, bool, error) {
	repo, ok := h.options.Repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		return sessiontree.TurnLease{}, false, nil
	}
	return repo.ClearExpiredTurnLease(ctx, threadID, cutoff)
}

func (h *AgentHarness) acquireTurnLease(ctx context.Context, threadID, turnID string) (sessiontree.TurnLease, error) {
	repo, ok := h.options.Repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		return sessiontree.TurnLease{}, nil
	}
	lease := sessiontree.TurnLease{ThreadID: threadID, TurnID: turnID, OwnerID: h.nextID("lease"), CreatedAt: h.now()}
	if err := repo.AcquireTurnLease(ctx, lease); err != nil {
		if errors.Is(err, sessiontree.ErrActiveTurn) {
			return sessiontree.TurnLease{}, ErrActiveTurn
		}
		return sessiontree.TurnLease{}, err
	}
	return lease, nil
}

func (h *AgentHarness) releaseTurnLease(ctx context.Context, lease sessiontree.TurnLease) error {
	if lease.ThreadID == "" {
		return nil
	}
	repo, ok := h.options.Repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		return nil
	}
	return repo.ReleaseTurnLease(ctx, lease)
}

func (t *Thread) ID() string {
	return t.id
}

func (t *Thread) Read(ctx context.Context) (ThreadSnapshot, error) {
	journal, err := t.Journal(ctx)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	lifecycle := sessionlifecycle.Derive(journal.Path, journal.Phase)
	return ThreadSnapshot{
		ID:               journal.Meta.ID,
		Title:            journal.Meta.Title,
		TitleStatus:      string(journal.Meta.TitleStatus),
		TitleSource:      string(journal.Meta.TitleSource),
		TitleUpdatedAt:   journal.Meta.TitleUpdatedAt,
		TitleError:       journal.Meta.TitleError,
		CreatedAt:        journal.Meta.CreatedAt,
		UpdatedAt:        journal.Meta.UpdatedAt,
		Phase:            lifecycle.Phase(),
		Status:           lifecycle.Status(),
		LatestTurnID:     lifecycle.LatestTurnID(),
		WaitingPrompt:    lifecycle.WaitingPrompt(),
		Recoverable:      lifecycle.Recoverable(),
		CanAppendMessage: lifecycle.CanAppendMessage(),
		CanRetry:         retryTarget(journal.Path).Entry.ID != "",
		Messages:         threadMessages(journal.Path),
	}, nil
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
	lifecycle := sessionlifecycle.Derive(path, phase)
	return ThreadSummary{
		ID:               meta.ID,
		Title:            meta.Title,
		TitleStatus:      string(meta.TitleStatus),
		TitleSource:      string(meta.TitleSource),
		TitleUpdatedAt:   meta.TitleUpdatedAt,
		TitleError:       meta.TitleError,
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
	return ThreadJournalSnapshot{
		Meta:    meta,
		Path:    path,
		Entries: entries,
		Context: sessiontree.BuildContext(path, sessiontree.ContextOptions{}),
		Phase:   phase,
	}, nil
}

func threadMessages(path []sessiontree.Entry) []ThreadMessage {
	out := make([]ThreadMessage, 0)
	for _, entry := range path {
		switch entry.Type {
		case sessiontree.EntryUserMessage, sessiontree.EntryAssistantMessage:
			if entry.Message.Content == "" {
				continue
			}
			out = append(out, ThreadMessage{
				Role:      entry.Message.Role,
				Content:   entry.Message.Content,
				TurnID:    entry.TurnID,
				CreatedAt: entry.CreatedAt,
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
	turnID := t.harness.nextID("turn")
	lease, err := t.harness.acquireTurnLease(ctx, t.id, turnID)
	if err != nil {
		return TurnResult{}, err
	}
	defer func() {
		persistCtx, cancel := turnFinalizationContext(ctx)
		defer cancel()
		_ = t.harness.releaseTurnLease(persistCtx, lease)
	}()
	snap, err := t.Journal(ctx)
	if err != nil {
		return TurnResult{}, err
	}
	target := retryTarget(snap.Path)
	if target.Entry.ID == "" {
		return TurnResult{}, ErrNoRetryTarget
	}
	if err := t.harness.options.Repo.MoveLeaf(ctx, t.id, target.Entry.ID); err != nil {
		return TurnResult{}, err
	}
	t.harness.emit(HarnessEvent{Type: EventRetryStarted, ThreadID: t.id, EntryID: target.Entry.ID, Metadata: map[string]string{"reason": opts.Reason, "source": target.Source}})
	return t.runLeased(ctx, "", RunOptions{RunID: t.harness.nextID("run"), TurnID: turnID, Labels: opts.Labels}, &target.Entry)
}

func (t *Thread) CompletePendingTool(ctx context.Context, completion PendingToolCompletion) (TurnResult, error) {
	input, err := pendingToolCompletionInput(completion)
	if err != nil {
		return TurnResult{}, err
	}
	return t.run(ctx, input, RunOptions{RunID: completion.RunID, TurnID: completion.TurnID, Labels: completion.Labels}, nil)
}

func (t *Thread) SettlePendingTool(ctx context.Context, settlement PendingToolSettlement) (SubAgentDetailEvent, error) {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return SubAgentDetailEvent{}, errors.New("thread is not initialized")
	}
	normalized, err := normalizePendingToolSettlement(settlement)
	if err != nil {
		return SubAgentDetailEvent{}, err
	}
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

func (t *Thread) MoveTo(ctx context.Context, entryID string, opts MoveOptions) error {
	release, err := t.enterMutation(ctx, t.harness.nextID("mutation"))
	if err != nil {
		return err
	}
	defer release()
	if err := t.harness.options.Repo.MoveLeaf(ctx, t.id, entryID); err != nil {
		return err
	}
	if opts.Summary != "" {
		entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{ThreadID: t.id, Type: sessiontree.EntryBranchSummary, Summary: opts.Summary}, sessiontree.AppendOptions{})
		if err != nil {
			return err
		}
		entryID = entry.ID
	}
	t.harness.emit(HarnessEvent{Type: EventLeafMoved, ThreadID: t.id, EntryID: entryID})
	return nil
}

func pendingToolCompletionInput(completion PendingToolCompletion) (string, error) {
	handle := strings.TrimSpace(completion.Handle)
	if handle == "" {
		return "", errors.New("pending tool completion requires handle")
	}
	if !pendingToolCompletionPublicToken(handle) {
		return "", errors.New("pending tool completion requires token-safe handle")
	}
	status := completion.Status
	switch status {
	case PendingToolCompleted, PendingToolFailed, PendingToolCanceled:
	default:
		return "", fmt.Errorf("pending tool completion returned invalid status %q", status)
	}
	summary := strings.TrimSpace(completion.Summary)
	if summary == "" {
		return "", errors.New("pending tool completion requires summary")
	}
	lines := []string{
		"<pending_tool_completion>",
		"<status>" + string(status) + "</status>",
		"<summary>" + html.EscapeString(summary) + "</summary>",
		"<handle>" + html.EscapeString(handle) + "</handle>",
	}
	if toolName := strings.TrimSpace(completion.ToolName); toolName != "" {
		lines = append(lines, "<tool_name>"+html.EscapeString(toolName)+"</tool_name>")
	}
	if toolCallID := strings.TrimSpace(completion.ToolCallID); toolCallID != "" {
		lines = append(lines, "<tool_call_id>"+html.EscapeString(toolCallID)+"</tool_call_id>")
	}
	if runID := strings.TrimSpace(completion.RunID); runID != "" {
		lines = append(lines, "<run_id>"+html.EscapeString(runID)+"</run_id>")
	}
	if output := strings.TrimSpace(completion.Output); output != "" {
		lines = append(lines, "<output>", html.EscapeString(output), "</output>")
	}
	lines = append(lines, "</pending_tool_completion>")
	return strings.Join(lines, "\n"), nil
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
		if pendingHandleFromSessionActivity(entry.Message.Activity) == settlement.Handle {
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
		strings.TrimSpace(entry.Metadata[pendingToolSettlementHandleKey]) == settlement.Handle
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
	runID := t.harness.nextID("compact")
	release, err := t.enterMutation(ctx, runID)
	if err != nil {
		return CompactResult{}, err
	}
	defer release()
	snap, err := t.Journal(ctx)
	if err != nil {
		return CompactResult{Status: engine.Failed, Err: err, Diagnostics: map[string]string{"diagnostic": "snapshot_error"}}, err
	}
	history := sessiontree.BuildContext(snap.Path, sessiontree.ContextOptions{})
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
	eng, err := engine.New(engine.Config{
		Provider:     t.harness.options.Provider,
		Tools:        t.harness.options.Tools,
		Prompt:       t.harness.options.PromptStore,
		SystemPrompt: t.harness.options.SystemPrompt,
		Approver:     t.harness.options.Approver,
		StopHook:     t.harness.options.StopHook,
		Compactor:    &durableCompactionManager{thread: t},
		Artifacts:    t.harness.options.Artifacts,
		Options:      engineOptions,
	})
	if err != nil {
		return CompactResult{Status: engine.Failed, Err: err, Diagnostics: map[string]string{"diagnostic": "engine_config_error"}}, err
	}
	downstream := t.harness.options.Sink
	if opts.Sink != nil {
		downstream = opts.Sink
	}
	projection := &turnProjection{thread: t, ctx: ctx, runID: runID, downstream: downstream}
	eng.SetSink(projection)
	result := eng.CompactContext(ctx, engine.RunInput{
		RunID:                 runID,
		ThreadID:              t.id,
		TraceID:               runID,
		PromptScopeID:         t.id,
		Labels:                opts.Labels,
		PreviousProviderState: provider.CloneState(opts.PreviousProviderState),
		History:               history,
	}, engine.ManualCompactionRequest{RequestID: requestID, Source: strings.TrimSpace(opts.Source)})
	persistCtx, cancelPersist := turnFinalizationContext(ctx)
	defer cancelPersist()
	projection.ctx = persistCtx
	if projection.err != nil {
		return CompactResult{Status: engine.Failed, Err: projection.err, Metrics: result.Metrics, Diagnostics: map[string]string{"diagnostic": "projection_error"}}, projection.err
	}
	if err := projection.Flush(); err != nil {
		return CompactResult{Status: engine.Failed, Err: err, Metrics: result.Metrics, Diagnostics: map[string]string{"diagnostic": "projection_flush_error"}}, err
	}
	return CompactResult{
		Status:        result.Status,
		Err:           result.Err,
		Metrics:       result.Metrics,
		ProviderState: provider.CloneState(result.ProviderState),
	}, result.Err
}

func (t *Thread) run(ctx context.Context, input string, opts RunOptions, retryUser *sessiontree.Entry) (TurnResult, error) {
	if strings.TrimSpace(input) == "" && retryUser == nil {
		return TurnResult{}, errors.New("input is required")
	}
	if err := t.enterTurn(); err != nil {
		return TurnResult{}, err
	}
	defer t.leaveTurn()
	return t.runEntered(ctx, input, opts, retryUser)
}

func (t *Thread) runEntered(ctx context.Context, input string, opts RunOptions, retryUser *sessiontree.Entry) (TurnResult, error) {
	turnID := opts.TurnID
	if turnID == "" {
		turnID = t.harness.nextID("turn")
	}
	lease, err := t.harness.acquireTurnLease(ctx, t.id, turnID)
	if err != nil {
		return TurnResult{}, err
	}
	defer func() {
		persistCtx, cancel := turnFinalizationContext(ctx)
		defer cancel()
		_ = t.harness.releaseTurnLease(persistCtx, lease)
	}()
	opts.TurnID = turnID
	return t.runLeased(ctx, input, opts, retryUser)
}

func (t *Thread) runLeased(ctx context.Context, input string, opts RunOptions, retryUser *sessiontree.Entry) (TurnResult, error) {
	turnID := opts.TurnID
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = t.harness.nextID("run")
	}
	if entry, err := sessiontree.AppendTurnMarker(ctx, t.harness.options.Repo, t.id, turnID, sessiontree.TurnStarted, map[string]string{"run_id": runID}); err != nil {
		return TurnResult{}, err
	} else {
		t.harness.emitEntryCommitted(entry, runID)
	}
	t.harness.emit(HarnessEvent{Type: EventTurnStarted, RunID: runID, ThreadID: t.id, TurnID: turnID})
	if retryUser == nil {
		entry, err := sessiontree.AppendMessage(ctx, t.harness.options.Repo, t.id, turnID, session.Message{Role: session.User, Content: input})
		if err != nil {
			persistCtx, cancelPersist := turnFinalizationContext(ctx)
			defer cancelPersist()
			return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "append_user_error")
		}
		t.harness.emitEntryCommitted(entry, runID)
		t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID})
	}
	snap, err := t.Journal(ctx)
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "snapshot_error")
	}
	history := sessiontree.BuildContext(snap.Path, sessiontree.ContextOptions{})
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
	if err := t.appendContextPolicyEvent(ctx, turnID, runID, engineOptions.ProviderName, engineOptions.Model, engineOptions.ContextPolicy); err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "append_context_policy_error")
	}
	eng, err := engine.New(engine.Config{
		Provider:     t.harness.options.Provider,
		Tools:        t.harness.options.Tools,
		Prompt:       t.harness.options.PromptStore,
		SystemPrompt: t.harness.options.SystemPrompt,
		Approver:     t.harness.options.Approver,
		StopHook:     t.harness.options.StopHook,
		Compactor:    &durableCompactionManager{thread: t, turnID: turnID},
		Artifacts:    t.harness.options.Artifacts,
		Options:      engineOptions,
	})
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, engine.Failed, err, "engine_config_error")
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
	persistCtx, cancelPersist := turnFinalizationContext(ctx)
	defer cancelPersist()
	projection.ctx = persistCtx
	if projection.err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(projection.err), projection.err, "projection_error")
	}
	if err := projection.FlushForTurnStatus(result.Status, result.Err); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "projection_flush_error")
	}
	current, err := t.Journal(persistCtx)
	if err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "snapshot_error")
	}
	if err := t.appendDelta(persistCtx, turnID, runID, history, result.Messages, current.Path); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "append_delta_error")
	}
	status := markerForStatus(result.Status)
	savePointMetadata := markerMetadata(runID, result)
	savePointMetadata["reason"] = "run_result"
	if entry, err := sessiontree.AppendTurnMarker(persistCtx, t.harness.options.Repo, t.id, turnID, sessiontree.TurnSavePoint, savePointMetadata); err != nil {
		return TurnResult{}, err
	} else {
		t.harness.emitEntryCommitted(entry, runID)
	}
	if result.Err != nil {
		if entry, err := sessiontree.AppendFailure(persistCtx, t.harness.options.Repo, t.id, turnID, result.Err.Error()); err != nil {
			return TurnResult{}, err
		} else {
			t.harness.emitEntryCommitted(entry, runID)
		}
	}
	terminalMetadata := markerMetadata(runID, result)
	mergeTerminalMetadata(terminalMetadata, opts.TerminalMetadata)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		mergeTerminalMetadata(terminalMetadata, opts.DeadlineMetadata)
	}
	if result.Err != nil {
		terminalMetadata["failure_reason"] = result.Err.Error()
	}
	if result.Status == engine.Waiting {
		terminalMetadata["interrupt_reason"] = "ask_user"
	}
	if entry, err := sessiontree.AppendTurnMarker(persistCtx, t.harness.options.Repo, t.id, turnID, status, terminalMetadata); err != nil {
		var committed sessiontree.AppendCommittedError
		if errors.As(err, &committed) {
			return t.turnResultFromEngine(turnID, runID, result, map[string]string{"terminal_persistence_error": err.Error()}), result.Err
		}
		return TurnResult{}, err
	} else {
		t.harness.emitEntryCommitted(entry, runID)
	}
	eventType := EventTurnCompleted
	if result.Status == engine.Failed {
		eventType = EventTurnFailed
	}
	if result.Status == engine.Cancelled {
		eventType = EventTurnAborted
	}
	t.harness.emit(HarnessEvent{Type: eventType, RunID: runID, ThreadID: t.id, TurnID: turnID, Status: string(result.Status), Message: result.Output})
	if result.Err == nil && (result.Status == engine.Completed || result.Status == engine.Waiting) {
		if err := t.ensureThreadTitle(persistCtx, turnID); err != nil {
			t.harness.emit(HarnessEvent{Type: EventTitleFailed, ThreadID: t.id, TurnID: turnID, Message: err.Error()})
		}
	}
	return t.turnResultFromEngine(turnID, runID, result, nil), result.Err
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

func (t *Thread) ensureThreadTitle(ctx context.Context, turnID string) error {
	generator := t.harness.options.TitleGenerator
	if generator == nil {
		return nil
	}
	meta, err := t.harness.options.Repo.Thread(ctx, t.id)
	if err != nil {
		return err
	}
	if strings.TrimSpace(meta.Title) != "" {
		return nil
	}
	path, err := t.harness.options.Repo.Path(ctx, t.id, meta.LeafID)
	if err != nil {
		return err
	}
	messages := sessiontree.BuildContext(path, sessiontree.ContextOptions{})
	result, err := generator.GenerateTitle(ctx, TitleRequest{ThreadID: t.id, TurnID: turnID, Messages: session.CloneMessages(messages)})
	now := t.harness.now()
	if err != nil {
		meta.Title = ""
		meta.TitleStatus = sessiontree.ThreadTitleFailed
		meta.TitleSource = ""
		meta.TitleUpdatedAt = now
		meta.TitleError = err.Error()
		if updateErr := updateThreadTitle(ctx, t.harness.options.Repo, meta); updateErr != nil {
			return updateErr
		}
		return err
	}
	title := normalizeThreadTitle(result.Title, defaultThreadTitleMaxRunes)
	if title == "" {
		err = errors.New("thread title is empty after normalization")
		meta.Title = ""
		meta.TitleStatus = sessiontree.ThreadTitleFailed
		meta.TitleSource = ""
		meta.TitleUpdatedAt = now
		meta.TitleError = err.Error()
		if updateErr := updateThreadTitle(ctx, t.harness.options.Repo, meta); updateErr != nil {
			return updateErr
		}
		return err
	}
	source := result.Source
	if source == "" {
		source = sessiontree.ThreadTitleSourceProvider
	}
	meta.Title = title
	meta.TitleStatus = sessiontree.ThreadTitleReady
	meta.TitleSource = source
	meta.TitleUpdatedAt = now
	meta.TitleError = ""
	if err := updateThreadTitle(ctx, t.harness.options.Repo, meta); err != nil {
		return err
	}
	t.harness.emit(HarnessEvent{Type: EventTitleUpdated, ThreadID: t.id, TurnID: turnID, Message: title, Metadata: map[string]string{"source": string(source)}})
	return nil
}

func updateThreadTitle(ctx context.Context, repo sessiontree.Repo, meta sessiontree.ThreadMeta) error {
	current, err := repo.Thread(ctx, meta.ID)
	if err != nil {
		return err
	}
	meta.LeafID = current.LeafID
	meta.ParentThreadID = current.ParentThreadID
	meta.ParentTurnID = current.ParentTurnID
	meta.ForkedFromThreadID = current.ForkedFromThreadID
	meta.ForkedFromEntryID = current.ForkedFromEntryID
	meta.TaskName = current.TaskName
	meta.TaskDescription = current.TaskDescription
	meta.AgentPath = current.AgentPath
	meta.HostProfileRef = current.HostProfileRef
	meta.Closed = current.Closed
	meta.Archived = current.Archived
	meta.CreatedAt = current.CreatedAt
	meta.UpdatedAt = current.UpdatedAt
	return repo.UpdateThread(ctx, meta)
}

func (t *Thread) finalizeFailedTurn(ctx context.Context, turnID, runID string, status engine.Status, err error, diagnostic string) (TurnResult, error) {
	if status == "" {
		status = statusForError(err)
	}
	result := engine.Result{Status: status, Err: err}
	if err != nil {
		if entry, appendErr := sessiontree.AppendFailure(ctx, t.harness.options.Repo, t.id, turnID, err.Error()); appendErr != nil {
			return TurnResult{}, appendErr
		} else {
			t.harness.emitEntryCommitted(entry, runID)
		}
	}
	metadata := markerMetadata(runID, result)
	if err != nil {
		metadata["failure_reason"] = err.Error()
	}
	if diagnostic != "" {
		metadata["diagnostic"] = diagnostic
	}
	if entry, appendErr := sessiontree.AppendTurnMarker(ctx, t.harness.options.Repo, t.id, turnID, markerForStatus(status), metadata); appendErr != nil {
		var committed sessiontree.AppendCommittedError
		if errors.As(appendErr, &committed) {
			return t.turnResultFromEngine(turnID, runID, result, map[string]string{"terminal_persistence_error": appendErr.Error(), "diagnostic": diagnostic}), err
		}
		return TurnResult{}, appendErr
	} else {
		t.harness.emitEntryCommitted(entry, runID)
	}
	eventType := EventTurnFailed
	if status == engine.Cancelled {
		eventType = EventTurnAborted
	}
	t.harness.emit(HarnessEvent{Type: eventType, RunID: runID, ThreadID: t.id, TurnID: turnID, Status: string(status)})
	return t.turnResultFromEngine(turnID, runID, result, map[string]string{"diagnostic": diagnostic}), err
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
	if opts.PreviousProviderState != nil {
		dst.PreviousProviderState = provider.CloneState(opts.PreviousProviderState)
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
	if opts.PreviousProviderState != nil {
		dst.PreviousProviderState = provider.CloneState(opts.PreviousProviderState)
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
		ProviderState:      provider.CloneState(result.ProviderState),
	}
}

func (t *Thread) turnResultFromEngine(turnID string, runID string, result engine.Result, diagnostics map[string]string) TurnResult {
	out := turnResultFromEngine(turnID, runID, result, diagnostics)
	if t != nil && t.harness != nil {
		out.PendingApprovals = t.harness.snapshotPendingApprovals(t.id)
	}
	return out
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

func (t *Thread) enterMutation(ctx context.Context, turnID string) (func(), error) {
	if err := t.enterTurn(); err != nil {
		return nil, err
	}
	lease, err := t.harness.acquireTurnLease(ctx, t.id, turnID)
	if err != nil {
		t.leaveTurn()
		return nil, err
	}
	return func() {
		persistCtx, cancel := turnFinalizationContext(ctx)
		defer cancel()
		_ = t.harness.releaseTurnLease(persistCtx, lease)
		t.leaveTurn()
	}, nil
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
	defer t.mu.Unlock()
	t.active = false
	t.phase = threadPhaseIdle
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
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   turnID,
		Type:     sessiontree.EntryCustom,
		Message:  session.Message{Activity: sessionActivityPresentation(sanitizeActivityPresentation(ev.Activity))},
		Metadata: metadata,
	}, sessiontree.AppendOptions{})
	if err != nil {
		return err
	}
	t.harness.emitEntryCommitted(entry, runID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: string(ev.Type)})
	return nil
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
	status := pendingToolSettlementActivityStatus(settlement.Status)
	metadata := map[string]string{
		subAgentDetailKindKey:           pendingToolSettlementEntryKind,
		subAgentDetailTypeKey:           pendingToolSettlementEntryKind,
		pendingToolSettlementStateKey:   string(settlement.Status),
		pendingToolSettlementToolIDKey:  settlement.ToolCallID,
		pendingToolSettlementNameKey:    settlement.ToolName,
		pendingToolSettlementHandleKey:  settlement.Handle,
		pendingToolSettlementRunIDKey:   settlement.RunID,
		pendingToolSettlementSummaryKey: settlement.Summary,
	}
	if status == string(observation.ActivityStatusCanceled) {
		metadata["tool_result_status"] = string(observation.ActivityStatusCanceled)
	}
	activity := settlement.Activity
	if activity == nil {
		activity = &observation.ActivityPresentation{Label: settlement.Summary}
	}
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   settlement.TurnID,
		Type:     sessiontree.EntryCustom,
		Message: session.Message{
			Content:    settlement.Output,
			ToolCallID: settlement.ToolCallID,
			ToolName:   settlement.ToolName,
			ToolResult: &session.ToolResultView{Status: status},
			Activity:   sessionActivityPresentation(sanitizeActivityPresentation(activity)),
		},
		Metadata: metadata,
	}, sessiontree.AppendOptions{})
	if err != nil {
		return sessiontree.Entry{}, err
	}
	t.harness.emitEntryCommitted(entry, settlement.RunID)
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, RunID: settlement.RunID, ThreadID: t.id, TurnID: settlement.TurnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: pendingToolSettlementEntryKind})
	return entry, nil
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
	compact, ok := subAgentContextCompactionFromEvent(ev)
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

func subAgentContextCompactionFromEvent(ev event.Event) (SubAgentDetailContextCompaction, bool) {
	if ev.Type != event.ContextCompact {
		return SubAgentDetailContextCompaction{}, false
	}
	meta, ok := ev.Metadata.(map[string]any)
	if !ok {
		return SubAgentDetailContextCompaction{}, false
	}
	phase := stringFromEventMetadata(meta["phase"])
	status := subAgentDetailContextCompactionStatus(phase)
	if phase == "" || status == "" {
		return SubAgentDetailContextCompaction{}, false
	}
	return SubAgentDetailContextCompaction{
		RunID:               ev.RunID,
		ThreadID:            ev.ThreadID,
		TurnID:              ev.TurnID,
		Step:                ev.Step,
		OperationID:         stringFromEventMetadata(meta["operation_id"]),
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
	}, true
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
	previousSummary := previous.Summary
	if req.PreviousSummary != "" {
		previousSummary = req.PreviousSummary
	}
	previousID := previous.CompactionID
	if req.PreviousCompactionID != "" {
		previousID = req.PreviousCompactionID
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
	prep, err := compaction.Prepare(ctx, compaction.Request{
		CompactionID:         compactionID,
		PreviousCompactionID: previousID,
		PreviousSummary:      previousSummary,
		History:              req.History,
		Policy:               req.Policy,
		Trigger:              req.Trigger,
		Reason:               req.Reason,
		Phase:                req.Phase,
		Step:                 req.Step,
		Details:              req.Details,
		Now:                  m.thread.harness.now(),
	}, generator)
	if err != nil {
		return compaction.Result{}, nil, err
	}
	if previous.CompactionGeneration > 0 {
		prep.Result.Details["compaction_generation"] = strconv.Itoa(previous.CompactionGeneration + 1)
	}
	if prep.Result.PreviousCompactionID == "" {
		prep.Result.PreviousCompactionID = previousID
	}
	return prep.Result, prep.ActiveMessages, nil
}

func (m *durableCompactionManager) CommitCompaction(ctx context.Context, req engine.CompactionCommitRequest) (compaction.Result, []session.Message, error) {
	if m == nil || m.thread == nil {
		return compaction.Result{}, nil, errors.New("durable compaction manager requires thread")
	}
	entry, err := sessiontree.AppendCompaction(ctx, m.thread.harness.options.Repo, m.thread.id, m.turnID, req.Result)
	if err != nil {
		var committed sessiontree.AppendCommittedError
		if !errors.As(err, &committed) {
			return compaction.Result{}, nil, err
		}
	}
	m.thread.harness.emitEntryCommitted(entry, req.RunID)
	result := req.Result
	result.Phase = req.Phase
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
					return retryTargetResult{Entry: path[i-1], Source: "save_point"}
				}
			}
		}
	}
	for i := len(path) - 1; i >= 0; i-- {
		if path[i].Type != sessiontree.EntryUserMessage {
			continue
		}
		return retryTargetResult{Entry: path[i], Source: "user"}
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
	err              error
}

type pendingToolMessage struct {
	message    session.Message
	observedAt time.Time
}

func (p *turnProjection) Emit(ev event.Event) {
	if isApprovalEvent(ev.Type) {
		p.thread.harness.updatePendingApproval(ev)
	}
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
			message:    session.Message{Role: session.Tool, Content: ev.Result, ToolCallID: ev.ToolID, ToolName: ev.ToolName, ToolResult: toolResultViewFromEvent(ev), Activity: sessionActivityPresentation(sanitizeActivityPresentation(ev.Activity))},
			observedAt: ev.Timestamp,
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
		if err := p.thread.appendMessageAt(p.ctx, p.turnID, p.runID, result.message, result.observedAt); err != nil {
			return err
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
			if ref.ID != "" || ref.SafeLabel != "" || ref.URL != "" {
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
		URL:       in.URL,
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
