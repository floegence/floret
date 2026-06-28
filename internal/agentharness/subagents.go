package agentharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/observation"
)

var (
	ErrSubAgentNotFound = errors.New("subagent not found")
	ErrSubAgentClosed   = errors.New("subagent is closed")
)

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

const (
	DefaultSubAgentWaitTimeout = 5 * time.Minute
	MaxSubAgentWaitTimeout     = 20 * time.Minute
	DefaultSubAgentRunTimeout  = 20 * time.Minute
	MaxSubAgentRunTimeout      = 20 * time.Minute
	DefaultSubAgentDetailLimit = 200
	MaxSubAgentDetailLimit     = 500

	DefaultThreadDetailEventLimit = DefaultSubAgentDetailLimit
	MaxThreadDetailEventLimit     = MaxSubAgentDetailLimit
)

const (
	subAgentInputEntryKind      = "subagent_input"
	subAgentInputStatePending   = "pending"
	subAgentInputStateConsumed  = "consumed"
	subAgentInputStateCancelled = "cancelled"

	subAgentInputKindKey        = "kind"
	subAgentInputStateKey       = "state"
	subAgentInputEntryIDKey     = "input_entry_id"
	subAgentInputMessageHashKey = "message_hash"
	subAgentInputInterruptKey   = "interrupt"
	subAgentInputTurnIDKey      = "turn_id"
	subAgentInputLabelHostKey   = "labels_host_json"
	subAgentInputLabelCorrKey   = "labels_correlation_json"

	subAgentApprovalEntryKind = "subagent_approval"
	subAgentDetailKindKey     = "kind"
	subAgentDetailTypeKey     = "type"
	subAgentApprovalStateKey  = "state"
	subAgentApprovalToolIDKey = "tool_id"
	subAgentApprovalNameKey   = "tool_name"
	subAgentApprovalKindKey   = "tool_kind"
	subAgentApprovalArgsKey   = "args_hash"
	subAgentApprovalReasonKey = "reason"

	subAgentLifecycleEntryKind = "subagent_lifecycle"
	subAgentLifecycleActionKey = "action"
	subAgentLifecycleReasonKey = "reason"

	subAgentTerminalReasonKey = "terminal_reason"
	subAgentRunTimeoutReason  = "child_run_timeout"
	subAgentDetailRawOmitted  = "raw_omitted"
)

type SubAgentForkMode string

const (
	SubAgentForkNone     SubAgentForkMode = "none"
	SubAgentForkFullPath SubAgentForkMode = "full_path"
)

type SpawnSubAgentOptions struct {
	ParentThreadID string
	ParentTurnID   string
	ThreadID       string
	TaskName       string
	Message        string
	HostProfileRef string
	ForkMode       SubAgentForkMode
	Labels         engine.RunLabels
}

type SendSubAgentInputOptions struct {
	ParentThreadID string
	ChildThreadID  string
	Message        string
	Interrupt      bool
	Labels         engine.RunLabels
}

type WaitSubAgentsOptions struct {
	ParentThreadID string
	ChildThreadIDs []string
	Timeout        time.Duration
}

type CloseSubAgentOptions struct {
	ParentThreadID string
	ChildThreadID  string
	Reason         string
}

type CloseSubAgentsOptions struct {
	ParentThreadID string
	Reason         string
}

type CloseSubAgentsResult struct {
	Snapshots []SubAgentSnapshot `json:"snapshots"`
	Closed    int                `json:"closed,omitempty"`
}

type ReadSubAgentDetailOptions struct {
	ParentThreadID string
	ChildThreadID  string
	AfterOrdinal   int64
	Limit          int
	IncludeRaw     bool
}

type ListThreadDetailEventsOptions struct {
	ThreadID     string
	AfterOrdinal int64
	Limit        int
	IncludeRaw   bool
}

type SubAgentSnapshot struct {
	ThreadID       string           `json:"thread_id"`
	Path           string           `json:"path"`
	TaskName       string           `json:"task_name"`
	ParentThreadID string           `json:"parent_thread_id"`
	ParentTurnID   string           `json:"parent_turn_id,omitempty"`
	HostProfileRef string           `json:"host_profile_ref,omitempty"`
	ForkMode       SubAgentForkMode `json:"fork_mode,omitempty"`
	Status         SubAgentStatus   `json:"status"`
	LatestTurnID   string           `json:"latest_turn_id,omitempty"`
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

type SubAgentDetail struct {
	Snapshot     SubAgentSnapshot      `json:"snapshot"`
	Events       []SubAgentDetailEvent `json:"events"`
	NextOrdinal  int64                 `json:"next_ordinal,omitempty"`
	HasMore      bool                  `json:"has_more,omitempty"`
	RetainedFrom int64                 `json:"retained_from,omitempty"`
	GeneratedAt  time.Time             `json:"generated_at"`
}

type ThreadDetailEvents struct {
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
	ThreadID  string                  `json:"thread_id"`
	TurnID    string                  `json:"turn_id,omitempty"`
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
	CallID        string        `json:"call_id,omitempty"`
	ToolName      string        `json:"tool_name,omitempty"`
	Status        string        `json:"status,omitempty"`
	Preview       string        `json:"preview,omitempty"`
	Content       string        `json:"content,omitempty"`
	Truncated     bool          `json:"truncated,omitempty"`
	OriginalBytes int           `json:"original_bytes,omitempty"`
	VisibleBytes  int           `json:"visible_bytes,omitempty"`
	OriginalLines int           `json:"original_lines,omitempty"`
	VisibleLines  int           `json:"visible_lines,omitempty"`
	Strategy      string        `json:"strategy,omitempty"`
	ContentSHA256 string        `json:"content_sha256,omitempty"`
	FullOutput    *artifact.Ref `json:"full_output,omitempty"`
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

type subagentInput struct {
	entryID string
	message string
	labels  engine.RunLabels
}

type subagentController struct {
	parentThreadID string
	threadID       string
	path           string
	taskName       string
	thread         *Thread

	mu      sync.Mutex
	queue   []subagentInput
	running bool
	turnID  string
	closed  bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func (h *AgentHarness) SpawnSubAgent(ctx context.Context, opts SpawnSubAgentOptions) (SubAgentSnapshot, error) {
	if h == nil {
		return SubAgentSnapshot{}, errors.New("agent harness is nil")
	}
	h.subagentSpawnMu.Lock()
	defer h.subagentSpawnMu.Unlock()
	parentID := strings.TrimSpace(opts.ParentThreadID)
	if parentID == "" {
		return SubAgentSnapshot{}, errors.New("parent thread id is required")
	}
	message := strings.TrimSpace(opts.Message)
	if message == "" {
		return SubAgentSnapshot{}, errors.New("subagent message is required")
	}
	taskName, err := normalizeSubAgentTaskName(opts.TaskName)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	parentMeta, err := h.options.Repo.Thread(ctx, parentID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	path := childAgentPath(parentMeta.AgentPath, taskName)
	childID := strings.TrimSpace(opts.ThreadID)
	if childID == "" {
		childID, err = h.nextSubAgentThreadID(ctx)
		if err != nil {
			return SubAgentSnapshot{}, err
		}
	}
	now := h.now()
	forkMode := opts.ForkMode
	if forkMode == "" {
		forkMode = SubAgentForkFullPath
	}
	var thread *Thread
	switch forkMode {
	case SubAgentForkNone:
		meta := sessiontree.ThreadMeta{
			ID:             childID,
			ParentThreadID: parentID,
			ParentTurnID:   strings.TrimSpace(opts.ParentTurnID),
			TaskName:       taskName,
			AgentPath:      path,
			HostProfileRef: strings.TrimSpace(opts.HostProfileRef),
			ForkMode:       string(forkMode),
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if _, err := h.options.Repo.CreateThread(ctx, meta); err != nil {
			return SubAgentSnapshot{}, err
		}
		thread = h.cacheThread(childID)
		h.emit(HarnessEvent{Type: EventThreadStarted, ThreadID: childID, ParentID: parentID})
	case SubAgentForkFullPath:
		thread, err = h.ForkThread(ctx, ForkOptions{
			SourceThreadID: parentID,
			NewThreadID:    childID,
		})
		if err != nil {
			return SubAgentSnapshot{}, err
		}
		meta, err := h.options.Repo.Thread(ctx, childID)
		if err != nil {
			return SubAgentSnapshot{}, err
		}
		meta.ParentThreadID = parentID
		meta.ParentTurnID = strings.TrimSpace(opts.ParentTurnID)
		meta.TaskName = taskName
		meta.AgentPath = path
		meta.HostProfileRef = strings.TrimSpace(opts.HostProfileRef)
		meta.ForkMode = string(forkMode)
		meta.Status = ""
		meta.UpdatedAt = now
		if err := h.options.Repo.UpdateThread(ctx, meta); err != nil {
			return SubAgentSnapshot{}, err
		}
	default:
		return SubAgentSnapshot{}, fmt.Errorf("unsupported subagent fork mode %q", forkMode)
	}
	ctrl, err := h.ensureSubAgentController(ctx, sessiontree.ThreadMeta{
		ID:             childID,
		ParentThreadID: parentID,
		TaskName:       taskName,
		AgentPath:      path,
	}, thread)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	h.emit(HarnessEvent{
		Type:     EventSubAgentSpawned,
		ThreadID: parentID,
		Message:  message,
		Metadata: map[string]string{
			"subagent_thread_id": childID,
			"subagent_path":      path,
			"task_name":          taskName,
			"host_profile_ref":   strings.TrimSpace(opts.HostProfileRef),
		},
	})
	input, err := h.appendSubAgentInput(ctx, childID, message, opts.Labels, false)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if err := h.enqueueSubAgentInput(ctx, ctrl, input, false); err != nil {
		if cancelErr := h.appendSubAgentInputState(ctx, childID, input.entryID, subAgentInputStateCancelled, nil); cancelErr != nil {
			return SubAgentSnapshot{}, errors.Join(err, cancelErr)
		}
		return SubAgentSnapshot{}, err
	}
	return h.subAgentSnapshot(ctx, childID)
}

func (h *AgentHarness) SendSubAgentInput(ctx context.Context, opts SendSubAgentInputOptions) (SubAgentSnapshot, error) {
	meta, err := h.resolveSubAgentMeta(ctx, opts.ParentThreadID, opts.ChildThreadID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if meta.Closed {
		return SubAgentSnapshot{}, ErrSubAgentClosed
	}
	message := strings.TrimSpace(opts.Message)
	if message == "" {
		return SubAgentSnapshot{}, errors.New("subagent message is required")
	}
	thread := h.cacheThread(meta.ID)
	ctrl, err := h.ensureSubAgentController(ctx, meta, thread)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	h.emit(HarnessEvent{
		Type:     EventSubAgentInput,
		ThreadID: strings.TrimSpace(opts.ParentThreadID),
		Message:  message,
		Metadata: map[string]string{
			"subagent_thread_id": meta.ID,
			"subagent_path":      meta.AgentPath,
			"interrupt":          fmt.Sprintf("%t", opts.Interrupt),
		},
	})
	input, err := h.appendSubAgentInput(ctx, meta.ID, message, opts.Labels, opts.Interrupt)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if err := h.enqueueSubAgentInput(ctx, ctrl, input, opts.Interrupt); err != nil {
		if cancelErr := h.appendSubAgentInputState(ctx, meta.ID, input.entryID, subAgentInputStateCancelled, nil); cancelErr != nil {
			return SubAgentSnapshot{}, errors.Join(err, cancelErr)
		}
		return SubAgentSnapshot{}, err
	}
	return h.subAgentSnapshot(ctx, meta.ID)
}

func (h *AgentHarness) WaitSubAgents(ctx context.Context, opts WaitSubAgentsOptions) (WaitSubAgentsResult, error) {
	if h == nil {
		return WaitSubAgentsResult{}, errors.New("agent harness is nil")
	}
	targets := cleanSubAgentTargets(opts.ChildThreadIDs)
	if len(targets) == 0 {
		return WaitSubAgentsResult{}, errors.New("subagent wait requires at least one target")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultSubAgentWaitTimeout
	}
	if timeout > MaxSubAgentWaitTimeout {
		timeout = MaxSubAgentWaitTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		snapshots, err := h.snapshotsForTargets(ctx, opts.ParentThreadID, targets)
		if err != nil {
			return WaitSubAgentsResult{}, err
		}
		if allSubAgentsSettledForWait(snapshots) {
			return WaitSubAgentsResult{Snapshots: snapshots}, nil
		}
		select {
		case <-ctx.Done():
			return WaitSubAgentsResult{}, ctx.Err()
		case <-timer.C:
			return WaitSubAgentsResult{Snapshots: snapshots, TimedOut: true}, nil
		case <-h.subAgentUpdateChannel():
		}
	}
}

func (h *AgentHarness) ReadSubAgentDetail(ctx context.Context, opts ReadSubAgentDetailOptions) (SubAgentDetail, error) {
	if h == nil {
		return SubAgentDetail{}, errors.New("agent harness is nil")
	}
	if opts.Limit < 0 {
		return SubAgentDetail{}, errors.New("subagent detail limit must be non-negative")
	}
	limit := opts.Limit
	if limit == 0 {
		limit = DefaultSubAgentDetailLimit
	}
	if limit > MaxSubAgentDetailLimit {
		limit = MaxSubAgentDetailLimit
	}
	meta, err := h.resolveSubAgentMeta(ctx, opts.ParentThreadID, opts.ChildThreadID)
	if err != nil {
		return SubAgentDetail{}, err
	}
	snapshot, err := h.subAgentSnapshotFromMeta(ctx, meta)
	if err != nil {
		return SubAgentDetail{}, err
	}
	thread := h.cacheThread(meta.ID)
	journal, err := thread.Journal(ctx)
	if err != nil {
		return SubAgentDetail{}, err
	}
	entries := journal.Path
	retainedFrom := subAgentDetailRetainedFrom(entries)
	activityContext := subAgentDetailActivityContext{resultCallIDs: subAgentDetailResultCallIDs(entries)}
	events := make([]SubAgentDetailEvent, 0, len(entries))
	var nextOrdinal int64
	var hasMore bool
	for index, entry := range entries {
		ordinal := int64(index + 1)
		if ordinal < retainedFrom || ordinal <= opts.AfterOrdinal {
			continue
		}
		event, ok := h.subAgentDetailEvent(entry, ordinal, opts.IncludeRaw, activityContext)
		if !ok {
			continue
		}
		if len(events) >= limit {
			hasMore = true
			break
		}
		events = append(events, event)
		nextOrdinal = ordinal
	}
	return SubAgentDetail{
		Snapshot:     snapshot,
		Events:       events,
		NextOrdinal:  nextOrdinal,
		HasMore:      hasMore,
		RetainedFrom: retainedFrom,
		GeneratedAt:  h.now(),
	}, nil
}

func (h *AgentHarness) ListThreadDetailEvents(ctx context.Context, opts ListThreadDetailEventsOptions) (ThreadDetailEvents, error) {
	if h == nil {
		return ThreadDetailEvents{}, errors.New("agent harness is nil")
	}
	if strings.TrimSpace(opts.ThreadID) == "" {
		return ThreadDetailEvents{}, errors.New("thread id is required")
	}
	if opts.Limit < 0 {
		return ThreadDetailEvents{}, errors.New("thread detail event limit must be non-negative")
	}
	limit := opts.Limit
	if limit == 0 {
		limit = DefaultThreadDetailEventLimit
	}
	if limit > MaxThreadDetailEventLimit {
		limit = MaxThreadDetailEventLimit
	}
	thread := h.cacheThread(opts.ThreadID)
	journal, err := thread.Journal(ctx)
	if err != nil {
		return ThreadDetailEvents{}, err
	}
	entries := journal.Path
	retainedFrom := threadDetailRetainedFrom(entries)
	activityContext := subAgentDetailActivityContext{resultCallIDs: subAgentDetailResultCallIDs(entries)}
	events := make([]SubAgentDetailEvent, 0, len(entries))
	var nextOrdinal int64
	var hasMore bool
	for index, entry := range entries {
		ordinal := int64(index + 1)
		if ordinal < retainedFrom || ordinal <= opts.AfterOrdinal {
			continue
		}
		event, ok := h.subAgentDetailEvent(entry, ordinal, opts.IncludeRaw, activityContext)
		if !ok {
			continue
		}
		if len(events) >= limit {
			hasMore = true
			break
		}
		events = append(events, event)
		nextOrdinal = ordinal
	}
	return ThreadDetailEvents{
		Events:       events,
		NextOrdinal:  nextOrdinal,
		HasMore:      hasMore,
		RetainedFrom: retainedFrom,
		GeneratedAt:  h.now(),
	}, nil
}

func threadDetailRetainedFrom(entries []sessiontree.Entry) int64 {
	if len(entries) == 0 {
		return 0
	}
	return 1
}

func subAgentDetailRetainedFrom(entries []sessiontree.Entry) int64 {
	for index, entry := range entries {
		if entry.Type == sessiontree.EntryCustom &&
			entry.Metadata[subAgentInputKindKey] == subAgentInputEntryKind &&
			entry.Metadata[subAgentInputStateKey] == subAgentInputStatePending {
			return int64(index + 1)
		}
	}
	if len(entries) == 0 {
		return 0
	}
	return 1
}

func (h *AgentHarness) subAgentDetailEvent(entry sessiontree.Entry, ordinal int64, includeRaw bool, activityContext subAgentDetailActivityContext) (SubAgentDetailEvent, bool) {
	event := SubAgentDetailEvent{
		ID:        entry.ID,
		Ordinal:   ordinal,
		ParentID:  entry.ParentID,
		ThreadID:  entry.ThreadID,
		TurnID:    entry.TurnID,
		CreatedAt: entry.CreatedAt,
	}
	switch entry.Type {
	case sessiontree.EntryUserMessage:
		event.Kind = SubAgentDetailEventUserMessage
		event.Type = string(sessiontree.EntryUserMessage)
		event.Message = subAgentDetailMessage(entry.Message, includeRaw)
	case sessiontree.EntryAssistantMessage:
		event.Kind = SubAgentDetailEventAssistantMessage
		event.Type = string(sessiontree.EntryAssistantMessage)
		event.Message = subAgentDetailMessage(entry.Message, includeRaw)
	case sessiontree.EntryToolCall:
		event.Kind = SubAgentDetailEventToolCall
		event.Type = string(sessiontree.EntryToolCall)
		event.Message = subAgentDetailMessage(entry.Message, includeRaw)
		event.ToolCall = subAgentDetailToolCall(entry.Message, includeRaw)
	case sessiontree.EntryToolResult:
		event.Kind = SubAgentDetailEventToolResult
		event.Type = string(sessiontree.EntryToolResult)
		event.Message = subAgentDetailMessage(entry.Message, includeRaw)
		event.ToolResult = subAgentDetailToolResult(entry.Message, includeRaw)
	case sessiontree.EntryTurnMarker:
		event.Kind = SubAgentDetailEventTurnMarker
		event.Type = string(sessiontree.EntryTurnMarker)
		event.TurnMarker = &SubAgentDetailTurnMarker{
			Status:   string(entry.TurnStatus),
			Metadata: cloneStringMap(entry.Metadata),
		}
	case sessiontree.EntryCompaction:
		event.Kind = SubAgentDetailEventCompaction
		event.Type = string(sessiontree.EntryCompaction)
		event.Compaction = subAgentDetailCompaction(entry)
	case sessiontree.EntryRunFailure:
		event.Kind = SubAgentDetailEventError
		event.Type = string(sessiontree.EntryRunFailure)
		event.Error = entry.Error
	case sessiontree.EntryCustom:
		event.Kind = SubAgentDetailEventCustom
		event.Type = entry.Metadata[subAgentDetailTypeKey]
		switch entry.Metadata[subAgentDetailKindKey] {
		case subAgentInputEntryKind:
			event.Kind = SubAgentDetailEventInput
			if event.Type == "" {
				event.Type = subAgentInputEntryKind
			}
			if strings.TrimSpace(entry.Message.Content) != "" {
				event.Message = subAgentDetailMessage(session.Message{Role: session.User, Content: entry.Message.Content}, includeRaw)
			}
		case subAgentApprovalEntryKind:
			event.Kind = SubAgentDetailEventApproval
			if event.Type == "" {
				event.Type = subAgentApprovalEntryKind
			}
			event.Approval = subAgentDetailApproval(entry.Metadata)
		case subAgentLifecycleEntryKind:
			event.Kind = SubAgentDetailEventCustom
			if event.Type == "" {
				event.Type = subAgentLifecycleEntryKind
			}
		}
		event.Metadata = cloneStringMap(entry.Metadata)
	default:
		return SubAgentDetailEvent{}, false
	}
	if event.Metadata == nil && entry.Type != sessiontree.EntryTurnMarker {
		event.Metadata = cloneStringMap(entry.Metadata)
	}
	if !includeRaw && subAgentDetailRawAvailable(event) {
		if event.Metadata == nil {
			event.Metadata = map[string]string{}
		}
		event.Metadata[subAgentDetailRawOmitted] = "true"
	}
	event.ActivityTimeline = subAgentDetailActivityTimeline(event, entry, activityContext)
	return event, true
}

func subAgentDetailRawAvailable(event SubAgentDetailEvent) bool {
	switch event.Kind {
	case SubAgentDetailEventInput, SubAgentDetailEventUserMessage, SubAgentDetailEventAssistantMessage, SubAgentDetailEventToolCall, SubAgentDetailEventToolResult:
		return true
	default:
		return false
	}
}

type subAgentDetailActivityContext struct {
	resultCallIDs map[string]struct{}
}

func (c subAgentDetailActivityContext) hasResult(callID string) bool {
	callID = strings.TrimSpace(callID)
	if callID == "" || len(c.resultCallIDs) == 0 {
		return false
	}
	_, ok := c.resultCallIDs[callID]
	return ok
}

func subAgentDetailResultCallIDs(entries []sessiontree.Entry) map[string]struct{} {
	out := map[string]struct{}{}
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryToolResult {
			continue
		}
		callID := strings.TrimSpace(entry.Message.ToolCallID)
		if callID == "" {
			continue
		}
		out[callID] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func subAgentDetailActivityTimeline(detail SubAgentDetailEvent, entry sessiontree.Entry, activityContext subAgentDetailActivityContext) *observation.ActivityTimeline {
	observed, ok := subAgentDetailObservationEvent(detail, entry, activityContext)
	if !ok {
		return nil
	}
	timeline := observation.BuildActivityTimeline(observation.ActivityRunMeta{
		RunID:    detail.TurnID,
		ThreadID: detail.ThreadID,
		TurnID:   detail.TurnID,
	}, []observation.Event{observed}, entry.CreatedAt.UnixMilli())
	return &timeline
}

func subAgentDetailObservationEvent(detail SubAgentDetailEvent, entry sessiontree.Entry, activityContext subAgentDetailActivityContext) (observation.Event, bool) {
	base := observation.Event{
		RunID:      detail.TurnID,
		ThreadID:   detail.ThreadID,
		TurnID:     detail.TurnID,
		Step:       int(detail.Ordinal),
		ObservedAt: entry.CreatedAt,
	}
	switch detail.Kind {
	case SubAgentDetailEventToolCall:
		if detail.ToolCall == nil || activityContext.hasResult(detail.ToolCall.ID) {
			return observation.Event{}, false
		}
		if detail.Message != nil && detail.Message.Kind == string(session.MessageKindControlSignal) {
			base.Type = observation.EventTypeControlSignal
			base.ToolKind = "control"
			base.Metadata = map[string]any{"control_disposition": "terminal"}
		} else {
			base.Type = observation.EventTypeToolCall
			base.ToolKind = "local"
		}
		base.ToolID = detail.ToolCall.ID
		base.ToolName = detail.ToolCall.Name
		base.Activity = observationActivityPresentation(entry.Message.Activity)
		return base, true
	case SubAgentDetailEventToolResult:
		if detail.ToolResult == nil {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeToolResult
		base.ToolID = detail.ToolResult.CallID
		base.ToolName = detail.ToolResult.ToolName
		base.ToolKind = "local"
		base.Activity = observationActivityPresentation(entry.Message.Activity)
		base.Metadata = subAgentDetailToolResultActivityMetadata(detail.ToolResult)
		if detail.ToolResult.Status == string(observation.ActivityStatusError) {
			base.Error = "tool_result_error"
		}
		return base, true
	case SubAgentDetailEventApproval:
		if detail.Approval == nil {
			return observation.Event{}, false
		}
		base.Type = subAgentDetailApprovalActivityType(detail.Approval.State)
		base.ToolID = detail.Approval.ToolID
		base.ToolName = detail.Approval.ToolName
		base.ToolKind = firstSubAgentDetailNonEmpty(detail.Approval.ToolKind, "local")
		base.ArgsHash = detail.Approval.ArgsHash
		base.Activity = subAgentDetailActivityPresentation("Tool approval", detail.Approval.State)
		base.Metadata = subAgentDetailApprovalActivityMetadata(detail.Approval.Metadata)
		if detail.Approval.State == "rejected" || detail.Approval.State == "timed_out" {
			base.Error = "tool_approval_" + detail.Approval.State
		}
		return base, true
	case SubAgentDetailEventInput:
		base.Type = observation.EventTypeControlSignal
		base.ToolID = "subagent_input"
		base.ToolName = "subagent_input"
		base.ToolKind = "control"
		base.Activity = subAgentDetailActivityPresentation("Subagent input", "queued")
		base.Metadata = map[string]any{"control_disposition": "continue"}
		return base, true
	case SubAgentDetailEventTurnMarker:
		if detail.TurnMarker == nil {
			return observation.Event{}, false
		}
		status := strings.TrimSpace(detail.TurnMarker.Status)
		if status == "" {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeControlSignal
		base.ToolID = "turn"
		base.ToolName = "turn"
		base.ToolKind = "control"
		base.Activity = subAgentDetailActivityPresentation("Turn "+status, status)
		base.Metadata = subAgentDetailTurnMarkerActivityMetadata(status)
		if status == string(sessiontree.TurnFailed) || status == string(sessiontree.TurnAborted) {
			base.Error = "turn_" + status
		}
		return base, true
	case SubAgentDetailEventCustom:
		if detail.Type != subAgentLifecycleEntryKind {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeControlSignal
		base.ToolID = "subagent_lifecycle"
		base.ToolName = "subagent_lifecycle"
		base.ToolKind = "control"
		action := firstSubAgentDetailNonEmpty(detail.Metadata[subAgentLifecycleActionKey], "updated")
		base.Activity = subAgentDetailActivityPresentation("Subagent "+action, action)
		base.Metadata = map[string]any{"control_disposition": "terminal"}
		return base, true
	default:
		return observation.Event{}, false
	}
}

func subAgentDetailToolResultActivityMetadata(result *SubAgentDetailToolResult) map[string]any {
	if result == nil {
		return nil
	}
	metadata := map[string]any{}
	switch result.Status {
	case string(observation.ActivityStatusError):
		metadata["error_present"] = true
	case string(observation.ActivityStatusRunning):
		metadata["pending_tool_result"] = true
		metadata["pending_state"] = string(observation.ActivityStatusRunning)
	}
	if result.Truncated {
		metadata["truncated"] = true
	}
	if result.OriginalBytes > 0 {
		metadata["original_bytes"] = result.OriginalBytes
	}
	if result.VisibleBytes > 0 {
		metadata["visible_bytes"] = result.VisibleBytes
	}
	if result.OriginalLines > 0 {
		metadata["original_lines"] = result.OriginalLines
	}
	if result.VisibleLines > 0 {
		metadata["visible_lines"] = result.VisibleLines
	}
	if strings.TrimSpace(result.Strategy) != "" {
		metadata["strategy"] = strings.TrimSpace(result.Strategy)
	}
	if strings.TrimSpace(result.ContentSHA256) != "" {
		metadata["content_sha256"] = strings.TrimSpace(result.ContentSHA256)
	}
	if result.FullOutput != nil && strings.TrimSpace(result.FullOutput.SHA256) != "" {
		metadata["artifact_sha256"] = strings.TrimSpace(result.FullOutput.SHA256)
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func subAgentDetailApprovalActivityType(state string) string {
	switch strings.TrimSpace(state) {
	case "approved":
		return observation.EventTypeToolApprovalApproved
	case "rejected":
		return observation.EventTypeToolApprovalRejected
	case "timed_out":
		return observation.EventTypeToolApprovalTimedOut
	case "canceled":
		return observation.EventTypeToolApprovalCanceled
	default:
		return observation.EventTypeToolApprovalRequested
	}
}

func subAgentDetailApprovalActivityMetadata(metadata map[string]string) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, key := range []string{"approval_id_hash", "effects", "read_only", "destructive", "open_world", "error_present"} {
		if value := strings.TrimSpace(metadata[key]); value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func subAgentDetailTurnMarkerActivityMetadata(status string) map[string]any {
	switch sessiontree.TurnMarkerStatus(status) {
	case sessiontree.TurnWaiting:
		return map[string]any{"control_disposition": "waiting"}
	case sessiontree.TurnFailed, sessiontree.TurnAborted:
		return map[string]any{"control_disposition": "terminal", "error_present": true}
	default:
		return map[string]any{"control_disposition": "terminal"}
	}
}

func subAgentDetailActivityPresentation(label, description string) *observation.ActivityPresentation {
	return &observation.ActivityPresentation{
		Label:       strings.TrimSpace(label),
		Description: strings.TrimSpace(description),
		Renderer:    observation.ActivityRendererStructured,
	}
}

func observationActivityPresentation(in *session.ActivityPresentation) *observation.ActivityPresentation {
	if in == nil {
		return nil
	}
	out := &observation.ActivityPresentation{
		Label:       in.Label,
		Description: in.Description,
		Renderer:    observation.ActivityRenderer(in.Renderer),
		Chips:       make([]observation.ActivityChip, 0, len(in.Chips)),
		TargetRefs:  make([]observation.ActivityTargetRef, 0, len(in.TargetRefs)),
		Payload:     cloneActivityPayload(in.Payload),
	}
	for _, chip := range in.Chips {
		out.Chips = append(out.Chips, observation.ActivityChip{
			Kind:  chip.Kind,
			Label: chip.Label,
			Value: chip.Value,
			Tone:  chip.Tone,
		})
	}
	for _, ref := range in.TargetRefs {
		out.TargetRefs = append(out.TargetRefs, observation.ActivityTargetRef{
			Kind:  ref.Kind,
			Label: ref.Label,
			URI:   ref.URI,
			Path:  ref.Path,
			Line:  ref.Line,
		})
	}
	return out
}

func firstSubAgentDetailNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func subAgentDetailApproval(metadata map[string]string) *SubAgentDetailApproval {
	if len(metadata) == 0 {
		return nil
	}
	return &SubAgentDetailApproval{
		State:    metadata[subAgentApprovalStateKey],
		ToolID:   metadata[subAgentApprovalToolIDKey],
		ToolName: metadata[subAgentApprovalNameKey],
		ToolKind: metadata[subAgentApprovalKindKey],
		ArgsHash: metadata[subAgentApprovalArgsKey],
		Reason:   metadata[subAgentApprovalReasonKey],
		Metadata: cloneStringMap(metadata),
	}
}

func subAgentDetailMessage(msg session.Message, includeRaw bool) *SubAgentDetailMessage {
	if msg.Role == "" && msg.Kind == "" && msg.Content == "" && msg.Reasoning == "" {
		return nil
	}
	out := &SubAgentDetailMessage{
		Role:    string(msg.Role),
		Kind:    string(msg.Kind),
		Preview: safeSubAgentDetailPreview(msg.Content, 500),
	}
	if includeRaw {
		out.Content = msg.Content
		out.Reasoning = msg.Reasoning
	}
	return out
}

func subAgentDetailToolCall(msg session.Message, includeRaw bool) *SubAgentDetailToolCall {
	if msg.ToolCallID == "" && msg.ToolName == "" && msg.ToolArgs == "" {
		return nil
	}
	args := strings.TrimSpace(msg.ToolArgs)
	out := &SubAgentDetailToolCall{
		ID:          msg.ToolCallID,
		Name:        msg.ToolName,
		ArgsPreview: safeSubAgentDetailPreview(args, 500),
		ArgsHash:    stableSubAgentDetailHash(args),
	}
	if includeRaw {
		out.ArgsJSON = args
	}
	return out
}

func subAgentDetailToolResult(msg session.Message, includeRaw bool) *SubAgentDetailToolResult {
	if msg.ToolCallID == "" && msg.ToolName == "" && msg.Content == "" && msg.ToolResult == nil {
		return nil
	}
	out := &SubAgentDetailToolResult{
		CallID:   msg.ToolCallID,
		ToolName: msg.ToolName,
		Preview:  safeSubAgentDetailPreview(msg.Content, 800),
	}
	if includeRaw {
		out.Content = msg.Content
	}
	if view := msg.ToolResult; view != nil {
		out.Status = view.Status
		out.Truncated = view.Truncated
		out.OriginalBytes = view.OriginalBytes
		out.VisibleBytes = view.VisibleBytes
		out.OriginalLines = view.OriginalLines
		out.VisibleLines = view.VisibleLines
		out.Strategy = view.Strategy
		out.ContentSHA256 = view.ContentSHA256
		if view.FullOutput != nil {
			ref := *view.FullOutput
			out.FullOutput = &ref
		}
	}
	if out.ContentSHA256 == "" {
		out.ContentSHA256 = stableSubAgentDetailHash(msg.Content)
	}
	return out
}

func subAgentDetailCompaction(entry sessiontree.Entry) *SubAgentDetailCompaction {
	return &SubAgentDetailCompaction{
		CompactionID:            entry.CompactionID,
		PreviousCompactionID:    entry.PreviousCompactionID,
		CompactedThroughEntryID: entry.CompactedThroughEntryID,
		SummarySchemaVersion:    entry.SummarySchemaVersion,
		CompactionGeneration:    entry.CompactionGeneration,
		CompactionWindowID:      entry.CompactionWindowID,
		FirstKeptEntryID:        entry.FirstKeptEntryID,
		KeptUserEntryIDs:        append([]string(nil), entry.KeptUserEntryIDs...),
		Summary:                 entry.Summary,
		Trigger:                 entry.CompactionTrigger,
		Reason:                  entry.CompactionReason,
		Phase:                   entry.CompactionPhase,
		TokensBefore:            entry.TokensBefore,
		TokensAfterEstimate:     entry.TokensAfterEstimate,
		Metadata:                cloneStringMap(entry.Metadata),
	}
}

func stableSubAgentDetailHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func safeSubAgentDetailPreview(value string, limit int) string {
	value = strings.TrimSpace(event.SafePathRefsText(value))
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if limit > 0 && len(runes) > limit {
		return string(runes[:limit]) + "..."
	}
	return event.Redact(value)
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (h *AgentHarness) ListSubAgents(ctx context.Context, parentThreadID string) ([]SubAgentSnapshot, error) {
	if h == nil {
		return nil, errors.New("agent harness is nil")
	}
	parentThreadID = strings.TrimSpace(parentThreadID)
	if parentThreadID == "" {
		return nil, errors.New("parent thread id is required")
	}
	metas, err := h.childThreadMetas(ctx, parentThreadID)
	if err != nil {
		return nil, err
	}
	out := make([]SubAgentSnapshot, 0, len(metas))
	for _, meta := range metas {
		snapshot, err := h.subAgentSnapshotFromMeta(ctx, meta)
		if err != nil {
			return nil, err
		}
		out = append(out, snapshot)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].Path < out[j].Path
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (h *AgentHarness) CloseSubAgent(ctx context.Context, opts CloseSubAgentOptions) (SubAgentSnapshot, error) {
	meta, err := h.resolveSubAgentMeta(ctx, opts.ParentThreadID, opts.ChildThreadID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	thread := h.cacheThread(meta.ID)
	ctrl, err := h.ensureSubAgentController(ctx, meta, thread)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	ctrl.mu.Lock()
	ctrl.closed = true
	queued := append([]subagentInput(nil), ctrl.queue...)
	ctrl.queue = nil
	cancel := ctrl.cancel
	done := ctrl.done
	ctrl.mu.Unlock()
	for _, input := range queued {
		if err := h.appendSubAgentInputState(ctx, meta.ID, input.entryID, subAgentInputStateCancelled, nil); err != nil {
			return SubAgentSnapshot{}, err
		}
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return SubAgentSnapshot{}, ctx.Err()
		}
	}
	meta, err = h.updateSubAgentMeta(ctx, meta.ID, func(current *sessiontree.ThreadMeta) {
		current.Closed = true
		current.Status = string(SubAgentStatusClosed)
	})
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if err := h.appendSubAgentLifecycleEvent(ctx, meta.ID, map[string]string{
		subAgentLifecycleActionKey: "closed",
		subAgentLifecycleReasonKey: closeSubAgentReason(opts.Reason),
		"parent_thread_id":         strings.TrimSpace(opts.ParentThreadID),
	}); err != nil {
		return SubAgentSnapshot{}, err
	}
	h.emit(HarnessEvent{
		Type:     EventSubAgentClosed,
		ThreadID: strings.TrimSpace(opts.ParentThreadID),
		Metadata: map[string]string{
			"subagent_thread_id": meta.ID,
			"subagent_path":      meta.AgentPath,
		},
	})
	h.notifySubAgentUpdate()
	return h.subAgentSnapshot(ctx, meta.ID)
}

func (h *AgentHarness) CloseSubAgents(ctx context.Context, opts CloseSubAgentsOptions) (CloseSubAgentsResult, error) {
	if h == nil {
		return CloseSubAgentsResult{}, errors.New("agent harness is nil")
	}
	parentThreadID := strings.TrimSpace(opts.ParentThreadID)
	if parentThreadID == "" {
		return CloseSubAgentsResult{}, errors.New("parent thread id is required")
	}
	metas, err := h.childThreadMetas(ctx, parentThreadID)
	if err != nil {
		return CloseSubAgentsResult{}, err
	}
	result := CloseSubAgentsResult{Snapshots: make([]SubAgentSnapshot, 0, len(metas))}
	for _, meta := range metas {
		snapshot, err := h.subAgentSnapshotFromMeta(ctx, meta)
		if err != nil {
			return CloseSubAgentsResult{}, err
		}
		if shouldCloseSubAgentForParentStop(snapshot) {
			snapshot, err = h.CloseSubAgent(ctx, CloseSubAgentOptions{
				ParentThreadID: parentThreadID,
				ChildThreadID:  snapshot.ThreadID,
				Reason:         opts.Reason,
			})
			if err != nil {
				return CloseSubAgentsResult{}, err
			}
			result.Closed++
		}
		result.Snapshots = append(result.Snapshots, snapshot)
	}
	return result, nil
}

func (h *AgentHarness) enqueueSubAgentInput(ctx context.Context, ctrl *subagentController, input subagentInput, interrupt bool) error {
	ctrl.mu.Lock()
	if ctrl.closed {
		ctrl.mu.Unlock()
		return ErrSubAgentClosed
	}
	var cancelled []subagentInput
	if interrupt {
		cancelled = append(cancelled, ctrl.queue...)
		if ctrl.cancel != nil {
			ctrl.cancel()
		}
		ctrl.queue = []subagentInput{input}
	} else {
		ctrl.queue = append(ctrl.queue, input)
	}
	shouldStart := !ctrl.running
	ctrl.mu.Unlock()
	for _, old := range cancelled {
		if err := h.appendSubAgentInputState(ctx, ctrl.threadID, old.entryID, subAgentInputStateCancelled, nil); err != nil {
			return err
		}
	}
	h.notifySubAgentUpdate()
	if shouldStart {
		h.startNextSubAgentTurn(ctrl)
	}
	return nil
}

func (h *AgentHarness) startNextSubAgentTurn(ctrl *subagentController) {
	ctrl.mu.Lock()
	if ctrl.closed || ctrl.running || len(ctrl.queue) == 0 {
		ctrl.mu.Unlock()
		return
	}
	turnID, err := h.nextSubAgentTurnID(context.Background(), ctrl.threadID)
	if err != nil {
		ctrl.mu.Unlock()
		h.notifySubAgentUpdate()
		return
	}
	input := ctrl.queue[0]
	ctrl.queue = ctrl.queue[1:]
	runCtx, cancel := h.subAgentRunContext()
	ctrl.running = true
	ctrl.turnID = turnID
	ctrl.cancel = cancel
	ctrl.done = make(chan struct{})
	thread := ctrl.thread
	done := ctrl.done
	ctrl.mu.Unlock()

	if !h.subAgentCanStartQueuedInput(context.Background(), thread) {
		cancel()
		ctrl.mu.Lock()
		ctrl.running = false
		ctrl.turnID = ""
		ctrl.cancel = nil
		ctrl.queue = append([]subagentInput{input}, ctrl.queue...)
		if ctrl.done == done {
			ctrl.done = nil
		}
		ctrl.mu.Unlock()
		if done != nil {
			close(done)
		}
		h.notifySubAgentUpdate()
		return
	}
	if err := h.appendSubAgentInputState(context.Background(), ctrl.threadID, input.entryID, subAgentInputStateConsumed, map[string]string{subAgentInputTurnIDKey: turnID}); err != nil {
		cancel()
		ctrl.mu.Lock()
		ctrl.running = false
		ctrl.turnID = ""
		ctrl.cancel = nil
		ctrl.queue = append([]subagentInput{input}, ctrl.queue...)
		if ctrl.done == done {
			ctrl.done = nil
		}
		ctrl.mu.Unlock()
		if done != nil {
			close(done)
		}
		h.notifySubAgentUpdate()
		return
	}
	h.notifySubAgentUpdate()
	go func() {
		result, err := thread.Run(runCtx, input.message, RunOptions{
			TurnID: turnID,
			Labels: input.labels,
			DeadlineMetadata: map[string]string{
				subAgentTerminalReasonKey: subAgentRunTimeoutReason,
			},
		})
		timeout := errors.Is(err, context.DeadlineExceeded)
		cancel()
		status := string(result.Status)
		if err != nil && status == "" {
			status = string(engine.Failed)
		}
		ctrl.mu.Lock()
		ctrl.running = false
		ctrl.turnID = ""
		ctrl.cancel = nil
		closed := ctrl.closed
		hasNext := len(ctrl.queue) > 0 && !closed
		done := ctrl.done
		ctrl.done = nil
		ctrl.mu.Unlock()
		if !closed && isSettledSubAgentStatus(SubAgentStatus(status)) {
			_, _ = h.updateSubAgentMeta(context.Background(), ctrl.threadID, func(current *sessiontree.ThreadMeta) {
				current.Status = status
				if timeout {
					current.Status = string(SubAgentStatusCancelled)
				}
			})
		}
		if done != nil {
			close(done)
		}
		h.emit(HarnessEvent{
			Type:     EventSubAgentCompleted,
			ThreadID: ctrl.parentThreadID,
			Status:   status,
			Message:  result.Output,
			Metadata: map[string]string{
				"subagent_thread_id": ctrl.threadID,
				"subagent_path":      ctrl.path,
			},
		})
		h.notifySubAgentUpdate()
		if hasNext {
			h.startNextSubAgentTurn(ctrl)
		}
	}()
}

func (h *AgentHarness) subAgentRunContext() (context.Context, context.CancelFunc) {
	base, baseCancel := context.WithCancel(context.Background())
	timeout := DefaultSubAgentRunTimeout
	if h != nil && h.options.SubAgentRunTimeout > 0 {
		timeout = h.options.SubAgentRunTimeout
	}
	if timeout > MaxSubAgentRunTimeout {
		timeout = MaxSubAgentRunTimeout
	}
	if timeout <= 0 {
		return base, baseCancel
	}
	ctx, timeoutCancel := context.WithTimeout(base, timeout)
	return ctx, func() {
		timeoutCancel()
		baseCancel()
	}
}

func (h *AgentHarness) subAgentCanStartQueuedInput(ctx context.Context, thread *Thread) bool {
	if thread == nil {
		return false
	}
	if lease, ok, err := h.activeTurnLease(ctx, thread.ID()); err != nil || ok && lease.TurnID != "" {
		return false
	}
	read, err := thread.Read(ctx)
	if err != nil {
		return false
	}
	return read.CanAppendMessage
}

func (h *AgentHarness) appendSubAgentLifecycleEvent(ctx context.Context, threadID string, metadata map[string]string) error {
	if h == nil || h.options.Repo == nil {
		return nil
	}
	meta := cloneStringMap(metadata)
	if meta == nil {
		meta = map[string]string{}
	}
	meta[subAgentDetailKindKey] = subAgentLifecycleEntryKind
	meta[subAgentDetailTypeKey] = subAgentLifecycleEntryKind
	entry, err := h.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: threadID,
		Type:     sessiontree.EntryCustom,
		Metadata: meta,
	}, sessiontree.AppendOptions{})
	if err != nil {
		return err
	}
	h.emit(HarnessEvent{Type: EventEntryAppended, ThreadID: threadID, EntryID: entry.ID, ParentID: entry.ParentID, Message: subAgentLifecycleEntryKind})
	return nil
}

func (h *AgentHarness) ensureSubAgentController(ctx context.Context, meta sessiontree.ThreadMeta, thread *Thread) (*subagentController, error) {
	key := subAgentControllerKey(meta.ID)
	h.mu.Lock()
	if h.subagents == nil {
		h.subagents = map[string]*subagentController{}
	}
	if h.subagentUpdates == nil {
		h.subagentUpdates = make(chan struct{})
	}
	if ctrl, ok := h.subagents[key]; ok {
		h.mu.Unlock()
		return ctrl, nil
	}
	h.mu.Unlock()
	pending, err := h.pendingSubAgentInputs(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	ctrl := &subagentController{
		parentThreadID: meta.ParentThreadID,
		threadID:       meta.ID,
		path:           meta.AgentPath,
		taskName:       meta.TaskName,
		thread:         thread,
		closed:         meta.Closed,
		queue:          pending,
	}
	h.mu.Lock()
	if existing, ok := h.subagents[key]; ok {
		h.mu.Unlock()
		return existing, nil
	}
	h.subagents[key] = ctrl
	h.mu.Unlock()
	return ctrl, nil
}

func (h *AgentHarness) notifySubAgentUpdate() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.subagentUpdates == nil {
		h.subagentUpdates = make(chan struct{})
	}
	close(h.subagentUpdates)
	h.subagentUpdates = make(chan struct{})
	h.mu.Unlock()
}

func (h *AgentHarness) subAgentUpdateChannel() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subagentUpdates == nil {
		h.subagentUpdates = make(chan struct{})
	}
	return h.subagentUpdates
}

func (h *AgentHarness) snapshotsForTargets(ctx context.Context, parentThreadID string, childThreadIDs []string) ([]SubAgentSnapshot, error) {
	out := make([]SubAgentSnapshot, 0, len(childThreadIDs))
	for _, childThreadID := range childThreadIDs {
		meta, err := h.resolveSubAgentMeta(ctx, parentThreadID, childThreadID)
		if err != nil {
			return nil, err
		}
		snapshot, err := h.subAgentSnapshotFromMeta(ctx, meta)
		if err != nil {
			return nil, err
		}
		out = append(out, snapshot)
	}
	return out, nil
}

func (h *AgentHarness) subAgentSnapshot(ctx context.Context, threadID string) (SubAgentSnapshot, error) {
	meta, err := h.options.Repo.Thread(ctx, threadID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	return h.subAgentSnapshotFromMeta(ctx, meta)
}

func (h *AgentHarness) subAgentSnapshotFromMeta(ctx context.Context, meta sessiontree.ThreadMeta) (SubAgentSnapshot, error) {
	thread := h.cacheThread(meta.ID)
	status := subAgentStatusFromMeta(meta)
	queued, err := h.pendingSubAgentInputCount(ctx, meta.ID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	liveRunning := false
	key := subAgentControllerKey(meta.ID)
	h.mu.Lock()
	ctrl := h.subagents[key]
	h.mu.Unlock()
	if ctrl == nil && queued > 0 && !meta.Closed {
		var err error
		ctrl, err = h.ensureSubAgentController(ctx, meta, thread)
		if err != nil {
			return SubAgentSnapshot{}, err
		}
	}
	if ctrl != nil && queued > 0 && !meta.Closed {
		h.startNextSubAgentTurn(ctrl)
		nextQueued, err := h.pendingSubAgentInputCount(ctx, meta.ID)
		if err != nil {
			return SubAgentSnapshot{}, err
		}
		queued = nextQueued
	}
	runningTurnID := ""
	if ctrl != nil {
		ctrl.mu.Lock()
		if ctrl.running && !meta.Closed {
			liveRunning = true
			runningTurnID = ctrl.turnID
			status = SubAgentStatusRunning
		}
		ctrl.mu.Unlock()
	}
	if queued > 0 && !meta.Closed && status != SubAgentStatusRunning {
		status = SubAgentStatusRunning
	}
	read, err := thread.Read(ctx)
	if err == nil {
		if !meta.Closed && queued == 0 && read.Status != "" {
			readStatus := SubAgentStatus(read.Status)
			if !liveRunning || runningTurnID != "" && read.LatestTurnID == runningTurnID && isSettledSubAgentStatus(readStatus) {
				liveRunning = false
				status = readStatus
			}
		}
		return SubAgentSnapshot{
			ThreadID:       meta.ID,
			Path:           meta.AgentPath,
			TaskName:       meta.TaskName,
			ParentThreadID: meta.ParentThreadID,
			ParentTurnID:   meta.ParentTurnID,
			HostProfileRef: meta.HostProfileRef,
			ForkMode:       SubAgentForkMode(meta.ForkMode),
			Status:         status,
			LatestTurnID:   read.LatestTurnID,
			LastMessage:    latestSubAgentMessage(read.Messages),
			WaitingPrompt:  read.WaitingPrompt,
			QueuedInputs:   queued,
			CreatedAt:      meta.CreatedAt,
			UpdatedAt:      meta.UpdatedAt,
			Closed:         meta.Closed,
			CanSendInput:   !meta.Closed,
			CanInterrupt:   !meta.Closed && status == SubAgentStatusRunning,
			CanClose:       !meta.Closed,
		}, nil
	}
	return SubAgentSnapshot{
		ThreadID:       meta.ID,
		Path:           meta.AgentPath,
		TaskName:       meta.TaskName,
		ParentThreadID: meta.ParentThreadID,
		ParentTurnID:   meta.ParentTurnID,
		HostProfileRef: meta.HostProfileRef,
		ForkMode:       SubAgentForkMode(meta.ForkMode),
		Status:         status,
		QueuedInputs:   queued,
		CreatedAt:      meta.CreatedAt,
		UpdatedAt:      meta.UpdatedAt,
		Closed:         meta.Closed,
		CanSendInput:   !meta.Closed,
		CanInterrupt:   !meta.Closed && status == SubAgentStatusRunning,
		CanClose:       !meta.Closed,
	}, nil
}

func subAgentStatusFromMeta(meta sessiontree.ThreadMeta) SubAgentStatus {
	if meta.Closed {
		return SubAgentStatusClosed
	}
	switch strings.TrimSpace(meta.Status) {
	case string(SubAgentStatusRunning):
		return SubAgentStatusRunning
	case string(SubAgentStatusWaiting):
		return SubAgentStatusWaiting
	case string(SubAgentStatusCompleted):
		return SubAgentStatusCompleted
	case string(SubAgentStatusFailed):
		return SubAgentStatusFailed
	case string(SubAgentStatusCancelled):
		return SubAgentStatusCancelled
	case string(SubAgentStatusInterrupted):
		return SubAgentStatusInterrupted
	default:
		return SubAgentStatusIdle
	}
}

func closeSubAgentReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "parent_close"
	}
	return reason
}

func shouldCloseSubAgentForParentStop(snapshot SubAgentSnapshot) bool {
	if !snapshot.CanClose || snapshot.Closed {
		return false
	}
	switch snapshot.Status {
	case SubAgentStatusCompleted, SubAgentStatusFailed, SubAgentStatusCancelled, SubAgentStatusClosed:
		return false
	default:
		return true
	}
}

func isSettledSubAgentStatus(status SubAgentStatus) bool {
	switch status {
	case SubAgentStatusCompleted, SubAgentStatusWaiting, SubAgentStatusFailed, SubAgentStatusCancelled, SubAgentStatusInterrupted, SubAgentStatusClosed:
		return true
	default:
		return false
	}
}

func latestSubAgentMessage(messages []ThreadMessage) string {
	for i := len(messages) - 1; i >= 0; i-- {
		text := strings.TrimSpace(messages[i].Content)
		if text != "" {
			return text
		}
	}
	return ""
}

func isSettledSubAgentStatusForWait(status SubAgentStatus) bool {
	switch status {
	case SubAgentStatusCompleted, SubAgentStatusWaiting, SubAgentStatusFailed, SubAgentStatusCancelled, SubAgentStatusInterrupted, SubAgentStatusClosed:
		return true
	default:
		return false
	}
}

func allSubAgentsSettledForWait(snapshots []SubAgentSnapshot) bool {
	if len(snapshots) == 0 {
		return false
	}
	for _, snapshot := range snapshots {
		if snapshot.QueuedInputs > 0 {
			return false
		}
		if !isSettledSubAgentStatusForWait(snapshot.Status) {
			return false
		}
	}
	return true
}

func (h *AgentHarness) resolveSubAgentMeta(ctx context.Context, parentThreadID, target string) (sessiontree.ThreadMeta, error) {
	parentThreadID = strings.TrimSpace(parentThreadID)
	if parentThreadID == "" {
		return sessiontree.ThreadMeta{}, errors.New("parent thread id is required")
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return sessiontree.ThreadMeta{}, errors.New("subagent child thread id is required")
	}
	meta, err := h.options.Repo.Thread(ctx, target)
	if err != nil {
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return sessiontree.ThreadMeta{}, ErrSubAgentNotFound
		}
		return sessiontree.ThreadMeta{}, err
	}
	if meta.ParentThreadID != parentThreadID || strings.TrimSpace(meta.AgentPath) == "" {
		return sessiontree.ThreadMeta{}, ErrSubAgentNotFound
	}
	return meta, nil
}

func (h *AgentHarness) updateSubAgentMeta(ctx context.Context, threadID string, update func(*sessiontree.ThreadMeta)) (sessiontree.ThreadMeta, error) {
	meta, err := h.options.Repo.Thread(ctx, threadID)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if update != nil {
		update(&meta)
	}
	meta.UpdatedAt = h.now()
	if err := h.options.Repo.UpdateThread(ctx, meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	return meta, nil
}

func (h *AgentHarness) childThreadMetas(ctx context.Context, parentThreadID string) ([]sessiontree.ThreadMeta, error) {
	parentThreadID = strings.TrimSpace(parentThreadID)
	if parentThreadID == "" {
		return nil, errors.New("parent thread id is required")
	}
	listRepo, ok := h.options.Repo.(sessiontree.ThreadListRepo)
	if !ok {
		return nil, errors.New("session tree repo does not support thread listing")
	}
	threads, err := listRepo.ListThreads(ctx, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return nil, err
	}
	children := make([]sessiontree.ThreadMeta, 0)
	for _, meta := range threads {
		if meta.ParentThreadID == parentThreadID && strings.TrimSpace(meta.AgentPath) != "" {
			children = append(children, meta)
		}
	}
	return children, nil
}

func (h *AgentHarness) nextSubAgentThreadID(ctx context.Context) (string, error) {
	for i := 0; i < 100; i++ {
		id := h.nextID("subagent")
		if strings.TrimSpace(id) == "" {
			continue
		}
		if _, err := h.options.Repo.Thread(ctx, id); errors.Is(err, sessiontree.ErrThreadNotFound) {
			return id, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("unable to allocate unique subagent thread id")
}

func (h *AgentHarness) nextSubAgentTurnID(ctx context.Context, threadID string) (string, error) {
	for i := 0; i < 100; i++ {
		id := h.nextID("turn")
		if strings.TrimSpace(id) == "" {
			continue
		}
		entries, err := h.options.Repo.Entries(ctx, threadID)
		if err != nil {
			return "", err
		}
		exists := false
		for _, entry := range entries {
			if entry.TurnID == id {
				exists = true
				break
			}
		}
		if !exists {
			return id, nil
		}
	}
	return "", errors.New("unable to allocate unique subagent turn id")
}

func (h *AgentHarness) appendSubAgentInput(ctx context.Context, threadID, message string, labels engine.RunLabels, interrupt bool) (subagentInput, error) {
	message = strings.TrimSpace(message)
	if message == "" {
		return subagentInput{}, errors.New("subagent message is required")
	}
	metadata := map[string]string{
		subAgentInputKindKey:        subAgentInputEntryKind,
		subAgentInputStateKey:       subAgentInputStatePending,
		subAgentInputMessageHashKey: subAgentInputMessageHash(message),
		subAgentInputInterruptKey:   fmt.Sprintf("%t", interrupt),
	}
	if len(labels.Host) > 0 {
		if data, err := json.Marshal(labels.Host); err == nil {
			metadata[subAgentInputLabelHostKey] = string(data)
		}
	}
	if len(labels.Correlation) > 0 {
		if data, err := json.Marshal(labels.Correlation); err == nil {
			metadata[subAgentInputLabelCorrKey] = string(data)
		}
	}
	entry, err := h.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: threadID,
		Type:     sessiontree.EntryCustom,
		Message:  session.Message{Content: message},
		Metadata: metadata,
	}, sessiontree.AppendOptions{})
	if err != nil {
		return subagentInput{}, err
	}
	return subagentInput{entryID: entry.ID, message: message, labels: labels}, nil
}

func (h *AgentHarness) appendSubAgentInputState(ctx context.Context, threadID, inputEntryID, state string, extra map[string]string) error {
	inputEntryID = strings.TrimSpace(inputEntryID)
	if inputEntryID == "" {
		return nil
	}
	metadata := map[string]string{
		subAgentInputKindKey:    subAgentInputEntryKind,
		subAgentInputStateKey:   state,
		subAgentInputEntryIDKey: inputEntryID,
	}
	for key, value := range extra {
		if strings.TrimSpace(key) != "" {
			metadata[key] = value
		}
	}
	_, err := h.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: threadID,
		Type:     sessiontree.EntryCustom,
		Metadata: metadata,
	}, sessiontree.AppendOptions{})
	return err
}

func (h *AgentHarness) pendingSubAgentInputCount(ctx context.Context, threadID string) (int, error) {
	inputs, err := h.pendingSubAgentInputs(ctx, threadID)
	if err != nil {
		return 0, err
	}
	return len(inputs), nil
}

func (h *AgentHarness) pendingSubAgentInputs(ctx context.Context, threadID string) ([]subagentInput, error) {
	entries, err := h.options.Repo.Entries(ctx, threadID)
	if err != nil {
		return nil, err
	}
	stateByID := map[string]string{}
	inputByID := map[string]subagentInput{}
	consumedTurnByID := map[string]string{}
	turnHasUserMessage := map[string]bool{}
	order := make([]string, 0)
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryUserMessage && entry.TurnID != "" {
			turnHasUserMessage[entry.TurnID] = true
		}
		if entry.Type != sessiontree.EntryCustom || entry.Metadata[subAgentInputKindKey] != subAgentInputEntryKind {
			continue
		}
		state := strings.TrimSpace(entry.Metadata[subAgentInputStateKey])
		switch state {
		case subAgentInputStatePending:
			labels := subAgentInputLabels(entry.Metadata)
			inputByID[entry.ID] = subagentInput{entryID: entry.ID, message: strings.TrimSpace(entry.Message.Content), labels: labels}
			stateByID[entry.ID] = state
			order = append(order, entry.ID)
		case subAgentInputStateConsumed, subAgentInputStateCancelled:
			target := strings.TrimSpace(entry.Metadata[subAgentInputEntryIDKey])
			if target != "" {
				stateByID[target] = state
				if state == subAgentInputStateConsumed {
					consumedTurnByID[target] = strings.TrimSpace(entry.Metadata[subAgentInputTurnIDKey])
				}
			}
		}
	}
	out := make([]subagentInput, 0, len(order))
	for _, id := range order {
		if stateByID[id] == subAgentInputStateConsumed && !turnHasUserMessage[consumedTurnByID[id]] {
			stateByID[id] = subAgentInputStatePending
		}
		if stateByID[id] != subAgentInputStatePending {
			continue
		}
		input := inputByID[id]
		if input.message == "" {
			continue
		}
		out = append(out, input)
	}
	return out, nil
}

func subAgentInputLabels(metadata map[string]string) engine.RunLabels {
	var labels engine.RunLabels
	if raw := strings.TrimSpace(metadata[subAgentInputLabelHostKey]); raw != "" {
		_ = json.Unmarshal([]byte(raw), &labels.Host)
	}
	if raw := strings.TrimSpace(metadata[subAgentInputLabelCorrKey]); raw != "" {
		_ = json.Unmarshal([]byte(raw), &labels.Correlation)
	}
	return labels
}

func subAgentInputMessageHash(message string) string {
	sum := sha256.Sum256([]byte(message))
	return hex.EncodeToString(sum[:])
}

func normalizeSubAgentTaskName(taskName string) (string, error) {
	taskName = strings.TrimSpace(strings.ToLower(taskName))
	if taskName == "" {
		return "", errors.New("subagent task name is required")
	}
	var b strings.Builder
	prevUnderscore := false
	for _, r := range taskName {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if r == '_' || r == '-' || r == ' ' {
			if !prevUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				prevUnderscore = true
			}
			continue
		}
		return "", fmt.Errorf("subagent task name %q must use letters, digits, spaces, hyphens, or underscores", taskName)
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "", errors.New("subagent task name is required")
	}
	if len(out) > 80 {
		return "", errors.New("subagent task name is too long")
	}
	return out, nil
}

func childAgentPath(parentPath, taskName string) string {
	parentPath = strings.TrimSpace(parentPath)
	if parentPath == "" {
		parentPath = "/root"
	}
	if !strings.HasPrefix(parentPath, "/") {
		parentPath = "/" + parentPath
	}
	return strings.TrimRight(parentPath, "/") + "/" + taskName
}

func subAgentControllerKey(threadID string) string {
	return strings.TrimSpace(threadID)
}

func cleanSubAgentTargets(targets []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	return out
}
