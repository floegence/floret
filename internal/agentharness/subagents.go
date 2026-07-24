package agentharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/session/contextpolicy"
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
	SubAgentStatusClosing     SubAgentStatus = "closing"
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
	subAgentAdmittedInputIDKey = "subagent_input_id"

	subAgentApprovalEntryKind = "subagent_approval"
	subAgentDetailKindKey     = "kind"
	subAgentDetailTypeKey     = "type"
	subAgentApprovalStateKey  = "state"
	subAgentApprovalToolIDKey = "tool_id"
	subAgentApprovalNameKey   = "tool_name"
	subAgentApprovalKindKey   = "tool_kind"
	subAgentApprovalArgsKey   = "args_hash"
	subAgentApprovalReasonKey = "reason"

	toolDispatchEntryKind = "tool_dispatch"
	toolDispatchToolIDKey = "tool_id"
	toolDispatchNameKey   = "tool_name"
	toolDispatchKindKey   = "tool_kind"
	toolDispatchArgsKey   = "args_hash"

	toolActivityEntryKind = "tool_activity"
	toolActivityToolIDKey = "tool_id"
	toolActivityNameKey   = "tool_name"
	toolActivityKindKey   = "tool_kind"
	toolActivityArgsKey   = "args_hash"

	pendingToolSettlementEntryKind  = "pending_tool_settlement"
	pendingToolSettlementStateKey   = "state"
	pendingToolSettlementToolIDKey  = "tool_id"
	pendingToolSettlementNameKey    = "tool_name"
	pendingToolSettlementHandleKey  = "handle"
	pendingToolSettlementRunIDKey   = "run_id"
	pendingToolSettlementSummaryKey = "summary"

	subAgentLifecycleEntryKind = "subagent_lifecycle"
	subAgentLifecycleActionKey = "action"
	subAgentLifecycleReasonKey = "reason"

	subAgentContextPolicyEntryKind     = "subagent_context_policy"
	subAgentContextStatusEntryKind     = "subagent_context_status"
	subAgentContextCompactionEntryKind = "subagent_context_compaction"
	subAgentContextProviderKey         = "provider"
	subAgentContextModelKey            = "model"
	subAgentContextPolicyKey           = "context_policy_json"
	subAgentContextStatusKey           = "context_status_json"
	subAgentContextCompactionKey       = "context_compaction_json"

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
	PublicationID   string
	ParentThreadID  string
	ParentTurnID    string
	ThreadID        string
	TaskName        string
	TaskDescription string
	Message         string
	Attachments     []session.MessageAttachment
	References      []session.MessageReference
	HostProfileRef  string
	ForkMode        SubAgentForkMode
	Labels          engine.RunLabels
}

type SendSubAgentInputOptions struct {
	InputRequestID string
	ParentThreadID string
	ChildThreadID  string
	Message        string
	Attachments    []session.MessageAttachment
	References     []session.MessageReference
	Interrupt      bool
	Labels         engine.RunLabels
}

type PublishSubAgentPendingToolCompletionOptions struct {
	InputRequestID string
	ParentThreadID string
	ChildThreadID  string
	Target         sessiontree.PendingToolSettlementTarget
	Status         PendingToolCompletionStatus
	Summary        string
	Output         string
	Message        string
	Attachments    []session.MessageAttachment
	References     []session.MessageReference
	Labels         engine.RunLabels
}

type WaitSubAgentsOptions struct {
	ParentThreadID string
	ChildThreadIDs []string
	Timeout        time.Duration
}

type CloseSubAgentOptions struct {
	CloseOperationID string
	ParentThreadID   string
	ChildThreadID    string
	Reason           string
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
	ThreadID        string           `json:"thread_id"`
	Path            string           `json:"path"`
	TaskName        string           `json:"task_name"`
	TaskDescription string           `json:"task_description,omitempty"`
	ParentThreadID  string           `json:"parent_thread_id"`
	ParentTurnID    string           `json:"parent_turn_id,omitempty"`
	HostProfileRef  string           `json:"host_profile_ref,omitempty"`
	ForkMode        SubAgentForkMode `json:"fork_mode,omitempty"`
	Status          SubAgentStatus   `json:"status"`
	LatestTurnID    string           `json:"latest_turn_id,omitempty"`
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
	Events           []SubAgentDetailEvent        `json:"events"`
	ActivityTimeline observation.ActivityTimeline `json:"activity_timeline"`
	Context          ThreadContextSnapshot        `json:"context,omitempty"`
	NextOrdinal      int64                        `json:"next_ordinal,omitempty"`
	HasMore          bool                         `json:"has_more,omitempty"`
	RetainedFrom     int64                        `json:"retained_from,omitempty"`
	GeneratedAt      time.Time                    `json:"generated_at"`
}

type ThreadContextSnapshot struct {
	Model       ThreadContextModel         `json:"model,omitempty"`
	Policy      ThreadContextPolicy        `json:"policy,omitempty"`
	Usage       *observation.ContextStatus `json:"usage,omitempty"`
	Compactions []ThreadContextCompaction  `json:"compactions,omitempty"`
	UpdatedAt   time.Time                  `json:"updated_at,omitempty"`
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
	RunID               string    `json:"run_id,omitempty"`
	ThreadID            string    `json:"thread_id,omitempty"`
	TurnID              string    `json:"turn_id,omitempty"`
	Step                int       `json:"step,omitempty"`
	OperationID         string    `json:"operation_id,omitempty"`
	RequestID           string    `json:"request_id,omitempty"`
	Phase               string    `json:"phase,omitempty"`
	Status              string    `json:"status,omitempty"`
	Trigger             string    `json:"trigger,omitempty"`
	Reason              string    `json:"reason,omitempty"`
	Source              string    `json:"source,omitempty"`
	TokensBefore        int64     `json:"tokens_before,omitempty"`
	TokensAfterEstimate int64     `json:"tokens_after_estimate,omitempty"`
	Error               string    `json:"error,omitempty"`
	ObservedAt          time.Time `json:"observed_at,omitempty"`
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
	SubAgentDetailEventToolDispatch     SubAgentDetailEventKind = "tool_dispatch"
	SubAgentDetailEventToolActivity     SubAgentDetailEventKind = "tool_activity"
	SubAgentDetailEventToolResult       SubAgentDetailEventKind = "tool_result"
	SubAgentDetailEventTurnMarker       SubAgentDetailEventKind = "turn_marker"
	SubAgentDetailEventCompaction       SubAgentDetailEventKind = "compaction"
	SubAgentDetailEventError            SubAgentDetailEventKind = "error"
	SubAgentDetailEventApproval         SubAgentDetailEventKind = "approval"
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
	Role        string                            `json:"role,omitempty"`
	Kind        string                            `json:"kind,omitempty"`
	Preview     string                            `json:"preview,omitempty"`
	Content     string                            `json:"content,omitempty"`
	Attachments []session.MessageAttachment       `json:"attachments,omitempty"`
	References  []session.MessageReference        `json:"references,omitempty"`
	Reasoning   string                            `json:"reasoning,omitempty"`
	Activity    *observation.ActivityPresentation `json:"activity,omitempty"`
}

type SubAgentDetailToolCall struct {
	ID            string                       `json:"id,omitempty"`
	Name          string                       `json:"name,omitempty"`
	ArgsPreview   string                       `json:"args_preview,omitempty"`
	ArgsJSON      string                       `json:"args_json,omitempty"`
	ArgsHash      string                       `json:"args_hash,omitempty"`
	ControlSignal *SubAgentDetailControlSignal `json:"control_signal,omitempty"`
}

type SubAgentDetailControlSignal struct {
	Name        string         `json:"name,omitempty"`
	CallID      string         `json:"call_id,omitempty"`
	Disposition string         `json:"disposition,omitempty"`
	Text        string         `json:"text,omitempty"`
	ArgsHash    string         `json:"args_hash,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type SubAgentDetailToolResult struct {
	CallID          string        `json:"call_id,omitempty"`
	ToolName        string        `json:"tool_name,omitempty"`
	EffectAttemptID string        `json:"effect_attempt_id,omitempty"`
	Status          string        `json:"status,omitempty"`
	Preview         string        `json:"preview,omitempty"`
	Content         string        `json:"content,omitempty"`
	Truncated       bool          `json:"truncated,omitempty"`
	OriginalBytes   int           `json:"original_bytes,omitempty"`
	VisibleBytes    int           `json:"visible_bytes,omitempty"`
	OriginalLines   int           `json:"original_lines,omitempty"`
	VisibleLines    int           `json:"visible_lines,omitempty"`
	Strategy        string        `json:"strategy,omitempty"`
	ContentSHA256   string        `json:"content_sha256,omitempty"`
	FullOutput      *artifact.Ref `json:"full_output,omitempty"`
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

type subagentController struct {
	parentThreadID string
	threadID       string
	path           string
	taskName       string
	thread         *Thread

	mu      sync.Mutex
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
	publicationID := strings.TrimSpace(opts.PublicationID)
	if publicationID == "" {
		return SubAgentSnapshot{}, errors.New("subagent publication id is required")
	}
	parentID := strings.TrimSpace(opts.ParentThreadID)
	if parentID == "" {
		return SubAgentSnapshot{}, errors.New("parent thread id is required")
	}
	childID := strings.TrimSpace(opts.ThreadID)
	if childID == "" {
		return SubAgentSnapshot{}, errors.New("subagent thread id is required")
	}
	message := session.Message{Role: session.User, Content: strings.TrimSpace(opts.Message), Attachments: session.CloneMessageAttachments(opts.Attachments), References: append([]session.MessageReference(nil), opts.References...)}
	if message.Content == "" && len(message.Attachments) == 0 {
		return SubAgentSnapshot{}, errors.New("subagent message or attachments are required")
	}
	taskName, err := normalizeSubAgentTaskName(opts.TaskName)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	parentMeta, err := h.subAgentParentThread(ctx, parentID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	forkMode := opts.ForkMode
	if forkMode != SubAgentForkNone && forkMode != SubAgentForkFullPath {
		return SubAgentSnapshot{}, fmt.Errorf("unsupported subagent fork mode %q", forkMode)
	}
	now := h.now()
	childMeta := sessiontree.ThreadMeta{
		ID:              childID,
		ParentThreadID:  parentID,
		ParentTurnID:    strings.TrimSpace(opts.ParentTurnID),
		TaskName:        taskName,
		TaskDescription: strings.TrimSpace(opts.TaskDescription),
		AgentPath:       childAgentPath(parentMeta.AgentPath, taskName),
		HostProfileRef:  strings.TrimSpace(opts.HostProfileRef),
		ForkMode:        string(forkMode),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	var forkOptions *sessiontree.ForkOptions
	var artifactClosure artifact.Closure
	var forkTurnIDs map[string]string
	var forkRunIDs map[string]string
	if forkMode == SubAgentForkFullPath {
		path, err := h.options.Repo.Path(ctx, parentID, parentMeta.LeafID)
		if err != nil {
			return SubAgentSnapshot{}, err
		}
		artifactRepo, ok := h.options.Repo.(sessiontree.ArtifactAuthorityRepo)
		if !ok {
			return SubAgentSnapshot{}, errors.New("session tree repo does not support artifact authority operations")
		}
		entryIDs := make([]string, len(path))
		for index, entry := range path {
			entryIDs[index] = entry.ID
		}
		artifactClosure, err = artifactRepo.ArtifactClosure(ctx, sessiontree.ArtifactClosureRequest{
			SourceThreadID:      parentID,
			DestinationThreadID: childID,
			EntryIDs:            entryIDs,
		})
		if err != nil {
			return SubAgentSnapshot{}, err
		}
		forkTurnIDs, forkRunIDs = subAgentForkIdentityRewrite(publicationID, path)
		forkOptions = &sessiontree.ForkOptions{
			SourceThreadID:       parentID,
			EntryID:              parentMeta.LeafID,
			EntryIDPinned:        true,
			ExpectedSourceLeafID: parentMeta.LeafID,
			NewThreadID:          childID,
			Now:                  now,
			DestinationMeta: &sessiontree.ForkDestinationMeta{
				ParentThreadID:  childMeta.ParentThreadID,
				ParentTurnID:    childMeta.ParentTurnID,
				TaskName:        childMeta.TaskName,
				TaskDescription: childMeta.TaskDescription,
				AgentPath:       childMeta.AgentPath,
				HostProfileRef:  childMeta.HostProfileRef,
				ForkMode:        childMeta.ForkMode,
			},
			ArtifactClosure: artifact.CloneClosure(artifactClosure),
			TurnIDMap:       forkTurnIDs,
			RunIDMap:        forkRunIDs,
			RewriteEntry:    rewriteForkContextEntry,
		}
	}
	fingerprint, err := subAgentPublicationFingerprint(
		publicationID, childMeta, forkMode, parentMeta.LeafID, artifactClosure.Fingerprint, message, opts.Labels,
	)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	repo, ok := h.options.Repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		return SubAgentSnapshot{}, errors.New("session tree repo does not support subagent authority operations")
	}
	published, err := repo.PublishSubAgent(ctx, sessiontree.PublishSubAgentRequest{
		PublicationID:      publicationID,
		RequestFingerprint: fingerprint,
		ParentThreadID:     parentID,
		ChildMeta:          childMeta,
		ForkOptions:        forkOptions,
		ArtifactClosure:    artifact.CloneClosure(artifactClosure),
		Message:            message,
		HostLabels:         cloneStringMap(opts.Labels.Host),
		CorrelationLabels:  cloneStringMap(opts.Labels.Correlation),
		Now:                now,
	})
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	thread := h.cacheThread(childID)
	if _, err := h.ensureSubAgentController(ctx, published.Thread, thread); err != nil {
		return SubAgentSnapshot{}, err
	}
	if !published.Replayed {
		if forkMode == SubAgentForkNone {
			h.emit(HarnessEvent{Type: EventThreadStarted, ThreadID: childID, ParentID: parentID})
		} else {
			h.emit(HarnessEvent{Type: EventThreadForked, ThreadID: childID, EntryID: published.Thread.ForkedFromEntryID, Metadata: map[string]string{"source_thread_id": parentID}})
		}
		h.emit(HarnessEvent{
			Type: EventSubAgentSpawned, ThreadID: parentID, Message: message.Content,
			Metadata: map[string]string{
				"subagent_thread_id": childID,
				"subagent_path":      childMeta.AgentPath,
				"task_name":          taskName,
				"task_description":   childMeta.TaskDescription,
				"host_profile_ref":   childMeta.HostProfileRef,
				"subagent_input_id":  published.Input.SubAgentInputID,
			},
		})
		h.notifySubAgentUpdate()
	}
	return h.subAgentSnapshot(ctx, childID)
}

func (h *AgentHarness) SendSubAgentInput(ctx context.Context, opts SendSubAgentInputOptions) (SubAgentSnapshot, error) {
	requestID := strings.TrimSpace(opts.InputRequestID)
	if requestID == "" {
		return SubAgentSnapshot{}, errors.New("subagent input request id is required")
	}
	meta, err := h.resolveSubAgentMeta(ctx, opts.ParentThreadID, opts.ChildThreadID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	message := session.Message{Role: session.User, Content: strings.TrimSpace(opts.Message), Attachments: session.CloneMessageAttachments(opts.Attachments), References: append([]session.MessageReference(nil), opts.References...)}
	if message.Content == "" && len(message.Attachments) == 0 {
		return SubAgentSnapshot{}, errors.New("subagent message or attachments are required")
	}
	fingerprint, err := subAgentInputFingerprint(requestID, strings.TrimSpace(opts.ParentThreadID), meta.ID, message, opts.Labels, opts.Interrupt)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	repo, ok := h.options.Repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		return SubAgentSnapshot{}, errors.New("session tree repo does not support subagent authority operations")
	}
	input, replayed, err := repo.PublishSubAgentInput(ctx, sessiontree.PublishSubAgentInputRequest{
		InputRequestID:     requestID,
		RequestFingerprint: fingerprint,
		ParentThreadID:     strings.TrimSpace(opts.ParentThreadID),
		ChildThreadID:      meta.ID,
		Message:            message,
		HostLabels:         cloneStringMap(opts.Labels.Host),
		CorrelationLabels:  cloneStringMap(opts.Labels.Correlation),
		Interrupt:          opts.Interrupt,
		Now:                h.now(),
	})
	if err != nil {
		if errors.Is(err, sessiontree.ErrThreadClosed) {
			return SubAgentSnapshot{}, ErrSubAgentClosed
		}
		return SubAgentSnapshot{}, err
	}
	if !replayed {
		h.emit(HarnessEvent{
			Type: EventSubAgentInput, ThreadID: strings.TrimSpace(opts.ParentThreadID), Message: message.Content,
			Metadata: map[string]string{
				"subagent_thread_id": meta.ID,
				"subagent_path":      meta.AgentPath,
				"subagent_input_id":  input.SubAgentInputID,
				"interrupt":          fmt.Sprintf("%t", opts.Interrupt),
			},
		})
		h.notifySubAgentUpdate()
	}
	return h.subAgentSnapshot(ctx, meta.ID)
}

func (h *AgentHarness) PublishSubAgentPendingToolCompletion(ctx context.Context, opts PublishSubAgentPendingToolCompletionOptions) (SubAgentSnapshot, error) {
	requestID := strings.TrimSpace(opts.InputRequestID)
	if requestID == "" {
		return SubAgentSnapshot{}, errors.New("subagent pending tool completion input request id is required")
	}
	parentID := strings.TrimSpace(opts.ParentThreadID)
	childID := strings.TrimSpace(opts.ChildThreadID)
	meta, err := h.resolveSubAgentMeta(ctx, parentID, childID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if strings.TrimSpace(opts.Target.ThreadID) != childID {
		return SubAgentSnapshot{}, errors.New("subagent pending tool completion target thread identity mismatch")
	}
	message := session.Message{Role: session.User, Content: strings.TrimSpace(opts.Message), Attachments: session.CloneMessageAttachments(opts.Attachments), References: append([]session.MessageReference(nil), opts.References...)}
	if message.Content == "" && len(message.Attachments) == 0 {
		return SubAgentSnapshot{}, errors.New("subagent pending tool completion requires message or attachments")
	}
	settlement, err := normalizePendingToolSettlement(PendingToolSettlement{
		TurnID: opts.Target.TurnID, RunID: opts.Target.RunID,
		ToolCallID: opts.Target.ToolCallID, ToolName: opts.Target.ToolName, Handle: opts.Target.Handle,
		EffectAttemptID: opts.Target.EffectAttemptID,
		Status:          pendingToolSettlementStatusFromCompletion(opts.Status), Summary: opts.Summary, Output: opts.Output,
	})
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	settlementEntry, settlementFingerprint, err := pendingToolSettlementAuthorityEntry(childID, settlement)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	fingerprint, err := subAgentPendingToolCompletionFingerprint(requestID, parentID, childID, opts.Target,
		opts.Status, settlement.Summary, settlement.Output, message, opts.Labels)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	repo, ok := h.options.Repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		return SubAgentSnapshot{}, errors.New("session tree repo does not support subagent authority operations")
	}
	published, err := repo.PublishSubAgentPendingToolCompletion(ctx, sessiontree.PublishSubAgentPendingToolCompletionRequest{
		InputRequestID: requestID, RequestFingerprint: fingerprint, SettlementFingerprint: settlementFingerprint,
		ParentThreadID: parentID, ChildThreadID: childID, Target: opts.Target, Settlement: settlementEntry,
		Message: message, HostLabels: cloneStringMap(opts.Labels.Host), CorrelationLabels: cloneStringMap(opts.Labels.Correlation), Now: h.now(),
	})
	if err != nil {
		if errors.Is(err, sessiontree.ErrThreadClosed) || errors.Is(err, sessiontree.ErrSubAgentClosing) {
			return SubAgentSnapshot{}, ErrSubAgentClosed
		}
		if errors.Is(err, sessiontree.ErrPendingToolTurnNotFound) {
			return SubAgentSnapshot{}, ErrPendingToolSettlementTargetTurnNotFound
		}
		if errors.Is(err, sessiontree.ErrPendingToolRunNotFound) {
			return SubAgentSnapshot{}, ErrPendingToolSettlementTargetRunNotFound
		}
		if errors.Is(err, sessiontree.ErrPendingToolNotFound) {
			return SubAgentSnapshot{}, ErrPendingToolSettlementTargetToolNotFound
		}
		if errors.Is(err, sessiontree.ErrPendingToolNotPending) {
			return SubAgentSnapshot{}, ErrPendingToolSettlementTargetNotActive
		}
		if errors.Is(err, sessiontree.ErrSubAgentRequestConflict) {
			return SubAgentSnapshot{}, err
		}
		if errors.Is(err, sessiontree.ErrRequestConflict) {
			return SubAgentSnapshot{}, ErrPendingToolSettlementConflict
		}
		return SubAgentSnapshot{}, err
	}
	if !published.Replayed {
		if !published.SettlementReplayed {
			h.emitEntryCommitted(published.Settlement, opts.Target.RunID)
			h.emit(HarnessEvent{Type: EventEntryAppended, RunID: opts.Target.RunID, ThreadID: childID,
				TurnID: opts.Target.TurnID, EntryID: published.Settlement.ID, ParentID: published.Settlement.ParentID, Message: pendingToolSettlementEntryKind})
		}
		h.emit(HarnessEvent{
			Type: EventSubAgentInput, ThreadID: parentID, Message: message.Content,
			Metadata: map[string]string{
				"subagent_thread_id": childID, "subagent_path": meta.AgentPath,
				"subagent_input_id": published.Input.SubAgentInputID, "pending_tool_completion": "true",
			},
		})
		h.notifySubAgentUpdate()
	}
	return h.subAgentSnapshot(ctx, childID)
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
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		if err := h.activateSubAgentTargets(ctx, opts.ParentThreadID, targets); err != nil {
			return WaitSubAgentsResult{}, err
		}
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
		case <-poll.C:
		case <-h.subAgentUpdateChannel():
		}
	}
}

func (h *AgentHarness) activateSubAgentTargets(ctx context.Context, parentThreadID string, childThreadIDs []string) error {
	for _, childThreadID := range childThreadIDs {
		meta, err := h.resolveSubAgentMeta(ctx, parentThreadID, childThreadID)
		if err != nil {
			return err
		}
		if meta.IsClosed() {
			continue
		}
		ctrl, err := h.ensureSubAgentController(ctx, meta, h.cacheThread(meta.ID))
		if err != nil {
			return err
		}
		h.startNextSubAgentTurn(ctrl)
	}
	return nil
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
	meta, err := h.resolveSubAgentDescendantMeta(ctx, opts.ParentThreadID, opts.ChildThreadID)
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
	activityContext := subAgentDetailActivityContext{
		resultCallIDs: subAgentDetailResultCallIDs(entries),
		runIDs:        subAgentDetailTurnRunIDs(entries),
	}
	generatedAt := h.now()
	activityTimeline := h.subAgentDetailActivityTimeline(entries, retainedFrom, activityContext, generatedAt)
	contextSnapshot, err := h.subAgentDetailContext(entries, retainedFrom, activityContext, generatedAt)
	if err != nil {
		return SubAgentDetail{}, err
	}
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
		Snapshot:         snapshot,
		Events:           events,
		ActivityTimeline: activityTimeline,
		Context:          contextSnapshot,
		NextOrdinal:      nextOrdinal,
		HasMore:          hasMore,
		RetainedFrom:     retainedFrom,
		GeneratedAt:      generatedAt,
	}, nil
}

func (h *AgentHarness) subAgentDetailActivityTimeline(entries []sessiontree.Entry, retainedFrom int64, activityContext subAgentDetailActivityContext, generatedAt time.Time) observation.ActivityTimeline {
	observed := make([]observation.Event, 0, len(entries))
	for index, entry := range entries {
		ordinal := int64(index + 1)
		if ordinal < retainedFrom {
			continue
		}
		detail, ok := h.subAgentDetailEvent(entry, ordinal, false, activityContext)
		if !ok {
			continue
		}
		ev, ok := subAgentDetailObservationEvent(detail, entry, activityContext)
		if !ok {
			continue
		}
		observed = append(observed, ev)
	}
	return observation.BuildActivityTimeline(observation.ActivityRunMeta{}, observed, generatedAt.UnixMilli())
}

func (h *AgentHarness) subAgentDetailContext(entries []sessiontree.Entry, retainedFrom int64, activityContext subAgentDetailActivityContext, generatedAt time.Time) (ThreadContextSnapshot, error) {
	out := ThreadContextSnapshot{}
	compactions := make([]ThreadContextCompaction, 0)
	seenCompactions := map[string]int{}
	latestContextObservedAt := time.Time{}
	hasPolicy := false
	for index, entry := range entries {
		if int64(index+1) < retainedFrom {
			continue
		}
		switch {
		case entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentDetailKindKey] == subAgentContextPolicyEntryKind:
			providerName := strings.TrimSpace(entry.Metadata[subAgentContextProviderKey])
			modelName := strings.TrimSpace(entry.Metadata[subAgentContextModelKey])
			if providerName == "" || modelName == "" {
				return ThreadContextSnapshot{}, errors.New("thread context policy requires provider and model")
			}
			policy, err := subAgentDetailContextPolicy(entry.Metadata)
			if err != nil {
				return ThreadContextSnapshot{}, err
			}
			out.Model.Provider = providerName
			out.Model.Model = modelName
			out.Policy = policy
			hasPolicy = true
			latestContextObservedAt = maxTime(latestContextObservedAt, entry.CreatedAt)
		case entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentDetailKindKey] == subAgentContextStatusEntryKind:
			status, err := subAgentDetailContextStatus(entry.Metadata)
			if err != nil {
				return ThreadContextSnapshot{}, err
			}
			if status.ThreadID != entry.ThreadID || status.TurnID != entry.TurnID {
				return ThreadContextSnapshot{}, errors.New("thread context status identity mismatch")
			}
			if runID := activityContext.runIDForTurn(entry.TurnID); runID != "" && status.RunID != runID {
				return ThreadContextSnapshot{}, errors.New("thread context status run identity mismatch")
			}
			if hasPolicy && (status.Provider != out.Model.Provider || status.Model != out.Model.Model) {
				return ThreadContextSnapshot{}, errors.New("thread context status model identity mismatch")
			}
			out.Usage = &status
			latestContextObservedAt = maxTime(latestContextObservedAt, nonZeroTime(status.ObservedAt, entry.CreatedAt))
		case entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentDetailKindKey] == subAgentContextCompactionEntryKind:
			compact, err := subAgentDetailContextCompaction(entry.Metadata)
			if err != nil {
				return ThreadContextSnapshot{}, err
			}
			if compact.ThreadID != entry.ThreadID || compact.TurnID != entry.TurnID {
				return ThreadContextSnapshot{}, errors.New("thread context compaction identity mismatch")
			}
			compactions = upsertSubAgentDetailCompaction(compactions, seenCompactions, compact)
			latestContextObservedAt = maxTime(latestContextObservedAt, nonZeroTime(compact.ObservedAt, entry.CreatedAt))
		}
	}
	if !hasPolicy && (out.Usage != nil || len(compactions) > 0) {
		return ThreadContextSnapshot{}, errors.New("thread context lifecycle is missing its policy")
	}
	if out.Usage != nil && (out.Usage.Provider != out.Model.Provider || out.Usage.Model != out.Model.Model) {
		return ThreadContextSnapshot{}, errors.New("thread context status model identity mismatch")
	}
	out.Compactions = compactions
	if !latestContextObservedAt.IsZero() {
		out.UpdatedAt = latestContextObservedAt
	} else if !generatedAt.IsZero() && (out.Model.Provider != "" || out.Model.Model != "" || out.Policy.ContextWindowTokens > 0 || out.Usage != nil || len(out.Compactions) > 0) {
		out.UpdatedAt = generatedAt
	}
	return out, nil
}

func subAgentDetailContextPolicy(metadata map[string]string) (ThreadContextPolicy, error) {
	raw := strings.TrimSpace(metadata[subAgentContextPolicyKey])
	if raw == "" {
		return ThreadContextPolicy{}, errors.New("thread context policy payload is required")
	}
	var policy ThreadContextPolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return ThreadContextPolicy{}, fmt.Errorf("decode thread context policy: %w", err)
	}
	if policy.ContextWindowTokens <= 0 {
		return ThreadContextPolicy{}, errors.New("thread context policy requires context window tokens")
	}
	return policy, nil
}

func subAgentDetailContextStatus(metadata map[string]string) (observation.ContextStatus, error) {
	raw := strings.TrimSpace(metadata[subAgentContextStatusKey])
	if raw == "" {
		return observation.ContextStatus{}, errors.New("thread context status payload is required")
	}
	var status observation.ContextStatus
	if err := json.Unmarshal([]byte(raw), &status); err != nil {
		return observation.ContextStatus{}, fmt.Errorf("decode thread context status: %w", err)
	}
	if err := status.Validate(); err != nil {
		return observation.ContextStatus{}, err
	}
	return status, nil
}

func subAgentDetailContextCompaction(metadata map[string]string) (ThreadContextCompaction, error) {
	raw := strings.TrimSpace(metadata[subAgentContextCompactionKey])
	if raw == "" {
		return ThreadContextCompaction{}, errors.New("thread context compaction payload is required")
	}
	var compact ThreadContextCompaction
	if err := json.Unmarshal([]byte(raw), &compact); err != nil {
		return ThreadContextCompaction{}, fmt.Errorf("decode thread context compaction: %w", err)
	}
	if strings.TrimSpace(compact.OperationID) == "" {
		return ThreadContextCompaction{}, errors.New("thread context compaction requires operation id")
	}
	if strings.TrimSpace(compact.RunID) == "" || strings.TrimSpace(compact.ThreadID) == "" || strings.TrimSpace(compact.RequestID) == "" {
		return ThreadContextCompaction{}, errors.New("thread context compaction requires run, thread, and request identities")
	}
	if err := (observation.CompactionEvent{Phase: observation.CompactionPhase(compact.Phase), Status: observation.CompactionStatus(compact.Status)}).Validate(); err != nil {
		return ThreadContextCompaction{}, err
	}
	return compact, nil
}

func upsertSubAgentDetailCompaction(compactions []ThreadContextCompaction, seen map[string]int, compact ThreadContextCompaction) []ThreadContextCompaction {
	key := subAgentDetailContextCompactionKey(compact)
	if key == "" {
		compactions = append(compactions, compact)
		return compactions
	}
	if index, ok := seen[key]; ok {
		compactions[index] = compact
		return compactions
	}
	seen[key] = len(compactions)
	return append(compactions, compact)
}

func subAgentDetailContextCompactionKey(compact ThreadContextCompaction) string {
	return strings.TrimSpace(compact.OperationID)
}

func subAgentPublicContextPolicy(policy contextpolicy.Policy) ThreadContextPolicy {
	normalized := contextpolicy.Normalize(policy)
	return ThreadContextPolicy{
		ContextWindowTokens:  normalized.ContextWindowTokens,
		MaxOutputTokens:      normalized.MaxOutputTokens,
		ReservedOutputTokens: normalized.ReservedOutputTokens,
	}
}

func subAgentContextPolicyMetadata(providerName, modelName string, policy contextpolicy.Policy) map[string]string {
	metadata := map[string]string{
		subAgentDetailKindKey:      subAgentContextPolicyEntryKind,
		subAgentDetailTypeKey:      subAgentContextPolicyEntryKind,
		subAgentContextProviderKey: strings.TrimSpace(providerName),
		subAgentContextModelKey:    strings.TrimSpace(modelName),
		subAgentContextPolicyKey:   mustSubAgentMetadataJSON(subAgentPublicContextPolicy(policy)),
	}
	return metadata
}

func mustSubAgentMetadataJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func nonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func maxTime(left, right time.Time) time.Time {
	if left.IsZero() {
		return right
	}
	if right.IsZero() || left.After(right) {
		return left
	}
	return right
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
	activityContext := subAgentDetailActivityContext{
		resultCallIDs: subAgentDetailResultCallIDs(entries),
		runIDs:        subAgentDetailTurnRunIDs(entries),
	}
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

func (h *AgentHarness) detailEventsForCanonicalEntries(entries []sessiontree.Entry, includeRaw bool) []SubAgentDetailEvent {
	activityContext := subAgentDetailActivityContext{
		resultCallIDs: subAgentDetailResultCallIDs(entries),
		runIDs:        subAgentDetailTurnRunIDs(entries),
	}
	events := make([]SubAgentDetailEvent, 0, len(entries))
	for index, entry := range entries {
		event, ok := h.subAgentDetailEvent(entry, int64(index+1), includeRaw, activityContext)
		if ok {
			events = append(events, event)
		}
	}
	return events
}

func (h *AgentHarness) ReadTurnDetailEvents(ctx context.Context, threadID, turnID, runID string, includeRaw bool) (ThreadDetailEvents, bool, error) {
	if h == nil || h.options.Repo == nil {
		return ThreadDetailEvents{}, false, errors.New("agent harness is not initialized")
	}
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	runID = strings.TrimSpace(runID)
	canonical, ok := h.options.Repo.(sessiontree.CanonicalTurnRepo)
	if !ok {
		return ThreadDetailEvents{}, false, errors.New("session tree repo does not support canonical turn reads")
	}
	entries, found, err := canonical.CanonicalTurnEntries(ctx, threadID, turnID, runID)
	if err != nil {
		return ThreadDetailEvents{}, found, err
	}
	if !found {
		return ThreadDetailEvents{}, false, nil
	}
	events := h.detailEventsForCanonicalEntries(entries, includeRaw)
	return ThreadDetailEvents{Events: events, NextOrdinal: int64(len(events)), RetainedFrom: 1, GeneratedAt: h.now()}, true, nil
}

// ReadLatestThreadDetailEvents reads only the active-path entries required to
// project the latest admitted turn. It walks backwards until it has the latest
// started marker and the canonical user input used by that turn.
func (h *AgentHarness) ReadLatestThreadDetailEvents(ctx context.Context, threadID string, includeRaw bool) (ThreadDetailEvents, error) {
	if h == nil {
		return ThreadDetailEvents{}, errors.New("agent harness is nil")
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return ThreadDetailEvents{}, errors.New("thread id is required")
	}
	meta, err := h.options.Repo.Thread(ctx, threadID)
	if err != nil {
		return ThreadDetailEvents{}, err
	}
	type pathEntry struct {
		entry   sessiontree.Entry
		ordinal int64
	}
	selected := make([]pathEntry, 0, DefaultThreadDetailEventLimit)
	beforeEntryID := ""
	latestTurnID := ""
	for {
		page, err := h.options.Repo.PathPage(ctx, threadID, meta.LeafID, beforeEntryID, MaxThreadDetailEventLimit)
		if err != nil {
			return ThreadDetailEvents{}, err
		}
		for index, entry := range page.Entries {
			ordinal := page.NewestOrdinal - int64(index)
			selected = append(selected, pathEntry{entry: entry, ordinal: ordinal})
			if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
				continue
			}
			candidateTurnID := strings.TrimSpace(entry.TurnID)
			if candidateTurnID == "" || strings.TrimSpace(entry.Metadata["run_id"]) == "" {
				return ThreadDetailEvents{}, errors.New("latest turn started marker has incomplete identity")
			}
			retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return ThreadDetailEvents{}, err
			}
			if retrySource != nil {
				latestTurnID = candidateTurnID
				break
			}
			for _, candidate := range selected {
				if candidate.entry.Type == sessiontree.EntryUserMessage && strings.TrimSpace(candidate.entry.TurnID) == candidateTurnID {
					latestTurnID = candidateTurnID
					break
				}
			}
			if latestTurnID != "" {
				break
			}
		}
		if latestTurnID != "" {
			break
		}
		if !page.HasMore {
			break
		}
		if strings.TrimSpace(page.NextEntryID) == "" {
			return ThreadDetailEvents{}, errors.New("session tree path pagination did not provide a continuation entry")
		}
		beforeEntryID = page.NextEntryID
	}
	if latestTurnID == "" {
		return ThreadDetailEvents{GeneratedAt: h.now()}, nil
	}
	slices.Reverse(selected)
	entries := make([]sessiontree.Entry, 0, len(selected))
	for _, item := range selected {
		entries = append(entries, item.entry)
	}
	activityContext := subAgentDetailActivityContext{
		resultCallIDs: subAgentDetailResultCallIDs(entries),
		runIDs:        subAgentDetailTurnRunIDs(entries),
	}
	events := make([]SubAgentDetailEvent, 0, len(entries))
	var nextOrdinal int64
	for _, item := range selected {
		event, ok := h.subAgentDetailEvent(item.entry, item.ordinal, includeRaw, activityContext)
		if !ok {
			continue
		}
		events = append(events, event)
		if item.ordinal > nextOrdinal {
			nextOrdinal = item.ordinal
		}
	}
	return ThreadDetailEvents{
		Events:       events,
		NextOrdinal:  nextOrdinal,
		RetainedFrom: selected[0].ordinal,
		GeneratedAt:  h.now(),
	}, nil
}

func (h *AgentHarness) latestThreadDetailEventsFromPath(path []sessiontree.Entry, includeRaw bool) (ThreadDetailEvents, error) {
	latestStartedIndex := -1
	latestTurnID := ""
	for index := len(path) - 1; index >= 0; index-- {
		entry := path[index]
		if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
			continue
		}
		latestTurnID = strings.TrimSpace(entry.TurnID)
		if latestTurnID == "" || strings.TrimSpace(entry.Metadata["run_id"]) == "" {
			return ThreadDetailEvents{}, errors.New("latest turn started marker has incomplete identity")
		}
		latestStartedIndex = index
		break
	}
	if latestStartedIndex < 0 {
		return ThreadDetailEvents{GeneratedAt: h.now()}, nil
	}
	admitted := false
	retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(path[latestStartedIndex])
	if err != nil {
		return ThreadDetailEvents{}, err
	}
	if retrySource != nil {
		admitted = true
	}
	for _, entry := range path[latestStartedIndex+1:] {
		if entry.Type == sessiontree.EntryUserMessage && strings.TrimSpace(entry.TurnID) == latestTurnID {
			admitted = true
			break
		}
	}
	if !admitted {
		return ThreadDetailEvents{GeneratedAt: h.now()}, nil
	}
	entries := path[latestStartedIndex:]
	activityContext := subAgentDetailActivityContext{
		resultCallIDs: subAgentDetailResultCallIDs(entries),
		runIDs:        subAgentDetailTurnRunIDs(entries),
	}
	events := make([]SubAgentDetailEvent, 0, len(entries))
	var nextOrdinal int64
	for offset, entry := range entries {
		ordinal := int64(latestStartedIndex + offset + 1)
		event, ok := h.subAgentDetailEvent(entry, ordinal, includeRaw, activityContext)
		if !ok {
			continue
		}
		events = append(events, event)
		nextOrdinal = ordinal
	}
	return ThreadDetailEvents{
		Events:       events,
		NextOrdinal:  nextOrdinal,
		RetainedFrom: int64(latestStartedIndex + 1),
		GeneratedAt:  h.now(),
	}, nil
}

func (h *AgentHarness) ReadThreadContext(ctx context.Context, threadID string) (ThreadContextSnapshot, error) {
	if h == nil {
		return ThreadContextSnapshot{}, errors.New("agent harness is nil")
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return ThreadContextSnapshot{}, errors.New("thread id is required")
	}
	thread := h.cacheThread(threadID)
	journal, err := thread.Journal(ctx)
	if err != nil {
		return ThreadContextSnapshot{}, err
	}
	entries := journal.Path
	activityContext := subAgentDetailActivityContext{
		resultCallIDs: subAgentDetailResultCallIDs(entries),
		runIDs:        subAgentDetailTurnRunIDs(entries),
	}
	return h.subAgentDetailContext(entries, threadDetailRetainedFrom(entries), activityContext, h.now())
}

func threadDetailRetainedFrom(entries []sessiontree.Entry) int64 {
	if len(entries) == 0 {
		return 0
	}
	return 1
}

func subAgentDetailRetainedFrom(entries []sessiontree.Entry) int64 {
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
		event.ToolResult = subAgentDetailToolResult(entry.Message, entry.Metadata, includeRaw)
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
		case subAgentApprovalEntryKind:
			event.Kind = SubAgentDetailEventApproval
			if event.Type == "" {
				event.Type = subAgentApprovalEntryKind
			}
			event.Approval = subAgentDetailApproval(entry.Metadata)
		case toolDispatchEntryKind:
			event.Kind = SubAgentDetailEventToolDispatch
			if event.Type == "" {
				event.Type = string(observation.EventTypeToolDispatchStarted)
			}
			event.Message = subAgentDetailMessage(entry.Message, includeRaw)
			event.ToolCall = subAgentDetailToolDispatch(entry)
		case toolActivityEntryKind:
			event.Kind = SubAgentDetailEventToolActivity
			if event.Type == "" {
				event.Type = string(observation.EventTypeToolActivityUpdated)
			}
			event.Message = subAgentDetailMessage(entry.Message, includeRaw)
			event.ToolCall = subAgentDetailToolActivity(entry)
		case pendingToolSettlementEntryKind:
			event.Kind = SubAgentDetailEventToolResult
			if event.Type == "" {
				event.Type = pendingToolSettlementEntryKind
			}
			event.Message = subAgentDetailMessage(entry.Message, includeRaw)
			event.ToolResult = subAgentDetailToolResult(entry.Message, entry.Metadata, includeRaw)
		case subAgentLifecycleEntryKind:
			event.Kind = SubAgentDetailEventCustom
			if event.Type == "" {
				event.Type = subAgentLifecycleEntryKind
			}
		case subAgentContextPolicyEntryKind, subAgentContextStatusEntryKind, subAgentContextCompactionEntryKind:
			return SubAgentDetailEvent{}, false
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
	case SubAgentDetailEventUserMessage, SubAgentDetailEventAssistantMessage, SubAgentDetailEventToolCall, SubAgentDetailEventToolDispatch, SubAgentDetailEventToolActivity, SubAgentDetailEventToolResult:
		return true
	default:
		return false
	}
}

type subAgentDetailActivityContext struct {
	resultCallIDs map[string]struct{}
	runIDs        map[string]string
}

func (c subAgentDetailActivityContext) hasResult(callID string) bool {
	callID = strings.TrimSpace(callID)
	if callID == "" || len(c.resultCallIDs) == 0 {
		return false
	}
	_, ok := c.resultCallIDs[callID]
	return ok
}

func (c subAgentDetailActivityContext) runIDForTurn(turnID string) string {
	if len(c.runIDs) == 0 {
		return ""
	}
	return strings.TrimSpace(c.runIDs[strings.TrimSpace(turnID)])
}

func subAgentDetailTurnRunIDs(entries []sessiontree.Entry) map[string]string {
	out := map[string]string{}
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
			continue
		}
		turnID := strings.TrimSpace(entry.TurnID)
		runID := strings.TrimSpace(entry.Metadata["run_id"])
		if turnID == "" || runID == "" {
			continue
		}
		out[turnID] = runID
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func subAgentDetailResultCallIDs(entries []sessiontree.Entry) map[string]struct{} {
	out := map[string]struct{}{}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentDetailKindKey] == pendingToolSettlementEntryKind {
			if callID := strings.TrimSpace(entry.Metadata[pendingToolSettlementToolIDKey]); callID != "" {
				out[callID] = struct{}{}
			}
			continue
		}
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
	runID := subAgentDetailRunID(detail, activityContext)
	timeline := observation.BuildActivityTimeline(observation.ActivityRunMeta{
		RunID:    runID,
		ThreadID: detail.ThreadID,
		TurnID:   detail.TurnID,
	}, []observation.Event{observed}, entry.CreatedAt.UnixMilli())
	return &timeline
}

func subAgentDetailObservationEvent(detail SubAgentDetailEvent, entry sessiontree.Entry, activityContext subAgentDetailActivityContext) (observation.Event, bool) {
	base := observation.Event{
		RunID:      subAgentDetailRunID(detail, activityContext),
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
	case SubAgentDetailEventToolDispatch:
		if detail.ToolCall == nil {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeToolDispatchStarted
		base.ToolID = detail.ToolCall.ID
		base.ToolName = detail.ToolCall.Name
		base.ToolKind = "local"
		if detail.Message != nil {
			base.Activity = observationActivityPresentation(entry.Message.Activity)
		}
		base.Metadata = subAgentDetailToolDispatchActivityMetadata(detail.Metadata)
		return base, true
	case SubAgentDetailEventToolActivity:
		if detail.ToolCall == nil {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeToolActivityUpdated
		base.ToolID = detail.ToolCall.ID
		base.ToolName = detail.ToolCall.Name
		base.ToolKind = "local"
		if detail.Message != nil {
			base.Activity = observationActivityPresentation(entry.Message.Activity)
		}
		base.Metadata = subAgentDetailToolActivityMetadata(detail.Metadata)
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
		base.Activity = observationActivityPresentation(entry.Message.Activity)
		if base.Activity == nil {
			base.Activity = subAgentDetailActivityPresentation("Tool approval", detail.Approval.State)
		}
		base.Metadata = subAgentDetailApprovalActivityMetadata(detail.Approval.Metadata)
		if detail.Approval.State == "rejected" || detail.Approval.State == "timed_out" {
			base.Error = "tool_approval_" + detail.Approval.State
		}
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

func subAgentDetailRunID(detail SubAgentDetailEvent, activityContext subAgentDetailActivityContext) string {
	if detail.Metadata != nil {
		if runID := strings.TrimSpace(detail.Metadata["run_id"]); runID != "" {
			return runID
		}
	}
	if detail.TurnMarker != nil {
		if runID := strings.TrimSpace(detail.TurnMarker.Metadata["run_id"]); runID != "" {
			return runID
		}
	}
	return activityContext.runIDForTurn(detail.TurnID)
}

func subAgentDetailToolResultActivityMetadata(result *SubAgentDetailToolResult) map[string]any {
	if result == nil {
		return nil
	}
	metadata := map[string]any{}
	switch result.Status {
	case string(observation.ActivityStatusError):
		metadata["error_present"] = true
		metadata["tool_result_status"] = string(observation.ActivityStatusError)
	case string(observation.ActivityStatusCanceled):
		metadata["tool_result_status"] = string(observation.ActivityStatusCanceled)
	case string(observation.ActivityStatusSuccess):
		metadata["tool_result_status"] = string(observation.ActivityStatusSuccess)
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

func subAgentDetailToolDispatch(entry sessiontree.Entry) *SubAgentDetailToolCall {
	id := firstSubAgentDetailNonEmpty(strings.TrimSpace(entry.Message.ToolCallID), strings.TrimSpace(entry.Metadata[toolDispatchToolIDKey]))
	name := firstSubAgentDetailNonEmpty(strings.TrimSpace(entry.Message.ToolName), strings.TrimSpace(entry.Metadata[toolDispatchNameKey]))
	if id == "" && name == "" {
		return nil
	}
	return &SubAgentDetailToolCall{
		ID:       id,
		Name:     name,
		ArgsHash: strings.TrimSpace(entry.Metadata[toolDispatchArgsKey]),
	}
}

func subAgentDetailToolActivity(entry sessiontree.Entry) *SubAgentDetailToolCall {
	id := firstSubAgentDetailNonEmpty(strings.TrimSpace(entry.Message.ToolCallID), strings.TrimSpace(entry.Metadata[toolActivityToolIDKey]))
	name := firstSubAgentDetailNonEmpty(strings.TrimSpace(entry.Message.ToolName), strings.TrimSpace(entry.Metadata[toolActivityNameKey]))
	if id == "" && name == "" {
		return nil
	}
	return &SubAgentDetailToolCall{
		ID:       id,
		Name:     name,
		ArgsHash: strings.TrimSpace(entry.Metadata[toolActivityArgsKey]),
	}
}

func subAgentDetailToolDispatchActivityMetadata(metadata map[string]string) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, key := range []string{"batch_index", "batch_size", "error_present"} {
		if value := strings.TrimSpace(metadata[key]); value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func subAgentDetailToolActivityMetadata(metadata map[string]string) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range metadata {
		switch key {
		case subAgentDetailKindKey, subAgentDetailTypeKey, toolActivityToolIDKey, toolActivityNameKey, toolActivityKindKey, toolActivityArgsKey:
			continue
		}
		if value = strings.TrimSpace(value); value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func subAgentDetailApprovalActivityType(state string) observation.EventType {
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
	activity := observationActivityPresentation(msg.Activity)
	if msg.Role == "" && msg.Kind == "" && msg.Content == "" && len(msg.Attachments) == 0 && len(msg.References) == 0 && msg.Reasoning == "" && activity == nil {
		return nil
	}
	out := &SubAgentDetailMessage{
		Role:        string(msg.Role),
		Kind:        string(msg.Kind),
		Preview:     safeSubAgentDetailPreview(msg.Content, 500),
		Attachments: session.CloneMessageAttachments(msg.Attachments),
		References:  append([]session.MessageReference(nil), msg.References...),
		Activity:    activity,
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
	if signal := session.CloneControlSignalView(msg.ControlSignal); signal != nil {
		out.ControlSignal = &SubAgentDetailControlSignal{
			Name:        signal.Name,
			CallID:      signal.CallID,
			Disposition: signal.Disposition,
			Text:        signal.OutputText,
			ArgsHash:    signal.ArgsHash,
			Payload:     signal.Payload,
		}
	}
	if includeRaw {
		out.ArgsJSON = args
	}
	return out
}

func subAgentDetailToolResult(msg session.Message, metadata map[string]string, includeRaw bool) *SubAgentDetailToolResult {
	if msg.ToolCallID == "" && msg.ToolName == "" && msg.Content == "" && msg.ToolResult == nil {
		return nil
	}
	out := &SubAgentDetailToolResult{
		CallID:          msg.ToolCallID,
		ToolName:        msg.ToolName,
		EffectAttemptID: strings.TrimSpace(metadata[sessiontree.PendingToolEffectAttemptIDKey]),
		Preview:         safeSubAgentDetailPreview(msg.Content, 800),
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
		OperationID:             entry.CompactionOperationID,
		RequestID:               entry.CompactionRequestID,
		Source:                  entry.CompactionSource,
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

func (h *AgentHarness) ValidateSubAgentAuthority(ctx context.Context, parentThreadID, childThreadID string) error {
	if h == nil {
		return errors.New("agent harness is nil")
	}
	_, err := h.resolveSubAgentMeta(ctx, parentThreadID, childThreadID)
	return err
}

func (h *AgentHarness) CloseSubAgent(ctx context.Context, opts CloseSubAgentOptions) (SubAgentSnapshot, error) {
	meta, err := h.resolveSubAgentMeta(ctx, opts.ParentThreadID, opts.ChildThreadID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	operationID := strings.TrimSpace(opts.CloseOperationID)
	if operationID == "" {
		return SubAgentSnapshot{}, errors.New("subagent close operation id is required")
	}
	reason := strings.TrimSpace(opts.Reason)
	if reason == "" {
		return SubAgentSnapshot{}, errors.New("subagent close reason is required")
	}
	closeRepo, ok := h.options.Repo.(sessiontree.SubAgentCloseAuthorityRepo)
	if !ok {
		return SubAgentSnapshot{}, errors.New("session tree repo does not support atomic subagent close authority")
	}
	key := subAgentControllerKey(meta.ID)
	h.mu.Lock()
	ctrl := h.subagents[key]
	h.mu.Unlock()
	thread := h.cacheThread(meta.ID)
	thread.authorityMu.Lock()
	var targetLease *sessiontree.TurnLease
	if local := thread.activeLeaseSnapshot(); strings.TrimSpace(local.OwnerID) != "" {
		copy := local
		targetLease = &copy
	}
	prepared, err := closeRepo.PrepareSubAgentClose(ctx, sessiontree.PrepareSubAgentCloseRequest{
		CloseOperationID: operationID,
		ParentThreadID:   strings.TrimSpace(opts.ParentThreadID),
		TargetThreadID:   meta.ID,
		Reason:           reason,
		TargetLease:      targetLease,
		Now:              h.now(),
	})
	if err != nil {
		thread.authorityMu.Unlock()
		return SubAgentSnapshot{}, err
	}
	var cancel context.CancelFunc
	var done chan struct{}
	if ctrl != nil {
		ctrl.mu.Lock()
		ctrl.closed = true
		cancel = ctrl.cancel
		done = ctrl.done
		ctrl.mu.Unlock()
	}
	if cancel != nil && prepared.Operation.State == sessiontree.SubAgentClosePrepared {
		cancel()
	}
	thread.authorityMu.Unlock()
	if done != nil && prepared.Operation.State == sessiontree.SubAgentClosePrepared {
		select {
		case <-done:
		case <-ctx.Done():
			return SubAgentSnapshot{}, ctx.Err()
		}
	}
	finished, err := closeRepo.FinishSubAgentClose(ctx, sessiontree.FinishSubAgentCloseRequest{
		CloseOperationID: operationID,
		ParentThreadID:   strings.TrimSpace(opts.ParentThreadID),
		TargetThreadID:   meta.ID,
		Reason:           reason,
		Now:              h.now(),
	})
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	for _, closedMeta := range finished.Threads {
		h.mu.Lock()
		closedController := h.subagents[subAgentControllerKey(closedMeta.ID)]
		h.mu.Unlock()
		if closedController != nil {
			closedController.mu.Lock()
			closedController.closed = true
			closedController.mu.Unlock()
		}
	}
	if !finished.Replayed {
		for _, entry := range finished.Entries {
			h.emit(HarnessEvent{Type: EventEntryAppended, ThreadID: entry.ThreadID, EntryID: entry.ID, ParentID: entry.ParentID, Message: subAgentLifecycleEntryKind})
		}
		h.emit(HarnessEvent{
			Type:     EventSubAgentClosed,
			ThreadID: strings.TrimSpace(opts.ParentThreadID),
			Metadata: map[string]string{
				"subagent_thread_id": meta.ID,
				"subagent_path":      meta.AgentPath,
				"close_operation_id": operationID,
			},
		})
	}
	h.notifySubAgentUpdate()
	return h.subAgentSnapshot(ctx, meta.ID)
}

func (h *AgentHarness) startNextSubAgentTurn(ctrl *subagentController) {
	ctrl.mu.Lock()
	if ctrl.closed || ctrl.running {
		ctrl.mu.Unlock()
		return
	}
	thread := ctrl.thread
	ctrl.mu.Unlock()

	executionCtx, finishExecution, err := h.beginSubAgentExecution()
	if err != nil {
		h.notifySubAgentUpdate()
		return
	}
	executionTransferred := false
	defer func() {
		if !executionTransferred {
			finishExecution()
		}
	}()

	turnID, err := h.nextSubAgentTurnID(executionCtx, ctrl.threadID)
	if err != nil {
		h.notifySubAgentUpdate()
		return
	}
	runID := h.nextID("run")
	if err := thread.enterTurn(); err != nil {
		h.notifySubAgentUpdate()
		return
	}
	repo, ok := h.options.Repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		thread.leaveTurn()
		h.notifySubAgentUpdate()
		return
	}
	admission, err := repo.AdmitSubAgentInput(executionCtx, sessiontree.AdmitSubAgentInputRequest{
		ParentThreadID: ctrl.parentThreadID,
		ChildThreadID:  ctrl.threadID,
		TurnID:         turnID,
		RunID:          runID,
		OwnerID:        h.nextID("lease"),
		Now:            h.now(),
	})
	if err != nil {
		thread.leaveTurn()
		if !errors.Is(err, sessiontree.ErrSubAgentInputNotFound) && !errors.Is(err, sessiontree.ErrActiveTurn) {
			h.notifySubAgentUpdate()
		}
		return
	}
	if admission.Replayed {
		thread.leaveTurn()
		h.notifySubAgentUpdate()
		return
	}
	lease := admission.Lease
	if err := thread.bindActiveLease(lease); err != nil {
		result, finishErr := thread.finalizeTurnStartupFailure(executionCtx, lease, turnID, runID, "local_owner_bind_error", err)
		if finishErr != nil && result.Status == "" {
			h.reportBackgroundError(fmt.Errorf("finalize subagent startup failure: %w", finishErr))
		}
		thread.leaveTurn()
		h.notifySubAgentUpdate()
		return
	}
	releaseLease := func(ctx context.Context) {
		thread.clearActiveLease(lease)
		thread.leaveTurn()
	}

	labels := engine.RunLabels{
		Host:        cloneStringMap(admission.Input.HostLabels),
		Correlation: cloneStringMap(admission.Input.CorrelationLabels),
	}
	runCtx, cancel := h.subAgentRunContext(executionCtx)
	ctrl.mu.Lock()
	cancelImmediately := ctrl.closed
	ctrl.running = true
	ctrl.turnID = turnID
	ctrl.cancel = cancel
	ctrl.done = make(chan struct{})
	ctrl.mu.Unlock()
	if cancelImmediately {
		cancel()
	}
	h.emitEntryCommitted(admission.TurnStarted, runID)
	h.emitEntryCommitted(admission.UserMessage, runID)
	h.emit(HarnessEvent{Type: EventTurnStarted, RunID: runID, ThreadID: ctrl.threadID, TurnID: turnID})
	h.emit(HarnessEvent{Type: EventEntryAppended, RunID: runID, ThreadID: ctrl.threadID, TurnID: turnID, EntryID: admission.UserMessage.ID, ParentID: admission.UserMessage.ParentID})
	h.notifySubAgentUpdate()
	executionTransferred = true
	go func() {
		defer finishExecution()
		leaseCtx := sessiontree.ContextWithTurnLease(runCtx, lease)
		renewedCtx, stopRenewal, renewalStartErr := thread.startLeaseRenewal(leaseCtx, lease)
		result, err := TurnResult{}, renewalStartErr
		if renewalStartErr != nil {
			result, err = thread.finalizeTurnStartupFailure(leaseCtx, lease, turnID, runID, "lease_renewal_start_error", renewalStartErr)
			if err != nil && result.Status == "" {
				h.reportBackgroundError(fmt.Errorf("finalize subagent startup failure: %w", err))
			}
		} else {
			result, err = thread.runLeased(renewedCtx, admission.Input.Message.Content, RunOptions{
				RunID:               runID,
				TurnID:              turnID,
				Attachments:         session.CloneMessageAttachments(admission.Input.Message.Attachments),
				Labels:              labels,
				AdmittedInputID:     admission.Input.SubAgentInputID,
				AdmissionCommitted:  true,
				AdmissionBaseLeafID: admission.TurnStarted.ParentID,
				DeadlineMetadata: map[string]string{
					subAgentTerminalReasonKey: subAgentRunTimeoutReason,
				},
			}, nil)
			if renewalErr := stopRenewal(); renewalErr != nil && err == nil {
				err = renewalErr
			}
		}
		cancel()
		releaseLease(leaseCtx)
		status := string(result.Status)
		if err != nil && status == "" {
			status = string(engine.Failed)
		}
		ctrl.mu.Lock()
		ctrl.running = false
		ctrl.turnID = ""
		ctrl.cancel = nil
		done := ctrl.done
		ctrl.done = nil
		ctrl.mu.Unlock()
		if done != nil {
			close(done)
		}
		h.emit(HarnessEvent{
			Type: EventSubAgentCompleted, ThreadID: ctrl.parentThreadID, Status: status, Message: result.Output,
			Metadata: map[string]string{"subagent_thread_id": ctrl.threadID, "subagent_path": ctrl.path},
		})
		h.notifySubAgentUpdate()
	}()
}

func (h *AgentHarness) beginSubAgentExecution() (context.Context, func(), error) {
	return h.beginBackgroundExecution("subagent execution")
}

func (h *AgentHarness) subAgentRunContext(base context.Context) (context.Context, context.CancelFunc) {
	timeout := DefaultSubAgentRunTimeout
	if h != nil && h.options.SubAgentRunTimeout > 0 {
		timeout = h.options.SubAgentRunTimeout
	}
	if timeout > MaxSubAgentRunTimeout {
		timeout = MaxSubAgentRunTimeout
	}
	if timeout <= 0 {
		return context.WithCancel(base)
	}
	return context.WithTimeout(base, timeout)
}

func subAgentLifecycleEntry(threadID string, metadata map[string]string) sessiontree.Entry {
	meta := cloneStringMap(metadata)
	if meta == nil {
		meta = map[string]string{}
	}
	meta[subAgentDetailKindKey] = subAgentLifecycleEntryKind
	meta[subAgentDetailTypeKey] = subAgentLifecycleEntryKind
	return sessiontree.Entry{ThreadID: threadID, Type: sessiontree.EntryCustom, Metadata: meta}
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
	ctrl := &subagentController{
		parentThreadID: meta.ParentThreadID,
		threadID:       meta.ID,
		path:           meta.AgentPath,
		taskName:       meta.TaskName,
		thread:         thread,
		closed:         meta.IsClosed(),
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
	status := SubAgentStatusIdle
	queued, err := h.pendingSubAgentInputCount(ctx, meta.ID)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	if meta.IsClosed() {
		queued = 0
	}
	writeClosed := meta.IsClosed() || meta.IsClosing()
	liveRunning := false
	key := subAgentControllerKey(meta.ID)
	h.mu.Lock()
	ctrl := h.subagents[key]
	h.mu.Unlock()
	runningTurnID := ""
	if ctrl != nil {
		ctrl.mu.Lock()
		if ctrl.running && !writeClosed {
			liveRunning = true
			runningTurnID = ctrl.turnID
			status = SubAgentStatusRunning
		}
		ctrl.mu.Unlock()
	}
	if queued > 0 && !writeClosed && status != SubAgentStatusRunning {
		status = SubAgentStatusRunning
	}
	journal, err := thread.Journal(ctx)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	read := threadSnapshotFromJournal(journal)
	if meta.IsClosed() {
		status = SubAgentStatusClosed
	} else if meta.IsClosing() {
		status = SubAgentStatusClosing
	} else if !writeClosed && queued == 0 && read.Status != "" {
		readStatus := SubAgentStatus(read.Status)
		if !liveRunning || runningTurnID != "" && read.LatestTurnID == runningTurnID &&
			isDurablySettledSubAgentStatus(readStatus, runningTurnID, journal.Path) {
			liveRunning = false
			status = readStatus
		}
	}
	return SubAgentSnapshot{
		ThreadID:        meta.ID,
		Path:            meta.AgentPath,
		TaskName:        meta.TaskName,
		TaskDescription: meta.TaskDescription,
		ParentThreadID:  meta.ParentThreadID,
		ParentTurnID:    meta.ParentTurnID,
		HostProfileRef:  meta.HostProfileRef,
		ForkMode:        SubAgentForkMode(meta.ForkMode),
		Status:          status,
		LatestTurnID:    read.LatestTurnID,
		LastMessage:     latestSubAgentMessage(read.Messages),
		WaitingPrompt:   read.WaitingPrompt,
		QueuedInputs:    queued,
		CreatedAt:       meta.CreatedAt,
		UpdatedAt:       meta.UpdatedAt,
		Closed:          meta.IsClosed(),
		CanSendInput:    !writeClosed,
		CanInterrupt:    !writeClosed && status == SubAgentStatusRunning,
		CanClose:        !writeClosed,
	}, nil
}

func isTerminalSubAgentStatus(status SubAgentStatus) bool {
	switch status {
	case SubAgentStatusCompleted, SubAgentStatusFailed, SubAgentStatusCancelled, SubAgentStatusClosed:
		return true
	default:
		return false
	}
}

func isDurablySettledSubAgentStatus(status SubAgentStatus, turnID string, path []sessiontree.Entry) bool {
	switch status {
	case SubAgentStatusCompleted, SubAgentStatusWaiting, SubAgentStatusFailed, SubAgentStatusCancelled, SubAgentStatusClosed:
		return true
	case SubAgentStatusInterrupted:
		for index := len(path) - 1; index >= 0; index-- {
			entry := path[index]
			if entry.TurnID != turnID || entry.Type != sessiontree.EntryTurnMarker {
				continue
			}
			return entry.TurnStatus == sessiontree.TurnAborted
		}
	default:
		return false
	}
	return false
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
	if _, err := h.subAgentParentThread(ctx, parentThreadID); err != nil {
		return sessiontree.ThreadMeta{}, err
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
	if meta.ParentThreadID != parentThreadID {
		return sessiontree.ThreadMeta{}, ErrSubAgentNotFound
	}
	return meta, nil
}

func (h *AgentHarness) resolveSubAgentDescendantMeta(ctx context.Context, parentThreadID, target string) (sessiontree.ThreadMeta, error) {
	parentThreadID = strings.TrimSpace(parentThreadID)
	if parentThreadID == "" {
		return sessiontree.ThreadMeta{}, errors.New("parent thread id is required")
	}
	parent, err := h.subAgentParentThread(ctx, parentThreadID)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if err := sessiontree.ValidateThreadMetaAuthority(parent); err != nil {
		return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return sessiontree.ThreadMeta{}, errors.New("subagent child thread id is required")
	}
	if target == parentThreadID {
		return sessiontree.ThreadMeta{}, ErrSubAgentNotFound
	}
	meta, err := h.options.Repo.Thread(ctx, target)
	if err != nil {
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return sessiontree.ThreadMeta{}, ErrSubAgentNotFound
		}
		return sessiontree.ThreadMeta{}, err
	}
	targetMeta := meta
	seen := map[string]struct{}{target: {}}
	for {
		if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
			return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
		}
		ancestorID := strings.TrimSpace(meta.ParentThreadID)
		if ancestorID == "" {
			return sessiontree.ThreadMeta{}, ErrSubAgentNotFound
		}
		if ancestorID == parentThreadID {
			return targetMeta, nil
		}
		if _, duplicate := seen[ancestorID]; duplicate {
			return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
		}
		seen[ancestorID] = struct{}{}
		meta, err = h.options.Repo.Thread(ctx, ancestorID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
	}
}

func (h *AgentHarness) subAgentParentThread(ctx context.Context, parentThreadID string) (sessiontree.ThreadMeta, error) {
	return h.options.Repo.Thread(ctx, strings.TrimSpace(parentThreadID))
}

func (h *AgentHarness) childThreadMetas(ctx context.Context, parentThreadID string) ([]sessiontree.ThreadMeta, error) {
	parentThreadID = strings.TrimSpace(parentThreadID)
	if parentThreadID == "" {
		return nil, errors.New("parent thread id is required")
	}
	if _, err := h.subAgentParentThread(ctx, parentThreadID); err != nil {
		return nil, err
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
		if meta.ParentThreadID == parentThreadID {
			children = append(children, meta)
		}
	}
	return children, nil
}

func (h *AgentHarness) pendingSubAgentInputCount(ctx context.Context, threadID string) (int, error) {
	repo, ok := h.options.Repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		return 0, errors.New("session tree repo does not support subagent authority operations")
	}
	inputs, err := repo.ListSubAgentInputs(ctx, threadID, sessiontree.SubAgentInputPending)
	if err != nil {
		return 0, err
	}
	return len(inputs), nil
}

func (h *AgentHarness) nextSubAgentTurnID(_ context.Context, _ string) (string, error) {
	id := strings.TrimSpace(h.nextID("turn"))
	if id == "" {
		return "", errors.New("subagent turn id generator returned an empty identity")
	}
	return id, nil
}

func subAgentPublicationFingerprint(publicationID string, meta sessiontree.ThreadMeta, forkMode SubAgentForkMode, sourceLeafID, artifactClosureFingerprint string,
	message session.Message, labels engine.RunLabels,
) (string, error) {
	meta.CreatedAt = time.Time{}
	meta.UpdatedAt = time.Time{}
	payload := struct {
		PublicationID              string
		Child                      sessiontree.ThreadMeta
		ForkMode                   SubAgentForkMode
		SourceLeafID               string
		ArtifactClosureFingerprint string
		Message                    session.Message
		HostLabels                 map[string]string
		CorrelationLabels          map[string]string
	}{
		PublicationID: publicationID, Child: meta, ForkMode: forkMode, SourceLeafID: sourceLeafID,
		ArtifactClosureFingerprint: artifactClosureFingerprint,
		Message:                    message, HostLabels: labels.Host, CorrelationLabels: labels.Correlation,
	}
	return hashSubAgentAuthorityPayload(payload)
}

func subAgentForkIdentityRewrite(publicationID string, path []sessiontree.Entry) (map[string]string, map[string]string) {
	turnIDs := map[string]string{}
	runIDs := map[string]string{}
	for _, entry := range path {
		turnID := strings.TrimSpace(entry.TurnID)
		if turnID != "" {
			if _, exists := turnIDs[turnID]; !exists {
				turnIDs[turnID] = deterministicSubAgentForkIdentity("turn", publicationID, turnID)
			}
		}
		runID := strings.TrimSpace(entry.Metadata["run_id"])
		if runID != "" {
			if _, exists := runIDs[runID]; !exists {
				runIDs[runID] = deterministicSubAgentForkIdentity("run", publicationID, runID)
			}
		}
	}
	return turnIDs, runIDs
}

func deterministicSubAgentForkIdentity(kind, publicationID, sourceID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(publicationID) + "\x00" + kind + "\x00" + strings.TrimSpace(sourceID)))
	return kind + "-" + hex.EncodeToString(sum[:16])
}

func subAgentInputFingerprint(requestID, parentThreadID, childThreadID string, message session.Message, labels engine.RunLabels, interrupt bool) (string, error) {
	payload := struct {
		InputRequestID    string
		ParentThreadID    string
		ChildThreadID     string
		Message           session.Message
		HostLabels        map[string]string
		CorrelationLabels map[string]string
		Interrupt         bool
	}{
		InputRequestID: requestID, ParentThreadID: parentThreadID, ChildThreadID: childThreadID,
		Message: message, HostLabels: labels.Host, CorrelationLabels: labels.Correlation, Interrupt: interrupt,
	}
	return hashSubAgentAuthorityPayload(payload)
}

func subAgentPendingToolCompletionFingerprint(requestID, parentThreadID, childThreadID string,
	target sessiontree.PendingToolSettlementTarget, status PendingToolCompletionStatus, summary, output string,
	message session.Message, labels engine.RunLabels,
) (string, error) {
	payload := struct {
		InputRequestID    string
		ParentThreadID    string
		ChildThreadID     string
		Target            sessiontree.PendingToolSettlementTarget
		Status            PendingToolCompletionStatus
		Summary           string
		Output            string
		Message           session.Message
		HostLabels        map[string]string
		CorrelationLabels map[string]string
	}{
		InputRequestID: requestID, ParentThreadID: parentThreadID, ChildThreadID: childThreadID,
		Target: target, Status: status, Summary: summary, Output: output, Message: session.CloneMessage(message),
		HostLabels: labels.Host, CorrelationLabels: labels.Correlation,
	}
	return hashSubAgentAuthorityPayload(payload)
}

func hashSubAgentAuthorityPayload(payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
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
