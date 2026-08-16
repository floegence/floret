package agentharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/v4/identity"
	"github.com/floegence/floret/v4/internal/event"
	"github.com/floegence/floret/v4/internal/session"
	"github.com/floegence/floret/v4/internal/session/artifact"
	"github.com/floegence/floret/v4/internal/session/contextpolicy"
	"github.com/floegence/floret/v4/internal/sessiontree"
	"github.com/floegence/floret/v4/observation"
	"github.com/floegence/floret/v4/tools"
)

const (
	DefaultThreadDetailLimit = 200
	MaxThreadDetailLimit     = 500

	DefaultThreadDetailEventLimit = DefaultThreadDetailLimit
	MaxThreadDetailEventLimit     = MaxThreadDetailLimit
)

const (
	subAgentApprovalEntryKind = "subagent_approval"
	threadDetailKindKey       = "kind"
	threadDetailTypeKey       = "type"
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
	threadDetailRawOmitted    = "raw_omitted"
)

type ListThreadDetailEventsOptions struct {
	ThreadID     string
	AfterOrdinal int64
	Limit        int
	IncludeRaw   bool
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
	Events       []ThreadDetailEvent `json:"events"`
	NextOrdinal  int64               `json:"next_ordinal,omitempty"`
	HasMore      bool                `json:"has_more,omitempty"`
	RetainedFrom int64               `json:"retained_from,omitempty"`
	GeneratedAt  time.Time           `json:"generated_at"`
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
	ThreadDetailEventCustom           ThreadDetailEventKind = "custom"
)

type ThreadDetailEvent struct {
	ID        string                `json:"id"`
	Ordinal   int64                 `json:"ordinal"`
	ParentID  string                `json:"parent_id,omitempty"`
	ThreadID  string                `json:"thread_id"`
	TurnID    string                `json:"turn_id,omitempty"`
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
	Role        string                      `json:"role,omitempty"`
	Kind        string                      `json:"kind,omitempty"`
	Preview     string                      `json:"preview,omitempty"`
	Content     string                      `json:"content,omitempty"`
	Attachments []session.MessageAttachment `json:"attachments,omitempty"`
	References  []session.MessageReference  `json:"references,omitempty"`
	Reasoning   string                      `json:"reasoning,omitempty"`
	Activity    *tools.ActivityPresentation `json:"activity,omitempty"`
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
	ErrorCode   string         `json:"error_code,omitempty"`
	Text        string         `json:"text,omitempty"`
	ArgsHash    string         `json:"args_hash,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type ThreadDetailToolResult struct {
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

func (h *AgentHarness) threadDetailActivityTimeline(entries []sessiontree.Entry, retainedFrom int64, activityContext threadDetailActivityContext, generatedAt time.Time) observation.ActivityTimeline {
	observed := make([]observation.Event, 0, len(entries))
	for index, entry := range entries {
		ordinal := int64(index + 1)
		if ordinal < retainedFrom {
			continue
		}
		detail, ok := h.threadDetailEvent(entry, ordinal, false, activityContext)
		if !ok {
			continue
		}
		ev, ok := threadDetailObservationEvent(detail, entry, activityContext)
		if !ok {
			continue
		}
		observed = append(observed, ev)
	}
	return observation.BuildActivityTimeline(observation.ActivityRunMeta{}, observed, generatedAt.UnixMilli())
}

func (h *AgentHarness) threadDetailContext(entries []sessiontree.Entry, retainedFrom int64, activityContext threadDetailActivityContext, generatedAt time.Time) (ThreadContextSnapshot, error) {
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
		case entry.Type == sessiontree.EntryCustom && entry.Metadata[threadDetailKindKey] == subAgentContextPolicyEntryKind:
			providerName := strings.TrimSpace(entry.Metadata[subAgentContextProviderKey])
			modelName := strings.TrimSpace(entry.Metadata[subAgentContextModelKey])
			if providerName == "" || modelName == "" {
				return ThreadContextSnapshot{}, errors.New("thread context policy requires provider and model")
			}
			policy, err := threadDetailContextPolicy(entry.Metadata)
			if err != nil {
				return ThreadContextSnapshot{}, err
			}
			out.Model.Provider = providerName
			out.Model.Model = modelName
			out.Policy = policy
			hasPolicy = true
			latestContextObservedAt = maxTime(latestContextObservedAt, entry.CreatedAt)
		case entry.Type == sessiontree.EntryCustom && entry.Metadata[threadDetailKindKey] == subAgentContextStatusEntryKind:
			status, err := threadDetailContextStatus(entry.Metadata)
			if err != nil {
				return ThreadContextSnapshot{}, err
			}
			if status.ThreadID.String() != entry.ThreadID || status.TurnID.String() != entry.TurnID {
				return ThreadContextSnapshot{}, errors.New("thread context status identity mismatch")
			}
			if entry.RunID != "" && status.RunID.String() != entry.RunID {
				return ThreadContextSnapshot{}, errors.New("thread context status run identity mismatch")
			}
			if hasPolicy && (status.Provider != out.Model.Provider || status.Model != out.Model.Model) {
				return ThreadContextSnapshot{}, errors.New("thread context status model identity mismatch")
			}
			out.Usage = &status
			latestContextObservedAt = maxTime(latestContextObservedAt, nonZeroTime(status.ObservedAt, entry.CreatedAt))
		case entry.Type == sessiontree.EntryCustom && entry.Metadata[threadDetailKindKey] == subAgentContextCompactionEntryKind:
			compact, err := threadDetailContextCompaction(entry.Metadata)
			if err != nil {
				return ThreadContextSnapshot{}, err
			}
			if compact.ThreadID != entry.ThreadID || compact.TurnID != entry.TurnID {
				return ThreadContextSnapshot{}, errors.New("thread context compaction identity mismatch")
			}
			compactions = upsertThreadDetailCompaction(compactions, seenCompactions, compact)
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

func threadDetailContextPolicy(metadata map[string]string) (ThreadContextPolicy, error) {
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

func threadDetailContextStatus(metadata map[string]string) (observation.ContextStatus, error) {
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

func threadDetailContextCompaction(metadata map[string]string) (ThreadContextCompaction, error) {
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

func upsertThreadDetailCompaction(compactions []ThreadContextCompaction, seen map[string]int, compact ThreadContextCompaction) []ThreadContextCompaction {
	key := threadDetailContextCompactionKey(compact)
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

func threadDetailContextCompactionKey(compact ThreadContextCompaction) string {
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
		threadDetailKindKey:        subAgentContextPolicyEntryKind,
		threadDetailTypeKey:        subAgentContextPolicyEntryKind,
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
	activityContext := newThreadDetailActivityContext(entries)
	events := make([]ThreadDetailEvent, 0, len(entries))
	var nextOrdinal int64
	var hasMore bool
	for index, entry := range entries {
		ordinal := int64(index + 1)
		if ordinal < retainedFrom || ordinal <= opts.AfterOrdinal {
			continue
		}
		event, ok := h.threadDetailEvent(entry, ordinal, opts.IncludeRaw, activityContext)
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

func (h *AgentHarness) detailEventsForCanonicalEntries(ctx context.Context, entries []sessiontree.Entry, includeRaw bool) ([]ThreadDetailEvent, error) {
	activityContext := newThreadDetailActivityContext(entries)
	events := make([]ThreadDetailEvent, 0, len(entries))
	for index, entry := range entries {
		event, ok := h.threadDetailEvent(entry, int64(index+1), includeRaw, activityContext)
		if ok {
			events = append(events, event)
		}
	}
	return events, nil
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
	events, err := h.detailEventsForCanonicalEntries(ctx, entries, includeRaw)
	if err != nil {
		return ThreadDetailEvents{}, false, err
	}
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
	activityContext := newThreadDetailActivityContext(entries)
	events := make([]ThreadDetailEvent, 0, len(entries))
	var nextOrdinal int64
	for _, item := range selected {
		event, ok := h.threadDetailEvent(item.entry, item.ordinal, includeRaw, activityContext)
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

func (h *AgentHarness) latestThreadDetailEventsFromPath(ctx context.Context, path []sessiontree.Entry, includeRaw bool) (ThreadDetailEvents, error) {
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
	activityContext := newThreadDetailActivityContext(entries)
	events := make([]ThreadDetailEvent, 0, len(entries))
	var nextOrdinal int64
	for offset, entry := range entries {
		ordinal := int64(latestStartedIndex + offset + 1)
		event, ok := h.threadDetailEvent(entry, ordinal, includeRaw, activityContext)
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
	activityContext := newThreadDetailActivityContext(entries)
	return h.threadDetailContext(entries, threadDetailRetainedFrom(entries), activityContext, h.now())
}

func threadDetailRetainedFrom(entries []sessiontree.Entry) int64 {
	if len(entries) == 0 {
		return 0
	}
	return 1
}

func (h *AgentHarness) threadDetailEvent(entry sessiontree.Entry, ordinal int64, includeRaw bool, activityContext threadDetailActivityContext) (ThreadDetailEvent, bool) {
	event := ThreadDetailEvent{
		ID:        entry.ID,
		Ordinal:   ordinal,
		ParentID:  entry.ParentID,
		ThreadID:  entry.ThreadID,
		TurnID:    entry.TurnID,
		CreatedAt: entry.CreatedAt,
	}
	switch entry.Type {
	case sessiontree.EntryUserMessage:
		event.Kind = ThreadDetailEventUserMessage
		event.Type = string(sessiontree.EntryUserMessage)
		event.Message = threadDetailMessage(entry.Message, includeRaw)
	case sessiontree.EntryAssistantMessage:
		event.Kind = ThreadDetailEventAssistantMessage
		event.Type = string(sessiontree.EntryAssistantMessage)
		event.Message = threadDetailMessage(entry.Message, includeRaw)
	case sessiontree.EntryToolCall:
		event.Kind = ThreadDetailEventToolCall
		event.Type = string(sessiontree.EntryToolCall)
		event.Message = threadDetailMessage(entry.Message, includeRaw)
		event.ToolCall = threadDetailToolCall(entry.Message, includeRaw)
	case sessiontree.EntryToolResult:
		event.Kind = ThreadDetailEventToolResult
		event.Type = string(sessiontree.EntryToolResult)
		event.Message = threadDetailMessage(entry.Message, includeRaw)
		event.ToolResult = threadDetailToolResult(entry.Message, entry.Metadata, includeRaw)
	case sessiontree.EntryTurnMarker:
		event.Kind = ThreadDetailEventTurnMarker
		event.Type = string(sessiontree.EntryTurnMarker)
		event.TurnMarker = &ThreadDetailTurnMarker{
			Status:   string(entry.TurnStatus),
			Metadata: cloneStringMap(entry.Metadata),
		}
	case sessiontree.EntryCompaction:
		event.Kind = ThreadDetailEventCompaction
		event.Type = string(sessiontree.EntryCompaction)
		event.Compaction = threadDetailCompaction(entry)
	case sessiontree.EntryRunFailure:
		event.Kind = ThreadDetailEventError
		event.Type = string(sessiontree.EntryRunFailure)
		event.Error = entry.Error
	case sessiontree.EntryCustom:
		event.Kind = ThreadDetailEventCustom
		event.Type = entry.Metadata[threadDetailTypeKey]
		switch entry.Metadata[threadDetailKindKey] {
		case subAgentApprovalEntryKind:
			event.Kind = ThreadDetailEventApproval
			if event.Type == "" {
				event.Type = subAgentApprovalEntryKind
			}
			event.Approval = threadDetailApproval(entry.Metadata)
		case toolDispatchEntryKind:
			event.Kind = ThreadDetailEventToolDispatch
			if event.Type == "" {
				event.Type = string(observation.EventTypeToolDispatchStarted)
			}
			event.Message = threadDetailMessage(entry.Message, includeRaw)
			event.ToolCall = threadDetailToolDispatch(entry)
		case toolActivityEntryKind:
			event.Kind = ThreadDetailEventToolActivity
			if event.Type == "" {
				event.Type = string(observation.EventTypeToolActivityUpdated)
			}
			event.Message = threadDetailMessage(entry.Message, includeRaw)
			event.ToolCall = threadDetailToolActivity(entry)
		case pendingToolSettlementEntryKind:
			event.Kind = ThreadDetailEventToolResult
			if event.Type == "" {
				event.Type = pendingToolSettlementEntryKind
			}
			event.Message = threadDetailMessage(entry.Message, includeRaw)
			event.ToolResult = threadDetailToolResult(entry.Message, entry.Metadata, includeRaw)
		case subAgentLifecycleEntryKind:
			event.Kind = ThreadDetailEventCustom
			if event.Type == "" {
				event.Type = subAgentLifecycleEntryKind
			}
		case subAgentContextPolicyEntryKind, subAgentContextStatusEntryKind, subAgentContextCompactionEntryKind:
			return ThreadDetailEvent{}, false
		}
		event.Metadata = cloneStringMap(entry.Metadata)
	default:
		return ThreadDetailEvent{}, false
	}
	if event.Metadata == nil && entry.Type != sessiontree.EntryTurnMarker {
		event.Metadata = cloneStringMap(entry.Metadata)
	}
	if !includeRaw && threadDetailRawAvailable(event) {
		if event.Metadata == nil {
			event.Metadata = map[string]string{}
		}
		event.Metadata[threadDetailRawOmitted] = "true"
	}
	event.ActivityTimeline = threadDetailActivityTimeline(event, entry, activityContext)
	return event, true
}

func threadDetailRawAvailable(event ThreadDetailEvent) bool {
	switch event.Kind {
	case ThreadDetailEventUserMessage, ThreadDetailEventAssistantMessage, ThreadDetailEventToolCall, ThreadDetailEventToolDispatch, ThreadDetailEventToolActivity, ThreadDetailEventToolResult:
		return true
	default:
		return false
	}
}

type threadDetailActivityContext struct {
	resultCallIDs map[string]struct{}
	runIDs        map[string]string
	presentations map[threadDetailActivityKey]*tools.ActivityPresentation
}

type threadDetailActivityKey struct {
	turnID string
	callID string
}

func newThreadDetailActivityContext(entries []sessiontree.Entry) threadDetailActivityContext {
	return threadDetailActivityContext{
		resultCallIDs: threadDetailResultCallIDs(entries),
		runIDs:        threadDetailTurnRunIDs(entries),
		presentations: threadDetailToolPresentations(entries),
	}
}

func (c threadDetailActivityContext) hasResult(callID string) bool {
	callID = strings.TrimSpace(callID)
	if callID == "" || len(c.resultCallIDs) == 0 {
		return false
	}
	_, ok := c.resultCallIDs[callID]
	return ok
}

func (c threadDetailActivityContext) runIDForTurn(turnID string) string {
	if len(c.runIDs) == 0 {
		return ""
	}
	return strings.TrimSpace(c.runIDs[strings.TrimSpace(turnID)])
}

func (c threadDetailActivityContext) presentation(turnID, callID string) *tools.ActivityPresentation {
	if len(c.presentations) == 0 {
		return nil
	}
	return tools.CloneActivityPresentation(c.presentations[threadDetailActivityKey{
		turnID: strings.TrimSpace(turnID),
		callID: strings.TrimSpace(callID),
	}])
}

func threadDetailToolPresentations(entries []sessiontree.Entry) map[threadDetailActivityKey]*tools.ActivityPresentation {
	out := map[threadDetailActivityKey]*tools.ActivityPresentation{}
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryToolCall || entry.Message.Activity == nil {
			continue
		}
		key := threadDetailActivityKey{
			turnID: strings.TrimSpace(entry.TurnID),
			callID: strings.TrimSpace(entry.Message.ToolCallID),
		}
		if key.turnID == "" || key.callID == "" {
			continue
		}
		out[key] = observationActivityPresentation(entry.Message.Activity)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func threadDetailTurnRunIDs(entries []sessiontree.Entry) map[string]string {
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

func threadDetailResultCallIDs(entries []sessiontree.Entry) map[string]struct{} {
	out := map[string]struct{}{}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCustom && entry.Metadata[threadDetailKindKey] == pendingToolSettlementEntryKind {
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

func threadDetailActivityTimeline(detail ThreadDetailEvent, entry sessiontree.Entry, activityContext threadDetailActivityContext) *observation.ActivityTimeline {
	observed, ok := threadDetailObservationEvent(detail, entry, activityContext)
	if !ok {
		return nil
	}
	runID := threadDetailRunID(detail, activityContext)
	timeline := observation.BuildActivityTimeline(observation.ActivityRunMeta{
		RunID:    identity.RunID(runID),
		ThreadID: identity.ThreadID(detail.ThreadID),
		TurnID:   identity.TurnID(detail.TurnID),
	}, []observation.Event{observed}, entry.CreatedAt.UnixMilli())
	return &timeline
}

func threadDetailObservationEvent(detail ThreadDetailEvent, entry sessiontree.Entry, activityContext threadDetailActivityContext) (observation.Event, bool) {
	base := observation.Event{
		RunID:      identity.RunID(threadDetailRunID(detail, activityContext)),
		ThreadID:   identity.ThreadID(detail.ThreadID),
		TurnID:     identity.TurnID(detail.TurnID),
		Step:       int(detail.Ordinal),
		ObservedAt: entry.CreatedAt,
	}
	switch detail.Kind {
	case ThreadDetailEventToolCall:
		if detail.ToolCall == nil || activityContext.hasResult(detail.ToolCall.ID) {
			return observation.Event{}, false
		}
		if detail.Message != nil && detail.Message.Kind == string(session.MessageKindControlSignal) {
			base.Type = observation.EventTypeControlSignal
			base.ToolKind = "control"
			if detail.ToolCall.ControlSignal != nil {
				if disposition := strings.TrimSpace(detail.ToolCall.ControlSignal.Disposition); disposition != "" {
					base.Metadata = map[string]any{"control_disposition": disposition}
				}
				if code := strings.TrimSpace(detail.ToolCall.ControlSignal.ErrorCode); code != "" {
					if base.Metadata == nil {
						base.Metadata = map[string]any{}
					}
					base.Metadata["control_error_code"] = code
					base.Metadata["error_present"] = true
					base.Error = code
				}
			}
		} else {
			base.Type = observation.EventTypeToolCall
			base.ToolKind = "local"
		}
		base.ToolID = detail.ToolCall.ID
		base.ToolName = detail.ToolCall.Name
		base.Activity = observationActivityPresentation(entry.Message.Activity)
		return base, true
	case ThreadDetailEventToolResult:
		if detail.ToolResult == nil {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeToolResult
		base.ToolID = detail.ToolResult.CallID
		base.ToolName = detail.ToolResult.ToolName
		base.ToolKind = "local"
		base.Activity = observationActivityPresentation(entry.Message.Activity)
		base.Metadata = threadDetailToolResultActivityMetadata(detail.ToolResult)
		if detail.ToolResult.Status == string(observation.ActivityStatusError) {
			base.Error = "tool_result_error"
		}
		return base, true
	case ThreadDetailEventToolDispatch:
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
		base.Metadata = threadDetailToolDispatchActivityMetadata(detail.Metadata)
		return base, true
	case ThreadDetailEventToolActivity:
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
		base.Metadata = threadDetailToolActivityMetadata(detail.Metadata)
		return base, true
	case ThreadDetailEventApproval:
		if detail.Approval == nil {
			return observation.Event{}, false
		}
		base.Type = threadDetailApprovalActivityType(detail.Approval.State)
		base.ToolID = detail.Approval.ToolID
		base.ToolName = detail.Approval.ToolName
		base.ToolKind = firstThreadDetailNonEmpty(detail.Approval.ToolKind, "local")
		base.ArgsHash = detail.Approval.ArgsHash
		base.Activity = observationActivityPresentation(entry.Message.Activity)
		if base.Activity == nil {
			base.Activity = activityContext.presentation(detail.TurnID, detail.Approval.ToolID)
		}
		if base.Activity == nil {
			base.Activity = threadDetailActivityPresentation("Tool approval", detail.Approval.State)
		}
		base.Metadata = threadDetailApprovalActivityMetadata(detail.Approval.Metadata)
		if detail.Approval.State == "rejected" || detail.Approval.State == "timed_out" {
			base.Error = "tool_approval_" + detail.Approval.State
		}
		return base, true
	case ThreadDetailEventTurnMarker:
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
		base.Activity = threadDetailActivityPresentation("Turn "+status, status)
		base.Metadata = threadDetailTurnMarkerActivityMetadata(status)
		if status == string(sessiontree.TurnFailed) || status == string(sessiontree.TurnAborted) {
			base.Error = "turn_" + status
		}
		return base, true
	case ThreadDetailEventCustom:
		if detail.Type != subAgentLifecycleEntryKind {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeControlSignal
		base.ToolID = "subagent_lifecycle"
		base.ToolName = "subagent_lifecycle"
		base.ToolKind = "control"
		action := firstThreadDetailNonEmpty(detail.Metadata[subAgentLifecycleActionKey], "updated")
		base.Activity = threadDetailActivityPresentation("Subagent "+action, action)
		base.Metadata = map[string]any{"control_disposition": "terminal"}
		return base, true
	default:
		return observation.Event{}, false
	}
}

func threadDetailRunID(detail ThreadDetailEvent, activityContext threadDetailActivityContext) string {
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

func threadDetailToolResultActivityMetadata(result *ThreadDetailToolResult) map[string]any {
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
	case string(observation.ActivityStatusDeclined):
		metadata["tool_result_status"] = string(observation.ActivityStatusDeclined)
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

func threadDetailToolDispatch(entry sessiontree.Entry) *ThreadDetailToolCall {
	id := firstThreadDetailNonEmpty(strings.TrimSpace(entry.Message.ToolCallID), strings.TrimSpace(entry.Metadata[toolDispatchToolIDKey]))
	name := firstThreadDetailNonEmpty(strings.TrimSpace(entry.Message.ToolName), strings.TrimSpace(entry.Metadata[toolDispatchNameKey]))
	if id == "" && name == "" {
		return nil
	}
	return &ThreadDetailToolCall{
		ID:       id,
		Name:     name,
		ArgsHash: strings.TrimSpace(entry.Metadata[toolDispatchArgsKey]),
	}
}

func threadDetailToolActivity(entry sessiontree.Entry) *ThreadDetailToolCall {
	id := firstThreadDetailNonEmpty(strings.TrimSpace(entry.Message.ToolCallID), strings.TrimSpace(entry.Metadata[toolActivityToolIDKey]))
	name := firstThreadDetailNonEmpty(strings.TrimSpace(entry.Message.ToolName), strings.TrimSpace(entry.Metadata[toolActivityNameKey]))
	if id == "" && name == "" {
		return nil
	}
	return &ThreadDetailToolCall{
		ID:       id,
		Name:     name,
		ArgsHash: strings.TrimSpace(entry.Metadata[toolActivityArgsKey]),
	}
}

func threadDetailToolDispatchActivityMetadata(metadata map[string]string) map[string]any {
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

func threadDetailToolActivityMetadata(metadata map[string]string) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range metadata {
		switch key {
		case threadDetailKindKey, threadDetailTypeKey, toolActivityToolIDKey, toolActivityNameKey, toolActivityKindKey, toolActivityArgsKey:
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

func threadDetailApprovalActivityType(state string) observation.EventType {
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

func threadDetailApprovalActivityMetadata(metadata map[string]string) map[string]any {
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

func threadDetailTurnMarkerActivityMetadata(status string) map[string]any {
	switch sessiontree.TurnMarkerStatus(status) {
	case sessiontree.TurnWaiting:
		return map[string]any{"control_disposition": "waiting"}
	case sessiontree.TurnFailed, sessiontree.TurnAborted:
		return map[string]any{"control_disposition": "terminal", "error_present": true}
	default:
		return map[string]any{"control_disposition": "terminal"}
	}
}

func threadDetailActivityPresentation(label, description string) *tools.ActivityPresentation {
	return &tools.ActivityPresentation{
		Label:       strings.TrimSpace(label),
		Description: strings.TrimSpace(description),
		Renderer:    tools.ActivityRendererStructured,
	}
}

func observationActivityPresentation(in *session.ActivityPresentation) *tools.ActivityPresentation {
	return session.CloneActivityPresentation(in)
}

func firstThreadDetailNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func threadDetailApproval(metadata map[string]string) *ThreadDetailApproval {
	if len(metadata) == 0 {
		return nil
	}
	return &ThreadDetailApproval{
		State:    metadata[subAgentApprovalStateKey],
		ToolID:   metadata[subAgentApprovalToolIDKey],
		ToolName: metadata[subAgentApprovalNameKey],
		ToolKind: metadata[subAgentApprovalKindKey],
		ArgsHash: metadata[subAgentApprovalArgsKey],
		Reason:   metadata[subAgentApprovalReasonKey],
		Metadata: cloneStringMap(metadata),
	}
}

func threadDetailMessage(msg session.Message, includeRaw bool) *ThreadDetailMessage {
	activity := observationActivityPresentation(msg.Activity)
	if msg.Role == "" && msg.Kind == "" && msg.Content == "" && len(msg.Attachments) == 0 && len(msg.References) == 0 && msg.Reasoning == "" && activity == nil {
		return nil
	}
	out := &ThreadDetailMessage{
		Role:        string(msg.Role),
		Kind:        string(msg.Kind),
		Preview:     safeThreadDetailPreview(msg.Content, 500),
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

func threadDetailToolCall(msg session.Message, includeRaw bool) *ThreadDetailToolCall {
	if msg.ToolCallID == "" && msg.ToolName == "" && msg.ToolArgs == "" {
		return nil
	}
	args := strings.TrimSpace(msg.ToolArgs)
	out := &ThreadDetailToolCall{
		ID:          msg.ToolCallID,
		Name:        msg.ToolName,
		ArgsPreview: safeThreadDetailPreview(args, 500),
		ArgsHash:    stableThreadDetailHash(args),
	}
	if signal := session.CloneControlSignalView(msg.ControlSignal); signal != nil {
		out.ControlSignal = &ThreadDetailControlSignal{
			Name:        signal.Name,
			CallID:      signal.CallID,
			Disposition: signal.Disposition,
			ErrorCode:   signal.ErrorCode,
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

func threadDetailToolResult(msg session.Message, metadata map[string]string, includeRaw bool) *ThreadDetailToolResult {
	if msg.ToolCallID == "" && msg.ToolName == "" && msg.Content == "" && msg.ToolResult == nil {
		return nil
	}
	out := &ThreadDetailToolResult{
		CallID:          msg.ToolCallID,
		ToolName:        msg.ToolName,
		EffectAttemptID: strings.TrimSpace(metadata[sessiontree.PendingToolEffectAttemptIDKey]),
		Preview:         safeThreadDetailPreview(msg.Content, 800),
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
		out.ContentSHA256 = stableThreadDetailHash(msg.Content)
	}
	return out
}

func threadDetailCompaction(entry sessiontree.Entry) *ThreadDetailCompaction {
	return &ThreadDetailCompaction{
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

func stableThreadDetailHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func safeThreadDetailPreview(value string, limit int) string {
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
