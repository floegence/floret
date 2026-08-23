package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v4/internal/control"
	"github.com/floegence/floret/v4/internal/session"
	"github.com/floegence/floret/v4/internal/session/artifact"
	"github.com/floegence/floret/v4/internal/session/compaction"
	"github.com/floegence/floret/v4/internal/session/contextpolicy"
)

type EntryType string

const (
	EntryThreadInfo       EntryType = "thread_info"
	EntryTurnMarker       EntryType = "turn_marker"
	EntryUserMessage      EntryType = "user_message"
	EntryAssistantMessage EntryType = "assistant_message"
	EntryToolCall         EntryType = "tool_call"
	EntryToolResult       EntryType = "tool_result"
	EntryModelChange      EntryType = "model_change"
	EntryActiveTools      EntryType = "active_tools_change"
	EntryCompaction       EntryType = "compaction"
	EntryBranchSummary    EntryType = "branch_summary"
	EntryRunFailure       EntryType = "run_failure"
	EntryQueueAdded       EntryType = "queue_added"
	EntryQueueReordered   EntryType = "queue_reordered"
	EntryQueueDeleted     EntryType = "queue_deleted"
	EntryQueuePromoted    EntryType = "queue_promoted"
	EntryInteractionAsked EntryType = "interaction_requested"
	EntryInteractionDone  EntryType = "interaction_resolved"
	EntryCancelRequested  EntryType = "turn_cancel_requested"
	EntryRuntimeRestarted EntryType = "runtime_restarted"
	EntryEffectAttempt    EntryType = "effect_attempt"
	EntryCustom           EntryType = "custom"
)

type TurnMarkerStatus string

const (
	TurnStarted                      TurnMarkerStatus = "started"
	TurnSavePoint                    TurnMarkerStatus = "save_point"
	TurnCompleted                    TurnMarkerStatus = "completed"
	TurnWaiting                      TurnMarkerStatus = "waiting"
	TurnFailed                       TurnMarkerStatus = "failed"
	TurnAborted                      TurnMarkerStatus = "aborted"
	BranchBoundaryTurnFailureMessage                  = "turn interrupted by branch boundary"
)

type SubAgentInputState string

const (
	SubAgentInputPending   SubAgentInputState = "pending"
	SubAgentInputAdmitted  SubAgentInputState = "admitted"
	SubAgentInputCancelled SubAgentInputState = "cancelled"
)

type SubAgentRequestKind string

const (
	SubAgentRequestPublication                     SubAgentRequestKind = "publication"
	SubAgentRequestInput                           SubAgentRequestKind = "input"
	SubAgentRequestPendingToolCompletion           SubAgentRequestKind = "pending_tool_completion"
	SubAgentInputIDMetadataKey                                         = "subagent_input_id"
	SubAgentUserMessageOriginMetadataKey                               = "subagent_user_message_origin"
	SubAgentUserMessageOriginDelegatedMission                          = "delegated_mission"
	SubAgentUserMessageOriginInput                                     = "subagent_input"
	SubAgentUserMessageOriginPendingToolCompletion                     = "pending_tool_completion"
)

func SubAgentUserMessageOrigin(kind SubAgentRequestKind) (string, error) {
	switch kind {
	case SubAgentRequestPublication:
		return SubAgentUserMessageOriginDelegatedMission, nil
	case SubAgentRequestInput:
		return SubAgentUserMessageOriginInput, nil
	case SubAgentRequestPendingToolCompletion:
		return SubAgentUserMessageOriginPendingToolCompletion, nil
	default:
		return "", fmt.Errorf("unsupported subagent request kind %q", kind)
	}
}

type SubAgentInputRecord struct {
	SubAgentInputID    string
	ParentThreadID     string
	ChildThreadID      string
	RequestKind        SubAgentRequestKind
	RequestID          string
	RequestFingerprint string
	Sequence           int64
	State              SubAgentInputState
	Message            session.Message
	HostLabels         map[string]string
	CorrelationLabels  map[string]string
	AdmittedTurnID     string
	AdmittedRunID      string
	CreatedAt          time.Time
	AdmittedAt         time.Time
	CancelledAt        time.Time
}

type ThreadTitleStatus string

const (
	ThreadTitlePending ThreadTitleStatus = "pending"
	ThreadTitleReady   ThreadTitleStatus = "ready"
	ThreadTitleFailed  ThreadTitleStatus = "failed"
)

type ThreadTitleSource string

const (
	ThreadTitleSourceFallback ThreadTitleSource = "fallback"
	ThreadTitleSourceProvider ThreadTitleSource = "provider"
	ThreadTitleSourceHost     ThreadTitleSource = "host"
)

var (
	ErrThreadNotFound           = errors.New("session tree thread not found")
	ErrEntryNotFound            = errors.New("session tree entry not found")
	ErrInvalidParent            = errors.New("session tree invalid parent")
	ErrActiveTurn               = errors.New("session tree thread already has an active turn")
	ErrThreadExists             = errors.New("session tree thread already exists")
	ErrInvalidThreadAuthority   = errors.New("session tree invalid thread authority")
	ErrThreadAuthorityBusy      = errors.New("session tree thread authority is busy")
	ErrThreadClosed             = errors.New("session tree thread is closed")
	ErrThreadDeleted            = errors.New("session tree thread is deleted")
	ErrSubAgentClosing          = errors.New("session tree subagent is closing")
	ErrStaleAuthority           = errors.New("session tree authority proof is stale")
	ErrRecoveryTargetResolved   = errors.New("session tree interrupted recovery target is resolved")
	ErrAuthorityCorrupt         = errors.New("session tree authority state is corrupt")
	ErrForkDestinationConflict  = errors.New("session tree fork destination conflicts with operation marker")
	ErrAgentTodoVersionConflict = errors.New("session tree agent todo version conflict")
	ErrStaleCanonicalTurnCursor = errors.New("session tree canonical turn cursor is stale")
	ErrRequestConflict          = errors.New("session tree request conflicts with the canonical request")
	ErrSubAgentRequestConflict  = fmt.Errorf("subagent request identity conflicts with canonical request: %w", ErrRequestConflict)
	ErrSubAgentInputNotFound    = errors.New("session tree subagent input not found")
	ErrEffectAttemptNotFound    = errors.New("session tree effect attempt not found")
	ErrEffectOutcomeUnknown     = errors.New("session tree effect outcome is unknown")
)

func canonicalTime(value time.Time, now func() time.Time) time.Time {
	if !value.IsZero() {
		return value.UTC()
	}
	return now().UTC()
}

func cloneStringMap(values map[string]string) map[string]string {
	return maps.Clone(values)
}

func terminalTurnMarker(status TurnMarkerStatus) bool {
	switch status {
	case TurnCompleted, TurnFailed, TurnAborted:
		return true
	default:
		return false
	}
}

func unfinishedTurnIDs(path []Entry) ([]string, error) {
	started := make(map[string]bool)
	terminal := make(map[string]bool)
	var order []string
	for _, entry := range path {
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type != EntryTurnMarker || turnID == "" {
			continue
		}
		if entry.TurnStatus == TurnStarted && !started[turnID] {
			started[turnID] = true
			order = append(order, turnID)
		}
		if terminalTurnMarker(entry.TurnStatus) {
			terminal[turnID] = true
		}
	}
	var unfinished []string
	for _, turnID := range order {
		if !terminal[turnID] {
			unfinished = append(unfinished, turnID)
		}
	}
	if len(unfinished) > 1 {
		return nil, ErrAuthorityCorrupt
	}
	return unfinished, nil
}

// PrepareBranchBoundaryEntry closes a copied unfinished turn so the fork has
// one idle canonical boundary before it accepts new work.
func PrepareBranchBoundaryEntry(path []Entry, threadID, parentEntryID, entryID, reason string, now time.Time) (Entry, error) {
	unfinished, err := unfinishedTurnIDs(path)
	if err != nil {
		return Entry{}, err
	}
	if len(unfinished) == 0 {
		return Entry{}, nil
	}
	threadID, entryID, reason = strings.TrimSpace(threadID), strings.TrimSpace(entryID), strings.TrimSpace(reason)
	if threadID == "" || entryID == "" || reason == "" {
		return Entry{}, errors.New("branch boundary requires thread, entry, and reason identities")
	}
	entry := Entry{
		ID: entryID, ThreadID: threadID, ParentID: strings.TrimSpace(parentEntryID), Type: EntryTurnMarker,
		TurnID: unfinished[0], TurnStatus: TurnAborted, CreatedAt: now.UTC(),
		Metadata: map[string]string{
			"authority_kind": "branch_boundary", "reason": reason,
			TurnFailureCodeMetadataKey: TurnFailureInterrupted, "failure_reason": BranchBoundaryTurnFailureMessage,
		},
	}
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	return entry, nil
}

type AppendCommittedError struct {
	Err error
}

func (e AppendCommittedError) Error() string {
	return fmt.Sprintf("session tree append committed but thread snapshot save failed: %v", e.Err)
}

func (e AppendCommittedError) Unwrap() error {
	return e.Err
}

type ThreadMeta struct {
	ID                      string            `json:"id"`
	OriginRequestKey        string            `json:"origin_request_key,omitempty"`
	OriginFingerprint       string            `json:"origin_fingerprint,omitempty"`
	LeafID                  string            `json:"leaf_id,omitempty"`
	ParentThreadID          string            `json:"parent_thread_id,omitempty"`
	ParentTurnID            string            `json:"parent_turn_id,omitempty"`
	ForkedFromThreadID      string            `json:"forked_from_thread_id,omitempty"`
	ForkedFromEntryID       string            `json:"forked_from_entry_id,omitempty"`
	LegacyForkRequestID     string            `json:"fork_operation_id,omitempty"`
	LegacyForkNodeID        string            `json:"fork_operation_node_id,omitempty"`
	TaskName                string            `json:"task_name,omitempty"`
	TaskDescription         string            `json:"task_description,omitempty"`
	AgentPath               string            `json:"agent_path,omitempty"`
	HostProfileRef          string            `json:"host_profile_ref,omitempty"`
	ForkMode                string            `json:"fork_mode,omitempty"`
	Lifecycle               ThreadLifecycle   `json:"lifecycle,omitempty"`
	Archived                bool              `json:"archived,omitempty"`
	Title                   string            `json:"title,omitempty"`
	TitleStatus             ThreadTitleStatus `json:"title_status,omitempty"`
	TitleSource             ThreadTitleSource `json:"title_source,omitempty"`
	TitleUpdatedAt          time.Time         `json:"title_updated_at,omitempty"`
	TitleError              string            `json:"title_error,omitempty"`
	TitleGeneration         int64             `json:"title_generation,omitempty"`
	TitleToken              string            `json:"title_token,omitempty"`
	TitleRequestKey         string            `json:"title_request_key,omitempty"`
	TitleRequestFingerprint string            `json:"title_request_fingerprint,omitempty"`
	CreatedAt               time.Time         `json:"created_at"`
	UpdatedAt               time.Time         `json:"updated_at"`
	LastViewedAt            time.Time         `json:"last_viewed_at,omitempty"`
}

// ValidateThreadMetaAuthority enforces the durable distinction between
// independent root threads and parent-owned SubAgent threads.
func ValidateThreadMetaAuthority(meta ThreadMeta) error {
	id := strings.TrimSpace(meta.ID)
	parentID := strings.TrimSpace(meta.ParentThreadID)
	if id == "" {
		return fmt.Errorf("%w: thread id is required", ErrInvalidThreadAuthority)
	}
	if err := ValidateThreadTitleState(meta); err != nil {
		return err
	}
	lifecycle, err := normalizeThreadLifecycle(meta)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidThreadAuthority, err)
	}
	if lifecycle == ThreadLifecycleDeleted {
		return fmt.Errorf("%w: live thread %q cannot use deleted lifecycle", ErrInvalidThreadAuthority, id)
	}
	if parentID == "" {
		if strings.TrimSpace(meta.ParentTurnID) != "" ||
			strings.TrimSpace(meta.TaskName) != "" ||
			strings.TrimSpace(meta.TaskDescription) != "" ||
			strings.TrimSpace(meta.AgentPath) != "" ||
			strings.TrimSpace(meta.HostProfileRef) != "" ||
			strings.TrimSpace(meta.ForkMode) != "" || meta.Lifecycle == ThreadLifecycleClosing || meta.Lifecycle == ThreadLifecycleClosed {
			return fmt.Errorf("%w: root thread %q contains subagent ownership metadata", ErrInvalidThreadAuthority, id)
		}
	} else {
		if parentID == id {
			return fmt.Errorf("%w: thread %q cannot own itself", ErrInvalidThreadAuthority, id)
		}
		if strings.TrimSpace(meta.TaskName) == "" || strings.TrimSpace(meta.HostProfileRef) == "" {
			return fmt.Errorf("%w: child thread %q requires task name and host profile", ErrInvalidThreadAuthority, id)
		}
	}
	return nil
}

// SameThreadAuthority reports whether an update preserves immutable ownership
// and lineage identity.
func SameThreadAuthority(left, right ThreadMeta) bool {
	return left.OriginRequestKey == right.OriginRequestKey &&
		left.OriginFingerprint == right.OriginFingerprint &&
		left.ParentThreadID == right.ParentThreadID &&
		left.ParentTurnID == right.ParentTurnID &&
		left.ForkedFromThreadID == right.ForkedFromThreadID &&
		left.ForkedFromEntryID == right.ForkedFromEntryID &&
		left.TaskName == right.TaskName &&
		left.TaskDescription == right.TaskDescription &&
		left.AgentPath == right.AgentPath &&
		left.HostProfileRef == right.HostProfileRef &&
		left.ForkMode == right.ForkMode
}

// ValidateThreadAuthorityGraph requires every SubAgent parent chain to be
// acyclic and terminate at an existing root thread.
func ValidateThreadAuthorityGraph(threads []ThreadMeta) error {
	byID := make(map[string]ThreadMeta, len(threads))
	for _, meta := range threads {
		if err := ValidateThreadMetaAuthority(meta); err != nil {
			return err
		}
		if _, duplicate := byID[meta.ID]; duplicate {
			return fmt.Errorf("%w: duplicate thread %q", ErrInvalidThreadAuthority, meta.ID)
		}
		byID[meta.ID] = meta
	}
	for _, start := range threads {
		seen := map[string]struct{}{}
		current := start
		for {
			if _, duplicate := seen[current.ID]; duplicate {
				return fmt.Errorf("%w: parent cycle includes thread %q", ErrInvalidThreadAuthority, current.ID)
			}
			seen[current.ID] = struct{}{}
			parentID := strings.TrimSpace(current.ParentThreadID)
			if parentID == "" {
				break
			}
			parent, ok := byID[parentID]
			if !ok {
				return fmt.Errorf("%w: thread %q parent %q is missing", ErrInvalidThreadAuthority, current.ID, parentID)
			}
			current = parent
		}
	}
	return nil
}

// ThreadAuthorityTreeIDs returns one root and all descendants owned through
// ParentThreadID after validating the complete authority graph.
func ThreadAuthorityTreeIDs(threads []ThreadMeta, rootThreadID string) ([]string, error) {
	rootThreadID = strings.TrimSpace(rootThreadID)
	if rootThreadID == "" {
		return nil, errors.New("root thread id is required")
	}
	if err := ValidateThreadAuthorityGraph(threads); err != nil {
		return nil, err
	}
	children := make(map[string][]string)
	rootExists := false
	for _, meta := range threads {
		if meta.ID == rootThreadID {
			rootExists = true
		}
		if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
			children[parentID] = append(children[parentID], meta.ID)
		}
	}
	if !rootExists {
		return nil, ErrThreadNotFound
	}
	for parentID := range children {
		slices.Sort(children[parentID])
	}
	out := make([]string, 0, len(threads))
	var walk func(string)
	walk = func(threadID string) {
		out = append(out, threadID)
		for _, childID := range children[threadID] {
			walk(childID)
		}
	}
	walk(rootThreadID)
	return out, nil
}

type Entry struct {
	ID                      string              `json:"id"`
	ThreadID                string              `json:"thread_id"`
	ParentID                string              `json:"parent_id,omitempty"`
	PathDepth               int64               `json:"path_depth"`
	Type                    EntryType           `json:"type"`
	TurnID                  string              `json:"turn_id,omitempty"`
	RunID                   string              `json:"run_id,omitempty"`
	RequestKey              string              `json:"request_key,omitempty"`
	RequestFingerprint      string              `json:"request_fingerprint,omitempty"`
	Payload                 json.RawMessage     `json:"payload,omitempty"`
	CreatedAt               time.Time           `json:"created_at"`
	Message                 session.Message     `json:"message,omitempty"`
	Raw                     string              `json:"raw,omitempty"`
	RawHash                 string              `json:"raw_hash,omitempty"`
	TurnStatus              TurnMarkerStatus    `json:"turn_status,omitempty"`
	Provider                string              `json:"provider,omitempty"`
	Model                   string              `json:"model,omitempty"`
	CompactionID            string              `json:"compaction_id,omitempty"`
	PreviousCompactionID    string              `json:"previous_compaction_id,omitempty"`
	CompactedThroughEntryID string              `json:"compacted_through_entry_id,omitempty"`
	SummarySchemaVersion    string              `json:"summary_schema_version,omitempty"`
	CompactionGeneration    int                 `json:"compaction_generation,omitempty"`
	CompactionWindowID      string              `json:"compaction_window_id,omitempty"`
	FirstKeptEntryID        string              `json:"first_kept_entry_id,omitempty"`
	KeptUserEntryIDs        []string            `json:"kept_user_entry_ids,omitempty"`
	Summary                 string              `json:"summary,omitempty"`
	CompactionTrigger       string              `json:"compaction_trigger,omitempty"`
	CompactionReason        string              `json:"compaction_reason,omitempty"`
	CompactionPhase         string              `json:"compaction_phase,omitempty"`
	CompactionOperationID   string              `json:"compaction_operation_id,omitempty"`
	CompactionRequestID     string              `json:"compaction_request_id,omitempty"`
	CompactionSource        string              `json:"compaction_source,omitempty"`
	TokensBefore            int64               `json:"tokens_before,omitempty"`
	TokensAfterEstimate     int64               `json:"tokens_after_estimate,omitempty"`
	ContextUsageBefore      contextpolicy.Usage `json:"context_usage_before,omitempty"`
	ContextUsageAfter       contextpolicy.Usage `json:"context_usage_after,omitempty"`
	Error                   string              `json:"error,omitempty"`
	Metadata                map[string]string   `json:"metadata,omitempty"`
}

type AppendOptions struct {
	ID       string
	ParentID string
	Now      time.Time
}

type ForkPosition string

const (
	ForkAt     ForkPosition = "at"
	ForkBefore ForkPosition = "before"
)

type ForkOptions struct {
	SourceThreadID       string
	EntryID              string
	EntryIDPinned        bool
	ExpectedSourceLeafID string
	Position             ForkPosition
	NewThreadID          string
	OriginRequestKey     string
	OriginFingerprint    string
	Now                  time.Time
	TurnIDMap            map[string]string
	RunIDMap             map[string]string
	DestinationMeta      *ForkDestinationMeta
	ArtifactClosure      artifact.Closure
	RewriteEntry         func(Entry, ForkEntryIdentity) (Entry, error)
}

// ForkDestinationMeta is the child ownership metadata written atomically with
// a fork destination. A nil value creates an independent root fork.
type ForkDestinationMeta struct {
	ParentThreadID  string          `json:"parent_thread_id"`
	ParentTurnID    string          `json:"parent_turn_id,omitempty"`
	TaskName        string          `json:"task_name,omitempty"`
	TaskDescription string          `json:"task_description,omitempty"`
	AgentPath       string          `json:"agent_path,omitempty"`
	HostProfileRef  string          `json:"host_profile_ref,omitempty"`
	ForkMode        string          `json:"fork_mode,omitempty"`
	Lifecycle       ThreadLifecycle `json:"lifecycle,omitempty"`
}

type ForkEntryIdentity struct {
	SourceThreadID      string
	DestinationThreadID string
	TurnIDMap           map[string]string
	RunIDMap            map[string]string
}

type ContextOptions struct{}

type ProjectionPurpose string

const (
	ProjectionProviderRequest ProjectionPurpose = "provider_request"
	ProjectionCompaction      ProjectionPurpose = "compaction"
	ProjectionTestUI          ProjectionPurpose = "test_ui"
)

type ContextProjectionOptions struct {
	Purpose ProjectionPurpose
}

type ContextProjection struct {
	Messages []session.Message  `json:"messages"`
	Segments []ProjectedSegment `json:"segments,omitempty"`
}

type ProjectedSegment struct {
	EntryID       string         `json:"entry_id,omitempty"`
	EntryType     EntryType      `json:"entry_type,omitempty"`
	MessageIndex  int            `json:"message_index"`
	Role          session.Role   `json:"role,omitempty"`
	ToolCallID    string         `json:"tool_call_id,omitempty"`
	ToolName      string         `json:"tool_name,omitempty"`
	TokenEstimate int64          `json:"token_estimate,omitempty"`
	ArtifactRefs  []artifact.Ref `json:"artifact_refs,omitempty"`
	UIPreview     string         `json:"ui_preview,omitempty"`
}

// JournalRepo is the durable journal capability used by normal Agent
// execution. It intentionally excludes lifecycle creation, deletion, fork,
// and metadata replacement capabilities.
type JournalRepo interface {
	Thread(context.Context, string) (ThreadMeta, error)
	Append(context.Context, Entry, AppendOptions) (Entry, error)
	Entry(context.Context, string, string) (Entry, error)
	Entries(context.Context, string) ([]Entry, error)
	Path(context.Context, string, string) ([]Entry, error)
	PathPage(context.Context, string, string, string, int) (PathPage, error)
}

// RuntimeJournalRepo is the single low-frequency canonical writer used by the
// v4 ThreadRuntime. It does not expose leases or receipts to hosts.
type RuntimeJournalRepo interface {
	AppendRuntimeFacts(context.Context, string, []Entry) ([]Entry, error)
}

// CanonicalTurnRepo reads all journal entries associated with one exact turn.
// Execution replay authority remains owned by TurnAuthorityRepo.
type CanonicalTurnRepo interface {
	CanonicalTurnEntries(context.Context, string, string, string) ([]Entry, bool, error)
}

// Repo is the internal storage implementation contract. Production runtime
// actors receive narrower capabilities such as JournalRepo instead.
type Repo interface {
	JournalRepo
	CreateThread(context.Context, ThreadMeta) (ThreadMeta, error)
	UpdateThread(context.Context, ThreadMeta) error
	MoveLeaf(context.Context, string, string) error
	Fork(context.Context, ForkOptions) (ThreadMeta, error)
}

type PathPage struct {
	Entries     []Entry
	NextEntryID string
	HasMore     bool
	// NewestOrdinal is the active-path ordinal of Entries[0]. Entries are
	// returned newest first, so later entries decrement this value by one.
	NewestOrdinal int64
}

type ThreadListRepo interface {
	ListThreads(context.Context, ListThreadsOptions) ([]ThreadMeta, error)
}

type ListThreadsOptions struct {
	IncludeArchived bool
	RootOnly        bool
	Limit           int
	AfterCreatedAt  time.Time
	AfterID         string
}

func ListThreads(ctx context.Context, repo JournalRepo, opts ListThreadsOptions) ([]ThreadMeta, error) {
	if repo == nil {
		return nil, errors.New("session tree repo is required")
	}
	listRepo, ok := repo.(ThreadListRepo)
	if !ok {
		return nil, errors.New("session tree repo does not support thread listing")
	}
	threads, err := listRepo.ListThreads(ctx, opts)
	if err != nil {
		return nil, err
	}
	return ApplyThreadListOptions(threads, opts), nil
}

func SortThreadsByCreatedAtDesc(threads []ThreadMeta) {
	slices.SortStableFunc(threads, func(left, right ThreadMeta) int {
		switch {
		case left.CreatedAt.After(right.CreatedAt):
			return -1
		case right.CreatedAt.After(left.CreatedAt):
			return 1
		case left.ID < right.ID:
			return -1
		case left.ID > right.ID:
			return 1
		default:
			return 0
		}
	})
}

func ApplyThreadListOptions(threads []ThreadMeta, opts ListThreadsOptions) []ThreadMeta {
	SortThreadsByCreatedAtDesc(threads)
	out := threads[:0]
	for _, meta := range threads {
		if meta.Archived && !opts.IncludeArchived {
			continue
		}
		if opts.RootOnly && strings.TrimSpace(meta.ParentThreadID) != "" {
			continue
		}
		if !threadAfterListCursor(meta, opts) {
			continue
		}
		out = append(out, meta)
		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}
	}
	return out
}

func threadAfterListCursor(meta ThreadMeta, opts ListThreadsOptions) bool {
	afterID := strings.TrimSpace(opts.AfterID)
	if opts.AfterCreatedAt.IsZero() || afterID == "" {
		return true
	}
	if meta.CreatedAt.Before(opts.AfterCreatedAt) {
		return true
	}
	if meta.CreatedAt.Equal(opts.AfterCreatedAt) && meta.ID > afterID {
		return true
	}
	return false
}

type ThreadPublishRepo interface {
	CreateThreadWithInitialEntry(context.Context, ThreadMeta, Entry) (ThreadMeta, Entry, error)
	ForkWithInitialEntry(context.Context, ForkOptions, Entry) (ThreadMeta, Entry, error)
}

type AgentTodoStatus string

// MaxAgentTodoItems is the canonical upper bound for one Agent todo state.
const MaxAgentTodoItems = 40

const (
	AgentTodoPending    AgentTodoStatus = "pending"
	AgentTodoInProgress AgentTodoStatus = "in_progress"
	AgentTodoCompleted  AgentTodoStatus = "completed"
)

type AgentTodoItem struct {
	ID      string          `json:"id"`
	Content string          `json:"content"`
	Status  AgentTodoStatus `json:"status"`
}

// ValidateAgentTodoItems checks canonical todo identity, content, status, and
// state-machine invariants at the storage boundary.
func ValidateAgentTodoItems(items []AgentTodoItem) error {
	if len(items) > MaxAgentTodoItems {
		return fmt.Errorf("agent todo state has %d items, maximum is %d", len(items), MaxAgentTodoItems)
	}
	seen := make(map[string]struct{}, len(items))
	inProgress := 0
	for index, item := range items {
		id := strings.TrimSpace(item.ID)
		content := strings.TrimSpace(item.Content)
		switch item.Status {
		case AgentTodoPending, AgentTodoInProgress, AgentTodoCompleted:
		default:
			return fmt.Errorf("agent todo item %d has unsupported status %q", index, item.Status)
		}
		if id == "" || content == "" {
			return fmt.Errorf("agent todo item %d requires id and content", index)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("agent todo state repeats item %q", id)
		}
		seen[id] = struct{}{}
		if item.Status == AgentTodoInProgress {
			inProgress++
			if inProgress > 1 {
				return errors.New("agent todo state has more than one in-progress item")
			}
		}
	}
	return nil
}

type AgentTodoState struct {
	ThreadID          string          `json:"thread_id"`
	Version           int64           `json:"version"`
	Items             []AgentTodoItem `json:"items"`
	UpdatedAt         time.Time       `json:"updated_at,omitempty"`
	UpdatedByTurnID   string          `json:"updated_by_turn_id,omitempty"`
	UpdatedByRunID    string          `json:"updated_by_run_id,omitempty"`
	UpdatedByToolCall string          `json:"updated_by_tool_call_id,omitempty"`
}

type AgentTodoStateRepo interface {
	ReadAgentTodoState(context.Context, string) (AgentTodoState, error)
	CompareAndSwapAgentTodoState(context.Context, AgentTodoState, int64) (AgentTodoState, error)
}

type MemoryRepo struct {
	mu                        sync.Mutex
	threads                   map[string]ThreadMeta
	entries                   map[string][]Entry
	entryOrdinals             map[string]map[string]int
	entryDepths               map[string]map[string]int64
	turnEntryOrdinals         map[string]map[string][]int
	turnEntryCounts           map[string]map[string]int
	now                       func() time.Time
	todos                     map[string]AgentTodoState
	tombstones                map[string]ThreadTombstone
	effectAttempts            map[string]EffectAttempt
	effectAttemptByInvocation map[string]string
	effectAttemptSequence     int64
	providerStates            map[string]ProviderStateRecord
	artifacts                 map[string]artifact.Record
	seq                       int64
}

func NewMemoryRepo() *MemoryRepo {
	return newMemoryRepo(time.Now)
}

func newMemoryRepo(now func() time.Time) *MemoryRepo {
	if now == nil {
		now = time.Now
	}
	return &MemoryRepo{
		threads:                   map[string]ThreadMeta{},
		entries:                   map[string][]Entry{},
		entryOrdinals:             map[string]map[string]int{},
		entryDepths:               map[string]map[string]int64{},
		turnEntryOrdinals:         map[string]map[string][]int{},
		turnEntryCounts:           map[string]map[string]int{},
		now:                       now,
		todos:                     map[string]AgentTodoState{},
		tombstones:                map[string]ThreadTombstone{},
		effectAttempts:            map[string]EffectAttempt{},
		effectAttemptByInvocation: map[string]string{},
		providerStates:            map[string]ProviderStateRecord{},
		artifacts:                 map[string]artifact.Record{},
	}
}

func (r *MemoryRepo) CreateThread(_ context.Context, meta ThreadMeta) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.createThreadLocked(meta)
}

func (r *MemoryRepo) createThreadLocked(meta ThreadMeta) (ThreadMeta, error) {
	if meta.ID == "" {
		for {
			r.seq++
			meta.ID = fmt.Sprintf("thread-%d", r.seq)
			if _, ok := r.threads[meta.ID]; !ok {
				break
			}
		}
	} else if _, ok := r.threads[meta.ID]; ok {
		return ThreadMeta{}, ErrThreadExists
	} else if _, ok := r.tombstones[meta.ID]; ok {
		return ThreadMeta{}, ErrThreadDeleted
	}
	now := meta.CreatedAt
	if now.IsZero() {
		now = r.now().UTC()
	}
	meta.CreatedAt = now
	meta.UpdatedAt = now
	if meta.Lifecycle == "" {
		meta.Lifecycle = ThreadLifecycleOpen
	}
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return ThreadMeta{}, err
	}
	if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
		parent, ok := r.threads[parentID]
		if !ok {
			return ThreadMeta{}, fmt.Errorf("%w: parent thread %q", ErrInvalidThreadAuthority, parentID)
		}
		if err := lifecycleRejectsWrite(parent); err != nil {
			return ThreadMeta{}, err
		}
	}
	r.threads[meta.ID] = meta
	return meta, nil
}

func (r *MemoryRepo) CreateThreadWithInitialEntry(ctx context.Context, meta ThreadMeta, initial Entry) (ThreadMeta, Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seqBefore := r.seq
	created, err := r.createThreadLocked(meta)
	if err != nil {
		return ThreadMeta{}, Entry{}, err
	}
	initial.ThreadID = created.ID
	saved, err := r.appendLocked(ctx, initial, AppendOptions{Now: created.CreatedAt})
	if err != nil {
		delete(r.threads, created.ID)
		r.deleteIndexedEntriesLocked(created.ID)
		delete(r.todos, created.ID)
		r.seq = seqBefore
		return ThreadMeta{}, Entry{}, err
	}
	return r.threads[created.ID], saved, nil
}

func (r *MemoryRepo) ReadAgentTodoState(_ context.Context, threadID string) (AgentTodoState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[threadID]; !ok {
		return AgentTodoState{}, ErrThreadNotFound
	}
	state, ok := r.todos[threadID]
	if !ok {
		return AgentTodoState{ThreadID: threadID}, nil
	}
	return cloneAgentTodoState(state), nil
}

func (r *MemoryRepo) CompareAndSwapAgentTodoState(ctx context.Context, state AgentTodoState, expectedVersion int64) (AgentTodoState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ValidateAgentTodoItems(state.Items); err != nil {
		return AgentTodoState{}, err
	}
	if _, ok := r.threads[state.ThreadID]; !ok {
		return AgentTodoState{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(r.threads[state.ThreadID]); err != nil {
		return AgentTodoState{}, err
	}
	_ = ctx
	current := r.todos[state.ThreadID]
	if current.Version != expectedVersion {
		return AgentTodoState{}, ErrAgentTodoVersionConflict
	}
	state.Version = expectedVersion + 1
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = time.Now()
	}
	state = cloneAgentTodoState(state)
	r.todos[state.ThreadID] = state
	return cloneAgentTodoState(state), nil
}

func (r *MemoryRepo) Thread(_ context.Context, threadID string) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[threadID]
	if !ok {
		return ThreadMeta{}, ErrThreadNotFound
	}
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return ThreadMeta{}, err
	}
	return meta, nil
}

func (r *MemoryRepo) ListThreads(_ context.Context, opts ListThreadsOptions) ([]ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ThreadMeta, 0, len(r.threads))
	for _, meta := range r.threads {
		if err := ValidateThreadMetaAuthority(meta); err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	return ApplyThreadListOptions(out, opts), nil
}

func (r *MemoryRepo) UpdateThread(ctx context.Context, meta ThreadMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.threads[meta.ID]
	if !ok {
		return ErrThreadNotFound
	}
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return err
	}
	if !SameThreadAuthority(current, meta) {
		return fmt.Errorf("%w: thread %q authority is immutable", ErrInvalidThreadAuthority, meta.ID)
	}
	if !SameThreadTitleState(current, meta) {
		return ErrRequestConflict
	}
	if err := lifecycleRejectsWrite(current); err != nil {
		return err
	}
	_ = ctx
	meta.UpdatedAt = nonZeroTime(meta.UpdatedAt)
	r.threads[meta.ID] = meta
	return nil
}

func (r *MemoryRepo) Append(ctx context.Context, entry Entry, opts AppendOptions) (Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.appendLocked(ctx, entry, opts)
}

func (r *MemoryRepo) AppendRuntimeFacts(ctx context.Context, threadID string, entries []Entry) ([]Entry, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" || len(entries) == 0 {
		return nil, errors.New("runtime journal append requires thread and entries")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[threadID]; !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return nil, ErrThreadDeleted
		}
		return nil, ErrThreadNotFound
	}
	writeCtx := ctx
	seen := make(map[string]struct{}, len(entries))
	replayed := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry.ID) == "" || entry.ThreadID != "" && entry.ThreadID != threadID {
			return nil, errors.New("runtime journal fact requires stable matching identity")
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return nil, ErrRequestConflict
		}
		seen[entry.ID] = struct{}{}
		var existing *Entry
		for index := range r.entries[threadID] {
			candidate := &r.entries[threadID][index]
			if candidate.ID == entry.ID {
				existing = candidate
				break
			}
		}
		if existing != nil {
			if existing.ID != entry.ID || existing.Type != entry.Type || existing.RequestFingerprint != entry.RequestFingerprint || !bytes.Equal(existing.Payload, entry.Payload) {
				return nil, ErrRequestConflict
			}
			replayed = append(replayed, cloneEntry(*existing))
		}
	}
	if len(replayed) != 0 {
		if len(replayed) != len(entries) {
			return nil, ErrAuthorityCorrupt
		}
		return replayed, nil
	}
	previousEntries := cloneEntries(r.entries[threadID])
	previousOrdinals := maps.Clone(r.entryOrdinals[threadID])
	previousDepths := maps.Clone(r.entryDepths[threadID])
	previousTurnOrdinals := cloneOrdinalLists(r.turnEntryOrdinals[threadID])
	previousTurnCounts := maps.Clone(r.turnEntryCounts[threadID])
	previousMeta := r.threads[threadID]
	previousSequence := r.seq
	rollback := func() {
		r.entries[threadID] = previousEntries
		r.entryOrdinals[threadID] = previousOrdinals
		r.entryDepths[threadID] = previousDepths
		r.turnEntryOrdinals[threadID] = previousTurnOrdinals
		r.turnEntryCounts[threadID] = previousTurnCounts
		r.threads[threadID] = previousMeta
		r.seq = previousSequence
	}
	out := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		entry.ThreadID = threadID
		saved, err := r.appendLocked(writeCtx, entry, AppendOptions{ID: entry.ID})
		if err != nil {
			rollback()
			return nil, err
		}
		out = append(out, saved)
	}
	return out, nil
}

func cloneOrdinalLists(source map[string][]int) map[string][]int {
	cloned := make(map[string][]int, len(source))
	for key, values := range source {
		cloned[key] = append([]int(nil), values...)
	}
	return cloned
}

func (r *MemoryRepo) appendLocked(ctx context.Context, entry Entry, opts AppendOptions) (Entry, error) {
	meta, ok := r.threads[entry.ThreadID]
	if !ok {
		return Entry{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return Entry{}, err
	}
	if turnID := strings.TrimSpace(entry.TurnID); turnID != "" {
		if _, terminal := runtimeTurnTerminal(r.entries[entry.ThreadID], turnID); terminal {
			return Entry{}, ErrStaleAuthority
		}
	}
	_ = ctx
	if err := ValidateNewEntryMessageAttachments(entry); err != nil {
		return Entry{}, err
	}
	if err := ValidateEntryMessageReferences(entry); err != nil {
		return Entry{}, err
	}
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		runID := strings.TrimSpace(entry.Metadata["run_id"])
		if runID == "" {
			return Entry{}, fmt.Errorf("%w: started turn requires run identity", ErrInvalidThreadAuthority)
		}
		if r.hasTurnStartedLocked(entry.ThreadID, entry.TurnID) {
			return Entry{}, ErrRequestConflict
		}
		entry.Metadata = cloneStringMap(entry.Metadata)
		entry.Metadata["run_id"] = runID
	}
	if opts.ParentID != "" {
		entry.ParentID = opts.ParentID
	} else if entry.ParentID == "" {
		entry.ParentID = meta.LeafID
	}
	if entry.ParentID != "" {
		if _, ok := r.entryOrdinals[entry.ThreadID][entry.ParentID]; !ok {
			return Entry{}, ErrInvalidParent
		}
	}
	if opts.ID != "" {
		entry.ID = opts.ID
	}
	if entry.ID == "" {
		entry.ID = r.nextEntryID(entry.ThreadID)
	} else if _, exists := r.entryOrdinals[entry.ThreadID][entry.ID]; exists {
		return Entry{}, fmt.Errorf("session tree entry id already exists: %s", entry.ID)
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = opts.Now
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	entry.PathDepth = 1
	if entry.ParentID != "" {
		parentDepth, ok := r.entryDepths[entry.ThreadID][entry.ParentID]
		if !ok || parentDepth <= 0 {
			return Entry{}, ErrAuthorityCorrupt
		}
		entry.PathDepth = parentDepth + 1
	}
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	r.appendIndexedEntriesLocked(entry.ThreadID, entry)
	meta.LeafID = entry.ID
	meta.UpdatedAt = entry.CreatedAt
	r.threads[entry.ThreadID] = meta
	return cloneEntry(entry), nil
}

func (r *MemoryRepo) hasTurnStartedLocked(threadID, turnID string) bool {
	for _, ordinal := range r.turnEntryOrdinals[threadID][turnID] {
		entries := r.entries[threadID]
		if ordinal >= 0 && ordinal < len(entries) && entries[ordinal].Type == EntryTurnMarker && entries[ordinal].TurnStatus == TurnStarted {
			return true
		}
	}
	return false
}

func (r *MemoryRepo) appendIndexedEntriesLocked(threadID string, entries ...Entry) {
	if len(entries) == 0 {
		return
	}
	if r.turnEntryOrdinals[threadID] == nil {
		r.turnEntryOrdinals[threadID] = map[string][]int{}
	}
	if r.entryOrdinals[threadID] == nil {
		r.entryOrdinals[threadID] = map[string]int{}
	}
	if r.entryDepths[threadID] == nil {
		r.entryDepths[threadID] = map[string]int64{}
	}
	if r.turnEntryCounts[threadID] == nil {
		r.turnEntryCounts[threadID] = map[string]int{}
	}
	for _, entry := range entries {
		ordinal := len(r.entries[threadID])
		copy := cloneEntry(entry)
		copy.PathDepth = 1
		if copy.ParentID != "" {
			copy.PathDepth = r.entryDepths[threadID][copy.ParentID] + 1
		}
		r.entries[threadID] = append(r.entries[threadID], copy)
		r.entryOrdinals[threadID][copy.ID] = ordinal
		r.entryDepths[threadID][copy.ID] = copy.PathDepth
		if strings.TrimSpace(copy.TurnID) != "" {
			r.turnEntryOrdinals[threadID][copy.TurnID] = append(r.turnEntryOrdinals[threadID][copy.TurnID], ordinal)
			r.turnEntryCounts[threadID][copy.TurnID]++
		}
	}
}

func (r *MemoryRepo) replaceIndexedEntriesLocked(threadID string, entries []Entry) error {
	entryOrdinals := make(map[string]int, len(entries))
	entryDepths := make(map[string]int64, len(entries))
	indexed := map[string][]int{}
	started := map[string]struct{}{}
	cloned := cloneEntries(entries)
	for ordinal, entry := range cloned {
		if entry.ThreadID != threadID {
			return ErrAuthorityCorrupt
		}
		if strings.TrimSpace(entry.ID) == "" {
			return ErrAuthorityCorrupt
		}
		if _, duplicate := entryOrdinals[entry.ID]; duplicate {
			return ErrAuthorityCorrupt
		}
		entryOrdinals[entry.ID] = ordinal
		expectedDepth := int64(1)
		unresolvedParent := false
		if entry.ParentID != "" {
			parentDepth, ok := entryDepths[entry.ParentID]
			if !ok || parentDepth <= 0 {
				// FileRepo keeps metadata readable when a journal is already
				// broken; path and canonical reads still fail on the missing
				// parent instead of repairing or adopting the orphan.
				unresolvedParent = true
			} else {
				expectedDepth = parentDepth + 1
			}
		}
		if !unresolvedParent && entry.PathDepth != expectedDepth {
			return ErrAuthorityCorrupt
		}
		if unresolvedParent {
			entryDepths[entry.ID] = 0
		} else {
			entryDepths[entry.ID] = entry.PathDepth
		}
		if err := ValidateEntryMessageReferences(entry); err != nil {
			return ErrAuthorityCorrupt
		}
		turnID := strings.TrimSpace(entry.TurnID)
		if turnID == "" {
			continue
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			if _, duplicate := started[turnID]; duplicate {
				return ErrAuthorityCorrupt
			}
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if runID == "" {
				return ErrAuthorityCorrupt
			}
			started[turnID] = struct{}{}
		}
		indexed[turnID] = append(indexed[turnID], ordinal)
	}
	r.entries[threadID] = cloned
	r.entryOrdinals[threadID] = entryOrdinals
	r.entryDepths[threadID] = entryDepths
	r.turnEntryOrdinals[threadID] = indexed
	counts := make(map[string]int, len(indexed))
	for turnID, ordinals := range indexed {
		counts[turnID] = len(ordinals)
	}
	r.turnEntryCounts[threadID] = counts
	return nil
}

func (r *MemoryRepo) deleteIndexedEntriesLocked(threadID string) {
	delete(r.entries, threadID)
	delete(r.entryOrdinals, threadID)
	delete(r.entryDepths, threadID)
	delete(r.turnEntryOrdinals, threadID)
	delete(r.turnEntryCounts, threadID)
}

func (r *MemoryRepo) nextEntryID(threadID string) string {
	for {
		r.seq++
		id := fmt.Sprintf("%s-entry-%d", threadID, r.seq)
		if !containsEntry(r.entries[threadID], id) {
			return id
		}
	}
}

func (r *MemoryRepo) Entry(_ context.Context, threadID, entryID string) (Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ordinal, ok := r.entryOrdinals[threadID][entryID]
	if !ok {
		return Entry{}, ErrEntryNotFound
	}
	entries := r.entries[threadID]
	if ordinal < 0 || ordinal >= len(entries) {
		return Entry{}, ErrAuthorityCorrupt
	}
	entry := entries[ordinal]
	depth, depthOK := r.entryDepths[threadID][entryID]
	if entry.ID != entryID || entry.ThreadID != threadID || !depthOK || depth <= 0 || entry.PathDepth != depth {
		return Entry{}, ErrAuthorityCorrupt
	}
	if err := ValidateEntryIntegrity(entry); err != nil {
		return Entry{}, err
	}
	return cloneEntry(entry), nil
}

func (r *MemoryRepo) Entries(_ context.Context, threadID string) ([]Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[threadID]; !ok {
		return nil, ErrThreadNotFound
	}
	return cloneEntries(r.entries[threadID]), nil
}

func (r *MemoryRepo) CanonicalTurnEntries(_ context.Context, threadID, turnID, runID string) ([]Entry, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[threadID]; !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return nil, false, ErrThreadDeleted
		}
		return nil, false, ErrThreadNotFound
	}
	ordinals := r.turnEntryOrdinals[threadID][turnID]
	if len(ordinals) == 0 {
		return nil, false, nil
	}
	if r.turnEntryCounts[threadID][turnID] != len(ordinals) {
		return nil, true, ErrAuthorityCorrupt
	}
	all := r.entries[threadID]
	entries := make([]Entry, 0, len(ordinals))
	previous := -1
	for _, ordinal := range ordinals {
		if ordinal <= previous || ordinal < 0 || ordinal >= len(all) {
			return nil, true, ErrAuthorityCorrupt
		}
		entry := all[ordinal]
		if entry.ThreadID != threadID || entry.TurnID != turnID {
			return nil, true, ErrAuthorityCorrupt
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return nil, true, err
		}
		entries = append(entries, cloneEntry(entry))
		previous = ordinal
	}
	entries = CanonicalTurnEntriesForRead(entries)
	if err := ValidateCanonicalTurnEntries(entries, threadID, turnID, runID); err != nil {
		return nil, true, err
	}
	return entries, true, nil
}

// CanonicalTurnEntriesForRead excludes retry/fork structural closures when the
// same turn already has its execution terminal. A copied unfinished turn keeps
// its branch boundary as the only canonical terminal.
func CanonicalTurnEntriesForRead(entries []Entry) []Entry {
	hasExecutionTerminal := false
	for _, entry := range entries {
		if entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) && entry.Metadata["authority_kind"] != "branch_boundary" {
			hasExecutionTerminal = true
			break
		}
	}
	if !hasExecutionTerminal {
		return entries
	}
	filtered := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) && entry.Metadata["authority_kind"] == "branch_boundary" {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func ValidateCanonicalTurnEntries(entries []Entry, threadID, turnID, runID string) error {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	runID = strings.TrimSpace(runID)
	if threadID == "" || turnID == "" || runID == "" || len(entries) == 0 {
		return ErrAuthorityCorrupt
	}
	seenIDs := make(map[string]int, len(entries))
	startedIndex := -1
	terminalIndex := -1
	storedRunID := ""
	userEntries := 0
	var retrySource *CanonicalTurnRetrySource
	for index, entry := range entries {
		if entry.ThreadID != threadID || entry.TurnID != turnID || strings.TrimSpace(entry.ID) == "" {
			return ErrAuthorityCorrupt
		}
		if _, duplicate := seenIDs[entry.ID]; duplicate {
			return ErrAuthorityCorrupt
		}
		seenIDs[entry.ID] = index
		if entry.Type == EntryUserMessage {
			userEntries++
		}
		if entry.Type != EntryTurnMarker {
			continue
		}
		if entry.TurnStatus == TurnStarted {
			if startedIndex >= 0 {
				return ErrAuthorityCorrupt
			}
			startedIndex = index
			storedRunID = strings.TrimSpace(entry.Metadata["run_id"])
			var err error
			retrySource, err = CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return err
			}
		}
		// TurnWaiting is an unresolved interaction boundary, not the final
		// outcome of the turn. A later interaction result and one final marker
		// may therefore follow it on the same canonical path.
		if terminalTurnMarker(entry.TurnStatus) && entry.TurnStatus != TurnWaiting {
			if terminalIndex >= 0 {
				return ErrAuthorityCorrupt
			}
			terminalIndex = index
		}
	}
	if startedIndex != 0 || storedRunID == "" || (terminalIndex >= 0 && terminalIndex < startedIndex) {
		return ErrAuthorityCorrupt
	}
	if parentIndex, internal := seenIDs[entries[0].ParentID]; internal && parentIndex >= 0 {
		return ErrAuthorityCorrupt
	}
	for index := 1; index < len(entries); index++ {
		entry := entries[index]
		if terminalIndex >= 0 && index > terminalIndex {
			if !isCanonicalPostTerminalTurnEntry(entry) || strings.TrimSpace(entry.ParentID) == "" || entry.ParentID == entry.ID {
				return ErrAuthorityCorrupt
			}
			if parentIndex, internal := seenIDs[entry.ParentID]; internal && parentIndex >= index {
				return ErrAuthorityCorrupt
			}
			continue
		}
		if entry.ParentID != entries[index-1].ID {
			return ErrAuthorityCorrupt
		}
	}
	if storedRunID != runID {
		return ErrRequestConflict
	}
	if retrySource != nil && userEntries != 0 {
		return ErrAuthorityCorrupt
	}
	return nil
}

func isCanonicalPostTerminalTurnEntry(entry Entry) bool {
	return entry.Type == EntryCustom &&
		entry.Metadata[PendingToolSettlementKindKey] == PendingToolSettlementKind &&
		strings.TrimSpace(entry.Metadata[PendingToolSettlementFingerprintKey]) != "" &&
		strings.TrimSpace(entry.Message.ToolCallID) != "" &&
		strings.TrimSpace(entry.Message.ToolName) != ""
}

func (r *MemoryRepo) Path(_ context.Context, threadID, leafID string) ([]Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return pathLocked(r.threads, r.entries, threadID, leafID)
}

func (r *MemoryRepo) PathPage(_ context.Context, threadID, leafID, beforeEntryID string, limit int) (PathPage, error) {
	if limit <= 0 {
		return PathPage{}, errors.New("path page limit must be positive")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	path, err := pathLocked(r.threads, r.entries, threadID, leafID)
	if err != nil {
		return PathPage{}, err
	}
	end := len(path)
	if beforeEntryID != "" {
		end = -1
		for index, entry := range path {
			if entry.ID == beforeEntryID {
				end = index
				break
			}
		}
		if end < 0 {
			return PathPage{}, ErrEntryNotFound
		}
	}
	start := end - limit
	if start < 0 {
		start = 0
	}
	entries := make([]Entry, 0, end-start)
	for index := end - 1; index >= start; index-- {
		entries = append(entries, cloneEntry(path[index]))
	}
	page := PathPage{Entries: entries, HasMore: start > 0, NewestOrdinal: int64(end)}
	if page.HasMore {
		page.NextEntryID = path[start].ID
	}
	return page, nil
}

func (r *MemoryRepo) MoveLeaf(_ context.Context, threadID, entryID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[threadID]
	if !ok {
		return ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return err
	}
	if entryID != "" && !containsEntry(r.entries[threadID], entryID) {
		return ErrEntryNotFound
	}
	meta.LeafID = entryID
	meta.UpdatedAt = time.Now()
	r.threads[threadID] = meta
	return nil
}

func (r *MemoryRepo) Fork(ctx context.Context, opts ForkOptions) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.forkLocked(ctx, opts)
}

func (r *MemoryRepo) forkLocked(ctx context.Context, opts ForkOptions) (ThreadMeta, error) {
	opts = snapshotForkIdentityMaps(opts)
	if opts.Position == "" {
		opts.Position = ForkAt
	}
	sourceMeta, ok := r.threads[opts.SourceThreadID]
	if !ok {
		return ThreadMeta{}, ErrThreadNotFound
	}
	if sourceMeta.IsClosed() {
		return ThreadMeta{}, ErrThreadClosed
	}
	targetID := opts.EntryID
	if targetID == "" && !opts.EntryIDPinned {
		targetID = sourceMeta.LeafID
	}
	if opts.Position == ForkBefore {
		entry, ok := findEntry(r.entries[opts.SourceThreadID], targetID)
		if !ok {
			return ThreadMeta{}, ErrEntryNotFound
		}
		targetID = entry.ParentID
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	newID := opts.NewThreadID
	if existing, ok := r.threads[newID]; newID != "" && ok {
		if forkDestinationMatches(existing, opts, targetID) {
			return existing, nil
		}
		return ThreadMeta{}, ErrThreadExists
	}
	path, err := pathLocked(r.threads, r.entries, opts.SourceThreadID, targetID)
	if err != nil {
		return ThreadMeta{}, err
	}
	if newID == "" {
		for {
			r.seq++
			newID = fmt.Sprintf("%s-fork-%d", opts.SourceThreadID, r.seq)
			if _, ok := r.threads[newID]; !ok {
				break
			}
		}
	}
	if _, deleted := r.tombstones[newID]; deleted {
		return ThreadMeta{}, ErrForkDestinationConflict
	}
	if len(r.entries[newID]) > 0 {
		return ThreadMeta{}, ErrThreadExists
	}
	closure := artifact.CloneClosure(opts.ArtifactClosure)
	if artifact.IsZeroClosure(closure) {
		closure, err = r.artifactClosureLocked(opts.SourceThreadID, newID, path)
		if err != nil {
			return ThreadMeta{}, err
		}
	}
	if err := r.validateArtifactClosureLocked(opts.SourceThreadID, newID, path, closure); err != nil {
		return ThreadMeta{}, err
	}
	meta := ThreadMeta{
		ID: newID, ForkedFromThreadID: opts.SourceThreadID, ForkedFromEntryID: targetID,
		OriginRequestKey: strings.TrimSpace(opts.OriginRequestKey), OriginFingerprint: strings.TrimSpace(opts.OriginFingerprint),
		CreatedAt: now, UpdatedAt: now,
	}
	applyForkDestinationMeta(&meta, opts.DestinationMeta)
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return ThreadMeta{}, err
	}
	if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
		parent, ok := r.threads[parentID]
		if !ok {
			return ThreadMeta{}, fmt.Errorf("%w: parent thread %q", ErrInvalidThreadAuthority, parentID)
		}
		if parent.IsClosed() {
			return ThreadMeta{}, ErrThreadClosed
		}
	}
	oldToNew := map[string]string{"": ""}
	retryTargetEntryIDs := make(map[string]struct{})
	for _, entry := range path {
		if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted {
			continue
		}
		retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entry)
		if err != nil {
			return ThreadMeta{}, err
		}
		if retrySource != nil {
			retryTargetEntryIDs[retrySource.EntryID] = struct{}{}
		}
	}
	forkedEntries := make([]Entry, 0, len(path))
	for _, entry := range path {
		r.seq++
		next := cloneEntry(entry)
		next.ID = fmt.Sprintf("%s-entry-%d", newID, r.seq)
		next.ThreadID = newID
		next.ParentID = oldToNew[entry.ParentID]
		next.TurnID = rewriteForkID(next.TurnID, opts.TurnIDMap)
		next.FirstKeptEntryID = oldToNew[entry.FirstKeptEntryID]
		next.CompactedThroughEntryID = oldToNew[entry.CompactedThroughEntryID]
		next.KeptUserEntryIDs = rewriteEntryIDs(entry.KeptUserEntryIDs, oldToNew)
		next.Metadata = rewriteForkMetadata(next.Metadata, oldToNew, opts.TurnIDMap, opts.RunIDMap)
		expectedID := next.ID
		expectedThreadID := next.ThreadID
		expectedParentID := next.ParentID
		expectedTurnID := next.TurnID
		expectedRunID := next.Metadata["run_id"]
		expectedRetrySourceTurnID := next.Metadata[RetrySourceTurnIDMetadataKey]
		expectedRetrySourceEntryID := next.Metadata[RetrySourceEntryIDMetadataKey]
		sourceStarted := entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted
		_, sourceTarget := retryTargetEntryIDs[entry.ID]
		if opts.RewriteEntry != nil {
			next, err = opts.RewriteEntry(next, ForkEntryIdentity{
				SourceThreadID:      opts.SourceThreadID,
				DestinationThreadID: newID,
				TurnIDMap:           cloneStringMap(opts.TurnIDMap),
				RunIDMap:            cloneStringMap(opts.RunIDMap),
			})
			if err != nil {
				return ThreadMeta{}, err
			}
		}
		destinationStarted := next.Type == EntryTurnMarker && next.TurnStatus == TurnStarted
		if sourceStarted != destinationStarted {
			return ThreadMeta{}, ErrAuthorityCorrupt
		}
		if (sourceStarted || sourceTarget) &&
			(next.ID != expectedID || next.ThreadID != expectedThreadID || next.ParentID != expectedParentID || next.TurnID != expectedTurnID) {
			return ThreadMeta{}, ErrAuthorityCorrupt
		}
		if sourceTarget && (next.Type != entry.Type || next.TurnStatus != entry.TurnStatus ||
			next.Metadata["run_id"] != expectedRunID ||
			next.Metadata[RetrySourceTurnIDMetadataKey] != expectedRetrySourceTurnID ||
			next.Metadata[RetrySourceEntryIDMetadataKey] != expectedRetrySourceEntryID) {
			return ThreadMeta{}, ErrAuthorityCorrupt
		}
		if sourceStarted {
			if next.Type != EntryTurnMarker || next.TurnStatus != TurnStarted {
				return ThreadMeta{}, ErrAuthorityCorrupt
			}
			sourceRetry, err := CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return ThreadMeta{}, err
			}
			destinationRetry, err := CanonicalTurnRetrySourceForStartedEntry(next)
			if err != nil {
				return ThreadMeta{}, err
			}
			if (sourceRetry == nil) != (destinationRetry == nil) {
				return ThreadMeta{}, ErrAuthorityCorrupt
			}
			if sourceRetry != nil && (destinationRetry.EntryID != oldToNew[sourceRetry.EntryID] ||
				destinationRetry.TurnID != rewriteForkID(sourceRetry.TurnID, opts.TurnIDMap)) {
				return ThreadMeta{}, ErrAuthorityCorrupt
			}
			if expectedRunID == "" || next.Metadata["run_id"] != expectedRunID {
				return ThreadMeta{}, ErrAuthorityCorrupt
			}
		}
		next.CreatedAt = now
		next.Raw = rawForEntry(next)
		next.RawHash = stableHash(next.Raw)
		oldToNew[entry.ID] = next.ID
		forkedEntries = append(forkedEntries, next)
		meta.LeafID = next.ID
	}
	stagedArtifacts, err := r.stageArtifactForkLocked(closure, oldToNew, now)
	if err != nil {
		return ThreadMeta{}, err
	}
	for _, item := range closure.Items {
		entry, ok := memoryEntryByID(forkedEntries, oldToNew[item.SourceEntryID])
		if !ok || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil || !reflect.DeepEqual(*entry.Message.ToolResult.FullOutput, item.Ref) {
			return ThreadMeta{}, ErrAuthorityCorrupt
		}
	}
	unfinished, err := unfinishedTurnIDs(forkedEntries)
	if err != nil {
		return ThreadMeta{}, err
	}
	if len(unfinished) != 0 {
		r.seq++
		boundary, err := PrepareBranchBoundaryEntry(forkedEntries, newID, meta.LeafID, fmt.Sprintf("%s-entry-%d", newID, r.seq), "fork", now)
		if err != nil {
			return ThreadMeta{}, err
		}
		forkedEntries = append(forkedEntries, boundary)
		forkedEntries[len(forkedEntries)-1].PathDepth = int64(len(forkedEntries))
		meta.LeafID = boundary.ID
	}
	var forkedTodo AgentTodoState
	hasForkedTodo := false
	if todo, ok := r.todos[opts.SourceThreadID]; ok {
		todo.ThreadID = newID
		todo.UpdatedByTurnID = rewriteForkID(todo.UpdatedByTurnID, opts.TurnIDMap)
		todo.UpdatedByRunID = rewriteForkID(todo.UpdatedByRunID, opts.RunIDMap)
		forkedTodo = cloneAgentTodoState(todo)
		hasForkedTodo = true
	}
	if err := r.replaceIndexedEntriesLocked(newID, forkedEntries); err != nil {
		return ThreadMeta{}, err
	}
	r.threads[newID] = meta
	for key, record := range stagedArtifacts {
		r.artifacts[key] = record
	}
	if hasForkedTodo {
		r.todos[newID] = forkedTodo
	}
	_ = ctx
	return meta, nil
}

func (r *MemoryRepo) ForkWithInitialEntry(ctx context.Context, opts ForkOptions, initial Entry) (ThreadMeta, Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seqBefore := r.seq
	forked, err := r.forkLocked(ctx, opts)
	if err != nil {
		return ThreadMeta{}, Entry{}, err
	}
	initial.ThreadID = forked.ID
	saved, err := r.appendLocked(ctx, initial, AppendOptions{Now: opts.Now})
	if err != nil {
		delete(r.threads, forked.ID)
		r.deleteIndexedEntriesLocked(forked.ID)
		delete(r.todos, forked.ID)
		for key, record := range r.artifacts {
			if record.ThreadID == forked.ID {
				delete(r.artifacts, key)
			}
		}
		r.seq = seqBefore
		return ThreadMeta{}, Entry{}, err
	}
	return r.threads[forked.ID], saved, nil
}

func snapshotForkIdentityMaps(opts ForkOptions) ForkOptions {
	opts.TurnIDMap = cloneStringMap(opts.TurnIDMap)
	opts.RunIDMap = cloneStringMap(opts.RunIDMap)
	return opts
}

func applyForkDestinationMeta(meta *ThreadMeta, destination *ForkDestinationMeta) {
	if meta == nil || destination == nil {
		return
	}
	meta.ParentThreadID = strings.TrimSpace(destination.ParentThreadID)
	meta.ParentTurnID = strings.TrimSpace(destination.ParentTurnID)
	meta.TaskName = strings.TrimSpace(destination.TaskName)
	meta.TaskDescription = strings.TrimSpace(destination.TaskDescription)
	meta.AgentPath = strings.TrimSpace(destination.AgentPath)
	meta.HostProfileRef = strings.TrimSpace(destination.HostProfileRef)
	meta.ForkMode = strings.TrimSpace(destination.ForkMode)
	meta.Lifecycle = destination.Lifecycle
}

func forkDestinationMatches(meta ThreadMeta, opts ForkOptions, targetID string) bool {
	return strings.TrimSpace(opts.OriginRequestKey) != "" &&
		meta.OriginRequestKey == strings.TrimSpace(opts.OriginRequestKey) &&
		meta.OriginFingerprint == strings.TrimSpace(opts.OriginFingerprint) &&
		meta.ForkedFromThreadID == opts.SourceThreadID &&
		meta.ForkedFromEntryID == targetID &&
		MatchesForkDestinationMeta(meta, opts.DestinationMeta)
}

// MatchesForkDestinationMeta reports whether persisted ownership metadata
// exactly matches the fork plan. It is used for replay idempotency.
func MatchesForkDestinationMeta(meta ThreadMeta, destination *ForkDestinationMeta) bool {
	want := ThreadMeta{}
	applyForkDestinationMeta(&want, destination)
	return meta.ParentThreadID == want.ParentThreadID &&
		meta.ParentTurnID == want.ParentTurnID &&
		meta.TaskName == want.TaskName &&
		meta.TaskDescription == want.TaskDescription &&
		meta.AgentPath == want.AgentPath &&
		meta.HostProfileRef == want.HostProfileRef &&
		meta.ForkMode == want.ForkMode
}

type FileRepo struct {
	root string
	mu   sync.Mutex
	mem  *MemoryRepo
}

func NewFileRepo(root string) *FileRepo {
	return &FileRepo{root: root, mem: NewMemoryRepo()}
}

func (r *FileRepo) CreateThread(ctx context.Context, meta ThreadMeta) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return ThreadMeta{}, err
	}
	meta, err := r.mem.CreateThread(ctx, meta)
	if err != nil {
		return ThreadMeta{}, err
	}
	return meta, r.saveThread(meta)
}

func (r *FileRepo) ReadAgentTodoState(ctx context.Context, threadID string) (AgentTodoState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return AgentTodoState{}, err
	}
	return r.mem.ReadAgentTodoState(ctx, threadID)
}

func (r *FileRepo) CompareAndSwapAgentTodoState(ctx context.Context, state AgentTodoState, expectedVersion int64) (AgentTodoState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return AgentTodoState{}, err
	}
	updated, err := r.mem.CompareAndSwapAgentTodoState(ctx, state, expectedVersion)
	if err != nil {
		return AgentTodoState{}, err
	}
	if err := r.saveAgentTodoState(updated); err != nil {
		return AgentTodoState{}, err
	}
	return updated, nil
}

func (r *FileRepo) Thread(ctx context.Context, threadID string) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return ThreadMeta{}, err
	}
	return r.mem.Thread(ctx, threadID)
}

func (r *FileRepo) ListThreads(ctx context.Context, opts ListThreadsOptions) ([]ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return nil, err
	}
	return r.mem.ListThreads(ctx, opts)
}

func (r *FileRepo) UpdateThread(ctx context.Context, meta ThreadMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return err
	}
	if err := r.mem.UpdateThread(ctx, meta); err != nil {
		return err
	}
	return r.saveThread(meta)
}

func (r *FileRepo) Append(ctx context.Context, entry Entry, opts AppendOptions) (Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return Entry{}, err
	}
	entry, err := r.mem.Append(ctx, entry, opts)
	if err != nil {
		return Entry{}, err
	}
	if err := r.appendEntry(entry); err != nil {
		return Entry{}, err
	}
	meta, err := r.mem.Thread(ctx, entry.ThreadID)
	if err != nil {
		return Entry{}, err
	}
	if err := r.saveThread(meta); err != nil {
		return entry, AppendCommittedError{Err: err}
	}
	return entry, nil
}

func (r *FileRepo) Entry(ctx context.Context, threadID, entryID string) (Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return Entry{}, err
	}
	return r.mem.Entry(ctx, threadID, entryID)
}

func (r *FileRepo) Entries(ctx context.Context, threadID string) ([]Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return nil, err
	}
	return r.mem.Entries(ctx, threadID)
}

func (r *FileRepo) CanonicalTurnEntries(ctx context.Context, threadID, turnID, runID string) ([]Entry, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return nil, false, err
	}
	return r.mem.CanonicalTurnEntries(ctx, threadID, turnID, runID)
}

func (r *FileRepo) ListCanonicalTurns(ctx context.Context, opts ListCanonicalTurnsOptions) (CanonicalTurnsPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return CanonicalTurnsPage{}, err
	}
	return r.mem.ListCanonicalTurns(ctx, opts)
}

func (r *FileRepo) Path(ctx context.Context, threadID, leafID string) ([]Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return nil, err
	}
	return r.mem.Path(ctx, threadID, leafID)
}

func (r *FileRepo) PathPage(ctx context.Context, threadID, leafID, beforeEntryID string, limit int) (PathPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return PathPage{}, err
	}
	return r.mem.PathPage(ctx, threadID, leafID, beforeEntryID, limit)
}

func (r *FileRepo) MoveLeaf(ctx context.Context, threadID, entryID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return err
	}
	if err := r.mem.MoveLeaf(ctx, threadID, entryID); err != nil {
		return err
	}
	meta, err := r.mem.Thread(ctx, threadID)
	if err != nil {
		return err
	}
	return r.saveThread(meta)
}

func (r *FileRepo) Fork(ctx context.Context, opts ForkOptions) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return ThreadMeta{}, err
	}
	if opts.NewThreadID != "" {
		source, err := r.mem.Thread(ctx, opts.SourceThreadID)
		if err != nil {
			return ThreadMeta{}, err
		}
		targetID := opts.EntryID
		if targetID == "" && !opts.EntryIDPinned {
			targetID = source.LeafID
		}
		if opts.Position == ForkBefore {
			entry, err := r.mem.Entry(ctx, opts.SourceThreadID, targetID)
			if err != nil {
				return ThreadMeta{}, err
			}
			targetID = entry.ParentID
		}
		if existing, err := r.mem.Thread(ctx, opts.NewThreadID); err == nil {
			if forkDestinationMatches(existing, opts, targetID) {
				return existing, nil
			}
			return ThreadMeta{}, ErrThreadExists
		} else if !errors.Is(err, ErrThreadNotFound) {
			return ThreadMeta{}, err
		}
	}
	meta, err := r.mem.Fork(ctx, opts)
	if err != nil {
		return ThreadMeta{}, err
	}
	entries, err := r.mem.Entries(ctx, meta.ID)
	if err != nil {
		r.rollbackFork(meta.ID)
		return ThreadMeta{}, err
	}
	var todo *AgentTodoState
	if state, ok := r.mem.todos[meta.ID]; ok {
		cloned := cloneAgentTodoState(state)
		todo = &cloned
	}
	if err := persistFileRepoFork(r.root, meta, entries, todo); err != nil {
		r.rollbackFork(meta.ID)
		return ThreadMeta{}, err
	}
	return meta, nil
}

func (r *FileRepo) rollbackFork(threadID string) {
	delete(r.mem.threads, threadID)
	r.mem.deleteIndexedEntriesLocked(threadID)
	delete(r.mem.todos, threadID)
}

func persistFileRepoFork(root string, meta ThreadMeta, entries []Entry, todo *AgentTodoState) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	tempDir, err := os.MkdirTemp(filepath.Dir(root), "."+filepath.Base(root)+"-fork-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	threadJSON, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tempDir, "thread.json"), threadJSON, 0o600); err != nil {
		return err
	}
	var journal bytes.Buffer
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		journal.Write(data)
		journal.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(tempDir, "entries.jsonl"), journal.Bytes(), 0o600); err != nil {
		return err
	}
	if todo != nil {
		data, err := json.MarshalIndent(todo, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(tempDir, "agent_todos.json"), data, 0o600); err != nil {
			return err
		}
	}
	finalDir := filepath.Join(root, safePath(meta.ID))
	if err := os.Rename(tempDir, finalDir); err != nil {
		return err
	}
	return nil
}

func (r *FileRepo) load(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	threads, err := filepath.Glob(filepath.Join(r.root, "*", "thread.json"))
	if err != nil {
		return err
	}
	mem := NewMemoryRepo()
	type legacyPathDepthMigration struct {
		path    string
		entries []Entry
	}
	var legacyPathDepthMigrations []legacyPathDepthMigration
	for _, path := range threads {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read thread metadata %s: %w", path, err)
		}
		var meta ThreadMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return fmt.Errorf("decode thread metadata %s: %w", path, err)
		}
		if meta.ID == "" {
			return fmt.Errorf("%w: thread metadata %s has empty id", ErrInvalidThreadAuthority, path)
		}
		dir := filepath.Dir(path)
		journalPath := filepath.Join(dir, "entries.jsonl")
		entries, err := readEntries(journalPath)
		if err != nil {
			return fmt.Errorf("read thread entries %s: %w", path, err)
		}
		var migratePathDepth bool
		entries, migratePathDepth, err = stageLegacyFileEntryPathDepths(entries)
		if err != nil {
			return fmt.Errorf("migrate thread entry path depths %s: %w", path, err)
		}
		if migratePathDepth {
			legacyPathDepthMigrations = append(legacyPathDepthMigrations, legacyPathDepthMigration{path: journalPath, entries: entries})
		}
		if len(entries) == 0 && strings.TrimSpace(meta.LeafID) != "" {
			return fmt.Errorf("thread metadata %s references leaf %q without journal entries", path, meta.LeafID)
		}
		repairedMeta := reconcileFileThreadLeaf(meta, entries)
		legacyMemory := NewMemoryRepo()
		legacyMemory.threads[meta.ID] = repairedMeta
		legacyMemory.entries[meta.ID] = entries
		if _, err := repairLegacyFallbackThreadTitles(legacyMemory); err != nil {
			return fmt.Errorf("repair thread title %s: %w", path, err)
		}
		repairedMeta = legacyMemory.threads[meta.ID]
		if err := ValidateThreadMetaAuthority(repairedMeta); err != nil {
			return fmt.Errorf("validate thread metadata %s: %w", path, err)
		}
		if repairedMeta != meta {
			meta = repairedMeta
			if err := r.saveThread(meta); err != nil {
				return err
			}
		}
		mem.threads[meta.ID] = meta
		if err := mem.replaceIndexedEntriesLocked(meta.ID, entries); err != nil {
			return fmt.Errorf("index thread entries %s: %w", path, err)
		}
		todoData, err := os.ReadFile(filepath.Join(dir, "agent_todos.json"))
		if err == nil {
			var todo AgentTodoState
			if err := json.Unmarshal(todoData, &todo); err != nil {
				return fmt.Errorf("decode agent todo state for thread %q: %w", meta.ID, err)
			}
			if todo.ThreadID != meta.ID {
				return fmt.Errorf("agent todo state thread id %q does not match %q", todo.ThreadID, meta.ID)
			}
			if err := ValidateAgentTodoItems(todo.Items); err != nil {
				return fmt.Errorf("invalid agent todo state for thread %q: %w", meta.ID, err)
			}
			mem.todos[meta.ID] = cloneAgentTodoState(todo)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	metas := make([]ThreadMeta, 0, len(mem.threads))
	for _, meta := range mem.threads {
		metas = append(metas, meta)
	}
	if err := ValidateThreadAuthorityGraph(metas); err != nil {
		return err
	}
	for _, migration := range legacyPathDepthMigrations {
		if err := writeEntriesAtomically(migration.path, migration.entries); err != nil {
			return fmt.Errorf("write migrated thread entry path depths %s: %w", migration.path, err)
		}
	}
	r.mem = mem
	return nil
}

func reconcileFileThreadLeaf(meta ThreadMeta, entries []Entry) ThreadMeta {
	// IMPORTANT: entries.jsonl is the durable append journal. If a process writes
	// an entry but exits before refreshing thread.json, reads must repair the
	// effective leaf instead of treating the stale snapshot as authoritative.
	if len(entries) == 0 {
		return meta
	}
	leafIndex := -1
	if meta.LeafID != "" {
		for i, entry := range entries {
			if entry.ID == meta.LeafID {
				leafIndex = i
				break
			}
		}
	}
	if leafIndex == len(entries)-1 {
		return meta
	}
	if leafIndex < 0 && meta.LeafID != "" {
		if newest, ok := newestRootReachableEntry(entries); ok {
			meta.LeafID = newest.ID
			meta.UpdatedAt = newest.CreatedAt
			return meta
		}
		return meta
	}
	reachable := reachableEntryIDs(entries, meta.LeafID)
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if meta.LeafID == "" || reachable[entry.ParentID] {
			meta.LeafID = entry.ID
			meta.UpdatedAt = entry.CreatedAt
			return meta
		}
	}
	return meta
}

func newestRootReachableEntry(entries []Entry) (Entry, bool) {
	reachable := map[string]bool{"": true}
	var newest Entry
	found := false
	for _, entry := range entries {
		if !reachable[entry.ParentID] {
			continue
		}
		reachable[entry.ID] = true
		newest = entry
		found = true
	}
	return newest, found
}

func reachableEntryIDs(entries []Entry, leafID string) map[string]bool {
	reachable := map[string]bool{"": leafID == ""}
	for _, entry := range entries {
		if leafID == "" || entry.ID == leafID || reachable[entry.ParentID] {
			reachable[entry.ID] = true
		}
	}
	return reachable
}

func (r *FileRepo) saveThread(meta ThreadMeta) error {
	dir := filepath.Join(r.root, safePath(meta.ID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "thread.json"), data, 0o600)
}

func (r *FileRepo) saveAgentTodoState(state AgentTodoState) error {
	dir := filepath.Join(r.root, safePath(state.ThreadID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "agent_todos.json"), data, 0o600)
}

func (r *FileRepo) appendEntry(entry Entry) error {
	dir := filepath.Join(r.root, safePath(entry.ThreadID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "entries.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func BuildContext(path []Entry, opts ContextOptions) []session.Message {
	messages, _ := BuildContextChecked(path, opts)
	return messages
}

func BuildContextChecked(path []Entry, opts ContextOptions) ([]session.Message, error) {
	projection, err := BuildContextProjectionChecked(path, ContextProjectionOptions{})
	return projection.Messages, err
}

func BuildContextProjection(path []Entry, opts ContextProjectionOptions) ContextProjection {
	projection, _ := BuildContextProjectionChecked(path, opts)
	return projection
}

func BuildContextProjectionChecked(path []Entry, opts ContextProjectionOptions) (ContextProjection, error) {
	if opts.Purpose == "" {
		opts.Purpose = ProjectionProviderRequest
	}
	contextPath, err := retryProjectedContextPath(path)
	if err != nil {
		return ContextProjection{}, err
	}
	messages := buildContextMessages(contextPath)
	return ContextProjection{Messages: messages, Segments: projectedSegments(contextPath, messages, opts.Purpose)}, nil
}

func retryProjectedContextPath(path []Entry) ([]Entry, error) {
	if len(path) == 0 {
		return nil, nil
	}
	projected := make([]Entry, 0, len(path))
	type contextPosition struct {
		pathIndex       int
		projectedLength int
	}
	positions := make(map[string]contextPosition, len(path))
	for index, entry := range path {
		if strings.TrimSpace(entry.ID) == "" {
			return nil, ErrAuthorityCorrupt
		}
		if _, duplicate := positions[entry.ID]; duplicate {
			return nil, ErrAuthorityCorrupt
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return nil, err
			}
			if retrySource != nil {
				source, ok := positions[retrySource.EntryID]
				if !ok || source.pathIndex >= index || validateRetrySourcePathIndex(path, source.pathIndex, retrySource.TurnID, retrySource.EntryID) != nil {
					return nil, ErrAuthorityCorrupt
				}
				if source.projectedLength <= 0 || source.projectedLength > len(projected) || projected[source.projectedLength-1].ID != retrySource.EntryID {
					return nil, ErrAuthorityCorrupt
				}
				projected = projected[:source.projectedLength]
			}
		}
		projected = append(projected, entry)
		positions[entry.ID] = contextPosition{pathIndex: index, projectedLength: len(projected)}
	}
	return projected, nil
}

func buildContextMessages(path []Entry) []session.Message {
	compactionIndex := -1
	firstKeptIndex := -1
	for i, entry := range path {
		if entry.Type == EntryCompaction {
			compactionIndex = i
			firstKeptIndex = -1
			if entry.FirstKeptEntryID != "" {
				firstKeptIndex = slices.IndexFunc(path, func(candidate Entry) bool { return candidate.ID == entry.FirstKeptEntryID })
			}
			if firstKeptIndex < 0 && len(entry.KeptUserEntryIDs) == 0 {
				firstKeptIndex = repairFirstKeptIndex(path, i)
			}
		}
	}
	var messages []session.Message
	if compactionIndex >= 0 {
		compactionEntry := path[compactionIndex]
		tailEntryIDs := map[string]struct{}{}
		if firstKeptIndex >= 0 && firstKeptIndex < compactionIndex {
			for _, entry := range path[firstKeptIndex:compactionIndex] {
				tailEntryIDs[entry.ID] = struct{}{}
			}
		}
		for _, entry := range path[compactionIndex+1:] {
			tailEntryIDs[entry.ID] = struct{}{}
		}
		var tail []session.Message
		if firstKeptIndex >= 0 && firstKeptIndex < compactionIndex {
			for _, entry := range path[firstKeptIndex:compactionIndex] {
				tail = appendProviderVisible(tail, entry)
			}
		}
		for _, entry := range path[compactionIndex+1:] {
			tail = appendProviderVisible(tail, entry)
		}
		if compactionEntry.Summary != "" {
			keptUsers := messagesForEntries(keptUserEntries(path[:compactionIndex], compactionEntry.KeptUserEntryIDs, tailEntryIDs))
			msg := compaction.BuildCheckpointMessage(compactionEntry.Summary, keptUsers, tail)
			msg.EntryID = compactionEntry.ID
			msg.ParentEntryID = compactionEntry.ParentID
			msg.CompactionID = compactionEntry.CompactionID
			msg.CompactionGeneration = compactionEntry.CompactionGeneration
			msg.CompactionWindowID = compactionEntry.CompactionWindowID
			messages = append(messages, msg)
		}
		messages = append(messages, tail...)
		return messages
	}
	for _, entry := range path {
		messages = appendProviderVisible(messages, entry)
	}
	return messages
}

func projectedSegments(path []Entry, messages []session.Message, purpose ProjectionPurpose) []ProjectedSegment {
	if len(messages) == 0 {
		return nil
	}
	entriesByID := make(map[string]Entry, len(path))
	for _, entry := range path {
		entriesByID[entry.ID] = entry
	}
	segments := make([]ProjectedSegment, 0, len(messages))
	for i, msg := range messages {
		seg := ProjectedSegment{
			EntryID:       msg.EntryID,
			MessageIndex:  i,
			Role:          msg.Role,
			ToolCallID:    msg.ToolCallID,
			ToolName:      msg.ToolName,
			TokenEstimate: contextpolicy.EstimateMessageTokens(msg),
		}
		if purpose == ProjectionTestUI {
			seg.ArtifactRefs = messageArtifactRefs(msg)
			seg.UIPreview = previewMessageContent(msg.Content, 240)
		}
		if entry, ok := entriesByID[msg.EntryID]; ok {
			seg.EntryType = entry.Type
		} else if msg.Kind == session.MessageKindCompactionSummary {
			seg.EntryType = EntryCompaction
		}
		segments = append(segments, seg)
	}
	return segments
}

func messageArtifactRefs(msg session.Message) []artifact.Ref {
	if msg.ToolResult == nil || msg.ToolResult.FullOutput == nil {
		return nil
	}
	return []artifact.Ref{*msg.ToolResult.FullOutput}
}

func previewMessageContent(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max] + "..."
}

func messagesForEntries(entries []Entry) []session.Message {
	out := make([]session.Message, 0, len(entries))
	for _, entry := range entries {
		if entry.Message.Role == "" {
			continue
		}
		msg := session.CloneMessage(entry.Message)
		msg.EntryID = entry.ID
		msg.ParentEntryID = entry.ParentID
		out = append(out, msg)
	}
	return out
}

func appendProviderVisible(messages []session.Message, entry Entry) []session.Message {
	switch entry.Type {
	case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult:
		if entry.Message.Role != "" {
			msg := session.CloneMessage(entry.Message)
			if entry.Type == EntryToolCall {
				if projected, ok := control.ProjectMessage(msg); ok {
					msg = projected
				}
			}
			msg.EntryID = entry.ID
			msg.ParentEntryID = entry.ParentID
			msg.Activity = nil
			messages = append(messages, msg)
		}
	case EntryBranchSummary:
		if entry.Summary != "" {
			messages = append(messages, session.Message{Role: session.Assistant, Content: entry.Summary, EntryID: entry.ID, ParentEntryID: entry.ParentID})
		}
	}
	return messages
}

func AppendMessage(ctx context.Context, repo JournalRepo, threadID, turnID string, msg session.Message) (Entry, error) {
	return AppendMessageAt(ctx, repo, threadID, turnID, msg, time.Time{})
}

func AppendMessageAt(ctx context.Context, repo JournalRepo, threadID, turnID string, msg session.Message, observedAt time.Time) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: typeForMessage(msg), Message: msg}, AppendOptions{Now: observedAt})
}

func AppendCompaction(ctx context.Context, repo JournalRepo, threadID, turnID string, result compaction.Result) (Entry, error) {
	entry, err := CompactionEntry(threadID, turnID, result)
	if err != nil {
		return Entry{}, err
	}
	return repo.Append(ctx, entry, AppendOptions{})
}

func CompactionEntry(threadID, turnID string, result compaction.Result) (Entry, error) {
	if strings.TrimSpace(result.CompactionID) == "" || result.CompactionGeneration <= 0 || strings.TrimSpace(result.CompactionWindowID) == "" {
		return Entry{}, errors.New("compaction result requires id, generation, and window id")
	}
	if strings.TrimSpace(result.OperationID) == "" || strings.TrimSpace(result.RequestID) == "" || strings.TrimSpace(result.Source) == "" {
		return Entry{}, errors.New("compaction result requires operation, request, and source identities")
	}
	for _, key := range []string{"operation_id", "request_id", "source"} {
		if _, exists := result.Details[key]; exists {
			return Entry{}, fmt.Errorf("compaction details must not contain identity alias %q", key)
		}
	}
	return Entry{
		ThreadID:                threadID,
		TurnID:                  turnID,
		Type:                    EntryCompaction,
		CompactionID:            result.CompactionID,
		PreviousCompactionID:    result.PreviousCompactionID,
		CompactedThroughEntryID: result.CompactedThroughEntryID,
		SummarySchemaVersion:    result.SummarySchemaVersion,
		CompactionGeneration:    result.CompactionGeneration,
		CompactionWindowID:      result.CompactionWindowID,
		FirstKeptEntryID:        result.FirstKeptEntryID,
		KeptUserEntryIDs:        append([]string(nil), result.KeptUserEntryIDs...),
		Summary:                 result.Summary,
		CompactionTrigger:       string(result.Trigger),
		CompactionReason:        string(result.Reason),
		CompactionPhase:         string(result.Phase),
		CompactionOperationID:   strings.TrimSpace(result.OperationID),
		CompactionRequestID:     strings.TrimSpace(result.RequestID),
		CompactionSource:        strings.TrimSpace(result.Source),
		TokensBefore:            result.TokensBefore,
		TokensAfterEstimate:     result.TokensAfterEstimate,
		ContextUsageBefore:      result.UsageBefore,
		ContextUsageAfter:       result.UsageAfter,
		Metadata:                mapsClone(result.Details),
	}, nil
}

func AppendTurnMarker(ctx context.Context, repo JournalRepo, threadID, turnID string, status TurnMarkerStatus, metadata map[string]string) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: EntryTurnMarker, TurnStatus: status, Metadata: metadata}, AppendOptions{})
}

func AppendTurnMarkerWithID(ctx context.Context, repo JournalRepo, threadID, turnID, entryID string, status TurnMarkerStatus, metadata map[string]string) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: EntryTurnMarker, TurnStatus: status, Metadata: metadata}, AppendOptions{ID: entryID})
}

func AppendActiveTools(ctx context.Context, repo JournalRepo, threadID string, metadata map[string]string) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, Type: EntryActiveTools, Metadata: metadata}, AppendOptions{})
}

func AppendFailure(ctx context.Context, repo JournalRepo, threadID, turnID string, message string) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: EntryRunFailure, Error: message}, AppendOptions{})
}

func repairFirstKeptIndex(path []Entry, compactionIndex int) int {
	if compactionIndex <= 0 {
		return -1
	}
	for i := compactionIndex - 1; i >= 0; i-- {
		switch path[i].Type {
		case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult:
			start := i
			for start > 0 && path[start].Type == EntryToolResult {
				start--
			}
			return start
		}
	}
	return -1
}

func keptUserEntries(prefix []Entry, ids []string, skip map[string]struct{}) []Entry {
	if len(ids) == 0 {
		return nil
	}
	byID := make(map[string]Entry, len(prefix))
	for _, entry := range prefix {
		if entry.Type == EntryUserMessage && entry.Message.Role == session.User {
			byID[entry.ID] = entry
		}
	}
	out := make([]Entry, 0, len(ids))
	seen := map[string]struct{}{}
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := skip[id]; ok {
			continue
		}
		entry, ok := byID[id]
		if !ok {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func rewriteEntryIDs(ids []string, oldToNew map[string]string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if next := oldToNew[id]; next != "" {
			out = append(out, next)
		}
	}
	return out
}

func rewriteForkID(value string, ids map[string]string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(ids) == 0 {
		return value
	}
	if next := strings.TrimSpace(ids[value]); next != "" {
		return next
	}
	return value
}

func rewriteForkMetadata(metadata map[string]string, entryIDs map[string]string, turnIDs map[string]string, runIDs map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		next := value
		switch strings.TrimSpace(key) {
		case "run_id", "trace_id":
			next = rewriteForkID(value, runIDs)
		case "turn_id":
			next = rewriteForkID(value, turnIDs)
		case RetrySourceTurnIDMetadataKey:
			next = rewriteForkID(value, turnIDs)
		case "entry_id", "parent_entry_id", "input_entry_id", "subagent_input_id":
			next = rewriteForkID(value, entryIDs)
		case RetrySourceEntryIDMetadataKey:
			next = rewriteForkID(value, entryIDs)
		}
		out[key] = next
	}
	return out
}

func typeForMessage(msg session.Message) EntryType {
	switch msg.Role {
	case session.User:
		return EntryUserMessage
	case session.Tool:
		return EntryToolResult
	case session.Assistant:
		if msg.ToolCallID != "" || msg.ToolName != "" || msg.ToolArgs != "" {
			return EntryToolCall
		}
		return EntryAssistantMessage
	default:
		return EntryCustom
	}
}

func pathLocked(threads map[string]ThreadMeta, entries map[string][]Entry, threadID, leafID string) ([]Entry, error) {
	meta, ok := threads[threadID]
	if !ok {
		return nil, ErrThreadNotFound
	}
	if leafID == "" {
		leafID = meta.LeafID
	}
	if leafID == "" {
		return nil, nil
	}
	byID := map[string]Entry{}
	for _, entry := range entries[threadID] {
		byID[entry.ID] = entry
	}
	var rev []Entry
	seen := map[string]struct{}{}
	for id := leafID; id != ""; {
		if _, duplicate := seen[id]; duplicate {
			return nil, ErrInvalidParent
		}
		seen[id] = struct{}{}
		entry, ok := byID[id]
		if !ok {
			if len(rev) != 0 {
				return nil, ErrInvalidParent
			}
			return nil, ErrEntryNotFound
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return nil, err
		}
		rev = append(rev, cloneEntry(entry))
		id = entry.ParentID
	}
	slices.Reverse(rev)
	return rev, nil
}

func readEntries(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var entries []Entry
	dec := json.NewDecoder(f)
	for {
		var entry Entry
		if err := dec.Decode(&entry); errors.Is(err, io.EOF) {
			return entries, nil
		} else if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
}

func stageLegacyFileEntryPathDepths(entries []Entry) ([]Entry, bool, error) {
	if len(entries) == 0 {
		return entries, false, nil
	}
	zeroDepths := 0
	for _, entry := range entries {
		if entry.PathDepth == 0 {
			zeroDepths++
		}
	}
	if zeroDepths == 0 {
		return entries, false, nil
	}
	if zeroDepths != len(entries) {
		return nil, false, fmt.Errorf("%w: file journal mixes legacy and canonical path depths", ErrAuthorityCorrupt)
	}
	migrated := cloneEntries(entries)
	depths := make(map[string]int64, len(migrated))
	threadID := strings.TrimSpace(migrated[0].ThreadID)
	if threadID == "" {
		return entries, false, nil
	}
	rootCount := 0
	for index := range migrated {
		entry := &migrated[index]
		if strings.TrimSpace(entry.ID) == "" || entry.ThreadID != threadID {
			return entries, false, nil
		}
		if _, duplicate := depths[entry.ID]; duplicate {
			return entries, false, nil
		}
		if err := ValidateEntryIntegrity(*entry); err != nil {
			return entries, false, nil
		}
		depth := int64(1)
		if entry.ParentID == "" {
			rootCount++
			if rootCount != 1 {
				return entries, false, nil
			}
		} else {
			parentDepth, ok := depths[entry.ParentID]
			if !ok || parentDepth <= 0 {
				return entries, false, nil
			}
			depth = parentDepth + 1
		}
		entry.PathDepth = depth
		depths[entry.ID] = depth
	}
	if rootCount != 1 {
		return entries, false, nil
	}
	return migrated, true, nil
}

func writeEntriesAtomically(path string, entries []Entry) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".entries-path-depth-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			_ = temp.Close()
			return err
		}
		if _, err := temp.Write(append(data, '\n')); err != nil {
			_ = temp.Close()
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func PrepareEntry(entry Entry) Entry {
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	return entry
}

func ValidateEntryMessageReferences(entry Entry) error {
	if len(entry.Message.References) == 0 {
		return nil
	}
	if entry.Type != EntryUserMessage || entry.Message.Role != session.User {
		return errors.New("message references are only valid on canonical user message entries")
	}
	return session.ValidateMessageReferences(entry.Message.References)
}

func ValidateEntryMessageAttachments(entry Entry) error {
	if err := validateEntryMessageAttachmentPlacement(entry); err != nil {
		return err
	}
	return session.ValidateStoredMessageAttachments(entry.Message.Attachments)
}

func ValidateNewEntryMessageAttachments(entry Entry) error {
	if err := validateEntryMessageAttachmentPlacement(entry); err != nil {
		return err
	}
	return session.ValidateMessageAttachments(entry.Message.Attachments)
}

func validateEntryMessageAttachmentPlacement(entry Entry) error {
	if len(entry.Message.Attachments) == 0 {
		return nil
	}
	if entry.Type != EntryUserMessage || entry.Message.Role != session.User {
		return errors.New("message attachments are only valid on canonical user message entries")
	}
	return nil
}

func ValidateEntryIntegrity(entry Entry) error {
	if err := ValidateEntryMessageAttachments(entry); err != nil {
		return ErrAuthorityCorrupt
	}
	if err := ValidateEntryMessageReferences(entry); err != nil {
		return ErrAuthorityCorrupt
	}
	if strings.TrimSpace(entry.Raw) == "" || strings.TrimSpace(entry.RawHash) == "" || stableHash(entry.Raw) != entry.RawHash {
		return ErrAuthorityCorrupt
	}
	if rawForEntry(entry) != entry.Raw {
		return ErrAuthorityCorrupt
	}
	return nil
}

func RawForEntry(entry Entry) string {
	return rawForEntry(entry)
}

func StableHash(value string) string {
	return stableHash(value)
}

func rawForEntry(entry Entry) string {
	type rawEntry struct {
		Type                    EntryType         `json:"type"`
		TurnStatus              TurnMarkerStatus  `json:"turn_status,omitempty"`
		Message                 session.Message   `json:"message,omitempty"`
		Provider                string            `json:"provider,omitempty"`
		Model                   string            `json:"model,omitempty"`
		CompactionID            string            `json:"compaction_id,omitempty"`
		PreviousCompactionID    string            `json:"previous_compaction_id,omitempty"`
		CompactedThroughEntryID string            `json:"compacted_through_entry_id,omitempty"`
		SummarySchemaVersion    string            `json:"summary_schema_version,omitempty"`
		CompactionGeneration    int               `json:"compaction_generation,omitempty"`
		CompactionWindowID      string            `json:"compaction_window_id,omitempty"`
		FirstKeptEntryID        string            `json:"first_kept_entry_id,omitempty"`
		KeptUserEntryIDs        []string          `json:"kept_user_entry_ids,omitempty"`
		Summary                 string            `json:"summary,omitempty"`
		CompactionTrigger       string            `json:"compaction_trigger,omitempty"`
		CompactionReason        string            `json:"compaction_reason,omitempty"`
		CompactionPhase         string            `json:"compaction_phase,omitempty"`
		TokensBefore            int64             `json:"tokens_before,omitempty"`
		TokensAfterEstimate     int64             `json:"tokens_after_estimate,omitempty"`
		Error                   string            `json:"error,omitempty"`
		Metadata                map[string]string `json:"metadata,omitempty"`
		Payload                 json.RawMessage   `json:"payload,omitempty"`
	}
	data, _ := json.Marshal(rawEntry{
		Type:                    entry.Type,
		TurnStatus:              entry.TurnStatus,
		Message:                 entry.Message,
		Provider:                entry.Provider,
		Model:                   entry.Model,
		CompactionID:            entry.CompactionID,
		PreviousCompactionID:    entry.PreviousCompactionID,
		CompactedThroughEntryID: entry.CompactedThroughEntryID,
		SummarySchemaVersion:    entry.SummarySchemaVersion,
		CompactionGeneration:    entry.CompactionGeneration,
		CompactionWindowID:      entry.CompactionWindowID,
		FirstKeptEntryID:        entry.FirstKeptEntryID,
		KeptUserEntryIDs:        entry.KeptUserEntryIDs,
		Summary:                 entry.Summary,
		CompactionTrigger:       entry.CompactionTrigger,
		CompactionReason:        entry.CompactionReason,
		CompactionPhase:         entry.CompactionPhase,
		TokensBefore:            entry.TokensBefore,
		TokensAfterEstimate:     entry.TokensAfterEstimate,
		Error:                   entry.Error,
		Metadata:                entry.Metadata,
		Payload:                 entry.Payload,
	})
	return string(data)
}

func stableHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func cloneEntries(entries []Entry) []Entry {
	out := make([]Entry, len(entries))
	for i, entry := range entries {
		out[i] = cloneEntry(entry)
	}
	return out
}

func cloneEntry(entry Entry) Entry {
	entry.Message = session.CloneMessage(entry.Message)
	entry.Payload = bytes.Clone(entry.Payload)
	if entry.Metadata != nil {
		entry.Metadata = mapsClone(entry.Metadata)
	}
	if entry.KeptUserEntryIDs != nil {
		entry.KeptUserEntryIDs = append([]string(nil), entry.KeptUserEntryIDs...)
	}
	return entry
}

func cloneAgentTodoState(state AgentTodoState) AgentTodoState {
	state.Items = append([]AgentTodoItem(nil), state.Items...)
	return state
}

func mapsClone(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func findEntry(entries []Entry, id string) (Entry, bool) {
	for _, entry := range entries {
		if entry.ID == id {
			return entry, true
		}
	}
	return Entry{}, false
}

func containsEntry(entries []Entry, id string) bool {
	_, ok := findEntry(entries, id)
	return ok
}

func nonZeroTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now()
	}
	return t
}

func safePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "_"
	}
	return "id_" + base64.RawURLEncoding.EncodeToString([]byte(value))
}
