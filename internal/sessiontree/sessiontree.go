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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/internal/control"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
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
	EntryCustom           EntryType = "custom"
)

type TurnMarkerStatus string

const (
	TurnStarted   TurnMarkerStatus = "started"
	TurnSavePoint TurnMarkerStatus = "save_point"
	TurnCompleted TurnMarkerStatus = "completed"
	TurnWaiting   TurnMarkerStatus = "waiting"
	TurnFailed    TurnMarkerStatus = "failed"
	TurnAborted   TurnMarkerStatus = "aborted"
)

type ThreadTitleStatus string

const (
	ThreadTitleReady  ThreadTitleStatus = "ready"
	ThreadTitleFailed ThreadTitleStatus = "failed"
)

type ThreadTitleSource string

const (
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
	ErrForkDestinationConflict  = errors.New("session tree fork destination conflicts with operation marker")
	ErrAgentTodoVersionConflict = errors.New("session tree agent todo version conflict")
)

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
	ID                  string            `json:"id"`
	LeafID              string            `json:"leaf_id,omitempty"`
	ParentThreadID      string            `json:"parent_thread_id,omitempty"`
	ParentTurnID        string            `json:"parent_turn_id,omitempty"`
	ForkedFromThreadID  string            `json:"forked_from_thread_id,omitempty"`
	ForkedFromEntryID   string            `json:"forked_from_entry_id,omitempty"`
	ForkOperationID     string            `json:"fork_operation_id,omitempty"`
	ForkOperationNodeID string            `json:"fork_operation_node_id,omitempty"`
	TaskName            string            `json:"task_name,omitempty"`
	TaskDescription     string            `json:"task_description,omitempty"`
	AgentPath           string            `json:"agent_path,omitempty"`
	HostProfileRef      string            `json:"host_profile_ref,omitempty"`
	ForkMode            string            `json:"fork_mode,omitempty"`
	Closed              bool              `json:"closed,omitempty"`
	Archived            bool              `json:"archived,omitempty"`
	Title               string            `json:"title,omitempty"`
	TitleStatus         ThreadTitleStatus `json:"title_status,omitempty"`
	TitleSource         ThreadTitleSource `json:"title_source,omitempty"`
	TitleUpdatedAt      time.Time         `json:"title_updated_at,omitempty"`
	TitleError          string            `json:"title_error,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	Status              string            `json:"status,omitempty"`
	LastViewedAt        time.Time         `json:"last_viewed_at,omitempty"`
}

// ValidateThreadMetaAuthority enforces the durable distinction between
// independent root threads and parent-owned SubAgent threads.
func ValidateThreadMetaAuthority(meta ThreadMeta) error {
	id := strings.TrimSpace(meta.ID)
	parentID := strings.TrimSpace(meta.ParentThreadID)
	if id == "" {
		return fmt.Errorf("%w: thread id is required", ErrInvalidThreadAuthority)
	}
	if parentID == "" {
		if strings.TrimSpace(meta.ParentTurnID) != "" ||
			strings.TrimSpace(meta.TaskName) != "" ||
			strings.TrimSpace(meta.TaskDescription) != "" ||
			strings.TrimSpace(meta.AgentPath) != "" ||
			strings.TrimSpace(meta.HostProfileRef) != "" ||
			strings.TrimSpace(meta.ForkMode) != "" || meta.Closed || strings.TrimSpace(meta.Status) != "" {
			return fmt.Errorf("%w: root thread %q contains subagent ownership metadata", ErrInvalidThreadAuthority, id)
		}
	} else {
		if parentID == id {
			return fmt.Errorf("%w: thread %q cannot own itself", ErrInvalidThreadAuthority, id)
		}
		if strings.TrimSpace(meta.TaskName) == "" || strings.TrimSpace(meta.AgentPath) == "" {
			return fmt.Errorf("%w: subagent thread %q requires task name and agent path", ErrInvalidThreadAuthority, id)
		}
	}
	if (strings.TrimSpace(meta.ForkOperationID) == "") != (strings.TrimSpace(meta.ForkOperationNodeID) == "") {
		return fmt.Errorf("%w: thread %q has incomplete fork operation identity", ErrInvalidThreadAuthority, id)
	}
	return nil
}

// SameThreadAuthority reports whether an update preserves immutable ownership
// and lineage identity.
func SameThreadAuthority(left, right ThreadMeta) bool {
	return left.ParentThreadID == right.ParentThreadID &&
		left.ParentTurnID == right.ParentTurnID &&
		left.ForkedFromThreadID == right.ForkedFromThreadID &&
		left.ForkedFromEntryID == right.ForkedFromEntryID &&
		left.ForkOperationID == right.ForkOperationID &&
		left.ForkOperationNodeID == right.ForkOperationNodeID &&
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
	Type                    EntryType           `json:"type"`
	TurnID                  string              `json:"turn_id,omitempty"`
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
	SourceThreadID  string
	EntryID         string
	EntryIDPinned   bool
	Position        ForkPosition
	NewThreadID     string
	OperationID     string
	OperationNodeID string
	Now             time.Time
	TurnIDMap       map[string]string
	RunIDMap        map[string]string
	DestinationMeta *ForkDestinationMeta
	RewriteEntry    func(Entry, ForkEntryIdentity) (Entry, error)
}

// ForkDestinationMeta is the child ownership metadata written atomically with
// a fork destination. A nil value creates an independent root fork.
type ForkDestinationMeta struct {
	ParentThreadID  string `json:"parent_thread_id"`
	ParentTurnID    string `json:"parent_turn_id,omitempty"`
	TaskName        string `json:"task_name,omitempty"`
	TaskDescription string `json:"task_description,omitempty"`
	AgentPath       string `json:"agent_path,omitempty"`
	HostProfileRef  string `json:"host_profile_ref,omitempty"`
	ForkMode        string `json:"fork_mode,omitempty"`
	Closed          bool   `json:"closed,omitempty"`
	Status          string `json:"status,omitempty"`
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

type Repo interface {
	CreateThread(context.Context, ThreadMeta) (ThreadMeta, error)
	Thread(context.Context, string) (ThreadMeta, error)
	UpdateThread(context.Context, ThreadMeta) error
	DeleteThread(context.Context, string) error
	Append(context.Context, Entry, AppendOptions) (Entry, error)
	Entry(context.Context, string, string) (Entry, error)
	Entries(context.Context, string) ([]Entry, error)
	Path(context.Context, string, string) ([]Entry, error)
	PathPage(context.Context, string, string, string, int) (PathPage, error)
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
	Limit           int
	AfterCreatedAt  time.Time
	AfterID         string
}

func ListThreads(ctx context.Context, repo Repo, opts ListThreadsOptions) ([]ThreadMeta, error) {
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

type TurnLease struct {
	ThreadID  string    `json:"thread_id"`
	TurnID    string    `json:"turn_id"`
	OwnerID   string    `json:"owner_id"`
	CreatedAt time.Time `json:"created_at"`
}

type TurnLeaseRepo interface {
	AcquireTurnLease(context.Context, TurnLease) error
	ReleaseTurnLease(context.Context, TurnLease) error
	ActiveTurnLease(context.Context, string) (TurnLease, bool, error)
	ClearExpiredTurnLease(context.Context, string, time.Time) (TurnLease, bool, error)
}

type AgentTodoStatus string

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
	mu      sync.Mutex
	threads map[string]ThreadMeta
	entries map[string][]Entry
	leases  map[string]TurnLease
	todos   map[string]AgentTodoState
	seq     int64
}

func NewMemoryRepo() *MemoryRepo {
	return &MemoryRepo{threads: map[string]ThreadMeta{}, entries: map[string][]Entry{}, leases: map[string]TurnLease{}, todos: map[string]AgentTodoState{}}
}

func (r *MemoryRepo) CreateThread(_ context.Context, meta ThreadMeta) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
	}
	now := meta.CreatedAt
	if now.IsZero() {
		now = time.Now()
	}
	meta.CreatedAt = now
	meta.UpdatedAt = now
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return ThreadMeta{}, err
	}
	if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
		if _, ok := r.threads[parentID]; !ok {
			return ThreadMeta{}, fmt.Errorf("%w: parent thread %q", ErrInvalidThreadAuthority, parentID)
		}
	}
	r.threads[meta.ID] = meta
	return meta, nil
}

func (r *MemoryRepo) AcquireTurnLease(_ context.Context, lease TurnLease) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[lease.ThreadID]; !ok {
		return ErrThreadNotFound
	}
	if active, ok := r.leases[lease.ThreadID]; ok && active.TurnID != "" {
		return ErrActiveTurn
	}
	if lease.CreatedAt.IsZero() {
		lease.CreatedAt = time.Now()
	}
	r.leases[lease.ThreadID] = lease
	return nil
}

func (r *MemoryRepo) ReleaseTurnLease(_ context.Context, lease TurnLease) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	active, ok := r.leases[lease.ThreadID]
	if !ok {
		return nil
	}
	if active.OwnerID != lease.OwnerID || active.TurnID != lease.TurnID {
		return nil
	}
	delete(r.leases, lease.ThreadID)
	return nil
}

func (r *MemoryRepo) ActiveTurnLease(_ context.Context, threadID string) (TurnLease, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[threadID]; !ok {
		return TurnLease{}, false, ErrThreadNotFound
	}
	lease, ok := r.leases[threadID]
	return lease, ok, nil
}

func (r *MemoryRepo) ClearExpiredTurnLease(_ context.Context, threadID string, cutoff time.Time) (TurnLease, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[threadID]; !ok {
		return TurnLease{}, false, ErrThreadNotFound
	}
	lease, ok := r.leases[threadID]
	if !ok || cutoff.IsZero() || lease.CreatedAt.IsZero() || !lease.CreatedAt.Before(cutoff) {
		return TurnLease{}, false, nil
	}
	delete(r.leases, threadID)
	return lease, true, nil
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

func (r *MemoryRepo) CompareAndSwapAgentTodoState(_ context.Context, state AgentTodoState, expectedVersion int64) (AgentTodoState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[state.ThreadID]; !ok {
		return AgentTodoState{}, ErrThreadNotFound
	}
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

func (r *MemoryRepo) UpdateThread(_ context.Context, meta ThreadMeta) error {
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
	meta.UpdatedAt = nonZeroTime(meta.UpdatedAt)
	r.threads[meta.ID] = meta
	return nil
}

func (r *MemoryRepo) DeleteThread(_ context.Context, threadID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[threadID]; !ok {
		return ErrThreadNotFound
	}
	delete(r.threads, threadID)
	delete(r.entries, threadID)
	delete(r.leases, threadID)
	delete(r.todos, threadID)
	return nil
}

func (r *MemoryRepo) Append(_ context.Context, entry Entry, opts AppendOptions) (Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[entry.ThreadID]
	if !ok {
		return Entry{}, ErrThreadNotFound
	}
	if opts.ParentID != "" {
		entry.ParentID = opts.ParentID
	} else if entry.ParentID == "" {
		entry.ParentID = meta.LeafID
	}
	if entry.ParentID != "" && !containsEntry(r.entries[entry.ThreadID], entry.ParentID) {
		return Entry{}, ErrInvalidParent
	}
	if opts.ID != "" {
		entry.ID = opts.ID
	}
	if entry.ID == "" {
		entry.ID = r.nextEntryID(entry.ThreadID)
	} else if containsEntry(r.entries[entry.ThreadID], entry.ID) {
		return Entry{}, fmt.Errorf("session tree entry id already exists: %s", entry.ID)
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = opts.Now
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	r.entries[entry.ThreadID] = append(r.entries[entry.ThreadID], cloneEntry(entry))
	meta.LeafID = entry.ID
	meta.UpdatedAt = entry.CreatedAt
	r.threads[entry.ThreadID] = meta
	return cloneEntry(entry), nil
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
	entry, ok := findEntry(r.entries[threadID], entryID)
	if !ok {
		return Entry{}, ErrEntryNotFound
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
	if opts.Position == "" {
		opts.Position = ForkAt
	}
	sourceMeta, ok := r.threads[opts.SourceThreadID]
	if !ok {
		return ThreadMeta{}, ErrThreadNotFound
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
	if newID == "" {
		for {
			r.seq++
			newID = fmt.Sprintf("%s-fork-%d", opts.SourceThreadID, r.seq)
			if _, ok := r.threads[newID]; !ok {
				break
			}
		}
	} else if existing, ok := r.threads[newID]; ok {
		if forkDestinationMatches(existing, opts, targetID) {
			return existing, nil
		}
		if opts.OperationID != "" || opts.OperationNodeID != "" {
			return ThreadMeta{}, ErrForkDestinationConflict
		}
		return ThreadMeta{}, ErrThreadExists
	}
	if len(r.entries[newID]) > 0 {
		return ThreadMeta{}, ErrThreadExists
	}
	path, err := pathLocked(r.threads, r.entries, opts.SourceThreadID, targetID)
	if err != nil {
		return ThreadMeta{}, err
	}
	meta := ThreadMeta{ID: newID, ForkedFromThreadID: opts.SourceThreadID, ForkedFromEntryID: targetID, ForkOperationID: opts.OperationID, ForkOperationNodeID: opts.OperationNodeID, CreatedAt: now, UpdatedAt: now}
	applyForkDestinationMeta(&meta, opts.DestinationMeta)
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return ThreadMeta{}, err
	}
	if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
		if _, ok := r.threads[parentID]; !ok {
			return ThreadMeta{}, fmt.Errorf("%w: parent thread %q", ErrInvalidThreadAuthority, parentID)
		}
	}
	oldToNew := map[string]string{"": ""}
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
		if opts.RewriteEntry != nil {
			next, err = opts.RewriteEntry(next, ForkEntryIdentity{
				SourceThreadID:      opts.SourceThreadID,
				DestinationThreadID: newID,
				TurnIDMap:           opts.TurnIDMap,
				RunIDMap:            opts.RunIDMap,
			})
			if err != nil {
				return ThreadMeta{}, err
			}
		}
		next.CreatedAt = now
		next.Raw = rawForEntry(next)
		next.RawHash = stableHash(next.Raw)
		oldToNew[entry.ID] = next.ID
		forkedEntries = append(forkedEntries, next)
		meta.LeafID = next.ID
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
	r.entries[newID] = forkedEntries
	r.threads[newID] = meta
	if hasForkedTodo {
		r.todos[newID] = forkedTodo
	}
	_ = ctx
	return meta, nil
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
	meta.Closed = destination.Closed
	meta.Status = strings.TrimSpace(destination.Status)
}

func forkDestinationMatches(meta ThreadMeta, opts ForkOptions, targetID string) bool {
	return opts.OperationID != "" && opts.OperationNodeID != "" &&
		meta.ForkOperationID == opts.OperationID &&
		meta.ForkOperationNodeID == opts.OperationNodeID &&
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

func (r *FileRepo) AcquireTurnLease(ctx context.Context, lease TurnLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return err
	}
	if _, err := r.mem.Thread(ctx, lease.ThreadID); err != nil {
		return err
	}
	if lease.CreatedAt.IsZero() {
		lease.CreatedAt = time.Now()
	}
	dir := filepath.Join(r.root, safePath(lease.ThreadID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "active_turn.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		return ErrActiveTurn
	}
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		_ = os.Remove(path)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func (r *FileRepo) ReleaseTurnLease(ctx context.Context, lease TurnLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	path := filepath.Join(r.root, safePath(lease.ThreadID), "active_turn.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var active TurnLease
	if err := json.Unmarshal(data, &active); err != nil {
		return err
	}
	if active.OwnerID != lease.OwnerID || active.TurnID != lease.TurnID {
		return nil
	}
	return os.Remove(path)
}

func (r *FileRepo) ActiveTurnLease(ctx context.Context, threadID string) (TurnLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return TurnLease{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	path := filepath.Join(r.root, safePath(threadID), "active_turn.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return TurnLease{}, false, nil
	}
	if err != nil {
		return TurnLease{}, false, err
	}
	var lease TurnLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return TurnLease{}, false, err
	}
	return lease, true, nil
}

func (r *FileRepo) ClearExpiredTurnLease(ctx context.Context, threadID string, cutoff time.Time) (TurnLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return TurnLease{}, false, err
	}
	if cutoff.IsZero() {
		return TurnLease{}, false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	path := filepath.Join(r.root, safePath(threadID), "active_turn.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return TurnLease{}, false, nil
	}
	if err != nil {
		return TurnLease{}, false, err
	}
	var lease TurnLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return TurnLease{}, false, err
	}
	if lease.CreatedAt.IsZero() || !lease.CreatedAt.Before(cutoff) {
		return TurnLease{}, false, nil
	}
	if err := os.Remove(path); err != nil {
		return TurnLease{}, false, err
	}
	return lease, true, nil
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

func (r *FileRepo) DeleteThread(ctx context.Context, threadID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return err
	}
	if _, ok := r.mem.threads[threadID]; !ok {
		return ErrThreadNotFound
	}
	delete(r.mem.threads, threadID)
	delete(r.mem.entries, threadID)
	delete(r.mem.todos, threadID)
	return os.RemoveAll(filepath.Join(r.root, safePath(threadID)))
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
			if opts.OperationID != "" || opts.OperationNodeID != "" {
				return ThreadMeta{}, ErrForkDestinationConflict
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
	delete(r.mem.entries, threadID)
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
		if err := ValidateThreadMetaAuthority(meta); err != nil {
			return fmt.Errorf("validate thread metadata %s: %w", path, err)
		}
		dir := filepath.Dir(path)
		entries, err := readEntries(filepath.Join(dir, "entries.jsonl"))
		if err != nil {
			return fmt.Errorf("read thread entries %s: %w", path, err)
		}
		if len(entries) == 0 && strings.TrimSpace(meta.LeafID) != "" {
			return fmt.Errorf("thread metadata %s references leaf %q without journal entries", path, meta.LeafID)
		}
		repairedMeta := reconcileFileThreadLeaf(meta, entries)
		if repairedMeta != meta {
			meta = repairedMeta
			if err := r.saveThread(meta); err != nil {
				return err
			}
		}
		mem.threads[meta.ID] = meta
		mem.entries[meta.ID] = entries
		todoData, err := os.ReadFile(filepath.Join(dir, "agent_todos.json"))
		if err == nil {
			var todo AgentTodoState
			if err := json.Unmarshal(todoData, &todo); err != nil {
				return fmt.Errorf("decode agent todo state for thread %q: %w", meta.ID, err)
			}
			if todo.ThreadID != meta.ID {
				return fmt.Errorf("agent todo state thread id %q does not match %q", todo.ThreadID, meta.ID)
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
	return BuildContextProjection(path, ContextProjectionOptions{}).Messages
}

func BuildContextProjection(path []Entry, opts ContextProjectionOptions) ContextProjection {
	if opts.Purpose == "" {
		opts.Purpose = ProjectionProviderRequest
	}
	messages := buildContextMessages(path)
	return ContextProjection{Messages: messages, Segments: projectedSegments(path, messages, opts.Purpose)}
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

func AppendMessage(ctx context.Context, repo Repo, threadID, turnID string, msg session.Message) (Entry, error) {
	return AppendMessageAt(ctx, repo, threadID, turnID, msg, time.Time{})
}

func AppendMessageAt(ctx context.Context, repo Repo, threadID, turnID string, msg session.Message, observedAt time.Time) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: typeForMessage(msg), Message: msg}, AppendOptions{Now: observedAt})
}

func AppendCompaction(ctx context.Context, repo Repo, threadID, turnID string, result compaction.Result) (Entry, error) {
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
	return repo.Append(ctx, Entry{
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
	}, AppendOptions{})
}

func AppendTurnMarker(ctx context.Context, repo Repo, threadID, turnID string, status TurnMarkerStatus, metadata map[string]string) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: EntryTurnMarker, TurnStatus: status, Metadata: metadata}, AppendOptions{})
}

func AppendTurnMarkerWithID(ctx context.Context, repo Repo, threadID, turnID, entryID string, status TurnMarkerStatus, metadata map[string]string) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: EntryTurnMarker, TurnStatus: status, Metadata: metadata}, AppendOptions{ID: entryID})
}

func AppendActiveTools(ctx context.Context, repo Repo, threadID string, metadata map[string]string) (Entry, error) {
	return repo.Append(ctx, Entry{ThreadID: threadID, Type: EntryActiveTools, Metadata: metadata}, AppendOptions{})
}

func AppendFailure(ctx context.Context, repo Repo, threadID, turnID, message string) (Entry, error) {
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
		case "entry_id", "parent_entry_id", "input_entry_id":
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
	for id := leafID; id != ""; {
		entry, ok := byID[id]
		if !ok {
			return nil, ErrEntryNotFound
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

func PrepareEntry(entry Entry) Entry {
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	return entry
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
