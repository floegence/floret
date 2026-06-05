package sessiontree

import (
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

	"github.com/floegence/floret/compaction"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/control"
	"github.com/floegence/floret/session"
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

var (
	ErrThreadNotFound = errors.New("session tree thread not found")
	ErrEntryNotFound  = errors.New("session tree entry not found")
	ErrInvalidParent  = errors.New("session tree invalid parent")
	ErrActiveTurn     = errors.New("session tree thread already has an active turn")
	ErrThreadExists   = errors.New("session tree thread already exists")
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
	ID                 string    `json:"id"`
	LeafID             string    `json:"leaf_id,omitempty"`
	ParentThreadID     string    `json:"parent_thread_id,omitempty"`
	ForkedFromThreadID string    `json:"forked_from_thread_id,omitempty"`
	ForkedFromEntryID  string    `json:"forked_from_entry_id,omitempty"`
	Archived           bool      `json:"archived,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
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
	SourceThreadID string
	EntryID        string
	Position       ForkPosition
	NewThreadID    string
	Now            time.Time
}

type ContextOptions struct {
	IncludeSystem bool
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
	MoveLeaf(context.Context, string, string) error
	Fork(context.Context, ForkOptions) (ThreadMeta, error)
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

type MemoryRepo struct {
	mu      sync.Mutex
	threads map[string]ThreadMeta
	entries map[string][]Entry
	leases  map[string]TurnLease
	seq     int64
}

func NewMemoryRepo() *MemoryRepo {
	return &MemoryRepo{threads: map[string]ThreadMeta{}, entries: map[string][]Entry{}, leases: map[string]TurnLease{}}
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

func (r *MemoryRepo) Thread(_ context.Context, threadID string) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[threadID]
	if !ok {
		return ThreadMeta{}, ErrThreadNotFound
	}
	return meta, nil
}

func (r *MemoryRepo) UpdateThread(_ context.Context, meta ThreadMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[meta.ID]; !ok {
		return ErrThreadNotFound
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
	if targetID == "" {
		targetID = sourceMeta.LeafID
	}
	if opts.Position == ForkBefore {
		entry, ok := findEntry(r.entries[opts.SourceThreadID], targetID)
		if !ok {
			return ThreadMeta{}, ErrEntryNotFound
		}
		targetID = entry.ParentID
	}
	path, err := pathLocked(r.threads, r.entries, opts.SourceThreadID, targetID)
	if err != nil {
		return ThreadMeta{}, err
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
	} else if _, ok := r.threads[newID]; ok {
		return ThreadMeta{}, ErrThreadExists
	}
	if len(r.entries[newID]) > 0 {
		return ThreadMeta{}, ErrThreadExists
	}
	meta := ThreadMeta{ID: newID, ParentThreadID: opts.SourceThreadID, ForkedFromThreadID: opts.SourceThreadID, ForkedFromEntryID: targetID, CreatedAt: now, UpdatedAt: now}
	oldToNew := map[string]string{"": ""}
	for _, entry := range path {
		r.seq++
		next := cloneEntry(entry)
		next.ID = fmt.Sprintf("%s-entry-%d", newID, r.seq)
		next.ThreadID = newID
		next.ParentID = oldToNew[entry.ParentID]
		next.FirstKeptEntryID = oldToNew[entry.FirstKeptEntryID]
		next.CompactedThroughEntryID = oldToNew[entry.CompactedThroughEntryID]
		next.KeptUserEntryIDs = rewriteEntryIDs(entry.KeptUserEntryIDs, oldToNew)
		next.CreatedAt = now
		next.Raw = rawForEntry(next)
		next.RawHash = stableHash(next.Raw)
		oldToNew[entry.ID] = next.ID
		r.entries[newID] = append(r.entries[newID], next)
		meta.LeafID = next.ID
	}
	r.threads[newID] = meta
	_ = ctx
	return meta, nil
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

func (r *FileRepo) Thread(ctx context.Context, threadID string) (ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return ThreadMeta{}, err
	}
	return r.mem.Thread(ctx, threadID)
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
	meta, err := r.mem.Fork(ctx, opts)
	if err != nil {
		return ThreadMeta{}, err
	}
	if err := r.saveThread(meta); err != nil {
		return ThreadMeta{}, err
	}
	entries, err := r.mem.Entries(ctx, meta.ID)
	if err != nil {
		return ThreadMeta{}, err
	}
	for _, entry := range entries {
		if err := r.appendEntry(entry); err != nil {
			return ThreadMeta{}, err
		}
	}
	return meta, nil
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
			continue
		}
		var meta ThreadMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		if meta.ID == "" {
			continue
		}
		dir := filepath.Dir(path)
		entries, err := readEntries(filepath.Join(dir, "entries.jsonl"))
		if err != nil {
			continue
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

func BuildContext(path []Entry, _ ContextOptions) []session.Message {
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
		compaction := path[compactionIndex]
		tailEntryIDs := map[string]struct{}{}
		if firstKeptIndex >= 0 && firstKeptIndex < compactionIndex {
			for _, entry := range path[firstKeptIndex:compactionIndex] {
				tailEntryIDs[entry.ID] = struct{}{}
			}
		}
		for _, entry := range path[compactionIndex+1:] {
			tailEntryIDs[entry.ID] = struct{}{}
		}
		for _, entry := range keptUserEntries(path[:compactionIndex], compaction.KeptUserEntryIDs, tailEntryIDs) {
			messages = appendProviderVisible(messages, entry)
		}
		if compaction.Summary != "" {
			messages = append(messages, session.Message{
				Role:                 session.Assistant,
				Content:              compaction.Summary,
				EntryID:              compaction.ID,
				ParentEntryID:        compaction.ParentID,
				Kind:                 session.MessageKindCompactionSummary,
				CompactionID:         compaction.CompactionID,
				CompactionGeneration: compaction.CompactionGeneration,
				CompactionWindowID:   compaction.CompactionWindowID,
			})
		}
		if firstKeptIndex >= 0 && firstKeptIndex < compactionIndex {
			for _, entry := range path[firstKeptIndex:compactionIndex] {
				messages = appendProviderVisible(messages, entry)
			}
		}
		for _, entry := range path[compactionIndex+1:] {
			messages = appendProviderVisible(messages, entry)
		}
		return messages
	}
	for _, entry := range path {
		messages = appendProviderVisible(messages, entry)
	}
	return messages
}

func appendProviderVisible(messages []session.Message, entry Entry) []session.Message {
	switch entry.Type {
	case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult:
		if entry.Message.Role != "" {
			msg := entry.Message
			if entry.Type == EntryToolCall {
				if projected, ok := control.ProjectMessage(msg); ok {
					msg = projected
				}
			}
			msg.EntryID = entry.ID
			msg.ParentEntryID = entry.ParentID
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
	return repo.Append(ctx, Entry{ThreadID: threadID, TurnID: turnID, Type: typeForMessage(msg), Message: msg}, AppendOptions{})
}

func AppendCompaction(ctx context.Context, repo Repo, threadID, turnID string, result compaction.Result) (Entry, error) {
	windowID := result.CompactionID
	if windowID == "" {
		windowID = result.FirstKeptEntryID
	}
	return repo.Append(ctx, Entry{
		ThreadID:                threadID,
		TurnID:                  turnID,
		Type:                    EntryCompaction,
		CompactionID:            result.CompactionID,
		PreviousCompactionID:    result.PreviousCompactionID,
		CompactedThroughEntryID: result.CompactedThroughEntryID,
		SummarySchemaVersion:    result.SummarySchemaVersion,
		CompactionGeneration:    nextCompactionGeneration(result),
		CompactionWindowID:      windowID,
		FirstKeptEntryID:        result.FirstKeptEntryID,
		KeptUserEntryIDs:        append([]string(nil), result.KeptUserEntryIDs...),
		Summary:                 result.Summary,
		CompactionTrigger:       string(result.Trigger),
		CompactionReason:        string(result.Reason),
		CompactionPhase:         string(result.Phase),
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

func nextCompactionGeneration(result compaction.Result) int {
	if value := result.Details["compaction_generation"]; value != "" {
		var generation int
		if _, err := fmt.Sscanf(value, "%d", &generation); err == nil && generation > 0 {
			return generation
		}
	}
	if result.PreviousCompactionID != "" {
		return 2
	}
	return 1
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
	if entry.Metadata != nil {
		entry.Metadata = mapsClone(entry.Metadata)
	}
	if entry.KeptUserEntryIDs != nil {
		entry.KeptUserEntryIDs = append([]string(nil), entry.KeptUserEntryIDs...)
	}
	return entry
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
