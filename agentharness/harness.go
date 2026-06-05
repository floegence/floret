package agentharness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/compaction"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/tools"
)

var (
	ErrActiveTurn    = errors.New("thread already has an active turn")
	ErrNoRetryTarget = errors.New("thread has no retryable turn")
)

const (
	threadPhaseIdle       = sessionlifecycle.PhaseIdle
	threadPhaseTurn       = sessionlifecycle.PhaseTurn
	staleTurnLeaseTimeout = 24 * time.Hour
)

type HarnessEventType string

const (
	EventThreadStarted HarnessEventType = "thread_started"
	EventThreadResumed HarnessEventType = "thread_resumed"
	EventThreadForked  HarnessEventType = "thread_forked"
	EventLeafMoved     HarnessEventType = "leaf_moved"
	EventTurnStarted   HarnessEventType = "turn_started"
	EventTurnCompleted HarnessEventType = "turn_completed"
	EventTurnFailed    HarnessEventType = "turn_failed"
	EventTurnAborted   HarnessEventType = "turn_aborted"
	EventEntryAppended HarnessEventType = "entry_appended"
	EventRetryStarted  HarnessEventType = "retry_started"
)

type HarnessEvent struct {
	Type      HarnessEventType  `json:"type"`
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
	PromptStore         promptcache.Store
	Repo                sessiontree.Repo
	Sink                event.Sink
	HarnessSink         HarnessSink
	Approver            tools.Approver
	StopHook            engine.StopHook
	CompactionGenerator compaction.SummaryGenerator
	TurnPolicy          TurnPolicy
	LoopLimits          LoopLimits
	NewID               func(string) string
	Now                 func() time.Time
}

type TurnPolicy struct {
	ContextPolicy         contextpolicy.Policy
	CacheRetention        promptcache.Retention
	HostedToolDefinitions []provider.HostedToolDefinition
	CompletionPolicy      engine.CompletionPolicy
}

type LoopLimits struct {
	MaxEmptyProviderRetries  int
	NoProgressLimit          int
	DuplicateToolLimit       int
	WallTime                 time.Duration
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	MaxLengthContinuations   int
	MaxStopHookContinuations int
}

type AgentHarness struct {
	mu      sync.Mutex
	options Options
	threads map[string]*Thread
	seq     int64
}

type StartThreadOptions struct {
	ThreadID string
}

type ResumeOptions struct{}

type ForkOptions struct {
	SourceThreadID string
	EntryID        string
	Position       sessiontree.ForkPosition
	NewThreadID    string
}

type MoveOptions struct {
	Summary string
}

type RunOptions struct {
	TurnID string
}

type RetryOptions struct {
	Reason string
}

type ThreadSnapshot struct {
	ID               string          `json:"id"`
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
		options.PromptStore = promptcache.NewMemoryStore()
	}
	if options.Tools == nil {
		options.Tools = tools.NewRegistry()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &AgentHarness{options: options, threads: map[string]*Thread{}}
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

func (h *AgentHarness) ResumeThread(ctx context.Context, id string, _ ResumeOptions) (*Thread, error) {
	meta, err := h.options.Repo.Thread(ctx, id)
	if err != nil {
		return nil, err
	}
	if lease, ok, err := h.activeTurnLease(ctx, meta.ID); err != nil {
		return nil, err
	} else if ok && lease.TurnID != "" {
		cutoff := h.now().Add(-staleTurnLeaseTimeout)
		if cleared, stale, clearErr := h.clearExpiredTurnLease(ctx, meta.ID, cutoff); clearErr != nil {
			return nil, clearErr
		} else if !stale || cleared.TurnID != lease.TurnID {
			return nil, ErrActiveTurn
		}
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
		if _, err := sessiontree.AppendFailure(ctx, h.options.Repo, meta.ID, turnID, "turn interrupted during previous process"); err != nil {
			return err
		}
		if _, err := sessiontree.AppendTurnMarker(ctx, h.options.Repo, meta.ID, turnID, sessiontree.TurnAborted, map[string]string{"recoverable": "true"}); err != nil {
			return err
		}
	}
	return nil
}

func (h *AgentHarness) ForkThread(ctx context.Context, opts ForkOptions) (*Thread, error) {
	meta, err := h.options.Repo.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: opts.SourceThreadID,
		EntryID:        opts.EntryID,
		Position:       opts.Position,
		NewThreadID:    opts.NewThreadID,
		Now:            h.now(),
	})
	if err != nil {
		return nil, err
	}
	thread := h.cacheThread(meta.ID)
	h.emit(HarnessEvent{Type: EventThreadForked, ThreadID: meta.ID, EntryID: meta.ForkedFromEntryID, Metadata: map[string]string{"source_thread_id": opts.SourceThreadID}})
	return thread, nil
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
		h.options.Sink.Emit(event.Sanitize(event.Event{Type: event.Type(ev.Type), RunID: ev.TurnID, SessionID: ev.ThreadID, Message: ev.Message, Timestamp: ev.Timestamp}))
	}
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
			if entry.Message.Kind == session.MessageKindMicrocompactMarker {
				continue
			}
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
	return t.runLeased(ctx, "", RunOptions{TurnID: turnID}, &target.Entry)
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

func (t *Thread) Compact(ctx context.Context, summary, firstKeptEntryID string) (sessiontree.Entry, error) {
	release, err := t.enterMutation(ctx, t.harness.nextID("compact"))
	if err != nil {
		return sessiontree.Entry{}, err
	}
	defer release()
	result := compaction.Result{
		CompactionID:         t.harness.nextID("compaction"),
		FirstKeptEntryID:     firstKeptEntryID,
		Summary:              summary,
		SummarySchemaVersion: compaction.SummarySchemaVersion,
		Trigger:              compaction.TriggerManual,
		Reason:               compaction.ReasonManual,
		Phase:                compaction.PhaseInstall,
		CreatedAt:            t.harness.now(),
	}
	entry, err := sessiontree.AppendCompaction(ctx, t.harness.options.Repo, t.id, "", result)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, ThreadID: t.id, EntryID: entry.ID, ParentID: entry.ParentID, Message: "compaction"})
	return entry, nil
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
	return t.runLeased(ctx, input, RunOptions{TurnID: turnID}, retryUser)
}

func (t *Thread) runLeased(ctx context.Context, input string, opts RunOptions, retryUser *sessiontree.Entry) (TurnResult, error) {
	turnID := opts.TurnID
	if _, err := sessiontree.AppendTurnMarker(ctx, t.harness.options.Repo, t.id, turnID, sessiontree.TurnStarted, nil); err != nil {
		return TurnResult{}, err
	}
	t.harness.emit(HarnessEvent{Type: EventTurnStarted, ThreadID: t.id, TurnID: turnID})
	if retryUser == nil {
		entry, err := sessiontree.AppendMessage(ctx, t.harness.options.Repo, t.id, turnID, session.Message{Role: session.User, Content: input})
		if err != nil {
			persistCtx, cancelPersist := turnFinalizationContext(ctx)
			defer cancelPersist()
			return t.finalizeFailedTurn(persistCtx, turnID, turnID, statusForError(err), err, "append_user_error")
		}
		t.harness.emit(HarnessEvent{Type: EventEntryAppended, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID})
	}
	snap, err := t.Journal(ctx)
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, turnID, statusForError(err), err, "snapshot_error")
	}
	history := sessiontree.BuildContext(snap.Path, sessiontree.ContextOptions{})
	runID := turnID
	engineOptions := t.harness.engineOptions()
	engineOptions.RunID = runID
	engineOptions.SessionID = t.id
	engineOptions.TraceID = runID
	engineOptions.ProviderName = t.harness.options.ProviderName
	engineOptions.Model = t.harness.options.Model
	engineOptions.ContextPolicy = contextpolicy.Normalize(engineOptions.ContextPolicy)
	eng, err := engine.New(engine.Config{
		Provider: t.harness.options.Provider,
		Tools:    t.harness.options.Tools,
		Prompt:   t.harness.options.PromptStore,
		Memory: &memory.Manager{
			SystemPrompt: t.harness.options.SystemPrompt,
		},
		Approver:  t.harness.options.Approver,
		StopHook:  t.harness.options.StopHook,
		Compactor: &durableCompactionManager{thread: t, turnID: turnID},
		Options:   engineOptions,
	})
	if err != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		defer cancelPersist()
		return t.finalizeFailedTurn(persistCtx, turnID, runID, engine.Failed, err, "engine_config_error")
	}
	projection := &turnProjection{thread: t, ctx: ctx, turnID: turnID, downstream: t.harness.options.Sink}
	eng.SetSink(projection)
	result := eng.RunTurn(ctx, engine.RunInput{RunID: runID, SessionID: t.id, TraceID: runID, History: history})
	persistCtx, cancelPersist := turnFinalizationContext(ctx)
	defer cancelPersist()
	projection.ctx = persistCtx
	if projection.err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, engine.Failed, projection.err, "projection_error")
	}
	if err := projection.Flush(); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, engine.Failed, err, "projection_flush_error")
	}
	deltaBase := history
	current, err := t.Journal(persistCtx)
	if err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "snapshot_error")
	}
	deltaBase = current.Context
	if err := t.appendDelta(persistCtx, turnID, deltaBase, result.Messages, current.Path); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "append_delta_error")
	}
	status := markerForStatus(result.Status)
	savePointMetadata := markerMetadata(runID, result)
	savePointMetadata["reason"] = "run_result"
	if _, err := sessiontree.AppendTurnMarker(persistCtx, t.harness.options.Repo, t.id, turnID, sessiontree.TurnSavePoint, savePointMetadata); err != nil {
		return TurnResult{}, err
	}
	if result.Err != nil {
		if _, err := sessiontree.AppendFailure(persistCtx, t.harness.options.Repo, t.id, turnID, result.Err.Error()); err != nil {
			return TurnResult{}, err
		}
	}
	terminalMetadata := markerMetadata(runID, result)
	if result.Err != nil {
		terminalMetadata["failure_reason"] = result.Err.Error()
	}
	if result.Status == engine.Waiting {
		terminalMetadata["interrupt_reason"] = "ask_user"
	}
	if _, err := sessiontree.AppendTurnMarker(persistCtx, t.harness.options.Repo, t.id, turnID, status, terminalMetadata); err != nil {
		var committed sessiontree.AppendCommittedError
		if errors.As(err, &committed) {
			return turnResultFromEngine(turnID, result, map[string]string{"terminal_persistence_error": err.Error()}), result.Err
		}
		return TurnResult{}, err
	}
	eventType := EventTurnCompleted
	if result.Status == engine.Failed {
		eventType = EventTurnFailed
	}
	if result.Status == engine.Cancelled {
		eventType = EventTurnAborted
	}
	t.harness.emit(HarnessEvent{Type: eventType, ThreadID: t.id, TurnID: turnID, Status: string(result.Status), Message: result.Output})
	return turnResultFromEngine(turnID, result, nil), result.Err
}

func (t *Thread) finalizeFailedTurn(ctx context.Context, turnID, runID string, status engine.Status, err error, diagnostic string) (TurnResult, error) {
	if status == "" {
		status = statusForError(err)
	}
	result := engine.Result{Status: status, Err: err}
	if err != nil {
		if _, appendErr := sessiontree.AppendFailure(ctx, t.harness.options.Repo, t.id, turnID, err.Error()); appendErr != nil {
			return TurnResult{}, appendErr
		}
	}
	metadata := markerMetadata(runID, result)
	if err != nil {
		metadata["failure_reason"] = err.Error()
	}
	if diagnostic != "" {
		metadata["diagnostic"] = diagnostic
	}
	if _, appendErr := sessiontree.AppendTurnMarker(ctx, t.harness.options.Repo, t.id, turnID, markerForStatus(status), metadata); appendErr != nil {
		var committed sessiontree.AppendCommittedError
		if errors.As(appendErr, &committed) {
			return turnResultFromEngine(turnID, result, map[string]string{"terminal_persistence_error": appendErr.Error(), "diagnostic": diagnostic}), err
		}
		return TurnResult{}, appendErr
	}
	eventType := EventTurnFailed
	if status == engine.Cancelled {
		eventType = EventTurnAborted
	}
	t.harness.emit(HarnessEvent{Type: eventType, ThreadID: t.id, TurnID: turnID, Status: string(status)})
	return turnResultFromEngine(turnID, result, map[string]string{"diagnostic": diagnostic}), err
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
		policy.ContextPolicy.MaxCompactionFailures > 0 ||
		policy.ContextPolicy.MicrocompactToolTokens > 0 ||
		policy.ContextPolicy.EstimatorSource != "" {
		engineOptions.ContextPolicy = policy.ContextPolicy
	}
	if policy.CacheRetention != "" {
		engineOptions.CacheRetention = policy.CacheRetention
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
	return engineOptions
}

func turnResultFromEngine(turnID string, result engine.Result, diagnostics map[string]string) TurnResult {
	return TurnResult{
		ID:                 turnID,
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

func (t *Thread) appendDelta(ctx context.Context, turnID string, before, after []session.Message, currentPath []sessiontree.Entry) error {
	start := sharedMessagePrefix(before, after)
	persisted := persistedTurnMessages(currentPath, turnID)
	for _, msg := range after[start:] {
		if nonDurableProjection(msg) {
			continue
		}
		// IMPORTANT: Realtime turn projection and appendDelta share the durable
		// journal for one turn. appendDelta may only backfill messages that were
		// not already persisted by projection; hiding duplicates in the UI or
		// deduping across turns would corrupt the session history contract.
		if persisted.skip(msg) {
			continue
		}
		if err := t.appendMessage(ctx, turnID, msg); err != nil {
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
		CompactionID:         msg.CompactionID,
		CompactionGeneration: msg.CompactionGeneration,
		CompactionWindowID:   msg.CompactionWindowID,
	}
}

func nonDurableProjection(msg session.Message) bool {
	return msg.Kind == session.MessageKindCompactionSummary || msg.Kind == session.MessageKindMicrocompactMarker
}

func (t *Thread) appendMessage(ctx context.Context, turnID string, msg session.Message) error {
	msg.EntryID = ""
	msg.ParentEntryID = ""
	entry, err := sessiontree.AppendMessage(ctx, t.harness.options.Repo, t.id, turnID, msg)
	if err != nil {
		return err
	}
	t.harness.emit(HarnessEvent{Type: EventEntryAppended, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID})
	return nil
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
	return a == b
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
		generator = compaction.ProviderSummaryGenerator{
			Provider:     req.Provider,
			ProviderName: req.ProviderName,
			Model:        req.Model,
			Policy:       req.Policy,
			Fallback:     compaction.ExtractiveSummaryGenerator{},
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
		Phase:                compaction.PhaseInstall,
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
	entry, err := sessiontree.AppendCompaction(ctx, m.thread.harness.options.Repo, m.thread.id, m.turnID, prep.Result)
	if err != nil {
		return compaction.Result{}, nil, err
	}
	prep.Result.CompactionID = entry.CompactionID
	for i := range prep.ActiveMessages {
		if prep.ActiveMessages[i].Kind != session.MessageKindCompactionSummary {
			continue
		}
		prep.ActiveMessages[i].EntryID = entry.ID
		prep.ActiveMessages[i].ParentEntryID = entry.ParentID
		prep.ActiveMessages[i].CompactionID = entry.CompactionID
		prep.ActiveMessages[i].CompactionGeneration = entry.CompactionGeneration
		prep.ActiveMessages[i].CompactionWindowID = entry.CompactionWindowID
	}
	m.thread.harness.emit(HarnessEvent{Type: EventEntryAppended, ThreadID: m.thread.id, TurnID: m.turnID, EntryID: entry.ID, ParentID: entry.ParentID, Message: "compaction"})
	return prep.Result, prep.ActiveMessages, nil
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
	downstream       event.Sink
	mu               sync.Mutex
	text             string
	reasoning        string
	pendingCalls     []session.Message
	pendingResults   []session.Message
	pendingBatchSize int
	err              error
}

func (p *turnProjection) Emit(ev event.Event) {
	if p.downstream != nil {
		p.downstream.Emit(event.Sanitize(ev))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return
	}
	switch ev.Type {
	case event.ProviderDelta:
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
		p.text += ev.Message
	case event.ProviderReasoning:
		p.reasoning += ev.Message
	case event.ToolCall:
		if p.text != "" {
			p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: p.text, Reasoning: p.reasoning})
			p.text = ""
			if p.err != nil {
				return
			}
		}
		p.pendingCalls = append(p.pendingCalls, session.Message{Role: session.Assistant, Content: "tool_call", Reasoning: p.reasoning, ToolCallID: ev.ToolID, ToolName: ev.ToolName, ToolArgs: ev.Args})
		if size := eventBatchSize(ev.Metadata); size > p.pendingBatchSize {
			p.pendingBatchSize = size
		}
	case event.ToolResult:
		if p.text != "" {
			p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: p.text, Reasoning: p.reasoning})
			p.text = ""
			p.reasoning = ""
			if p.err != nil {
				return
			}
		}
		p.pendingResults = append(p.pendingResults, session.Message{Role: session.Tool, Content: ev.Result, ToolCallID: ev.ToolID, ToolName: ev.ToolName})
		if size := eventBatchSize(ev.Metadata); size > p.pendingBatchSize {
			p.pendingBatchSize = size
		}
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
	case event.ContextContinue:
		if err := p.flushPendingToolBatch(false); err != nil {
			p.err = err
			return
		}
		if p.text != "" {
			p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: p.text, Reasoning: p.reasoning})
			p.text = ""
			p.reasoning = ""
			if p.err != nil {
				return
			}
		}
		p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.User, Content: ev.Message})
		if p.err != nil {
			return
		}
		metadata := map[string]string{"reason": "context_continue", "continuation_reason": ev.ContinuationReason}
		if ev.Result != "" {
			metadata["hook_reason"] = ev.Result
		}
		_, p.err = sessiontree.AppendTurnMarker(p.ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, sessiontree.TurnSavePoint, metadata)
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
	if p.text != "" {
		if err := p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: p.text, Reasoning: p.reasoning}); err != nil {
			p.err = err
			return err
		}
		p.text = ""
		p.reasoning = ""
	}
	return nil
}

func (p *turnProjection) flushPendingToolBatch(force bool) error {
	if len(p.pendingCalls) == 0 && len(p.pendingResults) == 0 {
		return nil
	}
	size := p.pendingBatchSize
	if size <= 0 {
		size = len(p.pendingCalls)
	}
	if !force && (len(p.pendingCalls) < size || len(p.pendingResults) < size) {
		return nil
	}
	if len(p.pendingCalls) != len(p.pendingResults) {
		return fmt.Errorf("incomplete tool result batch: %d calls, %d results", len(p.pendingCalls), len(p.pendingResults))
	}
	byID := make(map[string]session.Message, len(p.pendingResults))
	for _, result := range p.pendingResults {
		if result.ToolCallID == "" {
			return errors.New("tool result batch contains empty tool_call_id")
		}
		if _, ok := byID[result.ToolCallID]; ok {
			return fmt.Errorf("tool result batch contains duplicate tool_call_id %q", result.ToolCallID)
		}
		byID[result.ToolCallID] = result
	}
	for _, call := range p.pendingCalls {
		if err := p.thread.appendMessage(p.ctx, p.turnID, call); err != nil {
			return err
		}
	}
	for _, call := range p.pendingCalls {
		result, ok := byID[call.ToolCallID]
		if !ok {
			return fmt.Errorf("tool result batch missing result for %q", call.ToolCallID)
		}
		if err := p.thread.appendMessage(p.ctx, p.turnID, result); err != nil {
			return err
		}
	}
	if _, err := sessiontree.AppendTurnMarker(p.ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, sessiontree.TurnSavePoint, map[string]string{"reason": "tool_result_batch"}); err != nil {
		return err
	}
	p.pendingCalls = nil
	p.pendingResults = nil
	p.pendingBatchSize = 0
	p.reasoning = ""
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
