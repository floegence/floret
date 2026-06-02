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
	ContextPolicy       contextpolicy.Policy
	CompactionGenerator compaction.SummaryGenerator
	EngineOptions       engine.Options
	NewID               func(string) string
	Now                 func() time.Time
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
	thread := &Thread{harness: h, id: id, phase: "idle"}
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
		h.options.Sink.Emit(event.Event{Type: event.Type(ev.Type), RunID: ev.TurnID, SessionID: ev.ThreadID, Message: ev.Message, Timestamp: ev.Timestamp})
	}
}

func (t *Thread) ID() string {
	return t.id
}

func (t *Thread) Read(ctx context.Context) (ThreadSnapshot, error) {
	meta, err := t.harness.options.Repo.Thread(ctx, t.id)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	path, err := t.harness.options.Repo.Path(ctx, t.id, meta.LeafID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	entries, err := t.harness.options.Repo.Entries(ctx, t.id)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	t.mu.Lock()
	phase := t.phase
	t.mu.Unlock()
	return ThreadSnapshot{
		Meta:    meta,
		Path:    path,
		Entries: entries,
		Context: sessiontree.BuildContext(path, sessiontree.ContextOptions{}),
		Phase:   phase,
	}, nil
}

func (t *Thread) Run(ctx context.Context, input string, opts RunOptions) (TurnResult, error) {
	return t.run(ctx, input, opts, nil)
}

func (t *Thread) Retry(ctx context.Context, opts RetryOptions) (TurnResult, error) {
	snap, err := t.Read(ctx)
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
	return t.run(ctx, "", RunOptions{}, &target.Entry)
}

func (t *Thread) MoveTo(ctx context.Context, entryID string, opts MoveOptions) error {
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
	turnID := opts.TurnID
	if turnID == "" {
		turnID = t.harness.nextID("turn")
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, t.harness.options.Repo, t.id, turnID, sessiontree.TurnStarted, nil); err != nil {
		return TurnResult{}, err
	}
	t.harness.emit(HarnessEvent{Type: EventTurnStarted, ThreadID: t.id, TurnID: turnID})
	if retryUser == nil {
		entry, err := sessiontree.AppendMessage(ctx, t.harness.options.Repo, t.id, turnID, session.Message{Role: session.User, Content: input})
		if err != nil {
			return TurnResult{}, err
		}
		t.harness.emit(HarnessEvent{Type: EventEntryAppended, ThreadID: t.id, TurnID: turnID, EntryID: entry.ID, ParentID: entry.ParentID})
	}
	snap, err := t.Read(ctx)
	if err != nil {
		return TurnResult{}, err
	}
	history := sessiontree.BuildContext(snap.Path, sessiontree.ContextOptions{})
	runID := turnID
	engineOptions := t.harness.options.EngineOptions
	engineOptions.RunID = runID
	engineOptions.SessionID = t.id
	engineOptions.TraceID = runID
	engineOptions.ProviderName = t.harness.options.ProviderName
	engineOptions.Model = t.harness.options.Model
	engineOptions.ContextPolicy = contextpolicy.Normalize(mergeContextPolicy(engineOptions.ContextPolicy, t.harness.options.ContextPolicy))
	eng := &engine.Engine{
		Provider: t.harness.options.Provider,
		Tools:    t.harness.options.Tools,
		Prompt:   t.harness.options.PromptStore,
		Memory: &memory.Manager{
			SystemPrompt: t.harness.options.SystemPrompt,
		},
		Sink:      nil,
		Approver:  t.harness.options.Approver,
		StopHook:  t.harness.options.StopHook,
		Compactor: &durableCompactionManager{thread: t, turnID: turnID},
		Options:   engineOptions,
	}
	projection := &turnProjection{thread: t, ctx: ctx, turnID: turnID, downstream: t.harness.options.Sink}
	eng.Sink = projection
	result := eng.RunTurn(ctx, engine.RunInput{RunID: runID, SessionID: t.id, TraceID: runID, History: history})
	if projection.err != nil {
		return TurnResult{}, projection.err
	}
	deltaBase := history
	current, err := t.Read(ctx)
	if err != nil {
		return TurnResult{}, err
	}
	deltaBase = current.Context
	if err := t.appendDelta(ctx, turnID, deltaBase, result.Messages); err != nil {
		return TurnResult{}, err
	}
	status := markerForStatus(result.Status)
	savePointMetadata := markerMetadata(runID, result)
	savePointMetadata["reason"] = "run_result"
	if _, err := sessiontree.AppendTurnMarker(ctx, t.harness.options.Repo, t.id, turnID, sessiontree.TurnSavePoint, savePointMetadata); err != nil {
		return TurnResult{}, err
	}
	if result.Err != nil {
		if _, err := sessiontree.AppendFailure(ctx, t.harness.options.Repo, t.id, turnID, result.Err.Error()); err != nil {
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
	if _, err := sessiontree.AppendTurnMarker(ctx, t.harness.options.Repo, t.id, turnID, status, terminalMetadata); err != nil {
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
	return TurnResult{
		ID:                 turnID,
		Status:             result.Status,
		Output:             result.Output,
		Err:                result.Err,
		Metrics:            result.Metrics,
		CompletionReason:   result.CompletionReason,
		ContinuationReason: result.ContinuationReason,
		FinishReason:       result.FinishReason,
		RawFinishReason:    result.RawFinishReason,
		FinishInferred:     result.FinishInferred,
	}, result.Err
}

func (t *Thread) enterTurn() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active {
		return ErrActiveTurn
	}
	t.active = true
	t.phase = "turn"
	return nil
}

func (t *Thread) leaveTurn() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = false
	t.phase = "idle"
}

func (t *Thread) appendDelta(ctx context.Context, turnID string, before, after []session.Message) error {
	start := sharedMessagePrefix(before, after)
	for _, msg := range after[start:] {
		if nonDurableProjection(msg) {
			continue
		}
		if err := t.appendMessage(ctx, turnID, msg); err != nil {
			return err
		}
	}
	return nil
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
	switch status {
	case engine.Completed:
		return sessiontree.TurnCompleted
	case engine.Waiting:
		return sessiontree.TurnWaiting
	case engine.Cancelled:
		return sessiontree.TurnAborted
	default:
		return sessiontree.TurnFailed
	}
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
	snap, err := m.thread.Read(ctx)
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

func mergeContextPolicy(primary, fallback contextpolicy.Policy) contextpolicy.Policy {
	if primary.ContextWindowTokens <= 0 {
		primary.ContextWindowTokens = fallback.ContextWindowTokens
	}
	if primary.MaxOutputTokens <= 0 {
		primary.MaxOutputTokens = fallback.MaxOutputTokens
	}
	if primary.ReservedOutputTokens <= 0 {
		primary.ReservedOutputTokens = fallback.ReservedOutputTokens
	}
	if primary.ReservedSummaryTokens <= 0 {
		primary.ReservedSummaryTokens = fallback.ReservedSummaryTokens
	}
	if primary.RecentTailTokens <= 0 {
		primary.RecentTailTokens = fallback.RecentTailTokens
	}
	if primary.EstimatorSource == "" {
		primary.EstimatorSource = fallback.EstimatorSource
	}
	if primary.MaxCompactionFailures <= 0 {
		primary.MaxCompactionFailures = fallback.MaxCompactionFailures
	}
	if primary.MicrocompactToolTokens <= 0 {
		primary.MicrocompactToolTokens = fallback.MicrocompactToolTokens
	}
	return primary
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
	thread     *Thread
	ctx        context.Context
	turnID     string
	downstream event.Sink
	mu         sync.Mutex
	text       string
	err        error
}

func (p *turnProjection) Emit(ev event.Event) {
	if p.downstream != nil {
		p.downstream.Emit(ev)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return
	}
	switch ev.Type {
	case event.ProviderDelta:
		p.text += ev.Message
	case event.ToolCall:
		if p.text != "" {
			p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: p.text})
			p.text = ""
			if p.err != nil {
				return
			}
		}
		p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: ev.ToolID, ToolName: ev.ToolName, ToolArgs: ev.Args})
	case event.ToolResult:
		if p.text != "" {
			p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: p.text})
			p.text = ""
			if p.err != nil {
				return
			}
		}
		p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Tool, Content: ev.Result, ToolCallID: ev.ToolID, ToolName: ev.ToolName})
		if p.err != nil {
			return
		}
		_, p.err = sessiontree.AppendTurnMarker(p.ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, sessiontree.TurnSavePoint, map[string]string{"reason": "tool_result"})
	case event.ContextContinue:
		if p.text != "" {
			p.err = p.thread.appendMessage(p.ctx, p.turnID, session.Message{Role: session.Assistant, Content: p.text})
			p.text = ""
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
