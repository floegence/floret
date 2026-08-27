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

	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/internal/activityview"
	"github.com/floegence/floret/v5/internal/configbridge"
	"github.com/floegence/floret/v5/internal/engine"
	enginecompaction "github.com/floegence/floret/v5/internal/engine/compaction"
	"github.com/floegence/floret/v5/internal/event"
	"github.com/floegence/floret/v5/internal/provider"
	"github.com/floegence/floret/v5/internal/provider/cache"
	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/session/artifact"
	"github.com/floegence/floret/v5/internal/session/compaction"
	"github.com/floegence/floret/v5/internal/session/contextpolicy"
	"github.com/floegence/floret/v5/internal/sessionlifecycle"
	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/observation"
	"github.com/floegence/floret/v5/tools"
)

var ErrJournalInvariant = errors.New("thread journal invariant violated")

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
	options                     Options
	effectFinalizationTimeout   time.Duration
	effectOutcomeFingerprinter  func(tools.Result, session.Message, *artifact.FullOutput) (string, error)
	effectFinalizerRegistration func(error)
	threads                     map[string]*Thread
}

type ResumeOptions struct{}

type RunOptions struct {
	LogicalRequestID            string
	RunID                       string
	TurnID                      string
	AdmittedInputID             string
	AdmissionCommitted          bool
	AdmissionBaseLeafID         string
	PromotedQueueID             string
	PromotionRequestKey         string
	PromotionRequestFingerprint string
	InputRequestFingerprint     string
	Labels                      engine.RunLabels
	TerminalMetadata            map[string]string
	DeadlineMetadata            map[string]string
	CompletionPolicy            engine.CompletionPolicy
	ControlSpec                 engine.ControlSpec
	Reasoning                   provider.ReasoningSelection
	MaxInputTokens              int64
	MaxTotalTokens              int64
	MaxCostUSD                  float64
	MaxToolCalls                int
	MaxLengthContinuations      int
	MaxStopHookContinuations    int
	ManualCompactions           engine.ManualCompactionSource
	ToolSurfaceProvider         engine.ToolSurfaceProvider
	SupplementalContext         []engine.TurnSupplementalContextItem
	Attachments                 []session.MessageAttachment
	References                  []session.MessageReference
	Sink                        event.Sink
	SkipContextPolicyEvent      bool
}

type AcceptedTurn struct {
	ThreadID    string
	TurnID      string
	RunID       string
	UserEntryID string
	BaseLeafID  string
	Replayed    bool
}

type ThreadJournalSnapshot struct {
	Meta    sessiontree.ThreadMeta `json:"meta"`
	Path    []sessiontree.Entry    `json:"path"`
	Entries []sessiontree.Entry    `json:"entries"`
	Context []session.Message      `json:"context"`
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
	Replayed           bool
	AdmissionRunning   bool
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

type Thread struct {
	harness          *AgentHarness
	id               string
	effectFinalizeMu sync.Mutex
	effectFinalizers map[string]func(context.Context, engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error)
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
	Activity        *tools.ActivityPresentation
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
	}
}

func (h *AgentHarness) ResumeThread(ctx context.Context, id string, _ ResumeOptions) (*Thread, error) {
	meta, err := h.options.Repo.Thread(ctx, id)
	if err != nil {
		return nil, err
	}
	return h.threadForResume(meta.ID), nil
}

// ResumeWaitingThread binds a waiting turn for continuation. Its authority is
// validated by ResumeWaitingTurn, so the general whole-thread inspector is
// intentionally skipped while the successor lease is being established.
func (h *AgentHarness) ResumeWaitingThread(ctx context.Context, id string) (*Thread, error) {
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

func (h *AgentHarness) cacheThread(id string) *Thread {
	h.mu.Lock()
	defer h.mu.Unlock()
	if thread, ok := h.threads[id]; ok {
		return thread
	}
	thread := &Thread{harness: h, id: id}
	h.threads[id] = thread
	return thread
}

func (h *AgentHarness) threadForResume(id string) *Thread {
	h.mu.Lock()
	defer h.mu.Unlock()
	if thread, ok := h.threads[id]; ok {
		return thread
	}
	return &Thread{harness: h, id: id}
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
	detail, ok := h.threadDetailEvent(entry, ordinal, false, threadDetailActivityContext{})
	if !ok {
		return
	}
	if detail.Kind == ThreadDetailEventAssistantMessage && detail.Message != nil {
		detail.Message.Content = entry.Message.Content
		detail.Message.Reasoning = entry.Message.Reasoning
		if detail.Metadata != nil {
			delete(detail.Metadata, threadDetailRawOmitted)
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

func (t *Thread) ID() string {
	return t.id
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
	contextMessages, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
	if err != nil {
		return ThreadJournalSnapshot{}, err
	}
	return ThreadJournalSnapshot{
		Meta:    meta,
		Path:    path,
		Entries: entries,
		Context: contextMessages,
	}, nil
}

// ResumeInput continues a canonically resolved input interaction. ThreadRuntime
// owns the active attempt; AgentHarness only performs provider I/O against the
// accepted turn and never acquires a durable execution lease.
func (t *Thread) ResumeInput(ctx context.Context, turnID, waitingRunID, answer string, opts RunOptions) (TurnResult, error) {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return TurnResult{}, errors.New("thread is not initialized")
	}
	turnID = strings.TrimSpace(turnID)
	waitingRunID = strings.TrimSpace(waitingRunID)
	runID := strings.TrimSpace(opts.RunID)
	answer = strings.TrimSpace(answer)
	if turnID == "" || waitingRunID == "" || runID == "" || answer == "" {
		return TurnResult{}, errors.New("waiting turn resume requires turn, run, and answer")
	}
	if opts.SupplementalContext == nil {
		opts.SupplementalContext = []engine.TurnSupplementalContextItem{{
			Kind: "user_answer", Title: "User response to the pending question", Text: answer,
		}}
	}
	opts.TurnID = turnID
	opts.RunID = runID
	opts.AdmissionCommitted = true
	opts.AdmissionBaseLeafID = ""
	opts.SkipContextPolicyEvent = true
	return t.runAccepted(ctx, "", opts, nil)
}

func (t *Thread) ExecuteAccepted(ctx context.Context, accepted AcceptedTurn, input string, opts RunOptions) (TurnResult, error) {
	if accepted.ThreadID != t.id || strings.TrimSpace(accepted.TurnID) == "" || strings.TrimSpace(accepted.RunID) == "" {
		return TurnResult{}, sessiontree.ErrAuthorityCorrupt
	}
	journal, ok := t.harness.options.Repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return TurnResult{}, errors.New("session tree repo does not support canonical turn execution")
	}
	canonical, found, err := journal.ReadAcceptedTurn(ctx, t.id, accepted.TurnID, accepted.RunID)
	if err != nil {
		return TurnResult{}, err
	}
	if !found {
		return TurnResult{}, sessiontree.ErrAuthorityCorrupt
	}
	if canonical.Terminal != nil {
		return t.acceptedTurnReplayResult(ctx, canonical, accepted.TurnID, accepted.RunID)
	}
	opts.TurnID = accepted.TurnID
	opts.RunID = accepted.RunID
	opts.AdmissionCommitted = true
	opts.AdmissionBaseLeafID = canonical.BaseLeafID
	if sourceEntryID := strings.TrimSpace(canonical.TurnStarted.Metadata[sessiontree.RetrySourceEntryIDMetadataKey]); sourceEntryID != "" {
		source, sourceErr := t.harness.options.Repo.Entry(ctx, t.id, sourceEntryID)
		if sourceErr != nil {
			return TurnResult{}, sourceErr
		}
		return t.runAccepted(ctx, "", opts, &source)
	}
	message := canonical.UserMessage.Message
	automaticTitleExecution, titleErr := t.startAutomaticTitle(ctx, accepted.TurnID, accepted.RunID, canonical.UserMessage.ID, message)
	if titleErr != nil {
		persistCtx, cancelPersist := turnFinalizationContext(ctx)
		result, runErr := t.finalizeFailedTurn(persistCtx, accepted.TurnID, accepted.RunID, statusForError(titleErr), titleErr, "automatic_title_begin_error", engine.FailureOriginStorage)
		cancelPersist()
		return result, runErr
	}
	result, runErr := t.runAccepted(ctx, input, opts, nil)
	automaticTitleExecution.FinishMain(automaticTitleWorkerMustJoin(result, runErr))
	return result, runErr
}

func (t *Thread) acceptedTurnReplayResult(ctx context.Context, admission sessiontree.AcceptTurnResult, turnID, runID string) (TurnResult, error) {
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

func automaticTitleWorkerMustJoin(result TurnResult, runErr error) bool {
	return runErr != nil || result.Status == engine.Cancelled || result.Status == engine.Failed
}

func (t *Thread) runAccepted(ctx context.Context, input string, opts RunOptions, retrySource *sessiontree.Entry) (TurnResult, error) {
	turnID := opts.TurnID
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = t.harness.nextID("run")
	}
	if !opts.AdmissionCommitted {
		return TurnResult{}, errors.New("provider execution requires committed turn admission")
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
	engineOptions.LogicalRequestID = strings.TrimSpace(opts.LogicalRequestID)
	engineOptions.ThreadID = t.id
	engineOptions.TurnID = turnID
	engineOptions.TraceID = runID
	engineOptions.PromptScopeID = t.id
	engineOptions.ProviderName = t.harness.options.ProviderName
	engineOptions.Model = t.harness.options.Model
	engineOptions.Labels = opts.Labels
	engineOptions.ContextPolicy = contextpolicy.Normalize(engineOptions.ContextPolicy)
	applyRunOptions(&engineOptions, opts)
	// ThreadRuntime owns approval interactions. Effect dispatch retains
	// only the narrow prepared/dispatching/result safety boundary.
	engineOptions.EffectBatchPreflight = nil
	engineOptions.EffectDispatcher = t.effectDispatcher()
	engineOptions.EffectResultFinalizer = t.finalizeEffectResult
	engineOptions.ProviderRequestGate = nil
	engineOptions.PreviousProviderState = provider.CloneState(previousProviderState)
	if !opts.SkipContextPolicyEvent {
		if err := t.appendContextPolicyEvent(ctx, turnID, runID, engineOptions.ProviderName, engineOptions.Model, engineOptions.ContextPolicy); err != nil {
			persistCtx, cancelPersist := turnFinalizationContext(ctx)
			defer cancelPersist()
			return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "append_context_policy_error", engine.FailureOriginStorage)
		}
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
	result = normalizeCancelledEngineResult(ctx, result)
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
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(projection.err), projection.err, "projection_error", turnProjectionFailureOrigin(projection.err))
	}
	if err := projection.FlushForTurnStatus(result.Status, result.Err); err != nil {
		return t.finalizeFailedTurn(persistCtx, turnID, runID, statusForError(err), err, "projection_flush_error", turnProjectionFailureOrigin(err))
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
	if _, err := t.finishTurn(persistCtx, turnID, runID, terminalEntryID, status, terminalMetadata, failureMessage, stateToSave); err != nil {
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

func normalizeCancelledEngineResult(ctx context.Context, result engine.Result) engine.Result {
	if cancellation := contextCancellationError(ctx); cancellation != nil {
		result.Status = engine.Cancelled
		result.Err = cancellation
		result.FailureOrigin = engine.FailureOriginCancelled
	}
	return result
}

func (t *Thread) finishTurn(ctx context.Context, turnID, runID, terminalEntryID string, status sessiontree.TurnMarkerStatus, metadata map[string]string, failureMessage string, providerState *provider.State) (sessiontree.FinishTurnResult, error) {
	repo, ok := t.harness.options.Repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return sessiontree.FinishTurnResult{}, errors.New("session tree repo does not support canonical turn finish")
	}
	payload, err := json.Marshal(struct {
		ThreadID        string                       `json:"thread_id"`
		TurnID          string                       `json:"turn_id"`
		RunID           string                       `json:"run_id"`
		TerminalEntryID string                       `json:"terminal_entry_id"`
		Status          sessiontree.TurnMarkerStatus `json:"status"`
		Metadata        map[string]string            `json:"metadata,omitempty"`
		FailureMessage  string                       `json:"failure_message,omitempty"`
		ProviderState   *provider.State              `json:"provider_state,omitempty"`
	}{
		ThreadID: t.id, TurnID: strings.TrimSpace(turnID), RunID: strings.TrimSpace(runID),
		TerminalEntryID: strings.TrimSpace(terminalEntryID), Status: status, Metadata: cloneStringMap(metadata),
		FailureMessage: strings.TrimSpace(failureMessage),
		ProviderState:  provider.CloneState(providerState),
	})
	if err != nil {
		return sessiontree.FinishTurnResult{}, err
	}
	request := sessiontree.FinishTurnRequest{
		ThreadID: t.id, TurnID: strings.TrimSpace(turnID), RunID: strings.TrimSpace(runID), TerminalEntryID: strings.TrimSpace(terminalEntryID), Status: status,
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
			State: *provider.CloneState(providerState), CreatedByRunID: strings.TrimSpace(runID), CreatedByTurnID: strings.TrimSpace(turnID), UpdatedAt: t.harness.now(),
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
	if _, finishErr := t.finishTurn(ctx, turnID, runID, terminalTurnEntryID(t.id, turnID, runID), markerForStatus(status), metadata, failureMessage, nil); finishErr != nil {
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
		threadDetailKindKey:       subAgentApprovalEntryKind,
		threadDetailTypeKey:       string(ev.Type),
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
		threadDetailKindKey:   toolDispatchEntryKind,
		threadDetailTypeKey:   string(event.ToolDispatchStarted),
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
		threadDetailKindKey:   toolActivityEntryKind,
		threadDetailTypeKey:   string(event.ToolActivityUpdated),
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
		threadDetailKindKey:      subAgentContextStatusEntryKind,
		threadDetailTypeKey:      subAgentContextStatusEntryKind,
		subAgentContextStatusKey: mustSubAgentMetadataJSON(status),
	}
	entry, err := t.harness.options.Repo.Append(ctx, sessiontree.Entry{
		ThreadID: t.id,
		TurnID:   turnID,
		RunID:    runID,
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
		threadDetailKindKey:          subAgentContextCompactionEntryKind,
		threadDetailTypeKey:          subAgentContextCompactionEntryKind,
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
			RunID:             identity.RunID(ev.RunID),
			ThreadID:          identity.ThreadID(ev.ThreadID),
			TurnID:            identity.TurnID(ev.TurnID),
			Step:              ev.Step,
			RequestID:         stringFromEventMetadata(meta["request_id"]),
			LogicalRequestID:  identity.LogicalRequestID(stringFromEventMetadata(meta["logical_request_id"])),
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
			RunID:            identity.RunID(ev.RunID),
			ThreadID:         identity.ThreadID(ev.ThreadID),
			TurnID:           identity.TurnID(ev.TurnID),
			Step:             ev.Step,
			RequestID:        status.RequestID,
			LogicalRequestID: identity.LogicalRequestID(status.LogicalRequestID),
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
	Entry            sessiontree.Entry
	Source           string
	LogicalRequestID string
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
	logicalRequestIDForTurn := func(turnID string) string {
		for _, entry := range path {
			if entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == turnID && entry.TurnStatus == sessiontree.TurnStarted {
				return strings.TrimSpace(entry.Metadata[sessiontree.LogicalRequestIDMetadataKey])
			}
		}
		return ""
	}
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
						return retryTargetResult{Entry: candidate, Source: "save_point", LogicalRequestID: logicalRequestIDForTurn(candidate.TurnID)}
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
			return retryTargetResult{Entry: path[i], Source: "user", LogicalRequestID: logicalRequestIDForTurn(path[i].TurnID)}
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
	emitMu           sync.Mutex
	mu               sync.Mutex
	text             string
	reasoning        string
	pendingCalls     []pendingToolMessage
	pendingResults   []pendingToolMessage
	pendingBatchSize int
	pendingCallsSent bool
	lastCompaction   event.Event
	activeAttempt    providerAttemptIdentity
	err              error
}

type providerAttemptIdentity struct {
	logicalRequestID string
	attemptID        string
	attemptEpoch     int
}

type turnProjectionContractError struct {
	err error
}

func (failure *turnProjectionContractError) Error() string {
	return failure.err.Error()
}

func (failure *turnProjectionContractError) Unwrap() error {
	return failure.err
}

func newTurnProjectionContractError(message string) error {
	return &turnProjectionContractError{err: errors.New(message)}
}

func newTurnProjectionContractErrorf(format string, args ...any) error {
	return &turnProjectionContractError{err: fmt.Errorf(format, args...)}
}

func turnProjectionFailureOrigin(err error) engine.FailureOrigin {
	var contractFailure *turnProjectionContractError
	if errors.As(err, &contractFailure) {
		return engine.FailureOriginContract
	}
	return engine.FailureOriginStorage
}

func (identity providerAttemptIdentity) empty() bool {
	return identity.logicalRequestID == "" && identity.attemptID == "" && identity.attemptEpoch == 0
}

func (identity providerAttemptIdentity) valid() bool {
	return identity.logicalRequestID != "" && identity.attemptID != "" && identity.attemptEpoch > 0
}

type pendingToolMessage struct {
	message          session.Message
	observedAt       time.Time
	canonicalEntryID string
}

func (p *turnProjection) Emit(ev event.Event) {
	if p == nil {
		return
	}
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	p.mu.Lock()
	if p.err != nil {
		p.mu.Unlock()
		return
	}
	accepted, err := p.acceptProviderAttemptLocked(ev)
	if err != nil {
		p.err = err
		p.mu.Unlock()
		return
	}
	if !accepted {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
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

func (p *turnProjection) acceptProviderAttemptLocked(ev event.Event) (bool, error) {
	tracked := ev.Type == event.ProviderRequest || ev.Type == event.ProviderDelta ||
		ev.Type == event.ProviderReasoning || ev.Type == event.ProviderToolCallStart ||
		ev.Type == event.ProviderToolCallDelta || ev.Type == event.ProviderToolCallEnd
	if !tracked {
		return true, nil
	}
	identity, present := providerAttemptFromMetadata(ev.Metadata)
	if ev.Type == event.ProviderRequest {
		if !present || !identity.valid() {
			return false, newTurnProjectionContractError("provider request requires complete attempt identity")
		}
		if p.activeAttempt.empty() {
			p.activeAttempt = identity
			return true, nil
		}
		if identity.logicalRequestID != p.activeAttempt.logicalRequestID {
			return false, newTurnProjectionContractError("provider attempt identity conflict: logical request changed")
		}
		switch {
		case identity.attemptEpoch < p.activeAttempt.attemptEpoch:
			return false, nil
		case identity.attemptEpoch == p.activeAttempt.attemptEpoch && identity.attemptID != p.activeAttempt.attemptID:
			return false, newTurnProjectionContractError("provider attempt identity conflict: epoch reused by another attempt")
		case identity.attemptEpoch > p.activeAttempt.attemptEpoch:
			if len(p.pendingCalls) > 0 || len(p.pendingResults) > 0 {
				return false, newTurnProjectionContractError("provider attempt superseded with pending canonical tool batch")
			}
			p.text = ""
			p.reasoning = ""
			p.activeAttempt = identity
		}
		return true, nil
	}
	if !present {
		return true, nil
	}
	if !identity.valid() {
		return false, newTurnProjectionContractError("provider event requires complete attempt identity")
	}
	if p.activeAttempt.empty() {
		return false, newTurnProjectionContractError("provider event arrived before attempt activation")
	}
	if identity.logicalRequestID != p.activeAttempt.logicalRequestID {
		return false, newTurnProjectionContractError("provider attempt identity conflict: logical request changed")
	}
	if identity.attemptEpoch < p.activeAttempt.attemptEpoch {
		return false, nil
	}
	if identity.attemptEpoch == p.activeAttempt.attemptEpoch && identity.attemptID != p.activeAttempt.attemptID {
		return false, newTurnProjectionContractError("provider attempt identity conflict: epoch reused by another attempt")
	}
	if identity.attemptEpoch > p.activeAttempt.attemptEpoch {
		return false, newTurnProjectionContractError("provider event arrived for inactive future attempt")
	}
	return true, nil
}

func providerAttemptFromMetadata(metadata any) (providerAttemptIdentity, bool) {
	values, ok := metadata.(map[string]any)
	if !ok {
		return providerAttemptIdentity{}, false
	}
	logical, logicalOK := values["logical_request_id"]
	attemptID, attemptOK := values["attempt_id"]
	epoch, epochOK := values["attempt_epoch"]
	if !logicalOK && !attemptOK && !epochOK {
		return providerAttemptIdentity{}, false
	}
	logicalString, _ := logical.(string)
	attemptString, _ := attemptID.(string)
	return providerAttemptIdentity{
		logicalRequestID: strings.TrimSpace(logicalString),
		attemptID:        strings.TrimSpace(attemptString),
		attemptEpoch:     intFromEventMetadata(epoch),
	}, true
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
				return newTurnProjectionContractError("tool call batch contains empty tool_call_id")
			}
			if _, ok := seenCalls[call.message.ToolCallID]; ok {
				return newTurnProjectionContractErrorf("tool call batch contains duplicate tool_call_id %q", call.message.ToolCallID)
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
			return newTurnProjectionContractError("tool result batch contains empty tool_call_id")
		}
		if _, ok := byID[result.message.ToolCallID]; ok {
			return newTurnProjectionContractErrorf("tool result batch contains duplicate tool_call_id %q", result.message.ToolCallID)
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
		return newTurnProjectionContractErrorf("tool result batch references unknown tool_call_id %q", id)
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
	activity = activityview.WithTerminalStatus(activity, resultStatus, text)
	return session.Message{
		Role:       session.Tool,
		Content:    text,
		ToolCallID: strings.TrimSpace(call.ToolCallID),
		ToolName:   strings.TrimSpace(call.ToolName),
		ToolResult: &session.ToolResultView{Status: resultStatus},
		Activity:   activity,
	}
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
				return newTurnProjectionContractErrorf("incomplete tool call batch: %d calls, want %d", len(p.pendingCalls), size)
			}
			return nil
		}
		seenCalls := make(map[string]struct{}, len(p.pendingCalls))
		for _, call := range p.pendingCalls {
			if call.message.ToolCallID == "" {
				return newTurnProjectionContractError("tool call batch contains empty tool_call_id")
			}
			if _, ok := seenCalls[call.message.ToolCallID]; ok {
				return newTurnProjectionContractErrorf("tool call batch contains duplicate tool_call_id %q", call.message.ToolCallID)
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
			return newTurnProjectionContractError("tool result batch contains empty tool_call_id")
		}
		if _, ok := byID[result.message.ToolCallID]; ok {
			return newTurnProjectionContractErrorf("tool result batch contains duplicate tool_call_id %q", result.message.ToolCallID)
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
		canonical, committed, err := p.canonicalEffectResult(result.message.ToolCallID, result.message.ToolName, result.canonicalEntryID)
		if err != nil {
			return err
		}
		if committed {
			result.message = canonical
		} else {
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
			return newTurnProjectionContractErrorf("tool result batch references unknown tool_call_id %q", id)
		}
	}
	remainingCalls := p.pendingCalls[appendable:]
	if force && len(remainingCalls) > 0 {
		return newTurnProjectionContractErrorf("incomplete tool result batch: %d calls, %d results", len(remainingCalls), len(byID))
	}
	if appendedResult && len(remainingCalls) == 0 {
		if entry, err := sessiontree.AppendTurnMarker(p.ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, sessiontree.TurnSavePoint, map[string]string{"reason": "tool_result_batch", "run_id": p.runID}); err != nil {
			return fmt.Errorf("canonical tool result save point: %w", err)
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

func (p *turnProjection) canonicalEffectResult(toolCallID, toolName, canonicalEntryID string) (session.Message, bool, error) {
	canonicalEntryID = strings.TrimSpace(canonicalEntryID)
	if canonicalEntryID == "" {
		return session.Message{}, false, nil
	}
	entry, err := p.thread.harness.options.Repo.Entry(p.ctx, p.thread.id, canonicalEntryID)
	if err != nil {
		if errors.Is(err, sessiontree.ErrEntryNotFound) || errors.Is(err, sessiontree.ErrThreadNotFound) {
			return session.Message{}, false, fmt.Errorf("canonical effect result lookup: %w", sessiontree.ErrAuthorityCorrupt)
		}
		return session.Message{}, false, fmt.Errorf("canonical effect result lookup: %w", err)
	}
	if entry.ID != canonicalEntryID || entry.ThreadID != p.thread.id || entry.TurnID != p.turnID ||
		entry.Type != sessiontree.EntryToolResult || entry.Message.Role != session.Tool ||
		entry.Message.ToolCallID != toolCallID || entry.Message.ToolName != toolName ||
		entry.Message.ToolResult == nil || strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) == "" {
		return session.Message{}, false, fmt.Errorf("canonical effect result identity: %w", sessiontree.ErrAuthorityCorrupt)
	}
	if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
		return session.Message{}, false, fmt.Errorf("canonical effect result integrity: %w", err)
	}
	return session.CloneMessage(entry.Message), true, nil
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
	if metadataString(values, "outcome") == tools.ResultOutcomeDeclined || metadataString(values, "tool_result_status") == string(observation.ActivityStatusDeclined) {
		return string(observation.ActivityStatusDeclined)
	}
	if metadataBool(values, "pending_tool_result") {
		return string(observation.ActivityStatusRunning)
	}
	if strings.TrimSpace(ev.Err) != "" || metadataBool(values, "error_present") {
		return string(observation.ActivityStatusError)
	}
	return string(observation.ActivityStatusSuccess)
}

func sanitizeActivityPresentation(activity *tools.ActivityPresentation) *tools.ActivityPresentation {
	return event.Sanitize(event.Event{Activity: activity}).Activity
}

func sessionActivityPresentation(in *tools.ActivityPresentation) *session.ActivityPresentation {
	return session.CloneActivityPresentation(in)
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
