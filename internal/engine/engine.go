package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/control"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/memory"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

var (
	ErrNoProgress                 = errors.New("agent loop made no progress")
	ErrDuplicateTools             = errors.New("agent loop repeated identical tool calls")
	ErrDuplicateToolCallID        = errors.New("provider returned duplicate tool call id")
	ErrProviderTruncated          = errors.New("provider output was truncated")
	ErrContentFiltered            = errors.New("provider output was content filtered")
	ErrProviderFinishError        = errors.New("provider returned error finish reason")
	ErrStopHookLoop               = errors.New("stop hook requested too many continuations")
	ErrInvalidTokenEstimate       = errors.New("provider token estimate missing source or method")
	ErrCompactedRequestOverBudget = errors.New("compacted provider request still exceeds context budget")
	ErrFixedContextOverBudget     = errors.New("provider request fixed context overhead exceeds context budget")
	ErrCompactionNoop             = errors.New("context compaction is not needed")
)

type Status string

const (
	Completed Status = "completed"
	Waiting   Status = "waiting"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
)

type CompletionPolicy string

const (
	CompletionNaturalStop    CompletionPolicy = "natural_stop"
	CompletionExplicitSignal CompletionPolicy = "explicit_signal"
)

const (
	terminalCloseGrace                = 250 * time.Millisecond
	maxCompactionConvergenceAttempts  = 4
	compactionConvergenceSafetyTokens = 256
)

type CompletionReason string

const (
	CompletionReasonNaturalStop CompletionReason = "natural_stop"
	CompletionReasonToolSignal  CompletionReason = "tool_signal"
	CompletionReasonHookStop    CompletionReason = "hook_stop"
)

type ContinuationReason string

const (
	ContinueToolResults       ContinuationReason = "tool_results"
	ContinueCompaction        ContinuationReason = "compaction"
	ContinueProviderTruncated ContinuationReason = "provider_truncated"
	ContinueRetryEmpty        ContinuationReason = "retry_empty"
	ContinueNoProgress        ContinuationReason = "no_progress"
	ContinueHook              ContinuationReason = "hook"
)

type StopHook func(context.Context, StopHookContext) (StopHookResult, error)

type StopHookContext struct {
	RunID         string
	ThreadID      string
	TurnID        string
	TraceID       string
	PromptScopeID string
	Step          int

	LastAssistant   session.Message
	Messages        []session.Message
	FinishReason    provider.FinishReason
	RawFinishReason string
	FinishInferred  bool
	Metrics         RunMetrics
}

type StopHookResult struct {
	Continue bool
	Prompt   string
	Reason   string
}

type ToolSurfaceRequest struct {
	RunID         string
	ThreadID      string
	TurnID        string
	TraceID       string
	PromptScopeID string
	Step          int
	Phase         string
	Labels        RunLabels
	HostContext   map[string]string
}

type ToolSurface struct {
	Tools                 *tools.Registry
	ToolDefinitions       []provider.ToolDefinition
	HostedToolDefinitions []provider.HostedToolDefinition
	SystemPrompt          string
	HostContext           map[string]string
	Epoch                 string
	Reason                string
}

type ToolSurfaceProvider func(context.Context, ToolSurfaceRequest) (ToolSurface, error)

type ExecutionIdentity struct {
	RunID         string
	ThreadID      string
	TurnID        string
	TraceID       string
	PromptScopeID string
}

type Options struct {
	RunID                    string
	ThreadID                 string
	TurnID                   string
	TraceID                  string
	PromptScopeID            string
	ProviderName             string
	Model                    string
	Labels                   RunLabels
	CacheNamespace           string
	CacheRetention           cache.Retention
	ContextPolicy            contextpolicy.Policy
	Reasoning                provider.ReasoningSelection
	MaxEmptyProviderRetries  int
	NoProgressLimit          int
	DuplicateToolLimit       int
	WallTime                 time.Duration
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	HostedToolDefinitions    []provider.HostedToolDefinition
	CompletionPolicy         CompletionPolicy
	ControlSpec              ControlSpec
	PreviousProviderState    *provider.State
	MaxLengthContinuations   int
	MaxStopHookContinuations int
	ManualCompactions        ManualCompactionSource
	ToolSurfaceProvider      ToolSurfaceProvider

	toolDefinitions []provider.ToolDefinition
	toolSurface     resolvedToolSurface
}

type resolvedToolSurface struct {
	tools                 *tools.Registry
	toolDefinitions       []provider.ToolDefinition
	hostedToolDefinitions []provider.HostedToolDefinition
	systemPrompt          string
	hostContext           map[string]string
	epoch                 string
	reason                string
}

type RunLabels struct {
	Correlation map[string]string
	Host        map[string]string
}

type compactedRequestValidation struct {
	RequestEstimate      contextpolicy.RequestEstimate
	ContextPressure      contextpolicy.ContextPressure
	MessageContextUsage  contextpolicy.Usage
	FixedInputTokens     int64
	ReducibleInputTokens int64
	RequestSafeLimit     int64
}

type compactionBudgetPlan struct {
	SummaryTokens     int64
	RecentTailTokens  int64
	RecentUserTokens  int64
	RequestSafeLimit  int64
	FixedInputTokens  int64
	TargetInputTokens int64
}

type Result struct {
	Status             Status
	Output             string
	Err                error
	Metrics            RunMetrics
	Messages           []session.Message
	CompletionReason   CompletionReason
	ContinuationReason ContinuationReason
	FinishReason       provider.FinishReason
	RawFinishReason    string
	FinishInferred     bool
	ControlSignal      *ControlSignal
	ProviderState      *provider.State
}

type ContextCompactionResult struct {
	Status        Status
	Err           error
	Metrics       RunMetrics
	Messages      []session.Message
	Compaction    compaction.Result
	ProviderState *provider.State
}

type RunDecision struct {
	CompletionReason   CompletionReason
	ContinuationReason ContinuationReason
	FinishReason       provider.FinishReason
	RawFinishReason    string
	FinishInferred     bool
	Detail             string
	ControlSignal      *ControlSignal
	ProviderState      *provider.State
	Metadata           map[string]any
}

type StepOutput struct {
	Text            string
	Reasoning       string
	Calls           []provider.ToolCall
	Usage           provider.Usage
	ResponseID      string
	Retry           bool
	Truncated       bool
	FinishReason    provider.FinishReason
	RawFinishReason string
	FinishInferred  bool
	ResponseState   *provider.State
}

type RunInput struct {
	RunID                 string
	ThreadID              string
	TurnID                string
	TraceID               string
	PromptScopeID         string
	Labels                RunLabels
	PreviousProviderState *provider.State
	History               []session.Message
}

type CompactionManager interface {
	Compact(context.Context, CompactionRequest) (compaction.Result, []session.Message, error)
}

type CompactionCommitter interface {
	CommitCompaction(context.Context, CompactionCommitRequest) (compaction.Result, []session.Message, error)
}

type CompactionCommitRequest struct {
	CompactionRequest
	Result         compaction.Result
	ActiveMessages []session.Message
}

type committedCompaction struct {
	Result         compaction.Result
	ActiveMessages []session.Message
}

type CompactionRequest struct {
	RunID                string
	ThreadID             string
	TurnID               string
	TraceID              string
	PromptScopeID        string
	Step                 int
	History              []session.Message
	Policy               contextpolicy.Policy
	Trigger              compaction.Trigger
	Reason               compaction.Reason
	Phase                compaction.Phase
	Provider             provider.Provider
	ProviderName         string
	Model                string
	PreviousCompactionID string
	PreviousSummary      string
	ContextUsage         contextpolicy.Usage
	Details              map[string]string
}

type ManualCompactionSource interface {
	PollManualCompaction(context.Context, ManualCompactionPollRequest) (ManualCompactionRequest, bool, error)
}

type ManualCompactionPollRequest struct {
	RunID         string
	ThreadID      string
	TurnID        string
	TraceID       string
	PromptScopeID string
	Step          int
}

type ManualCompactionRequest struct {
	RequestID string
	Source    string
}

type Engine struct {
	provider  provider.Provider
	tools     *tools.Registry
	store     session.TranscriptStore
	prompt    cache.Store
	artifacts artifact.Store
	memory    *memory.Manager
	sink      event.Sink
	approver  tools.Approver
	stopHook  StopHook
	compactor CompactionManager
	options   Options
}

type turnState struct {
	activeMessages []session.Message
}

type Config struct {
	Provider provider.Provider
	Tools    *tools.Registry
	Store    session.TranscriptStore
	Prompt   cache.Store
	// Artifacts is required when a tool output policy preserves truncated full
	// output. Runtime and harness callers provide a default store; direct engine
	// callers may leave it nil only if their tools do not need preservation.
	Artifacts    artifact.Store
	SystemPrompt string
	Sink         event.Sink
	Approver     tools.Approver
	StopHook     StopHook
	Compactor    CompactionManager
	Options      Options
}

func New(cfg Config) (*Engine, error) {
	if cfg.Provider == nil {
		return nil, errors.New("provider is required")
	}
	if cfg.Store == nil {
		cfg.Store = session.NewMemoryStore()
	}
	if cfg.Prompt == nil {
		cfg.Prompt = cache.NewMemoryStore()
	}
	mem := &memory.Manager{SystemPrompt: cfg.SystemPrompt}
	if cfg.Tools == nil {
		cfg.Tools = tools.NewRegistry()
	}
	cfg.Options = normalizeOptions(cfg.Options)
	cfg.Options.toolDefinitions = providerToolDefinitionsFromTools(cfg.Tools.Definitions())
	if err := validateConfiguredTools(cfg.Options.toolDefinitions, cfg.Options.HostedToolDefinitions, false); err != nil {
		return nil, err
	}
	return &Engine{
		provider:  cfg.Provider,
		tools:     cfg.Tools,
		store:     cfg.Store,
		prompt:    cfg.Prompt,
		artifacts: cfg.Artifacts,
		memory:    mem,
		sink:      cfg.Sink,
		approver:  cfg.Approver,
		stopHook:  cfg.StopHook,
		compactor: cfg.Compactor,
		options:   cloneOptions(cfg.Options),
	}, nil
}

func (e *Engine) Options() Options {
	if e == nil {
		return Options{}
	}
	return cloneOptions(e.options)
}

func (e *Engine) WithOptions(options Options) (*Engine, error) {
	if e == nil {
		return nil, errors.New("engine is required")
	}
	options = normalizeOptions(options)
	options.toolDefinitions = registryDefinitions(e.tools)
	if err := validateConfiguredTools(options.toolDefinitions, options.HostedToolDefinitions, false); err != nil {
		return nil, err
	}
	next := *e
	next.options = cloneOptions(options)
	return &next, nil
}

// SetSink replaces the event sink for subsequent runs. It is a host wiring
// hook and must not be called concurrently with an active Run or RunTurn.
func (e *Engine) SetSink(sink event.Sink) {
	if e == nil {
		return
	}
	e.sink = sink
}

// SetApprover replaces the tool approver for subsequent runs. It is a host
// wiring hook and must not be called concurrently with an active Run or RunTurn.
func (e *Engine) SetApprover(approver tools.Approver) {
	if e == nil {
		return
	}
	e.approver = approver
}

// SetStopHook replaces the stop hook for subsequent runs. It is a host wiring
// hook and must not be called concurrently with an active Run or RunTurn.
func (e *Engine) SetStopHook(hook StopHook) {
	if e == nil {
		return
	}
	e.stopHook = hook
}

type LocalCompactionManager struct {
	Generator compaction.SummaryGenerator
	Now       func() time.Time
}

func (m LocalCompactionManager) Compact(ctx context.Context, req CompactionRequest) (compaction.Result, []session.Message, error) {
	if m.Generator == nil {
		return compaction.Result{}, nil, errors.New("local compaction manager requires summary generator")
	}
	now := time.Now()
	if m.Now != nil {
		now = m.Now()
	}
	prep, err := compaction.Prepare(ctx, compaction.Request{
		CompactionID:         "",
		PreviousCompactionID: req.PreviousCompactionID,
		PreviousSummary:      req.PreviousSummary,
		History:              req.History,
		Policy:               req.Policy,
		Trigger:              req.Trigger,
		Reason:               req.Reason,
		Phase:                req.Phase,
		Step:                 req.Step,
		Details:              req.Details,
		Now:                  now,
	}, m.Generator)
	if err != nil {
		return compaction.Result{}, nil, err
	}
	return prep.Result, prep.ActiveMessages, nil
}

func (e *Engine) Run(ctx context.Context, userText string) Result {
	runner, err := e.runner(e.store, e.options)
	if err != nil {
		return Result{Status: Failed, Err: err}
	}
	return runner.run(ctx, userText)
}

func (e *Engine) RunTurn(ctx context.Context, input RunInput) Result {
	store := session.NewMemoryStore()
	opts := e.options
	if input.RunID != "" {
		opts.RunID = input.RunID
	}
	if input.ThreadID != "" {
		opts.ThreadID = input.ThreadID
	}
	if input.TurnID != "" {
		opts.TurnID = input.TurnID
	}
	if input.TraceID != "" {
		opts.TraceID = input.TraceID
	}
	if input.PromptScopeID != "" {
		opts.PromptScopeID = input.PromptScopeID
	}
	if !input.Labels.isZero() {
		opts.Labels = cloneRunLabels(input.Labels)
	}
	if input.PreviousProviderState != nil {
		opts.PreviousProviderState = provider.CloneState(input.PreviousProviderState)
	}
	opts = normalizeOptions(opts)
	if len(input.History) > 0 {
		history := make([]session.Message, len(input.History))
		for i, msg := range input.History {
			history[i] = stableMessageAt(opts.RunID, i, msg)
		}
		if err := store.AppendTranscript(opts.RunID, history...); err != nil {
			return Result{Status: Failed, Err: err}
		}
	}
	runner, err := e.runner(store, opts)
	if err != nil {
		return Result{Status: Failed, Err: err}
	}
	return runner.run(ctx, "")
}

func (e *Engine) CompactContext(ctx context.Context, input RunInput, manual ManualCompactionRequest) ContextCompactionResult {
	store := session.NewMemoryStore()
	opts := e.options
	if input.RunID != "" {
		opts.RunID = input.RunID
	}
	if input.ThreadID != "" {
		opts.ThreadID = input.ThreadID
	}
	if input.TurnID != "" {
		opts.TurnID = input.TurnID
	}
	if input.TraceID != "" {
		opts.TraceID = input.TraceID
	}
	if input.PromptScopeID != "" {
		opts.PromptScopeID = input.PromptScopeID
	}
	if !input.Labels.isZero() {
		opts.Labels = cloneRunLabels(input.Labels)
	}
	if input.PreviousProviderState != nil {
		opts.PreviousProviderState = provider.CloneState(input.PreviousProviderState)
	}
	opts = normalizeOptions(opts)
	if input.TurnID == "" {
		opts.TurnID = ""
	}
	if len(input.History) > 0 {
		history := make([]session.Message, len(input.History))
		for i, msg := range input.History {
			history[i] = stableMessageAt(opts.RunID, i, msg)
		}
		if err := store.AppendTranscript(opts.RunID, history...); err != nil {
			return ContextCompactionResult{Status: Failed, Err: err}
		}
	}
	runner, err := e.runner(store, opts)
	if err != nil {
		return ContextCompactionResult{Status: Failed, Err: err}
	}
	return runner.compactContext(ctx, manual)
}

func (e *Engine) runner(store session.TranscriptStore, opts Options) (*Engine, error) {
	if e.provider == nil {
		return nil, errors.New("provider is required")
	}
	if store == nil {
		store = session.NewMemoryStore()
	}
	prompt := e.prompt
	if prompt == nil {
		prompt = cache.NewMemoryStore()
	}
	mem := e.memory
	if mem == nil {
		mem = &memory.Manager{}
	}
	registry := e.tools
	if registry == nil {
		registry = tools.NewRegistry()
	}
	opts = normalizeOptions(opts)
	opts.toolDefinitions = providerToolDefinitionsFromTools(registry.Definitions())
	if err := validateConfiguredTools(opts.toolDefinitions, opts.HostedToolDefinitions, false); err != nil {
		return nil, err
	}
	return &Engine{
		provider:  e.provider,
		tools:     registry,
		store:     store,
		prompt:    prompt,
		artifacts: e.artifacts,
		memory:    mem,
		sink:      e.sink,
		approver:  e.approver,
		stopHook:  e.stopHook,
		compactor: e.compactor,
		options:   cloneOptions(opts),
	}, nil
}

func (e *Engine) run(ctx context.Context, userText string) Result {
	state := &turnState{}
	opts := normalizeOptions(e.options)
	if opts.WallTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.WallTime)
		defer cancel()
	}
	var err error
	opts, err = e.resolveToolSurface(ctx, opts, 0, "init")
	if err != nil {
		return Result{Status: Failed, Err: err}
	}
	if err := validateConfiguredTools(opts.toolDefinitions, opts.HostedToolDefinitions, false); err != nil {
		return Result{Status: Failed, Err: err}
	}
	latestProviderState := provider.CloneState(opts.PreviousProviderState)
	if userText != "" {
		msg := e.stableMessage(opts.RunID, session.Message{Role: session.User, Content: userText})
		if err := e.store.AppendTranscript(opts.RunID, msg); err != nil {
			return Result{Status: Failed, Err: err}
		}
	}
	var output string
	emptyRetries := 0
	noProgress := 0
	lengthContinuations := 0
	stopHookContinuations := 0
	compactionFailures := 0
	lastToolSig := ""
	duplicateCount := 0
	started := time.Now()
	metrics := RunMetrics{}
	activeHistory, err := e.store.Transcript(opts.RunID)
	if err != nil {
		return Result{Status: Failed, Err: err}
	}
	state.activeMessages = append([]session.Message(nil), activeHistory...)
	pressureTracker := NewContextPressureTracker(opts.PromptScopeID)
	if anchor, ok, err := e.prompt.LatestPressureAnchor(ctx, opts.PromptScopeID, opts.ProviderName, opts.Model); err != nil {
		return Result{Status: Failed, Err: err}
	} else if ok {
		pressureTracker.SetAnchor(anchor)
	}
	for step := 1; ; step++ {
		opts.PreviousProviderState = provider.CloneState(latestProviderState)
		if ctx.Err() != nil {
			return e.end(state, opts, step, Cancelled, output, ctx.Err(), metrics, started, RunDecision{ProviderState: latestProviderState})
		}
		metrics.Steps = step
		e.emit(opts, event.Event{Type: event.StepStart, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model})
		opts, err = e.resolveToolSurface(ctx, opts, step, "provider_request")
		if err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		if err := validateConfiguredTools(opts.toolDefinitions, opts.HostedToolDefinitions, false); err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		req, preparedHistory, compacted, err := e.prepareOrdinaryRequest(ctx, opts, step, activeHistory, pressureTracker, &metrics, &compactionFailures)
		if err != nil {
			if isContextCancellation(err) {
				return e.end(state, opts, step, Cancelled, output, err, metrics, started, RunDecision{})
			}
			return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		activeHistory = preparedHistory
		state.activeMessages = append([]session.Message(nil), activeHistory...)
		if compacted {
			noProgress = 0
			duplicateCount = 0
		}
		var stepOutput StepOutput
		var providerLatency int64
		var overflowCompacted bool
		req, activeHistory, stepOutput, providerLatency, overflowCompacted, err = e.sendOrdinaryProviderRequest(ctx, opts, step, req, activeHistory, pressureTracker, &metrics, &compactionFailures)
		state.activeMessages = append([]session.Message(nil), activeHistory...)
		if overflowCompacted {
			noProgress = 0
			duplicateCount = 0
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return e.end(state, opts, step, Cancelled, output, err, metrics, started, RunDecision{})
			}
			return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		stepText := stepOutput.Text
		calls := stepOutput.Calls
		usage := stepOutput.Usage
		metrics.AddUsage(usage)
		normalizedUsage := usage.Normalized()
		nativePressure, pressureAnchor := pressureTracker.ObserveSuccess(req, activeHistory, usage)
		_ = e.prompt.AppendProviderResponse(ctx, cache.ProviderResponseRecord{
			RequestID:          fmt.Sprintf("%s:req:%d", opts.RunID, step),
			PromptScopeID:      opts.PromptScopeID,
			RunID:              opts.RunID,
			ThreadID:           opts.ThreadID,
			TurnID:             opts.TurnID,
			ProviderResponseID: stepOutput.ResponseID,
			InputTokens:        normalizedUsage.InputTokens,
			WindowInputTokens:  normalizedUsage.WindowInputTokens,
			OutputTokens:       normalizedUsage.OutputTokens,
			ReasoningTokens:    normalizedUsage.ReasoningTokens,
			CacheReadTokens:    normalizedUsage.CacheReadTokens,
			CacheWriteTokens:   normalizedUsage.CacheWriteTokens,
			TotalTokens:        normalizedUsage.TotalTokens,
			UsageSource:        string(normalizedUsage.Source),
			UsageAvailable:     normalizedUsage.Available,
			NativePressure:     nativePressure,
			PressureAnchor:     pressureAnchor,
			CreatedAt:          time.Now(),
		})
		e.emit(opts, event.Event{Type: event.ProviderUsage, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Metrics: normalizedUsage, Metadata: providerUsageContextStatus(req, normalizedUsage, nativePressure)})
		if stepOutput.ResponseState != nil {
			latestProviderState = provider.CloneState(stepOutput.ResponseState)
		}
		decision := RunDecision{FinishReason: stepOutput.FinishReason, RawFinishReason: stepOutput.RawFinishReason, FinishInferred: stepOutput.FinishInferred, ProviderState: provider.CloneState(latestProviderState)}
		if stepOutput.FinishReason != "" {
			e.emit(opts, event.Event{Type: event.ProviderFinish, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, FinishReason: string(stepOutput.FinishReason), RawFinishReason: stepOutput.RawFinishReason, FinishInferred: stepOutput.FinishInferred})
		}
		if budgetErr := e.checkBudget(opts, metrics, step); budgetErr != nil {
			return e.end(state, opts, step, Failed, output, budgetErr, metrics, started, decision)
		}
		if stepOutput.FinishReason == provider.FinishContentFilter {
			return e.end(state, opts, step, Failed, output, ErrContentFiltered, metrics, started, decision)
		}
		if stepOutput.FinishReason == provider.FinishError {
			return e.end(state, opts, step, Failed, output, ErrProviderFinishError, metrics, started, decision)
		}
		if stepOutput.FinishReason == provider.FinishCancelled {
			return e.end(state, opts, step, Cancelled, output, context.Canceled, metrics, started, decision)
		}
		stepReasoning := stepOutput.Reasoning
		if stepText != "" {
			output += stepText
			noProgress = 0
			msg := e.stableMessage(opts.RunID, session.Message{Role: session.Assistant, Content: stepText, Reasoning: stepReasoning})
			if err := e.store.AppendTranscript(opts.RunID, msg); err != nil {
				return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
			}
			activeHistory = append(activeHistory, msg)
			state.activeMessages = append([]session.Message(nil), activeHistory...)
		}
		if stepOutput.Truncated || stepOutput.FinishReason == provider.FinishLength {
			lengthContinuations++
			if lengthContinuations > opts.MaxLengthContinuations {
				return e.end(state, opts, step, Failed, output, ErrProviderTruncated, metrics, started, decision)
			}
			state.activeMessages = append([]session.Message(nil), activeHistory...)
			decision.ContinuationReason = ContinueProviderTruncated
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			continue
		}
		if stepOutput.Retry {
			emptyRetries++
			if emptyRetries > opts.MaxEmptyProviderRetries {
				return e.end(state, opts, step, Failed, output, errors.New("provider returned empty output"), metrics, started, decision)
			}
			metrics.Retries++
			e.emit(opts, event.Event{Type: event.ProviderRetry, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: "empty provider output"})
			decision.ContinuationReason = ContinueRetryEmpty
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			continue
		}
		emptyRetries = 0
		lengthContinuations = 0
		if stepText == "" && len(calls) == 0 {
			noProgress++
			if noProgress >= opts.NoProgressLimit {
				return e.end(state, opts, step, Failed, output, ErrNoProgress, metrics, started, decision)
			}
		}
		if len(calls) == 0 {
			if stepText != "" && provider.IsTerminalNaturalFinish(stepOutput.FinishReason) {
				hook, err := e.applyStopHook(ctx, opts, step, session.Message{Role: session.Assistant, Content: stepText, Reasoning: stepReasoning}, metrics, stepOutput)
				if err != nil {
					return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
				}
				if hook.Continue {
					stopHookContinuations++
					if stopHookContinuations > opts.MaxStopHookContinuations {
						return e.end(state, opts, step, Failed, output, ErrStopHookLoop, metrics, started, decision)
					}
					prompt := strings.TrimSpace(hook.Prompt)
					if prompt == "" {
						prompt = "Continue the task and address the remaining pending work."
					}
					msg := e.stableMessage(opts.RunID, session.Message{Role: session.User, Content: prompt})
					if err := e.store.AppendTranscript(opts.RunID, msg); err != nil {
						return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
					}
					activeHistory = append(activeHistory, msg)
					state.activeMessages = append([]session.Message(nil), activeHistory...)
					decision.ContinuationReason = ContinueHook
					decision.Detail = strings.TrimSpace(hook.Reason)
					e.emit(opts, event.Event{Type: event.ContextContinue, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: prompt, ContinuationReason: string(ContinueHook), Result: decision.Detail})
					e.emitStepEnd(opts, step, providerLatency, 0, usage, 0, decision)
					continue
				}
				decision.CompletionReason = CompletionReasonNaturalStop
				e.emitStepEnd(opts, step, providerLatency, 0, usage, 0, decision)
				return e.end(state, opts, step, Completed, output, nil, metrics, started, decision)
			}
			decision.ContinuationReason = ContinueNoProgress
			e.emitStepEnd(opts, step, providerLatency, 0, usage, 0, decision)
			continue
		}
		if err := validateToolCalls(calls); err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
		}
		classifiedCalls := classifyToolCalls(opts.ControlSpec, calls)
		controlCalls := classifiedCalls.Control
		if len(classifiedCalls.Ordinary) > 0 {
			calls = classifiedCalls.Ordinary
			if len(controlCalls) > 0 {
				decision.Metadata = deferredControlMetadata(controlCalls)
			}
		}
		for _, call := range calls {
			reasoning := call.Reasoning
			if reasoning == "" {
				reasoning = stepReasoning
			}
			msg := e.stableMessage(opts.RunID, session.Message{Role: session.Assistant, Content: "tool_call", Reasoning: reasoning, ToolCallID: call.ID, ToolName: call.Name, ToolArgs: call.Args})
			if err := e.store.AppendTranscript(opts.RunID, msg); err != nil {
				return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
			}
			activeHistory = append(activeHistory, msg)
			state.activeMessages = append([]session.Message(nil), activeHistory...)
		}
		sig := toolSignature(calls)
		if sig == lastToolSig {
			duplicateCount++
			if duplicateCount >= opts.DuplicateToolLimit {
				return e.end(state, opts, step, Failed, output, ErrDuplicateTools, metrics, started, decision)
			}
		} else {
			lastToolSig = sig
			duplicateCount = 0
		}
		if len(classifiedCalls.Ordinary) == 0 {
			if signal, ok, err := controlSignal(opts.ControlSpec, controlCalls, controlProjectionContext{
				StepText: stepText,
			}); err != nil {
				return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
			} else if ok {
				if len(signal.Labels) == 0 {
					signal.Labels = observabilityLabels(opts.Labels)
				}
				decision.ControlSignal = signal
				e.emitControlSignal(opts, step, signal)
				switch signal.Disposition {
				case ControlTerminal:
					decision.CompletionReason = CompletionReasonToolSignal
					e.emitStepEnd(opts, step, providerLatency, 0, usage, len(controlCalls), decision)
					return e.end(state, opts, step, Completed, signal.OutputText, nil, metrics, started, decision)
				case ControlWaiting:
					decision.CompletionReason = CompletionReasonToolSignal
					e.emitStepEnd(opts, step, providerLatency, 0, usage, len(controlCalls), decision)
					return e.end(state, opts, step, Waiting, signal.OutputText, nil, metrics, started, decision)
				case ControlContinue:
					msg := e.stableMessage(opts.RunID, session.Message{Role: session.Tool, Content: signal.OutputText, ToolCallID: signal.CallID, ToolName: signal.Name})
					if err := e.store.AppendTranscript(opts.RunID, msg); err != nil {
						return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
					}
					activeHistory = append(activeHistory, msg)
					state.activeMessages = append([]session.Message(nil), activeHistory...)
					decision.ContinuationReason = ContinueToolResults
					e.emitStepEnd(opts, step, providerLatency, 0, usage, len(controlCalls), decision)
					continue
				}
			}
		}
		metrics.ToolCalls += len(calls)
		if budgetErr := e.checkBudget(opts, metrics, step); budgetErr != nil {
			return e.end(state, opts, step, Failed, output, budgetErr, metrics, started, decision)
		}
		opts, err = e.resolveToolSurface(ctx, opts, step, "tool_dispatch")
		if err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
		}
		if err := validateConfiguredTools(opts.toolDefinitions, opts.HostedToolDefinitions, false); err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
		}
		activeToolRegistry := opts.toolSurface.tools
		if activeToolRegistry == nil {
			activeToolRegistry = e.tools
		}
		if activeToolRegistry == nil {
			activeToolRegistry = tools.NewRegistry()
		}
		toolRunOptions := tools.RunOptions{
			RunID:         opts.RunID,
			ThreadID:      opts.ThreadID,
			TurnID:        opts.TurnID,
			PromptScopeID: opts.PromptScopeID,
			Step:          step,
			Labels:        observabilityLabels(opts.Labels),
			HostContext:   opts.toolSurface.hostContext,
		}
		for i, call := range calls {
			activity, activityErr := activeToolRegistry.ActivityForCall(toolCall(call), toolRunOptions)
			if activityErr != nil {
				activity = nil
			}
			e.emit(opts, event.Event{Type: event.ToolCall, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: call.ID, ToolName: call.Name, ToolKind: "local", Args: call.Args, Activity: activity, Metadata: map[string]any{"batch_index": i, "batch_size": len(calls)}})
		}
		toolStarted := time.Now()
		results := activeToolRegistry.RunBatchWithOptions(ctx, toolCalls(calls), e.approverWithEvents(opts, step), toolRunOptions)
		toolLatency := time.Since(toolStarted).Milliseconds()
		for i, result := range results {
			result = preparePendingToolResult(result)
			projection := exactToolOutputProjection(result.Text)
			if result.Pending == nil {
				policy := tools.MergeOutputPolicy(activeToolRegistry.OutputPolicyFor(result.Name), result.OutputPolicy)
				var err error
				projection, err = tools.BuildOutputProjection(ctx, toolOutputArtifactResult(result, opts, step), policy, toolArtifactStoreFor(e.artifacts))
				if err != nil {
					e.emit(opts, event.Event{Type: event.ToolResult, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: result.CallID, ToolName: result.Name, ToolKind: "local", Err: err.Error(), Duration: toolLatency, Activity: result.Activity, Metadata: mergeToolResultMetadata(result.Metadata, i, len(results))})
					return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
				}
			}
			text := projection.VisibleText
			errText := ""
			if result.IsError {
				errText = text
				text = "ERROR: " + text
			}
			metadata := mergeToolResultMetadata(result.Metadata, i, len(results))
			resultView := (*session.ToolResultView)(nil)
			if result.Pending == nil {
				metadata = mergeToolResultMetadata(toolProjectionMetadata(result.Metadata, projection), i, len(results))
				resultView = toolResultView(projection)
			}
			e.emit(opts, event.Event{Type: event.ToolResult, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: result.CallID, ToolName: result.Name, ToolKind: "local", Result: text, Err: errText, Duration: toolLatency, Activity: result.Activity, Metadata: metadata, Artifacts: eventArtifacts(projection, result.Artifacts)})
			msg := e.stableMessage(opts.RunID, session.Message{Role: session.Tool, Content: text, ToolCallID: result.CallID, ToolName: result.Name, ToolResult: resultView})
			if err := e.store.AppendTranscript(opts.RunID, msg); err != nil {
				return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
			}
			activeHistory = append(activeHistory, msg)
			state.activeMessages = append([]session.Message(nil), activeHistory...)
		}
		state.activeMessages = append([]session.Message(nil), activeHistory...)
		decision.ContinuationReason = ContinueToolResults
		e.emitStepEnd(opts, step, providerLatency, toolLatency, usage, len(calls), decision)
	}
}

func (e *Engine) compactContext(ctx context.Context, manual ManualCompactionRequest) ContextCompactionResult {
	opts := normalizeOptions(e.options)
	step := 1
	var err error
	opts, err = e.resolveToolSurface(ctx, opts, step, "compact")
	if err != nil {
		return ContextCompactionResult{Status: Failed, Err: err}
	}
	if err := validateConfiguredTools(opts.toolDefinitions, opts.HostedToolDefinitions, false); err != nil {
		return ContextCompactionResult{Status: Failed, Err: err}
	}
	manual.RequestID = strings.TrimSpace(manual.RequestID)
	manual.Source = strings.TrimSpace(manual.Source)
	if manual.RequestID == "" {
		return ContextCompactionResult{Status: Failed, Err: errors.New("manual compaction request id is required")}
	}
	metrics := RunMetrics{Steps: step}
	activeHistory, err := e.store.Transcript(opts.RunID)
	if err != nil {
		return ContextCompactionResult{Status: Failed, Err: err}
	}
	tracker := NewContextPressureTracker(opts.PromptScopeID)
	if anchor, ok, err := e.prompt.LatestPressureAnchor(ctx, opts.PromptScopeID, opts.ProviderName, opts.Model); err != nil {
		return ContextCompactionResult{Status: Failed, Err: err}
	} else if ok {
		tracker.SetAnchor(anchor)
	}
	usage := contextpolicy.EstimateMessageContext(systemPromptForOptions(e, opts), activeHistory, opts.ContextPolicy)
	pressure := contextpolicy.PressureFromManual(usage, opts.ContextPolicy)
	active, _, compacted, err := e.runCompaction(ctx, opts, step, activeHistory, tracker, 1, false, compaction.TriggerManual, compaction.ReasonManual, usage, nil, pressure, manual, ContextCompactDebugNextActionReturnCompactedContext)
	if err != nil {
		if errors.Is(err, ErrCompactionNoop) {
			return ContextCompactionResult{Status: Completed, Metrics: metrics, Messages: append([]session.Message(nil), activeHistory...), ProviderState: provider.CloneState(opts.PreviousProviderState)}
		}
		if isContextCancellation(err) {
			return ContextCompactionResult{Status: Cancelled, Err: err, Metrics: metrics, Messages: append([]session.Message(nil), activeHistory...), ProviderState: provider.CloneState(opts.PreviousProviderState)}
		}
		return ContextCompactionResult{Status: Failed, Err: err, Metrics: metrics, Messages: append([]session.Message(nil), activeHistory...), ProviderState: provider.CloneState(opts.PreviousProviderState)}
	}
	metrics.Compactions = 1
	return ContextCompactionResult{Status: Completed, Metrics: metrics, Messages: active, Compaction: compacted, ProviderState: provider.CloneState(opts.PreviousProviderState)}
}

func exactToolOutputProjection(text string) tools.OutputProjection {
	return tools.OutputProjection{
		VisibleText: text,
	}
}

func preparePendingToolResult(result tools.Result) tools.Result {
	if result.Pending == nil || result.IsError {
		return result
	}
	pending := *result.Pending
	result.Text = tools.PendingToolResultText(pending)
	result.Metadata = mergeAnyMetadata(result.Metadata, tools.PendingToolResultMetadata(pending))
	result.Activity = tools.PendingToolActivity(pending, result.Activity)
	return result
}

func cloneOptions(o Options) Options {
	o.toolDefinitions = cloneProviderToolDefinitions(o.toolDefinitions)
	o.HostedToolDefinitions = cloneHostedToolDefinitions(o.HostedToolDefinitions)
	o.ContextPolicy = contextpolicy.Normalize(o.ContextPolicy)
	o.Reasoning = provider.NormalizeReasoningSelection(o.Reasoning)
	o.ControlSpec = cloneControlSpec(o.ControlSpec)
	o.Labels = cloneRunLabels(o.Labels)
	o.PreviousProviderState = provider.CloneState(o.PreviousProviderState)
	o.toolSurface = cloneResolvedToolSurface(o.toolSurface)
	return o
}

func cloneResolvedToolSurface(surface resolvedToolSurface) resolvedToolSurface {
	return resolvedToolSurface{
		tools:                 surface.tools,
		toolDefinitions:       cloneProviderToolDefinitions(surface.toolDefinitions),
		hostedToolDefinitions: cloneHostedToolDefinitions(surface.hostedToolDefinitions),
		systemPrompt:          surface.systemPrompt,
		hostContext:           cloneStringMap(surface.hostContext),
		epoch:                 strings.TrimSpace(surface.epoch),
		reason:                strings.TrimSpace(surface.reason),
	}
}

func (e *Engine) resolveToolSurface(ctx context.Context, opts Options, step int, phase string) (Options, error) {
	opts = cloneOptions(opts)
	baseTools := e.tools
	if opts.toolSurface.tools != nil {
		baseTools = opts.toolSurface.tools
	}
	if baseTools == nil {
		baseTools = tools.NewRegistry()
	}
	if len(opts.toolDefinitions) == 0 {
		opts.toolDefinitions = providerToolDefinitionsFromTools(baseTools.Definitions())
	}
	if len(opts.toolSurface.hostedToolDefinitions) > 0 {
		opts.HostedToolDefinitions = cloneHostedToolDefinitions(opts.toolSurface.hostedToolDefinitions)
	}
	if opts.ToolSurfaceProvider == nil {
		opts.toolSurface = resolvedToolSurface{
			tools:                 baseTools,
			toolDefinitions:       cloneProviderToolDefinitions(opts.toolDefinitions),
			hostedToolDefinitions: cloneHostedToolDefinitions(opts.HostedToolDefinitions),
			systemPrompt:          e.memory.SystemPrompt,
			hostContext:           cloneStringMap(opts.Labels.Host),
			epoch:                 opts.toolSurface.epoch,
			reason:                opts.toolSurface.reason,
		}
		return opts, nil
	}
	surface, err := opts.ToolSurfaceProvider(ctx, ToolSurfaceRequest{
		RunID:         opts.RunID,
		ThreadID:      opts.ThreadID,
		TurnID:        opts.TurnID,
		TraceID:       opts.TraceID,
		PromptScopeID: opts.PromptScopeID,
		Step:          step,
		Phase:         strings.TrimSpace(phase),
		Labels:        cloneRunLabels(opts.Labels),
		HostContext:   cloneStringMap(opts.Labels.Host),
	})
	if err != nil {
		return Options{}, err
	}
	surfaceTools := surface.Tools
	if surfaceTools == nil {
		surfaceTools = baseTools
	}
	toolDefs := cloneProviderToolDefinitions(surface.ToolDefinitions)
	if toolDefs == nil {
		toolDefs = providerToolDefinitionsFromTools(surfaceTools.Definitions())
	}
	hostedDefs := cloneHostedToolDefinitions(surface.HostedToolDefinitions)
	if hostedDefs == nil {
		hostedDefs = cloneHostedToolDefinitions(opts.HostedToolDefinitions)
	}
	systemPrompt := surface.SystemPrompt
	if systemPrompt == "" {
		if opts.toolSurface.systemPrompt != "" {
			systemPrompt = opts.toolSurface.systemPrompt
		} else {
			systemPrompt = e.memory.SystemPrompt
		}
	}
	hostContext := cloneStringMap(opts.Labels.Host)
	if surface.HostContext != nil {
		hostContext = cloneStringMap(surface.HostContext)
	}
	opts.toolDefinitions = toolDefs
	opts.HostedToolDefinitions = hostedDefs
	opts.toolSurface = resolvedToolSurface{
		tools:                 surfaceTools,
		toolDefinitions:       cloneProviderToolDefinitions(toolDefs),
		hostedToolDefinitions: cloneHostedToolDefinitions(hostedDefs),
		systemPrompt:          systemPrompt,
		hostContext:           hostContext,
		epoch:                 strings.TrimSpace(surface.Epoch),
		reason:                strings.TrimSpace(surface.Reason),
	}
	return opts, nil
}

func toolSurfaceMetadata(opts Options) map[string]any {
	meta := map[string]any{}
	if epoch := strings.TrimSpace(opts.toolSurface.epoch); epoch != "" {
		meta["tool_surface_epoch"] = epoch
	}
	if reason := strings.TrimSpace(opts.toolSurface.reason); reason != "" {
		meta["tool_surface_reason"] = reason
	}
	if promptHash := stableTextHash(opts.toolSurface.systemPrompt); promptHash != "" {
		meta["tool_surface_prompt_hash"] = promptHash
	}
	if toolHash := stableProviderToolDefinitionsHash(opts.toolDefinitions); toolHash != "" {
		meta["tool_surface_tool_hash"] = toolHash
	}
	if hostedHash := stableHostedToolDefinitionsHash(opts.HostedToolDefinitions); hostedHash != "" {
		meta["tool_surface_hosted_hash"] = hostedHash
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

func stableTextHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return cache.StableHash(value)
}

func stableProviderToolDefinitionsHash(defs []provider.ToolDefinition) string {
	if len(defs) == 0 {
		return ""
	}
	raw, err := cache.CanonicalJSON(convertToolDefinitions(defs))
	if err != nil {
		return ""
	}
	return cache.StableHash(raw)
}

func stableHostedToolDefinitionsHash(defs []provider.HostedToolDefinition) string {
	if len(defs) == 0 {
		return ""
	}
	raw, err := cache.CanonicalJSON(convertHostedToolDefinitions(defs))
	if err != nil {
		return ""
	}
	return cache.StableHash(raw)
}

func (l RunLabels) isZero() bool {
	return len(l.Correlation) == 0 && len(l.Host) == 0
}

func cloneRunLabels(labels RunLabels) RunLabels {
	return RunLabels{
		Correlation: cloneStringMap(labels.Correlation),
		Host:        cloneStringMap(labels.Host),
	}
}

func providerRequestLabels(labels RunLabels) provider.RequestLabels {
	return provider.RequestLabels{
		Correlation: cloneStringMap(labels.Correlation),
		Host:        cloneStringMap(labels.Host),
	}
}

func observabilityLabels(labels RunLabels) map[string]string {
	out := make(map[string]string, len(labels.Correlation)+len(labels.Host))
	for key, value := range labels.Correlation {
		if key = strings.TrimSpace(key); key != "" {
			out["correlation."+key] = value
		}
	}
	for key, value := range labels.Host {
		if key = strings.TrimSpace(key); key != "" {
			out["host."+key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func registryDefinitions(registry *tools.Registry) []provider.ToolDefinition {
	if registry == nil {
		return nil
	}
	return providerToolDefinitionsFromTools(registry.Definitions())
}

func cloneProviderToolDefinitions(defs []provider.ToolDefinition) []provider.ToolDefinition {
	if defs == nil {
		return nil
	}
	out := make([]provider.ToolDefinition, len(defs))
	for i, def := range defs {
		out[i] = def
		out[i].InputSchema = cloneAnyMap(def.InputSchema)
		out[i].OutputSchema = cloneAnyMap(def.OutputSchema)
		out[i].Annotations = cloneAnyMap(def.Annotations)
	}
	return out
}

func cloneHostedToolDefinitions(defs []provider.HostedToolDefinition) []provider.HostedToolDefinition {
	if defs == nil {
		return nil
	}
	out := make([]provider.HostedToolDefinition, len(defs))
	for i, def := range defs {
		out[i] = def
		out[i].Parameters = cloneAnyMap(def.Parameters)
		out[i].Options = cloneAnyMap(def.Options)
	}
	return out
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
		out := []string{}
		if len(v) > 0 {
			out = append(out, v...)
		}
		return out
	default:
		return value
	}
}

func cloneActivityPresentation(in *observation.ActivityPresentation) *observation.ActivityPresentation {
	if in == nil {
		return nil
	}
	out := *in
	out.Chips = append([]observation.ActivityChip(nil), in.Chips...)
	out.TargetRefs = append([]observation.ActivityTargetRef(nil), in.TargetRefs...)
	out.Payload = cloneAnyMap(in.Payload)
	return &out
}

func normalizeOptions(o Options) Options {
	if o.RunID == "" {
		o.RunID = "default"
	}
	if o.TurnID == "" {
		o.TurnID = o.RunID
	}
	if o.PromptScopeID == "" {
		if o.ThreadID != "" {
			o.PromptScopeID = o.ThreadID
		} else {
			o.PromptScopeID = o.RunID
		}
	}
	if o.TraceID == "" {
		o.TraceID = o.RunID
	}
	if o.CacheNamespace == "" {
		o.CacheNamespace = cache.DefaultNamespace(o.PromptScopeID, o.ProviderName, o.Model)
	}
	o.ContextPolicy = contextpolicy.Normalize(o.ContextPolicy)
	if o.MaxEmptyProviderRetries <= 0 {
		o.MaxEmptyProviderRetries = 1
	}
	if o.NoProgressLimit <= 0 {
		o.NoProgressLimit = 2
	}
	if o.DuplicateToolLimit <= 0 {
		o.DuplicateToolLimit = 3
	}
	if o.CompletionPolicy == "" {
		o.CompletionPolicy = CompletionNaturalStop
	}
	if o.MaxLengthContinuations <= 0 {
		o.MaxLengthContinuations = 2
	}
	if o.MaxStopHookContinuations <= 0 {
		o.MaxStopHookContinuations = 2
	}
	o.Labels = cloneRunLabels(o.Labels)
	o.ControlSpec = normalizeControlSpec(o.ControlSpec, o.CompletionPolicy)
	o.PreviousProviderState = provider.CloneState(o.PreviousProviderState)
	return o
}

func (e *Engine) applyStopHook(ctx context.Context, opts Options, step int, lastAssistant session.Message, metrics RunMetrics, stepOutput StepOutput) (StopHookResult, error) {
	if e.stopHook == nil {
		return StopHookResult{}, nil
	}
	messages, err := e.store.Transcript(opts.RunID)
	if err != nil {
		return StopHookResult{}, err
	}
	return e.stopHook(ctx, StopHookContext{
		RunID:           opts.RunID,
		ThreadID:        opts.ThreadID,
		TurnID:          opts.TurnID,
		TraceID:         opts.TraceID,
		PromptScopeID:   opts.PromptScopeID,
		Step:            step,
		LastAssistant:   lastAssistant,
		Messages:        messages,
		FinishReason:    stepOutput.FinishReason,
		RawFinishReason: stepOutput.RawFinishReason,
		FinishInferred:  stepOutput.FinishInferred,
		Metrics:         metrics,
	})
}

func (e *Engine) stableMessage(runID string, msg session.Message) session.Message {
	if strings.TrimSpace(msg.EntryID) != "" {
		return msg
	}
	index := 0
	if e != nil && e.store != nil {
		if messages, err := e.store.Transcript(runID); err == nil {
			index = len(messages)
		}
	}
	return stableMessageAt(runID, index, msg)
}

func stableMessageAt(runID string, index int, msg session.Message) session.Message {
	if strings.TrimSpace(msg.EntryID) != "" {
		return msg
	}
	msg.EntryID = stableMessageEntryID(runID, index, msg)
	return msg
}

func stableMessageEntryID(runID string, index int, msg session.Message) string {
	raw, err := cache.CanonicalJSON(map[string]any{
		"index":        index,
		"run_id":       runID,
		"role":         msg.Role,
		"content":      msg.Content,
		"reasoning":    msg.Reasoning,
		"tool_call_id": msg.ToolCallID,
		"tool_name":    msg.ToolName,
		"tool_args":    msg.ToolArgs,
		"kind":         msg.Kind,
		"compaction":   msg.CompactionID,
	})
	if err != nil {
		raw = fmt.Sprintf("%s:%s:%s:%s:%s", runID, msg.Role, msg.Content, msg.ToolCallID, msg.ToolName)
	}
	return fmt.Sprintf("%s:entry:%s", runID, cache.StableHash(raw)[:16])
}

func (e *Engine) providerRequest(ctx context.Context, opts Options, step int, history []session.Message) (provider.Request, error) {
	toolDefinitions := appendControlToolDefinitions(opts.toolDefinitions, opts.ControlSpec)
	if err := validateConfiguredTools(toolDefinitions, opts.HostedToolDefinitions, true); err != nil {
		return provider.Request{}, err
	}
	systemPrompt := opts.toolSurface.systemPrompt
	if systemPrompt == "" {
		systemPrompt = e.memory.SystemPrompt
	}
	toolset, _, err := cache.EnsureCurrentToolsetWithOptions(ctx, e.prompt, opts.PromptScopeID, opts.RunID, opts.ThreadID, opts.TurnID, opts.ProviderName, opts.Model, convertToolDefinitions(toolDefinitions), convertHostedToolDefinitions(opts.HostedToolDefinitions), time.Now(), cache.ToolsetOptions{AllowControlTools: true})
	if err != nil {
		return provider.Request{}, err
	}
	plan, messages, err := cache.BuildPlan(ctx, e.prompt, cache.BuildInput{
		PromptScopeID:  opts.PromptScopeID,
		RunID:          opts.RunID,
		ThreadID:       opts.ThreadID,
		TurnID:         opts.TurnID,
		Provider:       opts.ProviderName,
		Model:          opts.Model,
		AdapterVersion: cache.Version,
		CacheNamespace: opts.CacheNamespace,
		SystemPrompt:   systemPrompt,
		History:        providerSafeHistory(assembleMessages(systemPrompt, history)[systemOffsetForPrompt(systemPrompt):], opts.ControlSpec),
		Toolset:        toolset,
		HostedTools:    convertHostedToolDefinitions(opts.HostedToolDefinitions),
		Renderer:       rendererForProvider(e.provider),
		Now:            time.Now(),
	})
	if err != nil {
		return provider.Request{}, err
	}
	activeTools := providerToolDefinitions(toolset.Tools)
	activeHostedTools := hostedToolDefinitions(toolset.HostedTools)
	if err := validateNoLocalHostedToolNameConflict(activeTools, activeHostedTools); err != nil {
		return provider.Request{}, err
	}
	cachePolicy := cache.CachePolicy{
		Enabled:            true,
		Namespace:          opts.CacheNamespace,
		Retention:          opts.CacheRetention,
		PreferContinuation: true,
	}
	if cachePolicy.Retention == "" {
		if defaults, ok := e.provider.(provider.CacheRetentionDefault); ok {
			cachePolicy.Retention = defaults.DefaultCacheRetention()
		} else {
			cachePolicy.Retention = cache.RetentionInMemory
		}
	}
	cachePolicy.Enabled = cachePolicy.Retention != cache.RetentionNone
	if normalizer, ok := e.provider.(provider.CachePolicyNormalizer); ok {
		cachePolicy, err = normalizer.NormalizeCachePolicy(cachePolicy)
		if err != nil {
			return provider.Request{}, err
		}
	}
	req := provider.Request{
		RunID:           opts.RunID,
		ThreadID:        opts.ThreadID,
		TurnID:          opts.TurnID,
		PromptScopeID:   opts.PromptScopeID,
		TraceID:         opts.TraceID,
		Step:            step,
		Provider:        opts.ProviderName,
		Model:           opts.Model,
		Messages:        messages,
		Tools:           activeTools,
		HostedTools:     activeHostedTools,
		RawPlan:         plan,
		Cache:           cachePolicy,
		ContextPolicy:   opts.ContextPolicy,
		MaxOutputTokens: opts.ContextPolicy.MaxOutputTokens,
		Reasoning:       opts.Reasoning,
		PreviousState:   provider.CloneState(opts.PreviousProviderState),
		Labels:          providerRequestLabels(opts.Labels),
	}
	estimate, err := e.estimateRequestTokens(ctx, req)
	if err != nil {
		return provider.Request{}, err
	}
	req.RequestEstimate = estimate
	req.RawPlan.RequestEstimate = estimate
	if hasher, ok := e.provider.(provider.PayloadHasher); ok {
		payloadHash, err := hasher.PayloadHash(req)
		if err != nil {
			return provider.Request{}, err
		}
		req.RawPlan.PayloadHash = payloadHash
	}
	req.RawPlan.RequestShape = requestShapeHashes(req)
	return req, nil
}

func assembleMessages(systemPrompt string, history []session.Message) []session.Message {
	if strings.TrimSpace(systemPrompt) == "" {
		return append([]session.Message(nil), history...)
	}
	messages := make([]session.Message, 0, len(history)+1)
	messages = append(messages, session.Message{Role: session.System, Content: systemPrompt})
	messages = append(messages, history...)
	return messages
}

func systemPromptForOptions(e *Engine, opts Options) string {
	if strings.TrimSpace(opts.toolSurface.systemPrompt) != "" {
		return opts.toolSurface.systemPrompt
	}
	if e == nil || e.memory == nil {
		return ""
	}
	return e.memory.SystemPrompt
}

func systemOffsetForPrompt(systemPrompt string) int {
	if strings.TrimSpace(systemPrompt) == "" {
		return 0
	}
	return 1
}

func (e *Engine) estimateRequestTokens(ctx context.Context, req provider.Request) (contextpolicy.RequestEstimate, error) {
	if estimator, ok := e.provider.(provider.TokenEstimator); ok {
		estimate, err := estimator.EstimateTokens(ctx, req)
		if err != nil {
			return contextpolicy.RequestEstimate{}, err
		}
		if err := validateProviderTokenEstimate(estimate); err != nil {
			return contextpolicy.RequestEstimate{}, err
		}
		return providerEstimateToContextEstimate(estimate, req.ContextPolicy), nil
	}
	estimate, err := provider.GenericRequestEstimate(req)
	if err != nil {
		return contextpolicy.RequestEstimate{}, err
	}
	if err := validateProviderTokenEstimate(estimate); err != nil {
		return contextpolicy.RequestEstimate{}, err
	}
	return providerEstimateToContextEstimate(estimate, req.ContextPolicy), nil
}

func validateProviderTokenEstimate(estimate provider.TokenEstimate) error {
	if strings.TrimSpace(estimate.Source) == "" || estimate.Method == "" || contextpolicy.EstimateMethod(estimate.Method) == contextpolicy.EstimateMethodUnknown {
		return fmt.Errorf("%w: source=%q method=%q", ErrInvalidTokenEstimate, estimate.Source, estimate.Method)
	}
	return nil
}

func providerEstimateToContextEstimate(estimate provider.TokenEstimate, policy contextpolicy.Policy) contextpolicy.RequestEstimate {
	return contextpolicy.RequestEstimate{
		PrefixTokens:         estimate.PrefixTokens,
		MessageTokens:        estimate.MessageTokens,
		ToolDefinitionTokens: estimate.ToolDefinitionTokens,
		EstimatedInputTokens: estimate.EstimatedInputTokens,
		Source:               estimate.Source,
		Method:               contextpolicy.EstimateMethod(estimate.Method),
		Confidence:           contextpolicy.EstimateConfidence(estimate.Confidence),
	}.Normalized(policy)
}

func providerRequestMetadata(req provider.Request) map[string]any {
	estimate := req.RequestEstimate.Normalized(req.ContextPolicy)
	pressure := req.ContextPressure
	return map[string]any{
		"request_id":             requestID(req.RunID, req.Step),
		"logical_request_id":     req.LogicalRequestID,
		"attempt":                req.Attempt,
		"request_estimate":       estimate,
		"context_pressure":       pressure,
		"compaction_generation":  req.RawPlan.CompactionGeneration,
		"compaction_window_id":   req.RawPlan.CompactionWindowID,
		"message_count":          len(req.Messages),
		"raw_segment_count":      len(req.RawPlan.Segments),
		"local_tool_count":       len(req.Tools),
		"hosted_tool_count":      len(req.HostedTools),
		"prefix_hash":            shortHash(req.RawPlan.PrefixHash),
		"prefix_tokens":          estimate.PrefixTokens,
		"message_tokens":         estimate.MessageTokens,
		"tool_definition_tokens": estimate.ToolDefinitionTokens,
		"estimated_input_tokens": estimate.EstimatedInputTokens,
		"estimate_source":        estimate.Source,
		"estimate_method":        estimate.Method,
		"projected_input_tokens": pressure.ProjectedInputTokens,
		"context_window":         pressure.ContextWindowTokens,
		"threshold_tokens":       pressure.ThresholdTokens,
		"request_safe_limit":     pressure.RequestSafeLimit,
		"output_headroom":        pressure.OutputHeadroomTokens,
		"pressure_signal":        pressure.Signal,
		"pressure_source":        pressure.Source,
		"confidence":             pressure.Confidence,
		"hard_limit_exceeded":    pressure.HardLimitExceeded,
	}
}

func (e *Engine) prepareOrdinaryRequest(ctx context.Context, opts Options, step int, history []session.Message, tracker *ContextPressureTracker, metrics *RunMetrics, failures *int) (provider.Request, []session.Message, bool, error) {
	compacted := false
	if manual, ok, err := pollManualCompaction(ctx, opts, step); err != nil {
		usage := contextpolicy.EstimateMessageContext(systemPromptForOptions(e, opts), history, opts.ContextPolicy)
		pressure := contextpolicy.PressureFromManual(usage, opts.ContextPolicy)
		e.emitManualCompactionPollDebug(opts, step, usage, pressure, err)
		if isContextCancellation(err) {
			return provider.Request{}, history, false, err
		}
	} else if ok {
		usage := contextpolicy.EstimateMessageContext(systemPromptForOptions(e, opts), history, opts.ContextPolicy)
		pressure := contextpolicy.PressureFromManual(usage, opts.ContextPolicy)
		next, req, _, err := e.runCompaction(ctx, opts, step, history, tracker, 1, false, compaction.TriggerManual, compaction.ReasonManual, usage, nil, pressure, manual, ContextCompactDebugNextActionProviderRequest)
		if err == nil {
			if metrics != nil {
				metrics.Compactions++
			}
			return req, next, true, nil
		}
		if errors.Is(err, ErrCompactionNoop) {
			history = next
		} else if isContextCancellation(err) {
			return provider.Request{}, history, false, err
		}
	}
	if pressure, ok := tracker.ConsumePendingCompaction(); ok {
		next, req, err := e.compactForPressure(ctx, opts, step, history, tracker, 1, false, compaction.TriggerPostResponse, compaction.ReasonFollowUpPressure, pressure, metrics, failures)
		if err != nil {
			return provider.Request{}, history, false, err
		}
		history = next
		return req, history, true, nil
	}

	req, err := e.buildProjectedProviderRequest(ctx, opts, step, history, tracker, 1, false)
	if err != nil {
		return provider.Request{}, history, compacted, err
	}
	if !req.ContextPressure.HardLimitExceeded {
		return req, history, compacted, nil
	}

	next, req, err := e.compactForPressure(ctx, opts, step, history, tracker, 1, false, compaction.TriggerPreRequest, compaction.ReasonThreshold, req.ContextPressure, metrics, failures)
	if err != nil {
		return provider.Request{}, history, compacted, err
	}
	return req, next, true, nil
}

func (e *Engine) emitManualCompactionPollDebug(opts Options, step int, usage contextpolicy.Usage, pressure contextpolicy.ContextPressure, err error) {
	if err == nil {
		return
	}
	status := ContextCompactDebugStatusFailed
	nextAction := ContextCompactDebugNextActionProviderRequest
	if isContextCancellation(err) {
		status = ContextCompactDebugStatusCancelled
		nextAction = ContextCompactDebugNextActionFailTurn
	}
	e.emitCompactionDebug(opts, step, "", ContextCompactDebugStagePoll, status, compaction.TriggerManual, compaction.ReasonManual, pressure, usage, ManualCompactionRequest{}, map[string]any{
		"next_action": nextAction,
	}, err, time.Time{})
}

func (e *Engine) buildProjectedProviderRequest(ctx context.Context, opts Options, step int, history []session.Message, tracker *ContextPressureTracker, attempt int, overflowRetried bool) (provider.Request, error) {
	req, err := e.providerRequest(ctx, opts, step, history)
	if err != nil {
		return provider.Request{}, err
	}
	req.Attempt = attempt
	req.OverflowRetried = overflowRetried
	req.LogicalRequestID = fmt.Sprintf("%s:logical:%d", opts.RunID, step)
	req.ContextPressure = tracker.Project(req, history)
	req.RawPlan.ProjectedPressure = req.ContextPressure
	req.RawPlan.RequestShape = requestShapeHashes(req)
	return req, nil
}

func (e *Engine) sendOrdinaryProviderRequest(ctx context.Context, opts Options, step int, req provider.Request, history []session.Message, tracker *ContextPressureTracker, metrics *RunMetrics, failures *int) (provider.Request, []session.Message, StepOutput, int64, bool, error) {
	stepOutput, latency, overflow, err := e.sendProviderAttempt(ctx, opts, step, req, metrics)
	if err == nil || !overflow {
		return req, history, stepOutput, latency, false, err
	}
	pressure := tracker.Overflow(opts.ContextPolicy)
	next, retryReq, compactErr := e.compactForPressure(ctx, opts, step, history, tracker, 2, true, compaction.TriggerOverflow, compaction.ReasonProviderOverflow, pressure, metrics, failures)
	if compactErr != nil {
		return req, history, StepOutput{}, latency, false, compactErr
	}
	history = next
	stepOutput, retryLatency, overflow, err := e.sendProviderAttempt(ctx, opts, step, retryReq, metrics)
	latency += retryLatency
	if overflow {
		return retryReq, history, stepOutput, latency, true, fmt.Errorf("provider context overflow retry exhausted: %w", provider.ErrContextOverflow)
	}
	return retryReq, history, stepOutput, latency, true, err
}

func (e *Engine) sendProviderAttempt(ctx context.Context, opts Options, step int, req provider.Request, metrics *RunMetrics) (StepOutput, int64, bool, error) {
	if _, err := cache.RecordProviderRequest(ctx, e.prompt, providerRequestSnapshot(req)); err != nil {
		return StepOutput{}, 0, false, err
	}
	if metrics != nil {
		metrics.LLMRequests++
	}
	e.emit(opts, event.Event{Type: event.ProviderRequest, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: fmt.Sprintf("%d messages, %d raw segments, %d local tools, %d hosted tools, prefix %s", len(req.Messages), len(req.RawPlan.Segments), len(req.Tools), len(req.HostedTools), shortHash(req.RawPlan.PrefixHash)), Metadata: mergeAnyMetadata(providerRequestMetadata(req), toolSurfaceMetadata(opts))})
	started := time.Now()
	stream, err := e.provider.Stream(ctx, req)
	latency := time.Since(started).Milliseconds()
	if errors.Is(err, provider.ErrContextOverflow) {
		return StepOutput{}, latency, true, err
	}
	if err != nil {
		return StepOutput{}, latency, false, err
	}
	out, err := e.consume(ctx, opts, step, stream)
	latency = time.Since(started).Milliseconds()
	if errors.Is(err, provider.ErrContextOverflow) {
		return out, latency, true, err
	}
	return out, latency, false, err
}

func isContextCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func compactionErrorNextAction(trigger compaction.Trigger, reason compaction.Reason, configured string, err error) string {
	if isContextCancellation(err) {
		return ContextCompactDebugNextActionFailTurn
	}
	if trigger == compaction.TriggerManual && reason == compaction.ReasonManual && strings.TrimSpace(configured) == ContextCompactDebugNextActionProviderRequest {
		return ContextCompactDebugNextActionProviderRequest
	}
	return ContextCompactDebugNextActionFailTurn
}

func (e *Engine) compactForPressure(ctx context.Context, opts Options, step int, history []session.Message, tracker *ContextPressureTracker, attempt int, overflowRetried bool, trigger compaction.Trigger, reason compaction.Reason, pressure contextpolicy.ContextPressure, metrics *RunMetrics, failures *int) ([]session.Message, provider.Request, error) {
	usage := contextpolicy.EstimateMessageContext(systemPromptForOptions(e, opts), history, opts.ContextPolicy)
	next, req, _, err := e.runCompaction(ctx, opts, step, history, tracker, attempt, overflowRetried, trigger, reason, usage, failures, pressure, ManualCompactionRequest{}, ContextCompactDebugNextActionProviderRequest)
	if err != nil {
		return history, provider.Request{}, err
	}
	if metrics != nil {
		metrics.Compactions++
	}
	return next, req, nil
}

func pollManualCompaction(ctx context.Context, opts Options, step int) (ManualCompactionRequest, bool, error) {
	source := opts.ManualCompactions
	if source == nil {
		return ManualCompactionRequest{}, false, nil
	}
	request, ok, err := source.PollManualCompaction(ctx, ManualCompactionPollRequest{
		RunID:         opts.RunID,
		ThreadID:      opts.ThreadID,
		TurnID:        opts.TurnID,
		TraceID:       opts.TraceID,
		PromptScopeID: opts.PromptScopeID,
		Step:          step,
	})
	if err != nil || !ok {
		return ManualCompactionRequest{}, ok, err
	}
	request.RequestID = strings.TrimSpace(request.RequestID)
	request.Source = strings.TrimSpace(request.Source)
	if request.RequestID == "" {
		return ManualCompactionRequest{}, false, errors.New("manual compaction request id is required")
	}
	return request, true, nil
}

func providerRequestSnapshot(req provider.Request) cache.ProviderRequestSnapshot {
	return cache.ProviderRequestSnapshot{
		PromptScopeID:    req.PromptScopeID,
		RunID:            req.RunID,
		ThreadID:         req.ThreadID,
		TurnID:           req.TurnID,
		Step:             req.Step,
		LogicalRequestID: req.LogicalRequestID,
		Attempt:          req.Attempt,
		OverflowRetried:  req.OverflowRetried,
		Provider:         req.Provider,
		Model:            req.Model,
		Cache:            req.Cache,
		RawPlan:          req.RawPlan,
	}
}

func validateNoLocalHostedToolNameConflict(local []provider.ToolDefinition, hosted []provider.HostedToolDefinition) error {
	if len(local) == 0 || len(hosted) == 0 {
		return nil
	}
	localNames := map[string]struct{}{}
	for _, def := range local {
		if def.Name != "" {
			localNames[def.Name] = struct{}{}
		}
	}
	for _, def := range hosted {
		if def.Name == "" {
			continue
		}
		if _, ok := localNames[def.Name]; ok {
			return fmt.Errorf("tool %q cannot be exposed as both a local tool and a provider-hosted tool", def.Name)
		}
	}
	return nil
}

func validateConfiguredTools(local []provider.ToolDefinition, hosted []provider.HostedToolDefinition, allowControl bool) error {
	_, _, err := cache.NormalizeToolsetChecked(convertToolDefinitions(local), convertHostedToolDefinitions(hosted), cache.ToolsetOptions{AllowControlTools: allowControl})
	return err
}

func (e *Engine) runCompaction(ctx context.Context, opts Options, step int, history []session.Message, tracker *ContextPressureTracker, attempt int, overflowRetried bool, trigger compaction.Trigger, reason compaction.Reason, usage contextpolicy.Usage, failures *int, beforePressure contextpolicy.ContextPressure, manual ManualCompactionRequest, nextAction string) ([]session.Message, provider.Request, compaction.Result, error) {
	operationID := compactionOperationID(opts.RunID, step, trigger, reason, manual)
	e.emit(opts, event.Event{
		Type:     event.ContextCompact,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		Message:  fmt.Sprintf("%s/%s", trigger, reason),
		Metrics:  usage,
		Metadata: compactionStartMetadata(operationID, trigger, reason, beforePressure, usage, manual),
	})
	beginDebug := map[string]any{
		"history_message_count": len(history),
		"consecutive_failures":  derefInt(failures),
	}
	if opts.PreviousProviderState != nil {
		beginDebug["provider_state_kind"] = strings.TrimSpace(opts.PreviousProviderState.Kind)
	}
	e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageBegin, ContextCompactDebugStatusRunning, trigger, reason, beforePressure, usage, manual, beginDebug, nil, time.Time{})
	policy := compactionPolicyForUsage(opts.ContextPolicy, usage)
	if noop, noopReason := manualCompactionNoopReason(policy, usage, history, trigger, reason); noop {
		meta := withCompactionNextAction(cloneAnyMap(beginDebug), strings.TrimSpace(nextAction))
		meta["noop_reason"] = noopReason
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStagePreflight, ContextCompactDebugStatusOK, trigger, reason, beforePressure, usage, manual, meta, nil, time.Time{})
		e.emitCompactionNoop(opts, step, operationID, trigger, noopReason, beforePressure, usage, manual)
		return append([]session.Message(nil), history...), provider.Request{}, compaction.Result{}, ErrCompactionNoop
	}
	if failures != nil && *failures >= opts.ContextPolicy.MaxCompactionFailures {
		err := fmt.Errorf("compaction failure circuit breaker reached after %d failures", *failures)
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStagePreflight, ContextCompactDebugStatusFailed, trigger, reason, beforePressure, usage, manual, withCompactionNextAction(beginDebug, compactionErrorNextAction(trigger, reason, nextAction, err)), err, time.Time{})
		e.emitCompactionFailed(opts, step, operationID, trigger, reason, beforePressure, usage, manual, err)
		return nil, provider.Request{}, compaction.Result{}, err
	}
	manager := e.compactor
	if manager == nil {
		err := errors.New("compaction manager is required when context exceeds policy")
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStagePreflight, ContextCompactDebugStatusFailed, trigger, reason, beforePressure, usage, manual, withCompactionNextAction(beginDebug, compactionErrorNextAction(trigger, reason, nextAction, err)), err, time.Time{})
		e.emitCompactionFailed(opts, step, operationID, trigger, reason, beforePressure, usage, manual, err)
		return nil, provider.Request{}, compaction.Result{}, err
	}
	e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStagePreflight, ContextCompactDebugStatusOK, trigger, reason, beforePressure, usage, manual, beginDebug, nil, time.Time{})
	baseDetails := map[string]string{
		"run_id":                 opts.RunID,
		"thread_id":              opts.ThreadID,
		"turn_id":                opts.TurnID,
		"trace_id":               opts.TraceID,
		"prompt_scope_id":        opts.PromptScopeID,
		"operation_id":           operationID,
		"context_window":         fmt.Sprintf("%d", usage.ContextWindow),
		"threshold_tokens":       fmt.Sprintf("%d", usage.ThresholdTokens),
		"max_output_tokens":      fmt.Sprintf("%d", usage.MaxOutputTokens),
		"output_headroom_tokens": fmt.Sprintf("%d", usage.OutputHeadroom),
		"auto_compact_ratio_pct": fmt.Sprintf("%d", usage.AutoCompactRatio),
		"tokens_before":          fmt.Sprintf("%d", usage.InputTokens),
		"consecutive_failures":   fmt.Sprintf("%d", derefInt(failures)),
	}
	if manual.RequestID != "" {
		baseDetails["manual_request_id"] = manual.RequestID
	}
	if manual.Source != "" {
		baseDetails["manual_source"] = manual.Source
	}
	var result compaction.Result
	var active []session.Message
	var req provider.Request
	var validation compactedRequestValidation
	var err error
	for compactAttempt := 1; compactAttempt <= maxCompactionConvergenceAttempts; compactAttempt++ {
		attemptStarted := time.Now()
		details := cloneStringMap(baseDetails)
		details["compaction_convergence_attempt"] = fmt.Sprintf("%d", compactAttempt)
		attemptDebug := map[string]any{
			"compaction_convergence_attempt":  compactAttempt,
			"history_message_count":           len(history),
			"compacted_context_target_tokens": policy.CompactedContextTargetTokens,
			"recent_tail_tokens":              policy.RecentTailTokens,
			"recent_user_tokens":              policy.RecentUserTokens,
		}
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageGenerateAttemptStart, ContextCompactDebugStatusRunning, trigger, reason, beforePressure, usage, manual, attemptDebug, nil, time.Time{})
		result, active, err = manager.Compact(ctx, CompactionRequest{
			RunID:         opts.RunID,
			ThreadID:      opts.ThreadID,
			TurnID:        opts.TurnID,
			TraceID:       opts.TraceID,
			PromptScopeID: opts.PromptScopeID,
			Step:          step,
			History:       history,
			Policy:        policy,
			Trigger:       trigger,
			Reason:        reason,
			Phase:         compaction.PhaseGenerate,
			Provider:      e.provider,
			ProviderName:  opts.ProviderName,
			Model:         opts.Model,
			ContextUsage:  usage,
			Details:       details,
		})
		if err != nil {
			status := ContextCompactDebugStatusFailed
			if isContextCancellation(err) {
				status = ContextCompactDebugStatusCancelled
			}
			e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageGenerateAttemptComplete, status, trigger, reason, beforePressure, usage, manual, withCompactionNextAction(attemptDebug, compactionErrorNextAction(trigger, reason, nextAction, err)), err, attemptStarted)
			if manualCompactionNoopError(err, trigger, reason) {
				e.emitCompactionNoop(opts, step, operationID, trigger, "no_compactable_context", beforePressure, usage, manual)
				return append([]session.Message(nil), history...), provider.Request{}, compaction.Result{}, ErrCompactionNoop
			}
			break
		}
		attemptDoneDebug := cloneAnyMap(attemptDebug)
		attemptDoneDebug["active_message_count"] = len(active)
		attemptDoneDebug["compaction_id"] = result.CompactionID
		attemptDoneDebug["compaction_generation"] = result.CompactionGeneration
		attemptDoneDebug["compaction_window_id"] = result.CompactionWindowID
		attemptDoneDebug["tokens_after_estimate"] = result.TokensAfterEstimate
		attemptDoneDebug["context_after"] = result.UsageAfter
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageGenerateAttemptComplete, ContextCompactDebugStatusOK, trigger, reason, beforePressure, usage, manual, attemptDoneDebug, nil, attemptStarted)
		if manualCompactionResultHasInsufficientSavings(result, trigger, reason) {
			noopMeta := withCompactionNextAction(cloneAnyMap(attemptDoneDebug), strings.TrimSpace(nextAction))
			noopMeta["noop_reason"] = "insufficient_savings"
			e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestValidation, ContextCompactDebugStatusOK, trigger, reason, beforePressure, usage, manual, noopMeta, nil, time.Time{})
			e.emitCompactionNoop(opts, step, operationID, trigger, "insufficient_savings", beforePressure, usage, manual)
			return append([]session.Message(nil), history...), provider.Request{}, compaction.Result{}, ErrCompactionNoop
		}
		rebuildStarted := time.Now()
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestRebuildStart, ContextCompactDebugStatusRunning, trigger, reason, beforePressure, usage, manual, map[string]any{
			"compaction_convergence_attempt": compactAttempt,
			"active_message_count":           len(active),
			"compaction_id":                  result.CompactionID,
			"compaction_generation":          result.CompactionGeneration,
			"compaction_window_id":           result.CompactionWindowID,
		}, nil, time.Time{})
		req, err = e.buildProjectedProviderRequest(ctx, opts, step, active, tracker, attempt, overflowRetried)
		if err != nil {
			status := ContextCompactDebugStatusFailed
			if isContextCancellation(err) {
				status = ContextCompactDebugStatusCancelled
			}
			e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestRebuildComplete, status, trigger, reason, beforePressure, usage, manual, withCompactionNextAction(map[string]any{
				"compaction_convergence_attempt": compactAttempt,
				"active_message_count":           len(active),
				"compaction_id":                  result.CompactionID,
				"compaction_generation":          result.CompactionGeneration,
				"compaction_window_id":           result.CompactionWindowID,
			}, compactionErrorNextAction(trigger, reason, nextAction, err)), err, rebuildStarted)
			break
		}
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestRebuildComplete, ContextCompactDebugStatusOK, trigger, reason, beforePressure, usage, manual, map[string]any{
			"compaction_convergence_attempt": compactAttempt,
			"active_message_count":           len(active),
			"compaction_id":                  result.CompactionID,
			"compaction_generation":          result.CompactionGeneration,
			"compaction_window_id":           result.CompactionWindowID,
			"request_estimate":               req.RequestEstimate.Normalized(req.ContextPolicy),
			"validated_context_pressure":     req.ContextPressure,
			"message_count":                  len(req.Messages),
			"raw_segment_count":              len(req.RawPlan.Segments),
		}, nil, rebuildStarted)
		validation = compactedRequestValidationForRequest(req)
		if !validation.ContextPressure.HardLimitExceeded {
			e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestValidation, ContextCompactDebugStatusOK, trigger, reason, beforePressure, usage, manual, compactionValidationDebugMetadata(compactAttempt, result, validation, policy.CompactedContextTargetTokens, 0), nil, time.Time{})
			err = nil
			break
		}
		nextPolicy, ok := nextCompactionPolicyForValidation(policy, validation)
		if !ok {
			err = fmt.Errorf("%w: projected_input_tokens=%d request_safe_limit=%d fixed_input_tokens=%d", ErrFixedContextOverBudget, validation.ContextPressure.ProjectedInputTokens, validation.RequestSafeLimit, validation.FixedInputTokens)
			e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestValidation, ContextCompactDebugStatusFailed, trigger, reason, beforePressure, usage, manual, withCompactionNextAction(compactionValidationDebugMetadata(compactAttempt, result, validation, policy.CompactedContextTargetTokens, 0), compactionErrorNextAction(trigger, reason, nextAction, err)), err, time.Time{})
			break
		}
		if sameCompactionBudget(policy, nextPolicy) {
			err = fmt.Errorf("%w: projected_input_tokens=%d request_safe_limit=%d compacted_context_target_tokens=%d", ErrCompactedRequestOverBudget, validation.ContextPressure.ProjectedInputTokens, validation.RequestSafeLimit, policy.CompactedContextTargetTokens)
			e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestValidation, ContextCompactDebugStatusFailed, trigger, reason, beforePressure, usage, manual, withCompactionNextAction(compactionValidationDebugMetadata(compactAttempt, result, validation, policy.CompactedContextTargetTokens, nextPolicy.CompactedContextTargetTokens), compactionErrorNextAction(trigger, reason, nextAction, err)), err, time.Time{})
			break
		}
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageRequestValidation, ContextCompactDebugStatusRetrying, trigger, reason, beforePressure, usage, manual, compactionValidationDebugMetadata(compactAttempt, result, validation, policy.CompactedContextTargetTokens, nextPolicy.CompactedContextTargetTokens), nil, time.Time{})
		policy = nextPolicy
		err = fmt.Errorf("%w: projected_input_tokens=%d request_safe_limit=%d", ErrCompactedRequestOverBudget, validation.ContextPressure.ProjectedInputTokens, validation.RequestSafeLimit)
	}
	if err != nil {
		if failures != nil && !isContextCancellation(err) {
			*failures++
		}
		if isContextCancellation(err) {
			e.emitCompactionCancelled(opts, step, operationID, trigger, reason, beforePressure, usage, manual, err)
		} else {
			e.emitCompactionFailed(opts, step, operationID, trigger, reason, beforePressure, usage, manual, err)
		}
		return nil, provider.Request{}, compaction.Result{}, err
	}
	installDebug := compactionValidationDebugMetadata(0, result, validation, 0, 0)
	installDebug["active_message_count"] = len(active)
	e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageInstallStart, ContextCompactDebugStatusRunning, trigger, reason, beforePressure, usage, manual, installDebug, nil, time.Time{})
	installStarted := time.Now()
	committed, err := e.commitValidatedCompaction(ctx, manager, CompactionCommitRequest{
		CompactionRequest: CompactionRequest{
			RunID:         opts.RunID,
			ThreadID:      opts.ThreadID,
			TurnID:        opts.TurnID,
			TraceID:       opts.TraceID,
			PromptScopeID: opts.PromptScopeID,
			Step:          step,
			History:       history,
			Policy:        policy,
			Trigger:       trigger,
			Reason:        reason,
			Phase:         compaction.PhaseInstall,
			Provider:      e.provider,
			ProviderName:  opts.ProviderName,
			Model:         opts.Model,
			ContextUsage:  usage,
			Details:       cloneStringMap(baseDetails),
		},
		Result:         result,
		ActiveMessages: active,
	})
	if err != nil {
		if failures != nil && !isContextCancellation(err) {
			*failures++
		}
		status := ContextCompactDebugStatusFailed
		if isContextCancellation(err) {
			status = ContextCompactDebugStatusCancelled
		}
		e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageInstallComplete, status, trigger, reason, beforePressure, usage, manual, withCompactionNextAction(installDebug, compactionErrorNextAction(trigger, reason, nextAction, err)), err, installStarted)
		if isContextCancellation(err) {
			e.emitCompactionCancelled(opts, step, operationID, trigger, reason, beforePressure, usage, manual, err)
		} else {
			e.emitCompactionFailed(opts, step, operationID, trigger, reason, beforePressure, usage, manual, err)
		}
		return nil, provider.Request{}, compaction.Result{}, err
	}
	result = committed.Result
	active = committed.ActiveMessages
	req = providerRequestWithCommittedCompaction(req, committed)
	installDoneDebug := compactionValidationDebugMetadata(0, result, validation, 0, 0)
	installDoneDebug["active_message_count"] = len(active)
	installDoneDebug["context_after"] = result.UsageAfter
	installDoneDebug["next_action"] = strings.TrimSpace(nextAction)
	e.emitCompactionDebug(opts, step, operationID, ContextCompactDebugStageInstallComplete, ContextCompactDebugStatusOK, trigger, reason, beforePressure, usage, manual, installDoneDebug, nil, installStarted)
	if failures != nil {
		*failures = 0
	}
	e.emit(opts, event.Event{
		Type:     event.ContextCompact,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		Message:  result.CompactionID,
		Result:   result.Summary,
		Metrics:  result,
		Metadata: compactionCompleteMetadata(operationID, result, validation, manual),
	})
	return active, req, result, nil
}

func (e *Engine) emitCompactionFailed(opts Options, step int, operationID string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest, err error) {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	e.emit(opts, event.Event{
		Type:     event.ContextCompact,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		Message:  fmt.Sprintf("%s/%s", trigger, reason),
		Err:      errText,
		Metrics:  usage,
		Metadata: compactionFailedMetadata(operationID, trigger, reason, beforePressure, usage, manual),
	})
}

func (e *Engine) emitCompactionNoop(opts Options, step int, operationID string, trigger compaction.Trigger, noopReason string, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest) {
	e.emit(opts, event.Event{
		Type:     event.ContextCompact,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		Message:  fmt.Sprintf("%s/%s", trigger, noopReason),
		Metrics:  usage,
		Metadata: compactionNoopMetadata(operationID, trigger, noopReason, beforePressure, usage, manual),
	})
}

func (e *Engine) emitCompactionCancelled(opts Options, step int, operationID string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest, err error) {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	e.emit(opts, event.Event{
		Type:     event.ContextCompact,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		Message:  fmt.Sprintf("%s/%s", trigger, reason),
		Err:      errText,
		Metrics:  usage,
		Metadata: compactionCancelledMetadata(operationID, trigger, reason, beforePressure, usage, manual),
	})
}

func withCompactionNextAction(meta map[string]any, nextAction string) map[string]any {
	out := cloneAnyMap(meta)
	if action := strings.TrimSpace(nextAction); action != "" {
		out["next_action"] = action
	}
	return out
}

func (e *Engine) emitCompactionDebug(opts Options, step int, operationID string, stage string, status string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest, extra map[string]any, err error, started time.Time) {
	meta := compactionDebugMetadata(operationID, stage, status, trigger, reason, beforePressure, usage, manual)
	for key, value := range extra {
		meta[key] = value
	}
	var durationMS int64
	if !started.IsZero() {
		durationMS = time.Since(started).Milliseconds()
		meta["duration_ms"] = durationMS
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	e.emit(opts, event.Event{
		Type:     event.ContextCompactDebug,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		TurnID:   opts.TurnID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		Message:  fmt.Sprintf("%s/%s/%s", trigger, reason, stage),
		Err:      errText,
		Duration: durationMS,
		Metrics:  usage,
		Metadata: meta,
	})
}

func compactionValidationDebugMetadata(compactAttempt int, result compaction.Result, validation compactedRequestValidation, compactedTargetTokens int64, nextTargetTokens int64) map[string]any {
	out := map[string]any{
		"compaction_convergence_attempt": compactAttempt,
		"compaction_id":                  result.CompactionID,
		"compaction_generation":          result.CompactionGeneration,
		"compaction_window_id":           result.CompactionWindowID,
		"tokens_after_estimate":          result.TokensAfterEstimate,
		"context_after":                  result.UsageAfter,
		"request_estimate":               validation.RequestEstimate,
		"validated_context_pressure":     validation.ContextPressure,
		"hard_limit_exceeded":            validation.ContextPressure.HardLimitExceeded,
		"fixed_input_tokens":             validation.FixedInputTokens,
		"reducible_input_tokens":         validation.ReducibleInputTokens,
		"request_safe_limit":             validation.RequestSafeLimit,
	}
	if compactedTargetTokens > 0 {
		out["compacted_context_target_tokens"] = compactedTargetTokens
	}
	if nextTargetTokens > 0 {
		out["next_compacted_context_target_tokens"] = nextTargetTokens
	}
	return out
}

func (e *Engine) commitValidatedCompaction(ctx context.Context, manager CompactionManager, req CompactionCommitRequest) (committedCompaction, error) {
	committer, ok := manager.(CompactionCommitter)
	if !ok {
		return committedCompaction{Result: req.Result, ActiveMessages: req.ActiveMessages}, nil
	}
	result, active, err := committer.CommitCompaction(ctx, req)
	if err != nil {
		return committedCompaction{}, err
	}
	return committedCompaction{Result: result, ActiveMessages: active}, nil
}

func providerRequestWithCommittedCompaction(req provider.Request, committed committedCompaction) provider.Request {
	result := committed.Result
	checkpoint := committedCompactionSummary(committed.ActiveMessages)
	for i := range req.Messages {
		if req.Messages[i].Kind != session.MessageKindCompactionSummary {
			continue
		}
		if checkpoint.EntryID != "" {
			req.Messages[i].EntryID = checkpoint.EntryID
		}
		if checkpoint.ParentEntryID != "" {
			req.Messages[i].ParentEntryID = checkpoint.ParentEntryID
		}
		req.Messages[i].CompactionID = result.CompactionID
		req.Messages[i].CompactionGeneration = result.CompactionGeneration
		req.Messages[i].CompactionWindowID = result.CompactionWindowID
	}
	for i := range req.RawPlan.Segments {
		if req.RawPlan.Segments[i].Kind != cache.SegmentCompaction {
			continue
		}
		if checkpoint.EntryID != "" {
			req.RawPlan.Segments[i].EntryID = checkpoint.EntryID
		}
		if checkpoint.ParentEntryID != "" {
			req.RawPlan.Segments[i].ParentEntryID = checkpoint.ParentEntryID
		}
		req.RawPlan.Segments[i].CompactionGeneration = result.CompactionGeneration
		req.RawPlan.Segments[i].CompactionWindowID = result.CompactionWindowID
		req.RawPlan.Segments[i].CompactionEntryID = result.CompactionID
	}
	req.RawPlan.CompactionGeneration = result.CompactionGeneration
	req.RawPlan.CompactionWindowID = result.CompactionWindowID
	req.RawPlan.CompactionEntryID = result.CompactionID
	req.RawPlan.RequestEstimate = req.RequestEstimate
	req.RawPlan.ProjectedPressure = req.ContextPressure
	req.RawPlan.RequestShape = requestShapeHashes(req)
	return req
}

func committedCompactionSummary(messages []session.Message) session.Message {
	for _, msg := range messages {
		if msg.Kind == session.MessageKindCompactionSummary {
			return msg
		}
	}
	return session.Message{}
}

func compactionPolicyForUsage(policy contextpolicy.Policy, usage contextpolicy.Usage) contextpolicy.Policy {
	policy = contextpolicy.Normalize(policy)
	fixedInputTokens := usage.PrefixTokens
	if fixedInputTokens <= 0 {
		return policy
	}
	if policy.ContextWindowTokens > fixedInputTokens+1 {
		policy.ContextWindowTokens -= fixedInputTokens
	} else {
		policy.ContextWindowTokens = 1
	}
	return contextpolicy.Normalize(policy)
}

func manualCompactionNoopReason(policy contextpolicy.Policy, usage contextpolicy.Usage, history []session.Message, trigger compaction.Trigger, reason compaction.Reason) (bool, string) {
	if trigger != compaction.TriggerManual || reason != compaction.ReasonManual {
		return false, ""
	}
	policy = contextpolicy.Normalize(policy)
	target := policy.CompactedContextTargetTokens
	if target > 0 && usage.InputTokens < target {
		return true, "context_too_small"
	}
	if len(history) < 2 {
		return true, "no_compactable_context"
	}
	return false, ""
}

func manualCompactionNoopError(err error, trigger compaction.Trigger, reason compaction.Reason) bool {
	return trigger == compaction.TriggerManual &&
		reason == compaction.ReasonManual &&
		errors.Is(err, compaction.ErrNoCutPoint)
}

func manualCompactionResultHasInsufficientSavings(result compaction.Result, trigger compaction.Trigger, reason compaction.Reason) bool {
	return trigger == compaction.TriggerManual &&
		reason == compaction.ReasonManual &&
		result.TokensBefore > 0 &&
		result.TokensAfterEstimate >= result.TokensBefore
}

func compactedRequestValidationForRequest(req provider.Request) compactedRequestValidation {
	estimate := req.RequestEstimate.Normalized(req.ContextPolicy)
	messageUsage := contextpolicy.EstimateMessageContext("", providerRequestHistoryMessages(req.Messages), req.ContextPolicy)
	fixed := fixedInputTokens(estimate)
	if estimate.MessageTokens <= 0 && estimate.EstimatedInputTokens > 0 && messageUsage.InputTokens > 0 {
		renderedFixed := estimate.EstimatedInputTokens - messageUsage.InputTokens
		if renderedFixed > fixed {
			fixed = renderedFixed
		}
	}
	if fixed < 0 {
		fixed = 0
	}
	if estimate.EstimatedInputTokens > 0 && fixed > estimate.EstimatedInputTokens {
		fixed = estimate.EstimatedInputTokens
	}
	reducible := estimate.EstimatedInputTokens - fixed
	if reducible < 0 {
		reducible = 0
	}
	requestSafeLimit := req.ContextPressure.RequestSafeLimit
	if requestSafeLimit <= 0 {
		requestSafeLimit = contextpolicy.RequestSafeLimitTokens(req.ContextPolicy)
	}
	return compactedRequestValidation{
		RequestEstimate:      estimate,
		ContextPressure:      req.ContextPressure,
		MessageContextUsage:  messageUsage,
		FixedInputTokens:     fixed,
		ReducibleInputTokens: reducible,
		RequestSafeLimit:     requestSafeLimit,
	}
}

func providerRequestHistoryMessages(messages []session.Message) []session.Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]session.Message, 0, len(messages))
	for _, msg := range messages {
		if msg.Role == session.System {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func fixedInputTokens(estimate contextpolicy.RequestEstimate) int64 {
	fixed := estimate.PrefixTokens + estimate.ToolDefinitionTokens
	if estimate.MessageTokens > 0 {
		renderedFixed := estimate.EstimatedInputTokens - estimate.MessageTokens
		if renderedFixed > fixed {
			fixed = renderedFixed
		}
	}
	if fixed < 0 {
		return 0
	}
	return fixed
}

func nextCompactionPolicyForValidation(policy contextpolicy.Policy, validation compactedRequestValidation) (contextpolicy.Policy, bool) {
	policy = contextpolicy.Normalize(policy)
	plan, ok := compactionBudgetPlanForValidation(policy, validation)
	if !ok {
		return contextpolicy.Policy{}, false
	}
	next := policy
	next.CompactedContextTargetTokens = plan.TargetInputTokens
	next.ReservedSummaryTokens = plan.SummaryTokens
	next.RecentTailTokens = plan.RecentTailTokens
	next.RecentUserTokens = plan.RecentUserTokens
	return contextpolicy.Normalize(next), true
}

func compactionBudgetPlanForValidation(policy contextpolicy.Policy, validation compactedRequestValidation) (compactionBudgetPlan, bool) {
	requestSafeLimit := validation.RequestSafeLimit
	if requestSafeLimit <= 0 {
		return compactionBudgetPlan{}, false
	}
	target := requestSafeLimit - validation.FixedInputTokens - compactionConvergenceSafetyTokens
	if target <= 0 {
		return compactionBudgetPlan{}, false
	}
	if validation.ReducibleInputTokens > 1 && target >= validation.ReducibleInputTokens {
		target = validation.ReducibleInputTokens - 1
	}
	if validation.MessageContextUsage.InputTokens > 1 && target >= validation.MessageContextUsage.InputTokens {
		target = validation.MessageContextUsage.InputTokens - 1
	}
	currentTarget := contextpolicy.Normalize(policy).CompactedContextTargetTokens
	if currentTarget > 1 && target >= currentTarget {
		target = currentTarget * 3 / 4
	}
	if target < 6 {
		return compactionBudgetPlan{}, false
	}
	messageBudget := target
	if messageBudget > contextpolicy.DefaultCheckpointOverheadTokens+6 {
		messageBudget -= contextpolicy.DefaultCheckpointOverheadTokens
	}
	if messageBudget < 6 {
		return compactionBudgetPlan{}, false
	}
	summary, tail, users := splitCompactionMessageBudget(messageBudget)
	return compactionBudgetPlan{
		SummaryTokens:     summary,
		RecentTailTokens:  tail,
		RecentUserTokens:  users,
		RequestSafeLimit:  requestSafeLimit,
		FixedInputTokens:  validation.FixedInputTokens,
		TargetInputTokens: target,
	}, true
}

func splitCompactionMessageBudget(total int64) (summary, tail, users int64) {
	summary = total * 45 / 100
	tail = total * 35 / 100
	users = total - summary - tail
	if summary < 1 {
		summary = 1
	}
	if tail < 1 {
		tail = 1
	}
	if users < 1 {
		users = 1
	}
	for summary+tail+users > total {
		switch {
		case tail > 1:
			tail--
		case users > 1:
			users--
		case summary > 1:
			summary--
		default:
			return summary, tail, users
		}
	}
	return summary, tail, users
}

func sameCompactionBudget(left, right contextpolicy.Policy) bool {
	left = contextpolicy.Normalize(left)
	right = contextpolicy.Normalize(right)
	return left.CompactedContextTargetTokens == right.CompactedContextTargetTokens &&
		left.ReservedSummaryTokens == right.ReservedSummaryTokens &&
		left.RecentTailTokens == right.RecentTailTokens &&
		left.RecentUserTokens == right.RecentUserTokens
}

func derefInt(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func systemOffset(m *memory.Manager) int {
	if m != nil && m.SystemPrompt != "" {
		return 1
	}
	return 0
}

func rendererForProvider(p provider.Provider) cache.Renderer {
	renderer, _ := p.(cache.Renderer)
	return renderer
}

func (e *Engine) consume(ctx context.Context, opts Options, step int, stream <-chan provider.StreamEvent) (StepOutput, error) {
	out := StepOutput{}
	hostedTools := map[string]struct{}{}
	for _, def := range opts.HostedToolDefinitions {
		name := strings.TrimSpace(def.Name)
		if name != "" {
			hostedTools[name] = struct{}{}
		}
	}
	validator := provider.StreamValidator{}
	streamedToolCallEnds := map[string]struct{}{}
	for {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case ev, ok := <-stream:
			if !ok {
				if err := validator.Finish(); err != nil {
					return out, err
				}
				if out.Text == "" && len(out.Calls) == 0 {
					out.Retry = true
					return out, nil
				}
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, len(out.Calls) > 0, out.Truncated, out.Text != "")
				return out, nil
			}
			if ev.ResponseID != "" {
				out.ResponseID = ev.ResponseID
			}
			if ev.ResponseState != nil {
				out.ResponseState = provider.CloneState(ev.ResponseState)
			}
			if ev.Reason != "" {
				out.RawFinishReason = ev.Reason
			}
			if err := validator.Observe(ev); err != nil {
				if strings.Contains(err.Error(), "duplicate tool call id") {
					return out, ErrDuplicateToolCallID
				}
				return out, err
			}
			switch ev.Type {
			case provider.Delta:
				out.Text += ev.Text
				e.emit(opts, event.Event{Type: event.ProviderDelta, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: ev.Text})
			case provider.Reasoning:
				out.Reasoning += ev.Text
				e.emit(opts, event.Event{Type: event.ProviderReasoning, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: ev.Text})
			case provider.ToolCallStart:
				e.emit(opts, event.Event{Type: event.ProviderToolCallStart, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: ev.ToolCallStream.ID, ToolName: ev.ToolCallStream.Name})
			case provider.ToolCallDelta:
				e.emit(opts, event.Event{Type: event.ProviderToolCallDelta, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: ev.ToolCallStream.ID, ToolName: ev.ToolCallStream.Name})
			case provider.ToolCallEnd:
				e.emit(opts, event.Event{Type: event.ProviderToolCallEnd, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: ev.ToolCallStream.ID, ToolName: ev.ToolCallStream.Name})
				streamedToolCallEnds[ev.ToolCallStream.ID] = struct{}{}
			case provider.ToolCalls:
				out.Calls = append(out.Calls, ev.ToolCalls...)
				for _, call := range ev.ToolCalls {
					if _, ok := streamedToolCallEnds[call.ID]; ok {
						continue
					}
					e.emit(opts, event.Event{Type: event.ProviderToolCallEnd, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: call.ID, ToolName: call.Name})
					streamedToolCallEnds[call.ID] = struct{}{}
				}
			case provider.HostedToolCall:
				if err := validateHostedToolEvent(ev.ToolCall, hostedTools); err != nil {
					return out, err
				}
				e.emit(opts, event.Event{Type: event.HostedToolCall, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: ev.ToolCall.ID, ToolName: ev.ToolCall.Name, ToolKind: "hosted", Args: ev.ToolCall.Args})
			case provider.HostedToolResult:
				if err := validateHostedToolEvent(ev.ToolCall, hostedTools); err != nil {
					return out, err
				}
				e.emit(opts, event.Event{Type: event.HostedToolResult, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: ev.ToolCall.ID, ToolName: ev.ToolCall.Name, ToolKind: "hosted", Result: hostedToolResultText(ev), Metadata: hostedToolResultMetadata(ev.HostedResult)})
			case provider.UsageEvent:
				out.Usage = out.Usage.Add(ev.Usage)
				e.emit(opts, event.Event{Type: event.ProviderUsage, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Metrics: ev.Usage.Normalized(), Metadata: streamUsageMetadata()})
			case provider.SourcesEvent:
				e.emit(opts, event.Event{Type: event.ProviderSources, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Sources: eventSourceRefs(ev.Sources)})
			case provider.Empty:
				out.Retry = true
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, false, false, false)
				if err := validateNoEventAfterTerminal(ctx, stream, &validator); err != nil {
					return out, err
				}
				return out, nil
			case provider.Error:
				if ev.Err != nil {
					return out, ev.Err
				}
				if looksLikeProviderOverflowReason(ev.Reason) || looksLikeProviderOverflowReason(ev.Text) {
					return out, provider.ErrContextOverflow
				}
				if ev.Reason != "" {
					return out, errors.New(ev.Reason)
				}
				return out, errors.New("provider stream error")
			case provider.Truncated:
				out.Truncated = true
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, len(out.Calls) > 0, true, out.Text != "")
				if err := validateNoEventAfterTerminal(ctx, stream, &validator); err != nil {
					return out, err
				}
				return out, nil
			case provider.Done:
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, len(out.Calls) > 0, false, out.Text != "")
				if out.Text == "" && len(out.Calls) == 0 && out.FinishReason == provider.FinishUnknown {
					out.Retry = true
				}
				if err := validateNoEventAfterTerminal(ctx, stream, &validator); err != nil {
					return out, err
				}
				return out, nil
			}
		}
	}
}

func eventSourceRefs(in []provider.SourceRef) []event.SourceRef {
	out := make([]event.SourceRef, 0, len(in))
	for _, ref := range in {
		if strings.TrimSpace(ref.Title) == "" && strings.TrimSpace(ref.URL) == "" {
			continue
		}
		out = append(out, event.SourceRef{Title: strings.TrimSpace(ref.Title), URL: strings.TrimSpace(ref.URL)})
	}
	return out
}

func looksLikeProviderOverflowReason(value string) bool {
	text := strings.ToLower(value)
	return strings.Contains(text, "context") && (strings.Contains(text, "length") || strings.Contains(text, "window") || strings.Contains(text, "token")) ||
		strings.Contains(text, "input") && strings.Contains(text, "too large")
}

func hostedToolResultText(ev provider.StreamEvent) string {
	if text := ev.HostedResult.SummaryText(); text != "" {
		return text
	}
	return ev.Text
}

func hostedToolResultMetadata(result provider.HostedToolResultData) map[string]any {
	out := map[string]any{}
	if len(result.Results) > 0 {
		out["result_count"] = len(result.Results)
		items := make([]map[string]any, 0, len(result.Results))
		for _, item := range result.Results {
			entry := map[string]any{}
			if item.Title != "" {
				entry["title"] = item.Title
			}
			if item.URL != "" {
				entry["url"] = item.URL
			}
			if item.Snippet != "" {
				entry["snippet"] = item.Snippet
			}
			if item.Source != "" {
				entry["source"] = item.Source
			}
			if len(item.Metadata) > 0 {
				entry["metadata"] = item.Metadata
			}
			items = append(items, entry)
		}
		out["results"] = items
	}
	if result.Error != nil {
		if result.Error.Code != "" {
			out["error_code"] = result.Error.Code
		}
		if result.Error.Message != "" {
			out["error_message"] = result.Error.Message
		}
	}
	for key, value := range result.Metadata {
		if _, exists := out[key]; !exists {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func validateNoEventAfterTerminal(ctx context.Context, stream <-chan provider.StreamEvent, validator *provider.StreamValidator) error {
	timer := time.NewTimer(terminalCloseGrace)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-stream:
			if !ok {
				return nil
			}
			return validator.Observe(ev)
		case <-timer.C:
			return provider.ErrStreamNotClosedAfterTerminal
		}
	}
}

func validateHostedToolEvent(call provider.ToolCall, allowed map[string]struct{}) error {
	name := strings.TrimSpace(call.Name)
	if name == "" {
		return errors.New("provider hosted tool event missing tool name")
	}
	if len(allowed) == 0 {
		return fmt.Errorf("provider returned hosted tool %q but no hosted tools were requested", name)
	}
	if _, ok := allowed[name]; !ok {
		return fmt.Errorf("provider returned unrequested hosted tool %q", name)
	}
	return nil
}

func providerSafeHistory(history []session.Message, spec ControlSpec) []session.Message {
	out := make([]session.Message, 0, len(history))
	for _, msg := range history {
		if msg.Role == session.Assistant && msg.ToolName != "" && spec.isControlTool(msg.ToolName) {
			out = append(out, providerSafeControlMessage(msg, spec))
			continue
		}
		out = append(out, msg)
	}
	return out
}

func providerSafeControlMessage(msg session.Message, spec ControlSpec) session.Message {
	content := providerSafeControlText(ControlSignal{Name: strings.TrimSpace(msg.ToolName)})
	if signal, ok, err := projectProviderSafeControlSignal(msg, spec); err == nil && ok {
		content = providerSafeControlText(signal)
	}
	return session.Message{
		Role:          session.Assistant,
		Content:       content,
		EntryID:       msg.EntryID,
		ParentEntryID: msg.ParentEntryID,
		Kind:          session.MessageKindControlSignal,
	}
}

func projectProviderSafeControlSignal(msg session.Message, spec ControlSpec) (ControlSignal, bool, error) {
	call := provider.ToolCall{ID: msg.ToolCallID, Name: msg.ToolName, Args: msg.ToolArgs}
	return spec.project(call, controlProjectionContext{})
}

func providerSafeControlText(signal ControlSignal) string {
	text := strings.TrimSpace(signal.OutputText)
	switch signal.Name {
	case control.AskUserTool:
		if text != "" {
			return "Agent requested user input: " + text
		}
		return "Agent requested user input."
	case control.TaskCompleteTool:
		if text != "" {
			return "Agent completed the task: " + text
		}
		return "Agent completed the task."
	default:
		if text != "" {
			return fmt.Sprintf("Agent control signal %q: %s", signal.Name, text)
		}
		return fmt.Sprintf("Agent control signal %q was emitted.", signal.Name)
	}
}

func (e *Engine) approverWithEvents(opts Options, step int) tools.Approver {
	return func(ctx context.Context, req tools.ApprovalRequest) (tools.PermissionDecision, error) {
		e.emitApprovalEvent(opts, step, event.ToolApprovalRequested, req, "", "")
		if e.approver == nil {
			e.emitApprovalEvent(opts, step, event.ToolApprovalRejected, req, tools.ErrRejected.Error(), "")
			return tools.PermissionDecisionDeny, nil
		}
		decision, err := e.approver(ctx, req)
		if err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				e.emitApprovalEvent(opts, step, event.ToolApprovalTimedOut, req, err.Error(), "")
			case errors.Is(err, context.Canceled):
				e.emitApprovalEvent(opts, step, event.ToolApprovalCanceled, req, err.Error(), "")
			default:
				e.emitApprovalEvent(opts, step, event.ToolApprovalRejected, req, err.Error(), "")
			}
			return decision, err
		}
		if decision.Allowed() {
			e.emitApprovalEvent(opts, step, event.ToolApprovalApproved, req, strings.TrimSpace(decision.Reason), "")
			return decision, nil
		}
		reason := strings.TrimSpace(decision.RejectionReason())
		if reason == "" {
			reason = tools.ErrRejected.Error()
		}
		e.emitApprovalEvent(opts, step, event.ToolApprovalRejected, req, reason, "")
		return decision, nil
	}
}

func (e *Engine) emitApprovalEvent(opts Options, step int, typ event.Type, req tools.ApprovalRequest, reason string, result string) {
	e.emit(opts, event.Event{
		Type:     typ,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		ToolID:   req.ID,
		ToolName: req.Name,
		ToolKind: "local",
		Args:     req.Args,
		ArgsHash: req.ArgsHash,
		Result:   result,
		Err:      reason,
		Metadata: approvalEventMetadata(req, reason),
	})
}

func approvalEventMetadata(req tools.ApprovalRequest, reason string) map[string]any {
	metadata := map[string]any{
		"approval_id": req.ApprovalID,
		"resources":   approvalResources(req.Resources),
		"effects":     approvalEffects(req.Effects),
		"read_only":   req.ReadOnly,
		"destructive": req.Destructive,
		"open_world":  req.OpenWorld,
	}
	if strings.TrimSpace(reason) != "" {
		metadata["reason"] = reason
	}
	if len(req.Labels) > 0 {
		metadata["labels"] = cloneStringMap(req.Labels)
	}
	if len(req.HostContext) > 0 {
		metadata["host_context"] = cloneStringMap(req.HostContext)
	}
	return metadata
}

func approvalResources(resources []tools.ResourceRef) []map[string]string {
	if len(resources) == 0 {
		return nil
	}
	out := make([]map[string]string, 0, len(resources))
	for _, resource := range resources {
		out = append(out, map[string]string{
			"kind":  resource.Kind,
			"value": resource.Value,
		})
	}
	return out
}

func approvalEffects(effects []tools.Effect) []string {
	if len(effects) == 0 {
		return nil
	}
	out := make([]string, 0, len(effects))
	for _, effect := range effects {
		out = append(out, string(effect))
	}
	return out
}

func convertToolDefinitions(defs []provider.ToolDefinition) []cache.ToolDefinition {
	out := make([]cache.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, cache.ToolDefinition{
			Name:         def.Name,
			Title:        def.Title,
			Description:  def.Description,
			InputSchema:  def.InputSchema,
			OutputSchema: def.OutputSchema,
			Strict:       def.Strict,
			Annotations:  def.Annotations,
		})
	}
	return out
}

func providerToolDefinitionsFromTools(defs []tools.ToolDefinition) []provider.ToolDefinition {
	out := make([]provider.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, provider.ToolDefinition{
			Name:         def.Name,
			Title:        def.Title,
			Description:  def.Description,
			InputSchema:  def.InputSchema,
			OutputSchema: def.OutputSchema,
			Strict:       def.Strict,
			Annotations:  def.Annotations,
		})
	}
	return out
}

func convertHostedToolDefinitions(defs []provider.HostedToolDefinition) []cache.HostedToolDefinition {
	out := make([]cache.HostedToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, cache.HostedToolDefinition{
			Name:        def.Name,
			Type:        def.Type,
			Description: def.Description,
			Parameters:  def.Parameters,
			Options:     def.Options,
		})
	}
	return out
}

func toolCalls(calls []provider.ToolCall) []tools.ToolCall {
	out := make([]tools.ToolCall, 0, len(calls))
	for _, call := range calls {
		out = append(out, toolCall(call))
	}
	return out
}

func toolCall(call provider.ToolCall) tools.ToolCall {
	return tools.ToolCall{
		ID:        call.ID,
		Name:      call.Name,
		Args:      call.Args,
		Reasoning: call.Reasoning,
	}
}

func providerToolDefinitions(defs []cache.ToolDefinition) []provider.ToolDefinition {
	out := make([]provider.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, provider.ToolDefinition{
			Name:         def.Name,
			Title:        def.Title,
			Description:  def.Description,
			InputSchema:  def.InputSchema,
			OutputSchema: def.OutputSchema,
			Strict:       def.Strict,
			Annotations:  def.Annotations,
		})
	}
	return out
}

func hostedToolDefinitions(defs []cache.HostedToolDefinition) []provider.HostedToolDefinition {
	out := make([]provider.HostedToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, provider.HostedToolDefinition{
			Name:        def.Name,
			Type:        def.Type,
			Description: def.Description,
			Parameters:  def.Parameters,
			Options:     def.Options,
		})
	}
	return out
}

func appendControlToolDefinitions(defs []provider.ToolDefinition, spec ControlSpec) []provider.ToolDefinition {
	out := append([]provider.ToolDefinition(nil), defs...)
	for _, def := range spec.Definitions {
		out = append(out, def)
	}
	return out
}

func eventArtifacts(projection tools.OutputProjection, items []tools.ArtifactRef) []event.Artifact {
	out := make([]event.Artifact, 0, len(items)+1)
	if projection.FullOutput != nil {
		out = append(out, eventArtifact(*projection.FullOutput))
	}
	for _, item := range items {
		out = append(out, eventArtifact(item))
	}
	return out
}

func eventArtifact(ref tools.ArtifactRef) event.Artifact {
	return event.Artifact{
		ID:        ref.ID,
		SafeLabel: ref.SafeLabel,
		URL:       ref.URL,
		Kind:      ref.Kind,
		MIME:      ref.MIME,
		SizeBytes: ref.SizeBytes,
		SHA256:    ref.SHA256,
	}
}

type toolArtifactStore struct {
	store artifact.Store
}

func toolArtifactStoreFor(store artifact.Store) tools.ArtifactStore {
	if store == nil {
		return nil
	}
	return toolArtifactStore{store: store}
}

func (s toolArtifactStore) PutToolOutput(ctx context.Context, output tools.ToolOutputArtifact) (tools.ArtifactRef, error) {
	ref, err := s.store.PutToolOutput(ctx, artifact.ToolOutputArtifact{
		RunID:         output.RunID,
		ThreadID:      output.ThreadID,
		TurnID:        output.TurnID,
		PromptScopeID: output.PromptScopeID,
		Step:          output.Step,
		CallID:        output.CallID,
		ToolName:      output.ToolName,
		Text:          output.Text,
		MIME:          output.MIME,
		Kind:          output.Kind,
		Metadata:      output.Metadata,
	})
	if err != nil {
		return tools.ArtifactRef{}, err
	}
	return toolArtifactRef(ref), nil
}

func toolArtifactRef(ref artifact.Ref) tools.ArtifactRef {
	return tools.ArtifactRef{
		ID:        ref.ID,
		SafeLabel: ref.SafeLabel,
		URL:       ref.URL,
		Kind:      ref.Kind,
		MIME:      ref.MIME,
		SizeBytes: ref.SizeBytes,
		SHA256:    ref.SHA256,
	}
}

func artifactRef(ref tools.ArtifactRef) artifact.Ref {
	return artifact.Ref{
		ID:        ref.ID,
		SafeLabel: ref.SafeLabel,
		URL:       ref.URL,
		Kind:      ref.Kind,
		MIME:      ref.MIME,
		SizeBytes: ref.SizeBytes,
		SHA256:    ref.SHA256,
	}
}

func toolProjectionMetadata(base map[string]any, projection tools.OutputProjection) map[string]any {
	metadata := make(map[string]any, len(base)+12)
	for key, value := range base {
		metadata[key] = value
	}
	metadata["truncated"] = projection.Truncated
	metadata["original_bytes"] = projection.OriginalBytes
	metadata["visible_bytes"] = projection.VisibleBytes
	metadata["original_lines"] = projection.OriginalLines
	metadata["visible_lines"] = projection.VisibleLines
	metadata["strategy"] = string(projection.Strategy)
	metadata["content_sha256"] = projection.ContentSHA256
	if projection.FullOutput != nil {
		metadata["artifact_id"] = projection.FullOutput.ID
		metadata["artifact_label"] = projection.FullOutput.SafeLabel
		metadata["artifact_size_bytes"] = projection.FullOutput.SizeBytes
		metadata["artifact_sha256"] = projection.FullOutput.SHA256
	}
	return metadata
}

func mergeAnyMetadata(left, right map[string]any) map[string]any {
	if len(left) == 0 {
		if len(right) == 0 {
			return nil
		}
		out := make(map[string]any, len(right))
		for key, value := range right {
			out[key] = value
		}
		return out
	}
	out := make(map[string]any, len(left)+len(right))
	for key, value := range left {
		out[key] = value
	}
	for key, value := range right {
		out[key] = value
	}
	return out
}

func toolResultView(projection tools.OutputProjection) *session.ToolResultView {
	view := &session.ToolResultView{
		Truncated:     projection.Truncated,
		OriginalBytes: projection.OriginalBytes,
		VisibleBytes:  projection.VisibleBytes,
		OriginalLines: projection.OriginalLines,
		VisibleLines:  projection.VisibleLines,
		Strategy:      string(projection.Strategy),
		ContentSHA256: projection.ContentSHA256,
	}
	if projection.FullOutput != nil {
		ref := artifactRef(*projection.FullOutput)
		view.FullOutput = &ref
	}
	return view
}

func toolOutputArtifactResult(result tools.Result, opts Options, step int) tools.Result {
	if result.Metadata == nil {
		result.Metadata = map[string]any{}
	} else {
		metadata := make(map[string]any, len(result.Metadata)+5)
		for key, value := range result.Metadata {
			metadata[key] = value
		}
		result.Metadata = metadata
	}
	result.Metadata["run_id"] = opts.RunID
	result.Metadata["thread_id"] = opts.ThreadID
	result.Metadata["turn_id"] = opts.TurnID
	result.Metadata["prompt_scope_id"] = opts.PromptScopeID
	result.Metadata["step"] = step
	return result
}

func mergeToolResultMetadata(base map[string]any, batchIndex, batchSize int) map[string]any {
	metadata := make(map[string]any, len(base)+2)
	for key, value := range base {
		metadata[key] = value
	}
	metadata["batch_index"] = batchIndex
	metadata["batch_size"] = batchSize
	return metadata
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

func (e *Engine) emitStepEnd(opts Options, step int, providerLatency, toolLatency int64, usage provider.Usage, toolCalls int, decision RunDecision) {
	duration := providerLatency + toolLatency
	e.emit(opts, event.Event{
		Type:               event.StepEnd,
		TraceID:            opts.TraceID,
		RunID:              opts.RunID,
		ThreadID:           opts.ThreadID,
		Step:               step,
		Provider:           opts.ProviderName,
		Model:              opts.Model,
		Duration:           duration,
		FinishReason:       string(decision.FinishReason),
		RawFinishReason:    decision.RawFinishReason,
		FinishInferred:     decision.FinishInferred,
		CompletionReason:   string(decision.CompletionReason),
		ContinuationReason: string(decision.ContinuationReason),
		Message:            decision.Detail,
		Metadata:           decision.Metadata,
		Metrics: StepMetrics{
			Step:               step,
			Provider:           opts.ProviderName,
			Model:              opts.Model,
			Usage:              usage,
			ProviderLatencyMS:  providerLatency,
			ToolLatencyMS:      toolLatency,
			ToolCalls:          toolCalls,
			FinishReason:       string(decision.FinishReason),
			RawFinishReason:    decision.RawFinishReason,
			FinishInferred:     decision.FinishInferred,
			CompletionReason:   string(decision.CompletionReason),
			ContinuationReason: string(decision.ContinuationReason),
		},
	})
}

func (e *Engine) end(state *turnState, opts Options, step int, status Status, output string, err error, metrics RunMetrics, started time.Time, decision RunDecision) Result {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	metrics.WallTimeMS = time.Since(started).Milliseconds()
	e.emit(opts, event.Event{Type: event.RunEnd, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: string(status), Result: output, Err: errText, FinishReason: string(decision.FinishReason), RawFinishReason: decision.RawFinishReason, FinishInferred: decision.FinishInferred, CompletionReason: string(decision.CompletionReason), ContinuationReason: string(decision.ContinuationReason), Metrics: metrics})
	var messages []session.Message
	if len(state.activeMessages) > 0 {
		messages = append([]session.Message(nil), state.activeMessages...)
	} else if e.store != nil {
		messages, _ = e.store.Transcript(opts.RunID)
	}
	return Result{Status: status, Output: output, Err: err, Metrics: metrics, Messages: messages, CompletionReason: decision.CompletionReason, ContinuationReason: decision.ContinuationReason, FinishReason: decision.FinishReason, RawFinishReason: decision.RawFinishReason, FinishInferred: decision.FinishInferred, ControlSignal: cloneControlSignal(decision.ControlSignal), ProviderState: provider.CloneState(decision.ProviderState)}
}

func (e *Engine) emit(opts Options, ev event.Event) {
	if e.sink == nil {
		return
	}
	if ev.TraceID == "" {
		ev.TraceID = opts.TraceID
	}
	if ev.RunID == "" {
		ev.RunID = opts.RunID
	}
	if ev.ThreadID == "" {
		ev.ThreadID = opts.ThreadID
	}
	if ev.TurnID == "" {
		ev.TurnID = opts.TurnID
	}
	if ev.PromptScopeID == "" {
		ev.PromptScopeID = opts.PromptScopeID
	}
	ev.Metadata = eventMetadata(opts, ev.Metadata)
	ev.Timestamp = time.Now()
	e.sink.Emit(ev)
}

func (e *Engine) checkBudget(opts Options, metrics RunMetrics, step int) error {
	var err error
	message := ""
	switch {
	case opts.MaxTotalTokens > 0 && metrics.Usage.Normalized().TotalTokens > opts.MaxTotalTokens:
		err = fmt.Errorf("token budget exceeded")
		message = fmt.Sprintf("total tokens %d exceeded limit %d", metrics.Usage.Normalized().TotalTokens, opts.MaxTotalTokens)
		e.emit(opts, event.Event{Type: event.BudgetExceeded, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: message, Metrics: BudgetMetrics{Type: "tokens", Used: float64(metrics.Usage.Normalized().TotalTokens), Limit: float64(opts.MaxTotalTokens), Run: metrics}})
	case opts.MaxCostUSD > 0 && metrics.Usage.CostUSD > opts.MaxCostUSD:
		err = fmt.Errorf("cost budget exceeded")
		message = fmt.Sprintf("cost %.6f exceeded limit %.6f", metrics.Usage.CostUSD, opts.MaxCostUSD)
		e.emit(opts, event.Event{Type: event.BudgetExceeded, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: message, Metrics: BudgetMetrics{Type: "cost", Used: metrics.Usage.CostUSD, Limit: opts.MaxCostUSD, Run: metrics}})
	case opts.MaxToolCalls > 0 && metrics.ToolCalls > opts.MaxToolCalls:
		err = fmt.Errorf("tool call budget exceeded")
		message = fmt.Sprintf("tool calls %d exceeded limit %d", metrics.ToolCalls, opts.MaxToolCalls)
		e.emit(opts, event.Event{Type: event.BudgetExceeded, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: message, Metrics: BudgetMetrics{Type: "tool_calls", Used: float64(metrics.ToolCalls), Limit: float64(opts.MaxToolCalls), Run: metrics}})
	}
	return err
}

func eventMetadata(opts Options, base any) map[string]any {
	out := map[string]any{}
	if baseMap, ok := base.(map[string]any); ok {
		for key, value := range baseMap {
			if key == "schema_version" {
				continue
			}
			out[key] = value
		}
	} else if base != nil {
		out["details"] = base
	}
	if labels := observabilityLabels(opts.Labels); len(labels) > 0 {
		out["labels"] = labels
	}
	out["run_id"] = opts.RunID
	out["thread_id"] = opts.ThreadID
	out["turn_id"] = opts.TurnID
	out["trace_id"] = opts.TraceID
	out["prompt_scope_id"] = opts.PromptScopeID
	out["schema_version"] = "event.v1"
	return out
}

func (e *Engine) emitControlSignal(opts Options, step int, signal *ControlSignal) {
	if signal == nil {
		return
	}
	e.emit(opts, event.Event{
		Type:     event.ControlSignal,
		TraceID:  opts.TraceID,
		RunID:    opts.RunID,
		ThreadID: opts.ThreadID,
		TurnID:   opts.TurnID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		ToolID:   strings.TrimSpace(signal.CallID),
		ToolName: strings.TrimSpace(signal.Name),
		ToolKind: "control",
		ArgsHash: strings.TrimSpace(signal.ArgsHash),
		Activity: signal.Activity,
		Metadata: map[string]any{
			"control_disposition": string(signal.Disposition),
		},
	})
}

func controlSignal(spec ControlSpec, calls []provider.ToolCall, ctx controlProjectionContext) (*ControlSignal, bool, error) {
	for _, call := range calls {
		signal, ok, err := spec.project(call, ctx)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return cloneControlSignal(&signal), true, nil
		}
	}
	return nil, false, nil
}

func cloneControlSignal(in *ControlSignal) *ControlSignal {
	if in == nil {
		return nil
	}
	out := *in
	out.Payload = cloneAnyMap(in.Payload)
	out.Activity = cloneActivityPresentation(in.Activity)
	out.Labels = cloneStringMap(in.Labels)
	return &out
}

type toolCallBatch struct {
	Ordinary []provider.ToolCall
	Control  []provider.ToolCall
}

func classifyToolCalls(spec ControlSpec, calls []provider.ToolCall) toolCallBatch {
	var batch toolCallBatch
	for _, call := range calls {
		if spec.isControlTool(call.Name) {
			batch.Control = append(batch.Control, call)
			continue
		}
		batch.Ordinary = append(batch.Ordinary, call)
	}
	return batch
}

func deferredControlMetadata(calls []provider.ToolCall) map[string]any {
	items := make([]map[string]any, 0, len(calls))
	for _, call := range calls {
		item := map[string]any{
			"tool_name": strings.TrimSpace(call.Name),
		}
		if id := strings.TrimSpace(call.ID); id != "" {
			item["tool_id"] = id
		}
		if hash := providerStableHash(call.Args); hash != "" {
			item["args_hash"] = hash
		}
		items = append(items, item)
	}
	return map[string]any{
		"deferred_control_tool_count": len(calls),
		"deferred_control_tools":      items,
	}
}

func validateToolCalls(calls []provider.ToolCall) error {
	seen := map[string]struct{}{}
	for _, call := range calls {
		if call.ID == "" {
			continue
		}
		if _, ok := seen[call.ID]; ok {
			return ErrDuplicateToolCallID
		}
		seen[call.ID] = struct{}{}
	}
	return nil
}

func toolSignature(calls []provider.ToolCall) string {
	s := ""
	for _, c := range calls {
		s += c.Name + "\x00" + c.Args + "\x00"
	}
	return s
}
