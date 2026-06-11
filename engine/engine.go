package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/event"
	"github.com/floegence/floret/internal/control"
	"github.com/floegence/floret/internal/memory"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/artifact"
	"github.com/floegence/floret/session/compaction"
	"github.com/floegence/floret/session/contextpolicy"
	"github.com/floegence/floret/tools"
)

var (
	ErrNoProgress           = errors.New("agent loop made no progress")
	ErrDuplicateTools       = errors.New("agent loop repeated identical tool calls")
	ErrDuplicateToolCallID  = errors.New("provider returned duplicate tool call id")
	ErrMixedControlTools    = errors.New("provider returned control signal with ordinary tool calls")
	ErrProviderTruncated    = errors.New("provider output was truncated")
	ErrContentFiltered      = errors.New("provider output was content filtered")
	ErrProviderFinishError  = errors.New("provider returned error finish reason")
	ErrStopHookLoop         = errors.New("stop hook requested too many continuations")
	ErrInvalidTokenEstimate = errors.New("provider token estimate missing source or method")
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

const terminalCloseGrace = 250 * time.Millisecond

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

	toolDefinitions []provider.ToolDefinition
}

type RunLabels struct {
	Correlation map[string]string
	Host        map[string]string
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

type RunDecision struct {
	CompletionReason   CompletionReason
	ContinuationReason ContinuationReason
	FinishReason       provider.FinishReason
	RawFinishReason    string
	FinishInferred     bool
	Detail             string
	ControlSignal      *ControlSignal
	ProviderState      *provider.State
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
	cfg.Options.toolDefinitions = cfg.Tools.Definitions()
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
	opts.toolDefinitions = registry.Definitions()
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
	if len(opts.toolDefinitions) == 0 && e.tools != nil {
		opts.toolDefinitions = e.tools.Definitions()
	}
	if err := validateConfiguredTools(opts.toolDefinitions, opts.HostedToolDefinitions, false); err != nil {
		return Result{Status: Failed, Err: err}
	}
	opts.toolDefinitions = appendControlToolDefinitions(opts.toolDefinitions, opts.ControlSpec)
	if err := validateConfiguredTools(opts.toolDefinitions, opts.HostedToolDefinitions, true); err != nil {
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
		req, preparedHistory, compacted, err := e.prepareOrdinaryRequest(ctx, opts, step, activeHistory, pressureTracker, &metrics, &compactionFailures)
		if err != nil {
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
		if err := validateToolCalls(opts.ControlSpec, calls); err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
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
		if signal, ok, err := controlSignal(opts.ControlSpec, calls); err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
		} else if ok {
			if len(signal.Labels) == 0 {
				signal.Labels = observabilityLabels(opts.Labels)
			}
			decision.ControlSignal = signal
			switch signal.Disposition {
			case ControlTerminal:
				decision.CompletionReason = CompletionReasonToolSignal
				e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
				return e.end(state, opts, step, Completed, signal.OutputText, nil, metrics, started, decision)
			case ControlWaiting:
				decision.CompletionReason = CompletionReasonToolSignal
				e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
				return e.end(state, opts, step, Waiting, signal.OutputText, nil, metrics, started, decision)
			case ControlContinue:
				msg := e.stableMessage(opts.RunID, session.Message{Role: session.Tool, Content: signal.OutputText, ToolCallID: signal.CallID, ToolName: signal.Name})
				if err := e.store.AppendTranscript(opts.RunID, msg); err != nil {
					return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
				}
				activeHistory = append(activeHistory, msg)
				state.activeMessages = append([]session.Message(nil), activeHistory...)
				decision.ContinuationReason = ContinueToolResults
				e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
				continue
			}
		}
		metrics.ToolCalls += len(calls)
		if budgetErr := e.checkBudget(opts, metrics, step); budgetErr != nil {
			return e.end(state, opts, step, Failed, output, budgetErr, metrics, started, decision)
		}
		for i, call := range calls {
			e.emit(opts, event.Event{Type: event.ToolCall, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: call.ID, ToolName: call.Name, ToolKind: "local", Args: call.Args, Metadata: map[string]any{"batch_index": i, "batch_size": len(calls)}})
		}
		toolStarted := time.Now()
		results := e.tools.RunBatchWithOptions(ctx, calls, e.approverWithEvents(opts, step), tools.RunOptions{
			RunID:         opts.RunID,
			ThreadID:      opts.ThreadID,
			TurnID:        opts.TurnID,
			PromptScopeID: opts.PromptScopeID,
			Step:          step,
			Labels:        observabilityLabels(opts.Labels),
			HostContext:   opts.Labels.Host,
		})
		toolLatency := time.Since(toolStarted).Milliseconds()
		for i, result := range results {
			policy := tools.MergeOutputPolicy(e.tools.OutputPolicyFor(result.Name), result.OutputPolicy)
			projection, err := tools.BuildOutputProjection(ctx, toolOutputArtifactResult(result, opts, step), policy, e.artifacts)
			if err != nil {
				e.emit(opts, event.Event{Type: event.ToolResult, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: result.CallID, ToolName: result.Name, ToolKind: "local", Err: err.Error(), Duration: toolLatency, Metadata: mergeToolResultMetadata(result.Metadata, i, len(results))})
				return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
			}
			text := projection.VisibleText
			errText := ""
			if result.IsError {
				errText = text
				text = "ERROR: " + text
			}
			metadata := mergeToolResultMetadata(toolProjectionMetadata(result.Metadata, projection), i, len(results))
			e.emit(opts, event.Event{Type: event.ToolResult, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: result.CallID, ToolName: result.Name, ToolKind: "local", Result: text, Err: errText, Duration: toolLatency, Metadata: metadata, Artifacts: eventArtifacts(projection, result.Artifacts)})
			msg := e.stableMessage(opts.RunID, session.Message{Role: session.Tool, Content: text, ToolCallID: result.CallID, ToolName: result.Name, ToolResult: toolResultView(projection)})
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

func cloneOptions(o Options) Options {
	o.toolDefinitions = cloneProviderToolDefinitions(o.toolDefinitions)
	o.HostedToolDefinitions = cloneHostedToolDefinitions(o.HostedToolDefinitions)
	o.ContextPolicy = contextpolicy.Normalize(o.ContextPolicy)
	o.ControlSpec = cloneControlSpec(o.ControlSpec)
	o.Labels = cloneRunLabels(o.Labels)
	o.PreviousProviderState = provider.CloneState(o.PreviousProviderState)
	return o
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
	return registry.Definitions()
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
		return append([]string(nil), v...)
	default:
		return value
	}
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
	toolset, _, err := cache.EnsureCurrentToolsetWithOptions(ctx, e.prompt, opts.PromptScopeID, opts.RunID, opts.ThreadID, opts.TurnID, opts.ProviderName, opts.Model, convertToolDefinitions(opts.toolDefinitions), convertHostedToolDefinitions(opts.HostedToolDefinitions), time.Now(), cache.ToolsetOptions{AllowControlTools: true})
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
		SystemPrompt:   e.memory.SystemPrompt,
		History:        providerSafeHistory(e.memory.Assemble(history)[systemOffset(e.memory):], opts.ControlSpec),
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
		PreviousState:   provider.CloneState(opts.PreviousProviderState),
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
	if pressure, ok := tracker.ConsumePendingCompaction(); ok {
		next, err := e.compactForPressure(ctx, opts, step, history, compaction.TriggerPostResponse, compaction.ReasonFollowUpPressure, pressure, metrics, failures)
		if err != nil {
			return provider.Request{}, history, false, err
		}
		history = next
		compacted = true
	}

	req, err := e.buildProjectedProviderRequest(ctx, opts, step, history, tracker, 1, false)
	if err != nil {
		return provider.Request{}, history, compacted, err
	}
	if !req.ContextPressure.HardLimitExceeded {
		return req, history, compacted, nil
	}

	next, err := e.compactForPressure(ctx, opts, step, history, compaction.TriggerPreRequest, compaction.ReasonThreshold, req.ContextPressure, metrics, failures)
	if err != nil {
		return provider.Request{}, history, compacted, err
	}
	history = next
	compacted = true

	req, err = e.buildProjectedProviderRequest(ctx, opts, step, history, tracker, 1, false)
	if err != nil {
		return provider.Request{}, history, compacted, err
	}
	if req.ContextPressure.HardLimitExceeded {
		return provider.Request{}, history, compacted, ErrContextWouldOverflow
	}
	return req, history, compacted, nil
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
	next, compactErr := e.compactForPressure(ctx, opts, step, history, compaction.TriggerOverflow, compaction.ReasonProviderOverflow, pressure, metrics, failures)
	if compactErr != nil {
		return req, history, StepOutput{}, latency, false, compactErr
	}
	history = next
	retryReq, err := e.buildProjectedProviderRequest(ctx, opts, step, history, tracker, 2, true)
	if err != nil {
		return req, history, StepOutput{}, latency, true, err
	}
	if retryReq.ContextPressure.HardLimitExceeded {
		return retryReq, history, StepOutput{}, latency, true, ErrContextWouldOverflow
	}
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
	e.emit(opts, event.Event{Type: event.ProviderRequest, TraceID: opts.TraceID, RunID: opts.RunID, ThreadID: opts.ThreadID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: fmt.Sprintf("%d messages, %d raw segments, %d local tools, %d hosted tools, prefix %s", len(req.Messages), len(req.RawPlan.Segments), len(req.Tools), len(req.HostedTools), shortHash(req.RawPlan.PrefixHash)), Metadata: providerRequestMetadata(req)})
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

func (e *Engine) compactForPressure(ctx context.Context, opts Options, step int, history []session.Message, trigger compaction.Trigger, reason compaction.Reason, pressure contextpolicy.ContextPressure, metrics *RunMetrics, failures *int) ([]session.Message, error) {
	usage := contextpolicy.EstimateMessageContext(e.memory.SystemPrompt, history, opts.ContextPolicy)
	next, err := e.runCompaction(ctx, opts, step, history, trigger, reason, usage, failures, pressure)
	if err != nil {
		return history, err
	}
	if metrics != nil {
		metrics.Compactions++
	}
	return next, nil
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

func (e *Engine) runCompaction(ctx context.Context, opts Options, step int, history []session.Message, trigger compaction.Trigger, reason compaction.Reason, usage contextpolicy.Usage, failures *int, beforePressure contextpolicy.ContextPressure) ([]session.Message, error) {
	if failures != nil && *failures >= opts.ContextPolicy.MaxCompactionFailures {
		return nil, fmt.Errorf("compaction failure circuit breaker reached after %d failures", *failures)
	}
	manager := e.compactor
	if manager == nil {
		return nil, errors.New("compaction manager is required when context exceeds policy")
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
		Metrics:  usage,
		Metadata: compactionStartMetadata(trigger, reason, beforePressure, usage),
	})
	result, active, err := manager.Compact(ctx, CompactionRequest{
		RunID:         opts.RunID,
		ThreadID:      opts.ThreadID,
		TurnID:        opts.TurnID,
		TraceID:       opts.TraceID,
		PromptScopeID: opts.PromptScopeID,
		Step:          step,
		History:       history,
		Policy:        compactionPolicyForUsage(opts.ContextPolicy, usage),
		Trigger:       trigger,
		Reason:        reason,
		Phase:         compaction.PhaseGenerate,
		Provider:      e.provider,
		ProviderName:  opts.ProviderName,
		Model:         opts.Model,
		ContextUsage:  usage,
		Details: map[string]string{
			"run_id":                 opts.RunID,
			"thread_id":              opts.ThreadID,
			"turn_id":                opts.TurnID,
			"prompt_scope_id":        opts.PromptScopeID,
			"context_window":         fmt.Sprintf("%d", usage.ContextWindow),
			"threshold_tokens":       fmt.Sprintf("%d", usage.ThresholdTokens),
			"max_output_tokens":      fmt.Sprintf("%d", usage.MaxOutputTokens),
			"output_headroom_tokens": fmt.Sprintf("%d", usage.OutputHeadroom),
			"auto_compact_ratio_pct": fmt.Sprintf("%d", usage.AutoCompactRatio),
			"tokens_before":          fmt.Sprintf("%d", usage.InputTokens),
			"consecutive_failures":   fmt.Sprintf("%d", derefInt(failures)),
		},
	})
	if err != nil {
		if failures != nil {
			*failures++
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
			Err:      err.Error(),
			Metrics:  usage,
			Metadata: compactionFailedMetadata(trigger, reason, beforePressure, usage),
		})
		return nil, err
	}
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
		Metadata: compactionCompleteMetadata(result),
	})
	return active, nil
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
			case provider.ToolCalls:
				out.Calls = append(out.Calls, ev.ToolCalls...)
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
	signal, ok, err := projectProviderSafeControlSignal(msg, spec)
	content := fmt.Sprintf("Agent control signal %q was emitted.", msg.ToolName)
	if err == nil && ok {
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
	return spec.project(call)
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
	if req.CWD != "" {
		metadata["cwd"] = req.CWD
	}
	if len(req.Labels) > 0 {
		metadata["labels"] = cloneStringMap(req.Labels)
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

func eventArtifacts(projection tools.OutputProjection, items []artifact.Ref) []event.Artifact {
	out := make([]event.Artifact, 0, len(items)+1)
	if projection.FullOutput != nil {
		out = append(out, eventArtifact(*projection.FullOutput))
	}
	for _, item := range items {
		out = append(out, eventArtifact(item))
	}
	return out
}

func eventArtifact(ref artifact.Ref) event.Artifact {
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
		ref := *projection.FullOutput
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

func controlSignal(spec ControlSpec, calls []provider.ToolCall) (*ControlSignal, bool, error) {
	for _, call := range calls {
		signal, ok, err := spec.project(call)
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
	out.Labels = cloneStringMap(in.Labels)
	return &out
}

func validateToolCalls(spec ControlSpec, calls []provider.ToolCall) error {
	seen := map[string]struct{}{}
	controlCalls := 0
	ordinaryCalls := 0
	for _, call := range calls {
		if spec.isControlTool(call.Name) {
			controlCalls++
		} else {
			ordinaryCalls++
		}
		if call.ID == "" {
			continue
		}
		if _, ok := seen[call.ID]; ok {
			return ErrDuplicateToolCallID
		}
		seen[call.ID] = struct{}{}
	}
	if controlCalls > 0 && ordinaryCalls > 0 {
		return ErrMixedControlTools
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
