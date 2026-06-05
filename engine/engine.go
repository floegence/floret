package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/compaction"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/control"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

var (
	ErrNoProgress          = errors.New("agent loop made no progress")
	ErrDuplicateTools      = errors.New("agent loop repeated identical tool calls")
	ErrDuplicateToolCallID = errors.New("provider returned duplicate tool call id")
	ErrMixedControlTools   = errors.New("provider returned control signal with ordinary tool calls")
	ErrProviderTruncated   = errors.New("provider output was truncated")
	ErrContentFiltered     = errors.New("provider output was content filtered")
	ErrProviderFinishError = errors.New("provider returned error finish reason")
	ErrStopHookLoop        = errors.New("stop hook requested too many continuations")
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
	RunID           string
	SessionID       string
	TraceID         string
	Step            int
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

type Options struct {
	RunID                    string
	SessionID                string
	TraceID                  string
	ProviderName             string
	Model                    string
	CacheNamespace           string
	CacheRetention           promptcache.Retention
	ContextPolicy            contextpolicy.Policy
	MaxEmptyProviderRetries  int
	NoProgressLimit          int
	DuplicateToolLimit       int
	WallTime                 time.Duration
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	ToolDefinitions          []provider.ToolDefinition
	HostedToolDefinitions    []provider.HostedToolDefinition
	CompletionPolicy         CompletionPolicy
	MaxLengthContinuations   int
	MaxStopHookContinuations int
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
}

type RunDecision struct {
	CompletionReason   CompletionReason
	ContinuationReason ContinuationReason
	FinishReason       provider.FinishReason
	RawFinishReason    string
	FinishInferred     bool
	Detail             string
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
}

type RunInput struct {
	RunID     string
	SessionID string
	TraceID   string
	History   []session.Message
}

type CompactionManager interface {
	Compact(context.Context, CompactionRequest) (compaction.Result, []session.Message, error)
}

type CompactionRequest struct {
	RunID                string
	SessionID            string
	TraceID              string
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
	Provider  provider.Provider
	Tools     *tools.Registry
	Store     session.Store
	Prompt    promptcache.Store
	Memory    *memory.Manager
	Sink      event.Sink
	Approver  tools.Approver
	StopHook  StopHook
	Compactor CompactionManager
	Options   Options
}

type turnState struct {
	activeMessages []session.Message
}

type Config struct {
	Provider  provider.Provider
	Tools     *tools.Registry
	Store     session.Store
	Prompt    promptcache.Store
	Memory    *memory.Manager
	Sink      event.Sink
	Approver  tools.Approver
	StopHook  StopHook
	Compactor CompactionManager
	Options   Options
}

func New(cfg Config) (*Engine, error) {
	if cfg.Provider == nil {
		return nil, errors.New("provider is required")
	}
	return &Engine{
		Provider:  cfg.Provider,
		Tools:     cfg.Tools,
		Store:     cfg.Store,
		Prompt:    cfg.Prompt,
		Memory:    cfg.Memory,
		Sink:      cfg.Sink,
		Approver:  cfg.Approver,
		StopHook:  cfg.StopHook,
		Compactor: cfg.Compactor,
		Options:   cfg.Options,
	}, nil
}

type LocalCompactionManager struct {
	Generator compaction.SummaryGenerator
	Now       func() time.Time
}

func (m LocalCompactionManager) Compact(ctx context.Context, req CompactionRequest) (compaction.Result, []session.Message, error) {
	now := time.Now()
	if m.Now != nil {
		now = m.Now()
	}
	generator := m.Generator
	if generator == nil {
		generator = compaction.ExtractiveSummaryGenerator{}
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
	}, generator)
	if err != nil {
		return compaction.Result{}, nil, err
	}
	return prep.Result, prep.ActiveMessages, nil
}

func (e *Engine) Run(ctx context.Context, userText string) Result {
	runner, err := e.runner(e.Store, e.Options)
	if err != nil {
		return Result{Status: Failed, Err: err}
	}
	return runner.run(ctx, userText)
}

func (e *Engine) RunTurn(ctx context.Context, input RunInput) Result {
	store := session.NewMemoryStore()
	opts := e.Options
	if input.RunID != "" {
		opts.RunID = input.RunID
	}
	if input.SessionID != "" {
		opts.SessionID = input.SessionID
	}
	if input.TraceID != "" {
		opts.TraceID = input.TraceID
	}
	opts = normalizeOptions(opts)
	if len(input.History) > 0 {
		if err := store.Append(opts.RunID, input.History...); err != nil {
			return Result{Status: Failed, Err: err}
		}
	}
	runner, err := e.runner(store, opts)
	if err != nil {
		return Result{Status: Failed, Err: err}
	}
	return runner.run(ctx, "")
}

func (e *Engine) runner(store session.Store, opts Options) (*Engine, error) {
	if e.Provider == nil {
		return nil, errors.New("provider is required")
	}
	if store == nil {
		store = session.NewMemoryStore()
	}
	prompt := e.Prompt
	if prompt == nil {
		prompt = promptcache.NewMemoryStore()
	}
	mem := e.Memory
	if mem == nil {
		mem = &memory.Manager{}
	}
	registry := e.Tools
	if registry == nil {
		registry = tools.NewRegistry()
	}
	opts = normalizeOptions(opts)
	if len(opts.ToolDefinitions) == 0 {
		opts.ToolDefinitions = registry.Definitions()
	}
	if err := validateConfiguredTools(opts.ToolDefinitions, opts.HostedToolDefinitions, false); err != nil {
		return nil, err
	}
	return &Engine{
		Provider:  e.Provider,
		Tools:     registry,
		Store:     store,
		Prompt:    prompt,
		Memory:    mem,
		Sink:      e.Sink,
		Approver:  e.Approver,
		StopHook:  e.StopHook,
		Compactor: e.Compactor,
		Options:   opts,
	}, nil
}

func (e *Engine) run(ctx context.Context, userText string) Result {
	state := &turnState{}
	opts := normalizeOptions(e.Options)
	if len(opts.ToolDefinitions) == 0 && e.Tools != nil {
		opts.ToolDefinitions = e.Tools.Definitions()
	}
	if opts.WallTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.WallTime)
		defer cancel()
	}
	if err := validateConfiguredTools(opts.ToolDefinitions, opts.HostedToolDefinitions, false); err != nil {
		return Result{Status: Failed, Err: err}
	}
	opts.ToolDefinitions = appendControlToolDefinitions(opts.ToolDefinitions, opts.CompletionPolicy)
	if err := validateConfiguredTools(opts.ToolDefinitions, opts.HostedToolDefinitions, true); err != nil {
		return Result{Status: Failed, Err: err}
	}
	if userText != "" {
		if err := e.Store.Append(opts.RunID, session.Message{Role: session.User, Content: userText}); err != nil {
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
	activeHistory, err := e.Store.Messages(opts.RunID)
	if err != nil {
		return Result{Status: Failed, Err: err}
	}
	state.activeMessages = append([]session.Message(nil), activeHistory...)
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return e.end(state, opts, step, Cancelled, output, ctx.Err(), metrics, started, RunDecision{})
		}
		metrics.Steps = step
		e.emit(event.Event{Type: event.StepStart, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model})
		var compacted bool
		activeHistory, compacted, err = e.maybeCompact(ctx, opts, step, activeHistory, compaction.TriggerPreRequest, compaction.ReasonThreshold, &metrics, &compactionFailures)
		if err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{ContinuationReason: ContinueCompaction})
		}
		if compacted {
			noProgress = 0
			duplicateCount = 0
		}
		state.activeMessages = append([]session.Message(nil), activeHistory...)
		req, err := e.providerRequest(ctx, opts, step, activeHistory)
		if err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		metrics.LLMRequests++
		e.emit(event.Event{Type: event.ProviderRequest, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: fmt.Sprintf("%d messages, %d raw segments, %d local tools, %d hosted tools, prefix %s", len(req.Messages), len(req.RawPlan.Segments), len(req.Tools), len(req.HostedTools), shortHash(req.RawPlan.PrefixHash))})
		providerStarted := time.Now()
		stream, err := e.Provider.Stream(ctx, req)
		if errors.Is(err, provider.ErrContextOverflow) {
			activeHistory, _, err = e.forceCompact(ctx, opts, step, activeHistory, compaction.TriggerOverflow, compaction.ReasonProviderOverflow, &metrics, &compactionFailures)
			if err != nil {
				return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{})
			}
			e.emitStepEnd(opts, step, time.Since(providerStarted).Milliseconds(), 0, provider.Usage{}, 0, RunDecision{ContinuationReason: ContinueCompaction})
			state.activeMessages = append([]session.Message(nil), activeHistory...)
			continue
		}
		if err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		stepOutput, err := e.consume(ctx, opts, step, stream)
		providerLatency := time.Since(providerStarted).Milliseconds()
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
		_ = e.Prompt.AppendProviderResponse(ctx, promptcache.ProviderResponseRecord{
			RequestID:          fmt.Sprintf("%s:req:%d", opts.RunID, step),
			RunID:              opts.RunID,
			ThreadID:           opts.SessionID,
			TurnID:             opts.RunID,
			ProviderResponseID: stepOutput.ResponseID,
			CacheReadTokens:    usage.CacheReadTokens,
			CacheWriteTokens:   usage.CacheWriteTokens,
			CreatedAt:          time.Now(),
		})
		decision := RunDecision{FinishReason: stepOutput.FinishReason, RawFinishReason: stepOutput.RawFinishReason, FinishInferred: stepOutput.FinishInferred}
		if stepOutput.FinishReason != "" {
			e.emit(event.Event{Type: event.ProviderFinish, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, FinishReason: string(stepOutput.FinishReason), RawFinishReason: stepOutput.RawFinishReason, FinishInferred: stepOutput.FinishInferred})
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
			msg := session.Message{Role: session.Assistant, Content: stepText, Reasoning: stepReasoning}
			if err := e.Store.Append(opts.RunID, msg); err != nil {
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
			contextUsage := contextpolicy.EstimateMessages(e.Memory.SystemPrompt, activeHistory, len(opts.ToolDefinitions)+len(opts.HostedToolDefinitions), opts.ContextPolicy)
			if contextUsage.CompactionNeeded || contextUsage.TokenPressureHigh {
				activeHistory, _, err = e.forceCompact(ctx, opts, step, activeHistory, compaction.TriggerPostResponse, compaction.ReasonOutputContinuation, &metrics, &compactionFailures)
				if err != nil {
					return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
				}
				noProgress = 0
				duplicateCount = 0
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
			e.emit(event.Event{Type: event.ProviderRetry, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: "empty provider output"})
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
					if err := e.Store.Append(opts.RunID, session.Message{Role: session.User, Content: prompt}); err != nil {
						return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
					}
					activeHistory = append(activeHistory, session.Message{Role: session.User, Content: prompt})
					state.activeMessages = append([]session.Message(nil), activeHistory...)
					decision.ContinuationReason = ContinueHook
					decision.Detail = strings.TrimSpace(hook.Reason)
					e.emit(event.Event{Type: event.ContextContinue, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: prompt, ContinuationReason: string(ContinueHook), Result: decision.Detail})
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
		for _, call := range calls {
			reasoning := call.Reasoning
			if reasoning == "" {
				reasoning = stepReasoning
			}
			msg := session.Message{Role: session.Assistant, Content: "tool_call", Reasoning: reasoning, ToolCallID: call.ID, ToolName: call.Name, ToolArgs: call.Args}
			if err := e.Store.Append(opts.RunID, msg); err != nil {
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
		if final, ok, err := completionSignal(opts, calls); err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
		} else if ok {
			decision.CompletionReason = CompletionReasonToolSignal
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			return e.end(state, opts, step, Completed, final, nil, metrics, started, decision)
		}
		if prompt, ok, err := askUserSignal(calls); err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
		} else if ok {
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			return e.end(state, opts, step, Waiting, prompt, nil, metrics, started, decision)
		}
		metrics.ToolCalls += len(calls)
		if budgetErr := e.checkBudget(opts, metrics, step); budgetErr != nil {
			return e.end(state, opts, step, Failed, output, budgetErr, metrics, started, decision)
		}
		for i, call := range calls {
			e.emit(event.Event{Type: event.ToolCall, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: call.ID, ToolName: call.Name, ToolKind: "local", Args: call.Args, Metadata: map[string]any{"batch_index": i, "batch_size": len(calls)}})
		}
		toolStarted := time.Now()
		results := e.Tools.RunBatchWithOptions(ctx, calls, e.Approver, tools.RunOptions{RunID: opts.RunID, SessionID: opts.SessionID, Step: step})
		toolLatency := time.Since(toolStarted).Milliseconds()
		for i, result := range results {
			result = tools.ApplyResultLimit(result, e.Tools.LimitFor(result.Name))
			text := result.Text
			errText := ""
			if result.IsError {
				errText = text
				text = "ERROR: " + text
			}
			e.emit(event.Event{Type: event.ToolResult, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: result.CallID, ToolName: result.Name, ToolKind: "local", Result: text, Err: errText, Duration: toolLatency, Metadata: mergeToolResultMetadata(result.Metadata, i, len(results)), Artifacts: eventArtifacts(result.Artifacts)})
			msg := session.Message{Role: session.Tool, Content: text, ToolCallID: result.CallID, ToolName: result.Name}
			if err := e.Store.Append(opts.RunID, msg); err != nil {
				return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
			}
			activeHistory = append(activeHistory, msg)
			state.activeMessages = append([]session.Message(nil), activeHistory...)
		}
		activeHistory, compacted, err = e.maybeCompact(ctx, opts, step, activeHistory, compaction.TriggerPostResponse, compaction.ReasonFollowUpPressure, &metrics, &compactionFailures)
		if err != nil {
			return e.end(state, opts, step, Failed, output, err, metrics, started, decision)
		}
		if compacted {
			noProgress = 0
			duplicateCount = 0
		}
		state.activeMessages = append([]session.Message(nil), activeHistory...)
		decision.ContinuationReason = ContinueToolResults
		e.emitStepEnd(opts, step, providerLatency, toolLatency, usage, len(calls), decision)
	}
}

func normalizeOptions(o Options) Options {
	if o.RunID == "" {
		o.RunID = "default"
	}
	if o.SessionID == "" {
		o.SessionID = o.RunID
	}
	if o.TraceID == "" {
		o.TraceID = o.RunID
	}
	if o.CacheNamespace == "" {
		o.CacheNamespace = promptcache.DefaultNamespace(o.SessionID, o.ProviderName, o.Model)
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
	return o
}

func (e *Engine) applyStopHook(ctx context.Context, opts Options, step int, lastAssistant session.Message, metrics RunMetrics, stepOutput StepOutput) (StopHookResult, error) {
	if e.StopHook == nil {
		return StopHookResult{}, nil
	}
	messages, err := e.Store.Messages(opts.RunID)
	if err != nil {
		return StopHookResult{}, err
	}
	return e.StopHook(ctx, StopHookContext{
		RunID:           opts.RunID,
		SessionID:       opts.SessionID,
		TraceID:         opts.TraceID,
		Step:            step,
		LastAssistant:   lastAssistant,
		Messages:        messages,
		FinishReason:    stepOutput.FinishReason,
		RawFinishReason: stepOutput.RawFinishReason,
		FinishInferred:  stepOutput.FinishInferred,
		Metrics:         metrics,
	})
}

func (e *Engine) providerRequest(ctx context.Context, opts Options, step int, history []session.Message) (provider.Request, error) {
	toolset, _, err := promptcache.EnsureCurrentToolsetWithOptions(ctx, e.Prompt, opts.RunID, opts.SessionID, opts.ProviderName, opts.Model, convertToolDefinitions(opts.ToolDefinitions), convertHostedToolDefinitions(opts.HostedToolDefinitions), time.Now(), promptcache.ToolsetOptions{AllowControlTools: true})
	if err != nil {
		return provider.Request{}, err
	}
	plan, messages, err := promptcache.BuildPlan(ctx, e.Prompt, promptcache.BuildInput{
		RunID:          opts.RunID,
		SessionID:      opts.SessionID,
		Provider:       opts.ProviderName,
		Model:          opts.Model,
		AdapterVersion: promptcache.Version,
		CacheNamespace: opts.CacheNamespace,
		SystemPrompt:   e.Memory.SystemPrompt,
		History:        providerSafeHistory(e.Memory.Assemble(history)[systemOffset(e.Memory):]),
		Toolset:        toolset,
		HostedTools:    convertHostedToolDefinitions(opts.HostedToolDefinitions),
		Renderer:       rendererForProvider(e.Provider),
		ContextPolicy:  opts.ContextPolicy,
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
	cache := promptcache.CachePolicy{
		Enabled:            true,
		Namespace:          opts.CacheNamespace,
		Retention:          opts.CacheRetention,
		PreferContinuation: true,
	}
	if cache.Retention == "" {
		if defaults, ok := e.Provider.(provider.CacheRetentionDefault); ok {
			cache.Retention = defaults.DefaultCacheRetention()
		} else {
			cache.Retention = promptcache.RetentionInMemory
		}
	}
	cache.Enabled = cache.Retention != promptcache.RetentionNone
	if normalizer, ok := e.Provider.(provider.CachePolicyNormalizer); ok {
		cache, err = normalizer.NormalizeCachePolicy(cache)
		if err != nil {
			return provider.Request{}, err
		}
	}
	req := provider.Request{
		RunID:           opts.RunID,
		Step:            step,
		Provider:        opts.ProviderName,
		Model:           opts.Model,
		Messages:        messages,
		Tools:           activeTools,
		HostedTools:     activeHostedTools,
		RawPlan:         plan,
		Cache:           cache,
		ContextPolicy:   opts.ContextPolicy,
		ContextUsage:    contextpolicy.EstimateMessages(e.Memory.SystemPrompt, history, len(activeTools)+len(activeHostedTools), opts.ContextPolicy),
		MaxOutputTokens: opts.ContextPolicy.MaxOutputTokens,
	}
	if hasher, ok := e.Provider.(provider.PayloadHasher); ok {
		payloadHash, err := hasher.PayloadHash(req)
		if err != nil {
			return provider.Request{}, err
		}
		req.RawPlan.PayloadHash = payloadHash
	}
	if _, err := promptcache.RecordRequest(ctx, e.Prompt, opts.RunID, opts.SessionID, step, opts.ProviderName, opts.Model, cache, req.RawPlan); err != nil {
		return provider.Request{}, err
	}
	return req, nil
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
	_, _, err := promptcache.NormalizeToolsetChecked(convertToolDefinitions(local), convertHostedToolDefinitions(hosted), promptcache.ToolsetOptions{AllowControlTools: allowControl})
	return err
}

func (e *Engine) maybeCompact(ctx context.Context, opts Options, step int, history []session.Message, trigger compaction.Trigger, reason compaction.Reason, metrics *RunMetrics, failures *int) ([]session.Message, bool, error) {
	usage := contextpolicy.EstimateMessages(e.Memory.SystemPrompt, history, len(opts.ToolDefinitions)+len(opts.HostedToolDefinitions), opts.ContextPolicy)
	if !usage.CompactionNeeded && !usage.TokenPressureHigh {
		return history, false, nil
	}
	next, err := e.runCompaction(ctx, opts, step, history, trigger, reason, usage, failures)
	if err != nil {
		return history, false, err
	}
	if metrics != nil {
		metrics.Compactions++
	}
	return next, true, nil
}

func (e *Engine) forceCompact(ctx context.Context, opts Options, step int, history []session.Message, trigger compaction.Trigger, reason compaction.Reason, metrics *RunMetrics, failures *int) ([]session.Message, compaction.Result, error) {
	usage := contextpolicy.EstimateMessages(e.Memory.SystemPrompt, history, len(opts.ToolDefinitions)+len(opts.HostedToolDefinitions), opts.ContextPolicy)
	next, err := e.runCompaction(ctx, opts, step, history, trigger, reason, usage, failures)
	if err != nil {
		return history, compaction.Result{}, err
	}
	if metrics != nil {
		metrics.Compactions++
	}
	return next, latestCompaction(history, next), nil
}

func (e *Engine) runCompaction(ctx context.Context, opts Options, step int, history []session.Message, trigger compaction.Trigger, reason compaction.Reason, usage contextpolicy.Usage, failures *int) ([]session.Message, error) {
	if failures != nil && *failures >= opts.ContextPolicy.MaxCompactionFailures {
		return nil, fmt.Errorf("compaction failure circuit breaker reached after %d failures", *failures)
	}
	manager := e.Compactor
	if manager == nil {
		manager = LocalCompactionManager{}
	}
	e.emit(event.Event{
		Type:      event.ContextCompact,
		TraceID:   opts.TraceID,
		RunID:     opts.RunID,
		SessionID: opts.SessionID,
		Step:      step,
		Provider:  opts.ProviderName,
		Model:     opts.Model,
		Message:   fmt.Sprintf("%s/%s", trigger, reason),
		Metrics:   usage,
	})
	result, active, err := manager.Compact(ctx, CompactionRequest{
		RunID:        opts.RunID,
		SessionID:    opts.SessionID,
		TraceID:      opts.TraceID,
		Step:         step,
		History:      history,
		Policy:       opts.ContextPolicy,
		Trigger:      trigger,
		Reason:       reason,
		Phase:        compaction.PhaseGenerate,
		Provider:     e.Provider,
		ProviderName: opts.ProviderName,
		Model:        opts.Model,
		ContextUsage: usage,
		Details: map[string]string{
			"run_id":               opts.RunID,
			"session_id":           opts.SessionID,
			"context_window":       fmt.Sprintf("%d", opts.ContextPolicy.ContextWindowTokens),
			"threshold_tokens":     fmt.Sprintf("%d", usage.ThresholdTokens),
			"tokens_before":        fmt.Sprintf("%d", usage.InputTokens),
			"consecutive_failures": fmt.Sprintf("%d", derefInt(failures)),
		},
	})
	if err != nil {
		if failures != nil {
			*failures++
		}
		return nil, err
	}
	if failures != nil {
		*failures = 0
	}
	e.emit(event.Event{
		Type:      event.ContextCompact,
		TraceID:   opts.TraceID,
		RunID:     opts.RunID,
		SessionID: opts.SessionID,
		Step:      step,
		Provider:  opts.ProviderName,
		Model:     opts.Model,
		Message:   result.CompactionID,
		Result:    result.Summary,
		Metrics:   result,
	})
	return active, nil
}

func latestCompaction(_, active []session.Message) compaction.Result {
	for i := len(active) - 1; i >= 0; i-- {
		if active[i].Kind != session.MessageKindCompactionSummary {
			continue
		}
		return compaction.Result{CompactionID: active[i].CompactionID, Summary: active[i].Content}
	}
	return compaction.Result{}
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

func rendererForProvider(p provider.Provider) promptcache.Renderer {
	renderer, _ := p.(promptcache.Renderer)
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
				e.emit(event.Event{Type: event.ProviderDelta, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: ev.Text})
			case provider.Reasoning:
				out.Reasoning += ev.Text
				e.emit(event.Event{Type: event.ProviderReasoning, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: ev.Text})
			case provider.ToolCalls:
				out.Calls = append(out.Calls, ev.ToolCalls...)
			case provider.HostedToolCall:
				if err := validateHostedToolEvent(ev.ToolCall, hostedTools); err != nil {
					return out, err
				}
				e.emit(event.Event{Type: event.HostedToolCall, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: ev.ToolCall.ID, ToolName: ev.ToolCall.Name, ToolKind: "hosted", Args: ev.ToolCall.Args})
			case provider.HostedToolResult:
				if err := validateHostedToolEvent(ev.ToolCall, hostedTools); err != nil {
					return out, err
				}
				e.emit(event.Event{Type: event.HostedToolResult, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: ev.ToolCall.ID, ToolName: ev.ToolCall.Name, ToolKind: "hosted", Result: ev.Text})
			case provider.UsageEvent:
				out.Usage = out.Usage.Add(ev.Usage)
				e.emit(event.Event{Type: event.ProviderUsage, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Metrics: ev.Usage.Normalized()})
			case provider.Empty:
				out.Retry = true
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, false, false, false)
				if err := validateNoEventAfterTerminal(ctx, stream, &validator); err != nil {
					return out, err
				}
				return out, nil
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

func providerSafeHistory(history []session.Message) []session.Message {
	return control.ProjectHistory(history)
}

func convertToolDefinitions(defs []provider.ToolDefinition) []promptcache.ToolDefinition {
	out := make([]promptcache.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, promptcache.ToolDefinition{
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

func convertHostedToolDefinitions(defs []provider.HostedToolDefinition) []promptcache.HostedToolDefinition {
	out := make([]promptcache.HostedToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, promptcache.HostedToolDefinition{
			Name:        def.Name,
			Type:        def.Type,
			Description: def.Description,
			Parameters:  def.Parameters,
			Options:     def.Options,
		})
	}
	return out
}

func providerToolDefinitions(defs []promptcache.ToolDefinition) []provider.ToolDefinition {
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

func hostedToolDefinitions(defs []promptcache.HostedToolDefinition) []provider.HostedToolDefinition {
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

func appendControlToolDefinitions(defs []provider.ToolDefinition, policy CompletionPolicy) []provider.ToolDefinition {
	out := append([]provider.ToolDefinition(nil), defs...)
	seen := map[string]struct{}{}
	for _, def := range out {
		seen[strings.TrimSpace(def.Name)] = struct{}{}
	}
	for _, def := range control.ToolDefinitions(policy == CompletionExplicitSignal) {
		if _, ok := seen[def.Name]; ok {
			continue
		}
		out = append(out, def)
	}
	return out
}

func eventArtifacts(items []tools.ArtifactRef) []event.Artifact {
	out := make([]event.Artifact, 0, len(items))
	for _, item := range items {
		out = append(out, event.Artifact{Kind: item.Kind, Path: item.Path, MIME: item.MIME})
	}
	return out
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
	e.emit(event.Event{
		Type:               event.StepEnd,
		TraceID:            opts.TraceID,
		RunID:              opts.RunID,
		SessionID:          opts.SessionID,
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
	e.emit(event.Event{Type: event.RunEnd, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: string(status), Result: output, Err: errText, FinishReason: string(decision.FinishReason), RawFinishReason: decision.RawFinishReason, FinishInferred: decision.FinishInferred, CompletionReason: string(decision.CompletionReason), ContinuationReason: string(decision.ContinuationReason), Metrics: metrics})
	var messages []session.Message
	if len(state.activeMessages) > 0 {
		messages = append([]session.Message(nil), state.activeMessages...)
	} else if e.Store != nil {
		messages, _ = e.Store.Messages(opts.RunID)
	}
	return Result{Status: status, Output: output, Err: err, Metrics: metrics, Messages: messages, CompletionReason: decision.CompletionReason, ContinuationReason: decision.ContinuationReason, FinishReason: decision.FinishReason, RawFinishReason: decision.RawFinishReason, FinishInferred: decision.FinishInferred}
}

func (e *Engine) emit(ev event.Event) {
	if e.Sink == nil {
		return
	}
	ev.Timestamp = time.Now()
	e.Sink.Emit(ev)
}

func (e *Engine) checkBudget(opts Options, metrics RunMetrics, step int) error {
	var err error
	message := ""
	switch {
	case opts.MaxTotalTokens > 0 && metrics.Usage.Normalized().TotalTokens > opts.MaxTotalTokens:
		err = fmt.Errorf("token budget exceeded")
		message = fmt.Sprintf("total tokens %d exceeded limit %d", metrics.Usage.Normalized().TotalTokens, opts.MaxTotalTokens)
		e.emit(event.Event{Type: event.BudgetExceeded, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: message, Metrics: BudgetMetrics{Type: "tokens", Used: float64(metrics.Usage.Normalized().TotalTokens), Limit: float64(opts.MaxTotalTokens), Run: metrics}})
	case opts.MaxCostUSD > 0 && metrics.Usage.CostUSD > opts.MaxCostUSD:
		err = fmt.Errorf("cost budget exceeded")
		message = fmt.Sprintf("cost %.6f exceeded limit %.6f", metrics.Usage.CostUSD, opts.MaxCostUSD)
		e.emit(event.Event{Type: event.BudgetExceeded, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: message, Metrics: BudgetMetrics{Type: "cost", Used: metrics.Usage.CostUSD, Limit: opts.MaxCostUSD, Run: metrics}})
	case opts.MaxToolCalls > 0 && metrics.ToolCalls > opts.MaxToolCalls:
		err = fmt.Errorf("tool call budget exceeded")
		message = fmt.Sprintf("tool calls %d exceeded limit %d", metrics.ToolCalls, opts.MaxToolCalls)
		e.emit(event.Event{Type: event.BudgetExceeded, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: message, Metrics: BudgetMetrics{Type: "tool_calls", Used: float64(metrics.ToolCalls), Limit: float64(opts.MaxToolCalls), Run: metrics}})
	}
	return err
}

func completionSignal(opts Options, calls []provider.ToolCall) (string, bool, error) {
	if opts.CompletionPolicy != CompletionExplicitSignal {
		return "", false, nil
	}
	for _, call := range calls {
		signal, ok, err := control.Project(call)
		if err != nil {
			return "", false, err
		}
		if ok && signal.Kind == control.SignalTaskComplete {
			return signal.Output, true, nil
		}
	}
	return "", false, nil
}

func askUserSignal(calls []provider.ToolCall) (string, bool, error) {
	for _, call := range calls {
		signal, ok, err := control.Project(call)
		if err != nil {
			return "", false, err
		}
		if ok && signal.Kind == control.SignalAskUser {
			return signal.Prompt, true, nil
		}
	}
	return "", false, nil
}

func validateToolCalls(calls []provider.ToolCall) error {
	seen := map[string]struct{}{}
	controlCalls := 0
	ordinaryCalls := 0
	for _, call := range calls {
		if control.IsControlTool(call.Name) {
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

func isControlTool(name string) bool {
	return control.IsControlTool(name)
}

func toolSignature(calls []provider.ToolCall) string {
	s := ""
	for _, c := range calls {
		s += c.Name + "\x00" + c.Args + "\x00"
	}
	return s
}
