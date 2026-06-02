package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/event"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

var (
	ErrMaxSteps            = errors.New("agent loop reached max steps")
	ErrNoProgress          = errors.New("agent loop made no progress")
	ErrDuplicateTools      = errors.New("agent loop repeated identical tool calls")
	ErrDuplicateToolCallID = errors.New("provider returned duplicate tool call id")
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
	MaxSteps                 int
	HardMaxSteps             int
	MaxEmptyProviderRetries  int
	NoProgressLimit          int
	DuplicateToolLimit       int
	WallTime                 time.Duration
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	ToolDefinitions          []provider.ToolDefinition
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

type Engine struct {
	Provider provider.Provider
	Tools    *tools.Registry
	Store    session.Store
	Prompt   promptcache.Store
	Memory   *memory.Manager
	Sink     event.Sink
	Approver tools.Approver
	StopHook StopHook
	Options  Options
}

func (e *Engine) Run(ctx context.Context, userText string) Result {
	return e.run(ctx, userText)
}

func (e *Engine) RunTurn(ctx context.Context, input RunInput) Result {
	originalStore := e.Store
	originalOptions := e.Options
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
	e.Store = store
	e.Options = opts
	defer func() {
		e.Store = originalStore
		e.Options = originalOptions
	}()
	return e.run(ctx, "")
}

func (e *Engine) run(ctx context.Context, userText string) Result {
	if e.Provider == nil {
		return Result{Status: Failed, Err: errors.New("provider is required")}
	}
	if e.Store == nil {
		e.Store = session.NewMemoryStore()
	}
	if e.Prompt == nil {
		e.Prompt = promptcache.NewMemoryStore()
	}
	if e.Memory == nil {
		e.Memory = &memory.Manager{}
	}
	if e.Tools == nil {
		e.Tools = tools.NewRegistry()
	}
	opts := normalizeOptions(e.Options)
	if len(opts.ToolDefinitions) == 0 && e.Tools != nil {
		opts.ToolDefinitions = e.Tools.Definitions()
	}
	if opts.WallTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.WallTime)
		defer cancel()
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
	lastToolSig := ""
	duplicateCount := 0
	started := time.Now()
	metrics := RunMetrics{}
	for step := 1; step <= opts.HardMaxSteps; step++ {
		if step > opts.MaxSteps {
			return e.end(opts, step, Failed, output, ErrMaxSteps, metrics, started, RunDecision{})
		}
		if ctx.Err() != nil {
			return e.end(opts, step, Cancelled, output, ctx.Err(), metrics, started, RunDecision{})
		}
		metrics.Steps = step
		e.emit(event.Event{Type: event.StepStart, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model})
		history, err := e.Store.Messages(opts.RunID)
		if err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		req, err := e.providerRequest(ctx, opts, step, history)
		if err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		metrics.LLMRequests++
		e.emit(event.Event{Type: event.ProviderRequest, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: fmt.Sprintf("%d messages, %d raw segments, prefix %s", len(req.Messages), len(req.RawPlan.Segments), shortHash(req.RawPlan.PrefixHash))})
		providerStarted := time.Now()
		stream, err := e.Provider.Stream(ctx, req)
		if errors.Is(err, provider.ErrContextOverflow) {
			metrics.Compactions++
			if err := e.compact(opts, step); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started, RunDecision{})
			}
			e.emitStepEnd(opts, step, time.Since(providerStarted).Milliseconds(), 0, provider.Usage{}, 0, RunDecision{ContinuationReason: ContinueCompaction})
			continue
		}
		if err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started, RunDecision{})
		}
		stepOutput, err := e.consume(ctx, opts, step, stream)
		providerLatency := time.Since(providerStarted).Milliseconds()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return e.end(opts, step, Cancelled, output, err, metrics, started, RunDecision{})
			}
			return e.end(opts, step, Failed, output, err, metrics, started, RunDecision{})
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
			return e.end(opts, step, Failed, output, budgetErr, metrics, started, decision)
		}
		if stepOutput.Truncated || stepOutput.FinishReason == provider.FinishLength {
			lengthContinuations++
			if lengthContinuations > opts.MaxLengthContinuations {
				return e.end(opts, step, Failed, output, ErrProviderTruncated, metrics, started, decision)
			}
			metrics.Compactions++
			if err := e.compact(opts, step); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started, decision)
			}
			decision.ContinuationReason = ContinueProviderTruncated
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			continue
		}
		if stepOutput.FinishReason == provider.FinishContentFilter {
			return e.end(opts, step, Failed, output, ErrContentFiltered, metrics, started, decision)
		}
		if stepOutput.FinishReason == provider.FinishError {
			return e.end(opts, step, Failed, output, ErrProviderFinishError, metrics, started, decision)
		}
		if stepOutput.FinishReason == provider.FinishCancelled {
			return e.end(opts, step, Cancelled, output, context.Canceled, metrics, started, decision)
		}
		if stepOutput.Retry {
			emptyRetries++
			if emptyRetries > opts.MaxEmptyProviderRetries {
				return e.end(opts, step, Failed, output, errors.New("provider returned empty output"), metrics, started, decision)
			}
			metrics.Retries++
			e.emit(event.Event{Type: event.ProviderRetry, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: "empty provider output"})
			decision.ContinuationReason = ContinueRetryEmpty
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			continue
		}
		emptyRetries = 0
		lengthContinuations = 0
		if stepText != "" {
			output += stepText
			noProgress = 0
			if err := e.Store.Append(opts.RunID, session.Message{Role: session.Assistant, Content: stepText}); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started, decision)
			}
		} else if len(calls) == 0 {
			noProgress++
			if noProgress >= opts.NoProgressLimit {
				return e.end(opts, step, Failed, output, ErrNoProgress, metrics, started, decision)
			}
		}
		if len(calls) == 0 {
			if stepText != "" && provider.IsTerminalNaturalFinish(stepOutput.FinishReason) {
				hook, err := e.applyStopHook(ctx, opts, step, session.Message{Role: session.Assistant, Content: stepText}, metrics, stepOutput)
				if err != nil {
					return e.end(opts, step, Failed, output, err, metrics, started, decision)
				}
				if hook.Continue {
					stopHookContinuations++
					if stopHookContinuations > opts.MaxStopHookContinuations {
						return e.end(opts, step, Failed, output, ErrStopHookLoop, metrics, started, decision)
					}
					prompt := strings.TrimSpace(hook.Prompt)
					if prompt == "" {
						prompt = "Continue the task and address the remaining pending work."
					}
					if err := e.Store.Append(opts.RunID, session.Message{Role: session.User, Content: prompt}); err != nil {
						return e.end(opts, step, Failed, output, err, metrics, started, decision)
					}
					decision.ContinuationReason = ContinueHook
					decision.Detail = strings.TrimSpace(hook.Reason)
					e.emit(event.Event{Type: event.ContextContinue, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: prompt, ContinuationReason: string(ContinueHook), Result: decision.Detail})
					e.emitStepEnd(opts, step, providerLatency, 0, usage, 0, decision)
					continue
				}
				decision.CompletionReason = CompletionReasonNaturalStop
				e.emitStepEnd(opts, step, providerLatency, 0, usage, 0, decision)
				return e.end(opts, step, Completed, output, nil, metrics, started, decision)
			}
			decision.ContinuationReason = ContinueNoProgress
			e.emitStepEnd(opts, step, providerLatency, 0, usage, 0, decision)
			continue
		}
		if err := validateToolCalls(calls); err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started, decision)
		}
		for _, call := range calls {
			if err := e.Store.Append(opts.RunID, session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: call.ID, ToolName: call.Name, ToolArgs: call.Args}); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started, decision)
			}
		}
		sig := toolSignature(calls)
		if sig == lastToolSig {
			duplicateCount++
			if duplicateCount >= opts.DuplicateToolLimit {
				return e.end(opts, step, Failed, output, ErrDuplicateTools, metrics, started, decision)
			}
		} else {
			lastToolSig = sig
			duplicateCount = 0
		}
		if final, ok := completionSignal(opts, calls); ok {
			decision.CompletionReason = CompletionReasonToolSignal
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			return e.end(opts, step, Completed, final, nil, metrics, started, decision)
		}
		if prompt, ok := askUserSignal(calls); ok {
			e.emitStepEnd(opts, step, providerLatency, 0, usage, len(calls), decision)
			return e.end(opts, step, Waiting, prompt, nil, metrics, started, decision)
		}
		metrics.ToolCalls += len(calls)
		if budgetErr := e.checkBudget(opts, metrics, step); budgetErr != nil {
			return e.end(opts, step, Failed, output, budgetErr, metrics, started, decision)
		}
		for _, call := range calls {
			e.emit(event.Event{Type: event.ToolCall, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: call.ID, ToolName: call.Name, Args: call.Args})
		}
		toolStarted := time.Now()
		results := e.Tools.RunBatch(ctx, calls, e.Approver)
		toolLatency := time.Since(toolStarted).Milliseconds()
		for _, result := range results {
			text := result.Text
			errText := ""
			if result.Err != nil {
				errText = result.Err.Error()
				text = "ERROR: " + errText
			}
			e.emit(event.Event{Type: event.ToolResult, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: result.Call.ID, ToolName: result.Call.Name, Result: text, Err: errText, Duration: toolLatency})
			if err := e.Store.Append(opts.RunID, session.Message{Role: session.Tool, Content: text, ToolCallID: result.Call.ID, ToolName: result.Call.Name}); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started, decision)
			}
		}
		decision.ContinuationReason = ContinueToolResults
		e.emitStepEnd(opts, step, providerLatency, toolLatency, usage, len(calls), decision)
	}
	return e.end(opts, opts.HardMaxSteps, Failed, output, ErrMaxSteps, metrics, started, RunDecision{})
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
	if o.MaxSteps <= 0 {
		o.MaxSteps = 16
	}
	if o.HardMaxSteps <= 0 || o.HardMaxSteps < o.MaxSteps {
		o.HardMaxSteps = o.MaxSteps
	}
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
	toolset, _, err := promptcache.EnsureCurrentToolset(ctx, e.Prompt, opts.RunID, opts.SessionID, opts.ProviderName, opts.Model, convertToolDefinitions(opts.ToolDefinitions), time.Now())
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
		History:        providerSafeHistory(boundedHistory(e.Memory, history)),
		Toolset:        toolset,
		Renderer:       rendererForProvider(e.Provider),
		Now:            time.Now(),
	})
	if err != nil {
		return provider.Request{}, err
	}
	activeTools := providerToolDefinitions(toolset.Tools)
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
		RunID:    opts.RunID,
		Step:     step,
		Provider: opts.ProviderName,
		Model:    opts.Model,
		Messages: messages,
		Tools:    activeTools,
		RawPlan:  plan,
		Cache:    cache,
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

func rendererForProvider(p provider.Provider) promptcache.Renderer {
	renderer, _ := p.(promptcache.Renderer)
	return renderer
}

func (e *Engine) consume(ctx context.Context, opts Options, step int, stream <-chan provider.StreamEvent) (StepOutput, error) {
	out := StepOutput{}
	for {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case ev, ok := <-stream:
			if !ok {
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
			switch ev.Type {
			case provider.Delta:
				out.Text += ev.Text
				e.emit(event.Event{Type: event.ProviderDelta, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: ev.Text})
			case provider.ToolCalls:
				out.Calls = append(out.Calls, ev.ToolCalls...)
			case provider.UsageEvent:
				out.Usage = out.Usage.Add(ev.Usage)
				e.emit(event.Event{Type: event.ProviderUsage, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Metrics: ev.Usage.Normalized()})
			case provider.Empty:
				out.Retry = true
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, false, false, false)
				return out, nil
			case provider.Truncated:
				out.Truncated = true
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, len(out.Calls) > 0, true, out.Text != "")
				return out, nil
			case provider.Done:
				out.FinishReason, out.FinishInferred = provider.NormalizeFinishReason(out.RawFinishReason, len(out.Calls) > 0, false, out.Text != "")
				if out.Text == "" && len(out.Calls) == 0 && out.FinishReason == provider.FinishUnknown {
					out.Retry = true
				}
				return out, nil
			}
		}
	}
}

func (e *Engine) compact(opts Options, step int) error {
	history, err := e.Store.Messages(opts.RunID)
	if err != nil {
		return err
	}
	e.emit(event.Event{Type: event.ContextCompact, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model})
	return e.Store.Replace(opts.RunID, e.Memory.Compact(history))
}

func boundedHistory(m *memory.Manager, history []session.Message) []session.Message {
	if m == nil || m.MaxMessages <= 0 || len(history) <= m.MaxMessages {
		return append([]session.Message(nil), history...)
	}
	messages := m.Assemble(history)
	if len(messages) > 0 && messages[0].Role == session.System && messages[0].Content == m.SystemPrompt {
		return append([]session.Message(nil), messages[1:]...)
	}
	return append([]session.Message(nil), messages...)
}

func providerSafeHistory(history []session.Message) []session.Message {
	out := make([]session.Message, 0, len(history))
	for _, msg := range history {
		if msg.Role == session.Assistant && (msg.ToolName == "ask_user" || msg.ToolName == "task_complete") {
			msg = session.Message{Role: session.Assistant, Content: signalText(msg.ToolName, msg.ToolArgs)}
		}
		out = append(out, msg)
	}
	return out
}

func signalText(name, args string) string {
	if name == "ask_user" {
		return "Agent requested user input: " + args
	}
	if name == "task_complete" {
		return "Agent completed the task: " + args
	}
	return args
}

func convertToolDefinitions(defs []provider.ToolDefinition) []promptcache.ToolDefinition {
	out := make([]promptcache.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, promptcache.ToolDefinition{Name: def.Name, Description: def.Description})
	}
	return out
}

func providerToolDefinitions(defs []promptcache.ToolDefinition) []provider.ToolDefinition {
	out := make([]provider.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, provider.ToolDefinition{Name: def.Name, Description: def.Description})
	}
	return out
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

func (e *Engine) end(opts Options, step int, status Status, output string, err error, metrics RunMetrics, started time.Time, decision RunDecision) Result {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	metrics.WallTimeMS = time.Since(started).Milliseconds()
	e.emit(event.Event{Type: event.RunEnd, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: string(status), Result: output, Err: errText, FinishReason: string(decision.FinishReason), RawFinishReason: decision.RawFinishReason, FinishInferred: decision.FinishInferred, CompletionReason: string(decision.CompletionReason), ContinuationReason: string(decision.ContinuationReason), Metrics: metrics})
	var messages []session.Message
	if e.Store != nil {
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

func completionSignal(opts Options, calls []provider.ToolCall) (string, bool) {
	if opts.CompletionPolicy != CompletionExplicitSignal {
		return "", false
	}
	for _, c := range calls {
		if c.Name == "task_complete" {
			return c.Args, true
		}
	}
	return "", false
}

func askUserSignal(calls []provider.ToolCall) (string, bool) {
	for _, c := range calls {
		if c.Name == "ask_user" {
			return c.Args, true
		}
	}
	return "", false
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
