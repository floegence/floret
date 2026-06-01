package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/floegence/floret/event"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

var (
	ErrMaxSteps            = errors.New("agent loop reached max steps")
	ErrNoProgress          = errors.New("agent loop made no progress")
	ErrDuplicateTools      = errors.New("agent loop repeated identical tool calls")
	ErrDuplicateToolCallID = errors.New("provider returned duplicate tool call id")
)

type Status string

const (
	Completed Status = "completed"
	Waiting   Status = "waiting"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
)

type Options struct {
	RunID                   string
	SessionID               string
	TraceID                 string
	ProviderName            string
	Model                   string
	MaxSteps                int
	HardMaxSteps            int
	MaxEmptyProviderRetries int
	NoProgressLimit         int
	DuplicateToolLimit      int
	WallTime                time.Duration
	MaxTotalTokens          int64
	MaxCostUSD              float64
	MaxToolCalls            int
}

type Result struct {
	Status  Status
	Output  string
	Err     error
	Metrics RunMetrics
}

type Engine struct {
	Provider provider.Provider
	Tools    *tools.Registry
	Store    session.Store
	Memory   *memory.Manager
	Sink     event.Sink
	Approver tools.Approver
	Options  Options
}

func (e *Engine) Run(ctx context.Context, userText string) Result {
	if e.Provider == nil {
		return Result{Status: Failed, Err: errors.New("provider is required")}
	}
	if e.Store == nil {
		e.Store = session.NewMemoryStore()
	}
	if e.Memory == nil {
		e.Memory = &memory.Manager{}
	}
	if e.Tools == nil {
		e.Tools = tools.NewRegistry()
	}
	opts := normalizeOptions(e.Options)
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
	lastToolSig := ""
	duplicateCount := 0
	started := time.Now()
	metrics := RunMetrics{}
	for step := 1; step <= opts.HardMaxSteps; step++ {
		if step > opts.MaxSteps {
			return e.end(opts, step, Failed, output, ErrMaxSteps, metrics, started)
		}
		if ctx.Err() != nil {
			return e.end(opts, step, Cancelled, output, ctx.Err(), metrics, started)
		}
		metrics.Steps = step
		e.emit(event.Event{Type: event.StepStart, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model})
		history, err := e.Store.Messages(opts.RunID)
		if err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started)
		}
		req := provider.Request{RunID: opts.RunID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Messages: e.Memory.Assemble(history), Tools: e.Tools.Definitions()}
		metrics.LLMRequests++
		e.emit(event.Event{Type: event.ProviderRequest, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: fmt.Sprintf("%d messages", len(req.Messages))})
		providerStarted := time.Now()
		stream, err := e.Provider.Stream(ctx, req)
		if errors.Is(err, provider.ErrContextOverflow) {
			metrics.Compactions++
			if err := e.compact(opts, step); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started)
			}
			continue
		}
		if err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started)
		}
		stepText, calls, usage, retry, truncated, err := e.consume(ctx, opts, step, stream)
		providerLatency := time.Since(providerStarted).Milliseconds()
		if err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started)
		}
		metrics.AddUsage(usage)
		if budgetErr := e.checkBudget(opts, metrics, step); budgetErr != nil {
			return e.end(opts, step, Failed, output, budgetErr, metrics, started)
		}
		if truncated {
			metrics.Compactions++
			if err := e.compact(opts, step); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started)
			}
			continue
		}
		if retry {
			emptyRetries++
			if emptyRetries > opts.MaxEmptyProviderRetries {
				return e.end(opts, step, Failed, output, errors.New("provider returned empty output"), metrics, started)
			}
			metrics.Retries++
			e.emit(event.Event{Type: event.ProviderRetry, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: "empty provider output"})
			continue
		}
		emptyRetries = 0
		if stepText != "" {
			output += stepText
			noProgress = 0
			if err := e.Store.Append(opts.RunID, session.Message{Role: session.Assistant, Content: stepText}); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started)
			}
		} else if len(calls) == 0 {
			noProgress++
			if noProgress >= opts.NoProgressLimit {
				return e.end(opts, step, Failed, output, ErrNoProgress, metrics, started)
			}
		}
		if len(calls) == 0 {
			e.emit(event.Event{Type: event.StepEnd, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Duration: providerLatency})
			continue
		}
		if err := validateToolCalls(calls); err != nil {
			return e.end(opts, step, Failed, output, err, metrics, started)
		}
		sig := toolSignature(calls)
		if sig == lastToolSig {
			duplicateCount++
			if duplicateCount >= opts.DuplicateToolLimit {
				return e.end(opts, step, Failed, output, ErrDuplicateTools, metrics, started)
			}
		} else {
			lastToolSig = sig
			duplicateCount = 0
		}
		if final, ok := completionSignal(calls); ok {
			return e.end(opts, step, Completed, final, nil, metrics, started)
		}
		if prompt, ok := askUserSignal(calls); ok {
			return e.end(opts, step, Waiting, prompt, nil, metrics, started)
		}
		metrics.ToolCalls += len(calls)
		if budgetErr := e.checkBudget(opts, metrics, step); budgetErr != nil {
			return e.end(opts, step, Failed, output, budgetErr, metrics, started)
		}
		for _, call := range calls {
			e.emit(event.Event{Type: event.ToolCall, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, ToolID: call.ID, ToolName: call.Name, Args: call.Args})
		}
		for _, call := range calls {
			if err := e.Store.Append(opts.RunID, session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: call.ID, ToolName: call.Name, ToolArgs: call.Args}); err != nil {
				return e.end(opts, step, Failed, output, err, metrics, started)
			}
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
				return e.end(opts, step, Failed, output, err, metrics, started)
			}
		}
		e.emit(event.Event{Type: event.StepEnd, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Duration: providerLatency + toolLatency, Metrics: StepMetrics{Step: step, Provider: opts.ProviderName, Model: opts.Model, Usage: usage, ProviderLatencyMS: providerLatency, ToolLatencyMS: toolLatency, ToolCalls: len(calls)}})
	}
	return e.end(opts, opts.HardMaxSteps, Failed, output, ErrMaxSteps, metrics, started)
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
	return o
}

func (e *Engine) consume(ctx context.Context, opts Options, step int, stream <-chan provider.StreamEvent) (string, []provider.ToolCall, provider.Usage, bool, bool, error) {
	var text string
	var calls []provider.ToolCall
	var usage provider.Usage
	for {
		select {
		case <-ctx.Done():
			return text, calls, usage, false, false, ctx.Err()
		case ev, ok := <-stream:
			if !ok {
				if text == "" && len(calls) == 0 {
					return "", nil, usage, true, false, nil
				}
				return text, calls, usage, false, false, nil
			}
			switch ev.Type {
			case provider.Delta:
				text += ev.Text
				e.emit(event.Event{Type: event.ProviderDelta, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: ev.Text})
			case provider.ToolCalls:
				calls = append(calls, ev.ToolCalls...)
			case provider.UsageEvent:
				usage = usage.Add(ev.Usage)
				e.emit(event.Event{Type: event.ProviderUsage, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Metrics: ev.Usage.Normalized()})
			case provider.Empty:
				return "", nil, usage, true, false, nil
			case provider.Truncated:
				return text, calls, usage, false, true, nil
			case provider.Done:
				return text, calls, usage, false, false, nil
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

func (e *Engine) end(opts Options, step int, status Status, output string, err error, metrics RunMetrics, started time.Time) Result {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	metrics.WallTimeMS = time.Since(started).Milliseconds()
	e.emit(event.Event{Type: event.RunEnd, TraceID: opts.TraceID, RunID: opts.RunID, SessionID: opts.SessionID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: string(status), Result: output, Err: errText, Metrics: metrics})
	return Result{Status: status, Output: output, Err: err, Metrics: metrics}
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

func completionSignal(calls []provider.ToolCall) (string, bool) {
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
