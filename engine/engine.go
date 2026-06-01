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
	MaxSteps                int
	HardMaxSteps            int
	MaxEmptyProviderRetries int
	NoProgressLimit         int
	DuplicateToolLimit      int
	WallTime                time.Duration
}

type Result struct {
	Status Status
	Output string
	Err    error
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
	for step := 1; step <= opts.HardMaxSteps; step++ {
		if step > opts.MaxSteps {
			return e.end(opts.RunID, step, Failed, output, ErrMaxSteps)
		}
		if ctx.Err() != nil {
			return e.end(opts.RunID, step, Cancelled, output, ctx.Err())
		}
		e.emit(event.Event{Type: event.StepStart, RunID: opts.RunID, Step: step})
		history, err := e.Store.Messages(opts.RunID)
		if err != nil {
			return e.end(opts.RunID, step, Failed, output, err)
		}
		req := provider.Request{RunID: opts.RunID, Step: step, Messages: e.Memory.Assemble(history)}
		e.emit(event.Event{Type: event.ProviderRequest, RunID: opts.RunID, Step: step, Message: fmt.Sprintf("%d messages", len(req.Messages))})
		stream, err := e.Provider.Stream(ctx, req)
		if errors.Is(err, provider.ErrContextOverflow) {
			if err := e.compact(opts.RunID, step); err != nil {
				return e.end(opts.RunID, step, Failed, output, err)
			}
			continue
		}
		if err != nil {
			return e.end(opts.RunID, step, Failed, output, err)
		}
		stepText, calls, retry, truncated, err := e.consume(ctx, opts.RunID, step, stream)
		if err != nil {
			return e.end(opts.RunID, step, Failed, output, err)
		}
		if truncated {
			if err := e.compact(opts.RunID, step); err != nil {
				return e.end(opts.RunID, step, Failed, output, err)
			}
			continue
		}
		if retry {
			emptyRetries++
			if emptyRetries > opts.MaxEmptyProviderRetries {
				return e.end(opts.RunID, step, Failed, output, errors.New("provider returned empty output"))
			}
			e.emit(event.Event{Type: event.ProviderRetry, RunID: opts.RunID, Step: step, Message: "empty provider output"})
			continue
		}
		emptyRetries = 0
		if stepText != "" {
			output += stepText
			noProgress = 0
			if err := e.Store.Append(opts.RunID, session.Message{Role: session.Assistant, Content: stepText}); err != nil {
				return e.end(opts.RunID, step, Failed, output, err)
			}
		} else if len(calls) == 0 {
			noProgress++
			if noProgress >= opts.NoProgressLimit {
				return e.end(opts.RunID, step, Failed, output, ErrNoProgress)
			}
		}
		if len(calls) == 0 {
			e.emit(event.Event{Type: event.StepEnd, RunID: opts.RunID, Step: step})
			continue
		}
		if err := validateToolCalls(calls); err != nil {
			return e.end(opts.RunID, step, Failed, output, err)
		}
		sig := toolSignature(calls)
		if sig == lastToolSig {
			duplicateCount++
			if duplicateCount >= opts.DuplicateToolLimit {
				return e.end(opts.RunID, step, Failed, output, ErrDuplicateTools)
			}
		} else {
			lastToolSig = sig
			duplicateCount = 0
		}
		if final, ok := completionSignal(calls); ok {
			return e.end(opts.RunID, step, Completed, final, nil)
		}
		if prompt, ok := askUserSignal(calls); ok {
			return e.end(opts.RunID, step, Waiting, prompt, nil)
		}
		for _, call := range calls {
			e.emit(event.Event{Type: event.ToolCall, RunID: opts.RunID, Step: step, ToolID: call.ID, ToolName: call.Name, Args: call.Args})
		}
		results := e.Tools.RunBatch(ctx, calls, e.Approver)
		for _, result := range results {
			text := result.Text
			errText := ""
			if result.Err != nil {
				errText = result.Err.Error()
				text = "ERROR: " + errText
			}
			e.emit(event.Event{Type: event.ToolResult, RunID: opts.RunID, Step: step, ToolID: result.Call.ID, ToolName: result.Call.Name, Result: text, Err: errText})
			if err := e.Store.Append(opts.RunID, session.Message{Role: session.Tool, Content: text, ToolCallID: result.Call.ID, ToolName: result.Call.Name}); err != nil {
				return e.end(opts.RunID, step, Failed, output, err)
			}
		}
		e.emit(event.Event{Type: event.StepEnd, RunID: opts.RunID, Step: step})
	}
	return e.end(opts.RunID, opts.HardMaxSteps, Failed, output, ErrMaxSteps)
}

func normalizeOptions(o Options) Options {
	if o.RunID == "" {
		o.RunID = "default"
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

func (e *Engine) consume(ctx context.Context, runID string, step int, stream <-chan provider.StreamEvent) (string, []provider.ToolCall, bool, bool, error) {
	var text string
	var calls []provider.ToolCall
	for {
		select {
		case <-ctx.Done():
			return text, calls, false, false, ctx.Err()
		case ev, ok := <-stream:
			if !ok {
				if text == "" && len(calls) == 0 {
					return "", nil, true, false, nil
				}
				return text, calls, false, false, nil
			}
			switch ev.Type {
			case provider.Delta:
				text += ev.Text
				e.emit(event.Event{Type: event.ProviderDelta, RunID: runID, Step: step, Message: ev.Text})
			case provider.ToolCalls:
				calls = append(calls, ev.ToolCalls...)
			case provider.Empty:
				return "", nil, true, false, nil
			case provider.Truncated:
				return text, calls, false, true, nil
			case provider.Done:
				return text, calls, false, false, nil
			}
		}
	}
}

func (e *Engine) compact(runID string, step int) error {
	history, err := e.Store.Messages(runID)
	if err != nil {
		return err
	}
	e.emit(event.Event{Type: event.ContextCompact, RunID: runID, Step: step})
	return e.Store.Replace(runID, e.Memory.Compact(history))
}

func (e *Engine) end(runID string, step int, status Status, output string, err error) Result {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	e.emit(event.Event{Type: event.RunEnd, RunID: runID, Step: step, Message: string(status), Result: output, Err: errText})
	return Result{Status: status, Output: output, Err: err}
}

func (e *Engine) emit(ev event.Event) {
	if e.Sink == nil {
		return
	}
	ev.Timestamp = time.Now()
	e.Sink.Emit(ev)
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
