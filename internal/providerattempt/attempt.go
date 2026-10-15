package providerattempt

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/provider"
)

// RequestTimeoutPolicy bounds one physical model request, excluding local tools.
type RequestTimeoutPolicy struct {
	FirstOutputTimeout time.Duration `json:"first_output_timeout"`
	IdleTimeout        time.Duration `json:"idle_timeout"`
	HostedToolTimeout  time.Duration `json:"hosted_tool_timeout"`
	MaxAttemptDuration time.Duration `json:"max_attempt_duration"`
}

func DefaultRequestTimeoutPolicy() RequestTimeoutPolicy {
	return RequestTimeoutPolicy{5 * time.Minute, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute}
}
func (p RequestTimeoutPolicy) Validate() error {
	if p.FirstOutputTimeout < 0 || p.IdleTimeout < 0 || p.HostedToolTimeout < 0 || p.MaxAttemptDuration < 0 {
		return errors.New("provider request timeouts cannot be negative")
	}
	return nil
}
func (p RequestTimeoutPolicy) normalized() RequestTimeoutPolicy {
	d := DefaultRequestTimeoutPolicy()
	if p.FirstOutputTimeout == 0 {
		p.FirstOutputTimeout = d.FirstOutputTimeout
	}
	if p.IdleTimeout == 0 {
		p.IdleTimeout = d.IdleTimeout
	}
	if p.HostedToolTimeout == 0 {
		p.HostedToolTimeout = d.HostedToolTimeout
	}
	if p.MaxAttemptDuration == 0 {
		p.MaxAttemptDuration = d.MaxAttemptDuration
	}
	return p
}

func CloneTimeoutDetails(d *provider.RequestTimeoutDetails) *provider.RequestTimeoutDetails {
	if d == nil {
		return nil
	}
	v := *d
	if d.LastProgressAt != nil {
		at := *d.LastProgressAt
		v.LastProgressAt = &at
	}
	return &v
}
func TimeoutDetails(err error) *provider.RequestTimeoutDetails {
	var e *provider.RequestTimeoutError
	if errors.As(err, &e) {
		return CloneTimeoutDetails(&e.Details)
	}
	return nil
}

// Attempt owns the only semantic-progress clock for a physical provider call.
// Consumers report accepted events; cancellation reaches synchronous opening too.
type Attempt struct {
	mu          sync.Mutex
	parent      context.Context
	ctx         context.Context
	cancel      context.CancelCauseFunc
	policy      RequestTimeoutPolicy
	details     provider.RequestTimeoutDetails
	timer       *time.Timer
	closed      bool
	terminal    bool
	hosted      map[string]bool
	transitions map[string]bool
}

func StartAttempt(parent context.Context, p RequestTimeoutPolicy, req provider.Request) *Attempt {
	ctx, cancel := context.WithCancelCause(parent)
	a := &Attempt{parent: parent, ctx: ctx, cancel: cancel, policy: p.normalized(), details: provider.RequestTimeoutDetails{Stage: "first_output", LogicalRequestID: identity.LogicalRequestID(req.LogicalRequestID), AttemptID: req.AttemptID, AttemptEpoch: req.AttemptEpoch, StartedAt: time.Now()}, hosted: map[string]bool{}, transitions: map[string]bool{}}
	deadline, _ := a.deadline()
	a.timer = time.AfterFunc(time.Until(deadline), a.expire)
	return a
}
func (a *Attempt) Context() context.Context { return a.ctx }
func (a *Attempt) deadline() (time.Time, string) {
	duration, stage := a.policy.FirstOutputTimeout, "first_output"
	at := a.details.StartedAt
	if a.details.LastProgressAt != nil {
		duration, stage, at = a.policy.IdleTimeout, "output_idle", *a.details.LastProgressAt
	}
	if len(a.hosted) > 0 {
		duration, stage = a.policy.HostedToolTimeout, "hosted_tool"
	}
	idle := at.Add(duration)
	maximum := a.details.StartedAt.Add(a.policy.MaxAttemptDuration)
	if !idle.Before(maximum) {
		return maximum, "max_duration"
	}
	return idle, stage
}
func (a *Attempt) expire() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.terminal || a.ctx.Err() != nil {
		return
	}
	now := time.Now()
	if !a.expireAt(now) {
		deadline, _ := a.deadline()
		a.timer.Reset(deadline.Sub(now))
	}
}

// expireAt runs with mu held so a late event cannot renew an elapsed deadline.
func (a *Attempt) expireAt(now time.Time) bool {
	deadline, stage := a.deadline()
	if now.Before(deadline) {
		return false
	}
	a.details.Stage = stage
	a.details.TimedOutAt = now
	a.cancel(&provider.RequestTimeoutError{Details: *CloneTimeoutDetails(&a.details)})
	return true
}

func (a *Attempt) Observe(ev provider.StreamEvent) *time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.terminal || a.ctx.Err() != nil {
		return nil
	}
	if a.expireAt(time.Now()) {
		return nil
	}
	meaningful := false
	transition := func(key string) bool {
		if a.transitions[key] {
			return false
		}
		a.transitions[key] = true
		return true
	}
	switch ev.Type {
	case provider.Delta, provider.Reasoning, provider.ToolCallDelta:
		meaningful = ev.Text != ""
	case provider.ToolCallStart, provider.ToolCallEnd:
		meaningful = ev.ToolCallStream.ID != "" && transition(string(ev.Type)+":"+ev.ToolCallStream.ID)
	case provider.ToolCalls:
		for _, call := range ev.ToolCalls {
			if call.ID != "" && transition("calls:"+call.ID) {
				meaningful = true
			}
		}
	case provider.HostedToolCall:
		if ev.ToolCall.ID != "" && transition("hosted:"+ev.ToolCall.ID) {
			a.hosted[ev.ToolCall.ID] = true
			meaningful = true
		}
	case provider.HostedToolResult:
		if a.hosted[ev.ToolCall.ID] {
			delete(a.hosted, ev.ToolCall.ID)
			meaningful = true
		}
	case provider.Done, provider.Empty, provider.Truncated, provider.Error:
		a.terminal = true
		a.timer.Stop()
	}
	if meaningful {
		now := time.Now()
		a.details.LastProgressAt = &now
		deadline, _ := a.deadline()
		a.timer.Reset(time.Until(deadline))
	}
	if a.details.LastProgressAt == nil {
		return nil
	}
	at := *a.details.LastProgressAt
	return &at
}

// Error preserves independent failures and adds the timeout cause only when
// transport cancellation came from this attempt's clock.
func (a *Attempt) Error(err error) error {
	if a.parent.Err() != nil {
		if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
			return a.parent.Err()
		}
		return err
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		var timeout *provider.RequestTimeoutError
		if errors.As(context.Cause(a.ctx), &timeout) {
			if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
				return timeout
			}
			return errors.Join(err, timeout)
		}
	}
	return err
}
func (a *Attempt) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	a.timer.Stop()
	a.cancel(nil)
}
