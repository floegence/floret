// Package providerretry owns bounded, in-memory recovery of model requests.
package providerretry

import (
	"context"
	"crypto/x509"
	"errors"
	"github.com/floegence/floret/v7/observation"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Category is a protocol or transport classification, never inferred from prose.
type Category string

const (
	Transient   Category = "transient"
	RateLimit   Category = "rate_limit"
	Timeout     Category = "timeout"
	Interrupted Category = "stream_interrupted"
	Permanent   Category = "permanent"
)

// Advice contains immutable facts captured when the transport receives a failure.
type Advice struct {
	Category  Category  `json:"category,omitempty"`
	NotBefore time.Time `json:"not_before,omitempty"`
	Forbidden bool      `json:"forbidden,omitempty"`
}

// Adviser is optionally implemented by provider errors. Wrappers must preserve it.
type Adviser interface{ ProviderRetryAdvice() Advice }

// Policy bounds extra physical attempts for one frozen model request.
type Policy struct {
	Disabled          bool          `json:"disabled,omitempty"`
	MaxRetries        int           `json:"max_retries"`
	MaxTimeoutRetries int           `json:"max_timeout_retries"`
	BaseDelay         time.Duration `json:"base_delay"`
	MaxDelay          time.Duration `json:"max_delay"`
	Jitter            float64       `json:"jitter"`
	RecoveryWindow    time.Duration `json:"recovery_window"`
	MaxServerWait     time.Duration `json:"max_server_wait"`
}

func DefaultPolicy() Policy {
	return Policy{MaxRetries: 4, MaxTimeoutRetries: 3, BaseDelay: 2 * time.Second, MaxDelay: 10 * time.Second, Jitter: .2, RecoveryWindow: 5 * time.Minute, MaxServerWait: time.Minute}
}
func (p Policy) Validate() error {
	if p.MaxRetries < 0 || p.MaxTimeoutRetries < 0 || math.IsNaN(p.Jitter) || p.Jitter < 0 || p.Jitter > 1 || p.BaseDelay < 0 || p.MaxDelay < 0 || p.RecoveryWindow < 0 || p.MaxServerWait < 0 {
		return errors.New("invalid provider retry policy")
	}
	if !p.Disabled && (p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay || p.RecoveryWindow <= 0 || p.MaxServerWait <= 0 || p.MaxTimeoutRetries > p.MaxRetries) {
		return errors.New("provider retry policy requires bounded delays, window, and timeout budget")
	}
	return nil
}
func Normalize(p Policy) Policy {
	if p == (Policy{}) {
		return DefaultPolicy()
	}
	return p
}

// Info describes one scheduled replacement. It is live state, not a durable job.
type Info = observation.ProviderRetryProgress

// HTTPAdvice classifies status and exact machine codes, retaining server deadlines.
func HTTPAdvice(status int, code string, headers http.Header, received time.Time) Advice {
	a := Advice{}
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "insufficient_quota", "quota_exceeded", "billing_hard_limit_reached", "billing_not_active", "credit_balance_too_low", "insufficient_balance", "insufficient_funds", "billing_error", "usage_limit_reached", "monthly_limit_exceeded", "invalid_api_key", "authentication_error", "permission_error", "permission_denied", "model_not_found", "invalid_request_error", "invalid_argument", "content_policy_violation", "content_filter", "unsupported_request":
		a.Category = Permanent
	case "rate_limit_exceeded", "rate_limit_error", "too_many_requests", "concurrency_limit_exceeded":
		a.Category = RateLimit
	case "overloaded_error", "server_error", "internal_error", "internal_server_error", "service_unavailable", "upstream_error", "upstream_gateway_error", "request_conflict":
		a.Category = Transient
	case "request_timeout", "timeout_error":
		a.Category = Timeout
	case "basispoints_tool_arguments_stalled":
		a.Category = Interrupted
	}
	// Authentication and invalid contracts always outrank server retry hints.
	if status == 401 || status == 403 || status == 404 || status == 400 || status == 422 || status == 501 || status == 505 {
		a.Category = Permanent
	}
	if a.Category != Permanent && (status == 408 || status == 504 || status == 524) {
		a.Category = Timeout
	}
	if a.Category == "" {
		switch {
		case status == 408 || status == 504 || status == 524:
			a.Category = Timeout
		case status == 429:
			a.Category = RateLimit
		case status >= 500 && status <= 599:
			a.Category = Transient
		}
	}
	if strings.EqualFold(strings.TrimSpace(headers.Get("x-should-retry")), "false") {
		a.Forbidden = true
	}
	if raw := strings.TrimSpace(headers.Get("Retry-After")); raw != "" {
		if seconds, err := strconv.ParseInt(raw, 10, 32); err == nil && seconds >= 0 {
			a.NotBefore = received.Add(time.Duration(seconds) * time.Second)
		} else if date, err := http.ParseTime(raw); err == nil {
			a.NotBefore = date
		}
	}
	if ms, err := strconv.ParseInt(strings.TrimSpace(headers.Get("retry-after-ms")), 10, 53); err == nil && ms >= 0 && ms <= int64((1<<63-1)/int64(time.Millisecond)) {
		if date := received.Add(time.Duration(ms) * time.Millisecond); date.After(a.NotBefore) {
			a.NotBefore = date
		}
	}
	return a
}

func AdviceFor(err error) Advice {
	var adviser Adviser
	if errors.As(err, &adviser) {
		return adviser.ProviderRetryAdvice()
	}
	var cert x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	var hostname x509.HostnameError
	if errors.As(err, &cert) || errors.As(err, &invalid) || errors.As(err, &hostname) {
		return Advice{Category: Permanent}
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		if dns.IsTimeout {
			return Advice{Category: Timeout}
		}
		if dns.IsTemporary {
			return Advice{Category: Transient}
		}
		return Advice{Category: Permanent}
	}
	if errors.Is(err, context.Canceled) {
		return Advice{Category: Permanent}
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return Advice{Category: Timeout}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Advice{Category: Timeout}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return Advice{Category: Interrupted}
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return Advice{Category: Transient}
	}
	return Advice{}
}

type advisedError struct {
	cause  error
	advice Advice
}

func (e *advisedError) Error() string               { return e.cause.Error() }
func (e *advisedError) Unwrap() error               { return e.cause }
func (e *advisedError) ProviderRetryAdvice() Advice { return e.advice }
func WithAdvice(err error, a Advice) error {
	if err == nil {
		return nil
	}
	return &advisedError{err, a}
}

// Run is the only retry loop. Each callback must close all attempt resources.
// The initial request has no recovery deadline; the window starts at its failure.
func Run[T any](ctx context.Context, p Policy, attempt func(context.Context, int) (T, error), scheduled func(Info)) (T, error) {
	return run(ctx, p, attempt, scheduled, time.Now, func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}, rand.Float64)
}
func run[T any](ctx context.Context, p Policy, attempt func(context.Context, int) (T, error), scheduled func(Info), now func() time.Time, wait func(context.Context, time.Duration) error, random func() float64) (T, error) {
	p = Normalize(p)
	parentCtx := ctx
	var result T
	var lastErr error
	var deadline time.Time
	timeoutRetries := 0
	for n := 0; ; n++ {
		if !deadline.IsZero() && !now().Before(deadline) {
			return result, lastErr
		}
		if err := ctx.Err(); err != nil {
			if parentCtx.Err() == nil && lastErr != nil {
				return result, lastErr
			}
			return result, err
		}
		attemptCtx, closeAttempt := context.WithCancel(ctx)
		value, err := attempt(attemptCtx, n)
		closeAttempt()
		result = value
		previousErr := lastErr
		lastErr = err
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil {
			if parentCtx.Err() != nil {
				return result, parentCtx.Err()
			}
			if previousErr != nil && errors.Is(err, ctx.Err()) && AdviceFor(err).Category != Permanent {
				return result, errors.Join(previousErr, err)
			}
			if lastErr != nil {
				return result, lastErr
			}
			return result, ctx.Err()
		}
		a := AdviceFor(err)
		if p.Disabled || a.Forbidden || n >= p.MaxRetries {
			return result, err
		}
		switch a.Category {
		case Transient, RateLimit, Interrupted:
		case Timeout:
			if timeoutRetries >= p.MaxTimeoutRetries {
				return result, err
			}
			timeoutRetries++
		default:
			return result, err
		}
		current := now()
		if deadline.IsZero() {
			deadline = current.Add(p.RecoveryWindow)
			recoveryCtx, cancelRecovery := context.WithDeadline(ctx, deadline)
			defer cancelRecovery()
			ctx = recoveryCtx
		}
		delay := p.BaseDelay
		for i := 0; i < n && delay < p.MaxDelay; i++ {
			if delay > p.MaxDelay/2 {
				delay = p.MaxDelay
			} else {
				delay *= 2
			}
		}
		delay = min(delay, p.MaxDelay)
		delay = time.Duration(float64(delay) * (1 + p.Jitter*(2*random()-1)))
		next := current.Add(delay)
		if a.NotBefore.Sub(current) > p.MaxServerWait {
			return result, err
		}
		if a.NotBefore.After(next) {
			next = a.NotBefore
		}
		if !next.Before(deadline) {
			return result, err
		}
		if parentDeadline, ok := ctx.Deadline(); ok && !next.Before(parentDeadline) {
			return result, err
		}
		if scheduled != nil {
			scheduled(Info{Retry: n + 1, MaxRetries: p.MaxRetries, NextAttemptAt: next, Reason: string(a.Category)})
		}
		if waitErr := wait(ctx, next.Sub(current)); waitErr != nil {
			if parentCtx.Err() != nil {
				return result, parentCtx.Err()
			}
			return result, lastErr
		}
	}
}
