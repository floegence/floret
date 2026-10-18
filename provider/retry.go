package provider

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/providerretry"
	"net/http"
	"time"
)

// RetryCategory is a structured protocol or transport failure classification.
type RetryCategory string

const (
	RetryTransient         = RetryCategory(providerretry.Transient)
	RetryRateLimit         = RetryCategory(providerretry.RateLimit)
	RetryTimeout           = RetryCategory(providerretry.Timeout)
	RetryStreamInterrupted = RetryCategory(providerretry.Interrupted)
	RetryPermanent         = RetryCategory(providerretry.Permanent)
)

// RetryAdvice carries response-time facts, including an absolute server deadline.
type RetryAdvice struct {
	Category  RetryCategory `json:"category,omitempty"`
	NotBefore time.Time     `json:"not_before,omitempty"`
	Forbidden bool          `json:"forbidden,omitempty"`
}

// RetryAdviser is an optional provider-error contract. Unknown errors are not retried.
type RetryAdviser interface{ ProviderRetryAdvice() RetryAdvice }

// RetryAdviceForError returns provider transport facts without scheduling retries.
// The outermost explicit RetryAdviser takes precedence, including empty advice.
// Use it only at provider request or stream boundaries, not for storage, decoding,
// or tool failures. Unknown errors and nil return empty advice.
func RetryAdviceForError(err error) RetryAdvice {
	var adviser RetryAdviser
	if errors.As(err, &adviser) {
		return adviser.ProviderRetryAdvice()
	}
	a := providerretry.AdviceFor(err)
	return RetryAdvice{Category: RetryCategory(a.Category), NotBefore: a.NotBefore, Forbidden: a.Forbidden}
}

// HTTPRetryAdvice classifies exact protocol codes and parses Retry-After headers.
// receivedAt must be the time the response was received, not a later inspection.
func HTTPRetryAdvice(status int, code string, headers http.Header, receivedAt time.Time) RetryAdvice {
	a := providerretry.HTTPAdvice(status, code, headers, receivedAt)
	return RetryAdvice{Category: RetryCategory(a.Category), NotBefore: a.NotBefore, Forbidden: a.Forbidden}
}

// WithRetryAdvice preserves cause and exposes immutable protocol facts.
func WithRetryAdvice(cause error, advice RetryAdvice) error {
	if cause == nil {
		return nil
	}
	return &retryAdvisedError{cause: cause, advice: advice}
}

// ProviderRetryAdvice classifies an HTTP error without parsing its human message.
func (e *ProviderHTTPError) ProviderRetryAdvice() RetryAdvice {
	if e == nil {
		return RetryAdvice{}
	}
	return HTTPRetryAdvice(e.StatusCode, e.Code, nil, time.Time{})
}

// PreparedRequestReplay optionally creates a fresh single-use attempt from the
// original immutable rendering, even after Close. It must not reopen resources
// or resolve attachments. Only execution identity may differ in request.
type PreparedRequestReplay interface {
	PrepareRetry(context.Context, Request) (PreparedRequest, error)
}

type retryAdvisedError struct {
	cause  error
	advice RetryAdvice
}

func (e *retryAdvisedError) Error() string                    { return e.cause.Error() }
func (e *retryAdvisedError) Unwrap() error                    { return e.cause }
func (e *retryAdvisedError) ProviderRetryAdvice() RetryAdvice { return e.advice }
func publicRetryError(err error) error {
	var adviser providerretry.Adviser
	if errors.As(err, &adviser) {
		a := adviser.ProviderRetryAdvice()
		return WithRetryAdvice(err, RetryAdvice{Category: RetryCategory(a.Category), NotBefore: a.NotBefore, Forbidden: a.Forbidden})
	}
	return err
}
