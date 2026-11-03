package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
)

// FailureDetails retains original provider failure evidence. Message, Code and
// Body are untrusted, verbatim response data, not safe log or summary text. Body
// is the failed HTTP body or SSE data payload, never a request or successful
// transcript. BodyComplete is false if reading failed. Hosts authorize access
// and render these values as literal text; redaction belongs to a separate
// presentation projection. Details do not authorize retries.
type FailureDetails struct {
	Provider     string   `json:"provider,omitempty"`
	Phase        string   `json:"phase,omitempty"`
	HTTPStatus   int      `json:"http_status,omitempty"`
	RequestID    string   `json:"request_id,omitempty"`
	ResponseID   string   `json:"response_id,omitempty"`
	Event        string   `json:"event,omitempty"`
	Code         string   `json:"code,omitempty"`
	Message      string   `json:"message,omitempty"`
	Cause        string   `json:"cause,omitempty"`
	Causes       []string `json:"causes,omitempty"`
	Body         []byte   `json:"body,omitempty"`
	BodyComplete bool     `json:"body_complete"`
}

const failureDetailsPrefix = "floret_provider_failure_v1:"

type providerFailureDetailsError struct {
	details FailureDetails
	cause   error
}

func (e *providerFailureDetailsError) Error() string {
	raw, _ := json.Marshal(e.details)
	return failureDetailsPrefix + string(raw)
}
func (e *providerFailureDetailsError) Unwrap() error { return e.cause }
func (e *providerFailureDetailsError) ProviderRetryAdvice() RetryAdvice {
	return RetryAdviceForError(e.cause)
}
func (e *providerFailureDetailsError) ProviderFailureDetails() FailureDetails {
	return cloneFailureDetails(e.details)
}

func cloneFailureDetails(d FailureDetails) FailureDetails {
	d.Body = bytes.Clone(d.Body)
	d.Causes = append([]string(nil), d.Causes...)
	return d
}

// Retain distinct original error texts, including hidden wrapped causes.
func originalFailureCauses(err error) []string {
	var result []string
	visited := map[error]bool{}
	texts := map[string]bool{}
	var visit func(error)
	visit = func(current error) {
		if current == nil {
			return
		}
		if reflect.TypeOf(current).Comparable() {
			if visited[current] {
				return
			}
			visited[current] = true
		}
		message := current.Error()
		if !texts[message] {
			result = append(result, message)
			texts[message] = true
		}
		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return result
}

// WithFailureDetails copies evidence while preserving errors.Is/errors.As and
// retry advice. Missing Message/Cause use the original error text without
// normalization or truncation. Its serialized Error preserves evidence through
// canonical failure storage; never write that string to a public log.
func WithFailureDetails(cause error, details FailureDetails) error {
	if cause == nil {
		return nil
	}
	if details.Causes == nil {
		details.Causes = originalFailureCauses(cause)
	}
	if details.Cause == "" {
		details.Cause = cause.Error()
	}
	if details.Message == "" {
		details.Message = cause.Error()
	}
	return &providerFailureDetailsError{details: cloneFailureDetails(details), cause: cause}
}

// FailureDetailsForError returns a detached copy from the original chain or
// the exact versioned envelope retained by canonical storage. Older errors
// without evidence return nil; missing historical evidence is never invented.
func FailureDetailsForError(err error) *FailureDetails {
	if err == nil {
		return nil
	}
	var source interface{ ProviderFailureDetails() FailureDetails }
	if errors.As(err, &source) {
		d := cloneFailureDetails(source.ProviderFailureDetails())
		return &d
	}
	for _, line := range strings.Split(err.Error(), "\n") {
		line = strings.TrimPrefix(line, "provider context overflow retry exhausted: ")
		raw, ok := strings.CutPrefix(line, failureDetailsPrefix)
		if !ok {
			continue
		}
		var d FailureDetails
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&d) == nil && decoder.Decode(&struct{}{}) == io.EOF {
			return &d
		}
	}
	return nil
}
