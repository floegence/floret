package provider

import internalprovider "github.com/floegence/floret/v7/internal/provider"

// ErrRequestTimeout identifies a runtime-enforced physical request deadline.
var ErrRequestTimeout = internalprovider.ErrRequestTimeout

// RequestTimeoutDetails records runtime-authored timing and attempt identity.
// Stage is first_output, output_idle, hosted_tool, or max_duration.
type RequestTimeoutDetails = internalprovider.RequestTimeoutDetails

// RequestTimeoutError reports a retryable request timeout. It matches
// ErrRequestTimeout and context.DeadlineExceeded, never user cancellation.
type RequestTimeoutError = internalprovider.RequestTimeoutError
