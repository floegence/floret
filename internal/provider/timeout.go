package provider

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/floegence/floret/v7/identity"
)

// ErrRequestTimeout identifies a runtime-enforced physical request deadline.
var ErrRequestTimeout = errors.New("provider request timeout")

// RequestTimeoutDetails contains only runtime-authored facts, never model content.
// Stage is first_output, output_idle, hosted_tool, or max_duration.
type RequestTimeoutDetails struct {
	Stage            string                    `json:"stage"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id,omitempty"`
	AttemptID        string                    `json:"attempt_id,omitempty"`
	AttemptEpoch     int                       `json:"attempt_epoch,omitempty"`
	StartedAt        time.Time                 `json:"started_at"`
	LastProgressAt   *time.Time                `json:"last_progress_at,omitempty"`
	TimedOutAt       time.Time                 `json:"timed_out_at"`
}

// Validate checks the stage, optional logical identity and chronological timing.
func (d RequestTimeoutDetails) Validate() error {
	switch d.Stage {
	case "first_output", "output_idle", "hosted_tool", "max_duration":
	default:
		return errors.New("invalid provider timeout stage")
	}
	if d.LogicalRequestID != "" {
		if _, err := identity.ParseLogicalRequestID(d.LogicalRequestID.String()); err != nil {
			return err
		}
	}
	if d.StartedAt.IsZero() || d.TimedOutAt.Before(d.StartedAt) || d.AttemptEpoch < 0 {
		return errors.New("invalid provider timeout timing or identity")
	}
	if d.LastProgressAt != nil && (d.LastProgressAt.Before(d.StartedAt) || d.LastProgressAt.After(d.TimedOutAt)) {
		return errors.New("invalid provider last progress time")
	}
	return nil
}

// RequestTimeoutError is a retryable request failure, not execution cancellation.
type RequestTimeoutError struct{ Details RequestTimeoutDetails }

func (e *RequestTimeoutError) Error() string {
	return fmt.Sprintf("provider request timeout (%s)", e.Details.Stage)
}
func (e *RequestTimeoutError) Unwrap() error        { return context.DeadlineExceeded }
func (e *RequestTimeoutError) Is(target error) bool { return target == ErrRequestTimeout }
func (e *RequestTimeoutError) Timeout() bool        { return true }
func (e *RequestTimeoutError) Temporary() bool      { return true }
