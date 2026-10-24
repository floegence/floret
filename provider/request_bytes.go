package provider

import (
	"errors"
	"fmt"
)

// RequestByteLimitError reports an exact rendered JSON limit, independently of
// token capacity. It preserves ErrContextOverflow recovery compatibility.
// Limits are host policy, not inferred provider service limits.
type RequestByteLimitError struct {
	ActualBytes int64
	LimitBytes  int64
}

func (e *RequestByteLimitError) Error() string {
	return fmt.Sprintf("provider request byte limit exceeded: actual_bytes=%d limit_bytes=%d", e.ActualBytes, e.LimitBytes)
}
func (e *RequestByteLimitError) Unwrap() error { return ErrContextOverflow }

// CheckRequestBytes checks the final UTF-8 JSON body, including base64 and
// framing. Zero disables the host limit; a negative limit is invalid. Call it
// after rendering and output refitting, before freezing or dispatching bytes.
func CheckRequestBytes(payload []byte, limit int64) error {
	if limit < 0 {
		return errors.New("request byte limit must not be negative")
	}
	if limit > 0 && int64(len(payload)) > limit {
		return &RequestByteLimitError{ActualBytes: int64(len(payload)), LimitBytes: limit}
	}
	return nil
}
