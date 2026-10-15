package providerattempt

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/floegence/floret/v7/internal/provider"
)

func TestAttemptTimeoutStages(t *testing.T) {
	for _, tc := range []struct {
		name, stage string
		events      []StreamEvent
		wait        time.Duration
	}{
		{"first", "first_output", nil, 5 * time.Minute},
		{"arguments", "output_idle", []StreamEvent{{Type: ToolCallStart, ToolCallStream: ToolCallStream{ID: "call", Name: "write"}}, {Type: ToolCallDelta, Text: "{", ToolCallStream: ToolCallStream{ID: "call", Name: "write"}}}, 2 * time.Minute},
		{"hosted", "hosted_tool", []StreamEvent{{Type: HostedToolCall, ToolCall: ToolCall{ID: "search", Name: "search"}}}, 5 * time.Minute},
		{"hosted_finished", "output_idle", []StreamEvent{{Type: HostedToolCall, ToolCall: ToolCall{ID: "search", Name: "search"}}, {Type: HostedToolResult, ToolCall: ToolCall{ID: "search", Name: "search"}}}, 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := StartAttempt(t.Context(), RequestTimeoutPolicy{}, Request{LogicalRequestID: "logical", AttemptID: "attempt", AttemptEpoch: 1})
				defer a.Close()
				for _, ev := range tc.events {
					a.Observe(ev)
				}
				time.Sleep(tc.wait - time.Second)
				if a.Context().Err() != nil {
					t.Fatal("expired early")
				}
				time.Sleep(time.Second)
				synctest.Wait()
				var timeout *RequestTimeoutError
				if !errors.As(a.Error(a.Context().Err()), &timeout) || timeout.Details.Stage != tc.stage || !errors.Is(timeout, context.DeadlineExceeded) || !errors.Is(timeout, ErrRequestTimeout) {
					t.Fatalf("timeout=%+v error=%v", timeout, a.Error(a.Context().Err()))
				}
				if timeout.Details.AttemptID != "attempt" {
					t.Fatal("missing identity")
				}
			})
		})
	}
}

func TestAttemptOnlySemanticProgressExtendsIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := StartAttempt(t.Context(), RequestTimeoutPolicy{}, Request{})
		defer a.Close()
		a.Observe(StreamEvent{Type: Delta, Text: "hi"})
		for range 7 {
			time.Sleep(15 * time.Second)
			a.Observe(StreamEvent{Type: UsageEvent})
			a.Observe(StreamEvent{Type: Delta})
		}
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if a.Context().Err() == nil {
			t.Fatal("empty events renewed timeout")
		}
	})
}

func TestAttemptMaximumAndTerminal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := StartAttempt(t.Context(), RequestTimeoutPolicy{}, Request{})
		defer a.Close()
		for range 14 {
			time.Sleep(time.Minute)
			a.Observe(StreamEvent{Type: Reasoning, Text: "thinking"})
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		var timeout *RequestTimeoutError
		if !errors.As(a.Error(a.Context().Err()), &timeout) || timeout.Details.Stage != "max_duration" {
			t.Fatalf("timeout=%+v", timeout)
		}
		b := StartAttempt(t.Context(), RequestTimeoutPolicy{}, Request{})
		defer b.Close()
		b.Observe(StreamEvent{Type: Done})
		time.Sleep(16 * time.Minute)
		if b.Context().Err() != nil {
			t.Fatal("completed attempt timed out")
		}
	})
}

func TestAttemptParentCancellationAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		a := StartAttempt(ctx, RequestTimeoutPolicy{}, Request{})
		cancel()
		synctest.Wait()
		if !errors.Is(a.Error(a.Context().Err()), context.Canceled) {
			t.Fatal("lost parent cancellation")
		}
		a.Close()
		b := StartAttempt(t.Context(), RequestTimeoutPolicy{}, Request{})
		b.Close()
		time.Sleep(20 * time.Minute)
		if errors.Is(context.Cause(b.Context()), ErrRequestTimeout) {
			t.Fatal("closed attempt timed out")
		}
	})
}

func TestAttemptRepeatedLifecycleDoesNotRenewAndSnapshotsAreDetached(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := StartAttempt(t.Context(), RequestTimeoutPolicy{}, Request{})
		defer a.Close()
		ev := StreamEvent{Type: HostedToolCall, ToolCall: ToolCall{ID: "search"}}
		at := a.Observe(ev)
		*at = at.Add(time.Hour)
		for range 4 {
			time.Sleep(time.Minute)
			a.Observe(ev)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		var failure *RequestTimeoutError
		if !errors.As(a.Error(a.Context().Err()), &failure) || failure.Details.Stage != "hosted_tool" {
			t.Fatalf("failure=%v", failure)
		}
	})
}

func TestAttemptDoesNotReplaceIndependentFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		a := StartAttempt(ctx, RequestTimeoutPolicy{}, Request{})
		defer a.Close()
		failure := errors.New("storage failed")
		cancel()
		synctest.Wait()
		if got := a.Error(failure); got != failure {
			t.Fatalf("independent failure replaced: %v", got)
		}
	})
}
