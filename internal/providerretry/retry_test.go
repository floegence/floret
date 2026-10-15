package providerretry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestProtocolClassificationAndServerDeadlines(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		status int
		code   string
		want   Category
	}{
		{500, "", Transient}, {502, "", Transient}, {503, "", Transient}, {529, "", Transient}, {501, "", Permanent}, {505, "", Permanent},
		{408, "", Timeout}, {504, "server_error", Timeout}, {524, "", Timeout}, {409, "", ""}, {409, "request_conflict", Transient},
		{429, "", RateLimit}, {429, "insufficient_quota", Permanent}, {429, "billing_hard_limit_reached", Permanent}, {401, "server_error", Permanent},
		{403, "rate_limit_error", Permanent}, {422, "", Permanent}, {200, "overloaded_error", Transient}, {200, "unknown_error", ""},
	} {
		if got := HTTPAdvice(tc.status, tc.code, nil, now).Category; got != tc.want {
			t.Errorf("%d/%s = %s, want %s", tc.status, tc.code, got, tc.want)
		}
	}
	for _, tc := range []struct {
		seconds, ms string
		offset      time.Duration
	}{
		{"3", "", 3 * time.Second}, {now.Add(8 * time.Second).Format(http.TimeFormat), "", 8 * time.Second}, {"2", "3500", 3500 * time.Millisecond}, {"5", "1000", 5 * time.Second}, {"invalid", "-1", 0}, {"999999999999999999999", "bad", 0},
	} {
		headers := http.Header{"Retry-After": {tc.seconds}, "Retry-After-Ms": {tc.ms}}
		a := HTTPAdvice(429, "", headers, now)
		if tc.offset == 0 {
			if !a.NotBefore.IsZero() {
				t.Fatalf("invalid deadline: %v", a)
			}
		} else if !a.NotBefore.Equal(now.Add(tc.offset)) {
			t.Fatalf("deadline: %v, want %s", a, tc.offset)
		}
	}
	a := HTTPAdvice(500, "", http.Header{"X-Should-Retry": {"false"}}, now)
	if !a.Forbidden {
		t.Fatal("server prohibition lost")
	}
}
func TestTransportClassificationAndCause(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want Category
	}{
		{syscall.ECONNRESET, Transient}, {syscall.ECONNREFUSED, Transient}, {io.ErrUnexpectedEOF, Interrupted}, {&net.DNSError{IsTemporary: true}, Transient}, {&net.DNSError{IsNotFound: true}, Permanent}, {&net.DNSError{IsTimeout: true}, Timeout}, {context.Canceled, Permanent}, {errors.New("upstream 500 timeout"), ""},
	} {
		if got := AdviceFor(tc.err).Category; got != tc.want {
			t.Errorf("%v: %v", tc.err, got)
		}
	}
	cause := errors.New("cause")
	err := WithAdvice(cause, Advice{Category: Transient})
	if !errors.Is(err, cause) {
		t.Fatal("cause lost")
	}
}
func TestRetrySharedBudgetsAndBackoff(t *testing.T) {
	for _, tc := range []struct {
		name       string
		categories []Category
		wantCalls  int
		wantDelays []time.Duration
	}{
		{"transient", []Category{Transient, Transient, Transient, Transient, Transient}, 5, []time.Duration{2, 4, 8, 10}},
		{"timeout", []Category{Timeout, Timeout, Timeout, Timeout}, 4, []time.Duration{2, 4, 8}},
		{"shared", []Category{Transient, Timeout, Transient, Timeout, Transient}, 5, []time.Duration{2, 4, 8, 10}},
		{"success", []Category{Transient, Transient}, 3, []time.Duration{2, 4}},
		{"unknown", []Category{""}, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			calls := 0
			var delays []time.Duration
			var infos []Info
			_, err := run(t.Context(), DefaultPolicy(), func(context.Context, int) (int, error) {
				calls++
				if calls > len(tc.categories) {
					return 42, nil
				}
				return 0, WithAdvice(errors.New("upstream"), Advice{Category: tc.categories[calls-1]})
			}, func(info Info) { infos = append(infos, info) }, func() time.Time { return now }, func(_ context.Context, d time.Duration) error {
				delays = append(delays, d)
				now = now.Add(d)
				return nil
			}, func() float64 { return .5 })
			if calls != tc.wantCalls {
				t.Fatalf("calls=%d, err=%v", calls, err)
			}
			if len(delays) != len(tc.wantDelays) {
				t.Fatalf("delays=%v", delays)
			}
			for i, d := range delays {
				if d != tc.wantDelays[i]*time.Second || infos[i].Retry != i+1 {
					t.Fatalf("delay/info %v/%v", delays, infos)
				}
			}
		})
	}
}
func TestRetryWindowServerWaitAndCancellation(t *testing.T) {
	for _, mode := range []string{"long_server_wait", "window", "forbidden", "cancel_wait", "cancel_request", "disabled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			waits := 0
			p := DefaultPolicy()
			if mode == "disabled" {
				p.Disabled = true
			}
			if mode == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(ctx, now.Add(time.Second))
				defer stop()
			}
			_, err := run(ctx, p, func(ctx context.Context, n int) (int, error) {
				calls++
				a := Advice{Category: Transient}
				switch mode {
				case "long_server_wait":
					a.NotBefore = now.Add(61 * time.Second)
				case "window":
					now = now.Add(6 * time.Minute)
				case "forbidden":
					a.Forbidden = true
				case "cancel_request":
					cancel()
					return 0, ctx.Err()
				}
				return 0, WithAdvice(errors.New("upstream"), a)
			}, nil, func() time.Time { return now }, func(ctx context.Context, d time.Duration) error {
				waits++
				if mode == "window" {
					now = now.Add(6 * time.Minute)
					return nil
				}
				cancel()
				return ctx.Err()
			}, func() float64 { return .5 })
			if err == nil {
				t.Fatal("expected bounded failure")
			}
			if mode == "window" {
				if calls != 1 {
					t.Fatalf("calls=%d", calls)
				}
			} else if calls != 1 {
				t.Fatalf("calls=%d waits=%d", calls, waits)
			}
		})
	}
}

func TestRecoveryDeadlinePreservesLastFailureAndIndependentPriority(t *testing.T) {
	for _, independent := range []bool{false, true} {
		t.Run(fmt.Sprint(independent), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				first := errors.New("first request timed out")
				storage := WithAdvice(errors.New("storage unavailable"), Advice{Category: Permanent})
				p := DefaultPolicy()
				p.RecoveryWindow = time.Minute
				p.BaseDelay = time.Second
				calls := 0
				_, err := Run(t.Context(), p, func(ctx context.Context, n int) (int, error) {
					calls++
					if n == 0 {
						return 0, WithAdvice(first, Advice{Category: Timeout})
					}
					<-ctx.Done()
					if independent {
						return 0, storage
					}
					return 0, ctx.Err()
				}, nil)
				if calls != 2 {
					t.Fatalf("calls=%d", calls)
				}
				if independent {
					if err != storage {
						t.Fatalf("independent failure replaced: %v", err)
					}
				} else if !errors.Is(err, first) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("timeout evidence lost: %v", err)
				}
			})
		})
	}
}
