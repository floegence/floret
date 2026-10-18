package provider_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/provider"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestRetryAdviceForError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want provider.RetryCategory
	}{
		{"nil", nil, ""},
		{"eof", io.EOF, provider.RetryStreamInterrupted},
		{"wrapped eof", fmt.Errorf("failed to read frame header: %w", io.EOF), provider.RetryStreamInterrupted},
		{"unexpected eof", io.ErrUnexpectedEOF, provider.RetryStreamInterrupted},
		{"reset", syscall.ECONNRESET, provider.RetryTransient},
		{"timeout", context.DeadlineExceeded, provider.RetryTimeout},
		{"cancel", context.Canceled, provider.RetryPermanent},
		{"dns", &net.DNSError{IsNotFound: true}, provider.RetryPermanent},
		{"unknown prose", errors.New("upstream 500 unexpected EOF timeout"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := provider.RetryAdviceForError(tc.err); got.Category != tc.want {
				t.Fatalf("got=%#v want=%s", got, tc.want)
			}
		})
	}
}

func TestRetryAdviceForErrorPreservesOuterAdviceAndCause(t *testing.T) {
	for _, advice := range []provider.RetryAdvice{
		{}, {Category: provider.RetryPermanent}, {Category: provider.RetryStreamInterrupted, Forbidden: true},
		{Category: provider.RetryRateLimit, NotBefore: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)},
	} {
		err := provider.WithRetryAdvice(provider.WithRetryAdvice(io.EOF, provider.RetryAdvice{Category: provider.RetryTransient}), advice)
		if got := provider.RetryAdviceForError(fmt.Errorf("outer: %w", err)); got != advice {
			t.Fatalf("got=%#v want=%#v", got, advice)
		}
		if !errors.Is(err, io.EOF) {
			t.Fatal("cause lost")
		}
	}
}
