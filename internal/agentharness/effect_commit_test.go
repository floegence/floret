package agentharness

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestEffectCommitDispositionUsesTypedErrors(t *testing.T) {
	for _, sample := range []struct {
		err   error
		class string
		retry bool
	}{
		{sessiontree.ErrRequestConflict, "request_conflict", false},
		{errors.Join(sessiontree.ErrAuthorityCorrupt, context.DeadlineExceeded), "authority_corrupt", false},
		{sessiontree.ErrInvalidThreadAuthority, "invalid_authority", false},
		{sessiontree.ErrStaleAuthority, "stale_authority", false},
		{context.DeadlineExceeded, "deadline_exceeded", true},
		{context.Canceled, "cancelled", true},
		{spi.ErrConflict, "transaction_conflict", true},
		{errors.New("authority_corrupt: sensitive data"), "storage_error", true},
	} {
		if class, retry := effectCommitDisposition(sample.err); class != sample.class || retry != sample.retry {
			t.Fatalf("got %q/%v, want %q/%v", class, retry, sample.class, sample.retry)
		}
	}
}
