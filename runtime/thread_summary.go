package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/identity"
)

// ThreadSummaryReader reads canonical summaries without constructing ThreadView
// history or creating a thread actor. Hosts authorize the thread before reading.
type ThreadSummaryReader interface {
	Summary(context.Context, identity.ThreadID) (ThreadSummary, error)
}

var _ ThreadSummaryReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) Summary(ctx context.Context, id identity.ThreadID) (ThreadSummary, error) {
	if ctx == nil {
		return ThreadSummary{}, errors.New("thread summary requires a context and valid thread ID")
	}
	if err := ctx.Err(); err != nil {
		return ThreadSummary{}, err
	}
	if _, err := identity.ParseThreadID(id.String()); err != nil {
		return ThreadSummary{}, errors.New("thread summary requires a context and valid thread ID")
	}
	if err := service.host.available(); err != nil {
		return ThreadSummary{}, err
	}
	meta, err := service.host.store.repo.Thread(ctx, id.String())
	if err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	path, err := service.host.store.repo.Path(ctx, id.String(), meta.LeafID)
	if err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	summary, err := threadSummaryFromCanonicalPath(meta, path)
	if err != nil {
		return ThreadSummary{}, err
	}
	service.applyActiveThreadSummary(&summary)
	return summary, nil
}
