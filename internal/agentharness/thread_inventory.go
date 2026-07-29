package agentharness

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v2/internal/sessiontree"
)

type ListRootThreadSummariesOptions struct {
	Limit          int
	AfterCreatedAt time.Time
	AfterID        string
}

func (h *AgentHarness) ListRootThreadSummaries(ctx context.Context, opts ListRootThreadSummariesOptions) ([]ThreadSummary, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness is nil")
	}
	metas, err := sessiontree.ListThreads(ctx, h.options.Repo, sessiontree.ListThreadsOptions{
		IncludeArchived: true,
		RootOnly:        true,
		Limit:           opts.Limit,
		AfterCreatedAt:  opts.AfterCreatedAt,
		AfterID:         strings.TrimSpace(opts.AfterID),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ThreadSummary, 0, len(metas))
	for _, meta := range metas {
		summary, err := h.cacheThread(meta.ID).Summary(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, summary)
	}
	return out, nil
}
