package agentharness

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v3/internal/sessiontree"
)

type ListRootThreadSummariesOptions struct {
	Limit          int
	AfterCreatedAt time.Time
	AfterID        string
}

type RootThreadInventoryItem struct {
	Thread   ThreadSnapshot
	Revision sessiontree.ThreadRevision
}

type rootThreadInventoryReader interface {
	ListRootThreadInventory(context.Context, sessiontree.ListThreadsOptions) ([]sessiontree.RootThreadInventoryItem, error)
}

func (h *AgentHarness) ListRootThreadInventory(ctx context.Context, opts ListRootThreadSummariesOptions) ([]RootThreadInventoryItem, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness is nil")
	}
	reader, ok := h.options.Repo.(rootThreadInventoryReader)
	if !ok {
		return nil, errors.New("session tree repo does not support bounded root thread inventory")
	}
	items, err := reader.ListRootThreadInventory(ctx, sessiontree.ListThreadsOptions{
		IncludeArchived: true,
		RootOnly:        true,
		Limit:           opts.Limit,
		AfterCreatedAt:  opts.AfterCreatedAt,
		AfterID:         strings.TrimSpace(opts.AfterID),
	})
	if err != nil {
		return nil, err
	}
	out := make([]RootThreadInventoryItem, 0, len(items))
	for _, item := range items {
		thread := h.cacheThread(item.Meta.ID)
		thread.mu.Lock()
		phase := thread.phase
		thread.mu.Unlock()
		phase = thread.canonicalThreadPhaseFromAuthority(phase, item.Authority)
		out = append(out, RootThreadInventoryItem{
			Thread: threadSnapshotFromJournal(ThreadJournalSnapshot{
				Meta: item.Meta, Path: item.Path, Phase: phase,
			}),
			Revision: item.Revision,
		})
	}
	return out, nil
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
