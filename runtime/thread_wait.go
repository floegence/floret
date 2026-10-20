package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/identity"
)

// ThreadWaiter observes current state without admitting input or resuming parents.
// Omitted targets are frozen by the first scoped read.
type ThreadWaiter interface {
	WaitThreads(context.Context, WaitThreadsInput) (ThreadWaitResult, error)
}
type WaitThreadsInput struct {
	// PendingParentEvents returns pending canonical receipts for Scope.ParentID.
	// This is passive observation and never wakes an idle parent execution.
	PendingParentEvents bool

	Scope     ThreadScope
	ThreadIDs []identity.ThreadID
	Timeout   time.Duration
}
type ThreadWaitReason string

const ThreadWaitEvents ThreadWaitReason = "events"

const (
	ThreadWaitCompleted         ThreadWaitReason = "completed"
	ThreadWaitAttentionRequired ThreadWaitReason = "attention_required"
	ThreadWaitMissing           ThreadWaitReason = "missing"
	ThreadWaitTimedOut          ThreadWaitReason = "timed_out"
)

type ThreadWaitResult struct {
	ParentEvents     *ParentEventPage    `json:"parent_events,omitempty"`
	Threads          []ThreadSummary     `json:"threads"`
	MissingThreadIDs []identity.ThreadID `json:"missing_thread_ids"`
	Reason           ThreadWaitReason    `json:"reason"`
}

var _ ThreadWaiter = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) WaitThreads(ctx context.Context, in WaitThreadsInput) (ThreadWaitResult, error) {
	if in.PendingParentEvents && in.Scope.ParentID == nil {
		return ThreadWaitResult{}, errors.New("parent event wait requires a parent scope")
	}
	if in.Timeout <= 0 {
		return ThreadWaitResult{}, errors.New("thread wait requires a positive timeout")
	}
	targets := append([]identity.ThreadID(nil), in.ThreadIDs...)
	seen := make(map[identity.ThreadID]bool, len(targets))
	for _, id := range targets {
		if id == "" || seen[id] {
			return ThreadWaitResult{}, errors.New("thread wait requires unique nonempty thread IDs")
		}
		seen[id] = true
	}
	waitCtx, cancel := context.WithTimeout(ctx, in.Timeout)
	defer cancel()
	sub, err := service.subscribe(waitCtx, true)
	if err != nil {
		return ThreadWaitResult{}, err
	}
	defer sub.Close()
	var latest ThreadWaitResult
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return ThreadWaitResult{}, err
		}
		items, err := service.List(waitCtx, in.Scope)
		if err != nil {
			if first || ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded) || waitCtx.Err() == nil {
				return ThreadWaitResult{}, err
			}
			latest.Reason = ThreadWaitTimedOut
			return latest, nil
		}
		if first && len(targets) == 0 {
			for _, item := range items {
				targets = append(targets, item.ID)
			}
		}
		first = false
		latest = selectWaitThreads(items, targets)
		if in.PendingParentEvents && len(targets) > 0 && latest.Reason != ThreadWaitMissing && latest.Reason != ThreadWaitAttentionRequired {
			page, err := service.ListParentEvents(waitCtx, ListParentEventsInput{ThreadID: *in.Scope.ParentID, SourceThreadIDs: targets, PendingOnly: true, Limit: 100})
			if err != nil {
				if ctx.Err() != nil {
					return ThreadWaitResult{}, ctx.Err()
				}
				if errors.Is(err, context.DeadlineExceeded) && waitCtx.Err() != nil {
					latest.Reason = ThreadWaitTimedOut
					return latest, nil
				}
				return ThreadWaitResult{}, err
			}
			latest.ParentEvents = &page
			if len(page.Events) > 0 {
				latest.Reason = ThreadWaitEvents
			}
		}
		if latest.Reason != "" {
			return latest, nil
		}
		select {
		case <-ctx.Done():
			return ThreadWaitResult{}, ctx.Err()
		case <-waitCtx.Done():
			if err := ctx.Err(); err != nil {
				return ThreadWaitResult{}, err
			}
			latest.Reason = ThreadWaitTimedOut
			return latest, nil
		case <-sub.done:
			if err := ctx.Err(); err != nil {
				return ThreadWaitResult{}, err
			}
			sub.mu.Lock()
			err := sub.closeErr
			sub.mu.Unlock()
			if errors.Is(err, context.DeadlineExceeded) && waitCtx.Err() != nil {
				latest.Reason = ThreadWaitTimedOut
				return latest, nil
			}
			return ThreadWaitResult{}, err
		case <-sub.views:
		case <-sub.changed:
		}
	}
}
func selectWaitThreads(items []ThreadSummary, targets []identity.ThreadID) ThreadWaitResult {
	out := ThreadWaitResult{Threads: make([]ThreadSummary, 0, len(targets)), MissingThreadIDs: []identity.ThreadID{}}
	byID := make(map[identity.ThreadID]ThreadSummary, len(items))
	for _, item := range items {
		byID[item.ID] = item
	}
	attention, active := false, false
	for _, id := range targets {
		item, ok := byID[id]
		if !ok {
			out.MissingThreadIDs = append(out.MissingThreadIDs, id)
			continue
		}
		out.Threads = append(out.Threads, item)
		attention = attention || item.Attention.InputCount > 0 || item.Attention.ApprovalCount > 0
		active = active || item.Activity == ThreadActivityActive || item.QueueCount > 0
	}
	switch {
	case len(out.MissingThreadIDs) > 0:
		out.Reason = ThreadWaitMissing
	case attention:
		out.Reason = ThreadWaitAttentionRequired
	case !active:
		out.Reason = ThreadWaitCompleted
	}
	return out
}
