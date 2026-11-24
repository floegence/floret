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

type threadWaitCondition struct {
	turn      identity.TurnID
	run       identity.RunID
	activity  ThreadActivity
	attention AttentionSummary
	queued    int
}

func waitCondition(view *ThreadView) threadWaitCondition {
	return threadWaitCondition{view.TurnID, view.RunID, view.Activity, view.Attention, len(view.Queue)}
}

type threadWaitInterest struct {
	service      *threadRuntimeService
	targets      map[identity.ThreadID]bool
	parentEvents identity.ThreadID
	changed      chan struct{}
}

func (service *threadRuntimeService) registerWait(targets map[identity.ThreadID]bool, parent identity.ThreadID) (*threadWaitInterest, error) {
	if err := service.host.available(); err != nil {
		return nil, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.subscriptionsClosed {
		return nil, ErrHostClosed
	}
	if service.waiters == nil {
		service.waiters = make(map[*threadWaitInterest]struct{})
	}
	interest := &threadWaitInterest{service: service, targets: targets, parentEvents: parent, changed: make(chan struct{}, 1)}
	service.waiters[interest] = struct{}{}
	return interest, nil
}

func (interest *threadWaitInterest) Close() {
	interest.service.mu.Lock()
	defer interest.service.mu.Unlock()
	delete(interest.service.waiters, interest)
}

func (interest *threadWaitInterest) next(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, open := <-interest.changed:
		if !open {
			return ErrHostClosed
		}
		return nil
	}
}

func (service *threadRuntimeService) notifyWaiters(id identity.ThreadID, targetChanged, parentChanged bool) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.notifyWaitersLocked(id, targetChanged, parentChanged)
}

func (service *threadRuntimeService) notifyWaitersLocked(id identity.ThreadID, targetChanged, parentChanged bool) {
	for waiter := range service.waiters {
		if targetChanged && (waiter.targets == nil || waiter.targets[id]) || parentChanged && waiter.parentEvents == id {
			select {
			case waiter.changed <- struct{}{}:
			default:
			}
		}
	}
}

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
	var parent identity.ThreadID
	if in.PendingParentEvents {
		parent = *in.Scope.ParentID
	}
	initial := seen
	if len(targets) == 0 {
		initial = nil
	}
	interest, err := service.registerWait(initial, parent)
	if err != nil {
		return ThreadWaitResult{}, err
	}
	defer interest.Close()
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
				seen[item.ID] = true
			}
			service.mu.Lock()
			interest.targets = seen
			service.mu.Unlock()
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
			if len(page.Events) > 0 {
				// Receipts commit before finishSend installs the terminal view. Read
				// summaries after the receipts, then wait on the existing actor
				// subscription until that same execution's result is readable.
				items, err = service.List(waitCtx, in.Scope)
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
				latest = selectWaitThreads(items, targets)
				if latest.Reason != ThreadWaitMissing && latest.Reason != ThreadWaitAttentionRequired && parentEventsSettled(page, latest.Threads) {
					latest.ParentEvents = &page
					latest.Reason = ThreadWaitEvents
				}
			} else {
				latest.ParentEvents = &page
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
		case _, open := <-interest.changed:
			if err := ctx.Err(); err != nil {
				return ThreadWaitResult{}, err
			}
			if !open {
				return ThreadWaitResult{}, ErrHostClosed
			}
		}
	}
}

func parentEventsSettled(page ParentEventPage, threads []ThreadSummary) bool {
	for _, event := range page.Events {
		switch event.Kind {
		case ParentEventCompleted, ParentEventFailed, ParentEventCancelled, ParentEventInterrupted, ParentEventTimedOut:
			for _, thread := range threads {
				if thread.ID == event.SourceThreadID && thread.TurnID == event.SourceTurnID && thread.RunID == event.SourceRunID && thread.Activity == ThreadActivityActive {
					return false
				}
			}
		}
	}
	return true
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
