package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadSendStateReader resolves an original submission without activating the
// thread, resending input, or inheriting a later command's execution identity.
type ThreadSendStateReader interface {
	LookupSendState(context.Context, LookupSendInput) (SendDisposition, bool, error)
}

type SendState string

const (
	SendQueued  SendState = "queued"
	SendStarted SendState = "started"
	SendRemoved SendState = "removed"
	// SendRedirected means a different command consumed the queue item. Its
	// possibly shared execution is not a result or cancellation target of this send.
	SendRedirected SendState = "redirected"
)

// SendDisposition identifies an execution only when the original request
// started its own turn. QueueItemID is present only while still queued.
type SendDisposition struct {
	State       SendState       `json:"state"`
	QueueItemID string          `json:"queue_item_id,omitempty"`
	TurnID      identity.TurnID `json:"turn_id,omitempty"`
	RunID       identity.RunID  `json:"run_id,omitempty"`
}

var _ ThreadSendStateReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) LookupSendState(ctx context.Context, in LookupSendInput) (SendDisposition, bool, error) {
	var out SendDisposition
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return out, false, err
	}
	if _, err = service.ensureThread(ctx, in.ThreadID); err != nil {
		return out, false, err
	}
	thread := in.ThreadID.String()
	repo := service.host.store.repo
	reader, ok := repo.(interface {
		LatestQueueFact(context.Context, string, string) (sessiontree.Entry, bool, error)
	})
	if !ok {
		return out, false, ErrUnsupportedStoreCapability
	}
	id := "queue:" + key
	fact, queued, err := reader.LatestQueueFact(ctx, thread, id)
	if err != nil {
		return out, false, runtimeHostError(err)
	}
	if queued {
		switch fact.Type {
		case sessiontree.EntryQueueAdded:
			return SendDisposition{State: SendQueued, QueueItemID: id}, true, nil
		case sessiontree.EntryQueueDeleted:
			if fact.ID == "steer-queue:"+fact.RequestKey {
				return SendDisposition{State: SendRedirected}, true, nil
			}
			return SendDisposition{State: SendRemoved}, true, nil
		case sessiontree.EntryQueuePromoted: // Promotion and turn admission commit atomically.
		default:
			return out, false, ErrAuthorityCorrupt
		}
	} else {
		_, err := repo.Entry(ctx, thread, "user:"+key)
		if errors.Is(err, sessiontree.ErrEntryNotFound) {
			return out, false, nil
		}
		if err != nil {
			return out, false, runtimeHostError(err)
		}
	}
	started, found, err := repo.FindTurnRequest(ctx, thread, key)
	if err != nil {
		return out, false, runtimeHostError(err)
	}
	if !found && queued && fact.RequestKey != key {
		return SendDisposition{State: SendRedirected}, true, nil
	}
	if !found || started.TurnID == "" || started.RunID == "" {
		return out, false, ErrAuthorityCorrupt
	}
	return SendDisposition{State: SendStarted, TurnID: identity.TurnID(started.TurnID), RunID: identity.RunID(started.RunID)}, true, nil
}
