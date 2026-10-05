package runtime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadSendReader is an optional read-only extension implemented by
// Host.ThreadService. It returns the original admitted input for a send key,
// even after queue editing, promotion, deletion, or restart. Hosts can validate
// transport retries without resolving attachments or refreshing runtime context.
// It does not authorize a caller or admit new work.
type ThreadSendReader interface {
	LookupSend(context.Context, LookupSendInput) (UserInput, bool, error)
}

type LookupSendInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

var _ ThreadSendReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) LookupSend(ctx context.Context, in LookupSendInput) (UserInput, bool, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return UserInput{}, false, err
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return UserInput{}, false, err
	}
	entry, found, err := service.lookupSendEntry(ctx, in.ThreadID, key)
	if err != nil || !found {
		return UserInput{}, found, err
	}
	if entry.Type == sessiontree.EntryQueueAdded {
		var queued QueuedInput
		if err := json.Unmarshal(entry.Payload, &queued); err != nil {
			return UserInput{}, false, err
		}
		return queued.Input, true, nil
	}
	return turnInputFromSessionMessage(entry.Message), true, nil
}

func (service *threadRuntimeService) lookupSendEntry(ctx context.Context, threadID identity.ThreadID, key string) (sessiontree.Entry, bool, error) {
	// Queue edits change the promoted user message. The initial queue fact
	// remains the immutable authority for the original transport request.
	for _, id := range []string{"queue:" + key, "user:" + key} {
		entry, err := service.host.store.repo.Entry(ctx, threadID.String(), id)
		if errors.Is(err, sessiontree.ErrEntryNotFound) {
			continue
		}
		if err != nil {
			return sessiontree.Entry{}, false, runtimeHostError(err)
		}
		return entry, true, nil
	}
	return sessiontree.Entry{}, false, nil
}
