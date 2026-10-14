package runtime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadQueueEditReader reads a committed queue edit from canonical facts, even
// after subsequent editing, promotion, deletion or restart. Hosts can validate
// transport retries without resolving resources again. It grants no access.
type ThreadQueueEditReader interface {
	LookupQueuedEdit(context.Context, LookupQueuedEditInput) (QueuedInputEdit, bool, error)
}

type LookupQueuedEditInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

// QueuedInputEdit is the immutable before/after content of one committed edit.
type QueuedInputEdit struct {
	QueueItemID   string    `json:"queue_item_id"`
	ExpectedInput UserInput `json:"expected_input"`
	Input         UserInput `json:"input"`
}

var _ ThreadQueueEditReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) LookupQueuedEdit(ctx context.Context, in LookupQueuedEditInput) (QueuedInputEdit, bool, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return QueuedInputEdit{}, false, err
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return QueuedInputEdit{}, false, err
	}
	entry, err := service.host.store.repo.Entry(ctx, in.ThreadID.String(), "queue-edit:"+key+":replace")
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		return QueuedInputEdit{}, false, nil
	}
	if err != nil {
		return QueuedInputEdit{}, false, runtimeHostError(err)
	}
	var replacement QueuedInput
	if entry.Type != sessiontree.EntryQueueAdded {
		return QueuedInputEdit{}, false, ErrRequestConflict
	}
	if err := json.Unmarshal(entry.Payload, &replacement); err != nil {
		return QueuedInputEdit{}, false, err
	}
	entries, err := service.host.store.repo.Entries(ctx, in.ThreadID.String())
	if err != nil {
		return QueuedInputEdit{}, false, runtimeHostError(err)
	}
	var expected *UserInput
	for _, fact := range entries {
		if fact.ID == entry.ID {
			break
		}
		if fact.Type != sessiontree.EntryQueueAdded {
			continue
		}
		var queued QueuedInput
		if err := json.Unmarshal(fact.Payload, &queued); err != nil {
			return QueuedInputEdit{}, false, err
		}
		if queued.ID == replacement.ID {
			expected = &queued.Input
		}
	}
	if expected == nil {
		return QueuedInputEdit{}, false, ErrRequestConflict
	}
	return QueuedInputEdit{QueueItemID: replacement.ID, ExpectedInput: *expected, Input: replacement.Input}, true, nil
}
