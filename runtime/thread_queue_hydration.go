package runtime

import (
	"context"
	"encoding/json"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// Queue and restore state share one frozen journal inventory. Historical
// message bodies never enter this reducer, including on inactive branches.
func hydrateThreadQueues(ctx context.Context, repo sessiontree.JournalRepo, threadID identity.ThreadID) ([]QueuedInput, []RestoredInput, error) {
	queue := make([]QueuedInput, 0)
	// Keep only immutable record identities for consumed inputs. An explicit
	// restore marker still resolves the latest addition, including older stores
	// with repeated removal facts, without retaining every historical input.
	added := map[string]string{}
	var restored []RestoredInput
	observe := func(entry sessiontree.Entry) error {
		var err error
		queue, err = appendCanonicalQueue(queue, []sessiontree.Entry{entry})
		if err != nil {
			return err
		}
		if entry.Type == sessiontree.EntryQueueAdded {
			var item QueuedInput
			if err := json.Unmarshal(entry.Payload, &item); err != nil {
				return ErrAuthorityCorrupt
			}
			added[item.ID] = entry.ID
		}
		if entry.Type == sessiontree.EntryQueueDeleted || entry.Type == sessiontree.EntryQueuePromoted {
			var id string
			if err := json.Unmarshal(entry.Payload, &id); err != nil {
				return ErrAuthorityCorrupt
			}
			if entry.Type == sessiontree.EntryQueueDeleted && entry.Metadata[restoredFromBackupMetadata] == "true" {
				entryID, found := added[id]
				if !found {
					return ErrAuthorityCorrupt
				}
				source, err := repo.Entry(ctx, threadID.String(), entryID)
				if err != nil {
					return runtimeHostError(err)
				}
				var queued QueuedInput
				if source.Type != sessiontree.EntryQueueAdded || json.Unmarshal(source.Payload, &queued) != nil || queued.ID != id {
					return ErrAuthorityCorrupt
				}
				restored = append(restored, RestoredInput{ID: id, Input: queued.Input})
			}
		}
		return nil
	}
	if reader, ok := repo.(sessiontree.JournalMetadataReader); ok {
		options := sessiontree.JournalMetadataOptions{ThreadID: threadID.String(), Limit: 256}
		for {
			page, err := reader.JournalMetadata(ctx, options)
			if err != nil {
				return nil, nil, runtimeHostError(err)
			}
			for _, metadata := range page.Entries {
				if err := observe(metadata.Entry); err != nil {
					return nil, nil, err
				}
			}
			if !page.HasMore {
				break
			}
			if page.After <= options.After {
				return nil, nil, ErrAuthorityCorrupt
			}
			options.After, options.Through = page.After, page.Through
		}
	} else {
		// Existing custom JournalRepo implementations retain their exact adapter.
		entries, err := repo.Entries(ctx, threadID.String())
		if err != nil {
			return nil, nil, runtimeHostError(err)
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			if err := observe(entry); err != nil {
				return nil, nil, err
			}
		}
	}
	return queue, restored, nil
}
