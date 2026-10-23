package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
)

// One closure validation owns this ephemeral reducer. Related checkpoints on
// the same path advance it once; branches and Retry retain exact path selection.
// Nothing is published, persisted or trusted across a validation invocation.
type canonicalCheckpointValidation struct {
	leaf    entryLocator
	context ContextReducer
}

var errCheckpointRetryPath = errors.New("checkpoint requires retry path selection")

func (validation *canonicalCheckpointValidation) validate(ctx context.Context, tx spi.ReadTx, thread string, entry Entry) error {
	leaf, err := readEntryLocator(tx, thread, entry.ID)
	if err != nil {
		return err
	}
	if validation.leaf.ID != "" {
		same := false
		if validation.leaf.Depth < leaf.Depth {
			ancestor, err := entryAncestor(ctx, tx, thread, leaf, validation.leaf.Depth)
			if err != nil {
				return err
			}
			same = ancestor.ID == validation.leaf.ID
		}
		if !same {
			*validation = canonicalCheckpointValidation{}
		}
	}
	err = visitCanonicalPathMetadataAfter(ctx, tx, thread, entry.ID, validation.leaf.Depth, func(source Entry) error {
		if source.Type == EntryTurnMarker && source.TurnStatus == TurnStarted && (source.Metadata[RetrySourceTurnIDMetadataKey] != "" || source.Metadata[RetrySourceEntryIDMetadataKey] != "") {
			return errCheckpointRetryPath
		}
		complete, err := completeLocatedEntry(ctx, tx, source)
		if err != nil {
			return err
		}
		return validation.context.Append([]Entry{complete})
	})
	if errors.Is(err, errCheckpointRetryPath) {
		*validation = canonicalCheckpointValidation{}
		path, pathErr := canonicalCompletePath(ctx, tx, thread, entry.ID)
		if pathErr != nil {
			return pathErr
		}
		_, err = BuildContextChecked(path, ContextOptions{})
		return err
	}
	if err != nil {
		*validation = canonicalCheckpointValidation{}
		return err
	}
	validation.leaf = leaf
	return nil
}
