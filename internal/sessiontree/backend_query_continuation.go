package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

const queryContinuationCacheLimit = 32
const queryContinuationMaxBytes = 128 << 10

// A continuation is disposable in-process state. Cold reconstruction reads
// bounded canonical intervals outside the repository lock and write transaction.
// The mutation checks the pinned metadata and manifest before using the result.
type canonicalQueryContinuation struct {
	threadID string
	metadata []byte
	manifest []byte
	state    *queryThread
}

func (repo *BackendRepo) loadQueryContinuation(ctx context.Context, threadID string) (*canonicalQueryContinuation, error) {
	continuation := &canonicalQueryContinuation{threadID: threadID}
	var meta ThreadMeta
	err := func() error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.backend.View(ctx, func(tx spi.ReadTx) error {
			var err error
			meta, err = readPublishedThreadMeta(tx, backendDomainV32Format, threadID)
			if err != nil {
				return err
			}
			continuation.metadata, err = tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordThread, threadID))
			if err != nil {
				return err
			}
			continuation.manifest, err = tx.Get(queryManifestNamespace, []byte(threadID))
			if errors.Is(err, spi.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if previous := repo.queryThreads[threadID]; previous != nil && previous.leaf == meta.LeafID {
				continuation.state = previous
			}
			return nil
		})
	}()
	if err != nil {
		return nil, err
	}
	var manifest queryManifest
	if json.Unmarshal(continuation.manifest, &manifest) != nil || manifest.Version != 3 || manifest.Leaf != meta.LeafID {
		continuation.state = nil
		return continuation, nil
	}
	if continuation.state == nil {
		// Bound each storage snapshot so other threads and browsing can proceed
		// between pages. Canonical sources are immutable; each page verifies the
		// pinned metadata and manifest before feeding the same query projector.
		state := newQueryThread()
		projector := &queryProjector{metadataOnly: true}
		var leaf entryLocator
		err = repo.backend.View(ctx, func(tx spi.ReadTx) error {
			if err := continuation.verify(tx); err != nil {
				return err
			}
			if meta.LeafID == "" {
				return nil
			}
			var err error
			leaf, err = readEntryLocator(tx, threadID, meta.LeafID)
			return err
		})
		for first := int64(1); err == nil && first <= leaf.Depth; first += 128 {
			last := min(first+127, leaf.Depth)
			err = repo.backend.View(ctx, func(tx spi.ReadTx) error {
				if err := continuation.verify(tx); err != nil {
					return err
				}
				return visitCanonicalMetadataInterval(ctx, tx, threadID, leaf, first, last, func(entry Entry) error {
					entry, err := canonicalQueryProjectionEntry(ctx, tx, entry, false)
					if err != nil {
						return err
					}
					return projector.observe(state, threadID, entry)
				})
			})
		}
		projector.finish(state, threadID, meta)
		continuation.state = state
		if err != nil {
			if changed := repo.backend.View(ctx, continuation.verify); changed != nil {
				return nil, changed
			}
			return nil, err
		}
		compactQueryThread(continuation.state)
	}
	return continuation, nil
}

func (continuation *canonicalQueryContinuation) verify(tx spi.ReadTx) error {
	metadata, err := tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordThread, continuation.threadID))
	if errors.Is(err, spi.ErrNotFound) {
		return spi.ErrConflict
	}
	if err != nil {
		return err
	}
	manifest, err := tx.Get(queryManifestNamespace, []byte(continuation.threadID))
	if err != nil && !errors.Is(err, spi.ErrNotFound) {
		return err
	}
	if !bytes.Equal(metadata, continuation.metadata) || !bytes.Equal(manifest, continuation.manifest) {
		return spi.ErrConflict
	}
	return nil
}

// Only bounded continuations remain resident. Oversized histories are valid;
// their next mutation reconstructs a temporary continuation outside the lock.
func (repo *BackendRepo) rememberQueryThread(id string, state *queryThread) {
	delete(repo.queryThreads, id)
	if !queryContinuationFits(state) {
		return
	}
	if repo.queryThreads == nil {
		repo.queryThreads = map[string]*queryThread{}
	}
	if len(repo.queryThreads) >= queryContinuationCacheLimit {
		for previous := range repo.queryThreads {
			delete(repo.queryThreads, previous)
			break
		}
	}
	repo.queryThreads[id] = state
}

func queryContinuationFits(state *queryThread) bool {
	if state == nil {
		return false
	}
	size := len(state.leaf) + len(state.previousContentSource) + len(state.lifecycle.LatestTurnID) + 1024
	for _, ids := range []map[string]bool{state.lifecycle.started, state.lifecycle.terminal, state.lifecycle.baseStarted, state.lifecycle.baseTerminal} {
		for id := range ids {
			size += len(id) + 64
			if size > queryContinuationMaxBytes {
				return false
			}
		}
	}
	addRecord := func(record *queryRecord) {
		if record != nil {
			size += int(record.estimateBytes()) + len(record.row.ThreadID) + len(record.row.TurnID) + len(record.row.RunID) + len(record.row.ToolCallID) + len(record.row.ToolName)
		}
	}
	addRecord(state.previous)
	for execution, pending := range state.hostedPending {
		size += len(execution) + 64
		for _, record := range pending {
			addRecord(record)
			if size > queryContinuationMaxBytes {
				return false
			}
		}
	}
	return size <= queryContinuationMaxBytes
}
