package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/floegence/floret/v7/storage/spi"
)

// Freeze only mutable authority records. Immutable entries and body blocks are
// read in separate transactions; publication checks that their owner stayed put.
type canonicalSnapshotReader struct {
	migrationSourceReader
	format   backendDomainFormat
	metadata map[string]migrationMetadataRead
}

func (reader *canonicalSnapshotReader) Get(namespace string, key []byte) ([]byte, error) {
	mutable := false
	if namespace == reader.format.namespace {
		for _, kind := range []string{backendDomainRecordThread, backendDomainRecordTombstone, backendDomainRecordTodo, backendDomainRecordProviderState} {
			mutable = mutable || bytes.HasPrefix(key, backendDomainKey(kind))
		}
	}
	if !mutable {
		return reader.migrationSourceReader.Get(namespace, key)
	}
	if saved, found := reader.metadata[string(key)]; found {
		if !saved.found {
			return nil, spi.ErrNotFound
		}
		return saved.value, nil
	}
	raw, err := reader.migrationSourceReader.Get(namespace, key)
	if err != nil && !errors.Is(err, spi.ErrNotFound) {
		return nil, err
	}
	reader.metadata[string(key)] = migrationMetadataRead{key: bytes.Clone(key), value: raw, found: err == nil}
	return raw, err
}

func (reader *canonicalSnapshotReader) verify(tx spi.ReadTx) error {
	for _, previous := range reader.metadata {
		raw, err := tx.Get(reader.format.namespace, previous.key)
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		if (err == nil) != previous.found || !bytes.Equal(raw, previous.value) {
			return spi.ErrConflict
		}
	}
	return nil
}

func (reader *canonicalSnapshotReader) validationError(err error) error {
	if err == nil || reader.ctx.Err() != nil {
		return err
	}
	if changed := reader.backend.View(reader.ctx, reader.verify); changed != nil {
		return changed
	}
	return err
}

// The caller owns the migration gate. Canonical metadata reads
// release the backend between pages; each metadata transaction writes a bounded
// document batch. The manifest appears only after every batch commits.
func (repo *BackendRepo) prepareQueryThread(ctx context.Context, threadID string) error {
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: repo.domainFormat(), metadata: map[string]migrationMetadataRead{}}
	metas, err := validateCanonicalThreadClosure(ctx, reader, threadID)
	if err != nil {
		return reader.validationError(err)
	}
	if changed, err := repo.prepareParentEvents(ctx, reader, threadID); err != nil {
		return err
	} else if changed {
		// Reload after our own canonical append before validating index sources.
		return spi.ErrConflict
	}
	state, err := projectCanonicalQuery(ctx, reader, metas[threadID])
	if err != nil {
		return reader.validationError(err)
	}
	if state == nil {
		return ErrThreadNotFound
	}
	var complete bool
	err = repo.backend.View(ctx, func(tx spi.ReadTx) error {
		if err := reader.verify(tx); err != nil {
			return err
		}
		var err error
		complete, err = threadQueryIndexMatches(tx, threadID, state)
		return err
	})
	if err != nil && !errors.Is(err, spi.ErrIndexCorrupt) {
		return err
	}
	if !complete {
		// A missing manifest prevents every query from exposing partial metadata.
		err = func() error {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				if err := reader.verify(tx); err != nil {
					return err
				}
				if err := tx.Delete(queryManifestNamespace, []byte(threadID)); err != nil && !errors.Is(err, spi.ErrNotFound) {
					return err
				}
				return queryMetadataWriter(tx).deleteThread(threadID)
			})
			if err == nil {
				delete(repo.queryThreads, threadID)
			}
			return err
		}()
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(state.records))
		for key := range state.records {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		batch, batchBytes := []queryStoredDocument{}, 0
		flush := func() error {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				if err := reader.verify(tx); err != nil {
					return err
				}
				writer := queryMetadataWriter(tx)
				for _, document := range batch {
					if err := writer.putDocument(document); err != nil {
						return err
					}
				}
				return nil
			})
		}
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			document, err := queryDocument(state.records[key])
			if err != nil {
				return err
			}
			size := len(document.Metadata)
			if len(batch) > 0 && (len(batch) >= 64 || batchBytes+size > backendThreadMigrationBatchBytes) {
				if err := flush(); err != nil {
					return err
				}
				batch, batchBytes = nil, 0
			}
			batch, batchBytes = append(batch, document), batchBytes+size
		}
		if len(batch) > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	err = repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := reader.verify(tx); err != nil {
			return err
		}
		if err := clearThreadMigrationFailure(tx, threadID); err != nil {
			return err
		}
		return putQueryManifest(tx, threadID, state.leaf)
	})
	if err != nil {
		return err
	}
	if repo.queryThreads == nil {
		repo.queryThreads = map[string]*queryThread{}
	}
	compactQueryThread(state)
	repo.queryThreads[threadID] = state
	if !complete && repo.threadQuerySnapshots != nil {
		repo.threadQuerySnapshots.invalidateThread(threadID)
	}
	repo.rememberVerifiedThread(threadID, state.leaf)
	return nil
}

// Migration traversal checkpoints its cursor with the source manifest so a
// restart after each batch still reaches cleanup. A current store only needs a
// disposable verification cursor; incomplete per-thread metadata are rebuilt.
func (repo *BackendRepo) prepareNextQueryThread(ctx context.Context) (bool, error) {
	prefix := backendDomainKey(backendDomainRecordThread)
	var row spi.Record
	var checkpoint bool
	var cursor []byte
	err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		cursor = repo.queryPreparationCursor
		if repo.domainFormat().version >= backendDomainV32Version {
			manifest, err := readV32Manifest(tx)
			if err != nil {
				return err
			}
			checkpoint = manifest.MigrationPhase == "index"
			if checkpoint {
				cursor = manifest.MigrationCursor
			}
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: repo.domainFormat().namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: cursor, Limit: 1})
		if err != nil {
			return err
		}
		if len(page.Records) > 0 {
			row = page.Records[0]
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if row.Key == nil {
		err := repo.backend.View(ctx, migrationFailureRemaining)
		if err != nil {
			return false, err
		}
		return false, nil
	}
	record, err := decodeBackendDomainRecord(repo.domainFormat(), row.Value)
	if err != nil || record.Kind != backendDomainRecordThread || record.ID == "" || !bytes.Equal(row.Key, backendDomainKey(backendDomainRecordThread, record.ID)) {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	err = repo.prepareQueryThread(ctx, record.ID)
	if errors.Is(err, spi.ErrConflict) {
		return true, nil
	}
	if errors.Is(err, ErrThreadNotFound) || errors.Is(err, ErrThreadDeleted) {
		err = nil
	}
	if errors.Is(err, ErrAuthorityCorrupt) {
		if failed := repo.backend.Update(ctx, func(tx spi.WriteTx) error { return writeThreadMigrationFailure(tx, record.ID, err) }); failed != nil {
			return false, failed
		}
		err = nil
	}
	if err != nil {
		return false, err
	}
	if checkpoint {
		if err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			manifest, err := readV32Manifest(tx)
			if err != nil {
				return err
			}
			if manifest.MigrationPhase != "index" || !bytes.Equal(manifest.MigrationCursor, cursor) {
				return spi.ErrConflict
			}
			manifest.MigrationCursor = bytes.Clone(row.Key)
			return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
		}); err != nil {
			return false, err
		}
	}
	repo.queryPreparationCursor = bytes.Clone(row.Key)
	return true, nil
}
