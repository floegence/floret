package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

// Freeze only mutable authority records. Immutable entries and body blocks are
// read in separate transactions; publication checks that their owner stayed put.
type canonicalSnapshotReader struct {
	migrationSourceReader
	format   backendDomainFormat
	metadata map[string]migrationMetadataRead
	repo     *BackendRepo
	thread   string
	peers    map[string]*canonicalSnapshotReader
}

type snapshotView func(context.Context, func(spi.ReadTx) error) error

func (view snapshotView) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return view(ctx, read)
}

func (repo *BackendRepo) threadSnapshot(ctx context.Context, thread string) *canonicalSnapshotReader {
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: repo.domainFormat(), metadata: map[string]migrationMetadataRead{}}
	if _, scoped := repo.backend.(interface {
		ViewThreadRecords(context.Context, storagebridge.ThreadRead, func(spi.ReadTx) error) error
	}); scoped {
		reader.repo, reader.thread = repo, thread
		reader.peers = map[string]*canonicalSnapshotReader{thread: reader}
		reader.backend = snapshotView(func(ctx context.Context, read func(spi.ReadTx) error) error {
			return repo.viewThreadRecords(ctx, thread, read)
		})
	}
	return reader
}

func threadSnapshotRecords(tx spi.ReadTx, thread string) spi.ReadTx {
	reader, ok := tx.(*canonicalSnapshotReader)
	if !ok || reader.repo == nil {
		return tx
	}
	if peer := reader.peers[thread]; peer != nil {
		return peer
	}
	peer := reader.repo.threadSnapshot(reader.ctx, thread)
	peer.peers = reader.peers
	reader.peers[thread] = peer
	return peer
}

func (reader *canonicalSnapshotReader) verifyPeers() error {
	for id, peer := range reader.peers {
		if id == reader.thread || len(peer.metadata) == 0 {
			continue
		}
		if err := peer.backend.View(reader.ctx, peer.verify); err != nil {
			return err
		}
	}
	return nil
}

func (reader *canonicalSnapshotReader) update(repo *BackendRepo, thread string, write func(spi.WriteTx) error) error {
	if err := reader.verifyPeers(); err != nil {
		return err
	}
	return repo.updateThreadRecords(reader.ctx, thread, write)
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
	if changed := reader.verifyPeers(); changed != nil {
		return changed
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
	reader := repo.threadSnapshot(ctx, threadID)
	if changed, err := repo.prepareParentEvents(ctx, reader, threadID); err != nil {
		return err
	} else if changed {
		// Reload after our own canonical append before validating index sources.
		return spi.ErrConflict
	}
	if member.Enabled(repo.backend) {
		return repo.prepareMemberQuery(ctx, threadID)
	}
	metas, err := validateCanonicalThreadClosure(ctx, reader, threadID)
	if err != nil {
		return reader.validationError(err)
	}
	state, err := projectCanonicalQuery(ctx, reader, metas[threadID])
	if err != nil {
		return reader.validationError(err)
	}
	if state == nil {
		return ErrThreadNotFound
	}
	complete, err := threadQueryIndexMatches(reader, threadID, state)
	if err != nil && !errors.Is(err, spi.ErrIndexCorrupt) {
		return reader.validationError(err)
	}
	if !complete {
		// A missing manifest prevents every query from exposing partial metadata.
		more := true
		for more {
			err = func() error {
				repo.mu.Lock()
				defer repo.mu.Unlock()
				err := reader.update(repo, threadID, func(tx spi.WriteTx) error {
					if err := reader.verify(tx); err != nil {
						return err
					}
					if err := tx.Delete(queryManifestNamespace, []byte(threadID)); err != nil && !errors.Is(err, spi.ErrNotFound) {
						return err
					}
					var err error
					more, err = queryMetadataWriter(tx).deleteThreadPage(threadID)
					return err
				})
				if err == nil {
					delete(repo.queryThreads, threadID)
				}
				return err
			}()
			if err != nil {
				return err
			}
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
			return reader.update(repo, threadID, func(tx spi.WriteTx) error {
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
	err = reader.update(repo, threadID, func(tx spi.WriteTx) error {
		if err := reader.verify(tx); err != nil {
			return err
		}
		if err := clearThreadMigrationFailure(storagebridge.CatalogWriter(tx), threadID); err != nil {
			return err
		}
		return putQueryManifest(tx, threadID, state.leaf)
	})
	if err != nil {
		return err
	}
	compactQueryThread(state)
	repo.rememberQueryThread(threadID, state)
	if !complete && repo.threadQuerySnapshots != nil {
		repo.threadQuerySnapshots.invalidateThread(threadID)
	}
	repo.rememberVerifiedThread(threadID, state.leaf)
	return nil
}

// Migration traversal checkpoints its cursor with the source manifest so a
// restart after each batch still reaches cleanup. Current-store startup checks
// durable readiness anchors, not complete historical integrity. Missing query
// metadata still takes the validated rebuild path; explicit preparation and
// inspection retain full validation. The current-store cursor is disposable.
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
	ready := false
	if !checkpoint {
		ready, err = repo.ThreadPrepared(ctx, record.ID)
		if errors.Is(err, ErrAuthorityCorrupt) {
			// A retained failure is retried through full preparation, never
			// cleared merely because bounded readiness anchors look valid.
			ready, err = false, nil
		}
	}
	if err == nil && !ready {
		err = repo.prepareQueryThread(ctx, record.ID)
	} else if err == nil && repo.parentEventsEnabled() {
		// Recovery is independent of index readiness: enabling parent events
		// may need receipts even when both threads already have complete indexes.
		reader := repo.threadSnapshot(ctx, record.ID)
		var changed bool
		changed, err = repo.prepareParentEvents(ctx, reader, record.ID)
		if err == nil && changed {
			return true, nil
		}
	}
	// Optional display preparation belongs to the requested window or explicit
	// PrepareDisplayThread call. Startup prepares canonical execution/query
	// authority only; unopened history must not build a full hidden field tree.

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
