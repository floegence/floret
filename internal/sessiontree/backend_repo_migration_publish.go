package sessiontree

import (
	"bytes"
	"context"
	"errors"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

// Migration sources are immutable until cleanup. Each access releases the
// backend transaction; the migration gate keeps cleanup from racing validation.
// Runtime operations continue using the canonical owner between these reads.
type migrationSourceReader struct {
	ctx     context.Context
	backend spi.Backend
}

var _ spi.BoundedRecordReader = migrationSourceReader{}

func (reader migrationSourceReader) NamespaceHasRecords(namespace string) (found bool, err error) {
	if namespace == "" {
		return false, spi.ErrInvalidArgument
	}
	err = reader.backend.View(reader.ctx, func(tx spi.ReadTx) error {
		if bounded, ok := tx.(spi.BoundedRecordReader); ok {
			found, err = bounded.NamespaceHasRecords(namespace)
			return err
		}
		page, scanErr := tx.Scan(spi.ScanRequest{Namespace: namespace, Limit: 1})
		found = len(page.Records) != 0
		return scanErr
	})
	return found, err
}

func (reader migrationSourceReader) Get(namespace string, key []byte) (value []byte, err error) {
	err = reader.backend.View(reader.ctx, func(tx spi.ReadTx) error {
		value, err = tx.Get(namespace, key)
		return err
	})
	return value, err
}

func (reader migrationSourceReader) Scan(request spi.ScanRequest) (page spi.ScanPage, err error) {
	err = reader.backend.View(reader.ctx, func(tx spi.ReadTx) error {
		page, err = tx.Scan(request)
		return err
	})
	return page, err
}

func (reader migrationSourceReader) ReadFloretRecords(request storagebridge.RecordBatch) (values [][]byte, err error) {
	err = reader.backend.View(reader.ctx, func(tx spi.ReadTx) error {
		values, err = storagebridge.ReadRecords(tx, request)
		return err
	})
	return values, err
}

func (reader migrationSourceReader) RecordSize(namespace string, key []byte) (size int64, err error) {
	if namespace == "" || len(key) == 0 {
		return 0, spi.ErrInvalidArgument
	}
	err = reader.backend.View(reader.ctx, func(tx spi.ReadTx) error {
		if bounded, ok := tx.(spi.BoundedRecordReader); ok {
			size, err = bounded.RecordSize(namespace, key)
			return err
		}
		value, readErr := tx.Get(namespace, key)
		size = int64(len(value))
		return readErr
	})
	return size, err
}

func (reader migrationSourceReader) ReadRecordRange(namespace string, key []byte, offset int64, length int) (value []byte, err error) {
	if namespace == "" || len(key) == 0 || offset < 0 || length <= 0 {
		return nil, spi.ErrInvalidArgument
	}
	err = reader.backend.View(reader.ctx, func(tx spi.ReadTx) error {
		if bounded, ok := tx.(spi.BoundedRecordReader); ok {
			value, err = bounded.ReadRecordRange(namespace, key, offset, length)
			return err
		}
		raw, readErr := tx.Get(namespace, key)
		if readErr != nil {
			return readErr
		}
		if offset >= int64(len(raw)) {
			return nil
		}
		value = bytes.Clone(raw[offset : offset+min(int64(length), int64(len(raw))-offset)])
		return nil
	})
	return value, err
}

type migrationMetadataRead struct {
	key   []byte
	value []byte
	found bool
}

// Only the migration validator can see staged entries with source metadata.
// Canonical runtime readers never use this adapter. Target metadata is pinned
// and compared again inside the final publication transaction.
type migrationCandidateReader struct {
	migrationSourceReader
	sourceReader spi.ReadTx
	source       backendDomainFormat
	target       backendDomainFormat
	metadata     map[string]migrationMetadataRead
}

func (reader *migrationCandidateReader) targetMetadata(key []byte) ([]byte, error) {
	if previous, ok := reader.metadata[string(key)]; ok {
		if !previous.found {
			return nil, spi.ErrNotFound
		}
		return previous.value, nil
	}
	raw, err := reader.migrationSourceReader.Get(reader.target.namespace, key)
	if err != nil && !errors.Is(err, spi.ErrNotFound) {
		return nil, err
	}
	reader.metadata[string(key)] = migrationMetadataRead{key: bytes.Clone(key), value: raw, found: err == nil}
	return raw, err
}

func (reader *migrationCandidateReader) Get(namespace string, key []byte) ([]byte, error) {
	if namespace != reader.target.namespace || !bytes.HasPrefix(key, backendDomainKey(backendDomainRecordThread)) && !bytes.HasPrefix(key, backendDomainKey(backendDomainRecordTombstone)) {
		return reader.migrationSourceReader.Get(namespace, key)
	}
	raw, err := reader.targetMetadata(key)
	if !errors.Is(err, spi.ErrNotFound) {
		return raw, err
	}
	if bytes.HasPrefix(key, backendDomainKey(backendDomainRecordTombstone)) {
		return nil, err
	}
	raw, err = reader.sourceReader.Get(reader.source.namespace, key)
	if err != nil {
		return nil, err
	}
	record, err := decodeBackendDomainRecord(reader.source, raw)
	if err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if _, err := reader.targetMetadata(backendDomainKey(backendDomainRecordTombstone, record.ID)); err == nil {
		return nil, spi.ErrNotFound
	} else if !errors.Is(err, spi.ErrNotFound) {
		return nil, err
	}
	checkpoint, found, err := readThreadMigrationCheckpoint(reader.migrationSourceReader, record.ID)
	if err != nil || !found || !checkpoint.Complete {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	return encodeBackendDomainRecord(reader.target, record.Kind, record.ID, record.ThreadID, record.Ordinal, record.Value)
}

func (reader *migrationCandidateReader) verifyMetadata(tx spi.ReadTx) error {
	for _, previous := range reader.metadata {
		raw, err := tx.Get(reader.target.namespace, previous.key)
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		if (err == nil) != previous.found || !bytes.Equal(raw, previous.value) {
			return spi.ErrConflict
		}
	}
	return nil
}

func (reader *migrationCandidateReader) validationError(err error) error {
	if err == nil || reader.ctx.Err() != nil {
		return err
	}
	if changed := reader.backend.View(reader.ctx, reader.verifyMetadata); changed != nil {
		return changed
	}
	return err
}

func (repo *BackendRepo) publishSourceThread(ctx context.Context, threadID string) error {
	source := *repo.migrationSource
	reader := migrationSourceReader{ctx: ctx, backend: repo.backend}
	return repo.publishSourceThreadFrom(ctx, threadID, source, reader)
}

func (repo *BackendRepo) publishSourceThreadFrom(ctx context.Context, threadID string, source backendDomainFormat, reader spi.ReadTx) error {
	target := repo.domainFormat()
	if record, found, err := readBackendDomainRecord(reader, source, backendDomainKey(backendDomainRecordTombstone, threadID), backendDomainRecordTombstone, threadID, "", 0); err != nil {
		return err
	} else if found {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.backend.Update(ctx, func(tx spi.WriteTx) error { return copyMigrationTombstone(ctx, tx, source, target, record) })
	}
	candidateReader := &migrationCandidateReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, sourceReader: reader, source: source, target: target, metadata: map[string]migrationMetadataRead{}}
	unpublished := map[string]ThreadMeta{}
	seen, queue := map[string]bool{}, []string{threadID}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		if _, err := candidateReader.targetMetadata(backendDomainKey(backendDomainRecordThread, id)); err == nil {
			continue
		} else if !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		if _, err := candidateReader.targetMetadata(backendDomainKey(backendDomainRecordTombstone, id)); err == nil {
			continue
		} else if !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		meta, err := readPublishedThreadMeta(candidateReader, target, id)
		if err != nil {
			return err
		}
		checkpoint, found, err := readThreadMigrationCheckpoint(reader, id)
		if err != nil || !found || !checkpoint.Complete {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if meta.ParentThreadID != "" {
			queue = append(queue, meta.ParentThreadID)
		}
		count, err := visitCanonicalJournal(ctx, candidateReader, id, func(entry Entry) error {
			locator, err := readEntryLocator(candidateReader, id, entry.ID)
			if err != nil {
				return err
			}
			key := backendDomainEntryKey(id, locator.Ordinal)
			original, found, err := readBackendDomainRecord(reader, source, key, backendDomainRecordEntry, entry.ID, id, locator.Ordinal)
			if err != nil || !found {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			staged, found, err := readBackendDomainRecord(reader, target, key, backendDomainRecordEntry, entry.ID, id, locator.Ordinal)
			if err != nil || !found {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			originalEntry, err := migrationStoredEntry(source, original)
			if err != nil {
				return err
			}
			stagedEntry, err := decodeCanonicalStoredEntry(target, staged)
			if err != nil {
				return err
			}
			if err := compareMigrationStoredEntry(source, originalEntry, stagedEntry); err != nil {
				return err
			}
			if entry.Type == EntryParentEvent {
				event, err := decodeParentEvent(entry)
				if err != nil {
					return err
				}
				queue = append(queue, event.SourceThreadID)
			}
			if result := entry.Message.ToolResult; result != nil && result.FullOutput != nil {
				return compareMigrationSideRecord(reader, source, target, backendDomainRecordArtifact, artifactRecordKey(id, result.FullOutput.ID))
			}
			return nil
		})
		if err != nil || count != checkpoint.NextOrdinal {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		// Source and candidate must have exactly the same journal extent.
		prefix := backendDomainKey(backendDomainRecordEntry, id)
		page, err := reader.Scan(spi.ScanRequest{Namespace: source.namespace, Start: backendDomainEntryKey(id, count), End: backendKeyRangeEnd(prefix), Limit: 1})
		if err != nil || len(page.Records) != 0 {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		for _, kind := range []string{backendDomainRecordTodo, backendDomainRecordProviderState} {
			if err := compareMigrationSideRecord(reader, source, target, kind, id); err != nil {
				return err
			}
		}
		unpublished[id] = meta
	}
	// Every unpublished record is byte-identical to its immutable source. The
	// shared current validator checks their full invariant against already
	// published dependencies, without rebuilding either repository in memory.
	if _, err := validateCanonicalThreadClosure(ctx, candidateReader, threadID); err != nil {
		if errors.Is(err, ErrThreadDeleted) {
			return nil
		}
		return candidateReader.validationError(err)
	}
	sourceRecord, found, err := readBackendDomainRecord(reader, source, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
	if err != nil || !found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	var sourceManifest backendDomainManifest
	if err := decodeBackendDomainValue(source, sourceRecord, &sourceManifest); err != nil || sourceManifest.Version != source.version || sourceManifest.Sequence < 0 {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	err = repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		manifest, err := readV32Manifest(tx)
		if err != nil {
			return err
		}
		if manifest.SourceVersion != repo.migrationSource.version {
			return spi.ErrConflict
		}
		if err := candidateReader.verifyMetadata(tx); err != nil {
			return err
		}
		for id, meta := range unpublished {
			if meta.ParentThreadID == "" {
				if err := putBackendDomainRecordContext(ctx, tx, target, backendDomainKey(backendDomainRecordRootThread, id), backendDomainRecordRootThread, id, "", 0, backendDomainRootThread{ThreadID: id}); err != nil {
					return err
				}
			}
			if err := putBackendDomainRecordContext(ctx, tx, target, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, meta); err != nil {
				return err
			}
			if err := clearThreadMigrationFailure(tx, id); err != nil {
				return err
			}
			if err := tx.Delete(backendThreadMigrationNamespace, []byte(id)); err != nil {
				return err
			}
		}
		if historical, ok := reader.(historicalCandidateReader); ok && repo.parentEventsEnabled() {
			for _, event := range historical.events {
				if _, publishing := unpublished[event.SourceThreadID]; !publishing {
					continue
				}
				if _, err := appendCanonicalParentEvent(ctx, tx, event); err != nil {
					return err
				}
			}
		}
		manifest.Sequence = max(manifest.Sequence, sourceManifest.Sequence)
		return putBackendDomainRecordContext(ctx, tx, target, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
	})
	if err == nil {
		for id, meta := range unpublished {
			repo.rememberVerifiedThread(id, meta.LeafID)
		}
	}
	return err
}

func compareMigrationSideRecord(reader spi.ReadTx, source, target backendDomainFormat, kind, id string) error {
	key := backendDomainKey(kind, id)
	original, present, err := readBackendDomainRecord(reader, source, key, kind, id, "", 0)
	if err != nil {
		return err
	}
	staged, found, err := readBackendDomainRecord(reader, target, key, kind, id, "", 0)
	if err != nil || found != present || present && !bytes.Equal(original.Value, staged.Value) {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}
