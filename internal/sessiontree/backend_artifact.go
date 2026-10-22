package sessiontree

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

func canonicalArtifactReader(tx spi.ReadTx) artifactAuthorityReader {
	read := func(kind, id string, value any) (bool, error) {
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(kind, id), kind, id, "", 0)
		if err != nil || !found {
			return found, err
		}
		if err := decodeBackendDomainValue(backendDomainV32Format, record, value); err != nil {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		return true, nil
	}
	return artifactAuthorityReader{
		thread: func(id string) (ThreadMeta, bool, error) {
			var value ThreadMeta
			found, err := read(backendDomainRecordThread, id, &value)
			return value, found, err
		},
		tombstone: func(id string) (ThreadTombstone, bool, error) {
			var value ThreadTombstone
			found, err := read(backendDomainRecordTombstone, id, &value)
			return value, found, err
		},
		entry: func(threadID, entryID string) (Entry, bool, error) {
			entry, err := readCanonicalEntryMetadata(tx, threadID, entryID)
			if errors.Is(err, ErrEntryNotFound) {
				return Entry{}, false, nil
			}
			return entry, err == nil, err
		},
		record: func(threadID, artifactID string) (artifact.Record, bool, error) {
			var value artifact.Record
			found, err := read(backendDomainRecordArtifact, artifactRecordKey(threadID, artifactID), &value)
			return value, found, err
		},
	}
}

func (repo *BackendRepo) ReadArtifact(ctx context.Context, req ArtifactReadRequest) (ArtifactContent, error) {
	threadID, artifactID := strings.TrimSpace(req.ThreadID), strings.TrimSpace(req.ArtifactID)
	if threadID == "" || artifactID == "" {
		return ArtifactContent{}, errors.New("artifact read requires thread and artifact identities")
	}
	var result ArtifactContent
	err := repo.canonicalArtifactView(ctx, threadID, strings.TrimSpace(req.ParentThreadID), func(tx spi.ReadTx) error {
		reader := canonicalArtifactReader(tx)
		if err := reader.validateArtifactReadAuthorityLocked(req.ParentThreadID, threadID); err != nil {
			return err
		}
		record, found, err := reader.record(threadID, artifactID)
		if err != nil {
			return err
		}
		if !found {
			return ErrArtifactNotFound
		}
		if record.ThreadID != threadID || record.Ref.ID != artifactID {
			return ErrAuthorityCorrupt
		}
		if err := reader.validateArtifactRecordLocked(record); err != nil {
			return err
		}
		result = ArtifactContent{Ref: record.Ref, Text: record.Text}
		return nil
	}, func(memory *MemoryRepo) error { var err error; result, err = memory.ReadArtifact(ctx, req); return err })
	return result, err
}

// Authorization may intentionally distinguish missing parents, deleted children,
// and unrelated tombstones. Let the shared reader classify them in one snapshot.
func (repo *BackendRepo) canonicalArtifactView(ctx context.Context, threadID, parentID string, read func(spi.ReadTx) error, legacy func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.canonicalArtifactView(ctx, threadID, parentID, read, legacy)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return repo.view(ctx, legacy)
	}
	for _, id := range []string{parentID, threadID} {
		if id == "" {
			continue
		}
		if err := repo.prepareThreadIfNeeded(ctx, id); err != nil && !errors.Is(err, ErrThreadNotFound) && !errors.Is(err, ErrThreadDeleted) {
			return err
		}
	}
	return repo.backend.View(ctx, read)
}

func (repo *BackendRepo) ArtifactClosure(ctx context.Context, req ArtifactClosureRequest) (artifact.Closure, error) {
	sourceID, destinationID := strings.TrimSpace(req.SourceThreadID), strings.TrimSpace(req.DestinationThreadID)
	if sourceID == "" || destinationID == "" {
		return artifact.Closure{}, errors.New("artifact closure requires source and destination")
	}
	var result artifact.Closure
	err := repo.canonicalThreadView(ctx, sourceID, func(tx spi.ReadTx, meta ThreadMeta) error {
		if err := validateMemoryArtifactThread(meta, sourceID); err != nil {
			return err
		}
		entries := make([]Entry, 0, len(req.EntryIDs))
		for _, id := range req.EntryIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry, err := readCanonicalEntryMetadata(tx, sourceID, strings.TrimSpace(id))
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		var err error
		result, err = canonicalArtifactReader(tx).artifactClosureLocked(sourceID, destinationID, entries)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ArtifactClosure(ctx, req)
		return err
	})
	return result, err
}

func (repo *BackendRepo) ValidateArtifactForkDestination(ctx context.Context, closure artifact.Closure) error {
	if err := artifact.ValidateClosure(closure); err != nil {
		return ErrAuthorityCorrupt
	}
	return repo.canonicalArtifactView(ctx, closure.DestinationThreadID, "", func(tx spi.ReadTx) error {
		return canonicalArtifactReader(tx).validateArtifactForkDestinationLocked(closure)
	}, func(memory *MemoryRepo) error { return memory.ValidateArtifactForkDestination(ctx, closure) })
}
