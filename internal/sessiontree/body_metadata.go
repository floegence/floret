package sessiontree

import (
	"context"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

// CanonicalBodyMetadata resolves one field on the current active path without
// reading its source bytes. A locator is neither a snapshot nor authorization.
type CanonicalBodyMetadata struct {
	EntryID string
	Field   string
	SHA256  string
	Bytes   int
}

type CanonicalBodyMetadataReader interface {
	CanonicalBodyMetadata(context.Context, string, string, string) (CanonicalBodyMetadata, error)
}

func (r *MemoryRepo) CanonicalBodyMetadata(ctx context.Context, threadID, entryID, field string) (CanonicalBodyMetadata, error) {
	if ctx == nil {
		return CanonicalBodyMetadata{}, spi.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return CanonicalBodyMetadata{}, err
	}
	entry, err := r.activeBodyEntryLocked(threadID, entryID)
	if err != nil {
		return CanonicalBodyMetadata{}, err
	}
	value, valid := canonicalEntryBodyFields(&entry)[field]
	if !valid {
		return CanonicalBodyMetadata{}, spi.ErrInvalidArgument
	}
	ref, stored := entry.bodyFields[field]
	if !stored {
		ref = canonicalBodyRef{SHA256: stableHash(*value), Bytes: len(*value)}
	}
	return CanonicalBodyMetadata{EntryID: strings.Clone(entryID), Field: field, SHA256: ref.SHA256, Bytes: ref.Bytes}, nil
}

func (r *MemoryRepo) activeBodyEntryLocked(threadID, entryID string) (Entry, error) {
	meta, found := r.threads[threadID]
	if !found {
		return Entry{}, ErrThreadNotFound
	}
	ordinal, found := r.entryOrdinals[threadID][entryID]
	if !found {
		return Entry{}, ErrEntryNotFound
	}
	r.ensurePathIndexLocked(threadID)
	index := r.pathIndexes[threadID]
	leaf, found := r.entryOrdinals[threadID][meta.LeafID]
	if !found || ordinal < 0 || ordinal >= len(index) || leaf < 0 || leaf >= len(index) {
		return Entry{}, ErrAuthorityCorrupt
	}
	if index[ordinal].id != entryID || index[leaf].id != meta.LeafID {
		return Entry{}, ErrAuthorityCorrupt
	}
	if index[ordinal].err != nil {
		return Entry{}, index[ordinal].err
	}
	if index[leaf].err != nil {
		return Entry{}, index[leaf].err
	}
	if pathAncestor(index, leaf, index[ordinal].depth) != ordinal {
		return Entry{}, ErrEntryNotFound
	}
	return r.entries[threadID][ordinal], nil
}
