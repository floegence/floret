package sessiontree

import (
	"context"
	"maps"

	"github.com/floegence/floret/v7/storage/spi"
)

// CanonicalField identifies an immutable field without retaining its body. This
// is a source locator, not a second transcript or an access grant.
type CanonicalField struct {
	Field  string
	SHA256 string
	Bytes  int
}

// PathMetadata is deliberately distinct from a complete canonical Entry. Body
// fields and Raw are absent; consumers must not use it as provider context.
type PathMetadata struct {
	Entry  Entry
	Bodies []CanonicalField
}

type PathMetadataPage struct {
	Entries       []PathMetadata
	NextEntryID   string
	HasMore       bool
	NewestOrdinal int64
}

type PathMetadataReader interface {
	PathMetadataPage(context.Context, string, string, string, int) (PathMetadataPage, error)
}

// PathMetadataPage shares the exact validated ancestry and cursor kernel with
// PathPage, but performs no body I/O. Published complete reads keep their shape.
func (r *MemoryRepo) PathMetadataPage(ctx context.Context, threadID, leafID, beforeEntryID string, limit int) (PathMetadataPage, error) {
	if ctx == nil || limit <= 0 || limit > 256 {
		return PathMetadataPage{}, spi.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := PathMetadataPage{Entries: []PathMetadata{}}
	page, err := r.visitPathPageLocked(ctx, threadID, leafID, beforeEntryID, limit, func(entry Entry) error {
		metadata, err := canonicalPathMetadata(entry)
		if err != nil {
			return err
		}

		result.Entries = append(result.Entries, metadata)
		return nil
	})
	if err != nil {
		return PathMetadataPage{}, err
	}
	result.NextEntryID, result.HasMore, result.NewestOrdinal = page.NextEntryID, page.HasMore, page.NewestOrdinal
	return result, nil
}

// canonicalPathMetadata is the sole body-free entry detachment path.
func canonicalPathMetadata(entry Entry) (PathMetadata, error) {
	fields := maps.Clone(entry.bodyFields)
	if fields == nil {
		if err := ValidateEntryIntegrity(entry); err != nil {
			return PathMetadata{}, err
		}
		fields = map[string]canonicalBodyRef{}
		values, err := canonicalStoredFields(entry, true)
		if err != nil {
			return PathMetadata{}, err
		}
		for field, value := range values {
			fields[field] = canonicalBodyRef{SHA256: stableHash(value), Bytes: len(value)}
		}
	}
	for field := range fields {
		if err := clearCanonicalField(&entry, field); err != nil {
			return PathMetadata{}, err
		}
	}

	entry.Raw = ""
	entry.bodyFields = nil
	metadata := PathMetadata{Entry: cloneEntry(entry)}
	// Deterministic field order makes rebuilt metadata stable.
	for _, field := range []string{"content", "reasoning", "arguments", "summary", "error", "control_output", "activity_json"} {
		if ref, ok := fields[field]; ok {
			metadata.Bodies = append(metadata.Bodies, CanonicalField{Field: field, SHA256: ref.SHA256, Bytes: ref.Bytes})
		}
	}
	return metadata, nil
}
