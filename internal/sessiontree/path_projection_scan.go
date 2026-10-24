package sessiontree

import (
	"context"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// PathProjectionEntry is explicitly partial; it never substitutes for complete
// canonical Entries or provider history. Sources retain exact body locators.
type PathProjectionEntry struct {
	GoalBefore *session.GoalState
	Entry      Entry
	Sources    []CanonicalField
}

type PathProjectionPage struct {
	PathMetadataWindow
	Entries []PathProjectionEntry
}

func projectionPage(page PathMetadataWindow) PathProjectionPage {
	entries := make([]PathProjectionEntry, len(page.Entries))
	for i, source := range page.Entries {
		entries[i] = PathProjectionEntry{Entry: source.Entry, Sources: source.Bodies}
	}
	page.Entries = nil
	return PathProjectionPage{PathMetadataWindow: page, Entries: entries}
}

// PathProjectionScanner owns a single frozen metadata scan and its selected body
// reads. Preparation receives detached metadata, never executable history. The
// scan temporarily retains metadata proportional to the path, but body residency
// is page-local. It retains no state after return and publishes nothing durably.
type PathProjectionScanner interface {
	ScanPathProjection(context.Context, string, string, func([]PathMetadata) error, func(PathMetadata) []string, func(PathProjectionPage) error) error
}

func scanPathProjection(ctx context.Context, reader PathWindowReader, threadID, leafID string, restore func([]PathMetadataWindow) error, prepare func([]PathMetadata) error, materialize func(PathMetadataWindow) (PathProjectionPage, error), visit func(PathProjectionPage) error) error {
	if ctx == nil || strings.TrimSpace(threadID) == "" || prepare == nil || visit == nil {
		return spi.ErrInvalidArgument
	}
	options := PathWindowOptions{ThreadID: threadID, LeafID: leafID, Direction: PathWindowAfter, Limit: 256}
	var pages []PathMetadataWindow
	for {
		page, err := reader.PathMetadataWindow(ctx, options)
		if err != nil {
			return err
		}
		pages = append(pages, page)
		if !page.HasAfter {
			break
		}
		if len(page.Entries) == 0 || page.LastEntryID == options.AnchorID {
			return ErrAuthorityCorrupt
		}
		options.LeafID, options.AnchorID = page.LeafID, page.LastEntryID
	}
	if restore != nil {
		if err := restore(pages); err != nil {
			return err
		}
	}
	selected := make([]PathMetadata, 0, pages[0].ThroughOrdinal)
	for _, page := range pages {
		for _, source := range page.Entries {
			// Selection cannot mutate the scan's validated immutable sources.
			selected = append(selected, PathMetadata{Entry: cloneEntry(source.Entry), Bodies: append([]CanonicalField(nil), source.Bodies...)})
		}
	}
	if err := prepare(selected); err != nil {
		return err
	}
	selected = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, page := range pages {
		if err := ctx.Err(); err != nil {
			return err
		}
		projected, err := materialize(page)
		if err != nil {
			return err
		}
		if err := visit(projected); err != nil {
			return err
		}
		pages[i].Entries = nil
	}
	// The last callback can cancel, delete, or move the branch too. A completed
	// traversal must not certify a snapshot invalidated during that callback.
	_, err := reader.PathMetadataWindow(ctx, PathWindowOptions{ThreadID: threadID, LeafID: pages[len(pages)-1].LeafID, Direction: PathWindowBefore, Limit: 1})
	return err
}

func (repo *BackendRepo) ScanPathProjection(ctx context.Context, threadID, leafID string, prepare func([]PathMetadata) error, fields func(PathMetadata) []string, visit func(PathProjectionPage) error) error {
	if repo == nil || ctx == nil || fields == nil {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ScanPathProjection(ctx, threadID, leafID, prepare, fields, visit)
	}
	release, err := repo.migrationGate.foregroundRead(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := repo.prepareContextSourceMetadata(ctx, threadID); err != nil {
		return err
	}
	return scanPathProjection(ctx, contextProjectionWindowReader{repo}, threadID, leafID, func(pages []PathMetadataWindow) error {
		return repo.restoreSelectedContextStatuses(ctx, threadID, pages)
	}, prepare, func(page PathMetadataWindow) (PathProjectionPage, error) {
		var result PathProjectionPage
		err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
			if page.LeafID != "" {
				leaf, err := readEntryLocator(tx, threadID, page.LeafID)
				if err != nil {
					return err
				}
				active, err := readEntryLocator(tx, threadID, meta.LeafID)
				if err != nil {
					return err
				}
				ancestor, err := entryAncestor(ctx, tx, threadID, active, leaf.Depth)
				if err != nil {
					return err
				}
				if ancestor.ID != leaf.ID {
					return ErrEntryNotFound
				}
			}
			// Sources were decoded and checked by this scan. Canonical records are
			// immutable; only ancestry and requested body authority need new I/O.
			bodies := newCanonicalBodyPageReader(ctx, tx)
			var err error
			result, err = materializeProjectionPage(page, fields, bodies.prefetch, func(_ string, source CanonicalField) (string, error) {
				return bodies.read(canonicalBodyRef{SHA256: source.SHA256, Bytes: source.Bytes})
			})
			return err
		}, func(memory *MemoryRepo) error {
			var err error
			result, err = memory.materializeProjectionPage(ctx, threadID, page, fields)
			return err
		})
		return result, err
	}, visit)
}

func (repo *MemoryRepo) ScanPathProjection(ctx context.Context, threadID, leafID string, prepare func([]PathMetadata) error, fields func(PathMetadata) []string, visit func(PathProjectionPage) error) error {
	if fields == nil {
		return spi.ErrInvalidArgument
	}
	return scanPathProjection(ctx, repo, threadID, leafID, nil, prepare, func(page PathMetadataWindow) (PathProjectionPage, error) {
		return repo.materializeProjectionPage(ctx, threadID, page, fields)
	}, visit)
}

func (repo *MemoryRepo) materializeProjectionPage(ctx context.Context, threadID string, page PathMetadataWindow, fields func(PathMetadata) []string) (PathProjectionPage, error) {
	if _, err := repo.PathMetadataWindow(ctx, PathWindowOptions{ThreadID: threadID, LeafID: page.LeafID, Direction: PathWindowBefore, Limit: 1}); err != nil {
		return PathProjectionPage{}, err
	}
	result, err := materializeProjectionPage(page, fields, nil, func(entryID string, source CanonicalField) (string, error) {
		var value strings.Builder
		for value.Len() < source.Bytes {
			part, err := repo.ReadCanonicalBody(ctx, threadID, entryID, source.Field, value.Len(), canonicalBodyBlockBytes)
			if err != nil {
				return "", err
			}
			if part.Offset != value.Len() || part.Bytes != source.Bytes || part.Text == "" || len(part.Text) > source.Bytes-value.Len() {
				return "", ErrAuthorityCorrupt
			}
			value.WriteString(part.Text)
		}
		if stableHash(value.String()) != source.SHA256 {
			return "", ErrAuthorityCorrupt
		}
		return value.String(), nil
	})
	if err != nil {
		return PathProjectionPage{}, err
	}
	if _, err := repo.PathMetadataWindow(ctx, PathWindowOptions{ThreadID: threadID, LeafID: page.LeafID, Direction: PathWindowBefore, Limit: 1}); err != nil {
		return PathProjectionPage{}, err
	}
	return result, nil
}

func materializeProjectionPage(page PathMetadataWindow, fields func(PathMetadata) []string, prefetch func([]canonicalBodyRef) error, read func(string, CanonicalField) (string, error)) (PathProjectionPage, error) {
	result := projectionPage(page)
	selections := make([][]string, len(page.Entries))
	var refs []canonicalBodyRef
	for i, metadata := range page.Entries {
		selections[i] = fields(PathMetadata{Entry: cloneEntry(metadata.Entry), Bodies: append([]CanonicalField(nil), metadata.Bodies...)})
		for _, field := range selections[i] {
			found := false
			for _, source := range metadata.Bodies {
				if source.Field == field {
					refs = append(refs, canonicalBodyRef{SHA256: source.SHA256, Bytes: source.Bytes})
					found = true
					break
				}
			}
			if !found {
				return PathProjectionPage{}, spi.ErrInvalidArgument
			}
		}
	}
	if prefetch != nil {
		if err := prefetch(refs); err != nil {
			return PathProjectionPage{}, err
		}
	}
	for i, metadata := range page.Entries {
		result.Entries[i].Entry = cloneEntry(metadata.Entry)
		result.Entries[i].Sources = append([]CanonicalField(nil), metadata.Bodies...)
		selection := selections[i]
		for _, field := range selection {
			var source *CanonicalField
			for j := range metadata.Bodies {
				if metadata.Bodies[j].Field == field {
					source = &metadata.Bodies[j]
					break
				}
			}
			if source == nil {
				return PathProjectionPage{}, spi.ErrInvalidArgument
			}
			value, err := read(metadata.Entry.ID, *source)
			if err != nil {
				return PathProjectionPage{}, err
			}
			if err := restoreCanonicalField(&result.Entries[i].Entry, field, value); err != nil {
				return PathProjectionPage{}, err
			}
		}
	}
	return result, nil
}
