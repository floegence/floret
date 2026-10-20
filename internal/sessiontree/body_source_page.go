package sessiontree

import (
	"context"
	"sort"

	"github.com/floegence/floret/v7/storage/spi"
)

// SourceBodyPageOptions pages source locators, never reconstructed text. LeafID
// freezes the source at a canonical boundary. AfterEntryID is scan progress and
// may name an inactive sibling; it grants no access to that sibling's body.
type SourceBodyPageOptions struct {
	ThreadID, LeafID, SourceID, Field, AfterEntryID string
	Limit                                           int
}

type SourceBodyPage struct {
	LeafID      string
	Bodies      []CanonicalBodyMetadata
	NextEntryID string
	HasMore     bool
}

type SourceBodyPageReader interface {
	SourceBodyPage(context.Context, SourceBodyPageOptions) (SourceBodyPage, error)
}

func (r *MemoryRepo) SourceBodyPage(ctx context.Context, options SourceBodyPageOptions) (SourceBodyPage, error) {
	if ctx == nil || options.SourceID == "" || options.Limit < 1 || options.Limit > 150 || options.Field != "content" && options.Field != "reasoning" || options.AfterEntryID != "" && options.LeafID == "" {
		return SourceBodyPage{}, spi.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SourceBodyPage{}, err
	}
	meta, found := r.threads[options.ThreadID]
	if !found {
		return SourceBodyPage{}, ErrThreadNotFound
	}
	leafID := options.LeafID
	if leafID == "" {
		leafID = meta.LeafID
	}
	page := SourceBodyPage{LeafID: leafID, Bodies: []CanonicalBodyMetadata{}}
	if leafID == "" {
		if options.AfterEntryID != "" {
			return SourceBodyPage{}, ErrEntryNotFound
		}
		return page, nil
	}
	if _, err := r.activeBodyEntryLocked(options.ThreadID, leafID); err != nil {
		return SourceBodyPage{}, err
	}
	r.ensureOutputSourcesLocked(options.ThreadID)
	locators := r.outputSources[options.ThreadID][options.SourceID]
	position := 0
	if options.AfterEntryID != "" {
		ordinal, exists := r.entryOrdinals[options.ThreadID][options.AfterEntryID]
		if !exists {
			return SourceBodyPage{}, ErrEntryNotFound
		}
		position = sort.SearchInts(locators, ordinal)
		if position == len(locators) || locators[position] != ordinal {
			return SourceBodyPage{}, ErrEntryNotFound
		}
		position++
	}
	index := r.pathIndexes[options.ThreadID]
	leaf := r.entryOrdinals[options.ThreadID][leafID]
	// Bound scanning as well as returned rows when many inactive siblings share
	// a source. Empty pages still advance an explicit cursor.
	for scanned := 0; position < len(locators) && scanned < 256 && len(page.Bodies) < options.Limit; scanned++ {
		if err := ctx.Err(); err != nil {
			return SourceBodyPage{}, err
		}
		ordinal := locators[position]
		if ordinal > leaf {
			break
		}
		position++
		if ordinal < 0 || ordinal >= len(index) {
			return SourceBodyPage{}, ErrAuthorityCorrupt
		}
		entry := r.entries[options.ThreadID][ordinal]
		if entry.ID != index[ordinal].id || entry.ThreadID != options.ThreadID {
			return SourceBodyPage{}, ErrAuthorityCorrupt
		}
		page.NextEntryID = entry.ID
		if pathAncestor(index, leaf, index[ordinal].depth) != ordinal {
			continue
		}
		source := entry.Message.ContentSourceID
		if options.Field == "reasoning" {
			source = entry.Message.ReasoningSourceID
		}
		if source != options.SourceID {
			continue
		}
		field := options.Field
		if field == "content" && entry.Message.ControlSignal != nil {
			field = "control_output"
		}
		ref, stored := entry.bodyFields[field]
		if !stored {
			value := canonicalEntryBodyFields(&entry)[field]
			if value == nil {
				return SourceBodyPage{}, ErrAuthorityCorrupt
			}
			ref = canonicalBodyRef{SHA256: stableHash(*value), Bytes: len(*value)}
		}
		if ref.Bytes == 0 && field != "control_output" {
			return SourceBodyPage{}, ErrAuthorityCorrupt
		}
		page.Bodies = append(page.Bodies, CanonicalBodyMetadata{EntryID: entry.ID, Field: field, SHA256: ref.SHA256, Bytes: ref.Bytes})
		// Reasoning source occurrences repeat identical bytes; its manifest has
		// exactly one source field, whereas assistant content concatenates all.
		if options.Field == "reasoning" {
			page.NextEntryID = ""
			return page, nil
		}
	}
	page.HasMore = position < len(locators) && locators[position] <= leaf
	if !page.HasMore {
		page.NextEntryID = ""
	}
	return page, nil
}
