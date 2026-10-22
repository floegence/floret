package sessiontree

import (
	"context"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) Thread(ctx context.Context, threadID string) (ThreadMeta, error) {
	var result ThreadMeta
	err := repo.canonicalThreadView(ctx, threadID, func(_ spi.ReadTx, meta ThreadMeta) error {
		result = meta
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.Thread(ctx, threadID)
		return err
	})
	return result, err
}

func (repo *BackendRepo) Entry(ctx context.Context, threadID, entryID string) (Entry, error) {
	var result Entry
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		locator, err := readEntryLocator(tx, meta.ID, entryID)
		if err != nil {
			return err
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, locator)
		if err != nil {
			return err
		}
		fields := entry.bodyFields
		entry.bodyFields = nil
		result, err = materializeCanonicalStoredEntry(ctx, tx, canonicalStoredEntry{Entry: entry, Fields: fields})
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.Entry(ctx, threadID, entryID)
		return err
	})
	return result, err
}

// Canonical pages resolve only their requested source records after the
// thread's preparation gate. Locator reads never hydrate a complete journal.
func (repo *BackendRepo) canonicalThreadView(ctx context.Context, threadID string, direct func(spi.ReadTx, ThreadMeta) error, memoryRead func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.canonicalThreadView(ctx, threadID, direct, memoryRead)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return repo.viewThread(ctx, threadID, memoryRead)
	}
	if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil {
		return err
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0)
		if err != nil {
			return err
		}
		if !found {
			if _, deleted, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTombstone, threadID), backendDomainRecordTombstone, threadID, "", 0); err != nil {
				return err
			} else if deleted {
				return ErrThreadDeleted
			}
			return ErrThreadNotFound
		}
		var meta ThreadMeta
		if err := decodeBackendDomainValue(backendDomainV32Format, record, &meta); err != nil || meta.ID != threadID || ValidateThreadMetaAuthority(meta) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		// Publication and canonical writes validate the journal invariant. Reads
		// verify the requested immutable records, never all historical bodies.
		count, err := readJournalCount(tx, threadID)
		if err != nil || (count == 0) != (meta.LeafID == "") {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if meta.LeafID != "" {
			leaf, err := readEntryLocator(tx, threadID, meta.LeafID)
			if err != nil || leaf.Ordinal >= count {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if _, err := readLocatedEntry(tx, backendDomainV32Format, threadID, leaf); err != nil {
				return err
			}
		}
		return direct(tx, meta)
	})
}

func (repo *BackendRepo) ReadAgentTodoState(ctx context.Context, threadID string) (AgentTodoState, error) {
	result := AgentTodoState{ThreadID: threadID}
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTodo, meta.ID), backendDomainRecordTodo, meta.ID, "", 0)
		if err != nil || !found {
			return err
		}
		if err := decodeBackendDomainValue(backendDomainV32Format, record, &result); err != nil || result.ThreadID != meta.ID || ValidateAgentTodoItems(result.Items) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadAgentTodoState(ctx, threadID)
		return err
	})
	return result, err
}

func (repo *BackendRepo) ProviderState(ctx context.Context, threadID string) (ProviderStateRecord, error) {
	var result ProviderStateRecord
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordProviderState, meta.ID), backendDomainRecordProviderState, meta.ID, "", 0)
		if err != nil {
			return err
		}
		if !found {
			return ErrProviderStateNotFound
		}
		if err := decodeBackendDomainValue(backendDomainV32Format, record, &result); err != nil || result.ThreadID != meta.ID || validateProviderStateRecord(result) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ProviderState(ctx, threadID)
		return err
	})
	return result, err
}

func visitCanonicalPage(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leafID, beforeEntryID string, limit int, visit func(Entry) error) (PathPage, error) {
	if leafID == "" {
		leafID = meta.LeafID
	}
	page := PathPage{Entries: []Entry{}}
	if leafID == "" {
		if beforeEntryID != "" {
			return page, ErrEntryNotFound
		}
		return page, nil
	}
	node, err := readEntryLocator(tx, meta.ID, leafID)
	if err != nil {
		return page, err
	}
	if beforeEntryID != "" {
		before, err := readEntryLocator(tx, meta.ID, beforeEntryID)
		if err != nil {
			return page, err
		}
		ancestor, err := entryAncestor(ctx, tx, meta.ID, node, before.Depth)
		if err != nil {
			return page, err
		}
		if ancestor.ID != before.ID {
			return page, ErrEntryNotFound
		}
		if before.Parent == "" {
			return page, nil
		}
		node, err = readEntryLocator(tx, meta.ID, before.Parent)
		if err != nil {
			return page, err
		}
	}
	page.NewestOrdinal = node.Depth
	for count := 0; count < limit; count++ {
		if err := ctx.Err(); err != nil {
			return PathPage{}, err
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, node)
		if err != nil {
			return PathPage{}, err
		}
		if err := visit(entry); err != nil {
			return PathPage{}, err
		}
		if node.Parent == "" {
			return page, nil
		}
		if count+1 == limit {
			page.HasMore, page.NextEntryID = true, node.ID
			return page, nil
		}
		parent, err := readEntryLocator(tx, meta.ID, node.Parent)
		if err != nil || parent.Depth+1 != node.Depth || parent.Ordinal >= node.Ordinal {
			return PathPage{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		node = parent
	}
	return page, nil
}

func (repo *BackendRepo) PathMetadataPage(ctx context.Context, threadID, leafID, beforeEntryID string, limit int) (PathMetadataPage, error) {
	if ctx == nil || limit < 1 || limit > 256 {
		return PathMetadataPage{}, spi.ErrInvalidArgument
	}
	result := PathMetadataPage{Entries: []PathMetadata{}}
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		page, err := visitCanonicalPage(ctx, tx, meta, leafID, beforeEntryID, limit, func(entry Entry) error {
			metadata, err := canonicalPathMetadata(entry)
			if err == nil {
				result.Entries = append(result.Entries, metadata)
			}
			return err
		})
		result.NextEntryID, result.HasMore, result.NewestOrdinal = page.NextEntryID, page.HasMore, page.NewestOrdinal
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.PathMetadataPage(ctx, threadID, leafID, beforeEntryID, limit)
		return err
	})
	return result, err
}

func (repo *BackendRepo) PathPage(ctx context.Context, threadID, leafID, beforeEntryID string, limit int) (PathPage, error) {
	if ctx == nil || limit < 1 {
		return PathPage{}, spi.ErrInvalidArgument
	}
	result := PathPage{Entries: []Entry{}}
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		entries := []Entry{}
		page, err := visitCanonicalPage(ctx, tx, meta, leafID, beforeEntryID, limit, func(entry Entry) error {
			complete, err := completeLocatedEntry(ctx, tx, entry)
			if err == nil {
				entries = append(entries, cloneEntry(complete))
			}
			return err
		})
		result = page
		result.Entries = entries
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.PathPage(ctx, threadID, leafID, beforeEntryID, limit)
		return err
	})
	return result, err
}

func (repo *BackendRepo) JournalMetadata(ctx context.Context, options JournalMetadataOptions) (JournalMetadataPage, error) {
	if ctx == nil || options.After < 0 || options.Through < 0 || options.Limit < 1 || options.Limit > 256 {
		return JournalMetadataPage{}, spi.ErrInvalidArgument
	}
	var result JournalMetadataPage
	err := repo.canonicalThreadView(ctx, options.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		count, err := readJournalCount(tx, meta.ID)
		if err != nil {
			return err
		}
		through := options.Through
		if through == 0 {
			through = count
		}
		if through > count || options.After > through {
			return ErrEntryNotFound
		}
		end := min(through, options.After+options.Limit)
		result = JournalMetadataPage{Entries: []PathMetadata{}, After: end, Through: through, HasMore: end < through}
		for ordinal := options.After; ordinal < end; ordinal++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			raw, err := tx.Get(backendDomainV32Namespace, backendDomainEntryKey(meta.ID, ordinal))
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			record, err := decodeBackendDomainRecord(backendDomainV32Format, raw)
			if err != nil || record.Ordinal != ordinal || record.ThreadID != meta.ID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			stored, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
			if err != nil {
				return err
			}
			entry := stored.Entry
			entry.bodyFields = stored.Fields
			metadata, err := canonicalPathMetadata(entry)
			if err != nil {
				return err
			}
			result.Entries = append(result.Entries, metadata)
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.JournalMetadata(ctx, options)
		return err
	})
	return result, err
}

func (repo *BackendRepo) PathMetadataWindow(ctx context.Context, options PathWindowOptions) (PathMetadataWindow, error) {
	return repo.canonicalPathWindow(ctx, options, false)
}

func (repo *BackendRepo) PathEntriesWindow(ctx context.Context, options PathWindowOptions) (PathEntriesWindow, error) {
	page, err := repo.canonicalPathWindow(ctx, options, true)
	if err != nil {
		return PathEntriesWindow{}, err
	}
	entries := make([]Entry, len(page.Entries))
	for i, entry := range page.Entries {
		entries[i] = entry.Entry
	}
	page.Entries = nil
	return PathEntriesWindow{PathMetadataWindow: page, Entries: entries}, nil
}

func (repo *BackendRepo) canonicalPathWindow(ctx context.Context, options PathWindowOptions, complete bool) (PathMetadataWindow, error) {
	if ctx == nil || options.Limit < 1 || options.Limit > 256 || options.Direction != PathWindowBefore && options.Direction != PathWindowAfter && options.Direction != PathWindowAround {
		return PathMetadataWindow{}, spi.ErrInvalidArgument
	}
	var result PathMetadataWindow
	err := repo.canonicalThreadView(ctx, options.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		leafID := options.LeafID
		if leafID == "" {
			leafID = meta.LeafID
		}
		result = PathMetadataWindow{LeafID: leafID, Entries: []PathMetadata{}}
		if leafID == "" {
			if options.AnchorID != "" || options.Direction == PathWindowAround {
				return ErrEntryNotFound
			}
			return nil
		}
		leaf, err := readEntryLocator(tx, meta.ID, leafID)
		if err != nil {
			return err
		}
		active, err := readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return err
		}
		ancestor, err := entryAncestor(ctx, tx, meta.ID, active, leaf.Depth)
		if err != nil {
			return err
		}
		if ancestor.ID != leafID {
			return ErrEntryNotFound
		}
		result.ThroughOrdinal = leaf.Depth
		var anchor int64
		if options.AnchorID != "" {
			node, err := readEntryLocator(tx, meta.ID, options.AnchorID)
			if err != nil {
				return err
			}
			ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, node.Depth)
			if err != nil {
				return err
			}
			if ancestor.ID != node.ID {
				return ErrEntryNotFound
			}
			anchor = node.Depth
		} else if options.Direction == PathWindowAround {
			return spi.ErrInvalidArgument
		}
		limit := int64(options.Limit)
		var first, last int64
		switch options.Direction {
		case PathWindowBefore:
			last = leaf.Depth
			if anchor > 0 {
				last = anchor - 1
			}
			first = max(int64(1), last-limit+1)
		case PathWindowAfter:
			first = anchor + 1
			last = min(leaf.Depth, first+limit-1)
		case PathWindowAround:
			first = max(int64(1), anchor-limit/2)
			last = min(leaf.Depth, first+limit-1)
			first = max(int64(1), last-limit+1)
		}
		result.HasBefore, result.HasAfter = first > 1, last < leaf.Depth
		if first > last {
			return nil
		}
		node, err := entryAncestor(ctx, tx, meta.ID, leaf, last)
		if err != nil {
			return err
		}
		_, err = visitCanonicalPage(ctx, tx, meta, node.ID, "", int(last-first+1), func(entry Entry) error {
			var metadata PathMetadata
			var err error
			if complete {
				metadata.Entry, err = completeLocatedEntry(ctx, tx, entry)
			} else {
				metadata, err = canonicalPathMetadata(entry)
			}
			if err == nil {
				result.Entries = append(result.Entries, metadata)
			}
			return err
		})
		if err != nil {
			return err
		}
		slices.Reverse(result.Entries)
		result.FirstOrdinal, result.LastOrdinal = first, last
		result.FirstEntryID, result.LastEntryID = result.Entries[0].Entry.ID, result.Entries[len(result.Entries)-1].Entry.ID
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.readPathWindow(ctx, options, complete)
		return err
	})
	return result, err
}

func completeLocatedEntry(ctx context.Context, tx spi.ReadTx, entry Entry) (Entry, error) {
	fields := entry.bodyFields
	entry.bodyFields = nil
	return materializeCanonicalStoredEntry(ctx, tx, canonicalStoredEntry{Entry: entry, Fields: fields})
}

func (repo *BackendRepo) Path(ctx context.Context, threadID, leafID string) ([]Entry, error) {
	var result []Entry
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		_, err := visitCanonicalPage(ctx, tx, meta, leafID, "", int(^uint(0)>>1), func(entry Entry) error {
			complete, err := completeLocatedEntry(ctx, tx, entry)
			if err == nil {
				result = append(result, complete)
			}
			return err
		})
		slices.Reverse(result)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.Path(ctx, threadID, leafID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func activeLocatedEntry(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, entryID string) (Entry, error) {
	node, err := readEntryLocator(tx, meta.ID, entryID)
	if err != nil {
		return Entry{}, err
	}
	leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
	if err != nil {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, node.Depth)
	if err != nil {
		return Entry{}, err
	}
	if ancestor.ID != node.ID {
		return Entry{}, ErrEntryNotFound
	}
	return readLocatedEntry(tx, backendDomainV32Format, meta.ID, node)
}

func (repo *BackendRepo) CanonicalBodyMetadata(ctx context.Context, threadID, entryID, field string) (CanonicalBodyMetadata, error) {
	var result CanonicalBodyMetadata
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		entry, err := activeLocatedEntry(ctx, tx, meta, entryID)
		if err != nil {
			return err
		}
		value, err := canonicalFieldValue(entry, field)
		if err != nil {
			return err
		}
		ref, stored := entry.bodyFields[field]
		if !stored {
			ref = canonicalBodyRef{SHA256: stableHash(value), Bytes: len(value)}
		}
		result = CanonicalBodyMetadata{EntryID: strings.Clone(entryID), Field: field, SHA256: ref.SHA256, Bytes: ref.Bytes}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.CanonicalBodyMetadata(ctx, threadID, entryID, field)
		return err
	})
	return result, err
}

func (repo *BackendRepo) ReadCanonicalBody(ctx context.Context, threadID, entryID, field string, offset, limit int) (CanonicalBodyChunk, error) {
	if ctx == nil || offset < 0 || limit < utf8.UTFMax || limit > canonicalBodyBlockBytes {
		return CanonicalBodyChunk{}, spi.ErrInvalidArgument
	}
	var result CanonicalBodyChunk
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		entry, err := activeLocatedEntry(ctx, tx, meta, entryID)
		if err != nil {
			return err
		}
		result, err = readCanonicalEntryBody(ctx, entry, field, offset, limit, func(read func(spi.ReadTx) error) error { return read(tx) })
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadCanonicalBody(ctx, threadID, entryID, field, offset, limit)
		return err
	})
	if err != nil {
		return CanonicalBodyChunk{}, err
	}
	return result, nil
}
