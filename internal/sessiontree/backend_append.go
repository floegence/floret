package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) Append(ctx context.Context, entry Entry, opts AppendOptions) (Entry, error) {
	var result Entry
	err := repo.canonicalJournalUpdate(ctx, entry.ThreadID, func(mutation *canonicalJournalMutation) error {
		var err error
		result, err = mutation.append(entry, opts)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.Append(ctx, entry, opts)
		return err
	})
	if err != nil {
		return Entry{}, err
	}
	return cloneEntry(result), nil
}

func appendCanonicalEntry(ctx context.Context, tx spi.WriteTx, meta ThreadMeta, entry Entry, opts AppendOptions, settlement bool) (Entry, ThreadMeta, error) {
	fail := func(err error) (Entry, ThreadMeta, error) { return Entry{}, meta, err }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return fail(err)
	}
	if entry.ThreadID != meta.ID {
		return fail(ErrThreadNotFound)
	}
	turn, err := readTurnEntryLocator(tx, meta.ID, strings.TrimSpace(entry.TurnID))
	if err != nil {
		return fail(err)
	}
	if strings.TrimSpace(entry.TurnID) != "" && turn.Terminal != "" && !settlement {
		marker, err := readCanonicalEntryMetadata(tx, meta.ID, turn.Terminal)
		if err != nil || marker.Type != EntryTurnMarker || marker.TurnID != strings.TrimSpace(entry.TurnID) || !terminalTurnMarker(marker.TurnStatus) {
			return fail(errors.Join(ErrAuthorityCorrupt, err))
		}
		return fail(ErrStaleAuthority)
	}
	if err := ValidateNewEntryMessageAttachments(entry); err != nil {
		return fail(err)
	}
	if err := ValidateEntryMessageReferences(entry); err != nil {
		return fail(err)
	}
	if err := validateCanonicalEntryRunIdentity(entry); err != nil {
		return fail(err)
	}
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		runID := strings.TrimSpace(entry.RunID)
		if runID == "" {
			return fail(fmt.Errorf("%w: started turn requires run identity", ErrInvalidThreadAuthority))
		}
		if turn.Started != "" {
			marker, err := readCanonicalEntryMetadata(tx, meta.ID, turn.Started)
			if err != nil || marker.Type != EntryTurnMarker || marker.TurnStatus != TurnStarted || marker.TurnID != entry.TurnID {
				return fail(errors.Join(ErrAuthorityCorrupt, err))
			}
			return fail(ErrRequestConflict)
		}
		entry.Metadata = cloneStringMap(entry.Metadata)
		if entry.Metadata == nil {
			entry.Metadata = map[string]string{}
		}
		entry.Metadata["run_id"] = runID
	}
	if opts.ParentID != "" {
		entry.ParentID = opts.ParentID
	} else if entry.ParentID == "" {
		entry.ParentID = meta.LeafID
	}
	entry.PathDepth = 1
	if entry.ParentID != "" {
		parent, err := readEntryLocator(tx, meta.ID, entry.ParentID)
		if errors.Is(err, ErrEntryNotFound) {
			return fail(ErrInvalidParent)
		}
		if err != nil {
			return fail(err)
		}
		if _, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, parent); err != nil {
			return fail(err)
		}
		entry.PathDepth = parent.Depth + 1
	}
	if opts.ID != "" {
		entry.ID = opts.ID
	}
	if entry.ID == "" {
		manifest, err := readV32Manifest(tx)
		if err != nil {
			return fail(err)
		}
		for {
			if manifest.Sequence == math.MaxInt64 {
				return fail(ErrAuthorityCorrupt)
			}
			manifest.Sequence++
			entry.ID = fmt.Sprintf("%s-entry-%d", meta.ID, manifest.Sequence)
			_, err := readEntryLocator(tx, meta.ID, entry.ID)
			if errors.Is(err, ErrEntryNotFound) {
				break
			}
			if err != nil {
				return fail(err)
			}
		}
		if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
			return fail(err)
		}
	} else if _, err := readEntryLocator(tx, meta.ID, entry.ID); err == nil {
		return fail(fmt.Errorf("session tree entry id already exists: %s", entry.ID))
	} else if !errors.Is(err, ErrEntryNotFound) {
		return fail(err)
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = opts.Now
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	admitOutputSources(&entry)
	if err := validateCanonicalOutputSources(ctx, tx, entry); err != nil {
		return fail(err)
	}
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	count, err := readJournalCount(tx, meta.ID)
	if err != nil || (count == 0) != (meta.LeafID == "") {
		return fail(errors.Join(ErrAuthorityCorrupt, err))
	}
	if err := putCanonicalStoredEntry(ctx, tx, backendDomainV32Format, backendDomainEntryKey(meta.ID, count), entry.ID, meta.ID, count, entry); err != nil {
		return fail(err)
	}
	meta.LeafID, meta.UpdatedAt = entry.ID, entry.CreatedAt
	if err := validateCanonicalAppendedEntry(ctx, tx, meta, entry); err != nil {
		return fail(errors.Join(ErrAuthorityCorrupt, err))
	}
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, meta.ID), backendDomainRecordThread, meta.ID, "", 0, meta); err != nil {
		return fail(err)
	}
	return entry, meta, nil
}

func readCanonicalEntryMetadata(tx spi.ReadTx, threadID, entryID string) (Entry, error) {
	locator, err := readEntryLocator(tx, threadID, entryID)
	if err != nil {
		return Entry{}, err
	}
	return readLocatedEntry(tx, backendDomainV32Format, threadID, locator)
}

func validateCanonicalOutputSources(ctx context.Context, tx spi.ReadTx, entry Entry) error {
	facts, err := outputSourceFacts(entry)
	if err != nil || len(facts) == 0 || entry.ParentID == "" {
		return err
	}
	parent, err := readEntryLocator(tx, entry.ThreadID, entry.ParentID)
	if err != nil {
		return err
	}
	for source, fact := range facts {
		prefix := backendDomainKey("source", entry.ThreadID, source)
		var after []byte
		matched := false
		for !matched {
			if err := ctx.Err(); err != nil {
				return err
			}
			page, err := tx.Scan(spi.ScanRequest{Namespace: backendEntryIndexNamespace, Start: prefix, End: sourceLocatorKey(entry.ThreadID, source, parent.Ordinal+1), After: after, Limit: backendThreadEntryPageSize})
			if err != nil {
				return err
			}
			for _, row := range page.Records {
				locator, err := readEntryLocator(tx, entry.ThreadID, string(row.Value))
				if err != nil || !bytes.Equal(row.Key, sourceLocatorKey(entry.ThreadID, source, locator.Ordinal)) {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				if locator.Depth > parent.Depth {
					continue
				}
				ancestor, err := entryAncestor(ctx, tx, entry.ThreadID, parent, locator.Depth)
				if err != nil {
					return err
				}
				if ancestor.ID != locator.ID {
					continue
				}
				previous, err := readLocatedEntry(tx, backendDomainV32Format, entry.ThreadID, locator)
				if err != nil {
					return err
				}
				if !matchesStoredOutputSource(previous, source, fact) {
					return fmt.Errorf("%w: conflicting output source on %s", ErrAuthorityCorrupt, entry.ID)
				}
				matched = true
				break
			}
			if !page.HasMore {
				break
			}
			if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
				return ErrAuthorityCorrupt
			}
			after = bytes.Clone(page.Next)
		}
	}
	return nil
}
