package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// Only immutable source identities are indexed. Reducers always read the
// canonical records; no queue, parent-event, or thread lifecycle is duplicated.
const sparseSourcesNamespace = "floret.sessiontree.sparse-sources.v1"

func sparseCoverageKey(thread string) []byte { return backendDomainKey(thread, "coverage") }
func sparseSourceKey(thread, category string, ordinal int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(thread), storagecodec.TupleString(category), storagecodec.TupleOrdinal(uint64(ordinal)))
}
func sparseSourceCategory(entry Entry) string {
	switch entry.Type {
	case EntryQueueAdded, EntryQueueDeleted, EntryQueuePromoted, EntryQueueReordered:
		return "queue"
	case EntryParentEvent, EntryParentEventsAdmitted:
		return "parent"
	case EntryTurnMarker:
		if entry.TurnStatus == TurnStarted && entry.Metadata[TurnKindMetadataKey] != TurnKindContextCompaction {
			return "parent"
		}
	}
	return ""
}
func sparseCoverage(tx spi.ReadTx, thread string) (int, error) {
	raw, err := tx.Get(sparseSourcesNamespace, sparseCoverageKey(thread))
	if errors.Is(err, spi.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var count int
	if err := decodeCanonicalBodyJSON(raw, &count); err != nil || count < 0 {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return count, nil
}
func writeSparseCoverage(tx spi.WriteTx, thread string, count int) error {
	if count == 0 {
		return deleteCanonicalOptionalRecord(tx, sparseSourcesNamespace, sparseCoverageKey(thread))
	}
	raw, err := json.Marshal(count)
	if err != nil {
		return err
	}
	return tx.Put(sparseSourcesNamespace, sparseCoverageKey(thread), raw)
}
func putSparseSource(tx spi.WriteTx, entry Entry, ordinal int) error {
	category := sparseSourceCategory(entry)
	if category == "" {
		return nil
	}
	return tx.Put(sparseSourcesNamespace, sparseSourceKey(entry.ThreadID, category, ordinal), []byte(entry.ID))
}
func appendSparseSource(tx spi.WriteTx, thread string, ordinal int, entry Entry) error {
	through, err := sparseCoverage(tx, thread)
	if err != nil {
		return err
	}
	if through > ordinal {
		return ErrAuthorityCorrupt
	}
	if through != ordinal {
		return nil
	}
	if err := putSparseSource(tx, entry, ordinal); err != nil {
		return err
	}
	return writeSparseCoverage(tx, thread, ordinal+1)
}
func removeSparseSource(tx spi.WriteTx, thread string, ordinal int, entry Entry) error {
	through, err := sparseCoverage(tx, thread)
	if err != nil {
		return err
	}
	if through > ordinal+1 {
		return ErrAuthorityCorrupt
	}
	if category := sparseSourceCategory(entry); category != "" {
		if err := deleteCanonicalOptionalRecord(tx, sparseSourcesNamespace, sparseSourceKey(thread, category, ordinal)); err != nil {
			return err
		}
	}
	if through == ordinal+1 {
		return writeSparseCoverage(tx, thread, ordinal)
	}
	return nil
}

// Released stores prepare only missing source positions, in short transactions.
// Publication is coverage of a contiguous journal prefix. Appends and truncation
// maintain the same coverage transactionally with their canonical source.
func (repo *BackendRepo) prepareSparseSources(ctx context.Context, thread string) error {
	if _, err := repo.Thread(ctx, thread); err != nil {
		return err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.prepareSparseSources(ctx, thread)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return nil
	}
	complete := false
	if err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
		count, err := readJournalCount(tx, thread)
		if err != nil {
			return err
		}
		through, err := sparseCoverage(tx, thread)
		if err != nil {
			return err
		}
		if through > count {
			return ErrAuthorityCorrupt
		}
		complete = through == count
		return nil
	}, func(*MemoryRepo) error { complete = true; return nil }); err != nil || complete {
		return err
	}
	for {
		complete := false
		err := func() error {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread); err != nil {
					return err
				}
				count, err := readJournalCount(tx, thread)
				if err != nil {
					return err
				}
				through, err := sparseCoverage(tx, thread)
				if err != nil {
					return err
				}
				if through > count {
					return ErrAuthorityCorrupt
				}
				end := min(count, through+128)
				for ordinal := through; ordinal < end; ordinal++ {
					if err := ctx.Err(); err != nil {
						return err
					}
					entry, err := readCanonicalEntryOrdinal(tx, thread, ordinal)
					if err != nil {
						return err
					}
					if err := putSparseSource(tx, entry, ordinal); err != nil {
						return err
					}
				}
				complete = end == count
				if end == through {
					return nil
				}
				return writeSparseCoverage(tx, thread, end)
			})
		}()
		if err != nil || complete {
			return err
		}
	}
}

func visitSparseSources(ctx context.Context, tx spi.ReadTx, thread, category string, visit func(Entry) error) error {
	count, err := readJournalCount(tx, thread)
	if err != nil {
		return err
	}
	through, err := sparseCoverage(tx, thread)
	if err != nil {
		return err
	}
	if through != count {
		return spi.ErrConflict
	}
	prefix := backendDomainKey(thread, category)
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: sparseSourcesNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: 128})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			locator, err := readEntryLocator(tx, thread, string(row.Value))
			if err != nil || locator.Ordinal >= count || !bytes.Equal(row.Key, sparseSourceKey(thread, category, locator.Ordinal)) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			entry, err := readLocatedEntry(tx, backendDomainV32Format, thread, locator)
			if err != nil {
				return err
			}
			if sparseSourceCategory(entry) != category {
				return ErrAuthorityCorrupt
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}
