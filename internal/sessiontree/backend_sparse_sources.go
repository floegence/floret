package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// Only immutable source identities are indexed. Reducers always read the
// canonical records; no queue, parent-event, or thread lifecycle is duplicated.
const sparseSourcesNamespace = "floret.sessiontree.sparse-sources.v2"

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

type sparseSourceCoverage struct {
	Through int `json:"through"`
	Queue   int `json:"queue"`
	Parent  int `json:"parent"`
}

func (coverage *sparseSourceCoverage) add(entry Entry, delta int) {
	switch sparseSourceCategory(entry) {
	case "queue":
		coverage.Queue += delta
	case "parent":
		coverage.Parent += delta
	}
}

func sparseCoverage(tx spi.ReadTx, thread string) (int, error) {
	coverage, err := readSparseCoverage(tx, thread)
	return coverage.Through, err
}

func readSparseCoverage(tx spi.ReadTx, thread string) (sparseSourceCoverage, error) {
	raw, err := tx.Get(sparseSourcesNamespace, sparseCoverageKey(thread))
	if errors.Is(err, spi.ErrNotFound) {
		return sparseSourceCoverage{}, nil
	}
	if err != nil {
		return sparseSourceCoverage{}, err
	}
	var coverage sparseSourceCoverage
	if err := decodeCanonicalBodyJSON(raw, &coverage); err != nil || coverage.Through < 0 || coverage.Queue < 0 || coverage.Parent < 0 || coverage.Queue > coverage.Through || coverage.Parent > coverage.Through-coverage.Queue {
		return coverage, errors.Join(ErrAuthorityCorrupt, err)
	}
	return coverage, nil
}
func writeSparseCoverage(tx spi.WriteTx, thread string, coverage sparseSourceCoverage) error {
	if coverage.Through == 0 {
		return deleteCanonicalOptionalRecord(tx, sparseSourcesNamespace, sparseCoverageKey(thread))
	}
	raw, err := json.Marshal(coverage)
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
	coverage, err := readSparseCoverage(tx, thread)
	if err != nil {
		return err
	}
	if coverage.Through > ordinal {
		return ErrAuthorityCorrupt
	}
	if coverage.Through != ordinal {
		return nil
	}
	if err := putSparseSource(tx, entry, ordinal); err != nil {
		return err
	}
	coverage.Through = ordinal + 1
	coverage.add(entry, 1)
	return writeSparseCoverage(tx, thread, coverage)
}
func removeSparseSource(tx spi.WriteTx, thread string, ordinal int, entry Entry) error {
	coverage, err := readSparseCoverage(tx, thread)
	if err != nil {
		return err
	}
	if coverage.Through > ordinal+1 {
		return ErrAuthorityCorrupt
	}
	if category := sparseSourceCategory(entry); category != "" {
		if err := deleteCanonicalOptionalRecord(tx, sparseSourcesNamespace, sparseSourceKey(thread, category, ordinal)); err != nil {
			return err
		}
	}
	if coverage.Through == ordinal+1 {
		coverage.Through = ordinal
		coverage.add(entry, -1)
		return writeSparseCoverage(tx, thread, coverage)
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
		coverage, err := readSparseCoverage(tx, thread)
		if err != nil {
			return err
		}
		if coverage.Through > count {
			return ErrAuthorityCorrupt
		}
		complete = coverage.Through == count
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
				coverage, err := readSparseCoverage(tx, thread)
				if err != nil {
					return err
				}
				if coverage.Through > count {
					return ErrAuthorityCorrupt
				}
				end := min(count, coverage.Through+128)
				if end == coverage.Through {
					complete = true
					return nil
				}
				keys := make([][]byte, 0, end-coverage.Through)
				for ordinal := coverage.Through; ordinal < end; ordinal++ {
					keys = append(keys, backendDomainEntryKey(thread, ordinal))
				}
				records, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: backendDomainV32Namespace, Keys: keys})
				if err != nil {
					return err
				}
				for i, raw := range records {
					if err := ctx.Err(); err != nil {
						return err
					}
					ordinal := coverage.Through + i
					record, stored, err := decodeCanonicalEntryRecord(backendDomainV32Format, raw)
					if err != nil || record.ThreadID != thread || record.Ordinal != ordinal {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					entry := stored.Entry
					entry.bodyFields = stored.Fields
					if err := putSparseSource(tx, entry, ordinal); err != nil {
						return err
					}
					coverage.add(entry, 1)
				}
				complete = end == count
				coverage.Through = end
				return writeSparseCoverage(tx, thread, coverage)
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
	coverage, err := readSparseCoverage(tx, thread)
	if err != nil {
		return err
	}
	if coverage.Through != count {
		return spi.ErrConflict
	}
	expected := coverage.Queue
	if category == "parent" {
		expected = coverage.Parent
	} else if category != "queue" {
		return ErrAuthorityCorrupt
	}
	seen := 0
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
			seen++
			if seen > expected {
				return ErrAuthorityCorrupt
			}
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
			if seen != expected {
				return ErrAuthorityCorrupt
			}
			return nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}
