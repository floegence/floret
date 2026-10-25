package sessiontree

import (
	"context"
	"errors"
	"math/bits"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

// This cursor certifies immutable source metadata only. It is not a transcript,
// lifecycle or read snapshot. The canonical writer advances it in the source
// transaction; an upgrade resumes the missing journal suffix under migrationGate.
func displayCoverageKey(thread string) []byte { return backendDomainKey(thread, "display-coverage-v1") }

func displaySourceReady(tx spi.ReadTx, entry Entry) (bool, error) {
	for _, field := range []string{"content", "reasoning", "error"} {
		if ref := entry.bodyFields[field]; ref.Bytes > 0 {
			if _, err := readBodyTextFacts(tx, ref); err != nil {
				if errors.Is(err, spi.ErrNotFound) {
					return false, nil
				}
				return false, err
			}
		}
	}
	if ref := entry.bodyFields["activity_json"]; ref.Bytes > 0 {
		if _, err := readActivityFieldNode(tx, ref, ""); err != nil {
			if errors.Is(err, spi.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		if _, err := readActivityMergeMetadata(tx, ref); err != nil {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	return true, nil
}

func appendDisplayCoverage(tx spi.WriteTx, entry Entry, ordinal int) error {
	through, err := projectionCount(tx, displayCoverageKey(entry.ThreadID))
	if err != nil {
		return err
	}
	if through > ordinal {
		return ErrAuthorityCorrupt
	}
	if through != ordinal {
		return nil
	}
	source, err := readCanonicalEntryMetadata(tx, entry.ThreadID, entry.ID)
	if err != nil {
		return err
	}
	ready, err := displaySourceReady(tx, source)
	if err != nil || !ready {
		return err
	}
	return putProjectionCount(tx, displayCoverageKey(entry.ThreadID), ordinal+1)
}

// The caller owns migrationGate. Sources are validated outside write
// transactions, then bounded coverage is published after rechecking identity.
// Every retained branch is included so Retry/Fork never needs an old body scan.
func (repo *BackendRepo) prepareDisplaySourcesOwned(ctx context.Context, thread string) error {
	for {
		var entries []Entry
		var through, count int
		err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
			if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread); err != nil {
				return err
			}
			var err error
			count, err = readJournalCount(tx, thread)
			if err != nil {
				return err
			}
			through, err = projectionCount(tx, displayCoverageKey(thread))
			if err != nil {
				return err
			}
			if through > count {
				return ErrAuthorityCorrupt
			}
			end := min(count, through+128)
			keys := make([][]byte, 0, end-through)
			for ordinal := through; ordinal < end; ordinal++ {
				keys = append(keys, backendDomainEntryKey(thread, ordinal))
			}
			if len(keys) == 0 {
				return nil
			}
			records, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: backendDomainV32Namespace, Keys: keys})
			if err != nil {
				return err
			}
			for i, raw := range records {
				record, stored, err := decodeCanonicalEntryRecord(backendDomainV32Format, raw)
				if err != nil || record.ThreadID != thread || record.Ordinal != through+i {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				entry := stored.Entry
				entry.bodyFields = stored.Fields
				entries = append(entries, entry)
			}
			return nil
		})
		if err == nil && through < count {
			reportStartupWork(ctx, StartupPhaseIndexing, int64(through), int64(count))
		}
		if err != nil || through == count {
			return err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			for _, field := range []string{"content", "reasoning", "error"} {
				if ref := entry.bodyFields[field]; ref.Bytes > 0 {
					if err := repo.prepareBodyTextFactsOwned(ctx, thread, missingBodyTextFacts{entry.ID, field, ref}, false); err != nil {
						return err
					}
				}
			}
			if ref := entry.bodyFields["activity_json"]; ref.Bytes > 0 {
				ready := false
				if err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
					_, err := readActivityFieldNode(tx, ref, "")
					if errors.Is(err, spi.ErrNotFound) {
						return nil
					}
					if err != nil {
						return err
					}
					_, err = readActivityMergeMetadata(tx, ref)
					ready = err == nil
					return err
				}); err != nil {
					return err
				}
				if !ready {
					if err := repo.prepareActivityFieldsOwned(ctx, thread, entry.ID, false); err != nil {
						return err
					}
				}
			}
		}
		err = func() error {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread); err != nil {
					return err
				}
				current, err := projectionCount(tx, displayCoverageKey(thread))
				if err != nil {
					return err
				}
				if current != through {
					return spi.ErrConflict
				}
				for _, entry := range entries {
					if err := ctx.Err(); err != nil {
						return err
					}
					source, err := readCanonicalEntryMetadata(tx, thread, entry.ID)
					if err != nil {
						return err
					}
					if source.RawHash != entry.RawHash {
						return ErrAuthorityCorrupt
					}
					ready, err := displaySourceReady(tx, source)
					if err != nil || !ready {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
				}
				return putProjectionCount(tx, displayCoverageKey(thread), through+len(entries))
			})
		}()
		if err != nil {
			return err
		}
	}
}

// DisplayPrepared is a bounded metadata status, not a full integrity audit.
// It adds no preparation work and does not change canonical runtime readiness.
func (repo *BackendRepo) DisplayPrepared(ctx context.Context, thread string) (bool, error) {
	if repo == nil || ctx == nil || thread == "" {
		return false, spi.ErrInvalidArgument
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.DisplayPrepared(ctx, thread)
	}
	if repo.waitForReady != nil {
		return false, nil
	}
	ready := false
	err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread)
		if errors.Is(err, ErrThreadNotFound) || errors.Is(err, ErrThreadDeleted) {
			return nil
		}
		if err != nil {
			return err
		}
		count, err := readJournalCount(tx, thread)
		if err != nil {
			return err
		}
		for _, key := range [][]byte{projectionCoverageKey(thread), displayCoverageKey(thread)} {
			through, err := projectionCount(tx, key)
			if err != nil {
				return err
			}
			if through > count {
				return ErrAuthorityCorrupt
			}
			if through != count {
				return nil
			}
		}
		needs, err := missingWindowPreparation(tx, meta, windowPreparationNeeds{true, true, true, true})
		if err != nil || needs != (windowPreparationNeeds{}) {
			return err
		}
		source, err := pendingDisplayActivity(tx, meta)
		if err != nil || source != "" {
			return err
		}
		sparse, err := readSparseCoverage(tx, thread)
		if err != nil {
			return err
		}
		if sparse.Through != count {
			return nil
		}
		queue, found, err := readQueueSourceIndex(tx, thread)
		if err != nil {
			return err
		}
		ready = sparse.Queue == 0 || found && queue.Events == sparse.Queue
		return nil
	})
	return ready, err
}

// Find only one unprepared source through the existing range tree. Already
// prepared trees need one root read, independent of their historical length.
func pendingDisplayActivity(tx spi.ReadTx, meta ThreadMeta) (string, error) {
	manifest, found, err := readItemManifest(tx, meta.ID)
	if err != nil || !found || manifest.Leaf != meta.LeafID {
		return "", errors.Join(ErrAuthorityCorrupt, err)
	}
	if manifest.Slots == 0 {
		return "", nil
	}
	level := bits.Len(uint(manifest.Slots - 1))
	block := 0
	for {
		summary, err := readItemRangeSummary(tx, meta.ID, level, block)
		if err != nil {
			return "", err
		}
		if summary.Pending == 0 {
			return "", nil
		}
		if level == 0 {
			member, err := readItemSlot(tx, meta.ID, block)
			if err != nil {
				return "", err
			}
			if !member.ActivityPending {
				return "", ErrAuthorityCorrupt
			}
			return member.SourceID, nil
		}
		level--
		block *= 2
		left, err := readItemRangeSummary(tx, meta.ID, level, block)
		if err != nil {
			return "", err
		}
		if left.Pending == 0 {
			block++
		}
	}
}

func (repo *BackendRepo) prepareDisplayThreadOwned(ctx context.Context, thread string) error {
	if err := repo.prepareProjectionSources(ctx, thread); err != nil {
		return err
	}
	if err := repo.prepareDisplaySourcesOwned(ctx, thread); err != nil {
		return err
	}
	if err := repo.prepareWindowProjectionSnapshot(ctx, thread); err != nil {
		return err
	}
	for {
		var source string
		err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
			meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread)
			if err != nil {
				return err
			}
			source, err = pendingDisplayActivity(tx, meta)
			return err
		})
		if err != nil {
			return err
		}
		if source == "" {
			break
		}
		if err := repo.prepareItemActivitySummaryOwned(ctx, thread, source); err != nil {
			return err
		}
	}
	if err := repo.prepareSparseSources(ctx, thread); err != nil {
		return err
	}
	ready := false
	if err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		coverage, err := readSparseCoverage(tx, thread)
		if err != nil {
			return err
		}
		queue, found, err := readQueueSourceIndex(tx, thread)
		ready = coverage.Queue == 0 || found && queue.Events == coverage.Queue
		return err
	}); err != nil {
		return err
	}
	if ready {
		return nil
	}
	return repo.rebuildCurrentQueueSources(ctx, thread)
}

// Explicit upgrade preparation uses the same owner as the automatic worker.
func (repo *BackendRepo) PrepareDisplayThread(ctx context.Context, thread string) error {
	if repo == nil || ctx == nil || thread == "" {
		return spi.ErrInvalidArgument
	}
	if _, err := repo.Thread(ctx, thread); err != nil {
		return err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.PrepareDisplayThread(ctx, thread)
	}
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	for {
		err := repo.prepareDisplayThreadOwned(work, thread)
		if !errors.Is(err, spi.ErrConflict) {
			return err
		}
		if err := work.Err(); err != nil {
			return err
		}
	}
}
