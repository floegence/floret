package sessiontree

import (
	"context"
	"errors"
	"reflect"

	"github.com/floegence/floret/v7/storage/spi"
)

// Pending is private derived metadata, never a guessed public activity state.
// Readers request the exact member before publishing summaries or range facts.
type activitySummaryPreparationRequired struct{ source string }

type activitySummaryWindowPreparationRequired struct{ sources []string }

func (e *activitySummaryWindowPreparationRequired) Error() string {
	return "selected activity summaries require preparation"
}

func (e *activitySummaryPreparationRequired) Error() string {
	return "activity summary source requires preparation"
}

func (repo *BackendRepo) withActivitySummaryPreparation(ctx context.Context, thread string, read func() error) error {
	for {
		err := read()
		var pending *activitySummaryPreparationRequired
		if !errors.As(err, &pending) {
			return err
		}
		if pending.source == "" {
			return ErrAuthorityCorrupt
		}
		if err := repo.prepareItemActivitySummary(ctx, thread, pending.source); err != nil {
			return err
		}
	}
}

func (repo *BackendRepo) prepareItemActivitySummary(ctx context.Context, thread, source string) error {
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	for {
		err := repo.prepareItemActivitySummaryOwned(work, thread, source)
		if !errors.Is(err, spi.ErrConflict) {
			return err
		}
		if err := work.Err(); err != nil {
			return err
		}
	}
}

func pendingActivityMember(tx spi.ReadTx, thread, source string) (storedItemMember, error) {
	var slots []int
	if err := readItemJSON(tx, itemSourceKey(thread, source), &slots); err != nil {
		return storedItemMember{}, err
	}
	for _, slot := range slots {
		member, err := readItemSlot(tx, thread, slot)
		if err != nil {
			return storedItemMember{}, err
		}
		if member.SourceID != source {
			return storedItemMember{}, ErrAuthorityCorrupt
		}
		if member.Kind == "tool" {
			return member, nil
		}
	}
	return storedItemMember{}, ErrEntryNotFound
}

func (repo *BackendRepo) prepareItemActivitySummaryOwned(ctx context.Context, thread, source string) error {
	var frozen ThreadMeta
	var member storedItemMember
	var entries []Entry
	var group projectionSourceGroup
	var preparedCount int
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		tx = newProjectionSourceTx(tx)
		frozen = meta
		var err error
		member, err = pendingActivityMember(tx, thread, source)
		if err != nil || !member.ActivityPending {
			return err
		}
		first, err := activeLocatedEntry(ctx, tx, meta, source)
		if err != nil || first.RawHash != member.RawHash || first.PathDepth != int64(member.CanonicalOrdinal) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := validateItemActivitySource(ctx, tx, meta, member.ItemMember); err != nil {
			return err
		}
		leaf, err := readEntryLocator(tx, thread, meta.LeafID)
		if err != nil {
			return err
		}
		group = projectionSourceGroup{Kind: "tool", Turn: member.TurnID, Run: member.RunID, ID: first.Message.ToolCallID}
		preparedCount, err = projectionCount(tx, projectionGroupKey(thread, group))
		if err != nil {
			return err
		}
		return visitProjectionSources(ctx, tx, meta, leaf, group, func(entry Entry) error {
			if entry.PathDepth < first.PathDepth || entry.PathDepth > member.ActivitySource.Depth || entry.Type != EntryToolCall && entry.Type != EntryToolResult {
				return nil
			}
			entries = append(entries, entry)
			return nil
		})
	}, func(*MemoryRepo) error { return ErrUnsupportedStoreCapability })
	if err != nil || !member.ActivityPending {
		return err
	}
	if len(entries) == 0 || entries[0].ID != source || entries[len(entries)-1].ID != member.ActivitySource.ID {
		return ErrAuthorityCorrupt
	}
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: backendDomainV32Format, metadata: map[string]migrationMetadataRead{}}
	if err := repo.prepareActivitySummariesOwned(ctx, reader, frozen, entries); err != nil {
		return err
	}
	records := &windowPreparationRecords{ReadTx: reader, records: map[string][]byte{}}
	for i, entry := range entries {
		if err := appendActivitySourceMetadata(ctx, records, member.ItemMember, entry, i == 0); err != nil {
			return reader.validationError(err)
		}
	}
	// Catch up new sources in short transactions without rereading the prepared
	// prefix. Private field selection publishes only with the current member.
	for {
		published := false
		next := preparedCount
		staged := &windowPreparationRecords{records: map[string][]byte{}}
		for key, raw := range records.records {
			staged.records[key] = raw
		}
		err := func() error {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				current, err := verifyWindowPreparationOwner(ctx, tx, frozen)
				if err != nil {
					return err
				}
				latest, err := pendingActivityMember(tx, thread, source)
				if err != nil {
					return err
				}
				if !latest.ActivityPending {
					published = true
					return nil
				}
				// These facts can advance; the original item owner cannot.
				identity := latest
				identity.ActivitySource = member.ActivitySource
				identity.ActivityStatus = member.ActivityStatus
				identity.InteractionSource = member.InteractionSource
				identity.RangeFacts = member.RangeFacts
				if !reflect.DeepEqual(identity, member) {
					return spi.ErrConflict
				}
				manifest, found, err := readItemManifest(tx, thread)
				if err != nil || !found || manifest.Leaf != current.LeafID {
					return errors.Join(spi.ErrConflict, err)
				}
				staged.ReadTx = tx
				leaf, err := readEntryLocator(tx, thread, current.LeafID)
				if err != nil {
					return err
				}
				next, err = visitProjectionSourceRange(ctx, tx, current, leaf, group, preparedCount, 128, func(entry Entry) error {
					if entry.Type != EntryToolCall && entry.Type != EntryToolResult {
						return nil
					}
					if entry.PathDepth <= member.ActivitySource.Depth || entry.PathDepth > latest.ActivitySource.Depth {
						return ErrAuthorityCorrupt
					}
					// Canonical appends index their own fields atomically. This
					// catches up locators without reading hidden activity bodies.
					ready, err := activitySourceMetadataReady(tx, entry)
					if err != nil || !ready {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					return appendActivitySourceMetadata(ctx, staged, latest.ItemMember, entry, false)
				})
				if err != nil {
					return err
				}
				count, err := projectionCount(tx, projectionGroupKey(thread, group))
				if err != nil {
					return err
				}
				if next < count {
					return nil
				}
				selection, err := readStoredActivitySources(staged, thread, source)
				if err != nil || selection.Last != latest.ActivitySource {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				facts, err := itemRangeActivityFacts(staged, thread, latest.ItemMember)
				if err != nil {
					return err
				}
				for key, raw := range staged.records {
					if err := tx.Put(itemMembershipNamespace, []byte(key), raw); err != nil {
						return err
					}
				}
				latest.ActivityPending = false
				latest.RangeFacts = facts
				if err := putItemJSON(tx, itemSlotKey(thread, latest.Slot), latest); err != nil {
					return err
				}
				if err := updateItemRange(tx, thread, manifest.Slots, latest.ItemMember); err != nil {
					return err
				}
				published = true
				return nil
			})
		}()
		if err != nil || published {
			return err
		}
		records.records, preparedCount = staged.records, next
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
