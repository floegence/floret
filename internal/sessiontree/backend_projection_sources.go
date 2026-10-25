package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// Only canonical source positions are stored, never transcript or lifecycle.
// v7 adds path-bound parent receipt and admission sources. It excludes
// presentation-neutral savepoints from display dependencies and
// retains bounded ordinary tool source selection. Earlier unreleased preparation
// cannot certify those references and is deliberately not accepted here.
const projectionSourcesNamespace = "floret.sessiontree.projection-sources.v7"

type projectionSourceMember struct {
	EntryID string `json:"entry_id"`
	Ordinal int    `json:"ordinal"`
	RawHash string `json:"raw_hash"`
}

func projectionCoverageKey(thread string) []byte { return backendDomainKey(thread, "coverage") }
func projectionGroupKey(thread string, group projectionSourceGroup) []byte {
	return backendDomainKey(thread, "group", group.Kind, group.Turn, group.Run, group.ID)
}
func projectionMemberKey(thread string, group projectionSourceGroup, sequence int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(thread), storagecodec.TupleString("member"), storagecodec.TupleString(group.Kind), storagecodec.TupleString(group.Turn), storagecodec.TupleString(group.Run), storagecodec.TupleString(group.ID), storagecodec.TupleOrdinal(uint64(sequence)))
}
func projectionCount(tx spi.ReadTx, key []byte) (int, error) {
	raw, err := tx.Get(projectionSourcesNamespace, key)
	if errors.Is(err, spi.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var count int
	if err := storagecodec.DecodeJSON(raw, &count); err != nil || count < 1 {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return count, nil
}
func putProjectionCount(tx spi.WriteTx, key []byte, count int) error {
	if count < 0 {
		return ErrAuthorityCorrupt
	}
	if count == 0 {
		return deleteCanonicalOptionalRecord(tx, projectionSourcesNamespace, key)
	}
	raw, err := json.Marshal(count)
	if err != nil {
		return err
	}
	return tx.Put(projectionSourcesNamespace, key, raw)
}
func putProjectionSources(ctx context.Context, tx spi.WriteTx, entry Entry, ordinal int) error {
	return putProjectionSourceBatch(ctx, tx, entry.ThreadID, []Entry{entry}, ordinal)
}

// A preparation page often contains several records for the same tool. Read
// and publish its count once in the existing transaction; immutable members
// retain their exact order, identity and hashes.
func putProjectionSourceBatch(ctx context.Context, tx spi.WriteTx, thread string, entries []Entry, first int) error {
	type groupAppend struct {
		group   projectionSourceGroup
		members []projectionSourceMember
		entries []Entry
	}
	groups := []groupAppend{}
	positions := map[projectionSourceGroup]int{}
	// A batch already contains most entries' parents. Derive their source
	// references once in memory and publish them with the remaining records.
	// The map is transaction-local; forks outside the batch use canonical
	// predecessor metadata, and failures never advance durable coverage.
	goals := make(map[string]goalSourceContext, len(entries))
	parents := make(map[string]parentSourceContext, len(entries))
	var records []spi.Record
	for i, entry := range entries {
		if entry.ThreadID != thread {
			return ErrAuthorityCorrupt
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		parent, found := parents[entry.ParentID]
		if !found {
			var err error
			parent, err = readParentSourceContext(tx, thread, entry.ParentID)
			if err != nil {
				return err
			}
		}
		parent, err := deriveParentSourceContext(ctx, tx, entry, parent)
		if err != nil {
			return err
		}
		parents[entry.ID] = parent
		parentRaw, err := encodeActivityMetadata("parent-sources-v1", parent)
		if err != nil {
			return err
		}
		records = append(records, spi.Record{Key: parentSourceKey(thread, entry.ID), Value: parentRaw})
		refs, found := goals[entry.ParentID]
		if !found {
			var err error
			refs, err = readGoalSourceContext(tx, thread, entry.ParentID)
			if err != nil {
				return err
			}
		}
		refs, err = deriveGoalSourceContext(ctx, tx, entry, refs)
		if err != nil {
			return err
		}
		goals[entry.ID] = refs
		raw, err := encodeActivityMetadata("goal-sources-v1", refs)
		if err != nil {
			return err
		}
		records = append(records, spi.Record{Key: goalSourceKey(thread, entry.ID), Value: raw})
		memberships, err := projectionSourceGroups(entry)
		if err != nil {
			return err
		}
		for _, group := range memberships {
			position, found := positions[group]
			if !found {
				position = len(groups)
				positions[group] = position
				groups = append(groups, groupAppend{group: group})
			}
			groups[position].members = append(groups[position].members, projectionSourceMember{entry.ID, first + i, entry.RawHash})
			groups[position].entries = append(groups[position].entries, entry)
		}
	}
	for _, group := range groups {
		key := projectionGroupKey(thread, group.group)
		count, err := projectionCount(tx, key)
		if err != nil {
			return err
		}
		if count > 0 {
			previous, err := readProjectionMember(tx, thread, group.group, count-1)
			if err != nil || previous.Ordinal >= group.members[0].Ordinal {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if group.group.Kind == "tool" || group.group.Kind == "invocation" {
			state := windowToolSources{}
			if count > 0 {
				state, err = readWindowToolSources(tx, thread, group.group)
				if err != nil || state.Count != count {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
			}
			for i, entry := range group.entries {
				state.observe(entry, group.members[i])
			}
			if err := putWindowToolSources(tx, thread, group.group, state); err != nil {
				return err
			}
		}
		for i, member := range group.members {
			raw, err := json.Marshal(member)
			if err != nil {
				return err
			}
			records = append(records, spi.Record{Key: projectionMemberKey(thread, group.group, count+i), Value: raw})
		}
		raw, err := json.Marshal(count + len(group.members))
		if err != nil {
			return err
		}
		records = append(records, spi.Record{Key: key, Value: raw})
	}
	for first := 0; first < len(records); first += storagebridge.MaxRecordBatch {
		if err := storagebridge.WriteRecords(tx, projectionSourcesNamespace, records[first:min(first+storagebridge.MaxRecordBatch, len(records))]); err != nil {
			return err
		}
	}
	return nil
}
func readProjectionMember(tx spi.ReadTx, thread string, group projectionSourceGroup, sequence int) (projectionSourceMember, error) {
	raw, err := tx.Get(projectionSourcesNamespace, projectionMemberKey(thread, group, sequence))
	if err != nil {
		return projectionSourceMember{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var member projectionSourceMember
	if err := storagecodec.DecodeJSON(raw, &member); err != nil || member.EntryID == "" || member.Ordinal < 0 || member.RawHash == "" {
		return member, errors.Join(ErrAuthorityCorrupt, err)
	}
	return member, nil
}
func appendProjectionSources(ctx context.Context, tx spi.WriteTx, entry Entry, ordinal int) error {
	through, err := projectionCount(tx, projectionCoverageKey(entry.ThreadID))
	if err != nil {
		return err
	}
	if through > ordinal {
		return ErrAuthorityCorrupt
	}
	// Missing prefixes belong to resumable preparation, including this append.
	if through != ordinal {
		return nil
	}
	if err := putProjectionSources(ctx, tx, entry, ordinal); err != nil {
		return err
	}
	return putProjectionCount(tx, projectionCoverageKey(entry.ThreadID), ordinal+1)
}
func removeProjectionSources(tx spi.WriteTx, entry Entry, ordinal int) error {
	through, err := projectionCount(tx, projectionCoverageKey(entry.ThreadID))
	if err != nil {
		return err
	}
	if through > ordinal+1 {
		return ErrAuthorityCorrupt
	}
	if through <= ordinal {
		return nil
	}
	groups, err := projectionSourceGroups(entry)
	if err != nil {
		return err
	}
	for _, group := range groups {
		key := projectionGroupKey(entry.ThreadID, group)
		count, err := projectionCount(tx, key)
		if err != nil || count == 0 {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		member, err := readProjectionMember(tx, entry.ThreadID, group, count-1)
		if err != nil || member.EntryID != entry.ID || member.Ordinal != ordinal || member.RawHash != entry.RawHash {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := tx.Delete(projectionSourcesNamespace, projectionMemberKey(entry.ThreadID, group, count-1)); err != nil {
			return err
		}
		if err := putProjectionCount(tx, key, count-1); err != nil {
			return err
		}
		if group.Kind == "tool" || group.Kind == "invocation" {
			if count == 1 {
				if err := tx.Delete(projectionSourcesNamespace, windowToolSourcesKey(entry.ThreadID, group)); err != nil {
					return err
				}
			} else {
				// Removal is exceptional. Rebuild this source-only selection
				// from retained canonical members before publishing coverage.
				state := windowToolSources{}
				for i := 0; i < count-1; i++ {
					ref, err := readProjectionMember(tx, entry.ThreadID, group, i)
					if err != nil {
						return err
					}
					node, err := readEntryLocator(tx, entry.ThreadID, ref.EntryID)
					if err != nil {
						return err
					}
					source, err := readLocatedEntry(tx, backendDomainV32Format, entry.ThreadID, node)
					if err != nil || source.RawHash != ref.RawHash {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					state.observe(source, ref)
				}
				if err := putWindowToolSources(tx, entry.ThreadID, group, state); err != nil {
					return err
				}
			}
		}
	}
	if err := tx.Delete(projectionSourcesNamespace, goalSourceKey(entry.ThreadID, entry.ID)); err != nil {
		return err
	}
	if err := tx.Delete(projectionSourcesNamespace, parentSourceKey(entry.ThreadID, entry.ID)); err != nil {
		return err
	}
	if entry.Type == EntryParentEvent {
		if err := tx.Delete(projectionSourcesNamespace, parentReceiptKey(entry.ThreadID, entry.ID)); err != nil {
			return err
		}
	}
	if entry.Type == EntryParentEventsAdmitted {
		prefix := backendDomainKey(entry.ThreadID, "parent-admission", entry.ID)
		for {
			page, err := tx.Scan(spi.ScanRequest{Namespace: projectionSourcesNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128})
			if err != nil {
				return err
			}
			for _, record := range page.Records {
				if err := tx.Delete(projectionSourcesNamespace, record.Key); err != nil {
					return err
				}
			}
			if !page.HasMore {
				break
			}
		}
	}
	return putProjectionCount(tx, projectionCoverageKey(entry.ThreadID), ordinal)
}

// Coverage is private preparation progress, never authority for a partial read.
// Canonical bytes are unchanged; each short transaction advances one prefix.
func (repo *BackendRepo) prepareProjectionSources(ctx context.Context, thread string) error {
	if _, err := repo.Thread(ctx, thread); err != nil {
		return err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.prepareProjectionSources(ctx, thread)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return nil
	}
	complete := false
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
		count, err := readJournalCount(tx, thread)
		if err != nil {
			return err
		}
		through, err := projectionCount(tx, projectionCoverageKey(thread))
		if err != nil {
			return err
		}
		if through > count {
			return ErrAuthorityCorrupt
		}
		complete = through == count
		return nil
	}, func(*MemoryRepo) error { complete = true; return nil })
	if err != nil || complete {
		return err
	}
	for {
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
				through, err := projectionCount(tx, projectionCoverageKey(thread))
				if err != nil {
					return err
				}
				if through > count {
					return ErrAuthorityCorrupt
				}
				end := min(count, through+128)
				sparse, err := readSparseCoverage(tx, thread)
				if err != nil {
					return err
				}
				if sparse.Through > count {
					return ErrAuthorityCorrupt
				}
				// The same canonical batch supplies queue and parent locators.
				// Never skip an uncovered sparse prefix or rewrite covered entries.
				sharedSparse := sparse.Through >= through && sparse.Through < end
				keys := make([][]byte, 0, end-through)
				for ordinal := through; ordinal < end; ordinal++ {
					keys = append(keys, backendDomainEntryKey(thread, ordinal))
				}
				var records [][]byte
				if len(keys) > 0 {
					records, err = storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: backendDomainV32Namespace, Keys: keys})
					if err != nil {
						return err
					}
				}
				entries := make([]Entry, len(records))
				for i, raw := range records {
					if err := ctx.Err(); err != nil {
						return err
					}
					ordinal := through + i
					record, stored, err := decodeCanonicalEntryRecord(backendDomainV32Format, raw)
					if err != nil || record.ThreadID != thread || record.Ordinal != ordinal {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					entry := stored.Entry
					entry.bodyFields = stored.Fields
					entries[i] = entry
					if sharedSparse && ordinal >= sparse.Through {
						if err := appendCurrentQueueSources(tx, entry, sparse); err != nil {
							return err
						}
						if err := putSparseSource(tx, entry, ordinal); err != nil {
							return err
						}
						sparse.add(entry, 1)
					}
				}
				if err := putProjectionSourceBatch(ctx, tx, thread, entries, through); err != nil {
					return err
				}
				if sharedSparse {
					sparse.Through = end
					if err := writeSparseCoverage(tx, thread, sparse); err != nil {
						return err
					}
				}
				complete = end == count
				if end == through {
					return nil
				}
				return putProjectionCount(tx, projectionCoverageKey(thread), end)
			})
		}()
		if err != nil || complete {
			return err
		}
	}
}

// Dense membership rejects missing and extra records. Canonical sources verify
// identity and hash; ancestry excludes branches outside the requested leaf.
func visitProjectionSources(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf entryLocator, group projectionSourceGroup, visit func(Entry) error) error {
	_, err := visitProjectionSourceRange(ctx, tx, meta, leaf, group, 0, 0, visit)
	return err
}

// A positive limit reads the next bounded suffix of an already validated
// group. Immutable members retain dense sequence and ordinal checks.
func visitProjectionSourceRange(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf entryLocator, group projectionSourceGroup, first, limit int, visit func(Entry) error) (int, error) {
	count, err := readJournalCount(tx, meta.ID)
	if err != nil {
		return first, err
	}
	through, err := projectionCount(tx, projectionCoverageKey(meta.ID))
	if err != nil {
		return first, err
	}
	if through != count {
		return first, spi.ErrConflict
	}
	expected, err := projectionCount(tx, projectionGroupKey(meta.ID, group))
	if err != nil {
		return first, err
	}
	if first < 0 || first > expected || limit < 0 {
		return first, ErrAuthorityCorrupt
	}
	prefix := projectionMemberKey(meta.ID, group, first)
	end := backendKeyRangeEnd(backendDomainKey(meta.ID, "member", group.Kind, group.Turn, group.Run, group.ID))
	if limit > 0 && expected-first > limit {
		expected = first + limit
		end = projectionMemberKey(meta.ID, group, expected)
	}
	seen, previous := first, -1
	if first > 0 {
		prior, err := readProjectionMember(tx, meta.ID, group, first-1)
		if err != nil || prior.Ordinal >= count {
			return first, errors.Join(ErrAuthorityCorrupt, err)
		}
		previous = prior.Ordinal
	}
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return first, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: projectionSourcesNamespace, Start: prefix, End: end, After: after, Limit: 128})
		if err != nil {
			return first, err
		}
		for _, row := range page.Records {
			if seen >= expected || !bytes.Equal(row.Key, projectionMemberKey(meta.ID, group, seen)) {
				return first, ErrAuthorityCorrupt
			}
			var member projectionSourceMember
			if err := storagecodec.DecodeJSON(row.Value, &member); err != nil || member.EntryID == "" || member.Ordinal <= previous || member.Ordinal >= count {
				return first, errors.Join(ErrAuthorityCorrupt, err)
			}
			previous = member.Ordinal
			seen++
			node, err := readEntryLocator(tx, meta.ID, member.EntryID)
			if err != nil || node.Ordinal != member.Ordinal {
				return first, errors.Join(ErrAuthorityCorrupt, err)
			}
			entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, node)
			if err != nil || entry.RawHash != member.RawHash {
				return first, errors.Join(ErrAuthorityCorrupt, err)
			}
			groups, err := projectionSourceGroups(entry)
			if err != nil || !slices.Contains(groups, group) {
				return first, errors.Join(ErrAuthorityCorrupt, err)
			}
			if node.Depth > leaf.Depth || node.Ordinal > leaf.Ordinal {
				continue
			}
			ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, node.Depth)
			if err != nil {
				return first, err
			}
			if ancestor.ID == entry.ID {
				if err := visit(entry); err != nil {
					return first, err
				}
			}
		}
		if !page.HasMore {
			if seen != expected {
				return first, ErrAuthorityCorrupt
			}
			return seen, nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return first, ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}
