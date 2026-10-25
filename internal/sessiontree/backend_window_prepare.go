package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

// Readiness anchors are cheap startup checks, not a repeated integrity audit.
// Actual window reads still validate every requested source at one frozen leaf.
type windowPreparationNeeds struct{ membership, window, context, model bool }

func missingWindowPreparation(tx spi.ReadTx, meta ThreadMeta, wanted windowPreparationNeeds) (windowPreparationNeeds, error) {
	var needs windowPreparationNeeds
	if wanted.membership {
		membership, found, err := readItemManifest(tx, meta.ID)
		if err != nil {
			return needs, err
		}
		needs.membership = !found || membership.Leaf != meta.LeafID
	}
	var window storedWindowSources
	var context storedContextProjection
	var model storedModelContextSources
	for _, field := range []struct {
		key     []byte
		value   any
		leaf    *contextSourceRef
		missing *bool
		wanted  bool
	}{
		{windowSourcesKey(meta.ID), &window, &window.Leaf, &needs.window, wanted.window},
		{contextProjectionKey(meta.ID), &context, &context.Leaf, &needs.context, wanted.context},
		{modelContextSourcesKey(meta.ID), &model, &model.Leaf, &needs.model, wanted.model},
	} {
		if !field.wanted {
			continue
		}
		err := readItemJSON(tx, field.key, field.value)
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return needs, err
		}
		*field.missing = errors.Is(err, spi.ErrNotFound) || field.leaf.ID != meta.LeafID
	}
	if wanted.window {
		if window.SummaryVersion < 0 || window.SummaryVersion > 1 || window.LifecycleVersion < 0 || window.LifecycleVersion > 1 {
			return needs, ErrAuthorityCorrupt
		}
		needs.window = needs.window || window.SummaryVersion == 0 || window.LifecycleVersion == 0
	}
	return needs, nil
}
func (repo *BackendRepo) prepareWindowProjection(ctx context.Context, thread string) error {
	return repo.prepareWindowReadSources(ctx, thread, windowPreparationNeeds{true, true, true, true})
}

// Narrow readers and complete windows share preparation and publication. Only
// requested, missing metadata is rebuilt; unrelated source contracts stay out
// of the read and no complete-history work holds a write transaction.
func (repo *BackendRepo) prepareWindowReadSources(ctx context.Context, thread string, wanted windowPreparationNeeds) error {
	if err := repo.prepareProjectionSources(ctx, thread); err != nil {
		return err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.prepareWindowReadSources(ctx, thread, wanted)
	}
	var needs windowPreparationNeeds
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		var err error
		needs, err = missingWindowPreparation(tx, meta, wanted)
		return err
	}, func(*MemoryRepo) error { return nil })
	if err != nil || needs == (windowPreparationNeeds{}) {
		return err
	}
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	for {
		err := repo.prepareWindowReadSnapshot(work, thread, wanted)
		if !errors.Is(err, spi.ErrConflict) {
			return err
		}
		if err := work.Err(); err != nil {
			return err
		}
	}
}

// This is a transient write set of source references and numeric metadata.
// It never stores canonical bodies, reducer state or a second transcript.
type windowPreparationRecords struct {
	spi.ReadTx
	records map[string][]byte
}

func (tx *windowPreparationRecords) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == itemMembershipNamespace {
		if value, ok := tx.records[string(key)]; ok {
			return bytes.Clone(value), nil
		}
	}
	return tx.ReadTx.Get(namespace, key)
}
func (tx *windowPreparationRecords) Put(namespace string, key, value []byte) error {
	if namespace != itemMembershipNamespace {
		return spi.ErrInvalidArgument
	}
	tx.records[string(key)] = bytes.Clone(value)
	return nil
}
func (tx *windowPreparationRecords) Delete(string, []byte) error { return spi.ErrInvalidArgument }

// The migration gate serializes preparation, but holds no repository lock or
// storage transaction during the scan. Immutable sources are read once. Every
// short staging transaction verifies the frozen ancestor. Appended entries are
// caught up through existing reducers; requested readiness anchors publish only
// after private metadata reaches the current tail. Branch changes still fail.
func (repo *BackendRepo) prepareWindowProjectionSnapshot(ctx context.Context, thread string) error {
	return repo.prepareWindowReadSnapshot(ctx, thread, windowPreparationNeeds{true, true, true, true})
}

func (repo *BackendRepo) prepareWindowReadSnapshot(ctx context.Context, thread string, wanted windowPreparationNeeds) error {
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: backendDomainV32Format, metadata: map[string]migrationMetadataRead{}}
	meta, err := readPublishedThreadMeta(reader, backendDomainV32Format, thread)
	if err != nil {
		return err
	}
	needs, err := missingWindowPreparation(reader, meta, wanted)
	if err != nil || needs == (windowPreparationNeeds{}) {
		return err
	}
	if needs.window {
		var stored storedWindowSources
		if err := readItemJSON(reader, windowSourcesKey(thread), &stored); err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		} else if err == nil && stored.Leaf.ID == meta.LeafID && stored.SummaryVersion == 0 && stored.LifecycleVersion == 1 {
			if err := repo.prepareWindowSummarySource(ctx, reader, meta, stored); err != nil {
				return err
			}
			if needs == (windowPreparationNeeds{window: true}) {
				return nil
			}
			// Other independently missing projections keep their existing owner
			// and start from the newly published canonical snapshot.
			return repo.prepareWindowReadSnapshot(ctx, thread, wanted)
		}
	}
	// Window compaction anchors depend on membership at the same prefix.
	// Rebuild them together so catch-up uses one private set of anchors.
	if needs.window {
		needs.membership = true
	}
	var entries []Entry
	var metadata []PathMetadata
	if meta.LeafID != "" {
		leaf, err := readEntryLocator(reader, thread, meta.LeafID)
		if err != nil {
			return reader.validationError(err)
		}
		if needs.membership || needs.window {
			entries = make([]Entry, 0, leaf.Depth)
		}
		if needs.model {
			metadata = make([]PathMetadata, 0, leaf.Depth)
		}
	}
	var contextProjection ThreadContextProjection
	err = visitCanonicalPathMetadata(ctx, reader, thread, meta.LeafID, func(entry Entry) error {
		if needs.model {
			source, err := canonicalPathMetadata(entry)
			if err != nil {
				return err
			}
			metadata = append(metadata, source)
		}
		if needs.membership || needs.window {
			entries = append(entries, entry)
		}
		if needs.context {
			return contextProjection.Observe(entry)
		}
		return nil
	})
	if err != nil {
		return reader.validationError(err)
	}
	records := &windowPreparationRecords{ReadTx: reader, records: map[string][]byte{}}
	if needs.membership || needs.window {
		entries, err = membershipEntries(ctx, reader, entries)
		if err != nil {
			return reader.validationError(err)
		}
		window, turns, membership, err := projectWindowSources(ctx, reader, entries)
		if err != nil {
			return reader.validationError(err)
		}
		if needs.membership {
			if err := storeItemMembership(ctx, records, meta, entries, membership); err != nil {
				return reader.validationError(err)
			}
		}
		if needs.window {
			if err := storeWindowSources(records, thread, window, turns); err != nil {
				return reader.validationError(err)
			}
		}
	}
	if needs.context {
		if err := storeContextProjection(ctx, records, meta, &contextProjection); err != nil {
			return reader.validationError(err)
		}
	}
	if needs.model {
		refs, leaf, err := modelContextSourcesFromMetadata(ctx, reader, meta, metadata)
		if err != nil {
			return reader.validationError(err)
		}
		if err := putItemJSON(records, modelContextSourcesKey(thread), storedModelContextSources{Leaf: leaf, Sources: refs}); err != nil {
			return err
		}
	}

	var anchors, prefixes [][]byte
	if needs.membership {
		anchors = append(anchors, itemManifestKey(thread))
		for _, kind := range []string{"slot", "rank", "identity", "source", "range", "activity-source", "tool-call"} {
			prefixes = append(prefixes, backendDomainKey(thread, kind))
		}
	}
	if needs.window {
		anchors = append(anchors, windowSourcesKey(thread))
		prefixes = append(prefixes, backendDomainKey(thread, "window-turn"))
	}
	if needs.context {
		anchors = append(anchors, contextProjectionKey(thread))
	}
	if needs.model {
		anchors = append(anchors, modelContextSourcesKey(thread))
	}
	final := make([]spi.Record, 0, len(anchors))
	for _, key := range anchors {
		value, found := records.records[string(key)]
		if !found {
			return ErrAuthorityCorrupt
		}
		final = append(final, spi.Record{Key: key, Value: value})
		delete(records.records, string(key))
	}
	for key := range records.records {
		wanted := false
		for _, prefix := range prefixes {
			wanted = wanted || bytes.HasPrefix([]byte(key), prefix)
		}
		if !wanted {
			delete(records.records, key)
		}
	}
	update := func(write func(spi.WriteTx) error) error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			if _, err := verifyWindowPreparationOwner(ctx, tx, meta); err != nil {
				return err
			}
			return write(tx)
		})
	}
	if err := update(func(tx spi.WriteTx) error {
		for _, key := range anchors {
			if err := deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, prefix := range prefixes {
		for more := true; more; {
			if err := update(func(tx spi.WriteTx) error {
				page, err := tx.Scan(spi.ScanRequest{Namespace: itemMembershipNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128})
				if err != nil {
					return err
				}
				for _, record := range page.Records {
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := tx.Delete(itemMembershipNamespace, record.Key); err != nil {
						return err
					}
				}
				more = page.HasMore
				return nil
			}); err != nil {
				return err
			}
		}
	}
	keys := make([]string, 0, len(records.records))
	for key := range records.records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for first := 0; first < len(keys); {
		last, size := first, 0
		for last < len(keys) && last-first < 128 {
			next := len(keys[last]) + len(records.records[keys[last]])
			if last > first && size+next > backendThreadMigrationBatchBytes {
				break
			}
			size += next
			last++
		}
		if err := update(func(tx spi.WriteTx) error {
			batch := make([]spi.Record, 0, last-first)
			for _, key := range keys[first:last] {
				if err := ctx.Err(); err != nil {
					return err
				}
				batch = append(batch, spi.Record{Key: []byte(key), Value: records.records[key]})
			}
			return storagebridge.WriteRecords(tx, itemMembershipNamespace, batch)
		}); err != nil {
			return err
		}
		first = last
	}
	// Publish only once the same private projection catches the current tail.
	// Each transaction consumes one appended entry through the ordinary append
	// reducers. Unrelated appends never restart the already prepared prefix.
	private := map[string][]byte{}
	for _, record := range final {
		private[string(record.Key)] = record.Value
	}
	through := meta.LeafID
	for {
		published := false
		next := through
		var accepted map[string][]byte
		err := update(func(tx spi.WriteTx) error {
			current, err := verifyWindowPreparationOwner(ctx, tx, meta)
			if err != nil {
				return err
			}
			staged := &privateWindowPreparationTx{WriteTx: tx, anchors: map[string][]byte{}}
			for key, value := range private {
				staged.anchors[key] = bytes.Clone(value)
			}
			if through != current.LeafID {
				depth := int64(0)
				if through != "" {
					prior, err := activeLocatedEntry(ctx, tx, current, through)
					if err != nil {
						return errors.Join(spi.ErrConflict, err)
					}
					depth = prior.PathDepth
				}
				leaf, err := readEntryLocator(tx, thread, current.LeafID)
				if err != nil {
					return err
				}
				node, err := entryAncestor(ctx, tx, thread, leaf, depth+1)
				if err != nil {
					return err
				}
				entry, err := readLocatedEntry(tx, backendDomainV32Format, thread, node)
				if err != nil {
					return err
				}
				if entry.ParentID != through {
					return ErrAuthorityCorrupt
				}
				dependency := &canonicalMembershipAppend{}
				if needs.model {
					if err := appendModelContextSources(ctx, staged, entry, dependency); err != nil {
						return err
					}
				}
				if needs.context {
					if err := appendCanonicalContextProjection(ctx, staged, entry); err != nil {
						return err
					}
				}
				if needs.window {
					if err := appendWindowSources(ctx, staged, entry, dependency); err != nil {
						return err
					}
				}
				if needs.membership {
					if err := appendCanonicalItemMembership(ctx, staged, entry, dependency); err != nil {
						return err
					}
				}
				next = entry.ID
			}
			if next == current.LeafID {
				missing, err := missingWindowPreparation(staged, current, needs)
				if err != nil {
					return err
				}
				if missing != (windowPreparationNeeds{}) {
					return ErrAuthorityCorrupt
				}
				for _, key := range anchors {
					value, ok := staged.anchors[string(key)]
					if !ok || value == nil {
						return ErrAuthorityCorrupt
					}
					if err := tx.Put(itemMembershipNamespace, key, value); err != nil {
						return err
					}
				}
				published = true
			}
			accepted = staged.anchors
			return nil
		})
		if err != nil {
			return err
		}
		private, through = accepted, next
		if published {
			return nil
		}
	}

}

// Private readiness anchors never escape a catch-up transaction before every
// dependent projection reaches its canonical tail. Other metadata is staged
// in place and cannot be read without those anchors. Clone anchors per batch
// so write failure, cancellation or panic cannot advance private progress.
type privateWindowPreparationTx struct {
	spi.WriteTx
	anchors map[string][]byte
}

func (tx *privateWindowPreparationTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == itemMembershipNamespace {
		if value, ok := tx.anchors[string(key)]; ok {
			if value == nil {
				return nil, spi.ErrNotFound
			}
			return bytes.Clone(value), nil
		}
	}
	return tx.WriteTx.Get(namespace, key)
}
func (tx *privateWindowPreparationTx) Put(namespace string, key, value []byte) error {
	if namespace == itemMembershipNamespace {
		if _, ok := tx.anchors[string(key)]; ok {
			tx.anchors[string(key)] = bytes.Clone(value)
			return nil
		}
	}
	return tx.WriteTx.Put(namespace, key, value)
}
func (tx *privateWindowPreparationTx) Delete(namespace string, key []byte) error {
	if namespace == itemMembershipNamespace {
		if _, ok := tx.anchors[string(key)]; ok {
			tx.anchors[string(key)] = nil
			return nil
		}
	}
	return tx.WriteTx.Delete(namespace, key)
}
func verifyWindowPreparationOwner(ctx context.Context, tx spi.ReadTx, frozen ThreadMeta) (ThreadMeta, error) {
	current, err := readPublishedThreadMeta(tx, backendDomainV32Format, frozen.ID)
	if err != nil {
		return ThreadMeta{}, errors.Join(spi.ErrConflict, err)
	}
	owner := current
	owner.LeafID = frozen.LeafID
	owner.UpdatedAt = frozen.UpdatedAt
	if !reflect.DeepEqual(owner, frozen) {
		return ThreadMeta{}, spi.ErrConflict
	}
	if frozen.LeafID != "" {
		if _, err := activeLocatedEntry(ctx, tx, current, frozen.LeafID); err != nil {
			return ThreadMeta{}, errors.Join(spi.ErrConflict, err)
		}
	}
	return current, nil
}
