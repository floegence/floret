//go:build floret_layout_cutover

package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/floegence/floret/v7/internal/session/artifact"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

type OfflineJournalProof struct {
	Entries int
	Digest  [32]byte
}

func RegisterOfflineThread(ctx context.Context, source spi.ReadTx, target spi.WriteTx, thread string) error {
	meta, err := readPublishedThreadMeta(source, backendDomainV32Format, thread)
	if err != nil {
		return err
	}
	if meta.ParentThreadID != "" {
		if _, err := readPublishedThreadMeta(storagebridge.CatalogReader(target), backendDomainV32Format, meta.ParentThreadID); err != nil {
			return errors.Join(ErrInvalidThreadAuthority, err)
		}
	}
	if err := updateChildMembership(target, thread, "", meta.ParentThreadID); err != nil {
		return err
	}
	key := backendDomainKey(backendDomainRecordThread, thread)
	metaRaw, err := source.Get(backendDomainV32Namespace, key)
	if err != nil {
		return err
	}
	if err := storagebridge.PutIndexedThreadRecord(target, storagebridge.CatalogRecord{Namespace: backendDomainV32Namespace, Key: key}, metaRaw); err != nil {
		return err
	}
	key = backendDomainKey(backendDomainRecordRootThread, thread)
	raw, err := source.Get(backendDomainV32Namespace, key)
	if meta.ParentThreadID != "" {
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err != nil {
		return err
	}
	record, err := decodeBackendDomainRecord(backendDomainV32Format, raw)
	var root backendDomainRootThread
	if err != nil || record.Kind != backendDomainRecordRootThread || record.ID != thread || decodeBackendDomainValue(backendDomainV32Format, record, &root) != nil || root.ThreadID != thread {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return storagebridge.PutIndexedThreadRecord(target, storagebridge.CatalogRecord{Namespace: backendDomainV32Namespace, Key: key}, raw)
}

func InitializeOfflineSequence(source spi.ReadTx, target spi.WriteTx) error {
	sourceManifest, err := readV32Manifest(source)
	if err != nil || sourceManifest.Version != backendDomainV43Version || sourceManifest.SourceVersion != 0 {
		return errors.Join(spi.ErrMigrationRequired, err)
	}
	destination, err := readV32Manifest(target)
	if err != nil || destination.Sequence != 0 || destination.SourceVersion != 0 || destination.Version != sourceManifest.Version {
		return errors.Join(spi.ErrConflict, err)
	}
	key := backendDomainKey(backendDomainRecordManifest)
	raw, err := source.Get(backendDomainV32Namespace, key)
	if err != nil {
		return err
	}
	return storagebridge.PutCatalogRecord(target, storagebridge.CatalogRecord{Namespace: backendDomainV32Namespace, Key: key}, raw)
}

func VerifyOfflineJournal(ctx context.Context, records spi.ReadTx, thread string) (OfflineJournalProof, error) {
	var result OfflineJournalProof
	meta, err := readPublishedThreadMeta(records, backendDomainV32Format, thread)
	if err != nil {
		return result, err
	}
	count, err := readJournalCount(records, thread)
	if err != nil {
		return result, err
	}
	hash := sha256.New()
	hash.Write([]byte("floret.offline.journal.v1\x00"))
	write := func(value []byte) {
		hash.Write(binary.BigEndian.AppendUint64(nil, uint64(len(value))))
		hash.Write(value)
	}
	key := backendDomainKey(backendDomainRecordThread, thread)
	raw, err := records.Get(backendDomainV32Namespace, key)
	if err != nil {
		return result, err
	}
	write(key)
	write(raw)
	seen := make(map[canonicalBodyRef]struct{})
	foundLeaf := false
	entries, err := visitStoredJournal(ctx, records, thread, func(row spi.Record, stored canonicalStoredEntry) error {
		write(row.Key)
		write(row.Value)
		if stored.Entry.ID == meta.LeafID {
			if foundLeaf {
				return ErrAuthorityCorrupt
			}
			foundLeaf = true
		}
		for _, ref := range stored.Fields {
			if _, ok := seen[ref]; !ok {
				if err := visitCanonicalBody(ctx, records, ref, func([]byte) error { return nil }); err != nil {
					return err
				}
				seen[ref] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if count != entries || (entries != 0) != foundLeaf || foundLeaf != (meta.LeafID != "") {
		return result, ErrAuthorityCorrupt
	}
	result.Entries = entries
	copy(result.Digest[:], hash.Sum(nil))
	return result, nil
}

// StageOfflineJournal moves the complete immutable journal, not just the active
// path. Its caller owns an empty, unpublished destination and the transaction.
// It does not publish a thread or claim to transfer prompts, effects' locators,
// query projections or environment-wide records.
func StageOfflineJournal(ctx context.Context, source spi.ReadTx, target spi.WriteTx, thread string) error {
	if ctx == nil || source == nil || target == nil || thread == "" {
		return spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manifest, err := readV32Manifest(source)
	if err != nil {
		return err
	}
	if manifest.Version != backendDomainV43Version || manifest.SourceVersion != 0 {
		return spi.ErrMigrationRequired
	}
	membership, err := readChildMembershipState(source)
	if err != nil {
		return err
	}
	if membership.Phase != "ready" {
		return spi.ErrMigrationRequired
	}
	meta, err := readPublishedThreadMeta(source, backendDomainV32Format, thread)
	if err != nil {
		return err
	}
	count, err := readJournalCount(source, thread)
	if err != nil {
		return err
	}
	if (count == 0) != (meta.LeafID == "") {
		return ErrAuthorityCorrupt
	}
	foundLeaf := false
	outputs := offlineOutputSet{}
	effects := map[string]string{}
	actual, err := visitStoredJournal(ctx, source, thread, func(row spi.Record, stored canonicalStoredEntry) error {
		if stored.Entry.ID == meta.LeafID {
			if foundLeaf {
				return ErrAuthorityCorrupt
			}
			foundLeaf = true
		}
		for _, ref := range stored.Fields {
			if err := stageOfflineBody(ctx, source, target, ref); err != nil {
				return err
			}
		}
		if err := outputs.collect(source, stored.Entry, func(_ artifact.Record, key []byte) error {
			raw, err := source.Get(backendDomainV32Namespace, key)
			if err != nil {
				return err
			}
			return target.Put(backendDomainV32Namespace, key, raw)
		}); err != nil {
			return err
		}
		if stored.Entry.Type == EntryEffectAttempt {
			attempt, err := decodeEffectAttempt(stored.Entry)
			if err != nil || attempt.Invocation.ThreadID != thread {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			effects[attempt.EffectAttemptID] = stored.Entry.ID
		}
		return target.Put(backendDomainV32Namespace, row.Key, row.Value)
	})
	if err != nil {
		return err
	}
	if actual != count || (meta.LeafID != "") != foundLeaf {
		return ErrAuthorityCorrupt
	}
	if err := outputs.stage(ctx, source, target); err != nil {
		return err
	}
	for attempt, entry := range effects {
		owner, err := readEffectSourceLocator(source, attempt)
		if err != nil || owner.ThreadID != thread || owner.EntryID != entry {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		local, err := source.Get(backendEntryIndexNamespace, backendDomainKey("effect", thread, attempt))
		if err != nil || string(local) != entry {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		key := backendDomainKey("effect_owner", attempt)
		raw, err := source.Get(backendEntryIndexNamespace, key)
		if err != nil {
			return err
		}
		if err := storagebridge.PutIndexedThreadRecord(target, storagebridge.CatalogRecord{Namespace: backendEntryIndexNamespace, Key: key}, raw); err != nil {
			return err
		}
	}
	key := backendDomainKey(backendDomainRecordThread, thread)
	raw, err := source.Get(backendDomainV32Namespace, key)
	if err != nil {
		return err
	}
	if err := target.Put(backendDomainV32Namespace, key, raw); err != nil {
		return err
	}
	if err := putJournalCount(target, thread, count); err != nil {
		return err
	}
	for _, kind := range []string{backendDomainRecordTodo, backendDomainRecordProviderState} {
		key := backendDomainKey(kind, thread)
		record, found, err := readBackendDomainRecord(source, backendDomainV32Format, key, kind, thread, "", 0)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		switch kind {
		case backendDomainRecordTodo:
			var todo AgentTodoState
			if err := decodeBackendDomainValue(backendDomainV32Format, record, &todo); err != nil || todo.ThreadID != thread || ValidateAgentTodoItems(todo.Items) != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		case backendDomainRecordProviderState:
			var state ProviderStateRecord
			if err := decodeBackendDomainValue(backendDomainV32Format, record, &state); err != nil || state.ThreadID != thread || validateProviderStateRecord(state) != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if _, err := readCanonicalEntryMetadata(source, thread, state.LeafEntryID); err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if err := copyOfflineOptional(source, target, backendDomainV32Namespace, key); err != nil {
			return err
		}
	}
	return stageOfflineDisplaySources(ctx, source, target, thread)
}

func stageOfflineBody(ctx context.Context, source spi.ReadTx, target spi.WriteTx, ref canonicalBodyRef) error {
	original, err := readCanonicalBodyRecord(source, ref)
	if err != nil {
		return err
	}
	key := canonicalBodyKey(ref.SHA256)
	if _, err := target.Get(canonicalBodyNamespace, key); err == nil {
		local, err := readCanonicalBodyRecord(target, ref)
		if err != nil {
			return err
		}
		if local.References >= original.References {
			return ErrAuthorityCorrupt
		}
		local.References++
		return writeCanonicalBodyRecord(target, local)
	} else if !errors.Is(err, spi.ErrNotFound) {
		return err
	}
	for ordinal := 0; ordinal <= (ref.Bytes-1)/canonicalBodyBlockBytes; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		blockKey := canonicalBodyBlockKey(ref.SHA256, ordinal)
		raw, err := source.Get(canonicalBodyNamespace, blockKey)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := target.Put(canonicalBodyNamespace, blockKey, raw); err != nil {
			return err
		}
	}
	if err := writeCanonicalBodyRecord(target, canonicalBodyRecord{canonicalBodyRef: ref, References: 1}); err != nil {
		return err
	}
	if err := copyOfflineOptional(source, target, bodyTextFactsNamespace, canonicalBodyKey(ref.SHA256)); err != nil {
		return err
	}
	if err := copyOfflinePrefix(ctx, source, target, activityFieldsNamespace, backendDomainKey(ref.SHA256)); err != nil {
		return err
	}
	return visitCanonicalBody(ctx, target, ref, func([]byte) error { return nil })
}
