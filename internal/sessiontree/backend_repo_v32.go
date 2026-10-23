package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV32Namespace = "floret.domain.sessiontree.v17"
	backendDomainV32Version   = 32
)

var backendDomainV32Format = backendDomainFormat{
	label: "v32", namespace: backendDomainV32Namespace,
	envelope: "sessiontree-v32-record", version: backendDomainV32Version,
	scanLimit: 256, validate: validateBackendDomainV29Memory,
}

func readV32Manifest(tx spi.ReadTx) (backendDomainManifest, error) {
	record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
	if err != nil || !found {
		return backendDomainManifest{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var manifest backendDomainManifest
	if err := decodeBackendDomainValue(backendDomainV32Format, record, &manifest); err != nil || manifest.Version != backendDomainV32Version || manifest.Sequence < 0 || manifest.MigrationRoots < 0 || manifest.MigrationPhase != "verify" && manifest.MigrationRoots != 0 {
		return manifest, errors.Join(ErrAuthorityCorrupt, err)
	}
	switch manifest.SourceVersion {
	case 0:
		if manifest.MigrationPhase != "" || len(manifest.MigrationCursor) != 0 || manifest.MigrationTime != 0 {
			return manifest, ErrAuthorityCorrupt
		}
	case 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31:
		if manifest.SourceVersion < 27 && manifest.MigrationTime <= 0 || manifest.SourceVersion >= 27 && manifest.MigrationTime != 0 {
			return manifest, ErrAuthorityCorrupt
		}
		switch manifest.MigrationPhase {
		case "":
			if len(manifest.MigrationCursor) != 0 {
				return manifest, ErrAuthorityCorrupt
			}
		case "snapshot_cleanup":
			var sequence uint64
			if manifest.SourceVersion >= 6 || json.Unmarshal(manifest.MigrationCursor, &sequence) != nil || sequence == 0 {
				return manifest, ErrAuthorityCorrupt
			}
		case "publish", "index", "tombstones", "verify", "cleanup":
		default:
			return manifest, ErrAuthorityCorrupt
		}
	default:
		return manifest, ErrAuthorityCorrupt
	}
	return manifest, nil
}

// NewFreshCurrentBackendRepoInTransaction installs the current owner directly;
// fresh runtime stores never create a historical format to migrate on restart.
func NewFreshCurrentBackendRepoInTransaction(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time, parentEvents bool) (*BackendRepo, error) {
	fresh, err := IsFreshBackendRepoInTransaction(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !fresh {
		return nil, errors.Join(ErrAuthorityCorrupt, errors.New("fresh initialization encountered existing domain state"))
	}
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: backendDomainV32Version}); err != nil {
		return nil, err
	}
	return NewBackendRepoForFormat(backend, now, parentEvents, backendDomainV32Format)
}

func backendKeyRangeEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != 0xff {
			end[index]++
			return end[:index+1]
		}
	}
	return nil
}

func copyMigrationTombstone(ctx context.Context, tx spi.WriteTx, source, target backendDomainFormat, record backendDomainRecord) error {
	var value ThreadTombstone
	if err := decodeBackendDomainValue(source, record, &value); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := validateMemoryArtifactTombstone(value, record.ID); err != nil {
		return err
	}
	if _, found, err := readBackendDomainRecord(tx, source, backendDomainKey(backendDomainRecordThread, record.ID), backendDomainRecordThread, record.ID, "", 0); err != nil || found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if _, found, err := readBackendDomainRecord(tx, target, backendDomainKey(backendDomainRecordThread, record.ID), backendDomainRecordThread, record.ID, "", 0); err != nil || found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if previous, found, err := readBackendDomainRecord(tx, target, backendDomainKey(backendDomainRecordTombstone, record.ID), backendDomainRecordTombstone, record.ID, "", 0); err != nil {
		return err
	} else if found {
		var retained ThreadTombstone
		if err := decodeBackendDomainValue(target, previous, &retained); err != nil || retained != value {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	}
	return putBackendDomainRecordContext(ctx, tx, target, backendDomainKey(backendDomainRecordTombstone, record.ID), backendDomainRecordTombstone, record.ID, "", 0, value)
}

// Validate each retained source record before any cleanup begins. The cursor
// advances in bounded pages; publication already validated the thread closure.
func verifyMigrationSourceRecord(ctx context.Context, tx spi.ReadTx, source, target backendDomainFormat, row spi.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record, err := decodeBackendDomainRecord(source, row.Value)
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	wantKey := backendDomainKey(record.Kind, record.ID)
	threadID := record.ID
	switch record.Kind {
	case backendDomainRecordManifest:
		var manifest backendDomainManifest
		if source.version == 6 {
			var legacy backendDomainV6Manifest
			if err := decodeBackendDomainValue(source, record, &legacy); err != nil || legacy.Version != 6 || legacy.Sequence < 0 || legacy.EffectAttemptSequence < 0 {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			manifest.Version = legacy.Version
		} else if err := decodeBackendDomainValue(source, record, &manifest); err != nil || manifest.Version != source.version || manifest.SourceVersion != 0 || record.ID != "" {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		wantKey = backendDomainKey(record.Kind)
	case backendDomainRecordRootIndex:
		var roots backendDomainRootIndex
		if err := decodeBackendDomainValue(source, record, &roots); err != nil || roots.ThreadIDs == nil || record.ID != "" {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		seen := make(map[string]bool, len(roots.ThreadIDs))
		var previous ThreadMeta
		for _, id := range roots.ThreadIDs {
			if seen[id] || id == "" {
				return ErrAuthorityCorrupt
			}
			seen[id] = true
			metaRecord, found, err := readBackendDomainRecord(tx, source, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0)
			if err != nil || !found {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			var meta ThreadMeta
			if err := decodeBackendDomainValue(source, metaRecord, &meta); err != nil || meta.ID != id || meta.ParentThreadID != "" || ValidateThreadMetaAuthority(meta) != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if previous.ID != "" && (meta.CreatedAt.After(previous.CreatedAt) || meta.CreatedAt.Equal(previous.CreatedAt) && meta.ID <= previous.ID) {
				return ErrAuthorityCorrupt
			}
			previous = meta
		}
		wantKey = backendDomainKey(record.Kind)
	case backendDomainRecordEntry:
		var stored canonicalStoredEntry
		if source.version < 27 {
			var entry Entry
			if err := decodeBackendDomainValue(source, record, &entry); err != nil || entry.ID != record.ID || entry.ThreadID != record.ThreadID || record.Ordinal < 0 {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := ValidateEntryIntegrity(entry); err != nil {
				return err
			}
		} else {
			var err error
			stored, err = migrationStoredEntry(source, record)
			if err != nil {
				return err
			}
		}
		threadID, wantKey = record.ThreadID, backendDomainEntryKey(record.ThreadID, record.Ordinal)
		if _, deleted, err := readBackendDomainRecord(tx, target, backendDomainKey(backendDomainRecordTombstone, threadID), backendDomainRecordTombstone, threadID, "", 0); err != nil {
			return err
		} else if !deleted && source.version >= 27 {
			published, found, err := readBackendDomainRecord(tx, target, wantKey, record.Kind, record.ID, record.ThreadID, record.Ordinal)
			if err != nil || !found {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			current, err := decodeCanonicalStoredEntry(target, published)
			if err != nil {
				return err
			}
			if err := compareMigrationStoredEntry(source, stored, current); err != nil {
				return err
			}
		}
	case backendDomainRecordEffectAttempt:
		if source.version != 6 {
			return ErrAuthorityCorrupt
		}
		var attempt legacyEffectAttemptV6
		if err := decodeBackendDomainValue(source, record, &attempt); err != nil || attempt.EffectAttemptID != record.ID || record.ID == "" {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		threadID = attempt.Invocation.ThreadID
	case backendDomainRecordEffectInvocation:
		if source.version != 6 || record.ID == "" {
			return ErrAuthorityCorrupt
		}
		var attemptID string
		if err := decodeBackendDomainValue(source, record, &attemptID); err != nil || attemptID == "" {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		threadID = ""
	case backendDomainRecordThread:
		var meta ThreadMeta
		if err := decodeBackendDomainValue(source, record, &meta); err != nil || meta.ID != record.ID || ValidateThreadMetaAuthority(meta) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	case backendDomainRecordTombstone:
		var value, published ThreadTombstone
		if err := decodeBackendDomainValue(source, record, &value); err != nil || validateMemoryArtifactTombstone(value, record.ID) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		current, found, err := readBackendDomainRecord(tx, target, wantKey, record.Kind, record.ID, "", 0)
		if err != nil || !found {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := decodeBackendDomainValue(target, current, &published); err != nil || value != published {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		threadID = ""
	case backendDomainRecordTodo:
		var todo AgentTodoState
		if err := decodeBackendDomainValue(source, record, &todo); err != nil || todo.ThreadID != record.ID || ValidateAgentTodoItems(todo.Items) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	case backendDomainRecordProviderState:
		var state ProviderStateRecord
		if err := decodeBackendDomainValue(source, record, &state); err != nil || state.ThreadID != record.ID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	case backendDomainRecordArtifact:
		var value artifact.Record
		if err := decodeBackendDomainValue(source, record, &value); err != nil || artifactRecordKey(value.ThreadID, value.Ref.ID) != record.ID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		threadID = value.ThreadID
	default:
		return ErrAuthorityCorrupt
	}
	if !bytes.Equal(row.Key, wantKey) || record.Kind != backendDomainRecordEntry && (record.ThreadID != "" || record.Ordinal != 0) {
		return ErrAuthorityCorrupt
	}
	if threadID == "" {
		return nil
	}
	if _, found, err := readBackendDomainRecord(tx, source, backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0); err != nil || !found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if _, deleted, err := readBackendDomainRecord(tx, target, backendDomainKey(backendDomainRecordTombstone, threadID), backendDomainRecordTombstone, threadID, "", 0); err != nil {
		return err
	} else if deleted {
		return nil
	}
	if _, found, err := readBackendDomainRecord(tx, target, backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0); err != nil || !found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if record.Kind == backendDomainRecordArtifact {
		if _, found, err := readBackendDomainRecord(tx, target, wantKey, record.Kind, record.ID, "", 0); err != nil || !found {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	return nil
}

// A retained migration source and the published owner share immutable body
// blocks. Account for every owner, including partially reclaimed source pages.
func includeMigrationBodyReferences(ctx context.Context, tx spi.ReadTx, format backendDomainFormat, references map[string]canonicalBodyRecord) error {
	_, err := tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordManifest))
	if errors.Is(err, spi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	manifest, err := readV32Manifest(tx)
	if err != nil || manifest.SourceVersion == 0 {
		return err
	}
	other, err := migrationSourceFormat(manifest.SourceVersion)
	if err != nil {
		return err
	}
	if format.version == manifest.SourceVersion {
		other = backendDomainV32Format
	} else if format.version != backendDomainV32Version {
		return ErrAuthorityCorrupt
	}
	if other.version < backendDomainV30Version {
		return nil
	}
	prefix := backendDomainKey(backendDomainRecordEntry)
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: other.namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			record, err := decodeBackendDomainRecord(other, row.Value)
			if err != nil || record.Kind != backendDomainRecordEntry || !bytes.Equal(row.Key, backendDomainEntryKey(record.ThreadID, record.Ordinal)) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			stored, err := decodeCanonicalStoredEntry(other, record)
			if err != nil || stored.Entry.ID != record.ID || stored.Entry.ThreadID != record.ThreadID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			for _, ref := range stored.Fields {
				body := references[ref.SHA256]
				if body.References > 0 && body.canonicalBodyRef != ref {
					return ErrAuthorityCorrupt
				}
				body.canonicalBodyRef, body.References = ref, body.References+1
				references[ref.SHA256] = body
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
