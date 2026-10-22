package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/storage/spi"
)

const backendThreadMigrationNamespace = "floret.domain.sessiontree.migration.v32"
const backendThreadMigrationBatchBytes = 256 << 10

// Staged entries are immutable and retain their body ownership immediately.
// Only canonical thread metadata publishes them to runtime readers. A durable
// cursor resumes copying after interruption; it is never execution authority.
type threadMigrationCheckpoint struct {
	ThreadID     string   `json:"thread_id"`
	NextOrdinal  int      `json:"next_ordinal"`
	Complete     bool     `json:"complete"`
	Dependencies []string `json:"dependencies,omitempty"`
	BodyField    int      `json:"body_field,omitempty"`
	BodyBlocks   int      `json:"body_blocks,omitempty"`
	BodySHA256   string   `json:"body_sha256,omitempty"`
}

func readThreadMigrationCheckpoint(tx spi.ReadTx, threadID string) (threadMigrationCheckpoint, bool, error) {
	raw, err := tx.Get(backendThreadMigrationNamespace, []byte(threadID))
	if errors.Is(err, spi.ErrNotFound) {
		return threadMigrationCheckpoint{ThreadID: threadID}, false, nil
	}
	if err != nil {
		return threadMigrationCheckpoint{}, false, err
	}
	var checkpoint threadMigrationCheckpoint
	if err := decodeCanonicalBodyJSON(raw, &checkpoint); err != nil || checkpoint.ThreadID != threadID || checkpoint.NextOrdinal < 0 || checkpoint.BodyBlocks < 0 || checkpoint.BodyField < 0 || checkpoint.Complete && (checkpoint.BodyField != 0 || checkpoint.BodyBlocks != 0 || checkpoint.BodySHA256 != "") {
		return checkpoint, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	for i, id := range checkpoint.Dependencies {
		if id == "" || slices.Contains(checkpoint.Dependencies[:i], id) {
			return checkpoint, false, ErrAuthorityCorrupt
		}
	}
	return checkpoint, true, nil
}

func writeThreadMigrationCheckpoint(tx spi.WriteTx, checkpoint threadMigrationCheckpoint) error {
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return tx.Put(backendThreadMigrationNamespace, []byte(checkpoint.ThreadID), raw)
}

// stageSourceThread commits at most one journal page from the requested
// dependency closure. Cycles through parent-event references are legal; full
// source and target invariant validation still precedes atomic publication.
func (repo *BackendRepo) stageSourceThread(ctx context.Context, tx spi.WriteTx, threadID string) (bool, error) {
	source, target := *repo.migrationSource, repo.domainFormat()
	queue, seen := []string{threadID}, map[string]bool{}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		for _, kind := range []string{backendDomainRecordThread, backendDomainRecordTombstone} {
			if _, found, err := readBackendDomainRecord(tx, target, backendDomainKey(kind, id), kind, id, "", 0); err != nil {
				return false, err
			} else if found {
				seen[id] = false
				break
			}
		}
		if !seen[id] {
			continue
		}
		record, found, err := readBackendDomainRecord(tx, source, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0)
		if err != nil {
			return false, err
		}
		if !found {
			if tombstone, deleted, err := readBackendDomainRecord(tx, source, backendDomainKey(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0); err != nil {
				return false, err
			} else if deleted {
				if err := copyMigrationTombstone(ctx, tx, source, target, tombstone); err != nil {
					return false, err
				}
				continue
			}
			if id == threadID {
				return false, ErrThreadNotFound
			}
			return false, ErrAuthorityCorrupt
		}
		var meta ThreadMeta
		if err := decodeBackendDomainValue(source, record, &meta); err != nil || meta.ID != id || ValidateThreadMetaAuthority(meta) != nil {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		checkpoint, _, err := readThreadMigrationCheckpoint(tx, id)
		if err != nil {
			return false, err
		}
		if checkpoint.Complete {
			queue = append(queue, checkpoint.Dependencies...)
			continue
		}
		if meta.ParentThreadID != "" && !slices.Contains(checkpoint.Dependencies, meta.ParentThreadID) {
			checkpoint.Dependencies = append(checkpoint.Dependencies, meta.ParentThreadID)
		}
		prefix := backendDomainKey(backendDomainRecordEntry, id)
		var after []byte
		if checkpoint.NextOrdinal > 0 {
			after = backendDomainEntryKey(id, checkpoint.NextOrdinal-1)
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: source.namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return false, err
		}
		copiedBytes, copied := 0, 0
		for _, row := range page.Records {
			if copied > 0 && copiedBytes+len(row.Value) > backendThreadMigrationBatchBytes {
				break
			}
			entryRecord, err := decodeBackendDomainRecord(source, row.Value)
			if err != nil || entryRecord.ThreadID != id || entryRecord.Ordinal != checkpoint.NextOrdinal || !bytes.Equal(row.Key, backendDomainEntryKey(id, checkpoint.NextOrdinal)) {
				return false, errors.Join(ErrAuthorityCorrupt, err)
			}
			stored, err := migrationStoredEntry(source, entryRecord)
			if err != nil {
				return false, err
			}
			original := stored
			generated, err := canonicalStoredFields(stored.Entry, true)
			if err != nil {
				return false, err
			}
			if len(generated) > 0 {
				if copied > 0 {
					break
				}
				var complete bool
				stored, complete, err = stageMigrationBodies(ctx, tx, &checkpoint, stored)
				if err != nil {
					return false, err
				}
				if !complete {
					return false, writeThreadMigrationCheckpoint(tx, checkpoint)
				}
				copiedBytes = backendThreadMigrationBatchBytes
			} else if checkpoint.BodyField != 0 || checkpoint.BodyBlocks != 0 || checkpoint.BodySHA256 != "" {
				return false, ErrAuthorityCorrupt
			}

			if stored.Entry.Type == EntryParentEvent {
				event, err := decodeParentEvent(stored.Entry)
				if err != nil {
					return false, errors.Join(ErrAuthorityCorrupt, err)
				}
				if event.SourceThreadID != "" && !slices.Contains(checkpoint.Dependencies, event.SourceThreadID) {
					checkpoint.Dependencies = append(checkpoint.Dependencies, event.SourceThreadID)
				}
			}
			if _, err := tx.Get(target.namespace, row.Key); !errors.Is(err, spi.ErrNotFound) {
				return false, errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := retainMigrationFields(tx, checkpoint, original, stored); err != nil {
				return false, err
			}

			encoded, err := encodeBackendDomainRecord(target, entryRecord.Kind, entryRecord.ID, id, entryRecord.Ordinal, stored)
			if err != nil {
				return false, err
			}
			if err := tx.Put(target.namespace, row.Key, encoded); err != nil {
				return false, err
			}
			if err := putEntryLocator(ctx, tx, id, entryRecord.Ordinal, stored.Entry); err != nil {
				return false, err
			}
			if result := stored.Entry.Message.ToolResult; result != nil && result.FullOutput != nil {
				artifactID := artifactRecordKey(id, result.FullOutput.ID)
				key := backendDomainKey(backendDomainRecordArtifact, artifactID)
				record, found, err := readBackendDomainRecord(tx, source, key, backendDomainRecordArtifact, artifactID, "", 0)
				if err != nil || !found {
					return false, errors.Join(ErrAuthorityCorrupt, err)
				}
				if err := putBackendDomainRecordContext(ctx, tx, target, key, record.Kind, record.ID, "", 0, record.Value); err != nil {
					return false, err
				}
			}
			checkpoint.NextOrdinal++
			copied++
			copiedBytes += len(row.Value)
		}
		checkpoint.Complete = copied == len(page.Records) && !page.HasMore
		if checkpoint.Complete {
			for _, kind := range []string{backendDomainRecordTodo, backendDomainRecordProviderState} {
				key := backendDomainKey(kind, id)
				record, found, err := readBackendDomainRecord(tx, source, key, kind, id, "", 0)
				if err != nil {
					return false, err
				}
				if found {
					if err := putBackendDomainRecordContext(ctx, tx, target, key, kind, id, "", 0, record.Value); err != nil {
						return false, err
					}
				}
			}
		}
		if checkpoint.Complete && checkpoint.NextOrdinal == 0 {
			if err := putJournalCount(tx, id, 0); err != nil {
				return false, err
			}
		}
		if err := writeThreadMigrationCheckpoint(tx, checkpoint); err != nil {
			return false, err
		}
		if !checkpoint.Complete || len(checkpoint.Dependencies) != 0 {
			return false, nil
		}
		// An isolated small thread can publish in this transaction.
		return len(queue) == 0, nil
	}
	return true, nil
}

func stagedMigrationEntry(tx spi.ReadTx, format backendDomainFormat, record backendDomainRecord) (bool, error) {
	if format.version != backendDomainV32Version || record.Kind != backendDomainRecordEntry {
		return false, nil
	}
	if _, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordThread, record.ThreadID), backendDomainRecordThread, record.ThreadID, "", 0); err != nil || found {
		return false, err
	}
	checkpoint, found, err := readThreadMigrationCheckpoint(tx, record.ThreadID)
	if err != nil || !found || record.Ordinal >= checkpoint.NextOrdinal {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	return true, nil
}

func stagedMigrationSideRecord(tx spi.ReadTx, format backendDomainFormat, threadID string) (bool, error) {
	if format.version != backendDomainV32Version {
		return false, nil
	}
	if _, published, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0); err != nil || published {
		return false, err
	}
	_, staged, err := readThreadMigrationCheckpoint(tx, threadID)
	return staged, err
}
