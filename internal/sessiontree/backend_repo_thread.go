package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

const backendThreadEntryPageSize = 128
const backendVerifiedThreadCacheLimit = 256

func (repo *BackendRepo) threadMemoryForTransaction(ctx context.Context, tx spi.ReadTx, threadID string) (*MemoryRepo, error) {
	if repo.threadMemory == nil || repo.threadMemoryID != threadID {
		memory, err := loadBackendDomainThreadFormat(ctx, tx, repo.backend, repo.now, threadID, repo.parentEventsEnabled(), repo.domainFormat())
		if err != nil {
			return nil, err
		}
		repo.threadMemoryID, repo.threadMemory = threadID, memory
		return memory, nil
	}
	memory := repo.threadMemory
	queue := []string{threadID}
	seen := make(map[string]struct{}, 4)
	reload := false
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		currentID := queue[0]
		queue = queue[1:]
		if currentID == "" {
			continue
		}
		if _, duplicate := seen[currentID]; duplicate {
			continue
		}
		seen[currentID] = struct{}{}
		record, found, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordThread, currentID), backendDomainRecordThread, currentID, "", 0)
		if err != nil {
			return nil, err
		}
		if !found {
			// The full loader distinguishes a deleted thread from an absent one.
			loaded, loadErr := loadBackendDomainThreadFormat(ctx, tx, repo.backend, repo.now, threadID, repo.parentEventsEnabled(), repo.domainFormat())
			return loaded, loadErr
		}
		meta := ThreadMeta{}
		if err := decodeBackendDomainValue(repo.domainFormat(), record, &meta); err != nil || meta.ID != currentID {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := ValidateThreadMetaAuthority(meta); err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		previous, cached := memory.threads[currentID]
		if !cached || previous.ParentThreadID != meta.ParentThreadID || previous.ForkedFromThreadID != meta.ForkedFromThreadID || previous.LeafID != meta.LeafID {
			reload = true
		}
		memory.threads[currentID] = meta
		queue = append(queue, strings.TrimSpace(meta.ParentThreadID))
	}
	if reload {
		memory, err := loadBackendDomainThreadFormat(ctx, tx, repo.backend, repo.now, threadID, repo.parentEventsEnabled(), repo.domainFormat())
		if err != nil {
			return nil, err
		}
		repo.threadMemory = memory
		return memory, nil
	}
	manifestRecord, found, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
	if err != nil || !found {
		if err == nil {
			err = ErrAuthorityCorrupt
		}
		return nil, err
	}
	var manifest backendDomainManifest
	if err := decodeBackendDomainValue(repo.domainFormat(), manifestRecord, &manifest); err != nil || manifest.Version != repo.domainFormat().version {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	memory.seq = manifest.Sequence
	delete(memory.todos, threadID)
	delete(memory.tombstones, threadID)
	delete(memory.providerStates, threadID)
	if err := loadBackendDomainThreadSideRecords(tx, memory, threadID, repo.domainFormat()); err != nil {
		return nil, err
	}
	return memory, nil
}

// PrepareThread publishes one legacy thread and its explicit dependency
// closure into v32. The source namespace remains untouched until this
// transaction commits, so interruption or corruption never exposes a partial
// canonical journal.
func (repo *BackendRepo) PrepareThread(ctx context.Context, threadID string) error {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return errors.New("thread preparation requires repository, context, and thread id")
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.PrepareThread(ctx, threadID)
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.PrepareThread(ctx, threadID)
	}
	return repo.prepareThreadIfNeeded(ctx, threadID)
}

// HasPendingMigration reports whether this owner has a retained legacy source
// that can be drained incrementally in the background.
func (repo *BackendRepo) HasPendingMigration() bool {
	if repo == nil {
		return false
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.HasPendingMigration()
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.migrationSource != nil && repo.domainFormat().version >= backendDomainV32Version
}

// MigrateNextThread publishes one source thread or deletes one bounded cleanup
// page. The manifest cursor makes progress durable across restarts.
func (repo *BackendRepo) MigrateNextThread(ctx context.Context) (progressed bool, resultErr error) {
	if repo == nil || ctx == nil {
		return false, errors.New("background thread migration requires repository and context")
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.MigrateNextThread(ctx)
	}
	requestContext := ctx
	work, release, err := repo.migrationGate.acquire(ctx, false)
	if err != nil {
		return false, err
	}
	defer release()
	ctx = work
	defer func() {
		if errors.Is(resultErr, context.Canceled) && requestContext.Err() == nil {
			// Foreground preparation preempted this background batch.
			progressed, resultErr = true, nil
		}
	}()
	migrated, finished := false, false
	publishID, attemptedID := "", ""
	prepareIndexes, currentIndexes := false, false
	err = func() error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.migrationSource == nil {
			currentIndexes = true
			return nil
		}
		err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			manifest, err := readV32Manifest(tx)
			if err != nil {
				return err
			}
			source := *repo.migrationSource
			if manifest.SourceVersion != source.version {
				if manifest.SourceVersion == 0 {
					finished = true
					return nil
				}
				return errors.Join(ErrAuthorityCorrupt, errors.New("session-tree migration source version drifted"))
			}
			if manifest.MigrationPhase == "index" {
				prepareIndexes = true
				return nil
			}
			if manifest.MigrationPhase == "tombstones" {
				prefix := backendDomainKey(backendDomainRecordTombstone)
				page, err := tx.Scan(spi.ScanRequest{Namespace: source.namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: manifest.MigrationCursor, Limit: backendThreadEntryPageSize})
				if err != nil {
					return err
				}
				for _, row := range page.Records {
					record, err := decodeBackendDomainRecord(source, row.Value)
					if err != nil || record.Kind != backendDomainRecordTombstone || record.ID == "" || record.ThreadID != "" || record.Ordinal != 0 || !bytes.Equal(row.Key, backendDomainKey(record.Kind, record.ID)) {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					if err := copyMigrationTombstone(ctx, tx, source, repo.domainFormat(), record); err != nil {
						return err
					}
				}
				if page.HasMore {
					if len(page.Next) == 0 || bytes.Compare(page.Next, manifest.MigrationCursor) <= 0 {
						return ErrAuthorityCorrupt
					}
					manifest.MigrationCursor = bytes.Clone(page.Next)
				} else {
					manifest.MigrationPhase, manifest.MigrationCursor = "verify", nil
				}
				migrated = true
				return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
			}
			if manifest.MigrationPhase == "verify" {
				page, err := tx.Scan(spi.ScanRequest{Namespace: source.namespace, After: manifest.MigrationCursor, Limit: backendThreadEntryPageSize})
				if err != nil {
					return err
				}
				for _, row := range page.Records {
					if err := verifyMigrationSourceRecord(ctx, tx, source, repo.domainFormat(), row); err != nil {
						return err
					}
				}
				if page.HasMore {
					if len(page.Next) == 0 || bytes.Compare(page.Next, manifest.MigrationCursor) <= 0 {
						return ErrAuthorityCorrupt
					}
					manifest.MigrationCursor = bytes.Clone(page.Next)
				} else {
					manifest.MigrationPhase, manifest.MigrationCursor = "cleanup", nil
				}
				migrated = true
				return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
			}
			if manifest.MigrationPhase == "cleanup" {
				page, err := tx.Scan(spi.ScanRequest{Namespace: source.namespace, After: manifest.MigrationCursor, Limit: backendThreadEntryPageSize})
				if err != nil {
					return err
				}
				for _, stored := range page.Records {
					record, err := decodeBackendDomainRecord(source, stored.Value)
					if err != nil {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					if record.Kind == backendDomainRecordManifest {
						continue
					}
					if record.Kind == backendDomainRecordEntry && source.version >= backendDomainV30Version {
						if err := deleteCanonicalStoredEntry(ctx, tx, source, record.ThreadID, record.Ordinal); err != nil {
							return err
						}
						continue
					}
					if err := tx.Delete(source.namespace, stored.Key); err != nil {
						return err
					}
				}
				if page.HasMore {
					manifest.MigrationCursor = append(manifest.MigrationCursor[:0], page.Next...)
					migrated = true
				} else {
					if err := tx.Delete(source.namespace, backendDomainKey(backendDomainRecordManifest)); err != nil {
						return err
					}
					manifest.SourceVersion = 0
					manifest.MigrationPhase = ""
					manifest.MigrationCursor = nil
					finished = true
				}
				return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
			}
			prefix := backendDomainKey(backendDomainRecordThread)
			end := append([]byte(nil), prefix...)
			for index := len(end) - 1; index >= 0; index-- {
				if end[index] != 0xff {
					end[index]++
					end = end[:index+1]
					break
				}
			}
			page, err := tx.Scan(spi.ScanRequest{Namespace: source.namespace, Start: prefix, End: end, After: manifest.MigrationCursor, Limit: backendThreadEntryPageSize})
			if err != nil {
				return err
			}
			for _, stored := range page.Records {
				record, err := decodeBackendDomainRecord(source, stored.Value)
				if err != nil {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				if record.Kind != backendDomainRecordThread || record.ID == "" {
					return errors.Join(ErrAuthorityCorrupt, errors.New("legacy thread inventory record is malformed"))
				}
				if _, found, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordThread, record.ID), backendDomainRecordThread, record.ID, "", 0); err != nil {
					return err
				} else if found {
					continue
				}
				attemptedID = record.ID
				staged, err := repo.stageSourceThread(ctx, tx, record.ID)
				if err != nil {
					return err
				}
				if !staged {
					migrated = true
					return nil
				}
				publishID = record.ID
				migrated = true
				return nil
			}
			if page.HasMore {
				manifest.MigrationPhase = "publish"
				manifest.MigrationCursor = append(manifest.MigrationCursor[:0], page.Next...)
				migrated = true
				return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
			}
			manifest.MigrationPhase = "index"
			manifest.MigrationCursor = nil
			migrated = true
			return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
		})
		if err == nil && finished {
			repo.migrationSource = nil
			migrated = true
		}
		return err
	}()
	if currentIndexes {
		return repo.prepareNextQueryThread(ctx)
	}
	if err == nil && prepareIndexes {
		more, err := repo.prepareNextQueryThread(ctx)
		if err != nil || more {
			return more, err
		}
		err = repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			manifest, err := readV32Manifest(tx)
			if err != nil {
				return err
			}
			if manifest.MigrationPhase != "index" {
				return spi.ErrConflict
			}
			manifest.MigrationPhase, manifest.MigrationCursor = "tombstones", nil
			return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
		})
		return true, err
	}
	if err == nil && publishID != "" {
		err = repo.publishSourceThread(ctx, publishID)
		if errors.Is(err, spi.ErrConflict) {
			return true, nil
		}
		if err == nil {
			err = repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				manifest, err := readV32Manifest(tx)
				if err != nil {
					return err
				}
				manifest.MigrationPhase = "publish"
				manifest.MigrationCursor = backendDomainKey(backendDomainRecordThread, publishID)
				return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
			})
		}
	}
	if err != nil && attemptedID != "" && errors.Is(err, ErrAuthorityCorrupt) {
		if isolated := repo.isolateMigrationFailure(ctx, attemptedID, err); isolated != nil {
			return false, isolated
		}
		return true, nil
	}
	return migrated, err
}

func (repo *BackendRepo) prepareThreadIfNeeded(ctx context.Context, threadID string) error {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return errors.New("thread preparation requires repository, context and thread id")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ready, err := repo.prepareThreadBatch(ctx, threadID)
		if err != nil || ready {
			return err
		}
	}
}

// Release the repository and backend transaction between committed entry pages.
// An interrupted preparation resumes its immutable staged records on retry.
func (repo *BackendRepo) prepareThreadBatch(ctx context.Context, threadID string) (bool, error) {
	// Published threads do not wait behind another thread's validation work.
	var published bool
	err := func() error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.migrationSource == nil || repo.domainFormat().version < backendDomainV32Version {
			published = true
			return nil
		}
		return repo.backend.View(ctx, func(tx spi.ReadTx) error {
			_, found, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0)
			published = found
			return err
		})
	}()
	if err != nil || published {
		return published, err
	}
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return false, err
	}
	defer release()
	ctx = work
	var ready, staged bool
	err = func() error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.migrationSource == nil || repo.domainFormat().version < backendDomainV32Version {
			ready = true
			return nil
		}
		return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			_, found, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0)
			if err != nil || found {
				ready = found
				return err
			}
			staged, err = repo.stageSourceThread(ctx, tx, threadID)
			return err
		})
	}()
	if err != nil || ready || !staged {
		return ready, err
	}
	err = repo.publishSourceThread(ctx, threadID)
	if errors.Is(err, spi.ErrConflict) {
		return false, nil
	}
	return err == nil, err
}

func loadBackendDomainThread(ctx context.Context, tx spi.ReadTx, backend spi.Backend, now func() time.Time, threadID string, parentEvents bool) (*MemoryRepo, error) {
	return loadBackendDomainThreadFormat(ctx, tx, backend, now, threadID, parentEvents, backendDomainV31Format)
}

func loadBackendDomainThreadFormat(ctx context.Context, tx spi.ReadTx, backend spi.Backend, now func() time.Time, threadID string, parentEvents bool, format backendDomainFormat) (*MemoryRepo, error) {
	if ctx == nil || tx == nil || backend == nil || now == nil || strings.TrimSpace(threadID) == "" {
		return nil, errors.New("thread domain load requires context, transaction, backend, clock, and thread id")
	}
	memory := newMemoryRepo(now)
	memory.bodyBackend, memory.bodyTx, memory.bodyContext = backend, tx, ctx
	memory.parentEvents = parentEvents
	manifestRecord, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
	if err != nil || !found {
		if err == nil {
			err = ErrAuthorityCorrupt
		}
		return nil, err
	}
	var manifest backendDomainManifest
	if err := decodeBackendDomainValue(format, manifestRecord, &manifest); err != nil || manifest.Version != format.version {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	memory.seq = manifest.Sequence

	queue := []string{threadID}
	loadedEntries := make(map[string]struct{})
	loadedMetadata := make(map[string]struct{})
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		currentID := queue[0]
		queue = queue[1:]
		if currentID == "" {
			continue
		}
		if _, duplicate := memory.threads[currentID]; duplicate {
			if _, entriesLoaded := loadedEntries[currentID]; entriesLoaded {
				continue
			}
		}
		record, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordThread, currentID), backendDomainRecordThread, currentID, "", 0)
		if err != nil {
			return nil, err
		}
		if !found {
			if tombstone, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordTombstone, currentID), backendDomainRecordTombstone, currentID, "", 0); err != nil {
				return nil, err
			} else if found {
				value := ThreadTombstone{}
				if err := decodeBackendDomainValue(format, tombstone, &value); err != nil || validateMemoryArtifactTombstone(value, currentID) != nil {
					return nil, errors.Join(ErrAuthorityCorrupt, err)
				}
				memory.tombstones[currentID] = value
				if currentID != threadID {
					continue
				}
				return nil, ErrThreadDeleted
			}
			if currentID != threadID {
				return nil, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("thread %q has missing dependency %q", threadID, currentID))
			}
			return nil, ErrThreadNotFound
		}
		meta := ThreadMeta{}
		if err := decodeBackendDomainValue(format, record, &meta); err != nil || meta.ID != currentID {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := ValidateThreadMetaAuthority(meta); err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		memory.threads[currentID] = meta
		loadedMetadata[currentID] = struct{}{}
		if _, entriesLoaded := loadedEntries[currentID]; !entriesLoaded {
			if err := loadBackendDomainThreadEntries(ctx, tx, memory, currentID, format); err != nil {
				return nil, err
			}
			if err := loadBackendDomainThreadSideRecords(tx, memory, currentID, format); err != nil {
				return nil, err
			}
			loadedEntries[currentID] = struct{}{}
			for _, entry := range memory.entries[currentID] {
				if entry.Type != EntryParentEvent {
					continue
				}
				event, err := decodeParentEvent(entry)
				if err != nil {
					return nil, err
				}
				if sourceID := strings.TrimSpace(event.SourceThreadID); sourceID != "" {
					queue = append(queue, sourceID)
				}
			}
		}
		if _, metadataLoaded := loadedMetadata[currentID]; metadataLoaded {
			queue = append(queue, strings.TrimSpace(meta.ParentThreadID))
			delete(loadedMetadata, currentID)
		}
	}
	return memory, nil
}

func readBackendDomainRecord(tx spi.ReadTx, format backendDomainFormat, key []byte, kind, id, threadID string, ordinal int) (backendDomainRecord, bool, error) {
	encoded, err := tx.Get(format.namespace, key)
	if errors.Is(err, spi.ErrNotFound) {
		return backendDomainRecord{}, false, nil
	}
	if err != nil {
		return backendDomainRecord{}, false, err
	}
	record, err := decodeBackendDomainRecord(format, encoded)
	if err != nil {
		return backendDomainRecord{}, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if record.Kind != kind || record.ID != id || record.ThreadID != threadID || record.Ordinal != ordinal {
		return backendDomainRecord{}, false, errors.Join(ErrAuthorityCorrupt, errors.New("thread-scoped record identity mismatch"))
	}
	return record, true, nil
}

func loadBackendDomainThreadEntries(ctx context.Context, tx spi.ReadTx, memory *MemoryRepo, threadID string, format backendDomainFormat) error {
	prefix := backendDomainKey(backendDomainRecordEntry, threadID)
	end := bytes.Clone(prefix)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != 0xff {
			end[index]++
			end = end[:index+1]
			break
		}
	}
	var after []byte
	var entries []Entry
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: format.namespace, Start: prefix, End: end, After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return err
		}
		for _, storedRecord := range page.Records {
			if !bytes.HasPrefix(storedRecord.Key, prefix) {
				return errors.Join(ErrAuthorityCorrupt, errors.New("thread entry scan escaped its key range"))
			}
			record, err := decodeBackendDomainRecord(format, storedRecord.Value)
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			ordinal := len(entries)
			if record.Kind != backendDomainRecordEntry || record.ThreadID != threadID || record.Ordinal != ordinal || !bytes.Equal(storedRecord.Key, backendDomainEntryKey(threadID, ordinal)) {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("thread %q has a noncontiguous or malformed entry record", threadID))
			}
			stored, err := decodeCanonicalStoredEntry(format, record)
			if err != nil || stored.Entry.ID != record.ID || stored.Entry.ThreadID != threadID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			entry := stored.Entry
			entry.bodyFields = stored.Fields
			entries = append(entries, entry)
		}
		if !page.HasMore {
			break
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return errors.Join(ErrAuthorityCorrupt, errors.New("thread entry scan did not advance"))
		}
		after = bytes.Clone(page.Next)
	}
	if err := memory.replaceIndexedEntriesLocked(threadID, entries); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	meta := memory.threads[threadID]
	_, leafFound := memory.entryOrdinals[threadID][meta.LeafID]
	if (meta.LeafID == "") != (len(entries) == 0) || meta.LeafID != "" && !leafFound {
		return errors.Join(ErrAuthorityCorrupt, errors.New("thread leaf does not match its canonical journal"))
	}
	return nil
}

func loadBackendDomainThreadSideRecords(tx spi.ReadTx, memory *MemoryRepo, threadID string, format backendDomainFormat) error {
	for _, item := range []struct {
		kind string
		key  []byte
	}{
		{kind: backendDomainRecordTodo, key: backendDomainKey(backendDomainRecordTodo, threadID)},
		{kind: backendDomainRecordTombstone, key: backendDomainKey(backendDomainRecordTombstone, threadID)},
		{kind: backendDomainRecordProviderState, key: backendDomainKey(backendDomainRecordProviderState, threadID)},
	} {
		record, found, err := readBackendDomainRecord(tx, format, item.key, item.kind, threadID, "", 0)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		switch item.kind {
		case backendDomainRecordTodo:
			var value AgentTodoState
			if err := decodeBackendDomainValue(format, record, &value); err != nil || value.ThreadID != threadID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			memory.todos[threadID] = value
		case backendDomainRecordTombstone:
			var value ThreadTombstone
			if err := decodeBackendDomainValue(format, record, &value); err != nil || value.ThreadID != threadID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			memory.tombstones[threadID] = value
		case backendDomainRecordProviderState:
			var value ProviderStateRecord
			if err := decodeBackendDomainValue(format, record, &value); err != nil || value.ThreadID != threadID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			memory.providerStates[threadID] = value
		}
	}
	for _, entry := range memory.entries[threadID] {
		if entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil {
			continue
		}
		key := artifactRecordKey(threadID, entry.Message.ToolResult.FullOutput.ID)
		if _, loaded := memory.artifacts[key]; loaded {
			continue
		}
		record, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0)
		if err != nil || !found {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		var value artifact.Record
		if err := decodeBackendDomainValue(format, record, &value); err != nil || artifactRecordKey(value.ThreadID, value.Ref.ID) != key {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := memory.validateArtifactRecordLocked(value); err != nil {
			return err
		}
		memory.artifacts[key] = value
	}

	return nil
}

func installBackendDomainThread(destination, source *MemoryRepo, threadID string) {
	for id, meta := range source.threads {
		destination.threads[id] = meta
		journal := readDomainJournal(source, id)
		journal.install(destination, id)
		delete(destination.todos, id)
		delete(destination.tombstones, id)
		delete(destination.providerStates, id)
		if value, found := source.todos[id]; found {
			destination.todos[id] = value
		}
		if value, found := source.tombstones[id]; found {
			destination.tombstones[id] = value
		}
		if value, found := source.providerStates[id]; found {
			destination.providerStates[id] = value
		}
	}
	for key, record := range destination.artifacts {
		if _, loaded := source.threads[record.ThreadID]; loaded {
			delete(destination.artifacts, key)
		}
	}
	for key, record := range source.artifacts {
		destination.artifacts[key] = record
	}
	destination.seq = max(destination.seq, source.seq)
}

func loadBackendDomainThreadInventory(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, error) {
	return loadBackendDomainThreadInventoryFormat(ctx, tx, now, backendDomainV31Format)
}

func loadBackendDomainThreadInventoryFormat(ctx context.Context, tx spi.ReadTx, now func() time.Time, format backendDomainFormat) (*MemoryRepo, error) {
	manifestRecord, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
	if err != nil || !found {
		if err == nil {
			err = ErrAuthorityCorrupt
		}
		return nil, err
	}
	var manifest backendDomainManifest
	if err := decodeBackendDomainValue(format, manifestRecord, &manifest); err != nil || manifest.Version != format.version {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	memory := newMemoryRepo(now)
	memory.seq = manifest.Sequence
	prefix := backendDomainKey(backendDomainRecordThread)
	end := bytes.Clone(prefix)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != 0xff {
			end[index]++
			end = end[:index+1]
			break
		}
	}
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: format.namespace, Start: prefix, End: end, After: after, Limit: 256})
		if err != nil {
			return nil, err
		}
		for _, stored := range page.Records {
			if !bytes.HasPrefix(stored.Key, prefix) {
				return nil, errors.Join(ErrAuthorityCorrupt, errors.New("thread inventory scan escaped its key range"))
			}
			record, err := decodeBackendDomainRecord(format, stored.Value)
			if err != nil || record.Kind != backendDomainRecordThread || record.ID == "" || record.ThreadID != "" || record.Ordinal != 0 || !bytes.Equal(stored.Key, backendDomainKey(backendDomainRecordThread, record.ID)) {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			var meta ThreadMeta
			if err := decodeBackendDomainValue(format, record, &meta); err != nil || meta.ID != record.ID {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := ValidateThreadMetaAuthority(meta); err != nil {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			if _, duplicate := memory.threads[meta.ID]; duplicate {
				return nil, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate thread inventory identity"))
			}
			memory.threads[meta.ID] = meta
		}
		if !page.HasMore {
			break
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("thread inventory scan did not advance"))
		}
		after = bytes.Clone(page.Next)
	}
	if err := ValidateThreadAuthorityGraph(values(memory.threads)); err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if format.version < backendDomainV32Version {
		rootRecord, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0)
		if err != nil || !found {
			if err == nil {
				err = ErrAuthorityCorrupt
			}
			return nil, err
		}
		var rootIndex backendDomainRootIndex
		if err := decodeBackendDomainValue(format, rootRecord, &rootIndex); err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		if rootIndex.ThreadIDs == nil || !slices.Equal(rootIndex.ThreadIDs, backendDomainRootThreadIDs(memory)) {
			return nil, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("root inventory differs from canonical metadata: persisted %v, expected %v", rootIndex.ThreadIDs, backendDomainRootThreadIDs(memory)))
		}
	} else {
		for _, id := range backendDomainRootThreadIDs(memory) {
			if _, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordRootThread, id), backendDomainRecordRootThread, id, "", 0); err != nil || !found {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	}
	return memory, nil
}

func (repo *BackendRepo) updateNewThread(ctx context.Context, inputThreadID, parentThreadID string, mutate func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil || mutate == nil {
		return errors.New("new thread mutation requires repository, context, and callback")
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.updateNewThread(ctx, inputThreadID, parentThreadID, mutate)
	}
	if strings.TrimSpace(inputThreadID) == "" || strings.TrimSpace(parentThreadID) != "" {
		if strings.TrimSpace(parentThreadID) != "" && repo.threadScoped {
			if err := repo.updateThread(ctx, parentThreadID, mutate); err != nil {
				return fmt.Errorf("create child under %q: %w", parentThreadID, err)
			}
			return nil
		}
		return repo.update(ctx, mutate)
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.updateNewThread(ctx, inputThreadID, parentThreadID, mutate)
	}
	if !repo.threadScoped {
		return repo.update(ctx, mutate)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	var committed *backendDomainMutation
	var memory *MemoryRepo
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		format := repo.domainFormat()
		manifestRecord, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
		if err != nil || !found {
			if err == nil {
				err = ErrAuthorityCorrupt
			}
			return err
		}
		var manifest backendDomainManifest
		if err := decodeBackendDomainValue(format, manifestRecord, &manifest); err != nil || manifest.Version != format.version {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if _, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordThread, inputThreadID), backendDomainRecordThread, inputThreadID, "", 0); err != nil {
			return err
		} else if found {
			return ErrThreadExists
		}
		if repo.migrationSource != nil {
			for _, kind := range []string{backendDomainRecordThread, backendDomainRecordTombstone} {
				if _, found, err := readBackendDomainRecord(tx, *repo.migrationSource, backendDomainKey(kind, inputThreadID), kind, inputThreadID, "", 0); err != nil {
					return err
				} else if found {
					if kind == backendDomainRecordTombstone {
						return ErrThreadDeleted
					}
					return ErrThreadExists
				}
			}
		}
		memory = newMemoryRepo(repo.now)
		memory.seq = manifest.Sequence
		memory.parentEvents = repo.parentEvents
		memory.querySnapshots = repo.threadQuerySnapshots
		memory.bodyBackend, memory.bodyTx, memory.bodyContext = repo.backend, tx, ctx

		tombstone, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordTombstone, inputThreadID), backendDomainRecordTombstone, inputThreadID, "", 0)
		if err != nil {
			return err
		}
		if found {
			var value ThreadTombstone
			if err := decodeBackendDomainValue(format, tombstone, &value); err != nil || value.ThreadID != inputThreadID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			memory.tombstones[inputThreadID] = value
		}
		mutation := newBackendDomainMutation(memory)
		mutation.format = format
		defer mutation.install(memory)
		if err := mutate(mutation.memory); err != nil {
			return err
		}
		if err := mutation.validate(); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := mutation.persist(ctx, tx); err != nil {
			return err
		}
		if err := mutation.persistQuery(ctx, tx, repo.queryThreads); err != nil {
			return err
		}
		committed = mutation.snapshot()
		return nil
	})
	if err != nil {
		return err
	}
	committed.install(memory)
	repo.threadQuerySnapshots = memory.querySnapshots
	repo.installQueryThreads(committed.queryThreads)
	for id := range committed.threads {
		if meta, found := memory.threads[id]; found {
			repo.rememberVerifiedThread(id, meta.LeafID)
		}
	}
	for id := range committed.journals {
		if meta, found := memory.threads[id]; found {
			repo.rememberVerifiedThread(id, meta.LeafID)
		}
	}
	return nil
}

func (repo *BackendRepo) threadMetadataView(ctx context.Context, read func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil || read == nil {
		return errors.New("thread metadata view requires repository, context, and callback")
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.threadMetadataView(ctx, read)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.domainMemory != nil {
		return read(repo.domainMemory)
	}
	if !repo.threadScoped {
		return ErrAuthorityCorrupt
	}
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
		memory, err := loadBackendDomainThreadInventoryFormat(ctx, tx, repo.now, repo.domainFormat())
		if err != nil {
			return err
		}
		if repo.migrationSource != nil {
			legacy, err := loadBackendDomainMigrationInventory(ctx, tx, repo.now, *repo.migrationSource)
			if err != nil {
				return err
			}
			for id, meta := range legacy.threads {
				if _, published := memory.threads[id]; published {
					continue
				}
				if _, deleted, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0); err != nil {
					return err
				} else if !deleted {
					memory.threads[id] = meta
				}
			}
		}
		return read(memory)
	})
}

// loadBackendDomainMigrationInventory exposes source metadata while v32 is
// still being published. It intentionally does not require the legacy global
// root index: that index is not a per-thread authority and may be unavailable
// while independent published threads remain usable.
func loadBackendDomainMigrationInventory(ctx context.Context, tx spi.ReadTx, now func() time.Time, format backendDomainFormat) (*MemoryRepo, error) {
	memory := newMemoryRepo(now)
	prefix := backendDomainKey(backendDomainRecordThread)
	end := bytes.Clone(prefix)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != 0xff {
			end[index]++
			end = end[:index+1]
			break
		}
	}
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: format.namespace, Start: prefix, End: end, After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return nil, err
		}
		for _, stored := range page.Records {
			record, err := decodeBackendDomainRecord(format, stored.Value)
			if err != nil || record.Kind != backendDomainRecordThread || record.ID == "" {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			var meta ThreadMeta
			if err := decodeBackendDomainValue(format, record, &meta); err != nil || meta.ID != record.ID {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := ValidateThreadMetaAuthority(meta); err != nil {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			memory.threads[meta.ID] = meta
		}
		if !page.HasMore {
			return memory, nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("migration inventory scan did not advance"))
		}
		after = bytes.Clone(page.Next)
	}
}

func (repo *BackendRepo) ListThreads(ctx context.Context, options ListThreadsOptions) ([]ThreadMeta, error) {
	var result []ThreadMeta
	err := repo.canonicalMetadataView(ctx, func(tx spi.ReadTx) error {
		return repo.visitThreadMetadata(ctx, tx, func(meta ThreadMeta) error {
			if meta.Archived && !options.IncludeArchived || options.RootOnly && strings.TrimSpace(meta.ParentThreadID) != "" || !threadAfterListCursor(meta, options) {
				return nil
			}
			result = append(result, meta)
			// Keep only the requested page while scanning an unordered backend.
			if options.Limit > 0 && len(result) > options.Limit {
				result = ApplyThreadListOptions(result, options)
			}
			return nil
		})
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ListThreads(ctx, options)
		return err
	})
	if err != nil {
		return nil, err
	}
	return ApplyThreadListOptions(result, options), nil
}

func (repo *BackendRepo) PendingAutomaticThreadTitles(ctx context.Context) ([]ThreadMeta, error) {
	var result []ThreadMeta
	err := repo.canonicalMetadataView(ctx, func(tx spi.ReadTx) error {
		return repo.visitThreadMetadata(ctx, tx, func(meta ThreadMeta) error {
			if meta.TitleStatus == ThreadTitlePending {
				result = append(result, meta)
			}
			return nil
		})
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.PendingAutomaticThreadTitles(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(result, func(left, right ThreadMeta) int {
		if order := left.TitleUpdatedAt.Compare(right.TitleUpdatedAt); order != 0 {
			return order
		}
		return strings.Compare(left.ID, right.ID)
	})
	return result, nil
}

func (repo *BackendRepo) canonicalMetadataView(ctx context.Context, read func(spi.ReadTx) error, legacy func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.canonicalMetadataView(ctx, read, legacy)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return repo.threadMetadataView(ctx, legacy)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.View(ctx, read)
}

func (repo *BackendRepo) visitThreadMetadata(ctx context.Context, tx spi.ReadTx, visit func(ThreadMeta) error) error {
	// A deferred facade must use its installed owner's migration source.
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.visitThreadMetadata(ctx, tx, visit)
	}
	formats := []backendDomainFormat{repo.domainFormat()}
	if repo.migrationSource != nil {
		formats = append(formats, *repo.migrationSource)
	}
	for index, format := range formats {
		prefix := backendDomainKey(backendDomainRecordThread)
		var after []byte
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			page, err := tx.Scan(spi.ScanRequest{Namespace: format.namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
			if err != nil {
				return err
			}
			for _, row := range page.Records {
				record, err := decodeBackendDomainRecord(format, row.Value)
				if err != nil || record.Kind != backendDomainRecordThread || record.ID == "" || record.ThreadID != "" || record.Ordinal != 0 || !bytes.Equal(row.Key, backendDomainKey(backendDomainRecordThread, record.ID)) {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				if index > 0 {
					shadowed := false
					for _, kind := range []string{backendDomainRecordThread, backendDomainRecordTombstone} {
						_, found, err := readBackendDomainRecord(tx, formats[0], backendDomainKey(kind, record.ID), kind, record.ID, "", 0)
						if err != nil {
							return err
						}
						shadowed = shadowed || found
					}
					if shadowed {
						continue
					}
				}
				var meta ThreadMeta
				if err := decodeBackendDomainValue(format, record, &meta); err != nil || meta.ID != record.ID || ValidateThreadMetaAuthority(meta) != nil {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				if index == 0 {
					if meta.ParentThreadID == "" {
						root, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordRootThread, meta.ID), backendDomainRecordRootThread, meta.ID, "", 0)
						if err != nil || !found {
							return errors.Join(ErrAuthorityCorrupt, err)
						}
						var value backendDomainRootThread
						if err := decodeBackendDomainValue(format, root, &value); err != nil || value.ThreadID != meta.ID {
							return errors.Join(ErrAuthorityCorrupt, err)
						}
					}
					seen := map[string]bool{meta.ID: true}
					for parentID := meta.ParentThreadID; parentID != ""; {
						if seen[parentID] {
							return ErrAuthorityCorrupt
						}
						seen[parentID] = true
						parent, err := readPublishedThreadMeta(tx, format, parentID)
						if err != nil {
							return errors.Join(ErrAuthorityCorrupt, err)
						}
						parentID = parent.ParentThreadID
					}
				}
				if err := visit(meta); err != nil {
					return err
				}
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

// ThreadOrigin resolves the idempotency key without hydrating the complete
// session tree. During v31 -> v32 migration the v32 namespace is authoritative
// for published threads; the retained source namespace is consulted only when
// the key has not been published yet.
func (repo *BackendRepo) ThreadOrigin(ctx context.Context, requestKey string) (ThreadOrigin, error) {
	requestKey = strings.TrimSpace(requestKey)
	if repo == nil || ctx == nil || requestKey == "" {
		return ThreadOrigin{}, errors.New("thread origin requires repository, context, and request key")
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return ThreadOrigin{}, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ThreadOrigin(ctx, requestKey)
	}
	if repo.domainMemory != nil {
		return repo.domainMemory.ThreadOrigin(ctx, requestKey)
	}
	if !repo.threadScoped {
		var origin ThreadOrigin
		err := repo.view(ctx, func(memory *MemoryRepo) error {
			var err error
			origin, err = memory.ThreadOrigin(ctx, requestKey)
			return err
		})
		return origin, err
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	var origin ThreadOrigin
	var found bool
	err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		var err error
		origin, found, err = scanThreadOrigin(ctx, tx, repo.domainFormat(), requestKey)
		if err != nil || found || repo.migrationSource == nil {
			return err
		}
		origin, found, err = scanThreadOrigin(ctx, tx, *repo.migrationSource, requestKey)
		return err
	})
	if err != nil {
		return ThreadOrigin{}, err
	}
	if found {
		return origin, nil
	}
	return ThreadOrigin{}, ErrThreadNotFound
}

func scanThreadOrigin(ctx context.Context, tx spi.ReadTx, format backendDomainFormat, requestKey string) (ThreadOrigin, bool, error) {
	var result ThreadOrigin
	var after []byte
	scan := func(kind string, visit func(backendDomainRecord) error) error {
		prefix := backendDomainKey(kind)
		end := bytes.Clone(prefix)
		for index := len(end) - 1; index >= 0; index-- {
			if end[index] != 0xff {
				end[index]++
				end = end[:index+1]
				break
			}
		}
		after = nil
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			page, err := tx.Scan(spi.ScanRequest{Namespace: format.namespace, Start: prefix, End: end, After: after, Limit: backendThreadEntryPageSize})
			if err != nil {
				return err
			}
			for _, stored := range page.Records {
				record, err := decodeBackendDomainRecord(format, stored.Value)
				if err != nil {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				if err := visit(record); err != nil {
					return err
				}
			}
			if !page.HasMore {
				return nil
			}
			if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
				return errors.Join(ErrAuthorityCorrupt, errors.New("thread origin scan did not advance"))
			}
			after = bytes.Clone(page.Next)
		}
	}
	if err := scan(backendDomainRecordThread, func(record backendDomainRecord) error {
		if record.Kind != backendDomainRecordThread || record.ID == "" {
			return errors.Join(ErrAuthorityCorrupt, errors.New("malformed thread origin record"))
		}
		var meta ThreadMeta
		if err := decodeBackendDomainValue(format, record, &meta); err != nil || meta.ID != record.ID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if meta.OriginRequestKey != requestKey {
			return nil
		}
		if result.Thread != nil || result.Tombstone != nil {
			return ErrAuthorityCorrupt
		}
		copy := meta
		result.Thread = &copy
		return nil
	}); err != nil {
		return ThreadOrigin{}, false, err
	}
	if err := scan(backendDomainRecordTombstone, func(record backendDomainRecord) error {
		if record.Kind != backendDomainRecordTombstone || record.ID == "" {
			return errors.Join(ErrAuthorityCorrupt, errors.New("malformed tombstone origin record"))
		}
		var tombstone ThreadTombstone
		if err := decodeBackendDomainValue(format, record, &tombstone); err != nil || tombstone.ThreadID != record.ID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if tombstone.OriginRequestKey != requestKey {
			return nil
		}
		if result.Thread != nil || result.Tombstone != nil {
			return ErrAuthorityCorrupt
		}
		copy := tombstone
		result.Tombstone = &copy
		return nil
	}); err != nil {
		return ThreadOrigin{}, false, err
	}
	return result, result.Thread != nil || result.Tombstone != nil, nil
}

func (repo *BackendRepo) verifyThreadView(ctx context.Context, tx spi.ReadTx, memory *MemoryRepo, threadID string) error {
	meta := memory.threads[threadID]
	if leaf, verified := repo.verifiedThreadLeaves[threadID]; verified && leaf == meta.LeafID {
		return nil
	}
	err := repo.domainFormat().validate(memory)
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if repo.domainFormat().version >= backendDomainV32Version {
		if err := verifyThreadEntryLocators(ctx, tx, memory); err != nil {
			return err
		}
	}
	repo.rememberVerifiedThread(threadID, meta.LeafID)
	return nil
}

func (repo *BackendRepo) rememberVerifiedThread(threadID, leaf string) {
	if repo.verifiedThreadLeaves == nil {
		repo.verifiedThreadLeaves = make(map[string]string)
	}
	if _, exists := repo.verifiedThreadLeaves[threadID]; !exists && len(repo.verifiedThreadLeaves) >= backendVerifiedThreadCacheLimit {
		for existing := range repo.verifiedThreadLeaves {
			delete(repo.verifiedThreadLeaves, existing)
			break
		}
	}
	repo.verifiedThreadLeaves[threadID] = leaf
}

func (repo *BackendRepo) updateThreadInTransaction(ctx context.Context, threadID string, mutate func(*MemoryRepo) error) error {
	var committed *backendDomainMutation
	var memory *MemoryRepo
	var observed []string
	defer func() {
		if memory != nil {
			memory.bodyTx, memory.bodyContext = nil, nil
		}
	}()
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		var err error
		memory, err = repo.threadMemoryForTransaction(ctx, tx, threadID)
		if err != nil {
			return fmt.Errorf("load thread mutation %q: %w", threadID, err)
		}
		memory.querySnapshots = repo.threadQuerySnapshots
		memory.parentEvents = repo.parentEventsEnabled()
		memory.bodyTx, memory.bodyContext = tx, ctx
		mutation := newBackendDomainMutation(memory)
		mutation.format = repo.domainFormat()
		defer mutation.install(memory)
		if err := repo.verifyThreadView(ctx, tx, memory, threadID); err != nil {
			return fmt.Errorf("verify thread mutation %q: %w", threadID, err)
		}
		if err := mutate(mutation.memory); err != nil {
			return err
		}
		mutation.appendParentEvents()
		observed = mutation.observedParentEventThreads()
		if err := mutation.validate(); err != nil {
			return fmt.Errorf("validate thread mutation %q: %w", threadID, errors.Join(ErrAuthorityCorrupt, err))
		}
		if err := mutation.persist(ctx, tx); err != nil {
			return err
		}
		if err := mutation.persistQuery(ctx, tx, repo.queryThreads); err != nil {
			return err
		}
		for threadID, previous := range mutation.journals {
			entries := mutation.memory.entries[threadID]
			for ordinal := len(previous.entries); ordinal < len(entries); ordinal++ {
				entries[ordinal] = compactCanonicalEntry(entries[ordinal])
			}
		}
		committed = mutation.snapshot()
		return nil
	})
	if err != nil {
		return err
	}
	committed.install(memory)
	repo.threadMemoryID, repo.threadMemory = threadID, memory
	repo.threadQuerySnapshots = memory.querySnapshots
	repo.installQueryThreads(committed.queryThreads)
	for id := range committed.threads {
		if meta, found := memory.threads[id]; found {
			repo.rememberVerifiedThread(id, meta.LeafID)
		} else {
			delete(repo.verifiedThreadLeaves, id)
		}
	}
	for id := range committed.journals {
		if meta, found := memory.threads[id]; found {
			repo.rememberVerifiedThread(id, meta.LeafID)
		}
	}
	observer := repo.parentEventObserver
	if observer != nil && len(observed) > 0 {
		go observer(observed)
	}
	return nil
}

// ThreadPrepared is a status read. It neither imports source records nor reads
// journal bodies. Validation is performed by the normal thread preparation gate.
func (repo *BackendRepo) ThreadPrepared(ctx context.Context, threadID string) (bool, error) {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return false, errors.New("thread readiness requires repository, context and thread id")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ThreadPrepared(ctx, threadID)
	}
	if repo.waitForReady != nil {
		return false, nil
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	var ready bool
	err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		if err := readThreadMigrationFailure(tx, threadID); err != nil {
			return err
		}
		record, found, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0)
		if err != nil {
			return err
		}
		if !found {
			return readThreadMigrationFailure(tx, threadID)
		}
		var meta ThreadMeta
		if err := decodeBackendDomainValue(repo.domainFormat(), record, &meta); err != nil || meta.ID != threadID || ValidateThreadMetaAuthority(meta) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		leaf, verified := repo.verifiedThreadLeaves[threadID]
		ready = verified && leaf == meta.LeafID
		if repo.domainFormat().version >= backendDomainV32Version {
			// Published metadata and its complete index are durable readiness
			// authority. An optional validation cache cannot revoke readiness
			// after eviction or restart. Validate only these bounded anchors.
			count, err := readJournalCount(tx, threadID)
			if err != nil || (count == 0) != (meta.LeafID == "") {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if meta.LeafID != "" {
				locator, err := readEntryLocator(tx, threadID, meta.LeafID)
				if err != nil || locator.Ordinal >= count {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				if _, err := readLocatedEntry(tx, repo.domainFormat(), threadID, locator); err != nil {
					return err
				}
			}
			ready = true
		}
		if ready {
			raw, err := tx.Get(queryManifestNamespace, []byte(threadID))
			if errors.Is(err, spi.ErrNotFound) {
				ready = false
				return nil
			}
			if err != nil {
				return err
			}
			var index queryManifest
			ready = json.Unmarshal(raw, &index) == nil && index.Version == 3 && index.Leaf == meta.LeafID
		}
		return nil
	})
	return ready, err
}
