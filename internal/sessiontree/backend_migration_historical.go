package sessiontree

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

// Historical transforms may use the released in-memory algorithms, but only
// for the requested source closure. This state never becomes a runtime owner.
// The reader releases its transaction after every source page or record.
func loadHistoricalThreadClosure(ctx context.Context, reader spi.ReadTx, source backendDomainFormat, threadID string, now func() time.Time) (*MemoryRepo, error) {
	if ctx == nil || reader == nil || now == nil || threadID == "" || source.version < 6 || source.version > 26 {
		return nil, spi.ErrInvalidArgument
	}
	memory := newMemoryRepo(now)
	manifestRaw, err := reader.Get(source.namespace, backendDomainKey(backendDomainRecordManifest))
	if err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	manifestRecord, err := decodeBackendDomainRecord(source, manifestRaw)
	if err != nil || manifestRecord.Kind != backendDomainRecordManifest || manifestRecord.ID != "" || manifestRecord.ThreadID != "" || manifestRecord.Ordinal != 0 {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	var manifest backendDomainManifest
	if source.version == 6 {
		var legacy backendDomainV6Manifest
		if err := decodeBackendDomainValue(source, manifestRecord, &legacy); err != nil || legacy.Version != 6 || legacy.Sequence < 0 || legacy.EffectAttemptSequence < 0 {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		manifest.Version, manifest.Sequence = legacy.Version, legacy.Sequence
	} else if err := decodeBackendDomainValue(source, manifestRecord, &manifest); err != nil || manifest.Version != source.version || manifest.Sequence < 0 || manifest.SourceVersion != 0 || manifest.MigrationPhase != "" || len(manifest.MigrationCursor) != 0 {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	memory.seq = manifest.Sequence
	queue := []string{threadID}
	seen := map[string]bool{}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		row, found, err := readBackendDomainRecord(reader, source, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0)
		if err != nil {
			return nil, err
		}
		if !found {
			tombstone, deleted, err := readBackendDomainRecord(reader, source, backendDomainKey(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0)
			if err != nil {
				return nil, err
			}
			if deleted {
				var value ThreadTombstone
				if err := decodeBackendDomainValue(source, tombstone, &value); err != nil || validateMemoryArtifactTombstone(value, id) != nil {
					return nil, errors.Join(ErrAuthorityCorrupt, err)
				}
				memory.tombstones[id] = value
				if id == threadID {
					return nil, ErrThreadDeleted
				}
				continue
			}
			if id == threadID {
				return nil, ErrThreadNotFound
			}
			return nil, ErrAuthorityCorrupt
		}
		var meta ThreadMeta
		if err := decodeBackendDomainValue(source, row, &meta); err != nil || meta.ID != id || ValidateThreadMetaAuthority(meta) != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		memory.threads[id] = meta
		if meta.ParentThreadID != "" {
			queue = append(queue, meta.ParentThreadID)
		}
		if meta.ForkedFromThreadID != "" {
			queue = append(queue, meta.ForkedFromThreadID)
		}
		prefix := backendDomainKey(backendDomainRecordEntry, id)
		var after []byte
		var entries []Entry
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			page, err := reader.Scan(spi.ScanRequest{Namespace: source.namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
			if err != nil {
				return nil, err
			}
			for _, stored := range page.Records {
				record, err := decodeBackendDomainRecord(source, stored.Value)
				if err != nil || record.Kind != backendDomainRecordEntry || record.ID == "" || record.ThreadID != id || record.Ordinal != len(entries) || !bytes.Equal(stored.Key, backendDomainEntryKey(id, len(entries))) {
					return nil, errors.Join(ErrAuthorityCorrupt, err)
				}
				var entry Entry
				if err := decodeBackendDomainValue(source, record, &entry); err != nil || entry.ID != record.ID || entry.ThreadID != id {
					return nil, errors.Join(ErrAuthorityCorrupt, err)
				}
				entries = append(entries, entry)
				if entry.Type == EntryParentEvent {
					event, err := decodeParentEvent(entry)
					if err != nil {
						return nil, err
					}
					queue = append(queue, event.SourceThreadID)
				}
			}
			if !page.HasMore {
				break
			}
			if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
				return nil, ErrAuthorityCorrupt
			}
			after = bytes.Clone(page.Next)
		}
		replace := memory.replaceIndexedEntriesLocked
		if source.version < 14 {
			replace = memory.replaceLegacyIndexedEntriesLocked
		}
		if err := replace(id, entries); err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := loadHistoricalThreadSideRecords(reader, source, memory, id); err != nil {
			return nil, err
		}
	}
	if err := source.validate(memory); err != nil {
		return nil, fmt.Errorf("%w: validate %s source closure: %w", ErrAuthorityCorrupt, source.label, err)
	}
	return memory, nil
}

func loadHistoricalThreadSideRecords(reader spi.ReadTx, source backendDomainFormat, memory *MemoryRepo, id string) error {
	for _, kind := range []string{backendDomainRecordTodo, backendDomainRecordProviderState} {
		record, found, err := readBackendDomainRecord(reader, source, backendDomainKey(kind, id), kind, id, "", 0)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if kind == backendDomainRecordTodo {
			var todo AgentTodoState
			if err := decodeBackendDomainValue(source, record, &todo); err != nil || todo.ThreadID != id {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			memory.todos[id] = todo
		} else {
			var state ProviderStateRecord
			if err := decodeBackendDomainValue(source, record, &state); err != nil || state.ThreadID != id {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			memory.providerStates[id] = state
		}
	}
	for _, entry := range memory.entries[id] {
		if entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil {
			continue
		}
		key := artifactRecordKey(id, entry.Message.ToolResult.FullOutput.ID)
		if _, found := memory.artifacts[key]; found {
			continue
		}
		record, found, err := readBackendDomainRecord(reader, source, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0)
		if err != nil || !found {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		var value artifact.Record
		if err := decodeBackendDomainValue(source, record, &value); err != nil || artifactRecordKey(value.ThreadID, value.Ref.ID) != key {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		memory.artifacts[key] = value
	}
	return nil
}

// The temporary candidate is private to one migration attempt. Persistent
// checkpoints point into this deterministically regenerated journal, while
// ordinary runtime readers can see only the published v32 namespace.
type historicalCandidateReader struct {
	spi.ReadTx
	memory  *MemoryRepo
	source  backendDomainFormat
	version int
	events  []ParentEvent
}

func (reader historicalCandidateReader) Get(namespace string, key []byte) ([]byte, error) {
	if namespace != reader.source.namespace {
		return reader.ReadTx.Get(namespace, key)
	}
	if bytes.Equal(key, backendDomainKey(backendDomainRecordManifest)) {
		return encodeBackendDomainRecord(reader.source, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: reader.source.version, Sequence: reader.memory.seq})
	}
	for _, kind := range []string{backendDomainRecordTodo, backendDomainRecordProviderState, backendDomainRecordArtifact, backendDomainRecordTombstone} {
		prefix := backendDomainKey(kind)
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		tail := key[len(prefix):]
		if len(tail) < 5 || tail[0] != 1 || uint64(binary.BigEndian.Uint32(tail[1:5])) != uint64(len(tail)-5) {
			return nil, ErrAuthorityCorrupt
		}
		id := string(tail[5:])
		var value any
		switch kind {
		case backendDomainRecordTodo:
			if record, ok := reader.memory.todos[id]; ok {
				value = record
			}
		case backendDomainRecordProviderState:
			if record, ok := reader.memory.providerStates[id]; ok {
				value = record
			}
		case backendDomainRecordArtifact:
			if record, ok := reader.memory.artifacts[id]; ok {
				value = record
			}
		case backendDomainRecordTombstone:
			if record, ok := reader.memory.tombstones[id]; ok {
				value = record
			}
		}
		if value == nil {
			return nil, spi.ErrNotFound
		}
		return encodeBackendDomainRecord(reader.source, kind, id, "", 0, value)
	}
	for id, meta := range reader.memory.threads {
		if bytes.Equal(key, backendDomainKey(backendDomainRecordThread, id)) {
			return encodeBackendDomainRecord(reader.source, backendDomainRecordThread, id, "", 0, meta)
		}
		prefix := backendDomainKey(backendDomainRecordEntry, id)
		if bytes.HasPrefix(key, prefix) {
			parts, err := historicalEntryOrdinal(key, prefix)
			if err != nil {
				return nil, err
			}
			if parts >= len(reader.memory.entries[id]) {
				return nil, spi.ErrNotFound
			}
			entry := reader.memory.entries[id][parts]
			return encodeBackendDomainRecord(reader.source, backendDomainRecordEntry, entry.ID, id, parts, entry)
		}
	}
	raw, err := reader.ReadTx.Get(namespace, key)
	if err != nil {
		return nil, err
	}
	exact, err := migrationSourceFormat(reader.version)
	if err != nil {
		return nil, err
	}
	record, err := decodeBackendDomainRecord(exact, raw)
	if err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	return encodeBackendDomainRecord(reader.source, record.Kind, record.ID, record.ThreadID, record.Ordinal, record.Value)
}

func historicalEntryOrdinal(key, prefix []byte) (int, error) {
	tail := key[len(prefix):]
	if len(tail) != 13 || tail[0] != 2 || binary.BigEndian.Uint32(tail[1:5]) != 8 {
		return 0, ErrAuthorityCorrupt
	}
	ordinal := binary.BigEndian.Uint64(tail[5:])
	if ordinal > uint64(^uint(0)>>1) {
		return 0, ErrAuthorityCorrupt
	}
	return int(ordinal), nil
}

func (reader historicalCandidateReader) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace != reader.source.namespace {
		return reader.ReadTx.Scan(request)
	}
	for id := range reader.memory.threads {
		prefix := backendDomainKey(backendDomainRecordEntry, id)
		if !bytes.HasPrefix(request.Start, prefix) {
			continue
		}
		if request.Limit <= 0 {
			return spi.ScanPage{}, spi.ErrInvalidArgument
		}
		entries := reader.memory.entries[id]
		first := 0
		if !bytes.Equal(request.Start, prefix) {
			ordinal, err := historicalEntryOrdinal(request.Start, prefix)
			if err != nil {
				return spi.ScanPage{}, err
			}
			first = ordinal
		}
		if len(request.After) > 0 {
			if !bytes.HasPrefix(request.After, prefix) {
				return spi.ScanPage{}, spi.ErrInvalidArgument
			}
			ordinal, err := historicalEntryOrdinal(request.After, prefix)
			if err != nil {
				return spi.ScanPage{}, err
			}
			first = max(first, ordinal+1)
		}
		first = min(first, len(entries))
		last := first + min(request.Limit, len(entries)-first)
		page := spi.ScanPage{}
		for ordinal := first; ordinal < last; ordinal++ {
			key := backendDomainEntryKey(id, ordinal)
			if len(request.End) > 0 && bytes.Compare(key, request.End) >= 0 {
				last = ordinal
				break
			}
			entry := entries[ordinal]
			raw, err := encodeBackendDomainRecord(reader.source, backendDomainRecordEntry, entry.ID, id, ordinal, entry)
			if err != nil {
				return page, err
			}
			page.Records = append(page.Records, spi.Record{Key: key, Value: raw})
		}
		page.HasMore = last < len(entries) && (len(request.End) == 0 || bytes.Compare(backendDomainEntryKey(id, last), request.End) < 0)
		if len(page.Records) > 0 {
			page.Next = bytes.Clone(page.Records[len(page.Records)-1].Key)
		}
		return page, nil
	}
	return reader.ReadTx.Scan(request)
}

func historicalCandidate(ctx context.Context, reader spi.ReadTx, source backendDomainFormat, threadID string) (historicalCandidateReader, error) {
	manifest, err := readV32Manifest(reader)
	if err != nil {
		return historicalCandidateReader{}, err
	}
	if manifest.SourceVersion != source.version || manifest.MigrationTime <= 0 {
		return historicalCandidateReader{}, ErrAuthorityCorrupt
	}
	clock := func() time.Time { return time.Unix(0, manifest.MigrationTime).UTC() }
	var memory *MemoryRepo
	if source.version < 6 {
		memory, err = loadSnapshotThreadClosure(ctx, reader, threadID, clock)
	} else {
		memory, err = loadHistoricalThreadClosure(ctx, reader, source, threadID, clock)
	}
	if err != nil {
		return historicalCandidateReader{}, err
	}
	return historicalCandidateFromMemory(ctx, reader, source, memory)
}

func historicalCandidateFromMemory(ctx context.Context, reader spi.ReadTx, source backendDomainFormat, memory *MemoryRepo) (historicalCandidateReader, error) {
	terminals := map[string]map[string]bool{}
	for id, entries := range memory.entries {
		terminals[id] = map[string]bool{}
		for _, entry := range entries {
			if entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) {
				terminals[id][entry.ID] = true
			}
		}
	}
	// Transform only immutable source journals. Parent receipts belong to the
	// atomic publication transaction, since a parent may already be in use.
	if err := migrateHistoricalMemoryThroughV29(WithParentEvents(ctx, false), memory, max(6, source.version)); err != nil {
		return historicalCandidateReader{}, err
	}
	var events []ParentEvent
	memory.parentEvents = true
	ids := make([]string, 0, len(memory.threads))
	for id := range memory.threads {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		for _, entry := range memory.entries[id] {
			if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnFailed || terminals[id][entry.ID] || entry.Metadata[TurnFailureCodeMetadataKey] != TurnFailureEffectOutcomeUnknown {
				continue
			}
			if event, ok := memory.automaticParentEventLocked(entry); ok {
				events = append(events, event)
			}
		}
	}
	memory.parentEvents = false
	candidateFormat := backendDomainV29Format
	candidateFormat.namespace = source.namespace
	return historicalCandidateReader{ReadTx: reader, memory: memory, source: candidateFormat, version: source.version, events: events}, nil
}

func (repo *BackendRepo) prepareHistoricalThreadBatch(ctx context.Context, threadID string) (bool, error) {
	source := *repo.migrationSource
	reader := migrationSourceReader{ctx: ctx, backend: repo.backend}
	var candidate historicalCandidateReader
	var err error
	if source.version < 6 {
		if repo.snapshotSource == nil {
			checkpoint, found, readErr := readSnapshotImportCheckpoint(reader)
			if readErr != nil || !found || !checkpoint.Complete || checkpoint.Version != source.version {
				return false, errors.Join(ErrAuthorityCorrupt, readErr)
			}
			if err := verifyIndexedJSONRecord(ctx, reader, backendDomainNamespace, backendStateKey, checkpoint); err != nil {
				return false, err
			}
			verified, err := newSnapshotEffectiveSource(ctx, reader, source.version)
			if err != nil {
				return false, err
			}
			verified.tx = nil // Never retain an expired caller context or transaction.
			repo.snapshotSource = verified
		}
		manifest, readErr := readV32Manifest(reader)
		if readErr != nil || manifest.SourceVersion != source.version || manifest.MigrationTime <= 0 {
			return false, errors.Join(ErrAuthorityCorrupt, readErr)
		}
		verified := *repo.snapshotSource
		verified.tx = reader
		memory, readErr := loadSnapshotThreadClosureFrom(ctx, &verified, source.version, threadID, func() time.Time { return time.Unix(0, manifest.MigrationTime).UTC() })
		if readErr != nil {
			return false, readErr
		}
		candidate, err = historicalCandidateFromMemory(ctx, reader, source, memory)
	} else {
		candidate, err = historicalCandidate(ctx, reader, source, threadID)
	}
	if err != nil {
		return false, err
	}
	// Keep one private transform for this attempt. Each bounded write releases
	// the backend, and foreground priority cancels a background attempt between
	// pages. Restart resumes the durable staging cursor without replaying writes.
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var staged bool
		err = repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			var err error
			transactionCandidate := candidate
			transactionCandidate.ReadTx = tx
			staged, err = repo.stageSourceThreadFrom(ctx, tx, transactionCandidate, candidate.source, threadID)
			return err
		})
		if err != nil {
			return false, err
		}
		if staged {
			break
		}
	}
	err = repo.publishSourceThreadFrom(ctx, threadID, candidate.source, candidate)
	if errors.Is(err, spi.ErrConflict) {
		return false, nil
	}
	return err == nil, err
}
