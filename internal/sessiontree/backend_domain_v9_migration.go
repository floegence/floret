package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// migrateBackendDomainV8ToV9 removes duplicated execution identity from
// context payloads. A mismatched payload is repairable only when the exact v8
// entry is proven to have been copied through canonical fork lineage.
func migrateBackendDomainV8ToV9(ctx context.Context, memory *MemoryRepo) error {
	if ctx == nil || memory == nil {
		return errors.New("session-tree v8 to v9 migration requires context and memory")
	}
	ancestors, err := validatedForkAncestors(memory)
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("validate session-tree v8 fork lineage: %w", err))
	}
	original := cloneMemoryRepoForBackendUpdate(memory)
	threadIDs := make([]string, 0, len(memory.threads))
	for threadID := range memory.threads {
		threadIDs = append(threadIDs, threadID)
	}
	sort.Strings(threadIDs)
	for _, threadID := range threadIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries := cloneEntries(memory.entries[threadID])
		contextRunIDs, err := legacyThreadContextRunIDs(entries)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("resolve v8 thread %q context run identity: %w", threadID, err))
		}
		changed := false
		for index := range entries {
			entry := &entries[index]
			entryChanged := false
			switch kind := ThreadContextEntryKind(*entry); kind {
			case legacyThreadContextPolicyEntryKind:
				if err := validateLegacyThreadContextEntryShape(*entry, kind,
					threadContextKindKey, threadContextTypeKey, threadContextProviderKey, threadContextModelKey, threadContextPolicyKey,
				); err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				record, err := decodeThreadContextPolicyMetadata(entry.Metadata)
				if err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				if entryRunID := strings.TrimSpace(entry.RunID); entryRunID != "" && entryRunID != contextRunIDs[index] {
					return migrationThreadContextError(threadID, entry.ID, errors.New("legacy thread context policy run identity drifted"))
				}
				entry.RunID = contextRunIDs[index]
				entry.Metadata, err = encodeThreadContextPolicyMetadata(record)
				if err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				entryChanged = true
			case legacyThreadContextStatusEntryKind:
				if err := validateLegacyThreadContextEntryShape(*entry, kind,
					threadContextKindKey, threadContextTypeKey, threadContextStatusKey,
				); err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				status, err := decodeThreadContextStatusPayload(entry.Metadata[threadContextStatusKey], true)
				if err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				if err := validateLegacyThreadContextIdentity(original, ancestors[threadID], *entry, status.ThreadID.String(), status.TurnID.String(), status.RunID.String(), contextRunIDs[index]); err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				entry.RunID = status.RunID.String()
				entry.Metadata, err = encodeThreadContextStatusMetadata(status)
				if err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				entryChanged = true
			case legacyThreadContextCompactionEntryKind:
				if err := validateLegacyThreadContextEntryShape(*entry, kind,
					threadContextKindKey, threadContextTypeKey, threadContextCompactionKey,
				); err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				compaction, err := decodeThreadContextCompactionPayload(entry.Metadata[threadContextCompactionKey], true)
				if err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				if err := validateThreadContextCompaction(compaction); err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				if err := validateLegacyThreadContextIdentity(original, ancestors[threadID], *entry, compaction.ThreadID, compaction.TurnID, compaction.RunID, contextRunIDs[index]); err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				entry.RunID = strings.TrimSpace(compaction.RunID)
				entry.Metadata, err = encodeThreadContextCompactionMetadata(compaction)
				if err != nil {
					return migrationThreadContextError(threadID, entry.ID, err)
				}
				entryChanged = true
			case ThreadContextPolicyEntryKind, ThreadContextStatusEntryKind, ThreadContextCompactionEntryKind:
				return migrationThreadContextError(threadID, entry.ID, errors.New("v8 authority contains a v9 thread context kind"))
			default:
				if strings.HasPrefix(kind, "thread_context_") || strings.HasPrefix(kind, "subagent_context_") {
					return migrationThreadContextError(threadID, entry.ID, fmt.Errorf("unsupported thread context kind %q", kind))
				}
			}
			if entryChanged {
				entry.Raw = rawForEntry(*entry)
				entry.RawHash = stableHash(entry.Raw)
				changed = true
			}
		}
		if changed {
			if err := memory.replaceIndexedEntriesLocked(threadID, entries); err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	}
	return validateBackendDomainV9Memory(memory)
}

func validateLegacyThreadContextEntryShape(entry Entry, kind string, metadataKeys ...string) error {
	if entry.Type != EntryCustom || ThreadContextEntryKind(entry) != kind || strings.TrimSpace(entry.Metadata[threadContextTypeKey]) != kind {
		return fmt.Errorf("invalid %s entry shape", kind)
	}
	if strings.TrimSpace(entry.ThreadID) == "" || strings.TrimSpace(entry.TurnID) == "" {
		return fmt.Errorf("%s entry requires thread and turn identity", kind)
	}
	return requireExactThreadContextMetadata(entry.Metadata, metadataKeys...)
}

func validateLegacyThreadContextIdentity(original *MemoryRepo, ancestors map[string]struct{}, entry Entry, payloadThreadID, payloadTurnID, payloadRunID, canonicalRunID string) error {
	payloadThreadID = strings.TrimSpace(payloadThreadID)
	payloadTurnID = strings.TrimSpace(payloadTurnID)
	payloadRunID = strings.TrimSpace(payloadRunID)
	if payloadThreadID == "" || payloadTurnID == "" || payloadRunID == "" {
		return errors.New("legacy thread context payload has incomplete identity")
	}
	if payloadTurnID != strings.TrimSpace(entry.TurnID) {
		return errors.New("legacy thread context turn identity drifted")
	}
	if payloadRunID != strings.TrimSpace(canonicalRunID) {
		return errors.New("legacy thread context run identity does not match canonical turn authority")
	}
	if entryRunID := strings.TrimSpace(entry.RunID); entryRunID != "" && entryRunID != payloadRunID {
		return errors.New("legacy thread context run identity drifted")
	}
	if payloadThreadID == strings.TrimSpace(entry.ThreadID) {
		return nil
	}
	if _, ok := ancestors[payloadThreadID]; !ok {
		return errors.New("legacy thread context thread identity is not a fork ancestor")
	}
	return validateCopiedLegacyThreadContextEntry(original, entry)
}

func legacyThreadContextRunIDs(entries []Entry) ([]string, error) {
	runIDs := make([]string, len(entries))
	active := make(map[string]string)
	for index, entry := range entries {
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			turnID := strings.TrimSpace(entry.TurnID)
			runID := strings.TrimSpace(entry.RunID)
			if turnID == "" || runID == "" {
				return nil, fmt.Errorf("started entry %q has incomplete run identity", entry.ID)
			}
			active[turnID] = runID
		}
		kind := ThreadContextEntryKind(entry)
		if kind != legacyThreadContextPolicyEntryKind && kind != legacyThreadContextStatusEntryKind && kind != legacyThreadContextCompactionEntryKind {
			continue
		}
		runID := active[strings.TrimSpace(entry.TurnID)]
		if runID == "" {
			return nil, fmt.Errorf("context entry %q has no preceding canonical started run", entry.ID)
		}
		runIDs[index] = runID
	}
	return runIDs, nil
}

func validateCopiedLegacyThreadContextEntry(original *MemoryRepo, entry Entry) error {
	meta, found := original.threads[entry.ThreadID]
	if !found || strings.TrimSpace(meta.ForkedFromThreadID) == "" || strings.TrimSpace(meta.ForkedFromEntryID) == "" || !entry.CreatedAt.Equal(meta.CreatedAt) {
		return errors.New("legacy thread context entry is not a canonical fork copy")
	}
	sourcePath, err := pathLocked(original.threads, original.entries, meta.ForkedFromThreadID, meta.ForkedFromEntryID)
	if err != nil {
		return fmt.Errorf("load fork source path: %w", err)
	}
	matches := 0
	for _, source := range sourcePath {
		if source.Type == entry.Type && source.TurnID == entry.TurnID && source.RunID == entry.RunID && source.Raw == entry.Raw && source.RawHash == entry.RawHash {
			matches++
		}
	}
	if matches != 1 {
		return fmt.Errorf("legacy thread context fork source matches=%d, want 1", matches)
	}
	return nil
}

func migrationThreadContextError(threadID, entryID string, err error) error {
	return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("migrate v8 thread %q context entry %q: %w", threadID, entryID, err))
}
