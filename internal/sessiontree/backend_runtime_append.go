package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) AppendRuntimeFacts(ctx context.Context, threadID string, entries []Entry) ([]Entry, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" || len(entries) == 0 {
		return nil, errors.New("runtime journal append requires thread and entries")
	}
	var result []Entry
	err := repo.canonicalJournalUpdate(ctx, threadID, func(mutation *canonicalJournalMutation) error {
		seen := make(map[string]bool, len(entries))
		for _, entry := range entries {
			if strings.TrimSpace(entry.ID) == "" || entry.ThreadID != "" && entry.ThreadID != threadID {
				return errors.New("runtime journal fact requires stable matching identity")
			}
			if seen[entry.ID] {
				return ErrRequestConflict
			}
			seen[entry.ID] = true
			existing, err := readCanonicalEntryMetadata(mutation.tx, threadID, entry.ID)
			if errors.Is(err, ErrEntryNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if existing.Type != entry.Type || existing.RequestFingerprint != entry.RequestFingerprint || !bytes.Equal(existing.Payload, entry.Payload) {
				return ErrRequestConflict
			}
			existing, err = completeLocatedEntry(ctx, mutation.tx, existing)
			if err != nil {
				return err
			}
			result = append(result, cloneEntry(existing))
		}
		if len(result) != 0 {
			if len(result) != len(entries) {
				return ErrAuthorityCorrupt
			}
			return nil
		}
		for _, entry := range entries {
			switch entry.Type {
			case EntrySteeringRequested, EntrySteeringApplied, EntryModelContext:
				cancelled, err := canonicalTurnAuthority(mutation.tx, threadID, entry.TurnID, entry.RunID)
				if err != nil {
					return err
				}
				if cancelled {
					return ErrStaleAuthority
				}
			}
		}
		for _, entry := range entries {
			entry.ThreadID = threadID
			saved, err := mutation.append(entry, AppendOptions{ID: entry.ID})
			if err != nil {
				return err
			}
			switch saved.Type {
			case EntryQueueAdded, EntryQueueReordered, EntryQueueDeleted, EntryModelContext:
				if err := reanchorCanonicalProviderState(ctx, mutation.tx, saved); err != nil {
					return err
				}
			}
			result = append(result, cloneEntry(saved))
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.AppendRuntimeFacts(ctx, threadID, entries)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func reanchorCanonicalProviderState(ctx context.Context, tx spi.WriteTx, entry Entry) error {
	key := backendDomainKey(backendDomainRecordProviderState, entry.ThreadID)
	record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, key, backendDomainRecordProviderState, entry.ThreadID, "", 0)
	if err != nil || !found {
		return err
	}
	var state ProviderStateRecord
	if err := decodeBackendDomainValue(backendDomainV32Format, record, &state); err != nil || state.ThreadID != entry.ThreadID || validateProviderStateRecord(state) != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if state.LeafEntryID != entry.ParentID {
		return nil
	}
	state.LeafEntryID, state.UpdatedAt = entry.ID, entry.CreatedAt
	return putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, key, backendDomainRecordProviderState, entry.ThreadID, "", 0, state)
}

func (repo *BackendRepo) CommitProviderContext(ctx context.Context, req ProviderContextCommit) ([]Entry, error) {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" || req.Clear && (req.State != nil || len(req.Entries) != 0) {
		return nil, errors.New("provider context commit requires an exact execution and unambiguous state")
	}
	if req.Partial && (req.State != nil || req.Clear) {
		return nil, errors.New("partial provider output cannot change provider state")
	}
	if err := provider.ValidateState(req.State); err != nil {
		return nil, err
	}
	if req.State != nil && (strings.TrimSpace(req.State.Kind) == "" || strings.TrimSpace(req.State.ID) == "" || strings.TrimSpace(req.CompatibilityKey) == "") {
		return nil, errors.New("provider context state is incomplete")
	}
	var result []Entry
	err := repo.canonicalJournalUpdate(ctx, req.ThreadID, func(mutation *canonicalJournalMutation) error {
		if err := lifecycleRejectsWrite(mutation.meta); err != nil {
			return err
		}
		cancelled, err := canonicalTurnAuthority(mutation.tx, req.ThreadID, req.TurnID, req.RunID)
		if err != nil {
			return err
		}
		if !req.Partial && cancelled {
			return context.Canceled
		}
		key := backendDomainKey(backendDomainRecordProviderState, req.ThreadID)
		if req.Clear {
			err := mutation.tx.Delete(backendDomainV32Namespace, key)
			if errors.Is(err, spi.ErrNotFound) {
				return nil
			}
			return err
		}
		for _, entry := range req.Entries {
			if req.Partial && entry.Type != EntryAssistantMessage && (entry.Type != EntryCustom || entry.Metadata["kind"] != HostedToolEntryKind) {
				return ErrInvalidThreadAuthority
			}
			if entry.ThreadID != req.ThreadID || entry.TurnID != req.TurnID || entry.RunID != req.RunID || entry.Type != EntryAssistantMessage && entry.Type != EntryToolCall && entry.Type != EntryToolResult && entry.Type != EntryUserMessage && entry.Type != EntryGoalContext && entry.Type != EntryCustom {
				return ErrInvalidThreadAuthority
			}
		}
		for _, entry := range req.Entries {
			saved, err := mutation.append(entry, AppendOptions{Now: req.Now})
			if err != nil {
				return err
			}
			result = append(result, cloneEntry(saved))
		}
		if req.State != nil {
			state := ProviderStateRecord{ThreadID: req.ThreadID, LeafEntryID: mutation.meta.LeafID, CompatibilityKey: req.CompatibilityKey, State: *provider.CloneState(req.State), CreatedByRunID: req.RunID, CreatedByTurnID: req.TurnID, UpdatedAt: canonicalTime(req.Now, mutation.repo.now)}
			return putBackendDomainRecordContext(ctx, mutation.tx, backendDomainV32Format, key, backendDomainRecordProviderState, req.ThreadID, "", 0, state)
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.CommitProviderContext(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
