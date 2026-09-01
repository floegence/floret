package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

func migrateBackendDomainV6ToV7(ctx context.Context, memory *MemoryRepo) error {
	if ctx == nil || memory == nil {
		return errors.New("session-tree v6 to v7 migration requires context and memory")
	}
	forkAncestors, err := legacyForkAncestors(memory)
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("validate session-tree v6 fork lineage: %w", err))
	}
	threadIDs := make([]string, 0, len(memory.threads))
	for threadID := range memory.threads {
		threadIDs = append(threadIDs, threadID)
	}
	sort.Strings(threadIDs)
	for _, threadID := range threadIDs {
		if err := migrateBackendDomainV6Thread(memory, threadID, forkAncestors[threadID]); err != nil {
			return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("migrate session-tree v6 thread %q: %w", threadID, err))
		}
	}
	return convergeUnknownEffectTurns(ctx, memory)
}

func migrateBackendDomainV6Thread(memory *MemoryRepo, threadID string, forkAncestors map[string]struct{}) error {
	entries := memory.entries[threadID]
	activeTurnID, active := runtimeActiveTurn(entries)
	activeRunID := ""
	if active {
		var err error
		activeRunID, err = activeRunIdentity(entries, activeTurnID)
		if err != nil {
			return fmt.Errorf("resolve active run identity: %w", err)
		}
	}

	latestAttempts := make(map[string]legacyEffectAttemptV6)
	for _, entry := range entries {
		if entry.Type != EntryEffectAttempt {
			continue
		}
		attempt := legacyEffectAttemptV6{}
		if err := decodeBackendDomainV6ValueRaw(entry.Payload, &attempt); err != nil {
			return fmt.Errorf("decode legacy effect attempt %q: %w", entry.ID, err)
		}
		if strings.TrimSpace(attempt.EffectAttemptID) == "" || attempt.Invocation.TurnID != entry.TurnID || attempt.Invocation.RunID != entry.RunID || entry.RequestKey != attempt.EffectAttemptID || entry.RequestFingerprint != attempt.RequestFingerprint {
			return fmt.Errorf("legacy effect attempt %q identity does not match its entry", entry.ID)
		}
		if attempt.Invocation.ThreadID != threadID {
			_, ancestor := forkAncestors[attempt.Invocation.ThreadID]
			terminal, terminalFound := runtimeTurnTerminal(entries, entry.TurnID)
			if !ancestor || !terminalFound || terminal.RunID != entry.RunID {
				return fmt.Errorf("legacy effect attempt %q identity does not match a terminal fork ancestor", entry.ID)
			}
			continue
		}
		latestAttempts[attempt.EffectAttemptID] = attempt
	}
	retained, removedParents, err := removeEffectAuthorityEntries(entries)
	if err != nil {
		return fmt.Errorf("remove legacy effect authority: %w", err)
	}
	resolved, err := entriesWithExactRunIdentity(retained)
	if err != nil {
		return fmt.Errorf("repair exact run identity: %w", err)
	}
	for index := range resolved {
		resolved[index].Raw = rawForEntry(resolved[index])
		resolved[index].RawHash = stableHash(resolved[index].Raw)
	}
	meta := memory.threads[threadID]
	leafID, err := resolveRemovedEffectEntryID(meta.LeafID, removedParents)
	if err != nil {
		return fmt.Errorf("resolve migrated leaf: %w", err)
	}
	meta.LeafID = leafID
	memory.threads[threadID] = meta
	if err := memory.replaceIndexedEntriesLocked(threadID, resolved); err != nil {
		return fmt.Errorf("reindex migrated journal: %w", err)
	}
	if !active {
		return nil
	}

	activeAttempts := make(map[string]EffectAttempt)
	for _, legacy := range latestAttempts {
		if legacy.Invocation.TurnID != activeTurnID || legacy.Invocation.RunID != activeRunID {
			continue
		}
		state := EffectAttemptState(legacy.State)
		switch state {
		case EffectAttemptPrepared:
		case EffectAttemptDispatching, EffectAttemptUnknown:
			state = EffectAttemptUnknown
		case EffectAttemptState("retrying"):
			state = EffectAttemptUnknown
		default:
			continue
		}
		invocation := EffectInvocationIdentity{
			ThreadID: legacy.Invocation.ThreadID, TurnID: legacy.Invocation.TurnID, RunID: legacy.Invocation.RunID,
			ToolCallID: legacy.Invocation.ToolCallID, ToolName: legacy.Invocation.ToolName, ArgumentHash: legacy.Invocation.ArgumentHash,
		}
		if err := validateEffectInvocation(invocation); err != nil || strings.TrimSpace(legacy.RequestFingerprint) == "" {
			return errors.New("legacy active effect attempt has incomplete identity")
		}
		attemptID := effectAttemptID(invocation)
		attempt := EffectAttempt{
			EffectAttemptID: attemptID, Invocation: invocation, RequestFingerprint: legacy.RequestFingerprint,
			State: state, RejectionCode: legacy.RejectionCode, TerminalFingerprint: legacy.TerminalFingerprint,
			CreatedAt: legacy.CreatedAt, UpdatedAt: legacy.UpdatedAt,
		}
		if attempt.CreatedAt.IsZero() {
			attempt.CreatedAt = memory.now().UTC()
		}
		if attempt.UpdatedAt.IsZero() {
			attempt.UpdatedAt = attempt.CreatedAt
		}
		if state == EffectAttemptUnknown && strings.TrimSpace(attempt.TerminalFingerprint) == "" {
			attempt.TerminalFingerprint = "migration:effect-outcome-unknown"
		}
		if existing, duplicate := activeAttempts[attemptID]; !duplicate || existing.State == EffectAttemptPrepared && state == EffectAttemptUnknown {
			activeAttempts[attemptID] = attempt
		}
	}
	attemptIDs := make([]string, 0, len(activeAttempts))
	for attemptID := range activeAttempts {
		attemptIDs = append(attemptIDs, attemptID)
	}
	sort.Strings(attemptIDs)
	meta = memory.threads[threadID]
	for _, attemptID := range attemptIDs {
		if err := memory.appendEffectAttemptLocked(&meta, activeAttempts[attemptID]); err != nil {
			return fmt.Errorf("append migrated effect attempt: %w", err)
		}
	}
	return nil
}

func legacyForkAncestors(memory *MemoryRepo) (map[string]map[string]struct{}, error) {
	if memory == nil {
		return nil, errors.New("legacy fork lineage requires memory")
	}
	for threadID, meta := range memory.threads {
		sourceThreadID := strings.TrimSpace(meta.ForkedFromThreadID)
		sourceEntryID := strings.TrimSpace(meta.ForkedFromEntryID)
		if (sourceThreadID == "") != (sourceEntryID == "") {
			return nil, fmt.Errorf("thread %q has incomplete fork lineage", threadID)
		}
		if sourceThreadID == "" {
			continue
		}
		if _, found := memory.threads[sourceThreadID]; !found {
			return nil, fmt.Errorf("thread %q fork source %q is missing", threadID, sourceThreadID)
		}
		if _, found := findEntry(memory.entries[sourceThreadID], sourceEntryID); !found {
			return nil, fmt.Errorf("thread %q fork source entry %q is missing", threadID, sourceEntryID)
		}
	}

	ancestors := make(map[string]map[string]struct{}, len(memory.threads))
	for threadID := range memory.threads {
		lineage := make(map[string]struct{})
		currentID := threadID
		for {
			sourceThreadID := strings.TrimSpace(memory.threads[currentID].ForkedFromThreadID)
			if sourceThreadID == "" {
				break
			}
			if sourceThreadID == threadID {
				return nil, fmt.Errorf("fork lineage cycle includes thread %q", threadID)
			}
			if _, duplicate := lineage[sourceThreadID]; duplicate {
				return nil, fmt.Errorf("fork lineage cycle includes thread %q", sourceThreadID)
			}
			lineage[sourceThreadID] = struct{}{}
			currentID = sourceThreadID
		}
		ancestors[threadID] = lineage
	}
	return ancestors, nil
}

func entriesWithExactRunIdentity(entries []Entry) ([]Entry, error) {
	resolved := append([]Entry(nil), entries...)
	activeInitialRuns := make(map[string]string)
	latestLifecycleStatus := make(map[string]TurnMarkerStatus)
	for _, entry := range resolved {
		if entry.Type != EntryTurnMarker || strings.TrimSpace(entry.TurnID) == "" || strings.TrimSpace(entry.RunID) == "" {
			continue
		}
		switch entry.TurnStatus {
		case TurnStarted, TurnWaiting, TurnCompleted, TurnFailed, TurnAborted:
			latestLifecycleStatus[entry.TurnID] = entry.TurnStatus
			activeInitialRuns[entry.TurnID] = entry.RunID
		}
	}
	currentRuns := make(map[string]string)
	for turnID, status := range latestLifecycleStatus {
		if status == TurnStarted {
			currentRuns[turnID] = activeInitialRuns[turnID]
		}
	}
	for index := len(resolved) - 1; index >= 0; index-- {
		entry := &resolved[index]
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type == EntryTurnMarker && turnID != "" {
			runID := strings.TrimSpace(entry.RunID)
			switch entry.TurnStatus {
			case TurnStarted, TurnWaiting, TurnCompleted, TurnFailed, TurnAborted:
				if runID == "" {
					return nil, ErrAuthorityCorrupt
				}
				currentRuns[turnID] = runID
			}
			continue
		}
		if !presentationEntryRequiresRunIdentity(entry.Type) {
			continue
		}
		if turnID == "" {
			return nil, ErrAuthorityCorrupt
		}
		runID := strings.TrimSpace(entry.RunID)
		currentRunID := strings.TrimSpace(currentRuns[turnID])
		if runID == "" {
			if currentRunID == "" {
				return nil, ErrAuthorityCorrupt
			}
			entry.RunID = currentRunID
			continue
		}
		if currentRunID == "" {
			currentRuns[turnID] = runID
			continue
		}
		if currentRunID != runID {
			return nil, ErrAuthorityCorrupt
		}
	}
	return resolved, nil
}

func presentationEntryRequiresRunIdentity(entryType EntryType) bool {
	switch entryType {
	case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult, EntryInteractionAsked, EntryInteractionDone:
		return true
	default:
		return false
	}
}

func activeRunIdentity(entries []Entry, turnID string) (string, error) {
	for index := len(entries) - 1; index >= 0; index-- {
		entry := entries[index]
		if entry.Type != EntryTurnMarker || entry.TurnID != turnID {
			continue
		}
		if runID := strings.TrimSpace(entry.RunID); runID != "" {
			return runID, nil
		}
	}
	return "", ErrAuthorityCorrupt
}

func convergeUnknownEffectTurns(ctx context.Context, memory *MemoryRepo) error {
	threadIDs := make([]string, 0, len(memory.threads))
	for threadID := range memory.threads {
		threadIDs = append(threadIDs, threadID)
	}
	sort.Strings(threadIDs)
	for _, threadID := range threadIDs {
		entries := memory.entries[threadID]
		turnID, active := runtimeActiveTurn(entries)
		if !active {
			continue
		}
		runID, err := activeRunIdentity(entries, turnID)
		if err != nil {
			return err
		}
		unknown := false
		for _, attempt := range canonicalEffectAttemptsForTurn(entries, turnID) {
			if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
				unknown = true
				break
			}
		}
		if !unknown {
			continue
		}
		if _, err := memory.FailUnknownEffectTurn(ctx, FailUnknownEffectTurnRequest{
			ThreadID: threadID, TurnID: turnID, RunID: runID, Now: memory.now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func hasUnknownEffectTurns(memory *MemoryRepo) bool {
	for threadID := range memory.threads {
		entries := memory.entries[threadID]
		turnID, active := runtimeActiveTurn(entries)
		if !active {
			continue
		}
		for _, attempt := range canonicalEffectAttemptsForTurn(entries, turnID) {
			if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
				return true
			}
		}
	}
	return false
}
