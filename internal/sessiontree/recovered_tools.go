package sessiontree

import (
	"context"
	"fmt"
	"time"

	"github.com/floegence/floret/v7/internal/activityview"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/observation"
)

// RecoveredToolRepo closes known-undispatched calls only after the runtime has
// established that no live executor owns the turn and all interactions resolved.
// It never dispatches an effect or changes a historical tool identity.
type RecoveredToolRepo interface {
	CancelPreparedToolCalls(context.Context, string, string, string) error
}

type recoveredToolSettlement struct {
	attempt EffectAttempt
	result  Entry
}

func recoveredToolSettlements(entries runtimeEntrySelection, threadID, turnID, runID string, now time.Time, complete func(Entry) (Entry, error)) ([]recoveredToolSettlement, error) {
	if len(runtimePendingInteractionsIn(entries, turnID)) != 0 {
		return nil, ErrRequestConflict
	}
	attempts := make(map[string]EffectAttempt)
	var decodeErr error
	entries.each(func(entry Entry) bool {
		if entry.Type != EntryEffectAttempt {
			return true
		}
		attempt, err := DecodeCanonicalEffectAttempt(entry)
		if err != nil {
			decodeErr = err
			return false
		}
		attempts[attempt.EffectAttemptID] = attempt
		return true
	})
	if decodeErr != nil {
		return nil, decodeErr
	}
	byCall := make(map[string]EffectAttempt)
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
			return nil, ErrEffectOutcomeUnknown
		}
		if attempt.State != EffectAttemptPrepared {
			continue
		}
		if _, exists := byCall[attempt.Invocation.ToolCallID]; exists {
			return nil, ErrAuthorityCorrupt
		}
		byCall[attempt.Invocation.ToolCallID] = attempt
	}
	var result []recoveredToolSettlement
	for _, call := range runtimePendingToolCallsIn(entries, turnID) {
		attempt, ok := byCall[call.Message.ToolCallID]
		if !ok {
			continue
		}
		call, err := complete(call)
		if err != nil {
			return nil, err
		}
		inv := attempt.Invocation
		if inv.ThreadID != threadID || inv.TurnID != turnID || inv.RunID != runID || call.RunID != runID || inv.ToolName != call.Message.ToolName {
			return nil, fmt.Errorf("%w: recovered tool identity differs", ErrAuthorityCorrupt)
		}
		const message = "Tool call was not executed because its executor was lost on restart. Submit a new call if the action is still needed; its current permissions will be checked again."
		status := string(observation.ActivityStatusCanceled)
		entry := Entry{ID: "tool-restarted:" + turnID + ":" + call.Message.ToolCallID, ThreadID: threadID, TurnID: turnID, RunID: runID, Type: EntryToolResult, CreatedAt: now,
			Message: session.Message{Role: session.Tool, Content: message, ToolCallID: call.Message.ToolCallID, ToolName: call.Message.ToolName, ToolResult: &session.ToolResultView{Status: status}, Activity: activityview.WithTerminalStatus(call.Message.Activity, status, message)}}
		attempt.State, attempt.TerminalFingerprint, attempt.UpdatedAt = EffectAttemptCancelled, "runtime-restarted:"+StableHash(entry.ID), now
		result = append(result, recoveredToolSettlement{attempt: attempt, result: entry})
	}
	return result, nil
}

func (repo *BackendRepo) CancelPreparedToolCalls(ctx context.Context, threadID, turnID, runID string) error {
	return repo.canonicalJournalUpdateWithRecords(ctx, threadID, func(mutation *canonicalJournalMutation) error {
		if _, err := canonicalTurnAuthority(mutation.tx, threadID, turnID, runID); err != nil {
			return err
		}
		entries, err := canonicalRuntimeTurnEntries(ctx, mutation.tx, threadID, turnID)
		if err != nil {
			return err
		}
		settlements, err := recoveredToolSettlements(entries, threadID, turnID, runID, repo.now().UTC(), func(entry Entry) (Entry, error) { return completeLocatedEntry(ctx, mutation.tx, entry) })
		if err != nil {
			return err
		}
		for _, settlement := range settlements {
			if _, err := mutation.append(settlement.result, AppendOptions{}); err != nil {
				return err
			}
			if err := mutation.appendEffectAttempt(settlement.attempt, false); err != nil {
				return err
			}
		}
		return nil
	}, func(memory *MemoryRepo) error { return memory.CancelPreparedToolCalls(ctx, threadID, turnID, runID) }, nil)
}

func (repo *MemoryRepo) CancelPreparedToolCalls(ctx context.Context, threadID, turnID, runID string) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	activeTurn, active := repo.runtimeActiveTurnLocked(threadID)
	activeRun, err := repo.activeRunIdentityLocked(threadID, turnID)
	if err != nil || !active || activeTurn != turnID || activeRun != runID {
		return ErrStaleAuthority
	}
	meta := repo.threads[threadID]
	if err := lifecycleRejectsWrite(meta); err != nil {
		return err
	}
	entries, err := repo.runtimeTurnEntriesLocked(threadID, turnID)
	if err != nil {
		return err
	}
	settlements, err := recoveredToolSettlements(entries, threadID, turnID, runID, repo.now().UTC(), repo.completeEntryLocked)
	if err != nil {
		return err
	}
	checkpoint := repo.checkpointJournalAppendLocked(threadID)
	committed := false
	defer func() {
		if !committed {
			checkpoint.restore(repo)
		}
	}()
	for _, settlement := range settlements {
		if _, err := repo.appendLocked(ctx, settlement.result, AppendOptions{}); err != nil {
			return err
		}
		meta = repo.threads[threadID]
		if err := repo.appendEffectAttemptLocked(&meta, settlement.attempt); err != nil {
			return err
		}
	}
	committed = true
	return nil
}
