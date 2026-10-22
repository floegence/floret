package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/storage/spi"
)

func canonicalRuntimeTurnEntries(ctx context.Context, tx spi.ReadTx, threadID, turnID string) (runtimeEntrySelection, error) {
	turn, err := readTurnEntryLocator(tx, threadID, turnID)
	if err != nil {
		return runtimeEntrySelection{}, err
	}
	if turn.Started == "" {
		return runtimeEntrySelection{}, ErrAuthorityCorrupt
	}
	var entries []Entry
	err = visitCanonicalTurnEntries(ctx, tx, threadID, turnID, func(entry Entry) error {
		if entry.Type == EntryEffectAttempt {
			if _, err := decodeEffectAttempt(entry); err != nil {
				return err
			}
		}
		entries = append(entries, entry)
		return nil
	})
	return allRuntimeEntries(entries), err
}

func (mutation *canonicalJournalMutation) settleRunningCompaction(entries runtimeEntrySelection, turnID, runID, phase, status, message string, now time.Time) error {
	for i := len(entries.entries) - 1; i >= 0; i-- {
		entry := entries.entries[i]
		if entry.TurnID != turnID || entry.RunID != runID || ThreadContextEntryKind(entry) != ThreadContextCompactionEntryKind {
			continue
		}
		compact, err := DecodeThreadContextCompactionEntry(entry)
		if err != nil {
			return err
		}
		if compact.Status != "running" {
			return nil
		}
		compact.Phase, compact.Status, compact.Error, compact.ObservedAt = phase, status, message, now
		pending, err := NewThreadContextCompactionEntry(compact)
		if err != nil {
			return err
		}
		_, err = mutation.append(pending, AppendOptions{Now: now})
		return err
	}
	return nil
}

func (mutation *canonicalJournalMutation) cancelPendingSteering(entries runtimeEntrySelection, turnID, runID, reason string, now time.Time) error {
	resolved := map[string]bool{}
	for _, entry := range entries.entries {
		if entry.Type == EntrySteeringApplied || entry.Type == EntrySteeringCancelled {
			resolution, err := DecodeSteeringResolution(entry)
			if err != nil {
				return err
			}
			resolved[resolution.SteeringID] = true
		}
	}
	for _, entry := range entries.entries {
		if entry.Type != EntrySteeringRequested || entry.TurnID != turnID || resolved[entry.ID] {
			continue
		}
		payload, _ := json.Marshal(SteeringResolution{SteeringID: entry.ID, Reason: reason})
		if _, err := mutation.append(Entry{ID: "steer-resolved:" + entry.ID, ThreadID: mutation.meta.ID, Type: EntrySteeringCancelled, TurnID: turnID, RunID: runID, Payload: payload, CreatedAt: now}, AppendOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (mutation *canonicalJournalMutation) appendEffectAttempt(attempt EffectAttempt, settlement bool) error {
	entry, err := effectAttemptEntry(attempt, mutation.meta.LeafID)
	if err != nil {
		return err
	}
	existing, err := readCanonicalEntryMetadata(mutation.tx, mutation.meta.ID, entry.ID)
	if err == nil {
		decoded, err := decodeEffectAttempt(existing)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(decoded, attempt) {
			return fmt.Errorf("%w: effect settlement differs", ErrRequestConflict)
		}
		return nil
	}
	if !errors.Is(err, ErrEntryNotFound) {
		return err
	}
	if settlement {
		// Completion may retire already-prepared effects after its terminal.
		// No dispatched effect or new execution can enter this path.
		if attempt.State != EffectAttemptCancelled || !strings.HasPrefix(attempt.TerminalFingerprint, "turn-finish:") {
			return ErrAuthorityCorrupt
		}
		prepared, err := readCanonicalEntryMetadata(mutation.tx, mutation.meta.ID, effectAttemptEntryID(attempt.EffectAttemptID, EffectAttemptPrepared))
		if err != nil {
			return err
		}
		previous, err := decodeEffectAttempt(prepared)
		if err != nil || previous.Invocation != attempt.Invocation {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		turn, err := readTurnEntryLocator(mutation.tx, mutation.meta.ID, attempt.Invocation.TurnID)
		if err != nil || turn.Terminal == "" {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		terminal, err := readCanonicalEntryMetadata(mutation.tx, mutation.meta.ID, turn.Terminal)
		if err != nil || terminal.RunID != attempt.Invocation.RunID || attempt.TerminalFingerprint != "turn-finish:"+terminal.RequestFingerprint {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	_, err = mutation.appendFact(entry, AppendOptions{}, settlement)
	return err
}

func (mutation *canonicalJournalMutation) settleProviderState(terminal Entry, preserve, clear bool, compatibility string) error {
	key := backendDomainKey(backendDomainRecordProviderState, mutation.meta.ID)
	record, found, err := readBackendDomainRecord(mutation.tx, backendDomainV32Format, key, backendDomainRecordProviderState, mutation.meta.ID, "", 0)
	if err != nil || !found {
		return err
	}
	var state ProviderStateRecord
	if err := decodeBackendDomainValue(backendDomainV32Format, record, &state); err != nil || state.ThreadID != mutation.meta.ID || validateProviderStateRecord(state) != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	remove := func() error { return mutation.tx.Delete(backendDomainV32Namespace, key) }
	if preserve {
		if compatibility != "" && state.CompatibilityKey != compatibility {
			return remove()
		}
		node, err := readEntryLocator(mutation.tx, mutation.meta.ID, terminal.ID)
		if err != nil {
			return err
		}
		anchored := false
		for {
			if err := mutation.ctx.Err(); err != nil {
				return err
			}
			entry, err := readLocatedEntry(mutation.tx, backendDomainV32Format, mutation.meta.ID, node)
			if err != nil {
				return err
			}
			if entry.Type == EntryCompaction {
				return remove()
			}
			if entry.ID == state.LeafEntryID {
				anchored = true
				break
			}
			if node.Parent == "" {
				break
			}
			node, err = readEntryLocator(mutation.tx, mutation.meta.ID, node.Parent)
			if err != nil {
				return err
			}
		}
		if !anchored {
			return remove()
		}
	} else if clear {
		started, err := readCanonicalEntryMetadata(mutation.tx, mutation.meta.ID, runtimeTurnStartedEntryID(terminal.TurnID))
		if errors.Is(err, ErrEntryNotFound) {
			return remove()
		}
		if err != nil {
			return err
		}
		if started.Metadata[TurnKindMetadataKey] != TurnKindContextCompaction {
			return remove()
		}
		state.CreatedByRunID, state.CreatedByTurnID = terminal.RunID, terminal.TurnID
	} else {
		return nil
	}
	state.LeafEntryID, state.UpdatedAt = terminal.ID, terminal.CreatedAt
	return putBackendDomainRecordContext(mutation.ctx, mutation.tx, backendDomainV32Format, key, backendDomainRecordProviderState, mutation.meta.ID, "", 0, state)
}

func (repo *BackendRepo) FinishTurn(ctx context.Context, req FinishTurnRequest) (FinishTurnResult, error) {
	return repo.FinishTurnWithRecords(ctx, req, nil)
}

// FinishTurnWithRecords commits the turn and storage-kernel records atomically.
func (repo *BackendRepo) FinishTurnWithRecords(ctx context.Context, req FinishTurnRequest, settle func(spi.WriteTx) error) (FinishTurnResult, error) {
	if err := ValidateFinishTurnRequest(req); err != nil {
		return FinishTurnResult{}, err
	}
	var result FinishTurnResult
	err := repo.canonicalJournalUpdateWithRecords(ctx, req.ThreadID, func(mutation *canonicalJournalMutation) error {
		return mutation.finishTurn(req, &result)
	}, func(memory *MemoryRepo) error { var err error; result, err = memory.FinishTurn(ctx, req); return err }, settle)
	if err != nil {
		return FinishTurnResult{}, err
	}
	return result, nil
}

func (mutation *canonicalJournalMutation) finishTurn(req FinishTurnRequest, result *FinishTurnResult) error {
	ctx := mutation.ctx
	terminal, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, strings.TrimSpace(req.TerminalEntryID))
	if err == nil {
		if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID || terminal.RequestFingerprint != req.OutcomeFingerprint {
			return ErrRequestConflict
		}
		terminal, err = completeLocatedEntry(ctx, mutation.tx, terminal)
		if err != nil {
			return err
		}
		*result = FinishTurnResult{Terminal: terminal, Replayed: true}
		return nil
	}
	if !errors.Is(err, ErrEntryNotFound) {
		return err
	}
	if _, err := canonicalTurnAuthority(mutation.tx, req.ThreadID, req.TurnID, req.RunID); err != nil {
		return err
	}
	entries, err := canonicalRuntimeTurnEntries(ctx, mutation.tx, req.ThreadID, req.TurnID)
	if err != nil {
		return err
	}
	if req.Status != TurnWaiting && runtimeTurnHasPendingInteractionIn(entries, req.TurnID) {
		return ErrRequestConflict
	}
	turn, err := readTurnEntryLocator(mutation.tx, req.ThreadID, req.TurnID)
	if err != nil {
		return err
	}
	if (req.Status == TurnCompleted || req.Status == TurnWaiting) && turn.Cancellation != "" {
		return context.Canceled
	}
	attempts := canonicalEffectAttemptsIn(entries, req.TurnID)
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
			return ErrEffectOutcomeUnknown
		}
	}
	req.Metadata = cloneStringMap(req.Metadata)
	if req.Status == TurnCompleted && req.Metadata["goal_guard"] == "1" {
		facts, err := readCanonicalGoalContext(ctx, mutation.tx, mutation.meta, req.TurnID, req.RunID)
		if err != nil {
			return err
		}
		req.Metadata, err = finishGoalMetadata(facts.goal.State, facts.reviewEntry, facts.review, facts.validReview(), req)
		if err != nil {
			return err
		}
	}
	now := canonicalTime(req.Now, mutation.repo.now)
	if req.Status == TurnFailed || req.Status == TurnAborted {
		phase, status := "failed", "failed"
		if req.Status == TurnAborted {
			phase, status = "cancelled", "cancelled"
		}
		if err := mutation.settleRunningCompaction(entries, req.TurnID, req.RunID, phase, status, req.FailureMessage, now); err != nil {
			return err
		}
	}
	if req.Status != TurnWaiting {
		if err := mutation.cancelPendingSteering(entries, req.TurnID, req.RunID, "task_ended", now); err != nil {
			return err
		}
	}
	if message := strings.TrimSpace(req.FailureMessage); message != "" {
		failure, err := mutation.append(Entry{ID: "turn-failure:" + req.TurnID, ThreadID: req.ThreadID, Type: EntryRunFailure, TurnID: req.TurnID, RunID: req.RunID, CreatedAt: now, Error: message}, AppendOptions{})
		if err != nil {
			return err
		}
		result.Failure = &failure
	}
	terminal, err = mutation.append(Entry{ID: strings.TrimSpace(req.TerminalEntryID), ThreadID: req.ThreadID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID, RequestFingerprint: req.OutcomeFingerprint, CreatedAt: now, TurnStatus: req.Status, Metadata: req.Metadata}, AppendOptions{})
	if err != nil {
		return err
	}
	if req.ProviderState != nil {
		record := *req.ProviderState
		record.State = *provider.CloneState(&record.State)
		if err := putBackendDomainRecordContext(ctx, mutation.tx, backendDomainV32Format, backendDomainKey(backendDomainRecordProviderState, req.ThreadID), backendDomainRecordProviderState, req.ThreadID, "", 0, record); err != nil {
			return err
		}
	} else if err := mutation.settleProviderState(terminal, req.PreserveProviderState, req.ClearProviderState, req.StateCompatibilityKey); err != nil {
		return err
	}
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptPrepared {
			attempt.State, attempt.TerminalFingerprint, attempt.UpdatedAt = EffectAttemptCancelled, "turn-finish:"+req.OutcomeFingerprint, now
			if err := mutation.appendEffectAttempt(attempt, terminalTurnMarker(req.Status)); err != nil {
				return err
			}
		}
	}
	result.Terminal = cloneEntry(terminal)
	return nil
}
