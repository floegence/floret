package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/activityview"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) FailUnknownEffectTurn(ctx context.Context, req FailUnknownEffectTurnRequest) (FailUnknownEffectTurnResult, error) {
	return repo.FailUnknownEffectTurnWithRecords(ctx, req, nil)
}

// FailUnknownEffectTurnWithRecords commits the turn and storage-kernel records atomically.
func (repo *BackendRepo) FailUnknownEffectTurnWithRecords(ctx context.Context, req FailUnknownEffectTurnRequest, settle func(spi.WriteTx) error) (FailUnknownEffectTurnResult, error) {
	if err := ValidateFailUnknownEffectTurnRequest(req); err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	req.ThreadID, req.TurnID, req.RunID = strings.TrimSpace(req.ThreadID), strings.TrimSpace(req.TurnID), strings.TrimSpace(req.RunID)
	var result FailUnknownEffectTurnResult
	err := repo.canonicalJournalUpdateWithRecords(ctx, req.ThreadID, func(mutation *canonicalJournalMutation) error {
		var err error
		result, err = mutation.failUnknownEffectTurn(req)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.FailUnknownEffectTurn(ctx, req)
		return err
	}, settle)
	if err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	return result, nil
}

func (mutation *canonicalJournalMutation) failUnknownEffectTurn(req FailUnknownEffectTurnRequest) (FailUnknownEffectTurnResult, error) {
	terminalID := unknownEffectTerminalEntryID(req.ThreadID, req.TurnID, req.RunID)
	fingerprint := unknownEffectOutcomeFingerprint(req.ThreadID, req.TurnID, req.RunID)
	failureID := "turn-failure:" + req.TurnID
	terminal, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, terminalID)
	if err == nil {
		failure, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, failureID)
		if err != nil {
			return FailUnknownEffectTurnResult{}, errors.Join(ErrRequestConflict, err)
		}
		failure, err = completeLocatedEntry(mutation.ctx, mutation.tx, failure)
		if err != nil {
			return FailUnknownEffectTurnResult{}, err
		}
		if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID || terminal.TurnStatus != TurnFailed || terminal.RequestFingerprint != fingerprint ||
			terminal.Metadata[TurnFailureCodeMetadataKey] != TurnFailureEffectOutcomeUnknown || failure.Type != EntryRunFailure || failure.TurnID != req.TurnID || failure.RunID != req.RunID ||
			strings.TrimSpace(failure.Error) != EffectOutcomeUnknownFailureMessage || terminal.ParentID != failure.ID {
			return FailUnknownEffectTurnResult{}, ErrRequestConflict
		}
		return FailUnknownEffectTurnResult{Failure: failure, Terminal: terminal, Replayed: true}, nil
	}
	if !errors.Is(err, ErrEntryNotFound) {
		return FailUnknownEffectTurnResult{}, err
	}
	if _, err := canonicalTurnAuthority(mutation.tx, req.ThreadID, req.TurnID, req.RunID); err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	entries, err := canonicalRuntimeTurnEntries(mutation.ctx, mutation.tx, req.ThreadID, req.TurnID)
	if err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	attempts := canonicalEffectAttemptsIn(entries, req.TurnID)
	unknownCalls := make(map[string]bool)
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
			unknownCalls[attempt.Invocation.ToolCallID] = true
		}
	}
	if len(unknownCalls) == 0 {
		return FailUnknownEffectTurnResult{}, ErrRequestConflict
	}
	now := canonicalTime(req.Now, mutation.repo.now)
	if err := mutation.cancelPendingSteering(entries, req.TurnID, req.RunID, "effect_outcome_unknown", now); err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	result := FailUnknownEffectTurnResult{}
	result.InteractionResolutions, err = mutation.resolvePendingInteractions(entries, req.TurnID, "", "", json.RawMessage(`{"accepted":false,"outcome":"failed"}`), now)
	if err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	for _, attempt := range attempts {
		switch attempt.State {
		case EffectAttemptPrepared:
			attempt.State, attempt.TerminalFingerprint = EffectAttemptCancelled, "turn-failed:"+fingerprint
		case EffectAttemptDispatching:
			attempt.State, attempt.TerminalFingerprint = EffectAttemptUnknown, fingerprint
		default:
			continue
		}
		attempt.UpdatedAt = now
		if err := mutation.appendEffectAttempt(attempt, false); err != nil {
			return FailUnknownEffectTurnResult{}, err
		}
	}
	result.ToolResults, err = mutation.closePendingToolCalls(entries, req.TurnID, req.RunID, "tool-effect-unknown:", unknownCalls, now)
	if err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	result.Failure, err = mutation.append(Entry{ID: failureID, ThreadID: req.ThreadID, Type: EntryRunFailure, TurnID: req.TurnID, RunID: req.RunID, CreatedAt: now, Error: EffectOutcomeUnknownFailureMessage}, AppendOptions{})
	if err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	result.Terminal, err = mutation.append(Entry{
		ID: terminalID, ThreadID: req.ThreadID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID,
		RequestFingerprint: fingerprint, CreatedAt: now, TurnStatus: TurnFailed,
		Metadata: map[string]string{"run_id": req.RunID, "outcome": "failed", "failure_reason": EffectOutcomeUnknownFailureMessage, TurnFailureCodeMetadataKey: TurnFailureEffectOutcomeUnknown},
	}, AppendOptions{})
	if err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	if err := mutation.tx.Delete(backendDomainV32Namespace, backendDomainKey(backendDomainRecordProviderState, req.ThreadID)); err != nil && !errors.Is(err, spi.ErrNotFound) {
		return FailUnknownEffectTurnResult{}, err
	}
	return result, nil
}

func (mutation *canonicalJournalMutation) resolvePendingInteractions(entries runtimeEntrySelection, turnID, requestKey, fingerprint string, payload json.RawMessage, now time.Time) ([]Entry, error) {
	var result []Entry
	for _, interaction := range runtimePendingInteractionsIn(entries, turnID) {
		resolved, err := mutation.append(Entry{
			ID: "interaction-resolved:" + interaction.ID, ThreadID: mutation.meta.ID, TurnID: interaction.TurnID, RunID: interaction.RunID,
			Type: EntryInteractionDone, RequestKey: requestKey, RequestFingerprint: fingerprint, Payload: payload, CreatedAt: now,
		}, AppendOptions{})
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

func (mutation *canonicalJournalMutation) closePendingToolCalls(entries runtimeEntrySelection, turnID, runID, prefix string, unknownCalls map[string]bool, now time.Time) ([]Entry, error) {
	var result []Entry
	for _, call := range runtimePendingToolCallsIn(entries, turnID) {
		call, err := completeLocatedEntry(mutation.ctx, mutation.tx, call)
		if err != nil {
			return nil, err
		}
		status, message := observation.ActivityStatusCanceled, "Tool call was canceled before completion."
		if prefix == "tool-effect-unknown:" {
			message = "Tool call was canceled before execution."
		}
		if unknownCalls[call.Message.ToolCallID] {
			status, message = observation.ActivityStatusError, EffectOutcomeUnknownFailureMessage
		}
		closed, err := mutation.append(Entry{
			ID: prefix + turnID + ":" + strings.TrimSpace(call.Message.ToolCallID), ThreadID: mutation.meta.ID,
			TurnID: turnID, RunID: runID, Type: EntryToolResult, CreatedAt: now,
			Message: session.Message{Role: session.Tool, Content: message, ToolCallID: call.Message.ToolCallID, ToolName: call.Message.ToolName,
				ToolResult: &session.ToolResultView{Status: string(status)}, Activity: activityview.WithTerminalStatus(call.Message.Activity, string(status), message)},
		}, AppendOptions{})
		if err != nil {
			return nil, err
		}
		result = append(result, closed)
	}
	return result, nil
}

func (repo *BackendRepo) CancelTurn(ctx context.Context, req CancelTurnRequest) (CancelTurnResult, error) {
	return repo.CancelTurnWithRecords(ctx, req, nil)
}

// CancelTurnWithRecords commits the turn and storage-kernel records atomically.
func (repo *BackendRepo) CancelTurnWithRecords(ctx context.Context, req CancelTurnRequest, settle func(spi.WriteTx) error) (CancelTurnResult, error) {
	if err := ValidateCancelTurnRequest(req); err != nil {
		return CancelTurnResult{}, err
	}
	req.ThreadID, req.TurnID, req.RunID = strings.TrimSpace(req.ThreadID), strings.TrimSpace(req.TurnID), strings.TrimSpace(req.RunID)
	req.CancelEntryID, req.TerminalEntryID = strings.TrimSpace(req.CancelEntryID), strings.TrimSpace(req.TerminalEntryID)
	req.RequestKey, req.RequestFingerprint, req.OutcomeFingerprint = strings.TrimSpace(req.RequestKey), strings.TrimSpace(req.RequestFingerprint), strings.TrimSpace(req.OutcomeFingerprint)
	var result CancelTurnResult
	err := repo.canonicalJournalUpdateWithRecords(ctx, req.ThreadID, func(mutation *canonicalJournalMutation) error {
		terminal, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, req.TerminalEntryID)
		if err == nil {
			if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID || terminal.TurnStatus != TurnAborted || terminal.RequestFingerprint != req.OutcomeFingerprint {
				return fmt.Errorf("%w: cancellation terminal differs", ErrRequestConflict)
			}
			cancel, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, req.CancelEntryID)
			if err != nil || cancel.Type != EntryCancelRequested || cancel.TurnID != req.TurnID || cancel.RunID != req.RunID || cancel.RequestFingerprint != req.RequestFingerprint {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			result = CancelTurnResult{CancelRequest: cancel, Terminal: terminal, Replayed: true}
			return nil
		}
		if !errors.Is(err, ErrEntryNotFound) {
			return err
		}
		unknown, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, unknownEffectTerminalEntryID(req.ThreadID, req.TurnID, req.RunID))
		if err == nil && unknown.Metadata[TurnFailureCodeMetadataKey] == TurnFailureEffectOutcomeUnknown {
			failed, err := mutation.failUnknownEffectTurn(FailUnknownEffectTurnRequest{ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Now: req.Now})
			if err != nil {
				return err
			}
			cancel, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, req.CancelEntryID)
			if err != nil && !errors.Is(err, ErrEntryNotFound) {
				return err
			}
			result = CancelTurnResult{CancelRequest: cancel, Terminal: failed.Terminal, Replayed: true}
			return nil
		}
		if err != nil && !errors.Is(err, ErrEntryNotFound) {
			return err
		}
		turn, err := readTurnEntryLocator(mutation.tx, req.ThreadID, req.TurnID)
		if err != nil {
			return err
		}
		if turn.Terminal != "" && req.RequestOnly {
			terminal, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, turn.Terminal)
			if err != nil {
				return err
			}
			result = CancelTurnResult{Terminal: terminal, Replayed: true}
			return nil
		}
		if _, err := canonicalTurnAuthority(mutation.tx, req.ThreadID, req.TurnID, req.RunID); err != nil {
			return err
		}
		now := canonicalTime(req.Now, mutation.repo.now)
		cancel, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, req.CancelEntryID)
		if err == nil {
			if cancel.Type != EntryCancelRequested || cancel.TurnID != req.TurnID || cancel.RunID != req.RunID || cancel.RequestKey != req.RequestKey || cancel.RequestFingerprint != req.RequestFingerprint {
				return fmt.Errorf("%w: cancellation request differs", ErrRequestConflict)
			}
		} else if errors.Is(err, ErrEntryNotFound) {
			cancel, err = mutation.append(Entry{ID: req.CancelEntryID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Type: EntryCancelRequested,
				RequestKey: req.RequestKey, RequestFingerprint: req.RequestFingerprint, CreatedAt: now, Metadata: cloneStringMap(req.CancellationMetadata)}, AppendOptions{})
		}
		if err != nil {
			return err
		}
		result = CancelTurnResult{CancelRequest: cancel}
		if req.RequestOnly {
			return nil
		}
		entries, err := canonicalRuntimeTurnEntries(ctx, mutation.tx, req.ThreadID, req.TurnID)
		if err != nil {
			return err
		}
		attempts := canonicalEffectAttemptsIn(entries, req.TurnID)
		for _, attempt := range attempts {
			if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
				failed, err := mutation.failUnknownEffectTurn(FailUnknownEffectTurnRequest{ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Now: req.Now})
				if err != nil {
					return err
				}
				result.InteractionResolutions, result.ToolResults, result.Terminal, result.Replayed = failed.InteractionResolutions, failed.ToolResults, failed.Terminal, failed.Replayed
				return nil
			}
		}
		result.InteractionResolutions, err = mutation.resolvePendingInteractions(entries, req.TurnID, req.RequestKey, req.RequestFingerprint, req.InteractionResolutionPayload, now)
		if err != nil {
			return err
		}
		for _, attempt := range attempts {
			if attempt.State == EffectAttemptPrepared {
				attempt.State, attempt.TerminalFingerprint, attempt.UpdatedAt = EffectAttemptCancelled, "turn-cancel:"+req.OutcomeFingerprint, now
				if err := mutation.appendEffectAttempt(attempt, false); err != nil {
					return err
				}
			}
		}
		result.ToolResults, err = mutation.closePendingToolCalls(entries, req.TurnID, req.RunID, "tool-cancelled:", nil, now)
		if err != nil {
			return err
		}
		if err := mutation.settleRunningCompaction(entries, req.TurnID, req.RunID, "cancelled", "cancelled", "", now); err != nil {
			return err
		}
		if err := mutation.cancelPendingSteering(entries, req.TurnID, req.RunID, "stopped", now); err != nil {
			return err
		}
		result.Terminal, err = mutation.append(Entry{ID: req.TerminalEntryID, ThreadID: req.ThreadID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID,
			RequestFingerprint: req.OutcomeFingerprint, CreatedAt: now, TurnStatus: TurnAborted, Metadata: cloneStringMap(req.Metadata)}, AppendOptions{})
		if err != nil {
			return err
		}
		return mutation.settleProviderState(result.Terminal, req.PreserveProviderState, req.ClearProviderState, "")
	}, func(memory *MemoryRepo) error { var err error; result, err = memory.CancelTurn(ctx, req); return err }, settle)
	if err != nil {
		return CancelTurnResult{}, err
	}
	return result, nil
}
