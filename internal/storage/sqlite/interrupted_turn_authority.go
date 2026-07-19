package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) RecoverInterruptedTurn(ctx context.Context, req sessiontree.RecoverInterruptedTurnRequest) (sessiontree.RecoverInterruptedTurnResult, error) {
	if err := sessiontree.ValidateRecoverInterruptedTurnRequest(req); err != nil {
		return sessiontree.RecoverInterruptedTurnResult{}, err
	}
	threadID := strings.TrimSpace(req.ExpectedLease.ThreadID)
	turnID := strings.TrimSpace(req.ExpectedLease.TurnID)
	var result sessiontree.RecoverInterruptedTurnResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		finished, found, err := loadSQLiteTurnFinish(ctx, tx, threadID, turnID)
		if err != nil {
			return err
		}
		if found {
			if finished.Generation != req.ExpectedLease.Generation+1 {
				return sessiontree.ErrRequestConflict
			}
			terminal, err := loadEntry(ctx, tx, threadID, finished.TerminalEntryID)
			if errors.Is(err, sql.ErrNoRows) {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err != nil {
				return err
			}
			if terminal.Type != sessiontree.EntryTurnMarker || terminal.TurnID != turnID ||
				terminal.Metadata[sessiontree.InterruptedTurnRecoveryKindKey] != sessiontree.InterruptedTurnRecoveryKind ||
				terminal.Metadata[sessiontree.InterruptedTurnRecoveryFingerprintKey] != finished.OutcomeFingerprint {
				return sessiontree.ErrAuthorityCorrupt
			}
			switch terminal.TurnStatus {
			case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
			default:
				return sessiontree.ErrAuthorityCorrupt
			}
			if terminal.Metadata[sessiontree.InterruptedTurnRecoveryParentKey] != strings.TrimSpace(req.ParentThreadID) {
				return sessiontree.ErrInvalidThreadAuthority
			}
			failureMessage := ""
			var failure *sessiontree.Entry
			if finished.FailureEntryID != "" {
				entry, err := loadEntry(ctx, tx, threadID, finished.FailureEntryID)
				if errors.Is(err, sql.ErrNoRows) {
					return sessiontree.ErrAuthorityCorrupt
				}
				if err != nil {
					return err
				}
				if entry.Type != sessiontree.EntryRunFailure || entry.TurnID != turnID {
					return sessiontree.ErrAuthorityCorrupt
				}
				failureMessage = entry.Error
				failure = &entry
			}
			fingerprint, err := sessiontree.InterruptedTurnRecoveryFingerprint(
				req.ExpectedLease, req.ParentThreadID, finished.RunID, terminal.TurnStatus, failureMessage,
			)
			if err != nil {
				return err
			}
			if fingerprint != finished.OutcomeFingerprint {
				return sessiontree.ErrRequestConflict
			}
			if terminal.ID != "recovery-terminal-"+fingerprint[:24] {
				return sessiontree.ErrAuthorityCorrupt
			}
			result = sessiontree.RecoverInterruptedTurnResult{
				RunID: finished.RunID, Status: terminal.TurnStatus, OutcomeFingerprint: fingerprint,
				Failure: failure, Terminal: terminal, Generation: finished.Generation, Replayed: true,
			}
			return nil
		}
		meta, err := loadThread(ctx, tx, threadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, threadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			}
		}
		if err != nil {
			return err
		}
		lifecycle, err := sqliteThreadLifecycle(meta)
		if err != nil {
			return err
		}
		if lifecycle != sessiontree.ThreadLifecycleOpen && lifecycle != sessiontree.ThreadLifecycleClosing {
			return sessiontree.ErrThreadClosed
		}
		parentThreadID := strings.TrimSpace(req.ParentThreadID)
		if (parentThreadID == "" && strings.TrimSpace(meta.ParentThreadID) != "") ||
			(parentThreadID != "" && strings.TrimSpace(meta.ParentThreadID) != parentThreadID) {
			return sessiontree.ErrInvalidThreadAuthority
		}
		if parentThreadID != "" {
			parent, err := loadThread(ctx, tx, parentThreadID)
			if errors.Is(err, sessiontree.ErrThreadNotFound) {
				if _, tombstoneErr := loadThreadTombstone(ctx, tx, parentThreadID); tombstoneErr == nil {
					return sessiontree.ErrThreadDeleted
				}
			}
			if err != nil {
				return err
			}
			if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
				return err
			}
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, threadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrAuthorityCorrupt
		}
		active, activeOK, err := loadTurnLease(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if !activeOK || !sessiontree.SameTurnLease(active, req.ExpectedLease) {
			return sessiontree.ErrStaleAuthority
		}
		if !active.TakeoverEligible(s.now().UTC(), s.leasePolicy) {
			return sessiontree.ErrThreadAuthorityBusy
		}
		path, err := pathWithRunner(ctx, tx, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		plan, err := sessiontree.DeriveInterruptedTurnRecoveryPlan(path, req.ExpectedLease, req.ParentThreadID)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidateInterruptedTurnRecoveryPath(path, turnID, plan.RunID); err != nil {
			return err
		}
		if exists, err := entryExists(ctx, tx, threadID, plan.TerminalEntryID); err != nil {
			return err
		} else if exists {
			return sessiontree.ErrRequestConflict
		}
		var unsafe int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM effect_attempts
			WHERE thread_id = ? AND turn_id = ?
			AND state NOT IN ('prepared', 'dispatching', 'completed', 'failed', 'rejected', 'cancelled', 'unknown')`,
			threadID, turnID).Scan(&unsafe); err != nil {
			return err
		}
		if unsafe != 0 {
			return sessiontree.ErrAuthorityCorrupt
		}
		var storedGeneration int64
		if err := tx.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = ?`, threadID).Scan(&storedGeneration); err != nil {
			return err
		}
		if storedGeneration != active.Generation {
			return sessiontree.ErrAuthorityCorrupt
		}
		generation := active.Generation + 1
		if _, err := tx.ExecContext(ctx, `UPDATE threads SET lease_generation = ? WHERE id = ?`, generation, threadID); err != nil {
			return err
		}
		deleted, err := tx.ExecContext(ctx, `DELETE FROM active_turn_leases
			WHERE thread_id = ? AND purpose = 'turn' AND turn_id = ? AND owner_id = ?
			AND generation = ? AND heartbeat = ? AND acquired_at = ? AND renewed_at = ? AND expires_at = ?`,
			active.ThreadID, active.TurnID, active.OwnerID, active.Generation, active.Heartbeat,
			formatTime(active.AcquiredAt), formatTime(active.RenewedAt), formatTime(active.ExpiresAt))
		if err != nil {
			return err
		}
		if count, err := deleted.RowsAffected(); err != nil {
			return err
		} else if count != 1 {
			return sessiontree.ErrStaleAuthority
		}
		now := authorityNow(req.Now, s.now)
		if _, err := tx.ExecContext(ctx, `UPDATE effect_attempts SET
			state = CASE state WHEN 'prepared' THEN 'cancelled' ELSE 'unknown' END,
			terminal_fingerprint = CASE state WHEN 'prepared' THEN ? ELSE ? END,
			updated_at = ?
			WHERE thread_id = ? AND turn_id = ? AND state IN ('prepared', 'dispatching')`,
			"turn-recovery-cancelled:"+plan.OutcomeFingerprint, "turn-recovery-unknown:"+plan.OutcomeFingerprint,
			formatTime(now), threadID, turnID); err != nil {
			return err
		}
		parentID := meta.LeafID
		result.Generation = generation
		for _, call := range sessiontree.UnresolvedInterruptedTurnCalls(path, turnID) {
			entry, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
				ThreadID: threadID, ParentID: parentID, Type: sessiontree.EntryToolResult,
				TurnID: turnID, Message: sessiontree.InterruptedTurnToolResult(call.Message), CreatedAt: now,
			})
			if err != nil {
				return err
			}
			result.ToolResults = append(result.ToolResults, entry)
			parentID = entry.ID
		}
		if message := strings.TrimSpace(plan.FailureMessage); message != "" {
			failure, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
				ThreadID: threadID, ParentID: parentID, Type: sessiontree.EntryRunFailure,
				TurnID: turnID, Error: message, CreatedAt: now,
			})
			if err != nil {
				return err
			}
			result.Failure = &failure
			parentID = failure.ID
		}
		metadata := map[string]string{
			"recoverable": "true",
			sessiontree.InterruptedTurnRecoveryParentKey: strings.TrimSpace(req.ParentThreadID),
		}
		metadata[sessiontree.InterruptedTurnRecoveryKindKey] = sessiontree.InterruptedTurnRecoveryKind
		metadata[sessiontree.InterruptedTurnRecoveryFingerprintKey] = plan.OutcomeFingerprint
		terminal, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
			ID: plan.TerminalEntryID, ThreadID: threadID, ParentID: parentID, Type: sessiontree.EntryTurnMarker,
			TurnID: turnID, TurnStatus: plan.Status, Metadata: metadata, CreatedAt: now,
		})
		if err != nil {
			return err
		}
		meta.LeafID = terminal.ID
		meta.UpdatedAt = now
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM provider_states WHERE thread_id = ?`, threadID); err != nil {
			return err
		}
		failureID := ""
		if result.Failure != nil {
			failureID = result.Failure.ID
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO turn_finishes(
			thread_id, turn_id, run_id, generation, outcome_fingerprint,
			failure_entry_id, terminal_entry_id, finished_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, threadID, turnID, plan.RunID, generation,
			plan.OutcomeFingerprint, failureID, terminal.ID, formatTime(now)); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		result.RunID = plan.RunID
		result.Status = plan.Status
		result.OutcomeFingerprint = plan.OutcomeFingerprint
		result.Terminal = terminal
		return nil
	})
	return result, err
}
