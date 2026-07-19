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
			resolution, err := validateSQLiteInterruptedTurnResolution(ctx, tx, req, finished)
			if err != nil {
				return err
			}
			if resolution.recoveryReplay && resolution.exactProof {
				result = resolution.result
				return nil
			}
			return sessiontree.ErrRecoveryTargetResolved
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
		admission, found, err := loadSQLiteTurnAdmission(ctx, tx, threadID, turnID)
		if err != nil {
			return err
		}
		if !found || validateSQLiteInterruptedTurnAdmission(admission, active) != nil {
			return sessiontree.ErrAuthorityCorrupt
		}
		path, err := pathWithRunner(ctx, tx, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		plan, err := sessiontree.DeriveInterruptedTurnRecoveryPlan(path, req.ExpectedLease, req.ParentThreadID)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidateInterruptedTurnAdmissionPath(path, admission.ThreadID, turnID, admission.RunID, admission.TurnStartedID); err != nil {
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

func (s *Store) ValidateInterruptedTurnResolution(ctx context.Context, req sessiontree.RecoverInterruptedTurnRequest) error {
	if err := sessiontree.ValidateRecoverInterruptedTurnRequest(req); err != nil {
		return err
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		threadID := strings.TrimSpace(req.ExpectedLease.ThreadID)
		if _, err := loadThread(ctx, tx, threadID); errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, threadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
				return tombstoneErr
			}
			return sessiontree.ErrAuthorityCorrupt
		} else if err != nil {
			return err
		}
		finished, found, err := loadSQLiteTurnFinish(ctx, tx, req.ExpectedLease.ThreadID, req.ExpectedLease.TurnID)
		if err != nil {
			return err
		}
		if !found {
			return sessiontree.ErrAuthorityCorrupt
		}
		_, err = validateSQLiteInterruptedTurnResolution(ctx, tx, req, finished)
		return err
	})
}

type sqliteInterruptedTurnResolution struct {
	result         sessiontree.RecoverInterruptedTurnResult
	recoveryReplay bool
	exactProof     bool
}

func validateSQLiteInterruptedTurnResolution(
	ctx context.Context,
	tx sqlRunner,
	req sessiontree.RecoverInterruptedTurnRequest,
	finished sqliteTurnFinishLedger,
) (sqliteInterruptedTurnResolution, error) {
	expectedLease := req.ExpectedLease
	parentThreadID := strings.TrimSpace(req.ParentThreadID)
	meta, err := loadThread(ctx, tx, expectedLease.ThreadID)
	if errors.Is(err, sessiontree.ErrThreadNotFound) {
		if _, tombstoneErr := loadThreadTombstone(ctx, tx, expectedLease.ThreadID); tombstoneErr == nil {
			return sqliteInterruptedTurnResolution{}, sessiontree.ErrThreadDeleted
		} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
			return sqliteInterruptedTurnResolution{}, tombstoneErr
		}
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
	if err != nil {
		return sqliteInterruptedTurnResolution{}, err
	}
	if sessiontree.ValidateThreadMetaAuthority(meta) != nil {
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
	if strings.TrimSpace(meta.ParentThreadID) != parentThreadID {
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrInvalidThreadAuthority
	}
	if parentThreadID != "" {
		parent, err := loadThread(ctx, tx, parentThreadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return sqliteInterruptedTurnResolution{}, err
		}
		if sessiontree.ValidateThreadMetaAuthority(parent) != nil {
			return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	snapshot, err := inspectLiveThreadAuthority(ctx, tx, meta)
	if err != nil {
		return sqliteInterruptedTurnResolution{}, err
	}
	if snapshot.LeaseGeneration < expectedLease.Generation {
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
	if snapshot.Lease != nil {
		if sessiontree.ValidateInterruptedTurnLeaseSuccessor(expectedLease, *snapshot.Lease) != nil ||
			snapshot.Lease.Generation == expectedLease.Generation {
			return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	admission, found, err := loadSQLiteTurnAdmission(ctx, tx, expectedLease.ThreadID, expectedLease.TurnID)
	if err != nil {
		return sqliteInterruptedTurnResolution{}, err
	}
	if !found {
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
	if err := validateSQLiteInterruptedTurnAdmissionSuccessor(admission, expectedLease); err != nil {
		return sqliteInterruptedTurnResolution{}, err
	}
	started, err := loadEntry(ctx, tx, admission.ThreadID, admission.TurnStartedID)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
	if err != nil {
		return sqliteInterruptedTurnResolution{}, err
	}
	if sessiontree.ValidateInterruptedTurnStartedEntry(started, admission.ThreadID, admission.TurnID, admission.RunID, admission.TurnStartedID) != nil {
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
	if snapshot.LeaseGeneration < finished.Generation ||
		(snapshot.Lease != nil && snapshot.Lease.Generation <= finished.Generation) {
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
	switch finished.Generation {
	case admission.Lease.Generation:
		if err := validateSQLiteNormalInterruptedTurnFinish(ctx, tx, finished, admission); err != nil {
			return sqliteInterruptedTurnResolution{}, err
		}
		return sqliteInterruptedTurnResolution{}, nil
	case admission.Lease.Generation + 1:
		result, err := validateSQLiteInterruptedTurnRecoveryReplay(ctx, tx, finished, admission.Lease, parentThreadID)
		if err != nil {
			if errors.Is(err, sessiontree.ErrRequestConflict) {
				return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
			}
			return sqliteInterruptedTurnResolution{}, err
		}
		return sqliteInterruptedTurnResolution{
			result: result, recoveryReplay: true, exactProof: sessiontree.SameTurnLease(admission.Lease, expectedLease),
		}, nil
	default:
		return sqliteInterruptedTurnResolution{}, sessiontree.ErrAuthorityCorrupt
	}
}

func validateSQLiteInterruptedTurnAdmission(admission sqliteTurnAdmissionLedger, active sessiontree.TurnLease) error {
	if admission.ThreadID != active.ThreadID || admission.TurnID != active.TurnID ||
		strings.TrimSpace(admission.RunID) == "" || strings.TrimSpace(admission.RequestFingerprint) == "" ||
		strings.TrimSpace(admission.TurnStartedID) == "" || !sessiontree.SameTurnLease(admission.Lease, active) {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func validateSQLiteInterruptedTurnAdmissionSuccessor(admission sqliteTurnAdmissionLedger, expectedLease sessiontree.TurnLease) error {
	if admission.ThreadID != expectedLease.ThreadID || admission.TurnID != expectedLease.TurnID ||
		strings.TrimSpace(admission.RunID) == "" || strings.TrimSpace(admission.RequestFingerprint) == "" ||
		strings.TrimSpace(admission.TurnStartedID) == "" {
		return sessiontree.ErrAuthorityCorrupt
	}
	if admission.Lease.Generation != expectedLease.Generation ||
		sessiontree.ValidateInterruptedTurnLeaseSuccessor(expectedLease, admission.Lease) != nil {
		return sessiontree.ErrRequestConflict
	}
	return nil
}

func validateSQLiteNormalInterruptedTurnFinish(
	ctx context.Context,
	tx sqlRunner,
	finished sqliteTurnFinishLedger,
	admission sqliteTurnAdmissionLedger,
) error {
	if finished.ThreadID != admission.ThreadID || finished.TurnID != admission.TurnID ||
		finished.RunID != admission.RunID || finished.Generation != admission.Lease.Generation ||
		strings.TrimSpace(finished.OutcomeFingerprint) == "" || strings.TrimSpace(finished.TerminalEntryID) == "" {
		return sessiontree.ErrAuthorityCorrupt
	}
	terminal, err := loadEntry(ctx, tx, admission.ThreadID, finished.TerminalEntryID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ErrAuthorityCorrupt
	}
	if err != nil {
		return err
	}
	if terminal.Type != sessiontree.EntryTurnMarker || terminal.TurnID != admission.TurnID ||
		!isSQLiteTerminalTurnStatus(terminal.TurnStatus) {
		return sessiontree.ErrAuthorityCorrupt
	}
	if finished.FailureEntryID != "" {
		failure, err := loadEntry(ctx, tx, admission.ThreadID, finished.FailureEntryID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return err
		}
		if failure.Type != sessiontree.EntryRunFailure || failure.TurnID != admission.TurnID || terminal.ParentID != failure.ID {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
	return nil
}

func validateSQLiteInterruptedTurnRecoveryReplay(
	ctx context.Context,
	tx sqlRunner,
	finished sqliteTurnFinishLedger,
	expectedLease sessiontree.TurnLease,
	parentThreadID string,
) (sessiontree.RecoverInterruptedTurnResult, error) {
	terminal, err := loadEntry(ctx, tx, expectedLease.ThreadID, finished.TerminalEntryID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.RecoverInterruptedTurnResult{}, sessiontree.ErrAuthorityCorrupt
	}
	if err != nil {
		return sessiontree.RecoverInterruptedTurnResult{}, err
	}
	if terminal.Type != sessiontree.EntryTurnMarker || terminal.TurnID != expectedLease.TurnID ||
		terminal.Metadata[sessiontree.InterruptedTurnRecoveryKindKey] != sessiontree.InterruptedTurnRecoveryKind ||
		terminal.Metadata[sessiontree.InterruptedTurnRecoveryFingerprintKey] != finished.OutcomeFingerprint ||
		!isSQLiteTerminalTurnStatus(terminal.TurnStatus) {
		return sessiontree.RecoverInterruptedTurnResult{}, sessiontree.ErrAuthorityCorrupt
	}
	if terminal.Metadata[sessiontree.InterruptedTurnRecoveryParentKey] != strings.TrimSpace(parentThreadID) {
		return sessiontree.RecoverInterruptedTurnResult{}, sessiontree.ErrInvalidThreadAuthority
	}
	failureMessage := ""
	var failure *sessiontree.Entry
	if finished.FailureEntryID != "" {
		entry, err := loadEntry(ctx, tx, expectedLease.ThreadID, finished.FailureEntryID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessiontree.RecoverInterruptedTurnResult{}, sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return sessiontree.RecoverInterruptedTurnResult{}, err
		}
		if entry.Type != sessiontree.EntryRunFailure || entry.TurnID != expectedLease.TurnID || terminal.ParentID != entry.ID {
			return sessiontree.RecoverInterruptedTurnResult{}, sessiontree.ErrAuthorityCorrupt
		}
		failureMessage = entry.Error
		failure = &entry
	}
	fingerprint, err := sessiontree.InterruptedTurnRecoveryFingerprint(
		expectedLease, parentThreadID, finished.RunID, terminal.TurnStatus, failureMessage,
	)
	if err != nil {
		return sessiontree.RecoverInterruptedTurnResult{}, err
	}
	if fingerprint != finished.OutcomeFingerprint {
		return sessiontree.RecoverInterruptedTurnResult{}, sessiontree.ErrRequestConflict
	}
	if terminal.ID != "recovery-terminal-"+fingerprint[:24] {
		return sessiontree.RecoverInterruptedTurnResult{}, sessiontree.ErrAuthorityCorrupt
	}
	return sessiontree.RecoverInterruptedTurnResult{
		RunID: finished.RunID, Status: terminal.TurnStatus, OutcomeFingerprint: fingerprint,
		Failure: failure, Terminal: terminal, Generation: finished.Generation, Replayed: true,
	}, nil
}

func isSQLiteTerminalTurnStatus(status sessiontree.TurnMarkerStatus) bool {
	switch status {
	case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
		return true
	default:
		return false
	}
}
