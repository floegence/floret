package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

type sqliteTurnAdmissionLedger struct {
	ThreadID           string
	TurnID             string
	RunID              string
	RequestFingerprint string
	Lease              sessiontree.TurnLease
	BoundaryTerminalID string
	TurnStartedID      string
	UserMessageID      string
	BaseLeafID         string
}

type sqliteTurnFinishLedger struct {
	ThreadID           string
	TurnID             string
	RunID              string
	Generation         int64
	OutcomeFingerprint string
	FailureEntryID     string
	TerminalEntryID    string
	FinishedAt         time.Time
}

func (s *Store) AdmitTurn(ctx context.Context, req sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error) {
	if err := sessiontree.ValidateAdmitTurnRequestEnvelope(req); err != nil {
		return sessiontree.AdmitTurnResult{}, err
	}
	threadID := strings.TrimSpace(req.ThreadID)
	turnID := strings.TrimSpace(req.TurnID)
	runID := strings.TrimSpace(req.RunID)
	ownerID := strings.TrimSpace(req.OwnerID)
	fingerprint := strings.TrimSpace(req.RequestFingerprint)
	var result sessiontree.AdmitTurnResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, found, err := loadSQLiteTurnAdmission(ctx, tx, threadID, turnID)
		if err != nil {
			return err
		}
		if found {
			if existing.RunID != runID || existing.RequestFingerprint != fingerprint {
				return sessiontree.ErrRequestConflict
			}
			if err := sessiontree.ValidateAdmitTurnReplayRequest(req); err != nil {
				return err
			}
			result, err = loadSQLiteTurnAdmissionReplay(ctx, tx, existing)
			return err
		}
		if err := sessiontree.ValidateAdmitTurnRequest(req); err != nil {
			return err
		}

		meta, err := loadThread(ctx, tx, threadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, threadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
				return tombstoneErr
			}
		}
		if err != nil {
			return err
		}
		if err := rejectSQLiteTurnAdmissionLifecycle(meta); err != nil {
			return err
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, threadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrThreadAuthorityBusy
		}
		if _, active, err := loadTurnLease(ctx, tx, threadID); err != nil {
			return err
		} else if active {
			return sessiontree.ErrActiveTurn
		}

		baseLeafID := meta.LeafID
		admissionBaseLeafID := baseLeafID
		retrySourceTurnID := strings.TrimSpace(req.RetrySourceTurnID)
		retrySourceEntryID := strings.TrimSpace(req.RetrySourceEntryID)
		if retrySourceEntryID != "" {
			activePath, err := pathWithRunner(ctx, tx, threadID, meta.LeafID)
			if err != nil {
				return err
			}
			_, err = sessiontree.ValidateRetrySourcePath(activePath, retrySourceTurnID, retrySourceEntryID)
			if err != nil {
				return err
			}
			admissionBaseLeafID = retrySourceEntryID
		}

		lease, err := s.acquireTurnLeaseWithRunner(ctx, tx, sessiontree.TurnLease{
			ThreadID: threadID, Purpose: sessiontree.TurnLeasePurposeTurn, TurnID: turnID, OwnerID: ownerID,
		})
		if err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		startedMetadata := map[string]string{"run_id": runID}
		if retrySourceEntryID != "" {
			startedMetadata[sessiontree.RetrySourceTurnIDMetadataKey] = retrySourceTurnID
			startedMetadata[sessiontree.RetrySourceEntryIDMetadataKey] = retrySourceEntryID
		}
		started, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
			ThreadID: threadID, ParentID: baseLeafID, Type: sessiontree.EntryTurnMarker,
			TurnID: turnID, TurnStatus: sessiontree.TurnStarted,
			Metadata: startedMetadata, CreatedAt: now,
		})
		if err != nil {
			return err
		}
		var user sessiontree.Entry
		if retrySourceEntryID == "" {
			user, err = insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
				ThreadID: threadID, ParentID: started.ID, Type: sessiontree.EntryUserMessage,
				TurnID: turnID, Message: session.CloneMessage(req.Input), CreatedAt: now,
			})
			if err != nil {
				return err
			}
		}
		meta.LeafID = started.ID
		if user.ID != "" {
			meta.LeafID = user.ID
		}
		meta.UpdatedAt = now
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO turn_admissions(
			thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
			acquired_at, renewed_at, expires_at, boundary_terminal_id, turn_started_id, user_message_id, base_leaf_id
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			threadID, turnID, runID, fingerprint, lease.OwnerID, lease.Generation, lease.Heartbeat,
			formatTime(lease.AcquiredAt), formatTime(lease.RenewedAt), formatTime(lease.ExpiresAt),
			nil, started.ID, nullableTurnEntryID(user.ID), admissionBaseLeafID); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		result = sessiontree.AdmitTurnResult{
			Lease: lease, TurnStarted: started, UserMessage: user, BaseLeafID: admissionBaseLeafID,
		}
		return nil
	})
	return result, err
}

func (s *Store) ReadTurnAdmission(ctx context.Context, threadID, turnID, runID string) (sessiontree.AdmitTurnResult, bool, error) {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	runID = strings.TrimSpace(runID)
	var result sessiontree.AdmitTurnResult
	found := false
	err := s.withRead(ctx, func(q sqlRunner) error {
		if _, err := loadThread(ctx, q, threadID); err != nil {
			return err
		}
		existing, ok, err := loadSQLiteTurnAdmission(ctx, q, threadID, turnID)
		if err != nil || !ok {
			return err
		}
		found = true
		if existing.RunID != runID {
			return sessiontree.ErrRequestConflict
		}
		result, err = loadSQLiteTurnAdmissionReplay(ctx, q, existing)
		return err
	})
	return result, found, err
}

func loadSQLiteTurnAdmissionReplay(ctx context.Context, runner sqlRunner, existing sqliteTurnAdmissionLedger) (sessiontree.AdmitTurnResult, error) {
	var terminal *sessiontree.TurnTerminalOutcome
	finished, finishedOK, err := loadSQLiteTurnFinish(ctx, runner, existing.ThreadID, existing.TurnID)
	if err != nil {
		return sessiontree.AdmitTurnResult{}, err
	}
	if finishedOK {
		if finished.RunID != existing.RunID {
			return sessiontree.AdmitTurnResult{}, sessiontree.ErrAuthorityCorrupt
		}
		switch finished.Generation {
		case existing.Lease.Generation:
			if err := validateSQLiteNormalInterruptedTurnFinish(ctx, runner, finished, existing); err != nil {
				return sessiontree.AdmitTurnResult{}, err
			}
			terminalEntry, _ := loadEntry(ctx, runner, existing.ThreadID, finished.TerminalEntryID)
			terminal = &sessiontree.TurnTerminalOutcome{Terminal: terminalEntry}
			if finished.FailureEntryID != "" {
				failure, _ := loadEntry(ctx, runner, existing.ThreadID, finished.FailureEntryID)
				terminal.Failure = &failure
			}
		case existing.Lease.Generation + 1:
			meta, err := loadThread(ctx, runner, existing.ThreadID)
			if err != nil {
				return sessiontree.AdmitTurnResult{}, err
			}
			path, err := pathWithRunner(ctx, runner, existing.ThreadID, meta.LeafID)
			if err != nil {
				return sessiontree.AdmitTurnResult{}, err
			}
			recovered, err := validateSQLiteInterruptedTurnRecoveryReplay(ctx, runner, finished, existing.Lease, meta.ParentThreadID, path)
			if err != nil {
				if errors.Is(err, sessiontree.ErrRequestConflict) || errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
					return sessiontree.AdmitTurnResult{}, sessiontree.ErrAuthorityCorrupt
				}
				return sessiontree.AdmitTurnResult{}, err
			}
			terminal = &sessiontree.TurnTerminalOutcome{Terminal: recovered.Terminal, Failure: recovered.Failure}
		default:
			return sessiontree.AdmitTurnResult{}, sessiontree.ErrAuthorityCorrupt
		}
	} else {
		active, activeOK, err := loadTurnLease(ctx, runner, existing.ThreadID)
		if err != nil {
			return sessiontree.AdmitTurnResult{}, err
		}
		if !activeOK || !sessiontree.SameTurnLease(active, existing.Lease) {
			return sessiontree.AdmitTurnResult{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	started, err := loadEntry(ctx, runner, existing.ThreadID, existing.TurnStartedID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.AdmitTurnResult{}, sessiontree.ErrAuthorityCorrupt
	} else if err != nil {
		return sessiontree.AdmitTurnResult{}, err
	}
	var boundary sessiontree.Entry
	if existing.BoundaryTerminalID != "" {
		boundary, err = loadEntry(ctx, runner, existing.ThreadID, existing.BoundaryTerminalID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessiontree.AdmitTurnResult{}, sessiontree.ErrAuthorityCorrupt
		} else if err != nil {
			return sessiontree.AdmitTurnResult{}, err
		}
	}
	var user sessiontree.Entry
	if existing.UserMessageID != "" {
		user, err = loadEntry(ctx, runner, existing.ThreadID, existing.UserMessageID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessiontree.AdmitTurnResult{}, sessiontree.ErrAuthorityCorrupt
		} else if err != nil {
			return sessiontree.AdmitTurnResult{}, err
		}
	}
	for _, entry := range []sessiontree.Entry{started, boundary, user} {
		if entry.ID != "" {
			if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
				return sessiontree.AdmitTurnResult{}, err
			}
		}
	}
	if terminal != nil {
		if err := sessiontree.ValidateEntryIntegrity(terminal.Terminal); err != nil {
			return sessiontree.AdmitTurnResult{}, err
		}
		if terminal.Failure != nil {
			if err := sessiontree.ValidateEntryIntegrity(*terminal.Failure); err != nil {
				return sessiontree.AdmitTurnResult{}, err
			}
		}
	}
	return sessiontree.AdmitTurnResult{
		Lease: existing.Lease, BoundaryTerminal: boundary, TurnStarted: started, UserMessage: user,
		BaseLeafID: existing.BaseLeafID, Terminal: terminal, Replayed: true,
	}, nil
}

func (s *Store) FinishTurn(ctx context.Context, req sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	if err := sessiontree.ValidateFinishTurnRequest(req); err != nil {
		return sessiontree.FinishTurnResult{}, err
	}
	threadID := strings.TrimSpace(req.Lease.ThreadID)
	turnID := strings.TrimSpace(req.Lease.TurnID)
	runID := strings.TrimSpace(req.RunID)
	fingerprint := strings.TrimSpace(req.OutcomeFingerprint)
	var result sessiontree.FinishTurnResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		finished, found, err := loadSQLiteTurnFinish(ctx, tx, threadID, turnID)
		if err != nil {
			return err
		}
		if found {
			if finished.RunID != runID || finished.Generation != req.Lease.Generation || finished.OutcomeFingerprint != fingerprint {
				return sessiontree.ErrRequestConflict
			}
			pendingApprovals, err := sqliteTurnHasVisibleApproval(ctx, tx, threadID, turnID)
			if err != nil {
				return err
			}
			if pendingApprovals {
				return sessiontree.ErrAuthorityCorrupt
			}
			terminal, err := loadEntry(ctx, tx, threadID, finished.TerminalEntryID)
			if errors.Is(err, sql.ErrNoRows) {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err != nil {
				return err
			}
			result = sessiontree.FinishTurnResult{Terminal: terminal, Replayed: true}
			if finished.FailureEntryID != "" {
				failure, err := loadEntry(ctx, tx, threadID, finished.FailureEntryID)
				if errors.Is(err, sql.ErrNoRows) {
					return sessiontree.ErrAuthorityCorrupt
				}
				if err != nil {
					return err
				}
				result.Failure = &failure
			}
			return nil
		}

		active, activeOK, err := loadTurnLease(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if !activeOK || !sessiontree.SameTurnLease(active, req.Lease) || !active.Fresh(s.now().UTC()) {
			return sessiontree.ErrStaleAuthority
		}
		meta, err := loadThread(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteTurnFinishLifecycle(meta); err != nil {
			return err
		}
		pendingApprovals, err := sqliteTurnHasVisibleApproval(ctx, tx, threadID, turnID)
		if err != nil {
			return err
		}
		if pendingApprovals {
			return sessiontree.ErrRequestConflict
		}
		var dispatching int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM effect_attempts
			WHERE thread_id = ? AND turn_id = ? AND state = 'dispatching'`, threadID, turnID).Scan(&dispatching); err != nil {
			return err
		}
		if dispatching != 0 {
			return sessiontree.ErrEffectOutcomeUnknown
		}
		terminalEntryID := strings.TrimSpace(req.TerminalEntryID)
		if exists, err := entryExists(ctx, tx, threadID, terminalEntryID); err != nil {
			return err
		} else if exists {
			return sessiontree.ErrRequestConflict
		}

		now := authorityNow(req.Now, s.now)
		if _, err := tx.ExecContext(ctx, `UPDATE effect_attempts SET state = 'cancelled', terminal_fingerprint = ?, updated_at = ?
			WHERE thread_id = ? AND turn_id = ? AND state = 'prepared'`, "turn-finish:"+fingerprint, formatTime(now), threadID, turnID); err != nil {
			return err
		}
		var unsafe int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM effect_attempts WHERE thread_id = ? AND turn_id = ?
			AND state NOT IN ('completed', 'failed', 'rejected', 'cancelled', 'unknown')`, threadID, turnID).Scan(&unsafe); err != nil {
			return err
		}
		if unsafe != 0 {
			return sessiontree.ErrAuthorityCorrupt
		}
		parentID := meta.LeafID
		if failureMessage := strings.TrimSpace(req.FailureMessage); failureMessage != "" {
			failure, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
				ThreadID: threadID, ParentID: parentID, Type: sessiontree.EntryRunFailure,
				TurnID: turnID, Error: failureMessage, CreatedAt: now,
			})
			if err != nil {
				return err
			}
			result.Failure = &failure
			parentID = failure.ID
		}
		terminal, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
			ID: terminalEntryID, ThreadID: threadID, ParentID: parentID, Type: sessiontree.EntryTurnMarker,
			TurnID: turnID, TurnStatus: req.Status, Metadata: cloneStringMapSQLite(req.Metadata), CreatedAt: now,
		})
		if err != nil {
			return err
		}
		meta.LeafID = terminal.ID
		meta.UpdatedAt = now
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		if req.ProviderState != nil {
			rawState, err := json.Marshal(req.ProviderState.State)
			if err != nil {
				return err
			}
			if err := putProviderStateWithRunner(ctx, tx, *req.ProviderState, rawState); err != nil {
				return err
			}
		} else if req.ClearProviderState {
			if _, err := tx.ExecContext(ctx, `DELETE FROM provider_states WHERE thread_id = ?`, threadID); err != nil {
				return err
			}
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
		failureEntryID := ""
		if result.Failure != nil {
			failureEntryID = result.Failure.ID
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO turn_finishes(
			thread_id, turn_id, run_id, generation, outcome_fingerprint,
			failure_entry_id, terminal_entry_id, finished_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, threadID, turnID, runID, active.Generation,
			fingerprint, failureEntryID, terminal.ID, formatTime(now)); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		result.Terminal = terminal
		return nil
	})
	return result, err
}

func sqliteTurnHasVisibleApproval(ctx context.Context, q sqlRunner, threadID, turnID string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM approval_requests
		WHERE thread_id = ? AND turn_id = ? AND state IN ('requested', 'decision_submitted')`, threadID, turnID).Scan(&count); err != nil {
		return false, err
	}
	return count != 0, nil
}

func rejectSQLiteTurnAdmissionLifecycle(meta sessiontree.ThreadMeta) error {
	lifecycle, err := sqliteThreadLifecycle(meta)
	if err != nil {
		return err
	}
	switch lifecycle {
	case sessiontree.ThreadLifecycleOpen:
		return nil
	case sessiontree.ThreadLifecycleClosing:
		return sessiontree.ErrSubAgentClosing
	case sessiontree.ThreadLifecycleClosed:
		return sessiontree.ErrThreadClosed
	default:
		return sessiontree.ErrAuthorityCorrupt
	}
}

func rejectSQLiteTurnFinishLifecycle(meta sessiontree.ThreadMeta) error {
	lifecycle, err := sqliteThreadLifecycle(meta)
	if err != nil {
		return err
	}
	if lifecycle != sessiontree.ThreadLifecycleOpen && lifecycle != sessiontree.ThreadLifecycleClosing {
		return sessiontree.ErrThreadClosed
	}
	return nil
}

func sqliteThreadLifecycle(meta sessiontree.ThreadMeta) (sessiontree.ThreadLifecycle, error) {
	lifecycle, err := meta.CanonicalLifecycle()
	if err != nil || lifecycle == sessiontree.ThreadLifecycleDeleted {
		return "", sessiontree.ErrAuthorityCorrupt
	}
	return lifecycle, nil
}

func insertTurnAuthorityEntry(ctx context.Context, tx sqlRunner, entry sessiontree.Entry) (sessiontree.Entry, error) {
	if strings.TrimSpace(entry.ThreadID) == "" {
		return sessiontree.Entry{}, errors.New("turn authority entry requires thread identity")
	}
	if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
		runID := strings.TrimSpace(entry.Metadata["run_id"])
		if runID == "" {
			return sessiontree.Entry{}, fmt.Errorf("%w: started turn requires run identity", sessiontree.ErrInvalidThreadAuthority)
		}
		exists, err := turnStartedExists(ctx, tx, entry.ThreadID, entry.TurnID)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		if exists {
			return sessiontree.Entry{}, sessiontree.ErrRequestConflict
		}
		entry = cloneEntry(entry)
		entry.Metadata["run_id"] = runID
	}
	if entry.ParentID != "" {
		exists, err := entryExists(ctx, tx, entry.ThreadID, entry.ParentID)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		if !exists {
			return sessiontree.Entry{}, sessiontree.ErrInvalidParent
		}
	}
	if entry.ID == "" {
		id, err := nextEntryID(ctx, tx, entry.ThreadID)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		entry.ID = id
	} else if exists, err := entryExists(ctx, tx, entry.ThreadID, entry.ID); err != nil {
		return sessiontree.Entry{}, err
	} else if exists {
		return sessiontree.Entry{}, sessiontree.ErrRequestConflict
	}
	if entry.CreatedAt.IsZero() {
		return sessiontree.Entry{}, errors.New("turn authority entry requires store timestamp")
	}
	entry = sessiontree.PrepareEntry(entry)
	ordinal, err := nextOrdinal(ctx, tx, entry.ThreadID)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if err := insertEntry(ctx, tx, entry, ordinal, false); err != nil {
		return sessiontree.Entry{}, err
	}
	return cloneEntry(entry), nil
}

func turnStartedExists(ctx context.Context, q sqlRunner, threadID, turnID string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries
		WHERE thread_id = ? AND turn_id = ? AND type = ? AND turn_status = ?`,
		strings.TrimSpace(threadID), strings.TrimSpace(turnID), string(sessiontree.EntryTurnMarker), string(sessiontree.TurnStarted)).Scan(&count); err != nil {
		return false, err
	}
	if count > 1 {
		return false, sessiontree.ErrAuthorityCorrupt
	}
	return count == 1, nil
}

func loadSQLiteTurnAdmission(ctx context.Context, q sqlRunner, threadID, turnID string) (sqliteTurnAdmissionLedger, bool, error) {
	var ledger sqliteTurnAdmissionLedger
	var boundaryTerminalID, userMessageID sql.NullString
	var acquired, renewed, expires string
	err := q.QueryRowContext(ctx, sqliteTurnAdmissionQuery(), threadID, turnID).Scan(
		&ledger.ThreadID, &ledger.TurnID, &ledger.RunID, &ledger.RequestFingerprint,
		&ledger.Lease.OwnerID, &ledger.Lease.Generation, &ledger.Lease.Heartbeat,
		&acquired, &renewed, &expires, &boundaryTerminalID, &ledger.TurnStartedID, &userMessageID, &ledger.BaseLeafID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteTurnAdmissionLedger{}, false, nil
	}
	if err != nil {
		return sqliteTurnAdmissionLedger{}, false, err
	}
	ledger.Lease.ThreadID = ledger.ThreadID
	ledger.BoundaryTerminalID = boundaryTerminalID.String
	ledger.UserMessageID = userMessageID.String
	ledger.Lease.Purpose = sessiontree.TurnLeasePurposeTurn
	ledger.Lease.TurnID = ledger.TurnID
	ledger.Lease.AcquiredAt = parseTime(acquired)
	ledger.Lease.RenewedAt = parseTime(renewed)
	ledger.Lease.ExpiresAt = parseTime(expires)
	if err := ledger.Lease.Validate(); err != nil {
		return sqliteTurnAdmissionLedger{}, false, fmt.Errorf("%w: invalid turn admission proof: %v", sessiontree.ErrAuthorityCorrupt, err)
	}
	return ledger, true, nil
}

func sqliteTurnAdmissionQuery() string {
	return `SELECT thread_id, turn_id, run_id, request_fingerprint,
		owner_id, generation, heartbeat, acquired_at, renewed_at, expires_at,
		boundary_terminal_id, turn_started_id, user_message_id, base_leaf_id
		FROM turn_admissions WHERE thread_id = ? AND turn_id = ?`
}

func loadSQLiteTurnFinish(ctx context.Context, q sqlRunner, threadID, turnID string) (sqliteTurnFinishLedger, bool, error) {
	var ledger sqliteTurnFinishLedger
	var finishedAt string
	err := q.QueryRowContext(ctx, `SELECT thread_id, turn_id, run_id, generation,
		outcome_fingerprint, failure_entry_id, terminal_entry_id, finished_at
		FROM turn_finishes WHERE thread_id = ? AND turn_id = ?`, threadID, turnID).Scan(
		&ledger.ThreadID, &ledger.TurnID, &ledger.RunID, &ledger.Generation,
		&ledger.OutcomeFingerprint, &ledger.FailureEntryID, &ledger.TerminalEntryID, &finishedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteTurnFinishLedger{}, false, nil
	}
	if err != nil {
		return sqliteTurnFinishLedger{}, false, err
	}
	ledger.FinishedAt = parseTime(finishedAt)
	if ledger.Generation <= 0 || ledger.RunID == "" || ledger.OutcomeFingerprint == "" || ledger.TerminalEntryID == "" || ledger.FinishedAt.IsZero() {
		return sqliteTurnFinishLedger{}, false, sessiontree.ErrAuthorityCorrupt
	}
	return ledger, true, nil
}

func nullableTurnEntryID(entryID string) any {
	entryID = strings.TrimSpace(entryID)
	if entryID == "" {
		return nil
	}
	return entryID
}

var _ sessiontree.TurnAuthorityRepo = (*Store)(nil)
