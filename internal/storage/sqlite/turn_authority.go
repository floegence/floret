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
	if err := validateSQLiteAdmitTurnRequest(req); err != nil {
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
			active, activeOK, err := loadTurnLease(ctx, tx, threadID)
			if err != nil {
				return err
			}
			if !activeOK || !sessiontree.SameTurnLease(active, existing.Lease) {
				return sessiontree.ErrRequestConflict
			}
			started, err := loadEntry(ctx, tx, threadID, existing.TurnStartedID)
			if errors.Is(err, sql.ErrNoRows) {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err != nil {
				return err
			}
			var user sessiontree.Entry
			var boundary sessiontree.Entry
			if existing.BoundaryTerminalID != "" {
				boundary, err = loadEntry(ctx, tx, threadID, existing.BoundaryTerminalID)
				if errors.Is(err, sql.ErrNoRows) {
					return sessiontree.ErrAuthorityCorrupt
				}
				if err != nil {
					return err
				}
			}
			if existing.UserMessageID != "" {
				user, err = loadEntry(ctx, tx, threadID, existing.UserMessageID)
				if errors.Is(err, sql.ErrNoRows) {
					return sessiontree.ErrAuthorityCorrupt
				}
				if err != nil {
					return err
				}
			}
			result = sessiontree.AdmitTurnResult{
				Lease: active, BoundaryTerminal: boundary, TurnStarted: started, UserMessage: user,
				BaseLeafID: existing.BaseLeafID, Replayed: true,
			}
			return nil
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
		var boundary sessiontree.Entry
		if retryLeafID := strings.TrimSpace(req.RetryLeafID); retryLeafID != "" {
			exists, err := entryExists(ctx, tx, threadID, retryLeafID)
			if err != nil {
				return err
			}
			if !exists {
				return sessiontree.ErrEntryNotFound
			}
			baseLeafID = retryLeafID
			admissionBaseLeafID = retryLeafID
			path, err := pathWithRunner(ctx, tx, threadID, baseLeafID)
			if err != nil {
				return err
			}
			boundaryID, err := nextEntryID(ctx, tx, threadID)
			if err != nil {
				return err
			}
			boundary, err = sessiontree.PrepareBranchBoundaryEntry(path, threadID, baseLeafID, boundaryID, "retry", authorityNow(req.Now, s.now))
			if err != nil {
				return err
			}
		}

		lease, err := s.acquireTurnLeaseWithRunner(ctx, tx, sessiontree.TurnLease{
			ThreadID: threadID, Purpose: sessiontree.TurnLeasePurposeTurn, TurnID: turnID, OwnerID: ownerID,
		})
		if err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		if boundary.ID != "" {
			boundary, err = insertTurnAuthorityEntry(ctx, tx, boundary)
			if err != nil {
				return err
			}
			baseLeafID = boundary.ID
		}
		started, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
			ThreadID: threadID, ParentID: baseLeafID, Type: sessiontree.EntryTurnMarker,
			TurnID: turnID, TurnStatus: sessiontree.TurnStarted,
			Metadata: map[string]string{"run_id": runID}, CreatedAt: now,
		})
		if err != nil {
			return err
		}
		var user sessiontree.Entry
		if strings.TrimSpace(req.RetryLeafID) == "" {
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
			nullableTurnEntryID(boundary.ID), started.ID, nullableTurnEntryID(user.ID), admissionBaseLeafID); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		result = sessiontree.AdmitTurnResult{
			Lease: lease, BoundaryTerminal: boundary, TurnStarted: started, UserMessage: user, BaseLeafID: admissionBaseLeafID,
		}
		return nil
	})
	return result, err
}

func (s *Store) FinishTurn(ctx context.Context, req sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	if err := validateSQLiteFinishTurnRequest(req); err != nil {
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

func validateSQLiteAdmitTurnRequest(req sessiontree.AdmitTurnRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.OwnerID) == "" {
		return errors.New("turn admission requires thread, turn, run, and owner identities")
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return errors.New("turn admission request fingerprint is required")
	}
	if strings.TrimSpace(req.RetryLeafID) == "" {
		if req.Input.Role != session.User {
			return errors.New("turn admission input must be a user message")
		}
		if strings.TrimSpace(req.Input.Content) == "" && len(req.Input.Attachments) == 0 {
			return errors.New("turn admission requires text or attachments")
		}
	} else if req.Input.Role != "" || strings.TrimSpace(req.Input.Content) != "" || len(req.Input.Attachments) != 0 {
		return errors.New("retry admission cannot contain a replacement user message")
	}
	return nil
}

func validateSQLiteFinishTurnRequest(req sessiontree.FinishTurnRequest) error {
	if err := req.Lease.Validate(); err != nil {
		return err
	}
	if req.Lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return errors.New("turn finish requires a turn lease")
	}
	if strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.TerminalEntryID) == "" || strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return errors.New("turn finish requires run, terminal entry, and outcome identities")
	}
	switch req.Status {
	case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
	default:
		return fmt.Errorf("invalid terminal turn status %q", req.Status)
	}
	if req.ProviderState != nil && req.ClearProviderState {
		return errors.New("turn finish provider state mutation is ambiguous")
	}
	if req.ProviderState != nil {
		record := req.ProviderState
		if strings.TrimSpace(record.ThreadID) == "" || strings.TrimSpace(record.LeafEntryID) == "" || strings.TrimSpace(record.CompatibilityKey) == "" ||
			strings.TrimSpace(record.State.Kind) == "" || strings.TrimSpace(record.State.ID) == "" || strings.TrimSpace(record.CreatedByRunID) == "" ||
			strings.TrimSpace(record.CreatedByTurnID) == "" || record.UpdatedAt.IsZero() {
			return errors.New("provider state record is incomplete")
		}
		if record.ThreadID != req.Lease.ThreadID || record.LeafEntryID != strings.TrimSpace(req.TerminalEntryID) ||
			record.CreatedByRunID != strings.TrimSpace(req.RunID) || record.CreatedByTurnID != req.Lease.TurnID {
			return sessiontree.ErrInvalidThreadAuthority
		}
	}
	return nil
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

func loadSQLiteTurnAdmission(ctx context.Context, q sqlRunner, threadID, turnID string) (sqliteTurnAdmissionLedger, bool, error) {
	var ledger sqliteTurnAdmissionLedger
	var boundaryTerminalID, userMessageID sql.NullString
	var acquired, renewed, expires string
	err := q.QueryRowContext(ctx, `SELECT thread_id, turn_id, run_id, request_fingerprint,
		owner_id, generation, heartbeat, acquired_at, renewed_at, expires_at,
		boundary_terminal_id, turn_started_id, user_message_id, base_leaf_id
		FROM turn_admissions WHERE thread_id = ? AND turn_id = ?`, threadID, turnID).Scan(
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
