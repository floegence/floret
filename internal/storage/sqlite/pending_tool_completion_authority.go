package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

type sqlitePendingToolCompletionLedger struct {
	CompletionRequestID   string
	RequestFingerprint    string
	Target                sessiontree.PendingToolSettlementTarget
	SettlementFingerprint string
	SettlementEntryID     string
	ContinuationTurnID    string
	ContinuationRunID     string
	TurnStartedID         string
	UserMessageID         string
	BaseLeafID            string
	CreatedAt             time.Time
}

func (s *Store) ReadPendingToolCompletion(ctx context.Context, req sessiontree.AdmitPendingToolCompletionRequest) (sessiontree.AdmitPendingToolCompletionResult, bool, error) {
	if err := sessiontree.ValidateAdmitPendingToolCompletionRequest(req); err != nil {
		return sessiontree.AdmitPendingToolCompletionResult{}, false, err
	}
	var result sessiontree.AdmitPendingToolCompletionResult
	var found bool
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, ok, err := loadSQLitePendingToolCompletion(ctx, tx, strings.TrimSpace(req.CompletionRequestID))
		if err != nil || !ok {
			return err
		}
		found = true
		result, err = sqlitePendingToolCompletionReplay(ctx, tx, existing, req)
		return err
	})
	return result, found, err
}

func (s *Store) AdmitPendingToolCompletion(ctx context.Context, req sessiontree.AdmitPendingToolCompletionRequest) (sessiontree.AdmitPendingToolCompletionResult, error) {
	if err := sessiontree.ValidateAdmitPendingToolCompletionRequest(req); err != nil {
		return sessiontree.AdmitPendingToolCompletionResult{}, err
	}
	requestID := strings.TrimSpace(req.CompletionRequestID)
	fingerprint := strings.TrimSpace(req.RequestFingerprint)
	threadID := strings.TrimSpace(req.Target.ThreadID)
	turnID := strings.TrimSpace(req.ContinuationTurnID)
	runID := strings.TrimSpace(req.ContinuationRunID)
	settlementFingerprint := strings.TrimSpace(req.SettlementFingerprint)
	var result sessiontree.AdmitPendingToolCompletionResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, found, err := loadSQLitePendingToolCompletion(ctx, tx, requestID)
		if err != nil {
			return err
		}
		if found {
			result, err = sqlitePendingToolCompletionReplay(ctx, tx, existing, req)
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
			return sessiontree.ErrThreadAuthorityBusy
		}
		if _, found, err := loadSQLiteTurnAdmission(ctx, tx, threadID, turnID); err != nil {
			return err
		} else if found {
			return sessiontree.ErrRequestConflict
		}

		path, err := pathWithRunner(ctx, tx, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidatePendingToolCompletionPath(path); err != nil {
			return err
		}
		settlementRequest := sessiontree.SettlePendingToolRecoveryRequest{
			Target: req.Target, RequestFingerprint: settlementFingerprint, Settlement: req.Settlement,
		}
		existingSettlement, settlementReplayed, err := sqlitePendingToolRecoveryTarget(path, settlementRequest)
		if err != nil {
			return err
		}

		lease, err := s.acquireTurnLeaseWithRunner(ctx, tx, sessiontree.TurnLease{
			ThreadID: threadID, Purpose: sessiontree.TurnLeasePurposeTurn, TurnID: turnID, OwnerID: strings.TrimSpace(req.OwnerID),
		})
		if err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		settlement := cloneEntry(existingSettlement)
		baseLeafID := meta.LeafID
		if !settlementReplayed {
			settlement = cloneEntry(req.Settlement)
			settlement.ID = ""
			settlement.ParentID = meta.LeafID
			settlement.CreatedAt = now
			settlement, err = insertTurnAuthorityEntry(ctx, tx, settlement)
			if err != nil {
				return err
			}
			baseLeafID = settlement.ID
		}
		started, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
			ThreadID: threadID, ParentID: baseLeafID, Type: sessiontree.EntryTurnMarker,
			TurnID: turnID, TurnStatus: sessiontree.TurnStarted, CreatedAt: now,
			Metadata: map[string]string{"run_id": runID, "completion_request_id": requestID},
		})
		if err != nil {
			return err
		}
		user, err := insertTurnAuthorityEntry(ctx, tx, sessiontree.Entry{
			ThreadID: threadID, ParentID: started.ID, Type: sessiontree.EntryUserMessage,
			TurnID: turnID, Message: session.CloneMessage(req.Input), CreatedAt: now,
			Metadata: map[string]string{"completion_request_id": requestID},
		})
		if err != nil {
			return err
		}
		meta.LeafID = user.ID
		meta.UpdatedAt = now
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO turn_admissions(
			thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
			acquired_at, renewed_at, expires_at, boundary_terminal_id, turn_started_id, user_message_id, base_leaf_id
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)`,
			threadID, turnID, runID, fingerprint, lease.OwnerID, lease.Generation, lease.Heartbeat,
			formatTime(lease.AcquiredAt), formatTime(lease.RenewedAt), formatTime(lease.ExpiresAt),
			started.ID, user.ID, baseLeafID); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pending_tool_completions(
				completion_request_id, request_fingerprint, thread_id,
				target_turn_id, target_run_id, target_tool_call_id, target_tool_name, target_handle, target_effect_attempt_id,
				settlement_fingerprint, settlement_entry_id, continuation_turn_id, continuation_run_id,
				turn_started_id, user_message_id, base_leaf_id, created_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			requestID, fingerprint, threadID,
			req.Target.TurnID, req.Target.RunID, req.Target.ToolCallID, req.Target.ToolName, req.Target.Handle, req.Target.EffectAttemptID,
			settlementFingerprint, settlement.ID, turnID, runID, started.ID, user.ID, baseLeafID, formatTime(now)); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		result = sessiontree.AdmitPendingToolCompletionResult{
			Settlement:         settlement,
			SettlementReplayed: settlementReplayed,
			Admission: sessiontree.AdmitTurnResult{
				Lease: lease, TurnStarted: started, UserMessage: user, BaseLeafID: baseLeafID,
			},
		}
		return nil
	})
	return result, err
}

func sqlitePendingToolCompletionReplay(ctx context.Context, q sqlRunner, existing sqlitePendingToolCompletionLedger, req sessiontree.AdmitPendingToolCompletionRequest) (sessiontree.AdmitPendingToolCompletionResult, error) {
	if existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) || existing.Target != req.Target ||
		existing.SettlementFingerprint != strings.TrimSpace(req.SettlementFingerprint) ||
		existing.ContinuationTurnID != strings.TrimSpace(req.ContinuationTurnID) || existing.ContinuationRunID != strings.TrimSpace(req.ContinuationRunID) {
		return sessiontree.AdmitPendingToolCompletionResult{}, sessiontree.ErrRequestConflict
	}
	settlement, err := loadRequiredAuthorityEntry(ctx, q, existing.Target.ThreadID, existing.SettlementEntryID)
	if err != nil {
		return sessiontree.AdmitPendingToolCompletionResult{}, err
	}
	admissionLedger, found, err := loadSQLiteTurnAdmission(ctx, q, existing.Target.ThreadID, existing.ContinuationTurnID)
	if err != nil {
		return sessiontree.AdmitPendingToolCompletionResult{}, err
	}
	if !found || admissionLedger.RunID != existing.ContinuationRunID || admissionLedger.TurnStartedID != existing.TurnStartedID || admissionLedger.UserMessageID != existing.UserMessageID {
		return sessiontree.AdmitPendingToolCompletionResult{}, sessiontree.ErrAuthorityCorrupt
	}
	admission, err := loadSQLiteTurnAdmissionReplay(ctx, q, admissionLedger)
	if err != nil {
		return sessiontree.AdmitPendingToolCompletionResult{}, err
	}
	admission.Lease = sessiontree.TurnLease{}
	return sessiontree.AdmitPendingToolCompletionResult{
		Settlement: settlement,
		Admission:  admission,
		Replayed:   true,
	}, nil
}

func loadSQLitePendingToolCompletion(ctx context.Context, q sqlRunner, requestID string) (sqlitePendingToolCompletionLedger, bool, error) {
	var ledger sqlitePendingToolCompletionLedger
	var created string
	err := q.QueryRowContext(ctx, `SELECT completion_request_id, request_fingerprint, thread_id,
		target_turn_id, target_run_id, target_tool_call_id, target_tool_name, target_handle, target_effect_attempt_id,
		settlement_fingerprint, settlement_entry_id, continuation_turn_id, continuation_run_id,
		turn_started_id, user_message_id, base_leaf_id, created_at
		FROM pending_tool_completions WHERE completion_request_id = ?`, requestID).Scan(
		&ledger.CompletionRequestID, &ledger.RequestFingerprint, &ledger.Target.ThreadID,
		&ledger.Target.TurnID, &ledger.Target.RunID, &ledger.Target.ToolCallID, &ledger.Target.ToolName, &ledger.Target.Handle, &ledger.Target.EffectAttemptID,
		&ledger.SettlementFingerprint, &ledger.SettlementEntryID, &ledger.ContinuationTurnID, &ledger.ContinuationRunID,
		&ledger.TurnStartedID, &ledger.UserMessageID, &ledger.BaseLeafID, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sqlitePendingToolCompletionLedger{}, false, nil
	}
	if err != nil {
		return sqlitePendingToolCompletionLedger{}, false, err
	}
	ledger.CreatedAt = parseTime(created)
	if ledger.CompletionRequestID == "" || ledger.RequestFingerprint == "" || ledger.Target.ThreadID == "" ||
		ledger.SettlementFingerprint == "" || ledger.SettlementEntryID == "" || ledger.ContinuationTurnID == "" ||
		ledger.ContinuationRunID == "" || ledger.TurnStartedID == "" || ledger.UserMessageID == "" || ledger.BaseLeafID == "" || ledger.CreatedAt.IsZero() {
		return sqlitePendingToolCompletionLedger{}, false, sessiontree.ErrAuthorityCorrupt
	}
	return ledger, true, nil
}

func loadRequiredAuthorityEntry(ctx context.Context, q sqlRunner, threadID, entryID string) (sessiontree.Entry, error) {
	entry, err := loadEntry(ctx, q, threadID, entryID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.Entry{}, sessiontree.ErrAuthorityCorrupt
	}
	return entry, err
}

var _ sessiontree.PendingToolCompletionAuthorityRepo = (*Store)(nil)
