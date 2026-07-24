package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

type sqliteSubAgentPendingToolCompletionLedger struct {
	InputRequestID        string
	RequestFingerprint    string
	SettlementFingerprint string
	ParentThreadID        string
	ChildThreadID         string
	Target                sessiontree.PendingToolSettlementTarget
	SettlementEntryID     string
	SubAgentInputID       string
}

func (s *Store) PublishSubAgentPendingToolCompletion(ctx context.Context, req sessiontree.PublishSubAgentPendingToolCompletionRequest) (sessiontree.PublishSubAgentPendingToolCompletionResult, error) {
	if err := sessiontree.ValidatePublishSubAgentPendingToolCompletionEnvelope(req); err != nil {
		return sessiontree.PublishSubAgentPendingToolCompletionResult{}, err
	}
	requestID := strings.TrimSpace(req.InputRequestID)
	fingerprint := strings.TrimSpace(req.RequestFingerprint)
	settlementFingerprint := strings.TrimSpace(req.SettlementFingerprint)
	parentID := strings.TrimSpace(req.ParentThreadID)
	childID := strings.TrimSpace(req.ChildThreadID)
	var result sessiontree.PublishSubAgentPendingToolCompletionResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, found, err := loadSQLiteSubAgentPendingToolCompletion(ctx, tx, requestID)
		if err != nil {
			return err
		}
		if found {
			if existing.RequestFingerprint != fingerprint || existing.SettlementFingerprint != settlementFingerprint ||
				existing.ParentThreadID != parentID || existing.ChildThreadID != childID || existing.Target != req.Target {
				return sessiontree.ErrSubAgentRequestConflict
			}
			if err := sessiontree.ValidatePublishSubAgentPendingToolCompletionReplayRequest(req); err != nil {
				return err
			}
			settlement, err := loadRequiredAuthorityEntry(ctx, tx, childID, existing.SettlementEntryID)
			if err != nil {
				return err
			}
			input, found, err := loadSubAgentInput(ctx, tx, existing.SubAgentInputID)
			if err != nil {
				return err
			}
			if !found {
				return sessiontree.ErrAuthorityCorrupt
			}
			result = sessiontree.PublishSubAgentPendingToolCompletionResult{
				Settlement: settlement, SettlementReplayed: true, Input: input, Replayed: true,
			}
			return nil
		}
		if err := sessiontree.ValidatePublishSubAgentPendingToolCompletionRequest(req); err != nil {
			return err
		}

		parent, err := loadThread(ctx, tx, parentID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
			return err
		}
		child, err := loadThread(ctx, tx, childID)
		if err != nil {
			return err
		}
		if child.ParentThreadID != parentID {
			return sessiontree.ErrInvalidThreadAuthority
		}
		if err := rejectSQLiteThreadWriteLifecycle(child); err != nil {
			return err
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, parentID, childID); err != nil {
			return err
		}
		if _, active, err := loadTurnLease(ctx, tx, childID); err != nil {
			return err
		} else if active {
			return sessiontree.ErrActiveTurn
		}
		path, err := pathWithRunner(ctx, tx, childID, child.LeafID)
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
		now := authorityNow(req.Now, s.now)
		settlement := cloneEntry(existingSettlement)
		if !settlementReplayed {
			settlement = cloneEntry(req.Settlement)
			settlement.ID = ""
			settlement.ParentID = child.LeafID
			settlement.CreatedAt = now
			settlement, err = insertTurnAuthorityEntry(ctx, tx, settlement)
			if err != nil {
				return err
			}
			child.LeafID = settlement.ID
			child.UpdatedAt = now
			if err := updateThread(ctx, tx, child); err != nil {
				return err
			}
		}
		input, err := insertSubAgentInput(ctx, tx, sessiontree.SubAgentRequestPendingToolCompletion,
			requestID, fingerprint, parentID, childID, req.Message, req.HostLabels, req.CorrelationLabels, now)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO subagent_pending_tool_completions(
				input_request_id, request_fingerprint, settlement_fingerprint, parent_thread_id, child_thread_id,
				target_turn_id, target_run_id, target_tool_call_id, target_tool_name, target_handle, target_effect_attempt_id,
				settlement_entry_id, subagent_input_id, created_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			requestID, fingerprint, settlementFingerprint, parentID, childID,
			req.Target.TurnID, req.Target.RunID, req.Target.ToolCallID, req.Target.ToolName, req.Target.Handle, req.Target.EffectAttemptID,
			settlement.ID, input.SubAgentInputID, formatTime(now)); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrSubAgentRequestConflict
			}
			return err
		}
		result = sessiontree.PublishSubAgentPendingToolCompletionResult{
			Settlement: settlement, SettlementReplayed: settlementReplayed, Input: input,
		}
		return nil
	})
	return result, err
}

func loadSQLiteSubAgentPendingToolCompletion(ctx context.Context, q sqlRunner, requestID string) (sqliteSubAgentPendingToolCompletionLedger, bool, error) {
	var ledger sqliteSubAgentPendingToolCompletionLedger
	err := q.QueryRowContext(ctx, `SELECT input_request_id, request_fingerprint, settlement_fingerprint,
		parent_thread_id, child_thread_id, target_turn_id, target_run_id, target_tool_call_id,
		target_tool_name, target_handle, target_effect_attempt_id, settlement_entry_id, subagent_input_id
		FROM subagent_pending_tool_completions WHERE input_request_id = ?`, requestID).Scan(
		&ledger.InputRequestID, &ledger.RequestFingerprint, &ledger.SettlementFingerprint,
		&ledger.ParentThreadID, &ledger.ChildThreadID, &ledger.Target.TurnID, &ledger.Target.RunID,
		&ledger.Target.ToolCallID, &ledger.Target.ToolName, &ledger.Target.Handle, &ledger.Target.EffectAttemptID,
		&ledger.SettlementEntryID, &ledger.SubAgentInputID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteSubAgentPendingToolCompletionLedger{}, false, nil
	}
	if err != nil {
		return sqliteSubAgentPendingToolCompletionLedger{}, false, err
	}
	ledger.Target.ThreadID = ledger.ChildThreadID
	if ledger.InputRequestID == "" || ledger.RequestFingerprint == "" || ledger.SettlementFingerprint == "" ||
		ledger.ParentThreadID == "" || ledger.ChildThreadID == "" || ledger.SettlementEntryID == "" || ledger.SubAgentInputID == "" {
		return sqliteSubAgentPendingToolCompletionLedger{}, false, sessiontree.ErrAuthorityCorrupt
	}
	return ledger, true, nil
}
