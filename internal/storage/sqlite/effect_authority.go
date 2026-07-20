package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) PrepareEffectAttempt(ctx context.Context, req sessiontree.PrepareEffectAttemptRequest) (sessiontree.PrepareEffectAttemptResult, error) {
	if err := validateSQLiteEffectPreparation(req); err != nil {
		return sessiontree.PrepareEffectAttemptResult{}, err
	}
	var result sessiontree.PrepareEffectAttemptResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		if err := s.validateFreshEffectLease(ctx, tx, req.Lease); err != nil {
			return err
		}
		existing, found, err := loadEffectAttemptByInvocation(ctx, tx, req.Invocation)
		if err != nil {
			return err
		}
		if found {
			if existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) ||
				existing.Invocation.ToolName != strings.TrimSpace(req.Invocation.ToolName) ||
				existing.Invocation.ArgumentHash != strings.TrimSpace(req.Invocation.ArgumentHash) {
				return sessiontree.ErrRequestConflict
			}
			result = sessiontree.PrepareEffectAttemptResult{Attempt: existing, Replayed: true}
			return nil
		}
		now := authorityNow(req.Now, s.now)
		attemptID := "effect-" + sessiontree.StableHash(effectSQLiteInvocationKey(req.Invocation))[:24]
		attempt := sessiontree.EffectAttempt{
			EffectAttemptID: attemptID, Invocation: req.Invocation, RequestFingerprint: strings.TrimSpace(req.RequestFingerprint),
			State: sessiontree.EffectAttemptPrepared, OwnerID: req.Lease.OwnerID, Generation: req.Lease.Generation,
			CreatedAt: now, UpdatedAt: now,
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO effect_attempts(
			effect_attempt_id, thread_id, turn_id, run_id, tool_call_id, tool_name, argument_hash,
			request_fingerprint, state, owner_id, generation, created_at, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?, ?)`,
			attempt.EffectAttemptID, strings.TrimSpace(req.Invocation.ThreadID), strings.TrimSpace(req.Invocation.TurnID),
			strings.TrimSpace(req.Invocation.RunID), strings.TrimSpace(req.Invocation.ToolCallID), strings.TrimSpace(req.Invocation.ToolName),
			strings.TrimSpace(req.Invocation.ArgumentHash), attempt.RequestFingerprint, attempt.OwnerID, attempt.Generation,
			formatTime(now), formatTime(now))
		if isConstraintError(err) {
			return sessiontree.ErrRequestConflict
		}
		if err != nil {
			return err
		}
		result = sessiontree.PrepareEffectAttemptResult{Attempt: attempt}
		return nil
	})
	return result, err
}

func (s *Store) RejectEffectAttempt(ctx context.Context, req sessiontree.RejectEffectAttemptRequest) (sessiontree.EffectAttempt, error) {
	if strings.TrimSpace(req.EffectAttemptID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.RejectionCode) == "" || strings.TrimSpace(req.RejectionFingerprint) == "" {
		return sessiontree.EffectAttempt{}, errors.New("effect rejection requires attempt, request, code, and rejection fingerprints")
	}
	var result sessiontree.EffectAttempt
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		attempt, err := s.effectAttemptForLease(ctx, tx, req.Lease, req.EffectAttemptID, req.RequestFingerprint)
		if err != nil {
			return err
		}
		if attempt.State == sessiontree.EffectAttemptRejected {
			if attempt.RejectionCode != strings.TrimSpace(req.RejectionCode) || attempt.TerminalFingerprint != strings.TrimSpace(req.RejectionFingerprint) {
				return sessiontree.ErrRequestConflict
			}
			result = attempt
			return nil
		}
		if attempt.State != sessiontree.EffectAttemptPrepared {
			return sessiontree.ErrRequestConflict
		}
		now := authorityNow(req.Now, s.now)
		if _, err := tx.ExecContext(ctx, `UPDATE effect_attempts SET state = 'rejected', rejection_code = ?, terminal_fingerprint = ?, updated_at = ?
			WHERE effect_attempt_id = ? AND state = 'prepared'`, strings.TrimSpace(req.RejectionCode),
			strings.TrimSpace(req.RejectionFingerprint), formatTime(now), attempt.EffectAttemptID); err != nil {
			return err
		}
		attempt.State = sessiontree.EffectAttemptRejected
		attempt.RejectionCode = strings.TrimSpace(req.RejectionCode)
		attempt.TerminalFingerprint = strings.TrimSpace(req.RejectionFingerprint)
		attempt.UpdatedAt = now
		result = attempt
		return nil
	})
	return result, err
}

func (s *Store) BeginEffectDispatch(ctx context.Context, req sessiontree.BeginEffectDispatchRequest) (sessiontree.EffectAttempt, error) {
	if strings.TrimSpace(req.AuthorizationProofHash) == "" || req.ObservedHeartbeat < 0 {
		return sessiontree.EffectAttempt{}, errors.New("effect dispatch requires authorization proof and observed heartbeat")
	}
	var result sessiontree.EffectAttempt
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		attempt, err := s.effectAttemptForLease(ctx, tx, req.Lease, req.EffectAttemptID, req.RequestFingerprint)
		if err != nil {
			return err
		}
		active, _, err := loadTurnLease(ctx, tx, req.Lease.ThreadID)
		if err != nil {
			return err
		}
		if active.Heartbeat < req.ObservedHeartbeat {
			return sessiontree.ErrStaleAuthority
		}
		if attempt.State != sessiontree.EffectAttemptPrepared {
			if attempt.State == sessiontree.EffectAttemptDispatching || attempt.State == sessiontree.EffectAttemptUnknown {
				return sessiontree.ErrEffectOutcomeUnknown
			}
			return sessiontree.ErrRequestConflict
		}
		now := authorityNow(req.Now, s.now)
		if _, err := tx.ExecContext(ctx, `UPDATE effect_attempts SET state = 'dispatching', updated_at = ?
			WHERE effect_attempt_id = ? AND state = 'prepared'`, formatTime(now), attempt.EffectAttemptID); err != nil {
			return err
		}
		attempt.State = sessiontree.EffectAttemptDispatching
		attempt.UpdatedAt = now
		result = attempt
		return nil
	})
	return result, err
}

func (s *Store) FinishEffectDispatch(ctx context.Context, req sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	if strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return sessiontree.FinishEffectDispatchResult{}, errors.New("effect finish outcome fingerprint is required")
	}
	var result sessiontree.FinishEffectDispatchResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		attempt, err := s.effectAttemptForLease(ctx, tx, req.Lease, req.EffectAttemptID, req.RequestFingerprint)
		if err != nil {
			return err
		}
		wantState := sessiontree.EffectAttemptCompleted
		if req.Failed {
			wantState = sessiontree.EffectAttemptFailed
		}
		if attempt.State == wantState {
			if attempt.TerminalFingerprint != strings.TrimSpace(req.OutcomeFingerprint) {
				return sessiontree.ErrRequestConflict
			}
			entry, err := loadRequiredAuthorityEntry(ctx, tx, attempt.Invocation.ThreadID, attempt.ResultEntryID)
			if err != nil {
				return err
			}
			ref, err := validateSQLiteEffectArtifactReplay(ctx, tx, attempt, entry, req)
			if err != nil {
				return err
			}
			result = sessiontree.FinishEffectDispatchResult{Attempt: attempt, Result: entry, Artifact: artifact.CloneRefPtr(ref), Replayed: true}
			return nil
		}
		if attempt.State != sessiontree.EffectAttemptDispatching {
			return sessiontree.ErrRequestConflict
		}
		if req.Result.Type != sessiontree.EntryToolResult || req.Result.ThreadID != attempt.Invocation.ThreadID ||
			req.Result.TurnID != attempt.Invocation.TurnID || req.Result.Message.ToolCallID != attempt.Invocation.ToolCallID ||
			req.Result.Message.ToolName != attempt.Invocation.ToolName {
			return sessiontree.ErrInvalidThreadAuthority
		}
		if req.Result.Message.ToolResult == nil || req.Result.Message.ToolResult.FullOutput != nil {
			return sessiontree.ErrRequestConflict
		}
		meta, err := loadThread(ctx, tx, attempt.Invocation.ThreadID)
		if err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		entry := req.Result
		entry.Metadata = cloneStringMapSQLite(entry.Metadata)
		if entry.Metadata == nil {
			entry.Metadata = map[string]string{}
		}
		entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey] = attempt.EffectAttemptID
		entry.ID = ""
		entry.ParentID = meta.LeafID
		entry.CreatedAt = now
		var committedRef *artifact.Ref
		var full artifact.FullOutput
		if req.FullOutput != nil {
			ref, err := artifact.RefForEffect(attempt.EffectAttemptID, attempt.Invocation.ToolName, *req.FullOutput)
			if err != nil {
				return err
			}
			if _, collisionErr := loadSQLiteArtifactRecord(ctx, tx, meta.ID, ref.ID); collisionErr == nil {
				return sessiontree.ErrAuthorityCorrupt
			} else if !errors.Is(collisionErr, sql.ErrNoRows) {
				return collisionErr
			}
			entry.Message.ToolResult.FullOutput = &ref
			committedRef = &ref
			full = artifact.NormalizeFullOutput(*req.FullOutput)
		}
		entry, err = insertTurnAuthorityEntry(ctx, tx, entry)
		if err != nil {
			return err
		}
		meta.LeafID = entry.ID
		meta.UpdatedAt = now
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		if committedRef != nil {
			if _, err := tx.ExecContext(ctx, `INSERT INTO tool_output_artifacts(
				id, run_id, thread_id, turn_id, prompt_scope_id, step, call_id, tool_name,
				effect_attempt_id, canonical_entry_id, kind, mime, safe_label, size_bytes, sha256, text, metadata_json, created_at
			) VALUES(?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)`,
				committedRef.ID, attempt.Invocation.RunID, attempt.Invocation.ThreadID, attempt.Invocation.TurnID,
				attempt.Invocation.ThreadID, attempt.Invocation.ToolCallID, attempt.Invocation.ToolName,
				attempt.EffectAttemptID, entry.ID, committedRef.Kind, committedRef.MIME, committedRef.SafeLabel,
				committedRef.SizeBytes, committedRef.SHA256, full.Text, formatTime(now)); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE effect_attempts SET state = ?, terminal_fingerprint = ?, result_entry_id = ?, updated_at = ?
			WHERE effect_attempt_id = ? AND state = 'dispatching'`, string(wantState), strings.TrimSpace(req.OutcomeFingerprint),
			entry.ID, formatTime(now), attempt.EffectAttemptID); err != nil {
			return err
		}
		attempt.State = wantState
		attempt.TerminalFingerprint = strings.TrimSpace(req.OutcomeFingerprint)
		attempt.ResultEntryID = entry.ID
		attempt.UpdatedAt = now
		result = sessiontree.FinishEffectDispatchResult{Attempt: attempt, Result: entry, Artifact: artifact.CloneRefPtr(committedRef)}
		return nil
	})
	return result, err
}

func validateSQLiteEffectArtifactReplay(ctx context.Context, q sqlRunner, attempt sessiontree.EffectAttempt, entry sessiontree.Entry, req sessiontree.FinishEffectDispatchRequest) (*artifact.Ref, error) {
	if !sessiontree.EffectResultRequestMatches(entry, req.Result, attempt.EffectAttemptID) {
		return nil, sessiontree.ErrRequestConflict
	}
	committedRef := entry.Message.ToolResult.FullOutput
	if req.FullOutput == nil {
		if committedRef != nil {
			return nil, sessiontree.ErrRequestConflict
		}
		return nil, nil
	}
	if committedRef == nil {
		return nil, sessiontree.ErrAuthorityCorrupt
	}
	expected, err := artifact.RefForEffect(attempt.EffectAttemptID, attempt.Invocation.ToolName, *req.FullOutput)
	if err != nil {
		return nil, err
	}
	if *committedRef != expected {
		return nil, sessiontree.ErrRequestConflict
	}
	record, err := loadSQLiteArtifactRecord(ctx, q, attempt.Invocation.ThreadID, expected.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sessiontree.ErrAuthorityCorrupt
	}
	if err != nil {
		return nil, err
	}
	full := artifact.NormalizeFullOutput(*req.FullOutput)
	if record.Text != full.Text || record.Ref != expected || record.CanonicalEntryID != entry.ID {
		return nil, sessiontree.ErrRequestConflict
	}
	if err := validateSQLiteArtifactRecord(ctx, q, record); err != nil {
		return nil, err
	}
	return &expected, nil
}

func (s *Store) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	if strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return sessiontree.EffectAttempt{}, errors.New("effect unknown outcome fingerprint is required")
	}
	var result sessiontree.EffectAttempt
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		attempt, err := s.effectAttemptForLease(ctx, tx, req.Lease, req.EffectAttemptID, req.RequestFingerprint)
		if err != nil {
			return err
		}
		if attempt.State == sessiontree.EffectAttemptUnknown {
			if attempt.TerminalFingerprint != strings.TrimSpace(req.OutcomeFingerprint) {
				return sessiontree.ErrRequestConflict
			}
			result = attempt
			return nil
		}
		if attempt.State != sessiontree.EffectAttemptDispatching {
			return sessiontree.ErrRequestConflict
		}
		now := authorityNow(req.Now, s.now)
		if _, err := tx.ExecContext(ctx, `UPDATE effect_attempts SET state = 'unknown', terminal_fingerprint = ?, updated_at = ?
			WHERE effect_attempt_id = ? AND state = 'dispatching'`, strings.TrimSpace(req.OutcomeFingerprint),
			formatTime(now), attempt.EffectAttemptID); err != nil {
			return err
		}
		attempt.State = sessiontree.EffectAttemptUnknown
		attempt.TerminalFingerprint = strings.TrimSpace(req.OutcomeFingerprint)
		attempt.UpdatedAt = now
		result = attempt
		return nil
	})
	return result, err
}

func validateSQLiteEffectPreparation(req sessiontree.PrepareEffectAttemptRequest) error {
	inv := req.Invocation
	if strings.TrimSpace(inv.ThreadID) == "" || strings.TrimSpace(inv.TurnID) == "" || strings.TrimSpace(inv.RunID) == "" ||
		strings.TrimSpace(inv.ToolCallID) == "" || strings.TrimSpace(inv.ToolName) == "" || strings.TrimSpace(inv.ArgumentHash) == "" ||
		strings.TrimSpace(req.RequestFingerprint) == "" {
		return errors.New("effect preparation requires complete invocation and request fingerprint")
	}
	if err := req.Lease.Validate(); err != nil {
		return err
	}
	if req.Lease.Purpose != sessiontree.TurnLeasePurposeTurn || req.Lease.ThreadID != inv.ThreadID || req.Lease.TurnID != inv.TurnID {
		return sessiontree.ErrInvalidThreadAuthority
	}
	return nil
}

func (s *Store) validateFreshEffectLease(ctx context.Context, tx sqlRunner, lease sessiontree.TurnLease) error {
	if err := lease.Validate(); err != nil || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return sessiontree.ErrStaleAuthority
	}
	active, activeOK, err := loadTurnLease(ctx, tx, lease.ThreadID)
	if err != nil {
		return err
	}
	if !activeOK || sessiontree.ValidateEffectLeaseSuccessor(lease, active) != nil || !active.Fresh(s.now().UTC()) {
		return sessiontree.ErrStaleAuthority
	}
	meta, err := loadThread(ctx, tx, lease.ThreadID)
	if err != nil {
		return err
	}
	if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
		return err
	}
	if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, lease.ThreadID); err != nil {
		return err
	} else if claimed {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func (s *Store) effectAttemptForLease(ctx context.Context, tx sqlRunner, lease sessiontree.TurnLease, attemptID, fingerprint string) (sessiontree.EffectAttempt, error) {
	attempt, found, err := loadEffectAttempt(ctx, tx, strings.TrimSpace(attemptID))
	if err != nil {
		return sessiontree.EffectAttempt{}, err
	}
	if !found {
		return sessiontree.EffectAttempt{}, sessiontree.ErrEffectAttemptNotFound
	}
	if attempt.RequestFingerprint != strings.TrimSpace(fingerprint) {
		return sessiontree.EffectAttempt{}, sessiontree.ErrRequestConflict
	}
	if lease.ThreadID != attempt.Invocation.ThreadID || lease.TurnID != attempt.Invocation.TurnID ||
		lease.OwnerID != attempt.OwnerID || lease.Generation != attempt.Generation {
		return sessiontree.EffectAttempt{}, sessiontree.ErrStaleAuthority
	}
	if err := s.validateFreshEffectLease(ctx, tx, lease); err != nil {
		return sessiontree.EffectAttempt{}, err
	}
	return attempt, nil
}

func loadEffectAttemptByInvocation(ctx context.Context, q sqlRunner, inv sessiontree.EffectInvocationIdentity) (sessiontree.EffectAttempt, bool, error) {
	var attemptID string
	err := q.QueryRowContext(ctx, `SELECT effect_attempt_id FROM effect_attempts
		WHERE thread_id = ? AND turn_id = ? AND run_id = ? AND tool_call_id = ?`,
		strings.TrimSpace(inv.ThreadID), strings.TrimSpace(inv.TurnID), strings.TrimSpace(inv.RunID), strings.TrimSpace(inv.ToolCallID)).Scan(&attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.EffectAttempt{}, false, nil
	}
	if err != nil {
		return sessiontree.EffectAttempt{}, false, err
	}
	return loadEffectAttempt(ctx, q, attemptID)
}

func loadEffectAttempt(ctx context.Context, q sqlRunner, attemptID string) (sessiontree.EffectAttempt, bool, error) {
	var attempt sessiontree.EffectAttempt
	var state, createdAt, updatedAt string
	err := q.QueryRowContext(ctx, `SELECT effect_attempt_id, thread_id, turn_id, run_id, tool_call_id, tool_name,
		argument_hash, request_fingerprint, state, rejection_code, terminal_fingerprint, result_entry_id,
		owner_id, generation, created_at, updated_at FROM effect_attempts WHERE effect_attempt_id = ?`, attemptID).Scan(
		&attempt.EffectAttemptID, &attempt.Invocation.ThreadID, &attempt.Invocation.TurnID, &attempt.Invocation.RunID,
		&attempt.Invocation.ToolCallID, &attempt.Invocation.ToolName, &attempt.Invocation.ArgumentHash,
		&attempt.RequestFingerprint, &state, &attempt.RejectionCode, &attempt.TerminalFingerprint, &attempt.ResultEntryID,
		&attempt.OwnerID, &attempt.Generation, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.EffectAttempt{}, false, nil
	}
	if err != nil {
		return sessiontree.EffectAttempt{}, false, err
	}
	attempt.State = sessiontree.EffectAttemptState(state)
	attempt.CreatedAt = parseTime(createdAt)
	attempt.UpdatedAt = parseTime(updatedAt)
	return attempt, true, nil
}

func effectSQLiteInvocationKey(inv sessiontree.EffectInvocationIdentity) string {
	return strings.Join([]string{inv.ThreadID, inv.TurnID, inv.RunID, inv.ToolCallID}, "\x00")
}
