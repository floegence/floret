package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) ReadCompaction(ctx context.Context, threadID, requestID string) (sessiontree.CompactionOperation, bool, error) {
	operation, found, err := loadSQLiteCompactionOperation(ctx, s.db, strings.TrimSpace(requestID))
	if err != nil || !found {
		return operation, found, err
	}
	if operation.ThreadID != strings.TrimSpace(threadID) {
		return sessiontree.CompactionOperation{}, true, sessiontree.ErrRequestConflict
	}
	return operation, true, nil
}

func (s *Store) BeginCompaction(ctx context.Context, req sessiontree.BeginCompactionRequest) (sessiontree.BeginCompactionResult, error) {
	if err := sessiontree.ValidateBeginCompactionRequest(req); err != nil {
		return sessiontree.BeginCompactionResult{}, err
	}
	requestID := strings.TrimSpace(req.RequestID)
	threadID := strings.TrimSpace(req.ThreadID)
	var result sessiontree.BeginCompactionResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, found, err := loadSQLiteCompactionOperation(ctx, tx, requestID)
		if err != nil {
			return err
		}
		if found {
			if !sqliteCompactionRequestMatches(existing, req) {
				return sessiontree.ErrRequestConflict
			}
			result = sessiontree.BeginCompactionResult{Operation: existing, Replayed: true}
			if existing.State == sessiontree.CompactionOperationPrepared {
				active, ok, err := loadTurnLease(ctx, tx, threadID)
				if err != nil {
					return err
				}
				if !ok || !sessiontree.SameTurnLease(active, existing.Lease) {
					return sessiontree.ErrAuthorityCorrupt
				}
				result.TakeoverEligible = active.TakeoverEligible(s.now().UTC(), s.leasePolicy)
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
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
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
		if meta.LeafID != strings.TrimSpace(req.SourceLeafID) {
			return sessiontree.ErrRequestConflict
		}
		path, err := pathWithRunner(ctx, tx, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidatePendingToolCompletionPath(path); err != nil {
			return err
		}
		if sessiontree.ActivePathHash(path) != strings.TrimSpace(req.ActivePathHash) {
			return sessiontree.ErrRequestConflict
		}
		lease, err := s.acquireMutationLeaseWithRunner(ctx, tx, sessiontree.TurnLease{
			ThreadID: threadID, Purpose: sessiontree.TurnLeasePurposeMutation, MutationID: requestID,
			MutationKind: sessiontree.CompactionMutationKind, OwnerID: strings.TrimSpace(req.OwnerID),
		})
		if err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		operation := sessiontree.CompactionOperation{
			ThreadID: threadID, RequestID: requestID, RequestFingerprint: strings.TrimSpace(req.RequestFingerprint),
			Source: strings.TrimSpace(req.Source), SourceLeafID: strings.TrimSpace(req.SourceLeafID), ActivePathHash: strings.TrimSpace(req.ActivePathHash),
			SummarySchemaVersion: strings.TrimSpace(req.SummarySchemaVersion), PromptIdentity: strings.TrimSpace(req.PromptIdentity),
			RequestPayloadHash: strings.TrimSpace(req.RequestPayloadHash), State: sessiontree.CompactionOperationPrepared,
			Lease: lease, CreatedAt: now, UpdatedAt: now,
		}
		if err := insertSQLiteCompactionOperation(ctx, tx, operation); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		result = sessiontree.BeginCompactionResult{Operation: operation, Owner: true}
		return nil
	})
	return result, err
}

func (s *Store) TakeOverCompaction(ctx context.Context, req sessiontree.TakeOverCompactionRequest) (sessiontree.BeginCompactionResult, error) {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.RequestID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.OwnerID) == "" || req.ExpectedLease.Validate() != nil {
		return sessiontree.BeginCompactionResult{}, errors.New("compaction takeover requires request, expected proof, and new owner identities")
	}
	var result sessiontree.BeginCompactionResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		operation, found, err := loadSQLiteCompactionOperation(ctx, tx, strings.TrimSpace(req.RequestID))
		if err != nil {
			return err
		}
		if !found || operation.ThreadID != strings.TrimSpace(req.ThreadID) || operation.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return sessiontree.ErrRequestConflict
		}
		if operation.State != sessiontree.CompactionOperationPrepared {
			result = sessiontree.BeginCompactionResult{Operation: operation, Replayed: true}
			return nil
		}
		active, ok, err := loadTurnLease(ctx, tx, operation.ThreadID)
		if err != nil {
			return err
		}
		if !ok || !sessiontree.SameTurnLease(active, operation.Lease) || !sessiontree.SameTurnLease(active, req.ExpectedLease) {
			return sessiontree.ErrStaleAuthority
		}
		if !active.TakeoverEligible(s.now().UTC(), s.leasePolicy) {
			return sessiontree.ErrThreadAuthorityBusy
		}
		lease, err := s.replaceMutationLeaseWithRunner(ctx, tx, active, strings.TrimSpace(req.OwnerID), authorityNow(req.Now, s.now))
		if err != nil {
			return err
		}
		operation.Lease = lease
		operation.UpdatedAt = lease.AcquiredAt
		if err := updateSQLiteCompactionPreparedLease(ctx, tx, operation); err != nil {
			return err
		}
		result = sessiontree.BeginCompactionResult{Operation: operation, Owner: true, Replayed: true}
		return nil
	})
	return result, err
}

func (s *Store) FinishCompaction(ctx context.Context, req sessiontree.FinishCompactionRequest) (sessiontree.FinishCompactionResult, error) {
	if err := validateSQLiteFinishCompactionRequest(req); err != nil {
		return sessiontree.FinishCompactionResult{}, err
	}
	var result sessiontree.FinishCompactionResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		operation, found, err := loadSQLiteCompactionOperation(ctx, tx, strings.TrimSpace(req.RequestID))
		if err != nil {
			return err
		}
		if !found || operation.ThreadID != req.Lease.ThreadID || operation.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return sessiontree.ErrRequestConflict
		}
		if operation.State != sessiontree.CompactionOperationPrepared {
			if operation.FinishedGeneration != req.Lease.Generation || operation.FinishedOwnerID != req.Lease.OwnerID {
				return sessiontree.ErrStaleAuthority
			}
			if operation.OutcomeFingerprint != strings.TrimSpace(req.OutcomeFingerprint) {
				return sessiontree.ErrRequestConflict
			}
			result = sessiontree.FinishCompactionResult{Operation: operation, Replayed: true}
			if operation.ResultEntryID != "" {
				entry, err := loadRequiredAuthorityEntry(ctx, tx, operation.ThreadID, operation.ResultEntryID)
				if err != nil {
					return err
				}
				result.Entry = &entry
			}
			return nil
		}
		active, ok, err := loadTurnLease(ctx, tx, operation.ThreadID)
		if err != nil {
			return err
		}
		if !ok || !sessiontree.SameTurnLease(active, req.Lease) || !sessiontree.SameTurnLease(active, operation.Lease) || !active.Fresh(s.now().UTC()) {
			return sessiontree.ErrStaleAuthority
		}
		meta, err := loadThread(ctx, tx, operation.ThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		if req.Result != nil {
			entry := cloneEntry(*req.Result)
			entry.ID = ""
			entry.ParentID = meta.LeafID
			entry.CreatedAt = now
			entry, err = insertTurnAuthorityEntry(ctx, tx, entry)
			if err != nil {
				return err
			}
			meta.LeafID = entry.ID
			meta.UpdatedAt = now
			if err := updateThread(ctx, tx, meta); err != nil {
				return err
			}
			operation.State = sessiontree.CompactionOperationCompleted
			operation.ResultEntryID = entry.ID
			result.Entry = &entry
		} else {
			operation.State = sessiontree.CompactionOperationFailed
			operation.ErrorCode = strings.TrimSpace(req.ErrorCode)
			operation.ErrorMessage = strings.TrimSpace(req.ErrorMessage)
		}
		if err := deleteExactTurnLease(ctx, tx, active); err != nil {
			return err
		}
		operation.OutcomeFingerprint = strings.TrimSpace(req.OutcomeFingerprint)
		operation.FinishedOwnerID = req.Lease.OwnerID
		operation.FinishedGeneration = req.Lease.Generation
		operation.Lease = sessiontree.TurnLease{}
		operation.UpdatedAt = now
		operation.FinishedAt = now
		if err := finishSQLiteCompactionOperation(ctx, tx, operation); err != nil {
			return err
		}
		result.Operation = operation
		return nil
	})
	return result, err
}

func validateSQLiteFinishCompactionRequest(req sessiontree.FinishCompactionRequest) error {
	if err := req.Lease.Validate(); err != nil {
		return err
	}
	if req.Lease.Purpose != sessiontree.TurnLeasePurposeMutation || req.Lease.MutationKind != sessiontree.CompactionMutationKind ||
		req.Lease.MutationID != strings.TrimSpace(req.RequestID) {
		return sessiontree.ErrInvalidThreadAuthority
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" || strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return errors.New("compaction finish requires request and outcome fingerprints")
	}
	success := req.Result != nil
	failure := strings.TrimSpace(req.ErrorCode) != "" && strings.TrimSpace(req.ErrorMessage) != ""
	if success == failure {
		return errors.New("compaction finish requires exactly one success result or typed failure")
	}
	if success && (req.Result.Type != sessiontree.EntryCompaction || req.Result.ThreadID != req.Lease.ThreadID) {
		return sessiontree.ErrInvalidThreadAuthority
	}
	return nil
}

func sqliteCompactionRequestMatches(operation sessiontree.CompactionOperation, req sessiontree.BeginCompactionRequest) bool {
	return operation.ThreadID == strings.TrimSpace(req.ThreadID) && operation.RequestID == strings.TrimSpace(req.RequestID) &&
		operation.RequestFingerprint == strings.TrimSpace(req.RequestFingerprint) && operation.Source == strings.TrimSpace(req.Source) &&
		operation.SourceLeafID == strings.TrimSpace(req.SourceLeafID) && operation.ActivePathHash == strings.TrimSpace(req.ActivePathHash) &&
		operation.SummarySchemaVersion == strings.TrimSpace(req.SummarySchemaVersion) && operation.PromptIdentity == strings.TrimSpace(req.PromptIdentity) &&
		operation.RequestPayloadHash == strings.TrimSpace(req.RequestPayloadHash)
}

func insertSQLiteCompactionOperation(ctx context.Context, q sqlRunner, op sessiontree.CompactionOperation) error {
	_, err := q.ExecContext(ctx, `INSERT INTO compaction_operations(
		request_id, thread_id, request_fingerprint, source, source_leaf_id, active_path_hash,
		summary_schema_version, prompt_identity, request_payload_hash, state,
		lease_owner_id, lease_generation, lease_heartbeat, lease_acquired_at, lease_renewed_at, lease_expires_at,
		created_at, updated_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?, ?, ?, ?, ?, ?)`,
		op.RequestID, op.ThreadID, op.RequestFingerprint, op.Source, op.SourceLeafID, op.ActivePathHash,
		op.SummarySchemaVersion, op.PromptIdentity, op.RequestPayloadHash,
		op.Lease.OwnerID, op.Lease.Generation, op.Lease.Heartbeat, formatTime(op.Lease.AcquiredAt), formatTime(op.Lease.RenewedAt), formatTime(op.Lease.ExpiresAt),
		formatTime(op.CreatedAt), formatTime(op.UpdatedAt))
	return err
}

func loadSQLiteCompactionOperation(ctx context.Context, q sqlRunner, requestID string) (sessiontree.CompactionOperation, bool, error) {
	var op sessiontree.CompactionOperation
	var state, acquired, renewed, expires, created, updated, finished string
	var resultEntryID sql.NullString
	err := q.QueryRowContext(ctx, `SELECT request_id, thread_id, request_fingerprint, source, source_leaf_id, active_path_hash,
		summary_schema_version, prompt_identity, request_payload_hash, state,
		lease_owner_id, lease_generation, lease_heartbeat, lease_acquired_at, lease_renewed_at, lease_expires_at,
		result_entry_id, error_code, error_message, outcome_fingerprint, finished_owner_id, finished_generation,
		created_at, updated_at, finished_at FROM compaction_operations WHERE request_id = ?`, requestID).Scan(
		&op.RequestID, &op.ThreadID, &op.RequestFingerprint, &op.Source, &op.SourceLeafID, &op.ActivePathHash,
		&op.SummarySchemaVersion, &op.PromptIdentity, &op.RequestPayloadHash, &state,
		&op.Lease.OwnerID, &op.Lease.Generation, &op.Lease.Heartbeat, &acquired, &renewed, &expires,
		&resultEntryID, &op.ErrorCode, &op.ErrorMessage, &op.OutcomeFingerprint, &op.FinishedOwnerID, &op.FinishedGeneration,
		&created, &updated, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.CompactionOperation{}, false, nil
	}
	if err != nil {
		return sessiontree.CompactionOperation{}, false, err
	}
	op.State = sessiontree.CompactionOperationState(state)
	op.ResultEntryID = resultEntryID.String
	op.CreatedAt, op.UpdatedAt, op.FinishedAt = parseTime(created), parseTime(updated), parseTime(finished)
	if op.State == sessiontree.CompactionOperationPrepared {
		op.Lease.ThreadID = op.ThreadID
		op.Lease.Purpose = sessiontree.TurnLeasePurposeMutation
		op.Lease.MutationID = op.RequestID
		op.Lease.MutationKind = sessiontree.CompactionMutationKind
		op.Lease.AcquiredAt, op.Lease.RenewedAt, op.Lease.ExpiresAt = parseTime(acquired), parseTime(renewed), parseTime(expires)
		if err := op.Lease.Validate(); err != nil {
			return sessiontree.CompactionOperation{}, false, sessiontree.ErrAuthorityCorrupt
		}
	}
	return op, true, nil
}

func updateSQLiteCompactionPreparedLease(ctx context.Context, q sqlRunner, op sessiontree.CompactionOperation) error {
	result, err := q.ExecContext(ctx, `UPDATE compaction_operations SET
		lease_owner_id = ?, lease_generation = ?, lease_heartbeat = ?, lease_acquired_at = ?, lease_renewed_at = ?, lease_expires_at = ?, updated_at = ?
		WHERE request_id = ? AND state = 'prepared'`, op.Lease.OwnerID, op.Lease.Generation, op.Lease.Heartbeat,
		formatTime(op.Lease.AcquiredAt), formatTime(op.Lease.RenewedAt), formatTime(op.Lease.ExpiresAt), formatTime(op.UpdatedAt), op.RequestID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sessiontree.ErrStaleAuthority
	}
	return nil
}

func finishSQLiteCompactionOperation(ctx context.Context, q sqlRunner, op sessiontree.CompactionOperation) error {
	result, err := q.ExecContext(ctx, `UPDATE compaction_operations SET state = ?,
		lease_owner_id = '', lease_generation = 0, lease_heartbeat = 0, lease_acquired_at = '', lease_renewed_at = '', lease_expires_at = '',
		result_entry_id = ?, error_code = ?, error_message = ?, outcome_fingerprint = ?, finished_owner_id = ?, finished_generation = ?,
		updated_at = ?, finished_at = ? WHERE request_id = ? AND state = 'prepared'`,
		string(op.State), nullableTurnEntryID(op.ResultEntryID), op.ErrorCode, op.ErrorMessage, op.OutcomeFingerprint,
		op.FinishedOwnerID, op.FinishedGeneration, formatTime(op.UpdatedAt), formatTime(op.FinishedAt), op.RequestID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sessiontree.ErrStaleAuthority
	}
	return nil
}

func (s *Store) acquireMutationLeaseWithRunner(ctx context.Context, tx sqlRunner, request sessiontree.TurnLease) (sessiontree.TurnLease, error) {
	var generation int64
	if err := tx.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = ?`, request.ThreadID).Scan(&generation); err != nil {
		return sessiontree.TurnLease{}, err
	}
	generation++
	if _, err := tx.ExecContext(ctx, `UPDATE threads SET lease_generation = ? WHERE id = ?`, generation, request.ThreadID); err != nil {
		return sessiontree.TurnLease{}, err
	}
	now := s.now().UTC()
	proof := sessiontree.TurnLease{
		ThreadID: request.ThreadID, Purpose: sessiontree.TurnLeasePurposeMutation,
		MutationID: request.MutationID, MutationKind: request.MutationKind, OwnerID: request.OwnerID,
		Generation: generation, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(s.leasePolicy.TTL),
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO active_turn_leases(
		thread_id, purpose, turn_id, mutation_id, mutation_kind, owner_id, generation, heartbeat, acquired_at, renewed_at, expires_at
	) VALUES(?, 'mutation', '', ?, ?, ?, ?, 0, ?, ?, ?)`,
		proof.ThreadID, proof.MutationID, proof.MutationKind, proof.OwnerID, proof.Generation,
		formatTime(proof.AcquiredAt), formatTime(proof.RenewedAt), formatTime(proof.ExpiresAt))
	if isConstraintError(err) {
		return sessiontree.TurnLease{}, sessiontree.ErrActiveTurn
	}
	return proof, err
}

func (s *Store) replaceMutationLeaseWithRunner(ctx context.Context, tx sqlRunner, old sessiontree.TurnLease, ownerID string, now time.Time) (sessiontree.TurnLease, error) {
	var generation int64
	if err := tx.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = ?`, old.ThreadID).Scan(&generation); err != nil {
		return sessiontree.TurnLease{}, err
	}
	generation++
	if _, err := tx.ExecContext(ctx, `UPDATE threads SET lease_generation = ? WHERE id = ?`, generation, old.ThreadID); err != nil {
		return sessiontree.TurnLease{}, err
	}
	proof := sessiontree.TurnLease{
		ThreadID: old.ThreadID, Purpose: old.Purpose, MutationID: old.MutationID, MutationKind: old.MutationKind,
		OwnerID: ownerID, Generation: generation, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(s.leasePolicy.TTL),
	}
	result, err := tx.ExecContext(ctx, `UPDATE active_turn_leases SET owner_id = ?, generation = ?, heartbeat = 0,
		acquired_at = ?, renewed_at = ?, expires_at = ? WHERE thread_id = ? AND purpose = 'mutation' AND mutation_id = ? AND mutation_kind = ? AND
		owner_id = ? AND generation = ? AND heartbeat = ? AND acquired_at = ? AND renewed_at = ? AND expires_at = ?`,
		proof.OwnerID, proof.Generation, formatTime(proof.AcquiredAt), formatTime(proof.RenewedAt), formatTime(proof.ExpiresAt),
		old.ThreadID, old.MutationID, old.MutationKind, old.OwnerID, old.Generation, old.Heartbeat,
		formatTime(old.AcquiredAt), formatTime(old.RenewedAt), formatTime(old.ExpiresAt))
	if err != nil {
		return sessiontree.TurnLease{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return sessiontree.TurnLease{}, err
	}
	if rows != 1 {
		return sessiontree.TurnLease{}, sessiontree.ErrStaleAuthority
	}
	return proof, nil
}

func deleteExactTurnLease(ctx context.Context, q sqlRunner, lease sessiontree.TurnLease) error {
	result, err := q.ExecContext(ctx, `DELETE FROM active_turn_leases WHERE thread_id = ? AND purpose = ? AND turn_id = ? AND mutation_id = ? AND mutation_kind = ? AND
		owner_id = ? AND generation = ? AND heartbeat = ? AND acquired_at = ? AND renewed_at = ? AND expires_at = ?`,
		lease.ThreadID, string(lease.Purpose), lease.TurnID, lease.MutationID, lease.MutationKind,
		lease.OwnerID, lease.Generation, lease.Heartbeat, formatTime(lease.AcquiredAt), formatTime(lease.RenewedAt), formatTime(lease.ExpiresAt))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sessiontree.ErrStaleAuthority
	}
	return nil
}

var _ sessiontree.CompactionAuthorityRepo = (*Store)(nil)
