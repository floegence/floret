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
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) PublishSubAgent(ctx context.Context, req sessiontree.PublishSubAgentRequest) (sessiontree.PublishSubAgentResult, error) {
	if err := sessiontree.ValidatePublishSubAgentIdentity(req); err != nil {
		return sessiontree.PublishSubAgentResult{}, err
	}
	var result sessiontree.PublishSubAgentResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		parent, err := loadThread(ctx, tx, req.ParentThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
			return err
		}
		existing, found, err := loadSubAgentPublicationLedger(ctx, tx, req.PublicationID)
		if err != nil {
			return err
		}
		if found {
			if existing.parentThreadID != req.ParentThreadID || existing.childThreadID != req.ChildMeta.ID || existing.fingerprint != req.RequestFingerprint ||
				!artifact.EqualClosure(existing.artifactClosure, req.ArtifactClosure) {
				return sessiontree.ErrRequestConflict
			}
			input, found, err := loadSubAgentInput(ctx, tx, existing.inputID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("subagent publication %q input is missing", req.PublicationID)
			}
			if err := sessiontree.ValidatePublishSubAgentRequest(req); err != nil {
				return err
			}
			child, err := loadThread(ctx, tx, req.ChildMeta.ID)
			if errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err != nil {
				return err
			}
			if !sessiontree.PublishSubAgentChildMatches(req, child) || input.ParentThreadID != req.ParentThreadID || input.ChildThreadID != req.ChildMeta.ID ||
				input.RequestID != req.PublicationID || input.RequestFingerprint != req.RequestFingerprint {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err := validateSQLiteSubAgentPublicationArtifacts(ctx, tx, req.ArtifactClosure); err != nil {
				return err
			}
			result = sessiontree.PublishSubAgentResult{Thread: child, Input: input, Replayed: true}
			return nil
		}
		if err := sessiontree.ValidatePublishSubAgentRequest(req); err != nil {
			return err
		}
		if req.ForkOptions != nil && parent.LeafID != strings.TrimSpace(req.ForkOptions.ExpectedSourceLeafID) {
			return sessiontree.ErrStaleAuthority
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, req.ParentThreadID, req.ChildMeta.ID); err != nil {
			return err
		}
		if exists, err := threadExists(ctx, tx, req.ChildMeta.ID); err != nil {
			return err
		} else if exists {
			return sessiontree.ErrThreadExists
		}
		var child sessiontree.ThreadMeta
		if req.ForkOptions == nil {
			child, err = createThreadWithRunner(ctx, tx, req.ChildMeta)
		} else {
			opts := *req.ForkOptions
			opts.NewThreadID = req.ChildMeta.ID
			opts.ArtifactClosure = artifact.CloneClosure(req.ArtifactClosure)
			child, err = forkWithRunner(ctx, tx, opts)
		}
		if err != nil {
			return err
		}
		if !sessiontree.PublishSubAgentChildMatches(req, child) {
			return sessiontree.ErrAuthorityCorrupt
		}
		input, err := insertSubAgentInput(ctx, tx, sessiontree.SubAgentRequestPublication, req.PublicationID, req.RequestFingerprint, req.ParentThreadID, child.ID, req.Message, req.HostLabels, req.CorrelationLabels, authorityNow(req.Now, s.now))
		if err != nil {
			return err
		}
		closureJSON, err := json.Marshal(req.ArtifactClosure)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO subagent_publications(
			publication_id, parent_thread_id, child_thread_id, request_fingerprint, artifact_closure_json, subagent_input_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?)`, req.PublicationID, req.ParentThreadID, child.ID, req.RequestFingerprint, string(closureJSON), input.SubAgentInputID, formatTime(input.CreatedAt)); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		result = sessiontree.PublishSubAgentResult{Thread: child, Input: input}
		return nil
	})
	return result, err
}

func (s *Store) PublishSubAgentInput(ctx context.Context, req sessiontree.PublishSubAgentInputRequest) (sessiontree.SubAgentInputRecord, bool, error) {
	if err := sessiontree.ValidatePublishSubAgentInputRequest(req); err != nil {
		return sessiontree.SubAgentInputRecord{}, false, err
	}
	var input sessiontree.SubAgentInputRecord
	replayed := false
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		parent, err := loadThread(ctx, tx, req.ParentThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
			return err
		}
		existing, found, err := loadSubAgentRequestLedger(ctx, tx, "subagent_input_requests", "input_request_id", req.InputRequestID)
		if err != nil {
			return err
		}
		if found {
			if existing.parentThreadID != req.ParentThreadID || existing.childThreadID != req.ChildThreadID || existing.fingerprint != req.RequestFingerprint {
				return sessiontree.ErrSubAgentRequestConflict
			}
			input, found, err = loadSubAgentInput(ctx, tx, existing.inputID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("subagent input request %q input is missing", req.InputRequestID)
			}
			replayed = true
			return nil
		}
		child, err := loadThread(ctx, tx, req.ChildThreadID)
		if err != nil {
			return err
		}
		if child.ParentThreadID != req.ParentThreadID {
			return sessiontree.ErrInvalidThreadAuthority
		}
		if err := rejectSQLiteThreadWriteLifecycle(child); err != nil {
			return err
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, req.ParentThreadID, req.ChildThreadID); err != nil {
			return err
		}
		if active, ok, err := loadTurnLease(ctx, tx, req.ChildThreadID); err != nil {
			return err
		} else if ok && active.Purpose == sessiontree.TurnLeasePurposeMutation {
			return sessiontree.ErrActiveTurn
		}
		now := authorityNow(req.Now, s.now)
		if req.Interrupt {
			if _, err := tx.ExecContext(ctx, `UPDATE subagent_inputs SET state = 'cancelled', cancelled_at = ?
				WHERE child_thread_id = ? AND state = 'pending'`, formatTime(now), req.ChildThreadID); err != nil {
				return err
			}
		}
		input, err = insertSubAgentInput(ctx, tx, sessiontree.SubAgentRequestInput, req.InputRequestID, req.RequestFingerprint, req.ParentThreadID, req.ChildThreadID, req.Message, req.HostLabels, req.CorrelationLabels, now)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO subagent_input_requests(
			input_request_id, parent_thread_id, child_thread_id, request_fingerprint, subagent_input_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?)`, req.InputRequestID, req.ParentThreadID, req.ChildThreadID, req.RequestFingerprint, input.SubAgentInputID, formatTime(now)); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrSubAgentRequestConflict
			}
			return err
		}
		return nil
	})
	return input, replayed, err
}

func (s *Store) AdmitSubAgentInput(ctx context.Context, req sessiontree.AdmitSubAgentInputRequest) (sessiontree.AdmitSubAgentInputResult, error) {
	if strings.TrimSpace(req.ParentThreadID) == "" || strings.TrimSpace(req.ChildThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.OwnerID) == "" {
		return sessiontree.AdmitSubAgentInputResult{}, errors.New("subagent admission requires parent, child, turn, run, and owner identities")
	}
	req.ParentThreadID = strings.TrimSpace(req.ParentThreadID)
	req.ChildThreadID = strings.TrimSpace(req.ChildThreadID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.OwnerID = strings.TrimSpace(req.OwnerID)
	var result sessiontree.AdmitSubAgentInputResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		parent, err := loadThread(ctx, tx, req.ParentThreadID)
		if err != nil {
			return err
		}
		child, err := loadThread(ctx, tx, req.ChildThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(child); err != nil {
			return err
		}
		if child.ParentThreadID != req.ParentThreadID {
			return sessiontree.ErrInvalidThreadAuthority
		}
		admitted, found, err := loadAdmittedSubAgentInput(ctx, tx, req.ChildThreadID, req.TurnID, req.RunID)
		if err != nil {
			return err
		}
		if found {
			if admitted.AdmittedTurnID != req.TurnID || admitted.AdmittedRunID != req.RunID {
				return sessiontree.ErrRequestConflict
			}
			result = sessiontree.AdmitSubAgentInputResult{Input: admitted, Replayed: true}
			return nil
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, req.ParentThreadID, req.ChildThreadID); err != nil {
			return err
		}
		if _, active, err := loadTurnLease(ctx, tx, req.ChildThreadID); err != nil {
			return err
		} else if active {
			return sessiontree.ErrActiveTurn
		}
		input, found, err := loadNextPendingSubAgentInput(ctx, tx, req.ChildThreadID)
		if err != nil {
			return err
		}
		if !found {
			return sessiontree.ErrSubAgentInputNotFound
		}
		lease, err := s.acquireTurnLeaseWithRunner(ctx, tx, sessiontree.TurnLease{
			ThreadID: req.ChildThreadID, TurnID: req.TurnID, OwnerID: req.OwnerID, Purpose: sessiontree.TurnLeasePurposeTurn,
		})
		if err != nil {
			return err
		}
		leaseCtx := sessiontree.ContextWithTurnLease(ctx, lease)
		now := authorityNow(req.Now, s.now)
		started, err := appendWithRunner(leaseCtx, tx, sessiontree.Entry{
			ThreadID: req.ChildThreadID, TurnID: req.TurnID, Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{"run_id": req.RunID},
		}, sessiontree.AppendOptions{Now: now}, s.now)
		if err != nil {
			return err
		}
		user, err := appendWithRunner(leaseCtx, tx, sessiontree.Entry{
			ThreadID: req.ChildThreadID, TurnID: req.TurnID, Type: sessiontree.EntryUserMessage,
			Metadata: map[string]string{"subagent_input_id": input.SubAgentInputID}, Message: session.CloneMessage(input.Message),
		}, sessiontree.AppendOptions{Now: now}, s.now)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE subagent_inputs SET
			state = 'admitted', admitted_turn_id = ?, admitted_run_id = ?, admitted_at = ?
			WHERE subagent_input_id = ? AND state = 'pending'`, req.TurnID, req.RunID, formatTime(now), input.SubAgentInputID); err != nil {
			return err
		}
		input.State = sessiontree.SubAgentInputAdmitted
		input.AdmittedTurnID = req.TurnID
		input.AdmittedRunID = req.RunID
		input.AdmittedAt = now
		result = sessiontree.AdmitSubAgentInputResult{Input: input, Lease: lease, TurnStarted: started, UserMessage: user}
		return nil
	})
	return result, err
}

func (s *Store) ListSubAgentInputs(ctx context.Context, childThreadID string, state sessiontree.SubAgentInputState) ([]sessiontree.SubAgentInputRecord, error) {
	if exists, err := threadExists(ctx, s.db, childThreadID); err != nil {
		return nil, err
	} else if !exists {
		return nil, sessiontree.ErrThreadNotFound
	}
	query := `SELECT subagent_input_id FROM subagent_inputs WHERE child_thread_id = ?`
	args := []any{childThreadID}
	if state != "" {
		query += ` AND state = ?`
		args = append(args, string(state))
	}
	query += ` ORDER BY sequence, subagent_input_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var inputIDs []string
	for rows.Next() {
		var inputID string
		if err := rows.Scan(&inputID); err != nil {
			_ = rows.Close()
			return nil, err
		}
		inputIDs = append(inputIDs, inputID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var out []sessiontree.SubAgentInputRecord
	for _, inputID := range inputIDs {
		input, found, err := loadSubAgentInput(ctx, s.db, inputID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("subagent input %q disappeared during list", inputID)
		}
		out = append(out, input)
	}
	return out, nil
}

type sqliteSubAgentRequestLedger struct {
	parentThreadID  string
	childThreadID   string
	fingerprint     string
	inputID         string
	artifactClosure artifact.Closure
}

func loadSubAgentPublicationLedger(ctx context.Context, q sqlRunner, publicationID string) (sqliteSubAgentRequestLedger, bool, error) {
	var record sqliteSubAgentRequestLedger
	var closureJSON string
	err := q.QueryRowContext(ctx, `SELECT parent_thread_id, child_thread_id, request_fingerprint, subagent_input_id, artifact_closure_json
		FROM subagent_publications WHERE publication_id = ?`, publicationID).Scan(
		&record.parentThreadID, &record.childThreadID, &record.fingerprint, &record.inputID, &closureJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteSubAgentRequestLedger{}, false, nil
	}
	if err != nil {
		return sqliteSubAgentRequestLedger{}, false, err
	}
	if err := json.Unmarshal([]byte(closureJSON), &record.artifactClosure); err != nil {
		return sqliteSubAgentRequestLedger{}, false, sessiontree.ErrAuthorityCorrupt
	}
	if !artifact.IsZeroClosure(record.artifactClosure) {
		if err := artifact.ValidateClosure(record.artifactClosure); err != nil {
			return sqliteSubAgentRequestLedger{}, false, sessiontree.ErrAuthorityCorrupt
		}
	}
	return record, true, nil
}

func validateSQLiteSubAgentPublicationArtifacts(ctx context.Context, q sqlRunner, closure artifact.Closure) error {
	if !artifact.IsZeroClosure(closure) {
		return validateSQLiteArtifactForkDestination(ctx, q, closure)
	}
	return nil
}

func loadSubAgentRequestLedger(ctx context.Context, q sqlRunner, table, idColumn, requestID string) (sqliteSubAgentRequestLedger, bool, error) {
	query := `SELECT parent_thread_id, child_thread_id, request_fingerprint, subagent_input_id FROM ` + table + ` WHERE ` + idColumn + ` = ?`
	var record sqliteSubAgentRequestLedger
	err := q.QueryRowContext(ctx, query, requestID).Scan(&record.parentThreadID, &record.childThreadID, &record.fingerprint, &record.inputID)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteSubAgentRequestLedger{}, false, nil
	}
	return record, err == nil, err
}

func insertSubAgentInput(ctx context.Context, q sqlRunner, kind sessiontree.SubAgentRequestKind, requestID, fingerprint, parentThreadID, childThreadID string, message session.Message, hostLabels, correlationLabels map[string]string, now time.Time) (sessiontree.SubAgentInputRecord, error) {
	var sequence int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM subagent_inputs WHERE child_thread_id = ?`, childThreadID).Scan(&sequence); err != nil {
		return sessiontree.SubAgentInputRecord{}, err
	}
	input := sessiontree.SubAgentInputRecord{
		SubAgentInputID: fmt.Sprintf("%s-input-%d", childThreadID, sequence), ParentThreadID: parentThreadID, ChildThreadID: childThreadID,
		RequestKind: kind, RequestID: requestID, RequestFingerprint: fingerprint, Sequence: sequence, State: sessiontree.SubAgentInputPending,
		Message: session.CloneMessage(message), HostLabels: cloneStringMapSQLite(hostLabels), CorrelationLabels: cloneStringMapSQLite(correlationLabels), CreatedAt: now,
	}
	messageJSON, err := json.Marshal(input.Message)
	if err != nil {
		return sessiontree.SubAgentInputRecord{}, err
	}
	hostJSON, err := json.Marshal(input.HostLabels)
	if err != nil {
		return sessiontree.SubAgentInputRecord{}, err
	}
	correlationJSON, err := json.Marshal(input.CorrelationLabels)
	if err != nil {
		return sessiontree.SubAgentInputRecord{}, err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO subagent_inputs(
		subagent_input_id, parent_thread_id, child_thread_id, request_kind, request_id, request_fingerprint,
		sequence, state, message_json, host_labels_json, correlation_labels_json, created_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`,
		input.SubAgentInputID, parentThreadID, childThreadID, string(kind), requestID, fingerprint,
		sequence, string(messageJSON), string(hostJSON), string(correlationJSON), formatTime(now))
	if isConstraintError(err) {
		return sessiontree.SubAgentInputRecord{}, sessiontree.ErrRequestConflict
	}
	return input, err
}

func loadNextPendingSubAgentInput(ctx context.Context, q sqlRunner, childThreadID string) (sessiontree.SubAgentInputRecord, bool, error) {
	var inputID string
	err := q.QueryRowContext(ctx, `SELECT subagent_input_id FROM subagent_inputs
		WHERE child_thread_id = ? AND state = 'pending' ORDER BY sequence, subagent_input_id LIMIT 1`, childThreadID).Scan(&inputID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.SubAgentInputRecord{}, false, nil
	}
	if err != nil {
		return sessiontree.SubAgentInputRecord{}, false, err
	}
	return loadSubAgentInput(ctx, q, inputID)
}

func loadAdmittedSubAgentInput(ctx context.Context, q sqlRunner, childThreadID, turnID, runID string) (sessiontree.SubAgentInputRecord, bool, error) {
	var inputID string
	err := q.QueryRowContext(ctx, `SELECT subagent_input_id FROM subagent_inputs
		WHERE child_thread_id = ? AND state = 'admitted' AND (admitted_turn_id = ? OR admitted_run_id = ?)
		LIMIT 1`, childThreadID, turnID, runID).Scan(&inputID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.SubAgentInputRecord{}, false, nil
	}
	if err != nil {
		return sessiontree.SubAgentInputRecord{}, false, err
	}
	return loadSubAgentInput(ctx, q, inputID)
}

func loadSubAgentInput(ctx context.Context, q sqlRunner, inputID string) (sessiontree.SubAgentInputRecord, bool, error) {
	var input sessiontree.SubAgentInputRecord
	var kind, state, messageJSON, hostJSON, correlationJSON, created, admitted, cancelled string
	err := q.QueryRowContext(ctx, `SELECT
		subagent_input_id, parent_thread_id, child_thread_id, request_kind, request_id, request_fingerprint,
		sequence, state, message_json, host_labels_json, correlation_labels_json,
		admitted_turn_id, admitted_run_id, created_at, admitted_at, cancelled_at
		FROM subagent_inputs WHERE subagent_input_id = ?`, inputID).Scan(
		&input.SubAgentInputID, &input.ParentThreadID, &input.ChildThreadID, &kind, &input.RequestID, &input.RequestFingerprint,
		&input.Sequence, &state, &messageJSON, &hostJSON, &correlationJSON,
		&input.AdmittedTurnID, &input.AdmittedRunID, &created, &admitted, &cancelled,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.SubAgentInputRecord{}, false, nil
	}
	if err != nil {
		return sessiontree.SubAgentInputRecord{}, false, err
	}
	input.RequestKind = sessiontree.SubAgentRequestKind(kind)
	input.State = sessiontree.SubAgentInputState(state)
	input.CreatedAt = parseTime(created)
	input.AdmittedAt = parseTime(admitted)
	input.CancelledAt = parseTime(cancelled)
	if err := json.Unmarshal([]byte(messageJSON), &input.Message); err != nil {
		return sessiontree.SubAgentInputRecord{}, false, err
	}
	if err := json.Unmarshal([]byte(hostJSON), &input.HostLabels); err != nil {
		return sessiontree.SubAgentInputRecord{}, false, err
	}
	if err := json.Unmarshal([]byte(correlationJSON), &input.CorrelationLabels); err != nil {
		return sessiontree.SubAgentInputRecord{}, false, err
	}
	return input, true, nil
}

func (s *Store) acquireTurnLeaseWithRunner(ctx context.Context, tx sqlRunner, request sessiontree.TurnLease) (sessiontree.TurnLease, error) {
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
		ThreadID: request.ThreadID, Purpose: sessiontree.TurnLeasePurposeTurn, TurnID: request.TurnID, OwnerID: request.OwnerID,
		Generation: generation, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(s.leasePolicy.TTL),
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO active_turn_leases(
		thread_id, purpose, turn_id, mutation_id, mutation_kind, owner_id, generation, heartbeat, acquired_at, renewed_at, expires_at
	) VALUES(?, 'turn', ?, '', '', ?, ?, 0, ?, ?, ?)`,
		proof.ThreadID, proof.TurnID, proof.OwnerID, proof.Generation, formatTime(proof.AcquiredAt), formatTime(proof.RenewedAt), formatTime(proof.ExpiresAt))
	if isConstraintError(err) {
		return sessiontree.TurnLease{}, sessiontree.ErrActiveTurn
	}
	return proof, err
}

func authorityNow(value time.Time, now func() time.Time) time.Time {
	if !value.IsZero() {
		return value.UTC()
	}
	return now().UTC()
}

func cloneStringMapSQLite(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

var _ sessiontree.SubAgentInputAuthorityRepo = (*Store)(nil)
