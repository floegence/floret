package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) PrepareSubAgentClose(ctx context.Context, req sessiontree.PrepareSubAgentCloseRequest) (sessiontree.PrepareSubAgentCloseResult, error) {
	operationID, parentThreadID, targetThreadID, reason, intentFingerprint, err := sessiontree.NormalizeSubAgentCloseIntent(
		req.CloseOperationID, req.ParentThreadID, req.TargetThreadID, req.Reason,
	)
	if err != nil {
		return sessiontree.PrepareSubAgentCloseResult{}, err
	}
	var result sessiontree.PrepareSubAgentCloseResult
	err = s.withImmediate(ctx, func(tx sqlRunner) error {
		parent, err := loadThread(ctx, tx, parentThreadID)
		if err != nil {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, parentThreadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			}
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
			return err
		}
		existing, found, err := loadSubAgentCloseOperation(ctx, tx, operationID)
		if err != nil {
			return err
		}
		if found {
			if existing.IntentFingerprint != intentFingerprint {
				return sessiontree.ErrRequestConflict
			}
			if existing.State == sessiontree.SubAgentClosePrepared {
				if err := s.validatePreparedSubAgentClose(ctx, tx, existing, req.TargetLease); err != nil {
					return err
				}
			} else if existing.State != sessiontree.SubAgentCloseCompleted {
				return sessiontree.ErrAuthorityCorrupt
			}
			result = sessiontree.PrepareSubAgentCloseResult{Operation: existing, Replayed: true}
			return nil
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, parentThreadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrThreadAuthorityBusy
		}
		target, err := loadThread(ctx, tx, targetThreadID)
		if err != nil {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, targetThreadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			}
			return err
		}
		if target.ParentThreadID != parentThreadID {
			return sessiontree.ErrInvalidThreadAuthority
		}
		targetLifecycle, err := target.CanonicalLifecycle()
		if err != nil {
			return err
		}
		switch targetLifecycle {
		case sessiontree.ThreadLifecycleClosing:
			return sessiontree.ErrSubAgentClosing
		case sessiontree.ThreadLifecycleClosed:
			return sessiontree.ErrThreadClosed
		case sessiontree.ThreadLifecycleOpen:
		default:
			return sessiontree.ErrAuthorityCorrupt
		}
		threads, err := loadThreadAuthorityGraph(ctx, tx)
		if err != nil {
			return err
		}
		threadIDs, err := sessiontree.ThreadAuthorityTreeIDs(threads, targetThreadID)
		if err != nil {
			return err
		}
		byID := make(map[string]sessiontree.ThreadMeta, len(threads))
		for _, meta := range threads {
			byID[meta.ID] = meta
		}
		nodes := make([]sessiontree.SubAgentCloseNode, 0, len(threadIDs))
		for _, threadID := range threadIDs {
			meta := byID[threadID]
			lifecycle, err := meta.CanonicalLifecycle()
			if err != nil {
				return err
			}
			if lifecycle != sessiontree.ThreadLifecycleOpen && lifecycle != sessiontree.ThreadLifecycleClosed {
				return sessiontree.ErrSubAgentClosing
			}
			if strings.TrimSpace(meta.CloseOperationID) != "" {
				return sessiontree.ErrAuthorityCorrupt
			}
			if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, threadID); err != nil {
				return err
			} else if claimed {
				return sessiontree.ErrThreadAuthorityBusy
			}
			lease, active, err := loadTurnLease(ctx, tx, threadID)
			if err != nil {
				return err
			}
			if active {
				if threadID != targetThreadID || req.TargetLease == nil || !sessiontree.SameTurnLease(lease, *req.TargetLease) ||
					lease.Purpose != sessiontree.TurnLeasePurposeTurn || !lease.Fresh(s.now().UTC()) {
					return sessiontree.ErrThreadAuthorityBusy
				}
			} else if threadID == targetThreadID && req.TargetLease != nil {
				return sessiontree.ErrStaleAuthority
			}
			if lifecycle == sessiontree.ThreadLifecycleClosed {
				pending, err := countPendingSubAgentInputs(ctx, tx, threadID)
				if err != nil {
					return err
				}
				if pending != 0 {
					return sessiontree.ErrAuthorityCorrupt
				}
			}
			nodes = append(nodes, sessiontree.SubAgentCloseNode{ThreadID: threadID, WasOpen: lifecycle == sessiontree.ThreadLifecycleOpen})
		}
		requestFingerprint, err := sessiontree.SubAgentCloseRequestFingerprint(intentFingerprint, nodes)
		if err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		for _, node := range nodes {
			if !node.WasOpen {
				continue
			}
			meta := byID[node.ThreadID]
			meta.Lifecycle = sessiontree.ThreadLifecycleClosing
			meta.CloseOperationID = operationID
			meta.UpdatedAt = now
			if err := updateThread(ctx, tx, meta); err != nil {
				return err
			}
		}
		operation := sessiontree.SubAgentCloseOperation{
			CloseOperationID: operationID, ParentThreadID: parentThreadID, TargetThreadID: targetThreadID, Reason: reason,
			IntentFingerprint: intentFingerprint, RequestFingerprint: requestFingerprint, State: sessiontree.SubAgentClosePrepared,
			Nodes: append([]sessiontree.SubAgentCloseNode(nil), nodes...), PreparedAt: now,
		}
		if err := insertSubAgentCloseOperation(ctx, tx, operation); err != nil {
			return err
		}
		result = sessiontree.PrepareSubAgentCloseResult{Operation: operation}
		return nil
	})
	return result, err
}

func (s *Store) FinishSubAgentClose(ctx context.Context, req sessiontree.FinishSubAgentCloseRequest) (sessiontree.FinishSubAgentCloseResult, error) {
	operationID, parentThreadID, targetThreadID, reason, intentFingerprint, err := sessiontree.NormalizeSubAgentCloseIntent(
		req.CloseOperationID, req.ParentThreadID, req.TargetThreadID, req.Reason,
	)
	if err != nil {
		return sessiontree.FinishSubAgentCloseResult{}, err
	}
	var result sessiontree.FinishSubAgentCloseResult
	err = s.withImmediate(ctx, func(tx sqlRunner) error {
		operation, found, err := loadSubAgentCloseOperation(ctx, tx, operationID)
		if err != nil {
			return err
		}
		if !found {
			return sessiontree.ErrThreadNotFound
		}
		if operation.IntentFingerprint != intentFingerprint || operation.ParentThreadID != parentThreadID || operation.TargetThreadID != targetThreadID || operation.Reason != reason {
			return sessiontree.ErrRequestConflict
		}
		parent, err := loadThread(ctx, tx, parentThreadID)
		if err != nil {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, parentThreadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			}
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
			return err
		}
		if operation.State == sessiontree.SubAgentCloseCompleted {
			replayed, err := replayedSQLiteSubAgentClose(ctx, tx, operation)
			if err != nil {
				return err
			}
			result = replayed
			return nil
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, parentThreadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrThreadAuthorityBusy
		}
		if err := s.validatePreparedSubAgentClose(ctx, tx, operation, nil); err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		cancelled := make([]string, 0)
		for _, node := range operation.Nodes {
			if !node.WasOpen {
				continue
			}
			ids, err := pendingSubAgentInputIDs(ctx, tx, node.ThreadID)
			if err != nil {
				return err
			}
			cancelled = append(cancelled, ids...)
			if _, err := tx.ExecContext(ctx, `UPDATE subagent_inputs SET state = 'cancelled', cancelled_at = ?
				WHERE child_thread_id = ? AND state = 'pending'`, formatTime(now), node.ThreadID); err != nil {
				return err
			}
		}
		entries := make([]sessiontree.Entry, 0, len(operation.Nodes))
		threads := make([]sessiontree.ThreadMeta, 0, len(operation.Nodes))
		resultEntryIDs := make([]string, 0, len(operation.Nodes))
		for index := len(operation.Nodes) - 1; index >= 0; index-- {
			node := operation.Nodes[index]
			meta, err := loadThread(ctx, tx, node.ThreadID)
			if err != nil {
				return err
			}
			if node.WasOpen {
				entryID, err := nextEntryID(ctx, tx, node.ThreadID)
				if err != nil {
					return err
				}
				entry := sessiontree.PrepareSubAgentCloseLifecycleEntry(operation, node.ThreadID, meta.LeafID, entryID, now)
				ordinal, err := nextOrdinal(ctx, tx, node.ThreadID)
				if err != nil {
					return err
				}
				if err := insertEntry(ctx, tx, entry, ordinal, true); err != nil {
					return err
				}
				meta.LeafID = entry.ID
				meta.Lifecycle = sessiontree.ThreadLifecycleClosed
				meta.CloseOperationID = ""
				meta.UpdatedAt = now
				if err := updateThread(ctx, tx, meta); err != nil {
					return err
				}
				entries = append(entries, entry)
				resultEntryIDs = append(resultEntryIDs, entry.ID)
			}
			threads = append(threads, meta)
		}
		slices.Sort(cancelled)
		operation.State = sessiontree.SubAgentCloseCompleted
		operation.ResultEntryIDs = resultEntryIDs
		operation.FinishedAt = now
		if err := updateSubAgentCloseOperation(ctx, tx, operation); err != nil {
			return err
		}
		result = sessiontree.FinishSubAgentCloseResult{
			Operation: operation, Threads: threads, Entries: entries, CancelledInputIDs: cancelled,
		}
		return nil
	})
	return result, err
}

func (s *Store) validatePreparedSubAgentClose(ctx context.Context, tx sqlRunner, operation sessiontree.SubAgentCloseOperation, targetLease *sessiontree.TurnLease) error {
	if operation.State != sessiontree.SubAgentClosePrepared || len(operation.Nodes) == 0 {
		return sessiontree.ErrAuthorityCorrupt
	}
	threads, err := loadThreadAuthorityGraph(ctx, tx)
	if err != nil {
		return err
	}
	threadIDs, err := sessiontree.ThreadAuthorityTreeIDs(threads, operation.TargetThreadID)
	if err != nil || len(threadIDs) != len(operation.Nodes) {
		return sessiontree.ErrAuthorityCorrupt
	}
	byID := make(map[string]sessiontree.ThreadMeta, len(threads))
	for _, meta := range threads {
		byID[meta.ID] = meta
	}
	for index, threadID := range threadIDs {
		if operation.Nodes[index].ThreadID != threadID {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
	for _, node := range operation.Nodes {
		meta, ok := byID[node.ThreadID]
		if !ok {
			return sessiontree.ErrAuthorityCorrupt
		}
		lifecycle, err := meta.CanonicalLifecycle()
		if err != nil {
			return err
		}
		if node.WasOpen {
			if lifecycle != sessiontree.ThreadLifecycleClosing || meta.CloseOperationID != operation.CloseOperationID {
				return sessiontree.ErrAuthorityCorrupt
			}
		} else {
			pending, err := countPendingSubAgentInputs(ctx, tx, node.ThreadID)
			if err != nil {
				return err
			}
			if lifecycle != sessiontree.ThreadLifecycleClosed || strings.TrimSpace(meta.CloseOperationID) != "" || pending != 0 {
				return sessiontree.ErrAuthorityCorrupt
			}
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, node.ThreadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrThreadAuthorityBusy
		}
		lease, active, err := loadTurnLease(ctx, tx, node.ThreadID)
		if err != nil {
			return err
		}
		if !active {
			if node.ThreadID == operation.TargetThreadID && targetLease != nil {
				return sessiontree.ErrStaleAuthority
			}
			continue
		}
		if node.ThreadID != operation.TargetThreadID || targetLease == nil || !sessiontree.SameTurnLease(lease, *targetLease) || !lease.Fresh(s.now().UTC()) {
			return sessiontree.ErrThreadAuthorityBusy
		}
	}
	return nil
}

func insertSubAgentCloseOperation(ctx context.Context, q sqlRunner, operation sessiontree.SubAgentCloseOperation) error {
	nodesJSON, err := json.Marshal(operation.Nodes)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO subagent_close_operations(
		close_operation_id, parent_thread_id, target_thread_id, reason, intent_fingerprint, request_fingerprint,
		state, nodes_json, result_entry_ids_json, prepared_at, finished_at
	) VALUES(?, ?, ?, ?, ?, ?, 'prepared', ?, '[]', ?, '')`,
		operation.CloseOperationID, operation.ParentThreadID, operation.TargetThreadID, operation.Reason,
		operation.IntentFingerprint, operation.RequestFingerprint, string(nodesJSON), formatTime(operation.PreparedAt))
	if isConstraintError(err) {
		return sessiontree.ErrRequestConflict
	}
	return err
}

func updateSubAgentCloseOperation(ctx context.Context, q sqlRunner, operation sessiontree.SubAgentCloseOperation) error {
	entryIDsJSON, err := json.Marshal(operation.ResultEntryIDs)
	if err != nil {
		return err
	}
	result, err := q.ExecContext(ctx, `UPDATE subagent_close_operations SET
		state = 'completed', result_entry_ids_json = ?, finished_at = ?
		WHERE close_operation_id = ? AND state = 'prepared'`, string(entryIDsJSON), formatTime(operation.FinishedAt), operation.CloseOperationID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func loadSubAgentCloseOperation(ctx context.Context, q sqlRunner, operationID string) (sessiontree.SubAgentCloseOperation, bool, error) {
	var operation sessiontree.SubAgentCloseOperation
	var state, nodesJSON, resultEntryIDsJSON, preparedAt, finishedAt string
	err := q.QueryRowContext(ctx, `SELECT
		close_operation_id, parent_thread_id, target_thread_id, reason, intent_fingerprint, request_fingerprint,
		state, nodes_json, result_entry_ids_json, prepared_at, finished_at
		FROM subagent_close_operations WHERE close_operation_id = ?`, operationID).Scan(
		&operation.CloseOperationID, &operation.ParentThreadID, &operation.TargetThreadID, &operation.Reason,
		&operation.IntentFingerprint, &operation.RequestFingerprint, &state, &nodesJSON, &resultEntryIDsJSON, &preparedAt, &finishedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.SubAgentCloseOperation{}, false, nil
	}
	if err != nil {
		return sessiontree.SubAgentCloseOperation{}, false, err
	}
	operation.State = sessiontree.SubAgentCloseState(state)
	operation.PreparedAt = parseTime(preparedAt)
	operation.FinishedAt = parseTime(finishedAt)
	if err := json.Unmarshal([]byte(nodesJSON), &operation.Nodes); err != nil {
		return sessiontree.SubAgentCloseOperation{}, false, err
	}
	if err := json.Unmarshal([]byte(resultEntryIDsJSON), &operation.ResultEntryIDs); err != nil {
		return sessiontree.SubAgentCloseOperation{}, false, err
	}
	return operation, true, nil
}

func replayedSQLiteSubAgentClose(ctx context.Context, q sqlRunner, operation sessiontree.SubAgentCloseOperation) (sessiontree.FinishSubAgentCloseResult, error) {
	threads := make([]sessiontree.ThreadMeta, 0, len(operation.Nodes))
	openNodes := make([]sessiontree.SubAgentCloseNode, 0, len(operation.Nodes))
	for index := len(operation.Nodes) - 1; index >= 0; index-- {
		node := operation.Nodes[index]
		meta, err := loadThread(ctx, q, node.ThreadID)
		if err != nil || !meta.IsClosed() || strings.TrimSpace(meta.CloseOperationID) != "" {
			return sessiontree.FinishSubAgentCloseResult{}, sessiontree.ErrAuthorityCorrupt
		}
		threads = append(threads, meta)
		if node.WasOpen {
			openNodes = append(openNodes, node)
		}
	}
	if len(openNodes) != len(operation.ResultEntryIDs) {
		return sessiontree.FinishSubAgentCloseResult{}, sessiontree.ErrAuthorityCorrupt
	}
	entries := make([]sessiontree.Entry, 0, len(openNodes))
	for index, node := range openNodes {
		entry, err := loadEntry(ctx, q, node.ThreadID, operation.ResultEntryIDs[index])
		if err != nil || entry.Metadata["close_operation_id"] != operation.CloseOperationID {
			return sessiontree.FinishSubAgentCloseResult{}, sessiontree.ErrAuthorityCorrupt
		}
		entries = append(entries, entry)
	}
	return sessiontree.FinishSubAgentCloseResult{Operation: operation, Threads: threads, Entries: entries, Replayed: true}, nil
}

func countPendingSubAgentInputs(ctx context.Context, q sqlRunner, threadID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM subagent_inputs WHERE child_thread_id = ? AND state = 'pending'`, threadID).Scan(&count)
	return count, err
}

func pendingSubAgentInputIDs(ctx context.Context, q sqlRunner, threadID string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT subagent_input_id FROM subagent_inputs
		WHERE child_thread_id = ? AND state = 'pending' ORDER BY sequence, subagent_input_id`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

var _ sessiontree.SubAgentCloseAuthorityRepo = (*Store)(nil)
