package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

func rejectSQLiteThreadWriteLifecycle(meta sessiontree.ThreadMeta) error {
	lifecycle, err := meta.CanonicalLifecycle()
	if err != nil {
		return sessiontree.ErrAuthorityCorrupt
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

func (s *Store) CreateRoot(ctx context.Context, req sessiontree.CreateRootRequest) (sessiontree.CreateRootResult, error) {
	if err := sessiontree.ValidateCreateRootRequest(req); err != nil {
		return sessiontree.CreateRootResult{}, err
	}
	var result sessiontree.CreateRootResult
	fingerprint := sessiontree.CreateRootFingerprint(req)
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		var existingThreadID, existingFingerprint string
		err := tx.QueryRowContext(ctx, `SELECT thread_id, request_fingerprint FROM root_create_intents WHERE create_intent_id = ?`, strings.TrimSpace(req.CreateIntentID)).Scan(&existingThreadID, &existingFingerprint)
		if err == nil {
			if existingThreadID != strings.TrimSpace(req.ThreadID) || existingFingerprint != fingerprint {
				return sessiontree.ErrRequestConflict
			}
			meta, loadErr := loadThread(ctx, tx, req.ThreadID)
			if loadErr == nil {
				if !sessiontree.CreateRootReplayMatches(meta, req.ThreadID) {
					return sessiontree.ErrRequestConflict
				}
				result = sessiontree.CreateRootResult{Thread: meta, Replayed: true}
				return nil
			}
			if errors.Is(loadErr, sessiontree.ErrThreadNotFound) {
				if _, tombstoneErr := loadThreadTombstone(ctx, tx, req.ThreadID); tombstoneErr == nil {
					return sessiontree.ErrThreadDeleted
				}
				return sessiontree.ErrAuthorityCorrupt
			}
			if errors.Is(loadErr, sessiontree.ErrInvalidThreadAuthority) {
				if exists, existsErr := threadExists(ctx, tx, req.ThreadID); existsErr != nil {
					return existsErr
				} else if exists {
					return sessiontree.ErrRequestConflict
				}
			}
			return loadErr
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, tombstoneErr := loadThreadTombstone(ctx, tx, req.ThreadID); tombstoneErr == nil {
			return sessiontree.ErrThreadDeleted
		} else if !errors.Is(tombstoneErr, sql.ErrNoRows) && !errors.Is(tombstoneErr, sessiontree.ErrThreadNotFound) {
			return tombstoneErr
		}
		var existingIntentID string
		if err := tx.QueryRowContext(ctx, `SELECT create_intent_id FROM root_create_intents WHERE thread_id = ?`, strings.TrimSpace(req.ThreadID)).Scan(&existingIntentID); err == nil {
			return sessiontree.ErrRequestConflict
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if exists, err := threadExists(ctx, tx, req.ThreadID); err != nil {
			return err
		} else if exists {
			return sessiontree.ErrRequestConflict
		}
		metaRequest := req.Meta
		metaRequest.CreatedAt = s.now().UTC()
		metaRequest.UpdatedAt = metaRequest.CreatedAt
		metaRequest.Lifecycle = sessiontree.ThreadLifecycleOpen
		meta, err := createThreadWithRunner(ctx, tx, metaRequest)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO root_create_intents(
			create_intent_id, thread_id, contract_version, request_fingerprint, created_at
		) VALUES(?, ?, ?, ?, ?)`, strings.TrimSpace(req.CreateIntentID), strings.TrimSpace(req.ThreadID),
			strings.TrimSpace(req.ContractVersion), fingerprint, formatTime(meta.CreatedAt)); err != nil {
			return err
		}
		result = sessiontree.CreateRootResult{Thread: meta}
		return nil
	})
	return result, err
}

func (s *Store) DeleteRootTree(ctx context.Context, rootThreadID string) (sessiontree.DeleteRootTreeResult, error) {
	rootThreadID = strings.TrimSpace(rootThreadID)
	if rootThreadID == "" {
		return sessiontree.DeleteRootTreeResult{}, errors.New("root thread id is required")
	}
	var result sessiontree.DeleteRootTreeResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		if tombstone, err := loadThreadTombstone(ctx, tx, rootThreadID); err == nil {
			if tombstone.ThreadID != rootThreadID || tombstone.RootThreadID != rootThreadID {
				return sessiontree.ErrAuthorityCorrupt
			}
			rows, queryErr := tx.QueryContext(ctx, `SELECT thread_id FROM thread_tombstones WHERE root_thread_id = ? ORDER BY thread_id`, rootThreadID)
			if queryErr != nil {
				return queryErr
			}
			defer rows.Close()
			var threadIDs []string
			for rows.Next() {
				var threadID string
				if err := rows.Scan(&threadID); err != nil {
					return err
				}
				threadIDs = append(threadIDs, threadID)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			result = sessiontree.DeleteRootTreeResult{ThreadIDs: threadIDs, Replayed: true}
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, sessiontree.ErrThreadNotFound) {
			return err
		}
		threads, err := loadThreadAuthorityGraph(ctx, tx)
		if err != nil {
			return err
		}
		threadIDs, err := sessiontree.ThreadAuthorityTreeIDs(threads, rootThreadID)
		if err != nil {
			return err
		}
		byID := make(map[string]sessiontree.ThreadMeta, len(threads))
		for _, meta := range threads {
			byID[meta.ID] = meta
		}
		for _, threadID := range threadIDs {
			lifecycle, err := byID[threadID].CanonicalLifecycle()
			if err != nil {
				return err
			}
			if lifecycle == sessiontree.ThreadLifecycleClosing {
				return sessiontree.ErrSubAgentClosing
			}
			if _, active, err := loadTurnLease(ctx, tx, threadID); err != nil {
				return err
			} else if active {
				return sessiontree.ErrThreadAuthorityBusy
			}
			if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, threadID); err != nil {
				return err
			} else if claimed {
				return sessiontree.ErrThreadAuthorityBusy
			}
		}
		now := s.now().UTC()
		createIntentIDs := make(map[string]string, len(threadIDs))
		for _, threadID := range threadIDs {
			var createIntentID string
			err := tx.QueryRowContext(ctx, `SELECT create_intent_id FROM root_create_intents WHERE thread_id = ?`, threadID).Scan(&createIntentID)
			switch {
			case err == nil:
				createIntentIDs[threadID] = createIntentID
			case errors.Is(err, sql.ErrNoRows):
			default:
				return err
			}
		}
		for _, threadID := range threadIDs {
			meta := byID[threadID]
			if _, err := tx.ExecContext(ctx, `INSERT INTO thread_tombstones(
				thread_id, root_thread_id, parent_thread_id, create_intent_id,
				fork_operation_id, fork_operation_node_id, forked_from_thread_id,
				forked_from_entry_id, deleted_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, threadID, rootThreadID, meta.ParentThreadID, createIntentIDs[threadID],
				meta.ForkOperationID, meta.ForkOperationNodeID, meta.ForkedFromThreadID, meta.ForkedFromEntryID, formatTime(now)); err != nil {
				return err
			}
		}
		if err := deleteThreadTreeDataWithRunner(ctx, tx, threadIDs); err != nil {
			return err
		}
		result = sessiontree.DeleteRootTreeResult{ThreadIDs: append([]string(nil), threadIDs...)}
		return nil
	})
	return result, err
}

func (s *Store) ThreadTombstone(ctx context.Context, threadID string) (sessiontree.ThreadTombstone, error) {
	tombstone, err := loadThreadTombstone(ctx, s.db, threadID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ThreadTombstone{}, sessiontree.ErrThreadNotFound
	}
	return tombstone, err
}

func (s *Store) InspectThreadAuthority(ctx context.Context, threadID string) (sessiontree.ThreadAuthoritySnapshot, error) {
	var snapshot sessiontree.ThreadAuthoritySnapshot
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		meta, err := loadThread(ctx, tx, strings.TrimSpace(threadID))
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, threadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			}
		}
		if err != nil {
			return err
		}
		snapshot.Thread = meta
		if lease, active, err := loadTurnLease(ctx, tx, threadID); err != nil {
			return err
		} else if active {
			snapshot.Lease = &lease
		}
		if operationID, claimed, err := loadThreadAuthorityClaim(ctx, tx, threadID); err != nil {
			return err
		} else if claimed {
			snapshot.ClaimOperationID = operationID
		}
		path, err := pathWithRunner(ctx, tx, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidateThreadAuthorityState(path, snapshot.Lease, snapshot.ClaimOperationID); err != nil {
			return err
		}
		return nil
	})
	return snapshot, err
}

func loadThreadTombstone(ctx context.Context, q sqlRunner, threadID string) (sessiontree.ThreadTombstone, error) {
	var tombstone sessiontree.ThreadTombstone
	var deletedAt string
	err := q.QueryRowContext(ctx, `SELECT thread_id, root_thread_id, parent_thread_id, create_intent_id,
		fork_operation_id, fork_operation_node_id, forked_from_thread_id, forked_from_entry_id, deleted_at
		FROM thread_tombstones WHERE thread_id = ?`, strings.TrimSpace(threadID)).Scan(
		&tombstone.ThreadID, &tombstone.RootThreadID, &tombstone.ParentThreadID, &tombstone.CreateIntentID,
		&tombstone.ForkOperationID, &tombstone.ForkOperationNodeID, &tombstone.ForkedFromThreadID,
		&tombstone.ForkedFromEntryID, &deletedAt)
	if err != nil {
		return sessiontree.ThreadTombstone{}, err
	}
	tombstone.DeletedAt = parseTime(deletedAt)
	if tombstone.ThreadID == "" || tombstone.RootThreadID == "" || tombstone.DeletedAt.IsZero() {
		return sessiontree.ThreadTombstone{}, fmt.Errorf("%w: invalid thread tombstone %q", sessiontree.ErrAuthorityCorrupt, threadID)
	}
	return tombstone, nil
}

func deleteThreadTreeDataWithRunner(ctx context.Context, tx sqlRunner, threadIDs []string) error {
	for _, threadID := range threadIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM subagent_close_operations WHERE parent_thread_id = ? OR target_thread_id = ?`, threadID, threadID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tool_output_artifacts WHERE thread_id = ?`, threadID); err != nil {
			return err
		}
	}
	for _, promptScopeID := range threadIDs {
		for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE prompt_scope_id = ?`, promptScopeID); err != nil {
				return err
			}
		}
	}
	for i := len(threadIDs) - 1; i >= 0; i-- {
		if _, err := tx.ExecContext(ctx, `DELETE FROM threads WHERE id = ?`, threadIDs[i]); err != nil {
			return err
		}
	}
	return nil
}
