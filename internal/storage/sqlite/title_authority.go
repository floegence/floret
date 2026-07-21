package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) SetThreadTitle(ctx context.Context, req sessiontree.SetThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	if err := sessiontree.ValidateSetThreadTitleRequest(req); err != nil {
		return sessiontree.ThreadTitleMutationResult{}, err
	}
	return s.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta sessiontree.ThreadMeta) (sessiontree.ThreadMeta, bool, error) {
		title := strings.TrimSpace(req.Title)
		if meta.TitleStatus == sessiontree.ThreadTitleReady && meta.TitleSource == sessiontree.ThreadTitleSourceHost && meta.Title == title {
			return meta, false, nil
		}
		if meta.TitleGeneration == math.MaxInt64 {
			return meta, false, sessiontree.ErrAuthorityCorrupt
		}
		meta.Title = title
		meta.TitleStatus = sessiontree.ThreadTitleReady
		meta.TitleSource = sessiontree.ThreadTitleSourceHost
		meta.TitleUpdatedAt = req.Now.UTC()
		meta.TitleError = ""
		meta.TitleGeneration++
		meta.TitleToken = ""
		return meta, true, nil
	})
}

func (s *Store) BeginAutomaticThreadTitle(ctx context.Context, req sessiontree.BeginAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	if err := sessiontree.ValidateBeginAutomaticThreadTitleRequest(req); err != nil {
		return sessiontree.ThreadTitleMutationResult{}, err
	}
	token := strings.TrimSpace(req.Token)
	return s.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta sessiontree.ThreadMeta) (sessiontree.ThreadMeta, bool, error) {
		switch meta.TitleStatus {
		case sessiontree.ThreadTitleReady:
			return meta, false, nil
		case sessiontree.ThreadTitlePending:
			if meta.TitleToken == token {
				return meta, false, nil
			}
			return meta, false, sessiontree.ErrRequestConflict
		case sessiontree.ThreadTitleFailed:
			if meta.TitleToken == token {
				return meta, false, nil
			}
		}
		if meta.TitleGeneration == math.MaxInt64 {
			return meta, false, sessiontree.ErrAuthorityCorrupt
		}
		meta.Title = ""
		meta.TitleStatus = sessiontree.ThreadTitlePending
		meta.TitleSource = ""
		meta.TitleUpdatedAt = req.Now.UTC()
		meta.TitleError = ""
		meta.TitleGeneration++
		meta.TitleToken = token
		return meta, true, nil
	})
}

func (s *Store) CompleteAutomaticThreadTitle(ctx context.Context, req sessiontree.CompleteAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	if err := sessiontree.ValidateCompleteAutomaticThreadTitleRequest(req); err != nil {
		return sessiontree.ThreadTitleMutationResult{}, err
	}
	token := strings.TrimSpace(req.Token)
	title := strings.TrimSpace(req.Title)
	return s.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta sessiontree.ThreadMeta) (sessiontree.ThreadMeta, bool, error) {
		if meta.TitleGeneration != req.Generation || meta.TitleToken != token {
			return meta, false, sessiontree.ErrStaleAuthority
		}
		switch meta.TitleStatus {
		case sessiontree.ThreadTitleReady:
			if meta.TitleSource == sessiontree.ThreadTitleSourceProvider && meta.Title == title {
				return meta, false, nil
			}
			return meta, false, sessiontree.ErrRequestConflict
		case sessiontree.ThreadTitleFailed:
			return meta, false, sessiontree.ErrRequestConflict
		case sessiontree.ThreadTitlePending:
			meta.Title = title
			meta.TitleStatus = sessiontree.ThreadTitleReady
			meta.TitleSource = sessiontree.ThreadTitleSourceProvider
			meta.TitleUpdatedAt = req.Now.UTC()
			meta.TitleError = ""
			return meta, true, nil
		default:
			return meta, false, sessiontree.ErrAuthorityCorrupt
		}
	})
}

func (s *Store) FailAutomaticThreadTitle(ctx context.Context, req sessiontree.FailAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	if err := sessiontree.ValidateFailAutomaticThreadTitleRequest(req); err != nil {
		return sessiontree.ThreadTitleMutationResult{}, err
	}
	token := strings.TrimSpace(req.Token)
	titleError := strings.TrimSpace(req.Error)
	return s.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta sessiontree.ThreadMeta) (sessiontree.ThreadMeta, bool, error) {
		if meta.TitleGeneration != req.Generation || meta.TitleToken != token {
			return meta, false, sessiontree.ErrStaleAuthority
		}
		switch meta.TitleStatus {
		case sessiontree.ThreadTitleFailed:
			if meta.TitleError == titleError {
				return meta, false, nil
			}
			return meta, false, sessiontree.ErrRequestConflict
		case sessiontree.ThreadTitleReady:
			return meta, false, sessiontree.ErrRequestConflict
		case sessiontree.ThreadTitlePending:
			meta.Title = ""
			meta.TitleStatus = sessiontree.ThreadTitleFailed
			meta.TitleSource = ""
			meta.TitleUpdatedAt = req.Now.UTC()
			meta.TitleError = titleError
			return meta, true, nil
		default:
			return meta, false, sessiontree.ErrAuthorityCorrupt
		}
	})
}

func (s *Store) PendingAutomaticThreadTitles(ctx context.Context) ([]sessiontree.ThreadMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, lifecycle, close_operation_id, archived,
		title, title_status, title_source, title_updated_at, title_error, title_generation, title_token,
		created_at, updated_at, last_viewed_at
		FROM threads WHERE title_status = ? ORDER BY title_updated_at, id`, sessiontree.ThreadTitlePending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]sessiontree.ThreadMeta, 0)
	for rows.Next() {
		meta, err := scanThreadMeta(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) mutateThreadTitle(ctx context.Context, threadID string, mutate func(sessiontree.ThreadMeta) (sessiontree.ThreadMeta, bool, error)) (sessiontree.ThreadTitleMutationResult, error) {
	var result sessiontree.ThreadTitleMutationResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
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
		if err := rejectClaimedThreadAuthorities(ctx, tx, threadID); err != nil {
			return err
		}
		updated, changed, err := mutate(meta)
		if err != nil {
			return err
		}
		if !changed {
			result.Thread = meta
			return nil
		}
		if err := sessiontree.ValidateThreadTitleState(updated); err != nil {
			return err
		}
		if err := updateThread(ctx, tx, updated); err != nil {
			return err
		}
		result = sessiontree.ThreadTitleMutationResult{Thread: updated, Changed: true}
		return nil
	})
	return result, err
}
