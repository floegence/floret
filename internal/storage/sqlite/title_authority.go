package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) SetThreadTitle(ctx context.Context, req sessiontree.SetThreadTitleRequest) (sessiontree.SetThreadTitleResult, error) {
	if err := sessiontree.ValidateSetThreadTitleRequest(req); err != nil {
		return sessiontree.SetThreadTitleResult{}, err
	}
	var result sessiontree.SetThreadTitleResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		threadID := strings.TrimSpace(req.ThreadID)
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
		active, _, err := loadTurnLease(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if err := validateSQLiteTurnLeaseMutation(ctx, threadID, "", active, s.now().UTC()); err != nil {
			return err
		}
		title := strings.TrimSpace(req.Title)
		titleError := strings.TrimSpace(req.Error)
		if req.Mode == sessiontree.ThreadTitleMutationAutomatic && strings.TrimSpace(meta.Title) != "" {
			result.Thread = meta
			return nil
		}
		if meta.Title == title && meta.TitleStatus == req.Status && meta.TitleSource == req.Source && meta.TitleError == titleError {
			result.Thread = meta
			return nil
		}
		meta.Title = title
		meta.TitleStatus = req.Status
		meta.TitleSource = req.Source
		meta.TitleError = titleError
		meta.TitleUpdatedAt = req.Now
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		result = sessiontree.SetThreadTitleResult{Thread: meta, Changed: true}
		return nil
	})
	return result, err
}
