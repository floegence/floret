package sessiontree

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"time"
)

type SetThreadTitleRequest struct {
	ThreadID string
	Title    string
	Now      time.Time
}

type BeginAutomaticThreadTitleRequest struct {
	ThreadID string
	Token    string
	Now      time.Time
}

type CompleteAutomaticThreadTitleRequest struct {
	ThreadID   string
	Generation int64
	Token      string
	Title      string
	Now        time.Time
}

type FailAutomaticThreadTitleRequest struct {
	ThreadID   string
	Generation int64
	Token      string
	Error      string
	Now        time.Time
}

type ThreadTitleMutationResult struct {
	Thread  ThreadMeta
	Changed bool
}

type ThreadTitleAuthorityRepo interface {
	SetThreadTitle(context.Context, SetThreadTitleRequest) (ThreadTitleMutationResult, error)
	BeginAutomaticThreadTitle(context.Context, BeginAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error)
	CompleteAutomaticThreadTitle(context.Context, CompleteAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error)
	FailAutomaticThreadTitle(context.Context, FailAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error)
	PendingAutomaticThreadTitles(context.Context) ([]ThreadMeta, error)
}

func ValidateSetThreadTitleRequest(req SetThreadTitleRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.Title) == "" || req.Now.IsZero() {
		return errors.New("manual thread title requires thread identity, title, and time")
	}
	return nil
}

func ValidateBeginAutomaticThreadTitleRequest(req BeginAutomaticThreadTitleRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.Token) == "" || req.Now.IsZero() {
		return errors.New("automatic thread title begin requires thread identity, token, and time")
	}
	return nil
}

func ValidateCompleteAutomaticThreadTitleRequest(req CompleteAutomaticThreadTitleRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || req.Generation <= 0 || strings.TrimSpace(req.Token) == "" ||
		strings.TrimSpace(req.Title) == "" || req.Now.IsZero() {
		return errors.New("automatic thread title completion requires thread identity, generation, token, title, and time")
	}
	return nil
}

func ValidateFailAutomaticThreadTitleRequest(req FailAutomaticThreadTitleRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || req.Generation <= 0 || strings.TrimSpace(req.Token) == "" ||
		strings.TrimSpace(req.Error) == "" || req.Now.IsZero() {
		return errors.New("automatic thread title failure requires thread identity, generation, token, error, and time")
	}
	return nil
}

func ValidateThreadTitleState(meta ThreadMeta) error {
	title := strings.TrimSpace(meta.Title)
	token := strings.TrimSpace(meta.TitleToken)
	titleError := strings.TrimSpace(meta.TitleError)
	if title != meta.Title || token != meta.TitleToken || titleError != meta.TitleError {
		return ErrAuthorityCorrupt
	}
	switch meta.TitleStatus {
	case "":
		if title != "" || meta.TitleSource != "" || !meta.TitleUpdatedAt.IsZero() || titleError != "" || meta.TitleGeneration != 0 || token != "" {
			return ErrAuthorityCorrupt
		}
	case ThreadTitlePending:
		if title != "" || meta.TitleSource != "" || meta.TitleUpdatedAt.IsZero() || titleError != "" || meta.TitleGeneration <= 0 || token == "" {
			return ErrAuthorityCorrupt
		}
	case ThreadTitleReady:
		if title == "" || meta.TitleUpdatedAt.IsZero() || titleError != "" || meta.TitleGeneration <= 0 {
			return ErrAuthorityCorrupt
		}
		switch meta.TitleSource {
		case ThreadTitleSourceHost:
			if token != "" {
				return ErrAuthorityCorrupt
			}
		case ThreadTitleSourceProvider:
			if token == "" {
				return ErrAuthorityCorrupt
			}
		default:
			return ErrAuthorityCorrupt
		}
	case ThreadTitleFailed:
		if title != "" || meta.TitleSource != "" || meta.TitleUpdatedAt.IsZero() || titleError == "" || meta.TitleGeneration <= 0 || token == "" {
			return ErrAuthorityCorrupt
		}
	default:
		return ErrAuthorityCorrupt
	}
	return nil
}

func SameThreadTitleState(left, right ThreadMeta) bool {
	return left.Title == right.Title && left.TitleStatus == right.TitleStatus && left.TitleSource == right.TitleSource &&
		left.TitleUpdatedAt.Equal(right.TitleUpdatedAt) && left.TitleError == right.TitleError &&
		left.TitleGeneration == right.TitleGeneration && left.TitleToken == right.TitleToken
}

func (r *MemoryRepo) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateSetThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	return r.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta ThreadMeta) (ThreadMeta, bool, error) {
		title := strings.TrimSpace(req.Title)
		if meta.TitleStatus == ThreadTitleReady && meta.TitleSource == ThreadTitleSourceHost && meta.Title == title {
			return meta, false, nil
		}
		if meta.TitleGeneration == math.MaxInt64 {
			return meta, false, ErrAuthorityCorrupt
		}
		meta.Title = title
		meta.TitleStatus = ThreadTitleReady
		meta.TitleSource = ThreadTitleSourceHost
		meta.TitleUpdatedAt = req.Now.UTC()
		meta.TitleError = ""
		meta.TitleGeneration++
		meta.TitleToken = ""
		return meta, true, nil
	})
}

func (r *MemoryRepo) BeginAutomaticThreadTitle(ctx context.Context, req BeginAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateBeginAutomaticThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	token := strings.TrimSpace(req.Token)
	return r.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta ThreadMeta) (ThreadMeta, bool, error) {
		switch meta.TitleStatus {
		case ThreadTitleReady:
			return meta, false, nil
		case ThreadTitlePending:
			if meta.TitleToken == token {
				return meta, false, nil
			}
			return meta, false, ErrRequestConflict
		case ThreadTitleFailed:
			if meta.TitleToken == token {
				return meta, false, nil
			}
		}
		if meta.TitleGeneration == math.MaxInt64 {
			return meta, false, ErrAuthorityCorrupt
		}
		meta.Title = ""
		meta.TitleStatus = ThreadTitlePending
		meta.TitleSource = ""
		meta.TitleUpdatedAt = req.Now.UTC()
		meta.TitleError = ""
		meta.TitleGeneration++
		meta.TitleToken = token
		return meta, true, nil
	})
}

func (r *MemoryRepo) CompleteAutomaticThreadTitle(ctx context.Context, req CompleteAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateCompleteAutomaticThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	token := strings.TrimSpace(req.Token)
	title := strings.TrimSpace(req.Title)
	return r.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta ThreadMeta) (ThreadMeta, bool, error) {
		if meta.TitleGeneration != req.Generation || meta.TitleToken != token {
			return meta, false, ErrStaleAuthority
		}
		switch meta.TitleStatus {
		case ThreadTitleReady:
			if meta.TitleSource == ThreadTitleSourceProvider && meta.Title == title {
				return meta, false, nil
			}
			return meta, false, ErrRequestConflict
		case ThreadTitleFailed:
			return meta, false, ErrRequestConflict
		case ThreadTitlePending:
			meta.Title = title
			meta.TitleStatus = ThreadTitleReady
			meta.TitleSource = ThreadTitleSourceProvider
			meta.TitleUpdatedAt = req.Now.UTC()
			meta.TitleError = ""
			return meta, true, nil
		default:
			return meta, false, ErrAuthorityCorrupt
		}
	})
}

func (r *MemoryRepo) FailAutomaticThreadTitle(ctx context.Context, req FailAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateFailAutomaticThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	token := strings.TrimSpace(req.Token)
	titleError := strings.TrimSpace(req.Error)
	return r.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), func(meta ThreadMeta) (ThreadMeta, bool, error) {
		if meta.TitleGeneration != req.Generation || meta.TitleToken != token {
			return meta, false, ErrStaleAuthority
		}
		switch meta.TitleStatus {
		case ThreadTitleFailed:
			if meta.TitleError == titleError {
				return meta, false, nil
			}
			return meta, false, ErrRequestConflict
		case ThreadTitleReady:
			return meta, false, ErrRequestConflict
		case ThreadTitlePending:
			meta.Title = ""
			meta.TitleStatus = ThreadTitleFailed
			meta.TitleSource = ""
			meta.TitleUpdatedAt = req.Now.UTC()
			meta.TitleError = titleError
			return meta, true, nil
		default:
			return meta, false, ErrAuthorityCorrupt
		}
	})
}

func (r *MemoryRepo) PendingAutomaticThreadTitles(_ context.Context) ([]ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ThreadMeta, 0)
	for _, meta := range r.threads {
		if err := ValidateThreadTitleState(meta); err != nil {
			return nil, err
		}
		if meta.TitleStatus == ThreadTitlePending {
			out = append(out, meta)
		}
	}
	slices.SortFunc(out, func(left, right ThreadMeta) int {
		if order := left.TitleUpdatedAt.Compare(right.TitleUpdatedAt); order != 0 {
			return order
		}
		return strings.Compare(left.ID, right.ID)
	})
	return out, nil
}

func (r *MemoryRepo) mutateThreadTitle(ctx context.Context, threadID string, mutate func(ThreadMeta) (ThreadMeta, bool, error)) (ThreadTitleMutationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[threadID]
	if !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return ThreadTitleMutationResult{}, ErrThreadDeleted
		}
		return ThreadTitleMutationResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	if r.threadAuthorityClaimedLocked(threadID) {
		return ThreadTitleMutationResult{}, ErrThreadAuthorityBusy
	}
	if err := ValidateThreadTitleState(meta); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	updated, changed, err := mutate(meta)
	if err != nil {
		return ThreadTitleMutationResult{}, err
	}
	if !changed {
		return ThreadTitleMutationResult{Thread: meta}, nil
	}
	if err := ValidateThreadTitleState(updated); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	r.threads[threadID] = updated
	return ThreadTitleMutationResult{Thread: updated, Changed: true}, nil
}

func (r *FileRepo) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (ThreadTitleMutationResult, error) {
	return r.mutatePersistedThreadTitle(ctx, func() (ThreadTitleMutationResult, error) {
		return r.mem.SetThreadTitle(ctx, req)
	})
}

func (r *FileRepo) BeginAutomaticThreadTitle(ctx context.Context, req BeginAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	return r.mutatePersistedThreadTitle(ctx, func() (ThreadTitleMutationResult, error) {
		return r.mem.BeginAutomaticThreadTitle(ctx, req)
	})
}

func (r *FileRepo) CompleteAutomaticThreadTitle(ctx context.Context, req CompleteAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	return r.mutatePersistedThreadTitle(ctx, func() (ThreadTitleMutationResult, error) {
		return r.mem.CompleteAutomaticThreadTitle(ctx, req)
	})
}

func (r *FileRepo) FailAutomaticThreadTitle(ctx context.Context, req FailAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	return r.mutatePersistedThreadTitle(ctx, func() (ThreadTitleMutationResult, error) {
		return r.mem.FailAutomaticThreadTitle(ctx, req)
	})
}

func (r *FileRepo) PendingAutomaticThreadTitles(ctx context.Context) ([]ThreadMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return nil, err
	}
	return r.mem.PendingAutomaticThreadTitles(ctx)
}

func (r *FileRepo) mutatePersistedThreadTitle(ctx context.Context, mutate func() (ThreadTitleMutationResult, error)) (ThreadTitleMutationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(ctx); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	result, err := mutate()
	if err != nil || !result.Changed {
		return result, err
	}
	if err := r.saveThread(result.Thread); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	return result, nil
}
