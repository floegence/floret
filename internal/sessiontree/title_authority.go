package sessiontree

import (
	"context"
	"errors"
	"strings"
	"time"
)

type SetThreadTitleRequest struct {
	ThreadID string
	Mode     ThreadTitleMutationMode
	Title    string
	Status   ThreadTitleStatus
	Source   ThreadTitleSource
	Error    string
	Now      time.Time
}

type ThreadTitleMutationMode string

const (
	ThreadTitleMutationManual    ThreadTitleMutationMode = "manual"
	ThreadTitleMutationAutomatic ThreadTitleMutationMode = "automatic"
)

type SetThreadTitleResult struct {
	Thread  ThreadMeta
	Changed bool
}

type ThreadTitleAuthorityRepo interface {
	SetThreadTitle(context.Context, SetThreadTitleRequest) (SetThreadTitleResult, error)
}

func ValidateSetThreadTitleRequest(req SetThreadTitleRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || req.Now.IsZero() {
		return errors.New("thread title mutation requires thread identity and time")
	}
	switch req.Mode {
	case ThreadTitleMutationManual:
		if req.Status != ThreadTitleReady || req.Source != ThreadTitleSourceHost {
			return errors.New("manual title mutation requires a ready host title")
		}
	case ThreadTitleMutationAutomatic:
		if req.Status == ThreadTitleReady && req.Source != ThreadTitleSourceProvider {
			return errors.New("automatic ready title requires provider source")
		}
	case "":
		return errors.New("thread title mutation mode is required")
	default:
		return errors.New("thread title mutation mode is invalid")
	}
	switch req.Status {
	case ThreadTitleReady:
		if strings.TrimSpace(req.Title) == "" || strings.TrimSpace(req.Error) != "" {
			return errors.New("ready thread title requires a title without an error")
		}
		switch req.Source {
		case ThreadTitleSourceHost, ThreadTitleSourceProvider:
		default:
			return errors.New("ready thread title requires a valid source")
		}
	case ThreadTitleFailed:
		if strings.TrimSpace(req.Title) != "" || req.Source != "" || strings.TrimSpace(req.Error) == "" {
			return errors.New("failed thread title requires only an error")
		}
	default:
		return errors.New("thread title mutation requires a terminal title status")
	}
	return nil
}

func (r *MemoryRepo) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (SetThreadTitleResult, error) {
	if err := ValidateSetThreadTitleRequest(req); err != nil {
		return SetThreadTitleResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID := strings.TrimSpace(req.ThreadID)
	meta, ok := r.threads[threadID]
	if !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return SetThreadTitleResult{}, ErrThreadDeleted
		}
		return SetThreadTitleResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return SetThreadTitleResult{}, err
	}
	if r.threadAuthorityClaimedLocked(threadID) {
		return SetThreadTitleResult{}, ErrThreadAuthorityBusy
	}
	if err := r.requireThreadWriteAuthorityLocked(ctx, threadID); err != nil {
		return SetThreadTitleResult{}, err
	}
	title := strings.TrimSpace(req.Title)
	titleError := strings.TrimSpace(req.Error)
	if req.Mode == ThreadTitleMutationAutomatic && strings.TrimSpace(meta.Title) != "" {
		return SetThreadTitleResult{Thread: meta}, nil
	}
	if meta.Title == title && meta.TitleStatus == req.Status && meta.TitleSource == req.Source && meta.TitleError == titleError {
		return SetThreadTitleResult{Thread: meta}, nil
	}
	meta.Title = title
	meta.TitleStatus = req.Status
	meta.TitleSource = req.Source
	meta.TitleError = titleError
	meta.TitleUpdatedAt = req.Now
	r.threads[threadID] = meta
	return SetThreadTitleResult{Thread: meta, Changed: true}, nil
}
