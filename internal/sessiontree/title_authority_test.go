package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestThreadTitleAuthorityValidatesTypedOperations(t *testing.T) {
	now := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	if err := ValidateSetThreadTitleRequest(SetThreadTitleRequest{ThreadID: "thread", Title: "title", Now: now}); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"manual missing title": func() error {
			return ValidateSetThreadTitleRequest(SetThreadTitleRequest{ThreadID: "thread", Now: now})
		},
		"begin missing token": func() error {
			return ValidateBeginAutomaticThreadTitleRequest(BeginAutomaticThreadTitleRequest{ThreadID: "thread", Now: now})
		},
		"complete missing generation": func() error {
			return ValidateCompleteAutomaticThreadTitleRequest(CompleteAutomaticThreadTitleRequest{ThreadID: "thread", Token: "token", Title: "title", Now: now})
		},
		"failure missing error": func() error {
			return ValidateFailAutomaticThreadTitleRequest(FailAutomaticThreadTitleRequest{ThreadID: "thread", Generation: 1, Token: "token", Now: now})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(); err == nil {
				t.Fatal("validation succeeded")
			}
		})
	}
}

func TestMemoryRepoUpdateThreadCannotBypassTitleAuthority(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	meta, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	meta.Title = "bypass"
	meta.TitleStatus = ThreadTitleReady
	meta.TitleSource = ThreadTitleSourceHost
	meta.TitleUpdatedAt = time.Now().UTC()
	meta.TitleGeneration = 1
	if err := repo.UpdateThread(ctx, meta); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("UpdateThread err=%v, want ErrRequestConflict", err)
	}
	got, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "" || got.TitleStatus != "" || got.TitleGeneration != 0 {
		t.Fatalf("title bypass mutated thread: %#v", got)
	}
}

func TestMemoryRepoPendingAutomaticThreadTitlesRejectsCorruptState(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(context.Background(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	meta := repo.threads["thread"]
	meta.TitleStatus = ThreadTitlePending
	repo.threads["thread"] = meta
	repo.mu.Unlock()
	if _, err := repo.PendingAutomaticThreadTitles(context.Background()); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("pending corrupt state err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryRepoPendingAutomaticTitleFencesSubAgentClose(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{
		ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "/root/child",
	}); err != nil {
		t.Fatal(err)
	}
	begin := BeginAutomaticThreadTitleRequest{ThreadID: "child", Token: "title-worker", Now: time.Now().UTC()}
	pending, err := repo.BeginAutomaticThreadTitle(ctx, begin)
	if err != nil {
		t.Fatal(err)
	}
	closeRequest := PrepareSubAgentCloseRequest{
		CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "done", Now: time.Now().UTC(),
	}
	if _, err := repo.PrepareSubAgentClose(ctx, closeRequest); !errors.Is(err, ErrThreadAuthorityBusy) {
		t.Fatalf("PrepareSubAgentClose pending title err = %v, want ErrThreadAuthorityBusy", err)
	}
	meta, err := repo.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if meta.IsClosing() || meta.IsClosed() || meta.TitleStatus != ThreadTitlePending {
		t.Fatalf("pending title close changed child authority: %#v", meta)
	}
	if _, err := repo.FailAutomaticThreadTitle(ctx, FailAutomaticThreadTitleRequest{
		ThreadID: "child", Generation: pending.Thread.TitleGeneration, Token: begin.Token,
		Error: "title stopped", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PrepareSubAgentClose(ctx, closeRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishSubAgentClose(ctx, FinishSubAgentCloseRequest{
		CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "done", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	meta, err = repo.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if !meta.IsClosed() || meta.TitleStatus != ThreadTitleFailed {
		t.Fatalf("settled title close state = %#v", meta)
	}
	if pending, err := repo.PendingAutomaticThreadTitles(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("pending titles after close = %#v err=%v", pending, err)
	}
}
