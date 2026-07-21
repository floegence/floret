package agentharness

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

const (
	defaultAutomaticTitleTimeout = 30 * time.Second
	automaticTitleInterrupted    = "automatic title generation interrupted"
	automaticTitleRestarted      = "automatic title generation interrupted during previous process"
)

func (h *AgentHarness) RecoverPendingAutomaticThreadTitles(ctx context.Context) error {
	if h == nil || h.options.Repo == nil {
		return errors.New("agent harness is not configured")
	}
	authority, ok := h.options.Repo.(sessiontree.ThreadTitleAuthorityRepo)
	if !ok {
		return errors.New("session tree repo does not support automatic thread title authority")
	}
	pending, err := authority.PendingAutomaticThreadTitles(ctx)
	if err != nil {
		return err
	}
	for _, claim := range pending {
		failed, failErr := authority.FailAutomaticThreadTitle(ctx, sessiontree.FailAutomaticThreadTitleRequest{
			ThreadID: claim.ID, Generation: claim.TitleGeneration, Token: claim.TitleToken,
			Error: automaticTitleRestarted, Now: h.now(),
		})
		if failErr != nil {
			return failErr
		}
		if failed.Changed {
			h.emit(HarnessEvent{
				Type: EventTitleFailed, ThreadID: claim.ID, Message: automaticTitleRestarted,
				Metadata: map[string]string{"generation": strconv.FormatInt(failed.Thread.TitleGeneration, 10)},
			})
		}
	}
	return nil
}

func (h *AgentHarness) beginBackgroundExecution(kind string) (context.Context, func(), error) {
	if h == nil || h.options.BeginBackgroundExecution == nil {
		return nil, nil, errors.New(kind + " lifetime authority is required")
	}
	ctx, finish, err := h.options.BeginBackgroundExecution()
	if err != nil {
		return nil, nil, err
	}
	if ctx == nil || finish == nil {
		return nil, nil, errors.New(kind + " lifetime authority returned an invalid handle")
	}
	return ctx, finish, nil
}

type automaticTitleExecution struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

func (e *automaticTitleExecution) FinishMain(cancelWorker bool) {
	if e == nil || !cancelWorker {
		return
	}
	if e.cancel != nil {
		e.cancel()
	}
	if e.done != nil {
		<-e.done
	}
}

func (t *Thread) startAutomaticTitle(ctx context.Context, turnID, runID, userEntryID string, input session.Message) (*automaticTitleExecution, error) {
	if t == nil || t.harness == nil || t.harness.options.TitleGenerator == nil {
		return nil, nil
	}
	authority, ok := t.harness.options.Repo.(sessiontree.ThreadTitleAuthorityRepo)
	if !ok {
		return nil, errors.New("session tree repo does not support automatic thread title authority")
	}
	t.authorityMu.RLock()
	authorityTransferred := false
	defer func() {
		if !authorityTransferred {
			t.authorityMu.RUnlock()
		}
	}()
	meta, err := t.harness.options.Repo.Thread(ctx, t.id)
	if err != nil {
		return nil, err
	}
	if err := sessiontree.ValidateThreadTitleState(meta); err != nil {
		return nil, err
	}
	switch meta.TitleStatus {
	case sessiontree.ThreadTitlePending, sessiontree.ThreadTitleReady:
		return nil, nil
	}
	token := automaticTitleToken(t.id, turnID, userEntryID)
	begun, err := authority.BeginAutomaticThreadTitle(ctx, sessiontree.BeginAutomaticThreadTitleRequest{
		ThreadID: t.id,
		Token:    token,
		Now:      t.harness.now(),
	})
	if err != nil {
		return nil, err
	}
	if !begun.Changed {
		return nil, nil
	}
	t.harness.emit(HarnessEvent{
		Type: EventTitlePending, RunID: runID, ThreadID: t.id, TurnID: turnID,
		Metadata: map[string]string{"generation": strconv.FormatInt(begun.Thread.TitleGeneration, 10)},
	})
	lifetimeCtx, finish, err := t.harness.beginBackgroundExecution("automatic title execution")
	if err != nil {
		settlementErr := t.failAutomaticTitle(ctx, authority, begun.Thread, turnID, runID, err)
		if settlementErr != nil {
			return nil, errors.Join(err, fmt.Errorf("persist automatic title startup failure: %w", settlementErr))
		}
		return nil, err
	}
	workerCtx, cancel := t.automaticTitleContext(lifetimeCtx)
	done := make(chan struct{})
	execution := &automaticTitleExecution{cancel: cancel, done: done}
	go func() {
		defer close(done)
		defer t.authorityMu.RUnlock()
		defer finish()
		defer cancel()
		t.runAutomaticTitle(workerCtx, authority, begun.Thread, turnID, runID, automaticTitleMessages(input))
	}()
	authorityTransferred = true
	return execution, nil
}

func automaticTitleToken(threadID, turnID, userEntryID string) string {
	return "automatic-title-" + sessiontree.StableHash(strings.Join([]string{
		strings.TrimSpace(threadID), strings.TrimSpace(turnID), strings.TrimSpace(userEntryID),
	}, "\x00"))[:24]
}

func automaticTitleMessages(input session.Message) []session.Message {
	text := strings.TrimSpace(input.Content)
	if text == "" {
		labels := make([]string, 0, len(input.References)+len(input.Attachments))
		for _, reference := range input.References {
			if label := strings.TrimSpace(reference.Label); label != "" {
				labels = append(labels, label)
			}
		}
		for _, attachment := range input.Attachments {
			if name := strings.TrimSpace(attachment.Name); name != "" {
				labels = append(labels, name)
			}
		}
		text = strings.Join(labels, "\n")
	}
	if text == "" {
		return nil
	}
	return []session.Message{{Role: session.User, Content: text}}
}

func (t *Thread) automaticTitleContext(lifetimeCtx context.Context) (context.Context, context.CancelFunc) {
	timeout := t.harness.options.AutomaticTitleTimeout
	if timeout <= 0 {
		timeout = defaultAutomaticTitleTimeout
	}
	return context.WithTimeout(lifetimeCtx, timeout)
}

func (t *Thread) runAutomaticTitle(
	ctx context.Context,
	authority sessiontree.ThreadTitleAuthorityRepo,
	claim sessiontree.ThreadMeta,
	turnID string,
	runID string,
	messages []session.Message,
) {
	if len(messages) == 0 {
		t.settleAutomaticTitleFailure(ctx, authority, claim, turnID, runID, errors.New("automatic title input is empty"))
		return
	}
	if err := t.validateAutomaticTitleProviderRequest(ctx); err != nil {
		t.settleAutomaticTitleFailure(ctx, authority, claim, turnID, runID, err)
		return
	}
	result, err := t.harness.options.TitleGenerator.GenerateTitle(ctx, TitleRequest{
		ThreadID: t.id,
		TurnID:   turnID,
		Messages: session.CloneMessages(messages),
	})
	if err != nil {
		t.settleAutomaticTitleFailure(ctx, authority, claim, turnID, runID, err)
		return
	}
	title := normalizeThreadTitle(result.Title, defaultThreadTitleMaxRunes)
	if title == "" {
		t.settleAutomaticTitleFailure(ctx, authority, claim, turnID, runID, errors.New("thread title is empty after normalization"))
		return
	}
	if result.Source != "" && result.Source != sessiontree.ThreadTitleSourceProvider {
		t.settleAutomaticTitleFailure(ctx, authority, claim, turnID, runID, errors.New("automatic thread title source must be provider"))
		return
	}
	persistCtx, cancelPersist := turnFinalizationContext(ctx)
	defer cancelPersist()
	updated, err := authority.CompleteAutomaticThreadTitle(persistCtx, sessiontree.CompleteAutomaticThreadTitleRequest{
		ThreadID: t.id, Generation: claim.TitleGeneration, Token: claim.TitleToken, Title: title, Now: t.harness.now(),
	})
	if automaticTitleClaimEnded(err) {
		return
	}
	if err != nil {
		completionErr := fmt.Errorf("automatic title completion persistence failed: %w", err)
		if failErr := t.failAutomaticTitle(ctx, authority, claim, turnID, runID, completionErr); failErr != nil {
			t.harness.reportBackgroundError(errors.Join(
				completionErr,
				fmt.Errorf("automatic title failure persistence failed: %w", failErr),
			))
		}
		return
	}
	if updated.Changed {
		t.harness.emit(HarnessEvent{
			Type: EventTitleUpdated, RunID: runID, ThreadID: t.id, TurnID: turnID, Message: title,
			Metadata: map[string]string{
				"source":     string(sessiontree.ThreadTitleSourceProvider),
				"generation": strconv.FormatInt(updated.Thread.TitleGeneration, 10),
			},
		})
	}
}

func (t *Thread) settleAutomaticTitleFailure(
	ctx context.Context,
	authority sessiontree.ThreadTitleAuthorityRepo,
	claim sessiontree.ThreadMeta,
	turnID string,
	runID string,
	cause error,
) {
	if err := t.failAutomaticTitle(ctx, authority, claim, turnID, runID, cause); err != nil {
		t.harness.reportBackgroundError(fmt.Errorf(
			"automatic title failure persistence failed after %q: %w",
			automaticTitleFailureMessage(cause),
			err,
		))
	}
}

func (h *AgentHarness) reportBackgroundError(err error) {
	if h != nil && err != nil && h.options.ReportBackgroundError != nil {
		h.options.ReportBackgroundError(err)
	}
}

func (t *Thread) failAutomaticTitle(
	ctx context.Context,
	authority sessiontree.ThreadTitleAuthorityRepo,
	claim sessiontree.ThreadMeta,
	turnID string,
	runID string,
	cause error,
) error {
	message := automaticTitleFailureMessage(cause)
	persistCtx, cancelPersist := turnFinalizationContext(ctx)
	defer cancelPersist()
	failed, err := authority.FailAutomaticThreadTitle(persistCtx, sessiontree.FailAutomaticThreadTitleRequest{
		ThreadID: t.id, Generation: claim.TitleGeneration, Token: claim.TitleToken, Error: message, Now: t.harness.now(),
	})
	if automaticTitleClaimEnded(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if failed.Changed {
		t.harness.emit(HarnessEvent{
			Type: EventTitleFailed, RunID: runID, ThreadID: t.id, TurnID: turnID, Message: message,
			Metadata: map[string]string{"generation": strconv.FormatInt(failed.Thread.TitleGeneration, 10)},
		})
	}
	return nil
}

func automaticTitleClaimEnded(err error) bool {
	return errors.Is(err, sessiontree.ErrStaleAuthority) || errors.Is(err, sessiontree.ErrThreadDeleted)
}

func automaticTitleFailureMessage(cause error) string {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return automaticTitleInterrupted
	}
	message := strings.TrimSpace(cause.Error())
	if message == "" {
		return automaticTitleInterrupted
	}
	return message
}

func (t *Thread) validateAutomaticTitleProviderRequest(ctx context.Context) error {
	if t == nil || t.harness == nil || t.harness.options.Repo == nil {
		return errors.New("automatic title provider request requires an authority-bound thread")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inspector, ok := t.harness.options.Repo.(sessiontree.ThreadAuthorityInspectionRepo)
	if !ok {
		return errors.New("session tree repo does not support provider authority inspection")
	}
	snapshot, err := inspector.InspectThreadAuthority(ctx, t.id)
	if err != nil {
		return err
	}
	lifecycle, err := snapshot.Thread.CanonicalLifecycle()
	if err != nil {
		return err
	}
	if lifecycle != sessiontree.ThreadLifecycleOpen || snapshot.ClaimOperationID != "" {
		return sessiontree.ErrThreadAuthorityBusy
	}
	return nil
}
