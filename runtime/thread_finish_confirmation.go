package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

type ThreadFinishConfirmer interface {
	ConfirmTurnFinish(context.Context, identity.ThreadID, identity.TurnID, identity.RunID) error
}

const ThreadRunPhaseAwaitingConfirmation ThreadRunPhase = "awaiting_confirmation"

func (service *threadRuntimeService) exposePendingConfirmation(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.view.Activity == ThreadActivityActive && actor.state.turnID == turnID && actor.state.runID == runID {
			if actor.state.cancelOwner == "run:"+runID.String() {
				actor.state.cancel = nil
				actor.state.cancelOwner = ""
			}
			actor.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhaseAwaitingConfirmation}
			actor.state.view.ViewVersion++
		}
		return nil
	})
}

func (service *threadRuntimeService) confirmPersistedCompletion(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) bool {
	found, err := service.confirmTurnIntent(ctx, threadID, turnID, runID)
	if found && (errors.Is(err, context.Canceled) || errors.Is(err, sessiontree.ErrEffectOutcomeUnknown)) {
		return false
	}
	if err != nil {
		service.host.store.reportBackgroundError(err)
	}
	return found || err != nil
}

func (service *threadRuntimeService) ConfirmTurnFinish(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) error {
	if ctx == nil {
		return errors.New("turn finish confirmation requires a context")
	}
	if err := service.host.requireExecution(); err != nil {
		return err
	}
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return err
	}
	found, err := service.confirmTurnIntent(ctx, threadID, turnID, runID)
	if err != nil {
		return runtimeHostError(err)
	}
	if !found {
		return ErrStaleAuthority
	}
	return nil
}

func (service *threadRuntimeService) confirmTurnIntent(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) (bool, error) {
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return false, err
	}
	if _, err := identity.ParseTurnID(turnID.String()); err != nil {
		return false, err
	}
	if _, err := identity.ParseRunID(runID.String()); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	store := service.host.store.repo
	request, found, err := sessiontree.ReadCompletedTurnIntent(ctx, store, threadID.String(), turnID.String(), runID.String())
	if err != nil || !found {
		return false, err
	}
	terminal, readErr := store.Entry(ctx, threadID.String(), request.TerminalEntryID)
	if readErr == nil {
		if terminal.Type != sessiontree.EntryTurnMarker || terminal.TurnStatus != sessiontree.TurnCompleted || terminal.ThreadID != request.ThreadID || terminal.TurnID != request.TurnID || terminal.RunID != request.RunID || terminal.RequestFingerprint != request.OutcomeFingerprint {
			return true, sessiontree.ErrRequestConflict
		}
		service.finishSend(service.runtime(threadID), turnID, runID, TurnResult{ThreadID: threadID, TurnID: turnID, RunID: runID, Status: TurnStatusCompleted}, nil)
		return true, nil
	}
	if !errors.Is(readErr, sessiontree.ErrEntryNotFound) {
		return true, readErr
	}
	repo, ok := store.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return true, ErrUnsupportedStoreCapability
	}
	result, err := repo.FinishTurn(ctx, request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			service.exposePendingConfirmation(service.runtime(threadID), turnID, runID)
		}
		return true, err
	}
	if result.Terminal.RequestFingerprint != request.OutcomeFingerprint || result.Terminal.TurnStatus != sessiontree.TurnCompleted {
		return true, ErrAuthorityCorrupt
	}
	service.finishSend(service.runtime(threadID), turnID, runID, TurnResult{ThreadID: threadID, TurnID: turnID, RunID: runID, Status: TurnStatusCompleted}, nil)
	return true, nil
}
