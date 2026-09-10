package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/agentharness"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

const gracefulCancellationLimit = 5 * time.Second

func cloneThreadCancellation(in *ThreadCancellation) *ThreadCancellation {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func threadCancellationForTurn(entries []sessiontree.Entry, turnID identity.TurnID) *ThreadCancellation {
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryCancelRequested || entry.TurnID != turnID.String() {
			continue
		}
		source := entry.Metadata["cancellation_source"]
		// Historical requests did not record their source.
		if source == "" {
			return nil
		}
		return &ThreadCancellation{ThreadID: identity.ThreadID(entry.ThreadID), Source: source, Mode: CancelMode(entry.Metadata["cancellation_mode"]),
			TurnID: turnID, RunID: identity.RunID(entry.RunID), RequestedAt: entry.CreatedAt}
	}
	return nil
}

func (service *threadRuntimeService) requestGracefulCancellation(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, key string) (ThreadView, error) {
	repo, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	var cancel context.CancelCauseFunc
	var done <-chan struct{}
	var runID identity.RunID
	var request cancellationRequest
	var deadline time.Time
	err := actor.apply(ctx, func() error {
		if actor.state.turnID != turnID || actor.state.view.Activity != ThreadActivityActive || actor.state.view.Cancellation != nil {
			return nil
		}
		runID = actor.state.runID
		fingerprint, err := stableFingerprint(struct {
			ThreadID identity.ThreadID
			TurnID   identity.TurnID
			Mode     CancelMode
		}{threadID, turnID, CancelModeGraceful})
		if err != nil {
			return err
		}
		request = cancellationRequest{EntryID: "cancel:" + key, RequestKey: key, RequestFingerprint: fingerprint}
		now := time.Now().UTC()
		result, err := repo.CancelTurn(ctx, sessiontree.CancelTurnRequest{
			RequestOnly: true, ThreadID: threadID.String(), TurnID: turnID.String(), RunID: runID.String(),
			CancelEntryID: request.EntryID, RequestKey: key, RequestFingerprint: fingerprint,
			TerminalEntryID: stableCancellationEntryID(threadID, turnID, runID), OutcomeFingerprint: fingerprint,
			InteractionResolutionPayload: []byte(`{"accepted":false,"outcome":"cancelled"}`),
			Metadata:                     map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureCancelled},
			CancellationMetadata:         map[string]string{"cancellation_source": "user_stop", "cancellation_mode": string(CancelModeGraceful)},
			Now:                          now,
		})
		if err != nil {
			return err
		}
		if result.CancelRequest.ID == "" {
			// Natural completion committed before Stop acquired authority.
			return nil
		}
		actor.state.view.Cancellation = threadCancellationForTurn([]sessiontree.Entry{result.CancelRequest}, turnID)
		actor.state.view.ViewVersion++
		deadline = result.CancelRequest.CreatedAt.Add(gracefulCancellationLimit)
		cancel, done = actor.state.cancel, actor.state.executionDone
		if cancel != nil {
			cancel(&agentharness.GracefulCancellation{Deadline: deadline})
		}
		return nil
	})
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	current := service.currentView(actor)
	if deadline.IsZero() {
		return current, nil
	}
	service.recordCancellation(threadID, turnID, runID, "user_stop", "graceful stop requested")
	service.publish(current)
	go func() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		if done != nil {
			select {
			case <-done:
			case <-timer.C:
			}
		}
		finishCtx, finish := context.WithTimeout(context.Background(), 5*time.Second)
		defer finish()
		if _, err := service.settleCancellation(finishCtx, actor, threadID, turnID, runID, request, "user_stop", "graceful stop settled"); err != nil && !errors.Is(err, sessiontree.ErrStaleAuthority) && !errors.Is(err, ErrThreadDeleted) && !errors.Is(err, ErrHostClosed) {
			service.host.store.reportBackgroundError(err)
		}
	}()
	return current, nil
}
