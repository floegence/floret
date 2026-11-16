package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadSteeringCommandService admits steering through the same owner as
// ThreadSteeringController without materializing a presentation history.
type ThreadSteeringCommandService interface {
	SteerAck(context.Context, SteerInput) (ThreadCommandAck, error)
}

// ThreadSteeringReader returns the original accepted steering input, including
// when an idle thread started a turn. It survives application, cancellation and
// restart. Lookup does not authorize the caller or admit work.
type ThreadSteeringReader interface {
	LookupSteer(context.Context, LookupSteerInput) (UserInput, bool, error)
}

type LookupSteerInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

var _ ThreadSteeringCommandService = (*threadRuntimeService)(nil)
var _ ThreadSteeringReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) SteerAck(ctx context.Context, in SteerInput) (ThreadCommandAck, error) {
	view, err := service.steer(ctx, in, nil, threadAckResult)
	ack, err := commandAck(view, in.RequestKey, err)
	if err != nil {
		return ThreadCommandAck{}, err
	}
	// A replay can observe a later execution. Only the accepted request names
	// its original execution; transport cancellation cannot undo admission.
	ctx = context.WithoutCancel(ctx)
	entry, err := service.host.store.repo.Entry(ctx, in.ThreadID.String(), "steer:"+string(ack.RequestKey))
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		var found bool
		entry, found, err = service.host.store.repo.FindTurnRequest(ctx, in.ThreadID.String(), string(ack.RequestKey))
		if !found && err == nil {
			ack.TurnID, ack.RunID = "", ""
			return ack, nil
		}
	}
	if err != nil {
		return ThreadCommandAck{}, runtimeHostError(err)
	}
	ack.TurnID, ack.RunID = identity.TurnID(entry.TurnID), identity.RunID(entry.RunID)
	return ack, nil
}

func (service *threadRuntimeService) LookupSteer(ctx context.Context, in LookupSteerInput) (UserInput, bool, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return UserInput{}, false, err
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return UserInput{}, false, err
	}
	entry, err := service.host.store.repo.Entry(ctx, in.ThreadID.String(), "steer:"+key)
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		// Idle steering uses ordinary admission, which is also accepted as a
		// replay by Steer. Preserve that existing shared-key contract.
		return service.LookupSend(ctx, LookupSendInput{ThreadID: in.ThreadID, RequestKey: in.RequestKey})
	}
	if err != nil {
		return UserInput{}, false, runtimeHostError(err)
	}
	if entry.Type != sessiontree.EntrySteeringRequested {
		return UserInput{}, false, ErrRequestConflict
	}
	return turnInputFromSessionMessage(entry.Message), true, nil
}
