package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
)

// ThreadCommandAck confirms successful admission by the existing command owner.
// It is not a view baseline or a new lifecycle record. RequestKey retains the
// command's existing idempotency semantics; clients must not replay an unknown
// outcome under a new key. Read current presentation independently after success.
type ThreadCommandAck struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	RequestKey  RequestKey        `json:"request_key"`
	TurnID      identity.TurnID   `json:"turn_id,omitempty"`
	RunID       identity.RunID    `json:"run_id,omitempty"`
	ViewVersion uint64            `json:"view_version,omitempty"`
}

// ThreadCommandService is optional so existing ThreadService implementations
// remain compatible. Both return forms execute the same command implementation.
// An acknowledgment never materializes a complete result just to discard it.
type ThreadCommandService interface {
	CreateAck(context.Context, CreateThreadInput) (ThreadCommandAck, error)
	ForkAck(context.Context, ForkThreadInput) (ThreadCommandAck, error)
	SetTitleAck(context.Context, SetTitleInput) (ThreadCommandAck, error)
	SendAck(context.Context, SendInput) (ThreadCommandAck, error)
	RespondAck(context.Context, RespondInput) (ThreadCommandAck, error)
	CancelAck(context.Context, CancelInput) (ThreadCommandAck, error)
	RetryAck(context.Context, RetryInput) (ThreadCommandAck, error)
	ReorderQueueAck(context.Context, ReorderQueueInput) (ThreadCommandAck, error)
	DeleteQueuedAck(context.Context, DeleteQueuedInput) (ThreadCommandAck, error)
	PromoteQueuedAck(context.Context, PromoteQueuedInput) (ThreadCommandAck, error)
	EditQueuedAck(context.Context, EditQueuedInput) (ThreadCommandAck, error)
	SendQueuedNowAck(context.Context, PromoteQueuedInput) (ThreadCommandAck, error)
}

var _ ThreadCommandService = (*threadRuntimeService)(nil)

type threadResultMode bool

const (
	threadFullResult threadResultMode = false
	threadAckResult  threadResultMode = true
)

func threadResult(view ThreadView, mode threadResultMode) ThreadView {
	if mode == threadFullResult {
		return cloneThreadRuntimeView(view)
	}
	return ThreadView{ThreadID: view.ThreadID, TurnID: view.TurnID, RunID: view.RunID, ViewVersion: view.ViewVersion, Activity: view.Activity, Cancellation: cloneThreadCancellation(view.Cancellation)}
}

func commandAck(view ThreadView, key RequestKey, err error) (ThreadCommandAck, error) {
	if err != nil {
		return ThreadCommandAck{}, err
	}
	canonicalKey, err := cleanRequestKey(key)
	if err != nil {
		return ThreadCommandAck{}, err
	}
	return ThreadCommandAck{ThreadID: view.ThreadID, RequestKey: RequestKey(canonicalKey), TurnID: view.TurnID, RunID: view.RunID, ViewVersion: view.ViewVersion}, nil
}

func (service *threadRuntimeService) currentResult(actor *threadRuntimeState, mode threadResultMode) ThreadView {
	var result ThreadView
	_ = actor.apply(context.Background(), func() error {
		service.flushUpdateLocked(actor)
		result = threadResult(actor.state.view, mode)
		return nil
	})
	return result
}

func (service *threadRuntimeService) completedCommandResult(ctx context.Context, threadID identity.ThreadID, mode threadResultMode) (ThreadView, error) {
	if mode == threadFullResult {
		return service.View(ctx, threadID)
	}
	// The command has committed. Presentation availability cannot undo it.
	// Zero ViewVersion means no actor baseline was observed, not version zero
	// of canonical history. Reading a baseline remains a separate operation.
	return ThreadView{ThreadID: threadID}, nil
}

func (service *threadRuntimeService) CreateAck(ctx context.Context, in CreateThreadInput) (ThreadCommandAck, error) {
	view, err := service.createCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) ForkAck(ctx context.Context, in ForkThreadInput) (ThreadCommandAck, error) {
	view, err := service.forkCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) SetTitleAck(ctx context.Context, in SetTitleInput) (ThreadCommandAck, error) {
	view, err := service.setTitleCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) SendAck(ctx context.Context, in SendInput) (ThreadCommandAck, error) {
	view, err := service.sendCommand(ctx, in, threadAckResult)
	ack, err := commandAck(view, in.RequestKey, err)
	if err != nil {
		return ThreadCommandAck{}, err
	}
	// Admission may queue the input or replay an older send while a different
	// run is active. Only this request's canonical marker names its execution.
	// The command already committed; a canceled transport cannot undo its Ack.
	ack.TurnID, ack.RunID = "", ""
	entry, found, err := service.host.store.repo.FindTurnRequest(context.WithoutCancel(ctx), in.ThreadID.String(), string(ack.RequestKey))
	if err != nil {
		return ThreadCommandAck{}, runtimeHostError(err)
	}
	if found {
		ack.TurnID, ack.RunID = identity.TurnID(entry.TurnID), identity.RunID(entry.RunID)
	}
	return ack, nil
}

func (service *threadRuntimeService) RespondAck(ctx context.Context, in RespondInput) (ThreadCommandAck, error) {
	view, err := service.respondCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) CancelAck(ctx context.Context, in CancelInput) (ThreadCommandAck, error) {
	view, err := service.cancelCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) RetryAck(ctx context.Context, in RetryInput) (ThreadCommandAck, error) {
	view, err := service.retryCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) ReorderQueueAck(ctx context.Context, in ReorderQueueInput) (ThreadCommandAck, error) {
	view, err := service.reorderQueueCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) DeleteQueuedAck(ctx context.Context, in DeleteQueuedInput) (ThreadCommandAck, error) {
	view, err := service.deleteQueuedCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) PromoteQueuedAck(ctx context.Context, in PromoteQueuedInput) (ThreadCommandAck, error) {
	view, err := service.promoteQueuedCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) EditQueuedAck(ctx context.Context, in EditQueuedInput) (ThreadCommandAck, error) {
	view, err := service.editQueuedCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}

func (service *threadRuntimeService) SendQueuedNowAck(ctx context.Context, in PromoteQueuedInput) (ThreadCommandAck, error) {
	view, err := service.sendQueuedNowCommand(ctx, in, threadAckResult)
	return commandAck(view, in.RequestKey, err)
}
