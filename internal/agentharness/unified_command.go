package agentharness

import (
	"errors"
	"fmt"
	"sync"

	"github.com/floegence/floret/v3/identity"
)

type unifiedCommandKind string

const (
	unifiedCommandSend    unifiedCommandKind = "send"
	unifiedCommandResolve unifiedCommandKind = "resolve"
	unifiedCommandCancel  unifiedCommandKind = "cancel"
	unifiedCommandRetry   unifiedCommandKind = "retry"
)

type unifiedCommand struct {
	RequestID identity.LogicalRequestID
	Kind      unifiedCommandKind
	Payload   []byte
}

type unifiedAccepted struct {
	RequestID identity.LogicalRequestID
	ThreadID  identity.ThreadID
	TurnID    identity.TurnID
	RunID     identity.RunID
	Revision  int64
}

type unifiedCommandState struct {
	ThreadID        identity.ThreadID
	ActiveTurn      unifiedTurn
	Interaction     *unifiedPendingInteraction
	Terminal        bool
	CancelRequested bool
}

type unifiedCommandActor struct {
	mu       sync.Mutex
	state    unifiedCommandState
	log      *unifiedEventLog
	requests map[identity.LogicalRequestID]unifiedAccepted
	nextTurn uint64
	nextRun  uint64
}

func newUnifiedCommandActor(threadID identity.ThreadID) *unifiedCommandActor {
	return &unifiedCommandActor{state: unifiedCommandState{ThreadID: threadID}, log: newUnifiedEventLog(threadID), requests: make(map[identity.LogicalRequestID]unifiedAccepted)}
}

func (actor *unifiedCommandActor) apply(command unifiedCommand) (unifiedAccepted, error) {
	if actor == nil || actor.log == nil {
		return unifiedAccepted{}, errors.New("unified command actor is nil")
	}
	if command.RequestID == "" || command.Kind == "" {
		return unifiedAccepted{}, errors.New("unified command requires request id and kind")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if accepted, ok := actor.requests[command.RequestID]; ok {
		return accepted, nil
	}
	if actor.state.Terminal && command.Kind != unifiedCommandRetry {
		return unifiedAccepted{}, errors.New("thread is terminal")
	}
	var turnID identity.TurnID
	var runID identity.RunID
	switch command.Kind {
	case unifiedCommandSend:
		if actor.state.ActiveTurn.ID != "" && !actor.state.Terminal {
			return unifiedAccepted{}, errors.New("thread already has an active turn")
		}
		actor.nextTurn++
		actor.nextRun++
		turnID = identity.TurnID(fmt.Sprintf("turn-%d", actor.nextTurn))
		runID = identity.RunID(fmt.Sprintf("run-%d", actor.nextRun))
		actor.state.ActiveTurn = unifiedTurn{ID: turnID, ThreadID: actor.state.ThreadID, RunID: runID, Status: "running"}
		actor.state.Terminal = false
		if err := actor.appendEvent(unifiedCommandSend, turnID, runID); err != nil {
			return unifiedAccepted{}, err
		}
	case unifiedCommandResolve:
		if actor.state.Interaction == nil {
			return unifiedAccepted{}, errors.New("no pending interaction")
		}
		actor.state.Interaction = nil
		if err := actor.appendEvent(unifiedCommandResolve, actor.state.ActiveTurn.ID, actor.state.ActiveTurn.RunID); err != nil {
			return unifiedAccepted{}, err
		}
	case unifiedCommandCancel:
		if actor.state.Terminal {
			return actor.accepted(command.RequestID, "", "")
		}
		actor.state.CancelRequested = true
		actor.state.Terminal = true
		actor.state.ActiveTurn.Status = "cancelled"
		if err := actor.appendEvent(unifiedCommandCancel, actor.state.ActiveTurn.ID, actor.state.ActiveTurn.RunID); err != nil {
			return unifiedAccepted{}, err
		}
	case unifiedCommandRetry:
		if actor.state.ActiveTurn.ID == "" {
			return unifiedAccepted{}, errors.New("no turn to retry")
		}
		actor.nextRun++
		runID = identity.RunID(fmt.Sprintf("run-%d", actor.nextRun))
		actor.state.ActiveTurn.RunID = runID
		actor.state.ActiveTurn.Status = "running"
		actor.state.Terminal = false
		actor.state.CancelRequested = false
		turnID = actor.state.ActiveTurn.ID
		if err := actor.appendEvent(unifiedCommandRetry, turnID, runID); err != nil {
			return unifiedAccepted{}, err
		}
	default:
		return unifiedAccepted{}, fmt.Errorf("unsupported unified command %q", command.Kind)
	}
	return actor.accepted(command.RequestID, turnID, runID)
}

func (actor *unifiedCommandActor) appendEvent(kind unifiedCommandKind, turnID identity.TurnID, runID identity.RunID) error {
	revision := actor.log.currentRevision() + 1
	return actor.log.append(unifiedTimelineEvent{ID: fmt.Sprintf("event-%d", revision), ThreadID: actor.state.ThreadID, TurnID: turnID, RunID: runID, Revision: revision, Kind: unifiedEventKind(kind)})
}

func (actor *unifiedCommandActor) accepted(requestID identity.LogicalRequestID, turnID identity.TurnID, runID identity.RunID) (unifiedAccepted, error) {
	accepted := unifiedAccepted{RequestID: requestID, ThreadID: actor.state.ThreadID, TurnID: turnID, RunID: runID, Revision: actor.log.currentRevision()}
	actor.requests[requestID] = accepted
	return accepted, nil
}
