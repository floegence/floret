package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
)

// ThreadSubtreeStopper observes and stops concrete active executions in an
// authorized ownership subtree, even when the selected thread itself is idle.
type ThreadSubtreeStopper interface {
	SubtreeExecutions(context.Context, identity.ThreadID) ([]ThreadExecutionTarget, error)
	StopSubtree(context.Context, StopSubtreeInput) (StopSubtreeResult, error)
}

type ThreadExecutionTarget struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	TurnID   identity.TurnID   `json:"turn_id"`
	RunID    identity.RunID    `json:"run_id"`
}

type StopSubtreeInput struct {
	ThreadID   identity.ThreadID       `json:"thread_id"`
	Targets    []ThreadExecutionTarget `json:"targets"`
	RequestKey RequestKey              `json:"request_key"`
}

// Admission is not settlement. A failed call retains the confirmed prefix;
// callers must refresh the authoritative state before further decisions.
type StopSubtreeResult struct {
	Admitted     []ThreadExecutionTarget `json:"admitted"`
	AlreadyEnded []ThreadExecutionTarget `json:"already_ended"`
}

func (s *threadRuntimeService) SubtreeExecutions(ctx context.Context, root identity.ThreadID) ([]ThreadExecutionTarget, error) {
	if err := s.host.available(); err != nil {
		return nil, err
	}
	ids, err := s.subtreeThreadIDs(ctx, root)
	if err != nil {
		return nil, err
	}
	out := []ThreadExecutionTarget{}
	for _, id := range ids {
		v, err := s.Summary(ctx, id)
		if err != nil {
			return nil, err
		}
		if v.Activity == ThreadActivityActive {
			out = append(out, ThreadExecutionTarget{id, v.TurnID, v.RunID})
		}
	}
	return out, nil
}

func (s *threadRuntimeService) StopSubtree(ctx context.Context, in StopSubtreeInput) (out StopSubtreeResult, err error) {
	if err := s.host.requireExecution(); err != nil {
		return out, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return out, err
	}
	if len(in.Targets) == 0 {
		return out, errors.New("subtree stop requires exact execution targets")
	}
	targets := make(map[identity.ThreadID]ThreadExecutionTarget, len(in.Targets))
	for _, target := range in.Targets {
		if target.ThreadID == "" || target.TurnID == "" || target.RunID == "" || targets[target.ThreadID].ThreadID != "" {
			return out, errors.New("invalid subtree execution target")
		}
		targets[target.ThreadID] = target
	}
	s.host.mutationMu.Lock()
	defer s.host.mutationMu.Unlock()
	ids, err := s.subtreeThreadIDs(ctx, in.ThreadID)
	if err != nil {
		return out, err
	}
	// Reject foreign targets and incomplete or replaced snapshots before any
	// admission. Completion of the original execution is harmless.
	active := make([]ThreadExecutionTarget, 0, len(targets))
	for _, id := range ids {
		target, selected := targets[id]
		v, err := s.Summary(ctx, id)
		if err != nil {
			return out, err
		}
		if v.Activity == ThreadActivityActive && !selected {
			return out, ErrRequestConflict
		}
		if !selected {
			continue
		}
		delete(targets, id)
		if target.TurnID != v.TurnID || target.RunID != v.RunID {
			return out, ErrRequestConflict
		}
		if v.Activity == ThreadActivityActive {
			active = append(active, target)
		} else {
			out.AlreadyEnded = append(out.AlreadyEnded, target)
		}
	}
	if len(targets) > 0 {
		return StopSubtreeResult{}, ErrRequestConflict
	}
	// subtreeThreadIDs visits every parent before its dependencies.
	for _, target := range active {
		childKey, err := stableFingerprint([]string{"subtree-stop", in.ThreadID.String(), key, target.ThreadID.String()})
		if err != nil {
			return out, err
		}
		v, _, err := s.cancelResult(ctx, CancelInput{ThreadID: target.ThreadID, ExpectedTurnID: target.TurnID, ExpectedRunID: target.RunID, Mode: CancelModeGraceful}, childKey, threadAckResult)
		if err != nil {
			return out, err
		}
		if v.Cancellation != nil && v.Cancellation.TurnID == target.TurnID && v.Cancellation.RunID == target.RunID {
			out.Admitted = append(out.Admitted, target)
		} else if v.Activity == ThreadActivityIdle && v.TurnID == target.TurnID && v.RunID == target.RunID {
			out.AlreadyEnded = append(out.AlreadyEnded, target)
		} else {
			return out, ErrRequestConflict
		}
	}
	return out, nil
}
