package runtime

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadSubtreeStopper observes and stops concrete active executions in an
// authorized ownership subtree, even when the selected thread itself is idle.
type ThreadSubtreeStopper interface {
	SubtreeExecutions(context.Context, identity.ThreadID) (StopSubtreeInput, error)
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
	// Suppressed means a source can no longer start a host-work continuation.
	// It does not assert that the external work stopped.
	Suppressed []ThreadExecutionTarget `json:"suppressed"`
}

// The observation supplies the idempotency key. No server-side lease or
// receipt is needed to reject a retry carrying a different set after restart.
func subtreeStopKey(root identity.ThreadID, targets []ThreadExecutionTarget) (RequestKey, error) {
	ordered := slices.Clone(targets)
	slices.SortFunc(ordered, func(a, b ThreadExecutionTarget) int {
		return cmp.Or(cmp.Compare(a.ThreadID, b.ThreadID), cmp.Compare(a.TurnID, b.TurnID), cmp.Compare(a.RunID, b.RunID))
	})
	key, err := stableFingerprint(struct {
		Root    identity.ThreadID
		Targets []ThreadExecutionTarget
	}{root, ordered})
	return RequestKey(key), err
}

// Walk in admission order. A running turn owns any host work bound to that
// execution; an older turn can independently retain continuation authority.
func (s *threadRuntimeService) subtreeStopInventory(ctx context.Context, root identity.ThreadID) (ordered []ThreadExecutionTarget, latest map[identity.ThreadID]ThreadSummary, err error) {
	if err := s.host.available(); err != nil {
		return nil, nil, err
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return nil, nil, ErrUnsupportedStoreCapability
	}
	ids, err := s.subtreeThreadIDs(ctx, root)
	if err != nil {
		return nil, nil, err
	}
	latest = make(map[identity.ThreadID]ThreadSummary, len(ids))
	for _, id := range ids {
		v, err := s.Summary(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		latest[id] = v
		active := ThreadExecutionTarget{id, v.TurnID, v.RunID}
		if v.Activity == ThreadActivityActive {
			ordered = append(ordered, active)
		}
		sources, err := repo.HostWorkStopSources(ctx, id.String())
		if err != nil {
			return nil, nil, runtimeHostError(err)
		}
		for _, source := range sources {
			target := ThreadExecutionTarget{id, identity.TurnID(source.TurnID), identity.RunID(source.RunID)}
			if v.Activity != ThreadActivityActive || target != active {
				ordered = append(ordered, target)
			}
		}
	}
	return ordered, latest, nil
}

func (s *threadRuntimeService) SubtreeExecutions(ctx context.Context, root identity.ThreadID) (StopSubtreeInput, error) {
	out, _, err := s.subtreeStopInventory(ctx, root)
	if err != nil {
		return StopSubtreeInput{}, err
	}
	key, err := subtreeStopKey(root, out)
	return StopSubtreeInput{ThreadID: root, Targets: out, RequestKey: key}, err
}

func (s *threadRuntimeService) StopSubtree(ctx context.Context, in StopSubtreeInput) (out StopSubtreeResult, err error) {
	if err := s.host.requireExecution(); err != nil {
		return out, err
	}
	if len(in.Targets) == 0 {
		return out, errors.New("subtree stop requires exact execution targets")
	}
	targets := make(map[ThreadExecutionTarget]bool, len(in.Targets))
	for _, target := range in.Targets {
		if target.ThreadID == "" || target.TurnID == "" || target.RunID == "" || targets[target] {
			return out, errors.New("invalid subtree execution target")
		}
		targets[target] = true
	}
	key, err := subtreeStopKey(in.ThreadID, in.Targets)
	if err != nil {
		return out, err
	}
	if key != in.RequestKey {
		return out, ErrRequestConflict
	}
	s.host.mutationMu.Lock()
	defer s.host.mutationMu.Unlock()
	current, latest, err := s.subtreeStopInventory(ctx, in.ThreadID)
	if err != nil {
		return out, err
	}
	// Reject foreign targets and incomplete or replaced snapshots before any
	// admission. Completion of the original execution is harmless.
	for _, target := range current {
		if !targets[target] {
			return out, ErrRequestConflict
		}
		delete(targets, target)
	}
	for _, target := range in.Targets {
		if !targets[target] {
			continue
		}
		v, ok := latest[target.ThreadID]
		if !ok || target.TurnID != v.TurnID || target.RunID != v.RunID || v.Activity != ThreadActivityIdle {
			return StopSubtreeResult{}, ErrRequestConflict
		}
		out.AlreadyEnded = append(out.AlreadyEnded, target)
	}
	// subtreeThreadIDs visits every parent before its dependencies.
	for _, target := range current {
		before := latest[target.ThreadID]
		if before.Activity != ThreadActivityActive || before.TurnID != target.TurnID || before.RunID != target.RunID {
			repo := s.host.store.repo.(sessiontree.HostWorkRepo)
			if err := repo.StopHostWork(ctx, target.ThreadID.String(), target.TurnID.String(), target.RunID.String()); err != nil {
				return out, runtimeHostError(err)
			}
			out.Suppressed = append(out.Suppressed, target)
			continue
		}
		childKey, err := stableFingerprint([]string{"subtree-stop", string(key), target.ThreadID.String()})
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
