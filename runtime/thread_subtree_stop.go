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
	// Suppressed means a source can no longer automatically promote a queued
	// input or start a host-work continuation.
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

type subtreeStopThread struct {
	ThreadSummary
	actor *threadRuntimeState
	auto  *threadQueuePromotion
	work  map[ThreadExecutionTarget]bool
}

// Walk in admission order. A running turn owns any host work bound to that
// execution; an older turn can independently retain continuation authority.
func (s *threadRuntimeService) subtreeStopInventory(ctx context.Context, root identity.ThreadID) (ordered []ThreadExecutionTarget, latest map[identity.ThreadID]subtreeStopThread, err error) {
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
	latest = make(map[identity.ThreadID]subtreeStopThread, len(ids))
	for _, id := range ids {
		v, err := s.Summary(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		state := subtreeStopThread{ThreadSummary: v, work: map[ThreadExecutionTarget]bool{}}
		s.runtimesMu.Lock()
		state.actor = s.runtimes[id.String()]
		s.runtimesMu.Unlock()
		if state.actor != nil {
			if err := state.actor.apply(ctx, func() error { state.auto = state.actor.state.autoPromotion; return nil }); err != nil {
				return nil, nil, err
			}
		}
		active := ThreadExecutionTarget{id, v.TurnID, v.RunID}
		if v.Activity == ThreadActivityActive || state.auto != nil && state.auto.source == active {
			ordered = append(ordered, active)
		}
		sources, err := repo.HostWorkStopSources(ctx, id.String())
		if err != nil {
			return nil, nil, runtimeHostError(err)
		}
		for _, source := range sources {
			target := ThreadExecutionTarget{id, identity.TurnID(source.TurnID), identity.RunID(source.RunID)}
			state.work[target] = true
			if target != active || v.Activity != ThreadActivityActive && (state.auto == nil || state.auto.source != target) {
				ordered = append(ordered, target)
			}
		}
		latest[id] = state
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
		if !ok {
			return StopSubtreeResult{}, ErrRequestConflict
		}
		if target.TurnID != v.TurnID || target.RunID != v.RunID || v.Activity != ThreadActivityIdle {
			// Older work can remain bound across later turns. Trust its existing
			// cancellation fact only for the exact snapshot that wrote it.
			entry, readErr := s.host.store.repo.Entry(ctx, target.ThreadID.String(), sessiontree.HostWorkStopEntryID(target.TurnID.String()))
			if readErr != nil && !errors.Is(readErr, sessiontree.ErrEntryNotFound) {
				return StopSubtreeResult{}, runtimeHostError(readErr)
			}
			if readErr != nil || entry.Type != sessiontree.EntryCancelRequested || entry.TurnID != target.TurnID.String() || entry.RequestFingerprint != string(key) {
				return StopSubtreeResult{}, ErrRequestConflict
			}
		}
		out.AlreadyEnded = append(out.AlreadyEnded, target)
	}
	// subtreeThreadIDs visits every parent before its dependencies.
	type sourceTurn struct {
		thread identity.ThreadID
		turn   identity.TurnID
	}
	groups := make(map[sourceTurn][]ThreadExecutionTarget)
	for _, target := range current {
		group := sourceTurn{target.ThreadID, target.TurnID}
		groups[group] = append(groups[group], target)
	}
	for _, target := range current {
		group := sourceTurn{target.ThreadID, target.TurnID}
		sources := groups[group]
		if len(sources) == 0 {
			continue
		}
		delete(groups, group)
		before := latest[target.ThreadID]
		if before.Activity != ThreadActivityActive || before.TurnID != target.TurnID || before.RunID != target.RunID {
			var runs []string
			for _, source := range sources {
				if before.work[source] {
					runs = append(runs, source.RunID.String())
				}
			}
			stopWork := func() error {
				if len(runs) == 0 {
					return nil
				}
				repo := s.host.store.repo.(sessiontree.HostWorkRepo)
				stop := sessiontree.HostWorkStop{TurnID: target.TurnID.String(), Runs: runs, SnapshotKey: string(key)}
				if before.Activity == ThreadActivityActive && slices.Contains(out.Admitted, ThreadExecutionTarget{target.ThreadID, before.TurnID, before.RunID}) {
					stop.AdmittedTurnID = before.TurnID.String()
				}
				return runtimeHostError(repo.StopHostWork(ctx, target.ThreadID.String(), stop))
			}
			if before.auto != nil && slices.Contains(sources, before.auto.source) {
				err = before.actor.apply(ctx, func() error {
					if before.actor.state.autoPromotion != before.auto {
						return ErrRequestConflict
					}
					// A failed journal write must leave the in-memory grant intact.
					if err := stopWork(); err != nil {
						return err
					}
					before.actor.state.autoPromotion = nil
					return nil
				})
			} else {
				err = stopWork()
			}
			if err != nil {
				return out, err
			}
			out.Suppressed = append(out.Suppressed, sources...)
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
			out.Suppressed = append(out.Suppressed, sources[1:]...)
		} else if v.Activity == ThreadActivityIdle && v.TurnID == target.TurnID && v.RunID == target.RunID {
			out.AlreadyEnded = append(out.AlreadyEnded, target)
		} else {
			return out, ErrRequestConflict
		}
	}
	// A Send already in dispatch or a natural completion can win while the
	// exact requests are admitted. Do not acknowledge that new authority or
	// silently substitute it for the frozen targets.
	remaining, states, err := s.subtreeStopInventory(ctx, in.ThreadID)
	if err != nil {
		return out, err
	}
	for _, target := range remaining {
		c := states[target.ThreadID].Cancellation
		if c == nil || c.TurnID != target.TurnID || c.RunID != target.RunID || !slices.Contains(out.Admitted, target) {
			return out, ErrRequestConflict
		}
	}
	return out, nil
}
