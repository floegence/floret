package runtime

import (
	"context"
	"encoding/json"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadGoal is the current journal-derived objective, including user deletion suppression.
type ThreadGoal struct {
	Reviewing         bool   `json:"reviewing,omitempty"`
	Version           int64  `json:"version"`
	Text              string `json:"text,omitempty"`
	Status            string `json:"status"`
	Actor             string `json:"actor"`
	SuppressedAtInput string `json:"suppressed_at_input,omitempty"`
}

// ThreadGoalChange is a detached public timeline fact. User mutations may precede
// the first Turn and therefore do not invent Turn or Run identities.
type ThreadGoalChange struct {
	Goal      ThreadGoal `json:"goal"`
	Operation string     `json:"operation"`
	Reason    string     `json:"reason,omitempty"`
	Actor     string     `json:"actor"`
	At        time.Time  `json:"at"`
}

// ThreadGoalController changes an objective without starting or interrupting execution.
type ThreadGoalController interface {
	SetGoal(context.Context, SetGoalInput) (GoalMutationResult, error)
	DeleteGoal(context.Context, DeleteGoalInput) (GoalMutationResult, error)
}
type SetGoalInput struct {
	ThreadID        identity.ThreadID `json:"thread_id"`
	RequestKey      RequestKey        `json:"request_key"`
	ExpectedVersion int64             `json:"expected_version"`
	Text            string            `json:"text"`
}
type DeleteGoalInput struct {
	ThreadID        identity.ThreadID `json:"thread_id"`
	RequestKey      RequestKey        `json:"request_key"`
	ExpectedVersion int64             `json:"expected_version"`
}
type GoalMutationResult struct {
	Goal     ThreadGoal `json:"goal"`
	Conflict bool       `json:"conflict"`
	Replayed bool       `json:"replayed,omitempty"`
}

var _ ThreadGoalController = (*threadRuntimeService)(nil)

func publicThreadGoal(state session.GoalState) *ThreadGoal {
	return &ThreadGoal{Version: state.Version, Text: state.Text, Status: state.Status, Actor: state.Actor, SuppressedAtInput: state.SuppressedAtInput}
}
func goalFromEntries(entries []sessiontree.Entry) (*ThreadGoal, error) {
	state, err := sessiontree.GoalFromPath(entries)
	if err != nil {
		return nil, err
	}
	goal := publicThreadGoal(state)
	turnID, active := "", false
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryTurnMarker {
			continue
		}
		switch entry.TurnStatus {
		case sessiontree.TurnStarted:
			turnID, active = entry.TurnID, true
		case sessiontree.TurnCompleted, sessiontree.TurnFailed, sessiontree.TurnAborted:
			active = false
		}
	}
	if active && state.Status == "active" {
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			if entry.TurnID != turnID {
				continue
			}
			if entry.Type == sessiontree.EntryGoalReview {
				break
			}
			if entry.Message.Kind == session.MessageKindGoalReview {
				goal.Reviewing = true
				break
			}
		}
	}
	return goal, nil
}
func (service *threadRuntimeService) SetGoal(ctx context.Context, in SetGoalInput) (GoalMutationResult, error) {
	return service.mutateGoal(ctx, sessiontree.GoalMutation{ThreadID: in.ThreadID.String(), RequestKey: string(in.RequestKey), ExpectedVersion: in.ExpectedVersion, Actor: "user", Operation: "set", Text: in.Text})
}
func (service *threadRuntimeService) DeleteGoal(ctx context.Context, in DeleteGoalInput) (GoalMutationResult, error) {
	return service.mutateGoal(ctx, sessiontree.GoalMutation{ThreadID: in.ThreadID.String(), RequestKey: string(in.RequestKey), ExpectedVersion: in.ExpectedVersion, Actor: "user", Operation: "delete"})
}
func (service *threadRuntimeService) mutateGoal(ctx context.Context, in sessiontree.GoalMutation) (GoalMutationResult, error) {
	if err := service.host.available(); err != nil {
		return GoalMutationResult{}, err
	}
	if _, err := service.ensureThread(ctx, identity.ThreadID(in.ThreadID)); err != nil {
		return GoalMutationResult{}, err
	}
	repo, ok := service.host.store.repo.(sessiontree.GoalRepo)
	if !ok {
		return GoalMutationResult{}, ErrUnsupportedStoreCapability
	}
	actor := service.runtime(identity.ThreadID(in.ThreadID))
	var result sessiontree.GoalMutationResult
	err := actor.apply(ctx, func() error {
		var err error
		result, err = repo.MutateGoal(ctx, in)
		if err != nil {
			return runtimeHostError(err)
		}
		if result.Conflict || result.Replayed {
			return nil
		}
		// Read and install under the same actor boundary so streaming events
		// cannot turn a successful user mutation into a discarded refresh.
		canonical, err := service.loadCanonicalThreadView(ctx, identity.ThreadID(in.ThreadID))
		if err != nil {
			return err
		}
		if err := actor.installCanonicalView(canonical); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return GoalMutationResult{}, err
	}
	return GoalMutationResult{Goal: *publicThreadGoal(result.State), Conflict: result.Conflict, Replayed: result.Replayed}, nil
}
func goalItem(entry sessiontree.Entry) (ThreadItem, error) {
	var change sessiontree.GoalChange
	if err := json.Unmarshal(entry.Payload, &change); err != nil {
		return ThreadItem{}, err
	}
	goal := *publicThreadGoal(change.State)
	return ThreadItem{ID: entry.ID, TurnID: identity.TurnID(entry.TurnID), RunID: identity.RunID(entry.RunID), Kind: ThreadItemGoal, CreatedAt: entry.CreatedAt, Goal: &ThreadGoalChange{Goal: goal, Actor: goal.Actor, Operation: change.Operation, Reason: change.Reason, At: entry.CreatedAt}}, nil
}
