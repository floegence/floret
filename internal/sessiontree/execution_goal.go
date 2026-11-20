package sessiontree

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type ExecutionGoalFacts struct {
	State         session.GoalState
	ReviewPrompts int
	ValidReview   bool
	ReviewEntry   Entry
	Review        GoalReview
	ModelDeleted  bool
}

func ReadExecutionGoal(ctx context.Context, repo JournalRepo, thread, turn, run string) (ExecutionGoalFacts, error) {
	if reader, ok := repo.(interface {
		readExecutionGoal(context.Context, string, string, string) (ExecutionGoalFacts, error)
	}); ok {
		return reader.readExecutionGoal(ctx, thread, turn, run)
	}
	meta, err := repo.Thread(ctx, thread)
	if err != nil {
		return ExecutionGoalFacts{}, err
	}
	path, err := repo.Path(ctx, thread, meta.LeafID)
	if err != nil {
		return ExecutionGoalFacts{}, err
	}
	state, err := GoalFromPath(path)
	facts := ExecutionGoalFacts{State: state}
	if err != nil || turn == "" {
		return facts, err
	}
	for _, entry := range path {
		if entry.TurnID == turn && entry.Message.Kind == session.MessageKindGoalReview {
			facts.ReviewPrompts++
		}
		if entry.Type == EntryGoal {
			facts.ModelDeleted = entry.TurnID == turn && state.Status == "absent" && state.Actor == "model"
		}
	}
	facts.ReviewEntry, facts.Review, facts.ValidReview = CurrentGoalReview(path, turn, run, state)
	return facts, nil
}

func (repo *BackendRepo) readExecutionGoal(ctx context.Context, thread, turn, run string) (ExecutionGoalFacts, error) {
	if err := repo.prepareProjectionSources(ctx, thread); err != nil {
		return ExecutionGoalFacts{}, err
	}
	var facts ExecutionGoalFacts
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		return readExecutionGoalFacts(ctx, tx, meta, turn, run, &facts)
	}, func(memory *MemoryRepo) error {
		var err error
		facts, err = ReadExecutionGoal(ctx, memory, thread, turn, run)
		return err
	})
	if err != nil {
		return ExecutionGoalFacts{}, err
	}
	return facts, nil
}

func readExecutionGoalFacts(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, turn, run string, facts *ExecutionGoalFacts) error {
	thread := meta.ID
	if meta.LeafID == "" {
		facts.State.Status = "absent"
		return nil
	}
	leaf, err := readEntryLocator(tx, thread, meta.LeafID)
	if err != nil {
		return err
	}
	refs, err := readGoalSourceContext(tx, thread, leaf.ID)
	if err != nil {
		return err
	}
	facts.State, err = goalSourceState(ctx, tx, thread, leaf, refs)
	if err != nil || turn == "" {
		return err
	}
	var lastReview Entry
	checkDeletion := false
	err = visitCanonicalTurnEntries(ctx, tx, thread, turn, func(entry Entry) error {
		if entry.Type != EntryGoal && entry.Type != EntryGoalReview && entry.Message.Kind != session.MessageKindGoalReview {
			return nil
		}
		if entry.PathDepth > leaf.Depth {
			return nil
		}
		ancestor, err := entryAncestor(ctx, tx, thread, leaf, entry.PathDepth)
		if err != nil || ancestor.ID != entry.ID {
			return err
		}
		if entry.Message.Kind == session.MessageKindGoalReview {
			facts.ReviewPrompts++
		}
		if entry.Type == EntryGoalReview && entry.RunID == run {
			lastReview = entry
		}
		if entry.Type == EntryGoal && facts.State.Status == "absent" && facts.State.Actor == "model" {
			checkDeletion = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	var review GoalReview
	if lastReview.ID != "" && json.Unmarshal(lastReview.Payload, &review) != nil {
		return ErrAuthorityCorrupt
	}
	checkBoundary := lastReview.ID != "" && review.Version == facts.State.Version && (review.Outcome == "completed" || review.Outcome == "blocked")
	if !checkBoundary && !checkDeletion {
		return nil
	}
	var boundary string
	found := errors.New("goal sources resolved")
	_, err = visitCanonicalPage(ctx, tx, meta, leaf.ID, "", int(^uint(0)>>1), func(entry Entry) error {
		if checkDeletion && entry.Type == EntryGoal {
			facts.ModelDeleted = entry.TurnID == turn
			checkDeletion = false
		}
		if checkBoundary && boundary == "" {
			boundary = GoalWorkBoundary([]Entry{entry})
		}
		if !checkDeletion && (!checkBoundary || boundary != "") {
			return found
		}
		return nil
	})
	if err != nil && err != found {
		return err
	}
	facts.ValidReview = checkBoundary && review.Boundary == boundary
	facts.ReviewEntry, facts.Review = lastReview, review
	return nil
}
