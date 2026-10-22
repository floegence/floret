package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type canonicalGoalContext struct {
	goal              GoalProjection
	input, boundary   string
	reviewEntry       Entry
	review            GoalReview
	pending           map[string]bool
	unknownSinceInput bool
}

func readCanonicalGoalContext(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, turnID, runID string) (canonicalGoalContext, error) {
	facts := canonicalGoalContext{goal: GoalProjection{State: session.GoalState{Status: "absent"}}, pending: map[string]bool{}}
	err := visitCanonicalPathMetadata(ctx, tx, meta.ID, meta.LeafID, func(entry Entry) error {
		if err := facts.goal.Observe(entry); err != nil {
			return err
		}
		if isCanonicalUserEntry(entry) {
			facts.unknownSinceInput = false
		}
		if entry.Metadata[TurnFailureCodeMetadataKey] == TurnFailureEffectOutcomeUnknown {
			facts.unknownSinceInput = true
		}
		if entry.Type == EntryUserMessage && entry.Message.Kind == session.MessageKindNormal && entry.Metadata[SubAgentUserMessageOriginMetadataKey] != SubAgentUserMessageOriginPendingToolCompletion {
			facts.input = entry.ID
		}
		if boundary := GoalWorkBoundary([]Entry{entry}); boundary != "" {
			facts.boundary = boundary
		}
		if entry.TurnID != turnID || entry.RunID != runID {
			return nil
		}
		if entry.Type == EntryGoalReview {
			if json.Unmarshal(entry.Payload, &facts.review) != nil {
				return ErrAuthorityCorrupt
			}
			facts.reviewEntry = entry
		}
		if entry.Message.ToolName != "goal" {
			if entry.Type == EntryToolCall {
				facts.pending[entry.Message.ToolCallID] = true
			}
			if entry.Type == EntryToolResult {
				delete(facts.pending, entry.Message.ToolCallID)
			}
		}
		return nil
	})
	return facts, err
}

func (facts canonicalGoalContext) validReview() bool {
	return facts.reviewEntry.ID != "" && facts.review.Version == facts.goal.State.Version && facts.review.Boundary == facts.boundary && (facts.review.Outcome == "completed" || facts.review.Outcome == "blocked")
}

func (repo *BackendRepo) MutateGoal(ctx context.Context, in GoalMutation) (GoalMutationResult, error) {
	var result GoalMutationResult
	err := repo.canonicalJournalUpdate(ctx, in.ThreadID, func(mutation *canonicalJournalMutation) error {
		return mutation.mutateGoal(in, &result)
	}, func(memory *MemoryRepo) error { var err error; result, err = memory.MutateGoal(ctx, in); return err })
	return result, err
}

func (repo *BackendRepo) RecordGoalReview(ctx context.Context, threadID, turnID, runID, callID string, version int64, outcome, evidence string) (Entry, error) {
	if callID == "" || version < 0 || outcome != "completed" && outcome != "continue" && outcome != "blocked" || strings.TrimSpace(evidence) == "" {
		return Entry{}, errors.New("goal review requires an outcome and evidence")
	}
	var result Entry
	err := repo.canonicalJournalUpdate(ctx, threadID, func(mutation *canonicalJournalMutation) error {
		id := "goal-review:" + StableHash(threadID + "\x00" + runID + "\x00" + callID)[:32]
		existing, err := readCanonicalEntryMetadata(mutation.tx, threadID, id)
		if err == nil {
			var review GoalReview
			if existing.Type != EntryGoalReview || existing.TurnID != turnID || existing.RunID != runID || json.Unmarshal(existing.Payload, &review) != nil || review.Version != version || review.Outcome != outcome || review.Evidence != evidence {
				return ErrRequestConflict
			}
			result = existing
			return nil
		}
		if !errors.Is(err, ErrEntryNotFound) {
			return err
		}
		facts, err := readCanonicalGoalContext(ctx, mutation.tx, mutation.meta, turnID, runID)
		if err != nil {
			return err
		}
		cancelled, err := canonicalTurnAuthority(mutation.tx, threadID, turnID, runID)
		if err != nil {
			return err
		}
		if cancelled {
			return ErrStaleAuthority
		}
		if facts.goal.State.Version != version {
			return ErrRequestConflict
		}
		if outcome == "completed" && facts.goal.State.Status == "absent" {
			return errors.New("a deleted goal cannot be marked completed")
		}
		if len(facts.pending) != 0 {
			return errors.New("finish pending work tools before reviewing the goal")
		}
		payload, _ := json.Marshal(GoalReview{Version: version, Outcome: outcome, Evidence: evidence, Boundary: facts.boundary})
		result, err = mutation.append(Entry{ID: id, ThreadID: threadID, TurnID: turnID, RunID: runID, Type: EntryGoalReview, Payload: payload}, AppendOptions{})
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.RecordGoalReview(ctx, threadID, turnID, runID, callID, version, outcome, evidence)
		return err
	})
	if err != nil {
		return Entry{}, err
	}
	return result, nil
}

func (mutation *canonicalJournalMutation) mutateGoal(in GoalMutation, result *GoalMutationResult) error {
	if in.RequestKey == "" || in.Actor != "user" && in.Actor != "model" || in.Operation != "set" && in.Operation != "delete" || in.ExpectedVersion < 0 || !utf8.ValidString(in.Text) || in.Operation == "set" && strings.TrimSpace(in.Text) == "" {
		return errors.New("invalid goal mutation")
	}
	ctx := mutation.ctx
	facts, err := readCanonicalGoalContext(ctx, mutation.tx, mutation.meta, in.TurnID, in.RunID)
	if err != nil {
		return err
	}
	state := facts.goal.State
	raw, _ := json.Marshal(in)
	fingerprint := StableHash(string(raw))
	id := "goal:" + StableHash(in.ThreadID + "\x00" + in.RequestKey)[:32]
	existing, err := readCanonicalEntryMetadata(mutation.tx, in.ThreadID, id)
	if err == nil {
		if existing.Type != EntryGoal || existing.RequestFingerprint != fingerprint {
			return ErrRequestConflict
		}
		*result = GoalMutationResult{State: state, Entry: existing, Replayed: true}
		return nil
	}
	if !errors.Is(err, ErrEntryNotFound) {
		return err
	}
	if state.Version != in.ExpectedVersion {
		*result = GoalMutationResult{State: state, Conflict: true}
		return nil
	}
	if in.Actor == "model" {
		cancelled, err := canonicalTurnAuthority(mutation.tx, in.ThreadID, in.TurnID, in.RunID)
		if err != nil {
			return err
		}
		if cancelled {
			return ErrStaleAuthority
		}
		if in.Operation == "set" && state.SuppressedAtInput != "" && (!in.NewTask || facts.input == state.SuppressedAtInput) {
			result.State = state
			return errors.New("user deleted this goal; do not recreate it for the same task")
		}
	}
	next := session.GoalState{Version: state.Version + 1, Text: in.Text, Status: "active", Actor: in.Actor}
	if in.Operation == "delete" {
		next.Text, next.Status, next.SuppressedAtInput = "", "absent", state.SuppressedAtInput
		if in.Actor == "user" {
			next.SuppressedAtInput = facts.input
			if next.SuppressedAtInput == "" {
				next.SuppressedAtInput = "before-first-input"
			}
		}
	}
	payload, _ := json.Marshal(GoalChange{State: next, Operation: in.Operation, Reason: in.Reason})
	entry, err := mutation.append(Entry{ID: id, ThreadID: in.ThreadID, TurnID: in.TurnID, RunID: in.RunID, Type: EntryGoal, RequestKey: in.RequestKey, RequestFingerprint: fingerprint, Payload: payload}, AppendOptions{})
	if err != nil {
		return err
	}
	*result = GoalMutationResult{State: next, Entry: entry}
	return nil
}
