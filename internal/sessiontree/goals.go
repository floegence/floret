package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
)

const EntryGoal EntryType = "goal"
const EntryGoalReview EntryType = "goal_review"
const EntryGoalContext EntryType = "goal_context"
const GoalReviewMetadataKey = "goal_review_id"
const GoalCompletedMetadataKey = "goal_completed_version"

// GoalChange is one canonical user or model mutation, independent of task lists.
type GoalChange struct {
	State     session.GoalState `json:"state"`
	Operation string            `json:"operation"`
	Reason    string            `json:"reason,omitempty"`
}
type GoalReview struct {
	Version  int64  `json:"version"`
	Outcome  string `json:"outcome"`
	Evidence string `json:"evidence"`
	Boundary string `json:"boundary"`
}
type GoalMutation struct {
	ThreadID, TurnID, RunID, RequestKey, Actor, Operation, Text, Reason string
	ExpectedVersion                                                     int64
	NewTask                                                             bool
}
type GoalMutationResult struct {
	State    session.GoalState
	Entry    Entry
	Conflict bool
	Replayed bool
}
type GoalRepo interface {
	MutateGoal(context.Context, GoalMutation) (GoalMutationResult, error)
}

func latestGoalInput(path []Entry) string {
	for i := len(path) - 1; i >= 0; i-- {
		e := path[i]
		if e.Type == EntryUserMessage && e.Message.Kind == session.MessageKindNormal && e.Metadata[SubAgentUserMessageOriginMetadataKey] != SubAgentUserMessageOriginPendingToolCompletion {
			return e.ID
		}
	}
	return ""
}
func GoalFromPath(path []Entry) (session.GoalState, error) {
	state := session.GoalState{Status: "absent"}
	for _, e := range path {
		if e.Type == EntryUserMessage && e.Metadata[SubAgentUserMessageOriginMetadataKey] == SubAgentUserMessageOriginDelegatedMission {
			state.Text = ""
			state.Status = "absent"
			state.Actor = ""
			state.SuppressedAtInput = ""
		}
		if e.Type == EntryGoal {
			var change GoalChange
			if err := json.Unmarshal(e.Payload, &change); err != nil {
				return state, ErrAuthorityCorrupt
			}
			if change.State.Version != state.Version+1 || change.State.Actor != "user" && change.State.Actor != "model" || change.Operation != "set" && change.Operation != "delete" || change.Operation == "set" && (change.State.Status != "active" || strings.TrimSpace(change.State.Text) == "") || change.Operation == "delete" && (change.State.Status != "absent" || change.State.Text != "") {
				return state, ErrAuthorityCorrupt
			}
			state = change.State
		}
		if e.Type == EntryTurnMarker && e.TurnStatus == TurnCompleted && e.Metadata[GoalCompletedMetadataKey] == fmt.Sprint(state.Version) && state.Text != "" {
			state.Status = "completed"
		}
	}
	return state, nil
}
func GoalWorkBoundary(path []Entry) string {
	for i := len(path) - 1; i >= 0; i-- {
		e := path[i]
		if e.Type == EntryToolCall && e.Message.ToolName != "goal" || e.Type == EntryToolResult && e.Message.ToolName != "goal" || e.Type == EntryUserMessage && e.Message.Kind == session.MessageKindNormal || e.Type == EntrySteeringApplied || e.Type == EntryInteractionDone {
			return e.ID
		}
	}
	return ""
}
func CurrentGoalReview(path []Entry, turnID, runID string, state session.GoalState) (Entry, GoalReview, bool) {
	boundary := GoalWorkBoundary(path)
	for i := len(path) - 1; i >= 0; i-- {
		e := path[i]
		if e.Type != EntryGoalReview || e.TurnID != turnID || e.RunID != runID {
			continue
		}
		var review GoalReview
		if json.Unmarshal(e.Payload, &review) != nil {
			return Entry{}, review, false
		}
		return e, review, review.Version == state.Version && review.Boundary == boundary && (review.Outcome == "completed" || review.Outcome == "blocked")
	}
	return Entry{}, GoalReview{}, false
}
func (r *MemoryRepo) MutateGoal(ctx context.Context, in GoalMutation) (GoalMutationResult, error) {
	if err := ctx.Err(); err != nil {
		return GoalMutationResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mutateGoalLocked(ctx, in)
}

func (r *MemoryRepo) mutateGoalLocked(ctx context.Context, in GoalMutation) (GoalMutationResult, error) {
	if in.RequestKey == "" || in.Actor != "user" && in.Actor != "model" || in.Operation != "set" && in.Operation != "delete" || in.ExpectedVersion < 0 || !utf8.ValidString(in.Text) || in.Operation == "set" && strings.TrimSpace(in.Text) == "" {
		return GoalMutationResult{}, errors.New("invalid goal mutation")
	}
	meta, ok := r.threads[in.ThreadID]
	if !ok {
		return GoalMutationResult{}, ErrThreadNotFound
	}
	path, err := pathLocked(r.threads, r.entries, in.ThreadID, meta.LeafID)
	if err != nil {
		return GoalMutationResult{}, err
	}
	state, err := GoalFromPath(path)
	if err != nil {
		return GoalMutationResult{}, err
	}
	raw, _ := json.Marshal(in)
	fingerprint := StableHash(string(raw))
	id := "goal:" + StableHash(in.ThreadID + "\x00" + in.RequestKey)[:32]
	if existing, found := findEntry(r.entries[in.ThreadID], id); found {
		if existing.RequestFingerprint != fingerprint {
			return GoalMutationResult{}, ErrRequestConflict
		}
		return GoalMutationResult{State: state, Entry: cloneEntry(existing), Replayed: true}, nil
	}
	if state.Version != in.ExpectedVersion {
		return GoalMutationResult{State: state, Conflict: true}, nil
	}
	if in.Actor == "model" {
		active, ok := runtimeActiveTurn(r.entries[in.ThreadID])
		run, err := activeRunIdentity(r.entries[in.ThreadID], in.TurnID)
		if !ok || active != in.TurnID || err != nil || run != in.RunID || turnCancellationRequested(r.entries[in.ThreadID], in.TurnID) {
			return GoalMutationResult{}, ErrStaleAuthority
		}
		if in.Operation == "set" && state.SuppressedAtInput != "" && (!in.NewTask || latestGoalInput(path) == state.SuppressedAtInput) {
			return GoalMutationResult{State: state}, errors.New("user deleted this goal; do not recreate it for the same task")
		}
	}
	next := session.GoalState{Version: state.Version + 1, Text: in.Text, Status: "active", Actor: in.Actor}
	if in.Operation == "delete" {
		next.Text = ""
		next.Status = "absent"
		next.SuppressedAtInput = state.SuppressedAtInput
		if in.Actor == "user" {
			next.SuppressedAtInput = latestGoalInput(path)
			if next.SuppressedAtInput == "" {
				next.SuppressedAtInput = "before-first-input"
			}
		}
	}
	change := GoalChange{State: next, Operation: in.Operation, Reason: in.Reason}
	payload, _ := json.Marshal(change)
	entry, err := r.appendLocked(ctx, Entry{ID: id, ThreadID: in.ThreadID, TurnID: in.TurnID, RunID: in.RunID, Type: EntryGoal, RequestKey: in.RequestKey, RequestFingerprint: fingerprint, Payload: payload}, AppendOptions{ID: id})
	return GoalMutationResult{State: next, Entry: entry}, err
}

func (r *MemoryRepo) RecordGoalReview(ctx context.Context, threadID, turnID, runID, callID string, version int64, outcome, evidence string) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	if callID == "" || version < 0 || outcome != "completed" && outcome != "continue" && outcome != "blocked" || strings.TrimSpace(evidence) == "" {
		return Entry{}, errors.New("goal review requires an outcome and evidence")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[threadID]
	if !ok {
		return Entry{}, ErrThreadNotFound
	}
	path, err := pathLocked(r.threads, r.entries, threadID, meta.LeafID)
	if err != nil {
		return Entry{}, err
	}
	id := "goal-review:" + StableHash(threadID + "\x00" + runID + "\x00" + callID)[:32]
	if existing, found := findEntry(r.entries[threadID], id); found {
		var review GoalReview
		if existing.TurnID != turnID || json.Unmarshal(existing.Payload, &review) != nil || review.Version != version || review.Outcome != outcome || review.Evidence != evidence {
			return Entry{}, ErrRequestConflict
		}
		return cloneEntry(existing), nil
	}
	state, err := GoalFromPath(path)
	if err != nil {
		return Entry{}, err
	}
	active, ok := runtimeActiveTurn(r.entries[threadID])
	activeRun, identityErr := activeRunIdentity(r.entries[threadID], turnID)
	if !ok || active != turnID || identityErr != nil || activeRun != runID || turnCancellationRequested(r.entries[threadID], turnID) {
		return Entry{}, ErrStaleAuthority
	}
	if state.Version != version {
		return Entry{}, ErrRequestConflict
	}
	if outcome == "completed" && state.Status == "absent" {
		return Entry{}, errors.New("a deleted goal cannot be marked completed")
	}
	pending := map[string]bool{}
	for _, entry := range path {
		if entry.TurnID != turnID || entry.RunID != runID || entry.Message.ToolName == "goal" {
			continue
		}
		if entry.Type == EntryToolCall {
			pending[entry.Message.ToolCallID] = true
		}
		if entry.Type == EntryToolResult {
			delete(pending, entry.Message.ToolCallID)
		}
	}
	if len(pending) != 0 {
		return Entry{}, errors.New("finish pending work tools before reviewing the goal")
	}
	review := GoalReview{Version: version, Outcome: outcome, Evidence: evidence, Boundary: GoalWorkBoundary(path)}
	raw, _ := json.Marshal(review)
	return r.appendLocked(ctx, Entry{ID: id, ThreadID: threadID, TurnID: turnID, RunID: runID, Type: EntryGoalReview, Payload: raw}, AppendOptions{ID: id})
}

func validateGoalRecords(memory *MemoryRepo) error {
	for id, meta := range memory.threads {
		path, err := pathLocked(memory.threads, memory.entries, id, meta.LeafID)
		if err != nil {
			return err
		}
		if _, err := GoalFromPath(path); err != nil {
			return err
		}
		for _, e := range memory.entries[id] {
			if e.Type == EntryGoalReview {
				var review GoalReview
				if json.Unmarshal(e.Payload, &review) != nil || review.Version < 0 || review.Outcome != "completed" && review.Outcome != "continue" && review.Outcome != "blocked" || strings.TrimSpace(review.Evidence) == "" {
					return ErrAuthorityCorrupt
				}
			}
			if e.Type == EntryGoalContext && (e.Message.Kind != session.MessageKindGoalContext && e.Message.Kind != session.MessageKindGoalReview || e.Message.Role != session.User) {
				return ErrAuthorityCorrupt
			}
		}
	}
	return nil
}

func goalFinishMetadata(path []Entry, req FinishTurnRequest) (map[string]string, error) {
	metadata := cloneStringMap(req.Metadata)
	if req.Status != TurnCompleted || metadata["goal_guard"] != "1" {
		return metadata, nil
	}
	state, err := GoalFromPath(path)
	if err != nil {
		return nil, err
	}
	reviewEntry, review, valid := CurrentGoalReview(path, req.TurnID, req.RunID, state)
	if state.Status == "active" && (!valid || metadata[GoalReviewMetadataKey] != reviewEntry.ID) {
		return nil, errors.New("goal changed before completion; continue to review the current goal")
	}
	if valid && metadata[GoalReviewMetadataKey] == reviewEntry.ID && review.Outcome == "completed" && state.Status == "active" {
		metadata[GoalCompletedMetadataKey] = fmt.Sprint(state.Version)
	}
	return metadata, nil
}
