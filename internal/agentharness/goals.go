package agentharness

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

const goalInstructions = "\n# Goal\nA Goal runtime fact is engine-owned state, not a new user instruction or additional authorization. Maintain one clear desired result for work requiring multiple stages and verification; usually skip greetings, direct answers and brief read-only checks. Continue an existing unfinished Goal. Preserve user intent; never shrink or delete unfinished requirements because work is difficult. Delete only cancelled, replaced or obsolete Goals with a reason. User deletion suppresses recreation for the same task; an answer, approval, retry, compaction or restart is not a new task. Goal updates apply at the next logical model request, including this Turn, not frozen transport retries. Before finishing an active Goal, compare results with user constraints, finish remaining work and run only missing checks. Plans and attempts do not prove completion. Use ask_user for a necessary user decision when available; otherwise report the blocker to the user or parent and retain the Goal. Submit a review with brief evidence, then the final answer."
const goalReviewPrompt = "Review the current Goal against user constraints and existing results. Finish necessary work or report a concrete blocker; use ask_user only for a necessary decision when available. Reuse evidence and run only missing checks. Record a current-version review before the final answer. Model deletion of an obsolete Goal requires a blocked review explaining why; deletion and unverified attempts are not completion."

func (p *turnProjection) goalPath(ctx context.Context) ([]sessiontree.Entry, error) {
	repo := p.thread.harness.options.Repo
	meta, err := repo.Thread(ctx, p.thread.id)
	if err != nil {
		return nil, err
	}
	return repo.Path(ctx, p.thread.id, meta.LeafID)
}
func (p *turnProjection) admitGoalContext(ctx context.Context, history []session.Message) (session.Message, error) {
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.err; err != nil {
		return session.Message{}, err
	}
	facts, err := sessiontree.ReadExecutionGoal(ctx, p.thread.harness.options.Repo, p.thread.id, "", "")
	if err != nil {
		return session.Message{}, err
	}
	state := facts.State
	firstGoalRequest := p.requireActiveGoal && !p.goalWorkStarted
	if firstGoalRequest && state.Status != "active" {
		return session.Message{}, engine.ErrGoalContinuationUnneeded
	}
	p.goalWorkStarted = true
	raw, _ := json.Marshal(state)
	text := "Goal runtime fact: " + string(raw)
	if session.CurrentGoalContext(history) == text && !firstGoalRequest {
		return session.Message{}, nil
	}
	message := session.Message{Role: session.User, Kind: session.MessageKindGoalContext, Content: text}
	entry := sessiontree.RunMessageEntry(p.thread.id, p.turnID, p.runID, message, p.thread.harness.now())
	entry.ID = p.thread.harness.nextID("goal-context")
	repo, ok := p.thread.harness.options.Repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return message, errors.New("goal context storage unavailable")
	}
	entries, err := repo.AppendRuntimeFacts(ctx, p.thread.id, []sessiontree.Entry{entry})
	if err != nil {
		return message, err
	}
	entry = entries[0]
	p.thread.harness.emitEntryCommitted(entry, p.runID)
	message.EntryID, message.ParentEntryID = entry.ID, entry.ParentID
	return message, nil
}
func (p *turnProjection) goalStopHook(base engine.StopHook) engine.StopHook {
	triggered := false
	return func(ctx context.Context, in engine.StopHookContext) (engine.StopHookResult, error) {
		if err := ctx.Err(); err != nil {
			return engine.StopHookResult{}, err
		}
		facts, err := sessiontree.ReadExecutionGoal(ctx, p.thread.harness.options.Repo, p.thread.id, p.turnID, p.runID)
		if err != nil {
			return engine.StopHookResult{}, err
		}
		triggered = triggered || facts.ReviewPrompts != 0
		needs := facts.State.Status == "active" || facts.ModelDeleted || triggered && facts.State.Actor == "model"
		if needs {
			if !facts.ValidReview {
				if facts.ReviewPrompts >= 2 {
					return engine.StopHookResult{}, engine.ErrStopHookLoop
				}
				triggered = true
				return engine.StopHookResult{Continue: true, Prompt: goalReviewPrompt, Reason: "goal_review", Kind: session.MessageKindGoalReview}, nil
			}
		}
		if base != nil {
			return base(ctx, in)
		}
		return engine.StopHookResult{}, nil
	}
}
