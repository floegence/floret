package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/goalactivity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

const goalInstructions = "\n# Goal\nMaintain one clear desired result for work requiring multiple stages and verification; usually skip greetings, direct answers and brief read-only checks. Continue an existing unfinished Goal. Preserve user intent; never shrink or delete unfinished requirements because work is difficult. Delete only cancelled, replaced or obsolete Goals with a reason. User deletion suppresses recreation for the same task; an answer, approval, retry, compaction or restart is not a new task. Goal updates apply at the next logical model request, including this Turn, not frozen transport retries. Before finishing an active Goal, compare results with user constraints, finish remaining work and run only missing checks. Plans and attempts do not prove completion. Use ask_user for a necessary user decision when available; otherwise report the blocker to the user or parent and retain the Goal. Once necessary work tools finish, submit a review with brief evidence before composing the final deliverable. After review, the final answer must contain the requested result and essential supporting sources; a Goal status confirmation alone is insufficient."
const goalReviewPrompt = "Review the current Goal against user constraints and existing results. Finish necessary work or report a concrete blocker; use ask_user only for a necessary decision when available. Reuse evidence and run only missing checks. Record a current-version review before the final answer. Any answer drafted before this review is intermediate; preserve its useful substance and essential supporting sources in the final deliverable after review. Model deletion of an obsolete Goal requires a blocked review explaining why; deletion and unverified attempts are not completion."

type goalToolInput struct {
	Operation       string `json:"operation"`
	Text            string `json:"text"`
	ExpectedVersion int64  `json:"expected_version"`
	Reason          string `json:"reason"`
	Outcome         string `json:"outcome"`
	Evidence        string `json:"evidence"`
	NewTask         bool   `json:"new_task"`
}

func goalTool(p *turnProjection) tools.Tool {
	schema := map[string]any{"type": "object", "properties": map[string]any{
		"operation":        map[string]any{"type": "string", "enum": []string{"set", "delete", "review"}},
		"expected_version": map[string]any{"type": "integer", "minimum": 0},
		"text":             map[string]any{"type": "string"}, "reason": map[string]any{"type": "string"},
		"outcome":  map[string]any{"type": "string", "enum": []string{"completed", "continue", "blocked"}},
		"evidence": map[string]any{"type": "string"}, "new_task": map[string]any{"type": "boolean"},
	}, "required": []string{"operation", "expected_version"}, "additionalProperties": false}
	return tools.Define[goalToolInput](tools.Definition{Name: "goal", Title: "Goal", Description: "Maintain the thread's desired result or review whether it is complete. Write clear, concise, understandable goal text. Preserve user intent. set cannot mark completion. delete requires a reason. review requires outcome and brief evidence. Use the latest version from runtime facts; after conflict read the returned state. Submit review separately after necessary work tools finish, before composing the final deliverable.", InputSchema: schema, ReadOnly: true, Activity: func(inv tools.Invocation[any]) (*tools.ActivityPresentation, error) {
		path, err := p.goalPath(context.Background())
		if err != nil {
			return nil, err
		}
		return goalactivity.Presentation(path, p.turnID, p.runID, inv.CallID, inv.RawArgs, "running", ""), nil
	}}, nil, nil, func(ctx context.Context, inv tools.Invocation[goalToolInput]) (output tools.Result, resultErr error) {
		defer func() {
			path, err := p.goalPath(context.Background())
			if err != nil {
				return
			}
			status, failure := "success", ""
			if resultErr != nil {
				status = "error"
				failure = resultErr.Error()
			}
			output.Activity = goalactivity.Presentation(path, p.turnID, p.runID, inv.CallID, inv.RawArgs, status, failure)
		}()
		in := inv.Args
		if in.Operation == "review" {
			repo, ok := p.thread.harness.options.Repo.(interface {
				RecordGoalReview(context.Context, string, string, string, string, int64, string, string) (sessiontree.Entry, error)
			})
			if !ok {
				return tools.Result{}, errors.New("goal review storage unavailable")
			}
			entry, err := repo.RecordGoalReview(ctx, p.thread.id, p.turnID, p.runID, inv.CallID, in.ExpectedVersion, in.Outcome, in.Evidence)
			if err != nil {
				return tools.Result{}, err
			}
			p.thread.harness.emitEntryCommitted(entry, p.runID)
			return tools.Result{Text: "Goal review recorded. Deliver the requested result and essential supporting sources in your final answer, or continue necessary work."}, nil
		}
		if in.Operation == "delete" && strings.TrimSpace(in.Reason) == "" {
			return tools.Result{}, errors.New("goal deletion requires a reason")
		}
		repo, ok := p.thread.harness.options.Repo.(sessiontree.GoalRepo)
		if !ok {
			return tools.Result{}, errors.New("goal storage unavailable")
		}
		result, err := repo.MutateGoal(ctx, sessiontree.GoalMutation{ThreadID: p.thread.id, TurnID: p.turnID, RunID: p.runID, RequestKey: "model:" + p.runID + ":" + inv.CallID, Actor: "model", Operation: in.Operation, Text: in.Text, Reason: in.Reason, ExpectedVersion: in.ExpectedVersion, NewTask: in.NewTask})
		if err != nil {
			return tools.Result{}, err
		}
		if !result.Replayed && !result.Conflict {
			p.thread.harness.emitEntryCommitted(result.Entry, p.runID)
		}
		raw, _ := json.Marshal(result.State)
		return tools.Result{Text: string(raw), Structured: map[string]any{"goal": result.State, "conflict": result.Conflict}}, nil
	})
}
func (p *turnProjection) goalSurface(base engine.ToolSurfaceProvider, registry *tools.Registry) engine.ToolSurfaceProvider {
	return func(ctx context.Context, request engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
		surface := engine.ToolSurface{}
		var err error
		if base != nil {
			surface, err = base(ctx, request)
			if err != nil {
				return surface, err
			}
		}
		source := surface.Tools
		if source == nil {
			source = registry
		}
		if source == nil {
			source = tools.NewRegistry()
		}
		surface.Tools, err = source.WithTools(goalTool(p))
		if err != nil {
			return surface, err
		}
		if surface.ToolDefinitions != nil {
			for _, def := range surface.Tools.Definitions() {
				if def.Name == "goal" {
					surface.ToolDefinitions = append(surface.ToolDefinitions, def)
				}
			}
		}
		if surface.SystemPrompt == "" {
			surface.SystemPrompt = p.thread.harness.options.SystemPrompt
		}
		surface.SystemPrompt += goalInstructions
		return surface, nil
	}
}
func (p *turnProjection) goalPath(ctx context.Context) ([]sessiontree.Entry, error) {
	journal, err := p.thread.Journal(ctx)
	return journal.Path, err
}
func (p *turnProjection) admitGoalContext(ctx context.Context, history []session.Message) (session.Message, error) {
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.err; err != nil {
		return session.Message{}, err
	}
	path, err := p.goalPath(ctx)
	if err != nil {
		return session.Message{}, err
	}
	state, err := sessiontree.GoalFromPath(path)
	if err != nil {
		return session.Message{}, err
	}
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
		path, err := p.goalPath(ctx)
		if err != nil {
			return engine.StopHookResult{}, err
		}
		state, err := sessiontree.GoalFromPath(path)
		if err != nil {
			return engine.StopHookResult{}, err
		}
		reviews := 0
		for _, entry := range path {
			if entry.TurnID == p.turnID && entry.Message.Kind == session.MessageKindGoalReview {
				triggered = true
				reviews++
			}
		}
		modelDeleted := false
		for i := len(path) - 1; i >= 0; i-- {
			if path[i].Type == sessiontree.EntryGoal {
				modelDeleted = path[i].TurnID == p.turnID && state.Status == "absent" && state.Actor == "model"
				break
			}
		}
		needs := state.Status == "active" || modelDeleted || triggered && state.Actor == "model"
		if needs {
			_, _, valid := sessiontree.CurrentGoalReview(path, p.turnID, p.runID, state)
			if !valid {
				if reviews >= 2 {
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
