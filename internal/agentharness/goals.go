package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

const goalInstructions = "\n# Goal\nUse goal to maintain one clear, concise, understandable desired result for sustained execution, investigation or research. Usually skip simple questions and greetings. Preserve the user's intent when updating. Do not delete or reduce unfinished goals because work is difficult. Delete only cancelled, replaced or obsolete goals with an honest reason. A user deletion suppresses recreation for the same task; new_task may be true only for a genuinely new user task, never an answer, approval, retry, compaction or restart. Before naturally ending, verify the current goal against user constraints and existing results. Use existing evidence first and run only necessary missing checks. Plans, attempts and unverified assumptions are not completion. Use ask_user when an answer is needed. When externally blocked, report the reason and retain the goal. Submit goal review with completed, continue or blocked and brief evidence. After review, provide the final user-facing answer. Goal changes take effect on the next model request."
const goalReviewPrompt = "Check the current Goal, user constraints and existing execution results. Continue necessary unfinished work. Use ask_user if a user answer is required. If externally blocked, explain honestly and preserve the Goal. Submit goal review with the current version and brief evidence before finishing. If you removed an obsolete Goal, explain that decision in a blocked review; deletion alone is not a completion check. Plans, attempts and unverified assumptions are not completion evidence."

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
	return tools.Define[goalToolInput](tools.Definition{Name: "goal", Title: "Goal", Description: "Maintain the thread's desired result or review whether it is complete. Write clear, concise, understandable goal text. Preserve user intent. set cannot mark completion. delete requires a reason. review requires outcome and brief evidence. Use the latest version from runtime facts; after conflict read the returned state. Submit review separately after necessary work tools finish.", InputSchema: schema, ReadOnly: true}, nil, nil, func(ctx context.Context, inv tools.Invocation[goalToolInput]) (tools.Result, error) {
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
			return tools.Result{Text: "Goal review recorded. Provide a final answer, or continue work if needed."}, nil
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
	if err := p.flushPendingToolBatch(true); err != nil {
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
	raw, _ := json.Marshal(state)
	text := "Goal runtime fact: " + string(raw)
	if session.CurrentGoalContext(history) == text {
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
