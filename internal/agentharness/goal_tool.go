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

const goalToolName = "goal"

// installGoals binds the canonical Goal tool and its lifecycle to this execution.
// Ownership comes from installing the handler in every composed, sealed surface;
// host tool names and annotations cannot opt into this dispatch path.
func (p *turnProjection) installGoals(options *engine.Options, stopHook engine.StopHook) engine.StopHook {
	tool := goalTool(p)
	dispatch := options.EffectDispatcher
	finalize := options.EffectResultFinalizer
	options.ToolSurfaceProvider = p.goalSurface(options.ToolSurfaceProvider, p.thread.harness.options.Tools, tool)
	options.AdmitGoalContext = p.admitGoalContext
	options.EffectDispatcher = func(ctx context.Context, request tools.EffectDispatchRequest, invoke func(context.Context) tools.Result) tools.Result {
		if request.Name != tool.Definition.Name {
			return dispatch(ctx, request, invoke)
		}
		if request.ThreadID.String() != p.thread.id || request.TurnID.String() != p.turnID || request.RunID.String() != p.runID || strings.TrimSpace(request.CallID) == "" ||
			request.ReadOnly || request.Destructive || request.OpenWorld || len(request.Effects) != 1 || request.Effects[0] != tools.EffectWrite || len(request.Resources) != 0 || request.Permission.Mode != tools.PermissionAllow {
			return effectDispatchError(request.CallID, request.Name, ErrAuthorizationContract)
		}
		if err := ctx.Err(); err != nil {
			return effectDispatchError(request.CallID, request.Name, err)
		}
		return invoke(ctx)
	}
	options.EffectResultFinalizer = func(ctx context.Context, request engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
		if request.Message.ToolName != tool.Definition.Name {
			return finalize(ctx, request)
		}
		if request.ThreadID != p.thread.id || request.TurnID != p.turnID || request.RunID != p.runID || strings.TrimSpace(request.ToolCallID) == "" || request.Message.ToolCallID != request.ToolCallID || request.Message.Role != session.Tool {
			return engine.EffectResultFinalizationResult{}, ErrAuthorizationContract
		}
		// Goal mutations and reviews already use the canonical journal writer.
		// The Engine commits their paired result through CommitMessages.
		return engine.EffectResultFinalizationResult{}, nil
	}
	return p.goalStopHook(stopHook)
}

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
	return tools.Define[goalToolInput](tools.Definition{Name: goalToolName, Title: "Goal", Description: "Maintain the thread's desired result or review whether it is complete. Write clear, concise, understandable goal text. Preserve user intent. set cannot mark completion. delete requires a reason. review requires outcome and brief evidence. Use the latest version from runtime facts; after conflict read the returned state. Submit review separately after necessary work tools finish.", InputSchema: schema, Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, Activity: func(inv tools.Invocation[any]) (*tools.ActivityPresentation, error) {
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
func (p *turnProjection) goalSurface(base engine.ToolSurfaceProvider, registry *tools.Registry, tool tools.Tool) engine.ToolSurfaceProvider {
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
		// A host registry cannot replace the installed handler, even with the
		// same name or annotations. Seal the independent composed registry.
		surface.Tools, err = source.WithTools(tool)
		if err != nil {
			return surface, err
		}
		surface.Tools.Seal()
		if surface.ToolDefinitions != nil {
			definitions := append([]tools.ToolDefinition{}, surface.ToolDefinitions...)
			for _, def := range definitions {
				if def.Name == tool.Definition.Name {
					return surface, tools.ErrDuplicate
				}
			}
			for _, def := range surface.Tools.Definitions() {
				if def.Name == tool.Definition.Name {
					definitions = append(definitions, def)
				}
			}
			surface.ToolDefinitions = definitions
		}
		if surface.SystemPrompt == "" {
			surface.SystemPrompt = p.thread.harness.options.SystemPrompt
		}
		surface.SystemPrompt += goalInstructions
		return surface, nil
	}
}
