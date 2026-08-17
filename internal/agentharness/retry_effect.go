package agentharness

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v4/identity"
	"github.com/floegence/floret/v4/internal/engine"
	"github.com/floegence/floret/v4/internal/session"
	"github.com/floegence/floret/v4/internal/session/artifact"
	"github.com/floegence/floret/v4/internal/sessiontree"
	"github.com/floegence/floret/v4/observation"
	"github.com/floegence/floret/v4/tools"
)

// RetryUnknownEffect performs one explicitly acknowledged new attempt. The
// original unknown attempt remains immutable and is never replayed.
func (t *Thread) RetryUnknownEffect(ctx context.Context, sourceAttemptID, requestKey string) (sessiontree.Entry, error) {
	if t == nil || t.harness == nil || t.harness.options.Tools == nil {
		return sessiontree.Entry{}, errors.New("thread effect runtime is unavailable")
	}
	reader, ok := t.harness.options.Repo.(sessiontree.EffectAttemptReader)
	if !ok {
		return sessiontree.Entry{}, errors.New("session tree repo does not support effect reads")
	}
	source, err := reader.EffectAttempt(ctx, t.id, strings.TrimSpace(sourceAttemptID))
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if source.State != sessiontree.EffectAttemptUnknown && source.State != sessiontree.EffectAttemptRetrying {
		return sessiontree.Entry{}, sessiontree.ErrRequestConflict
	}
	call, err := t.retryEffectToolCall(ctx, source)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	retryCtx := withEffectRetry(ctx, requestKey, source.EffectAttemptID)
	result := t.harness.options.Tools.Dispatch(retryCtx, call, tools.DispatchOptions{
		RunID: identity.RunID(source.Invocation.RunID), ThreadID: identity.ThreadID(source.Invocation.ThreadID),
		TurnID:           identity.TurnID(source.Invocation.TurnID),
		PromptScopeID:    identity.PromptScopeID("effect-retry:" + source.EffectAttemptID),
		EffectDispatcher: t.effectDispatcher(),
	})
	if result.DispatchErr != nil {
		return sessiontree.Entry{}, result.DispatchErr
	}
	if !result.RequiresEffectFinalization() {
		return sessiontree.Entry{}, errors.New("retried effect did not cross the one-shot authorization fence")
	}
	projection := tools.BuildOutputProjection(result, tools.MergeOutputPolicy(t.harness.options.Tools.OutputPolicyFor(result.Name), result.OutputPolicy))
	status := string(observation.ActivityStatusSuccess)
	content := projection.VisibleText
	if result.IsError {
		status = string(observation.ActivityStatusError)
		content = "ERROR: " + content
	}
	view := &session.ToolResultView{
		Status: status, Truncated: projection.Truncated,
		OriginalBytes: projection.OriginalBytes, VisibleBytes: projection.VisibleBytes,
		OriginalLines: projection.OriginalLines, VisibleLines: projection.VisibleLines,
		Strategy: string(projection.Strategy), ContentSHA256: projection.ContentSHA256,
	}
	message := session.Message{Role: session.Tool, Content: content, ToolCallID: call.ID, ToolName: call.Name, ToolResult: view}
	var full *artifact.FullOutput
	if projection.FullOutputPlan != nil {
		full = &artifact.FullOutput{Text: projection.FullOutputPlan.Text, Kind: projection.FullOutputPlan.Kind, MIME: projection.FullOutputPlan.MIME}
	}
	finalized, err := t.finalizeEffectResult(retryCtx, engine.EffectResultFinalizationRequest{
		RunID: source.Invocation.RunID, ThreadID: source.Invocation.ThreadID, TurnID: source.Invocation.TurnID,
		ToolCallID: call.ID, Message: message, FullOutput: full,
	})
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if !finalized.Handled || strings.TrimSpace(finalized.CanonicalEntryID) == "" {
		return sessiontree.Entry{}, sessiontree.ErrAuthorityCorrupt
	}
	return t.harness.options.Repo.Entry(ctx, t.id, finalized.CanonicalEntryID)
}

func (t *Thread) retryEffectToolCall(ctx context.Context, source sessiontree.EffectAttempt) (tools.ToolCall, error) {
	entries, err := t.harness.options.Repo.Entries(ctx, t.id)
	if err != nil {
		return tools.ToolCall{}, err
	}
	for index := len(entries) - 1; index >= 0; index-- {
		entry := entries[index]
		if entry.TurnID != source.Invocation.TurnID || entry.Type != sessiontree.EntryToolCall ||
			entry.Message.ToolCallID != source.Invocation.ToolCallID || entry.Message.ToolName != source.Invocation.ToolName {
			continue
		}
		if sessiontree.StableHash(strings.TrimSpace(entry.Message.ToolArgs)) != source.Invocation.ArgumentHash {
			return tools.ToolCall{}, sessiontree.ErrAuthorityCorrupt
		}
		return tools.ToolCall{ID: entry.Message.ToolCallID, Name: entry.Message.ToolName, Args: entry.Message.ToolArgs}, nil
	}
	return tools.ToolCall{}, sessiontree.ErrEffectAttemptNotFound
}
