package agentharness

import (
	"context"

	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/tools"
)

// Approval is represented by one canonical interaction requested by the
// ThreadRuntime authorization gate. Batch preflight therefore validates no
// second queue or generation state.
func (t *Thread) preflightEffectBatch(context.Context, []tools.EffectDispatchRequest) error {
	return nil
}

func effectAuthorizationRequest(request tools.EffectDispatchRequest, attempt sessiontree.EffectAttempt, fingerprint string) EffectAuthorizationRequest {
	return EffectAuthorizationRequest{
		EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: fingerprint,
		ThreadID: request.ThreadID.String(), TurnID: request.TurnID.String(), RunID: request.RunID.String(),
		ToolCallID: request.CallID, ToolName: request.Name, Arguments: request.RawArgs,
		ArgumentHash: attempt.Invocation.ArgumentHash, Step: request.Step,
		BatchIndex: request.BatchIndex, BatchSize: request.BatchSize,
		Labels: cloneStringMap(request.Labels), HostContext: cloneStringMap(request.HostContext),
		Activity:  tools.CloneActivityPresentation(request.Activity),
		Resources: append([]tools.ResourceRef(nil), request.Resources...),
		Effects:   append([]tools.Effect(nil), request.Effects...), Permission: request.Permission,
		ReadOnly: request.ReadOnly, Destructive: request.Destructive, OpenWorld: request.OpenWorld,
	}
}
