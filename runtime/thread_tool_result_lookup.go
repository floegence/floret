package runtime

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

// ThreadToolResultReader reads a confirmed local tool result from the active
// canonical path. Hosts authorize access and interpret their own tool output;
// these private facts must not be included in public presentation by default.
// Lookup never dispatches a tool, reads an artifact, or changes journal state.
type ThreadToolResultReader interface {
	LookupToolResult(context.Context, LookupToolResultInput) (ThreadToolResult, bool, error)
}

type LookupToolResultInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	TurnID     identity.TurnID   `json:"turn_id"`
	RunID      identity.RunID    `json:"run_id"`
	ToolCallID string            `json:"tool_call_id"`
}

// ThreadToolResult contains the original canonical arguments and visible output,
// without rebuilding provider history or resolving any host resource.
type ThreadToolResult struct {
	EffectOutcome tools.EffectOutcome `json:"effect_outcome,omitempty"`
	ToolName      string              `json:"tool_name"`
	Arguments     string              `json:"arguments"`
	Content       string              `json:"content"`
	Status        string              `json:"status"`
}

var _ ThreadToolResultReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) LookupToolResult(ctx context.Context, in LookupToolResultInput) (ThreadToolResult, bool, error) {
	if strings.TrimSpace(in.TurnID.String()) == "" || strings.TrimSpace(in.RunID.String()) == "" || strings.TrimSpace(in.ToolCallID) == "" {
		return ThreadToolResult{}, false, errors.New("tool result execution identity is required")
	}
	meta, err := service.ensureThread(ctx, in.ThreadID)
	if err != nil {
		return ThreadToolResult{}, false, err
	}
	entries, err := service.host.store.repo.Path(ctx, in.ThreadID.String(), meta.LeafID)
	if err != nil {
		return ThreadToolResult{}, false, runtimeHostError(err)
	}
	var call *session.Message
	for _, entry := range entries {
		if entry.TurnID != in.TurnID.String() || entry.RunID != in.RunID.String() || entry.Message.ToolCallID != in.ToolCallID || entry.Message.Kind != session.MessageKindNormal {
			continue
		}
		switch entry.Type {
		case sessiontree.EntryToolCall:
			message := entry.Message
			call = &message
		case sessiontree.EntryToolResult:
			if call == nil || call.ToolName != entry.Message.ToolName || entry.Message.ToolResult == nil {
				return ThreadToolResult{}, false, ErrAuthorityCorrupt
			}
			return ThreadToolResult{
				ToolName: call.ToolName, Arguments: call.ToolArgs, EffectOutcome: entry.Message.ToolResult.EffectOutcome,
				Content: entry.Message.Content, Status: entry.Message.ToolResult.Status,
			}, true, nil
		}
	}
	return ThreadToolResult{}, false, nil
}
