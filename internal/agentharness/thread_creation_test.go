package agentharness

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

// StartThreadOptions and StartThread exist only in package tests. Production
// creation must enter through the root-create coordinator and BindCreatedRoot.
type StartThreadOptions struct {
	ThreadID string
}

func (h *AgentHarness) StartThread(ctx context.Context, opts StartThreadOptions) (*Thread, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness is not configured")
	}
	threadID := strings.TrimSpace(opts.ThreadID)
	authority, ok := h.options.Repo.(sessiontree.RootAuthorityRepo)
	if !ok {
		return nil, errors.New("session tree repo does not support root authority")
	}
	created, err := authority.CreateRoot(ctx, sessiontree.CreateRootRequest{
		ThreadID: threadID, CreateIntentID: "agentharness-test-create:" + threadID, ContractVersion: "1",
		Meta: sessiontree.ThreadMeta{ID: threadID},
	})
	if err != nil {
		return nil, err
	}
	return h.BindCreatedRoot(created.Thread, created.Replayed)
}
