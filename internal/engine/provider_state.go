package engine

import (
	"context"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

// One owner advances state only after the complete canonical output commits.
type runProviderState struct {
	state  *provider.State
	fresh  bool
	commit func(context.Context, *provider.State, []session.Message, bool) error
}

func (owner *runProviderState) clear(ctx context.Context) error {
	if owner.commit != nil {
		if err := owner.commit(ctx, nil, nil, true); err != nil {
			return err
		}
	}
	owner.state, owner.fresh = nil, false
	return nil
}

func (owner *runProviderState) complete(ctx context.Context, state *provider.State, calls []session.Message, supplemental bool) error {
	if err := provider.ValidateState(state); err != nil {
		return err
	}
	if supplemental {
		state = nil
	}
	if owner.commit != nil {
		if err := owner.commit(ctx, state, calls, false); err != nil {
			return err
		}
	}
	if state != nil {
		owner.state, owner.fresh = provider.CloneState(state), true
	}
	return nil
}
