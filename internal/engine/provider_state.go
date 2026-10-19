package engine

import (
	"context"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

// ProviderContextCommitResult separates committed facts from notification failure.
// Notification errors never turn a completed transaction into an uncommitted output.
type ProviderContextCommitResult struct {
	Messages        []session.Message
	NotificationErr error
}

// One owner advances state only after the complete canonical output commits.
type runProviderState struct {
	state  *provider.State
	fresh  bool
	commit func(context.Context, *provider.State, ResponseOutput, []session.Message, bool) (ProviderContextCommitResult, error)
}

func (owner *runProviderState) clear(ctx context.Context) error {
	if owner.commit != nil {
		if result, err := owner.commit(ctx, nil, ResponseOutput{}, nil, true); err != nil {
			return err
		} else if result.NotificationErr != nil {
			owner.state, owner.fresh = nil, false
			return result.NotificationErr
		}
	}
	owner.state, owner.fresh = nil, false
	return nil
}

func (owner *runProviderState) complete(ctx context.Context, state *provider.State, output ResponseOutput, calls []session.Message, supplemental bool) (ProviderContextCommitResult, error) {
	if err := provider.ValidateState(state); err != nil {
		return ProviderContextCommitResult{}, err
	}
	if supplemental {
		state = nil
	}
	result := ProviderContextCommitResult{}
	if owner.commit != nil {
		var err error
		result, err = owner.commit(ctx, state, output, calls, false)
		if err != nil {
			return ProviderContextCommitResult{}, err
		}
	} else {
		for _, item := range output.Items {
			if item.Hosted == nil {
				result.Messages = append(result.Messages, item.Message)
			}
		}
		result.Messages = append(result.Messages, calls...)
	}
	if state != nil {
		owner.state, owner.fresh = provider.CloneState(state), true
	}
	return result, nil
}
