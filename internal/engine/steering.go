package engine

import (
	"context"
	"github.com/floegence/floret/v7/internal/session"
)

// SteeringControl belongs to the thread actor. Begin installs one interruptible
// execution fragment and returns refreshed canonical history only after steering
// was applied. Finish atomically seals completion or asks the Engine to continue.
type SteeringControl struct {
	CanContinue func(error) bool
	Begin       func(context.Context, func() error) (context.Context, []session.Message, error)
	Finish      func(context.Context) (bool, error)
}
