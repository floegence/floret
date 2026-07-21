package agentharness

import "github.com/floegence/floret/internal/sessiontree"

type TurnExecutionRegistry struct {
	Register   func(sessiontree.TurnLease) error
	Renew      func(sessiontree.TurnLease, sessiontree.TurnLease) error
	Unregister func(sessiontree.TurnLease)
	Active     func(string) (sessiontree.TurnLease, bool)
}

func (r *TurnExecutionRegistry) validate() bool {
	return r != nil && r.Register != nil && r.Renew != nil && r.Unregister != nil && r.Active != nil
}
