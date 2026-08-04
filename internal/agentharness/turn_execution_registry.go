package agentharness

import "github.com/floegence/floret/v3/internal/sessiontree"

type TurnExecutionRegistry struct {
	Register       func(sessiontree.TurnLease) error
	Renew          func(sessiontree.TurnLease, sessiontree.TurnLease) error
	Unregister     func(sessiontree.TurnLease)
	Active         func(string) (sessiontree.TurnLease, bool)
	BeginAdmission func(string, string)
	EndAdmission   func(string, string)
	Snapshot       func(string, string) (sessiontree.TurnLease, bool, bool)
}

func (r *TurnExecutionRegistry) validate() bool {
	return r != nil && r.Register != nil && r.Renew != nil && r.Unregister != nil && r.Active != nil
}

func (r *TurnExecutionRegistry) beginAdmission(threadID, turnID string) func() {
	if r == nil || r.BeginAdmission == nil || r.EndAdmission == nil {
		return func() {}
	}
	r.BeginAdmission(threadID, turnID)
	return func() { r.EndAdmission(threadID, turnID) }
}

func (r *TurnExecutionRegistry) snapshot(threadID, turnID string) (sessiontree.TurnLease, bool, bool) {
	if r == nil {
		return sessiontree.TurnLease{}, false, false
	}
	if r.Snapshot != nil {
		return r.Snapshot(threadID, turnID)
	}
	if r.Active == nil {
		return sessiontree.TurnLease{}, false, false
	}
	lease, ok := r.Active(threadID)
	return lease, ok, false
}
