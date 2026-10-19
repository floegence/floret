package agentharness

import ()

// The Engine calls this only after the interrupted fragment has settled. A
// partial provider draft is discarded; confirmed tool facts keep their existing
// finalizer and incomplete calls receive the normal cancellation closure.
func (p *turnProjection) prepareSteeringBoundary() error {
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	path, err := p.thread.harness.options.Repo.Path(p.ctx, p.thread.id, "")
	if err != nil {
		return err
	}
	if err := p.thread.harness.closeInterruptedTurnToolCalls(p.ctx, p.thread.id, p.turnID, path); err != nil {
		return err
	}
	p.finalCandidate = nil
	return nil
}
