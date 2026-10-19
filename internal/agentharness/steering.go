package agentharness

import (
	"context"
	"github.com/floegence/floret/v7/internal/engine"
)

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
	if err := p.closePendingToolBatchForTerminalTurn(engine.Cancelled, context.Canceled); err != nil {
		return err
	}
	p.finalCandidate = nil
	return nil
}
