package agentharness

import (
	"context"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func (p *turnProjection) admitParentEvents(ctx context.Context) (session.Message, error) {
	repo, ok := p.thread.harness.options.Repo.(sessiontree.ParentEventRepo)
	if !ok {
		return session.Message{}, nil
	}
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return session.Message{}, p.err
	}
	if err := p.flushPendingToolBatch(true); err != nil {
		return session.Message{}, err
	}
	entry, err := repo.AdmitParentEvents(ctx, p.thread.id, p.turnID, p.runID)
	if err != nil || entry.ID == "" {
		return session.Message{}, err
	}
	p.thread.harness.emitEntryCommitted(entry, p.runID)
	message := session.CloneMessage(entry.Message)
	message.EntryID, message.ParentEntryID = entry.ID, entry.ParentID
	return message, nil
}
