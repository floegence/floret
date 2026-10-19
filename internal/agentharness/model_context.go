package agentharness

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func (p *turnProjection) admitModelContext(ctx context.Context, snapshot *session.ModelContextSnapshot) (session.Message, error) {
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return session.Message{}, p.err
	}
	if err := p.err; err != nil {
		return session.Message{}, err
	}
	repo, ok := p.thread.harness.options.Repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return session.Message{}, errors.New("repo does not support canonical model context admission")
	}
	message := session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: session.CloneModelContext(snapshot)}
	entry := sessiontree.RunMessageEntry(p.thread.id, p.turnID, p.runID, message, p.thread.harness.now())
	entry.ID = p.thread.harness.nextID("model-context")
	entries, err := repo.AppendRuntimeFacts(ctx, p.thread.id, []sessiontree.Entry{entry})
	if err != nil {
		return session.Message{}, err
	}
	entry = entries[0]
	p.thread.harness.emitEntryCommitted(entry, p.runID)
	message.EntryID, message.ParentEntryID = entry.ID, entry.ParentID
	return message, nil
}
