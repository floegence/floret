package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/sessiontree"
)

// InterruptedTurnRecoveryCandidate identifies one exact root or child thread
// that has a canonical turn lease requiring host-side recovery inspection.
// ParentThreadID is empty for roots.
type InterruptedTurnRecoveryCandidate struct {
	ThreadID       identity.ThreadID `json:"thread_id"`
	ParentThreadID identity.ThreadID `json:"parent_thread_id,omitempty"`
}

// ListInterruptedTurnRecoveryCandidates discovers recovery targets with one
// canonical store read. It only returns identities; callers must still bind
// the exact lease proof through ThreadLifecycle.InterruptedTurnRecovery or
// Child.InterruptedTurnRecovery before recovering.
func (threads *Threads) ListInterruptedTurnRecoveryCandidates(ctx context.Context) ([]InterruptedTurnRecoveryCandidate, error) {
	if threads == nil || threads.host == nil {
		return nil, errors.New("thread collection is required")
	}
	if ctx == nil {
		return nil, errors.New("recovery candidate context is required")
	}
	host := threads.host
	if err := host.available(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	done, err := beginHostOperation(host.store)
	if err != nil {
		return nil, err
	}
	defer done()
	reader, ok := host.store.repo.(sessiontree.InterruptedTurnRecoveryCandidateRepo)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	items, err := reader.ListInterruptedTurnRecoveryCandidates(ctx)
	if err != nil {
		return nil, runtimeHostError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]InterruptedTurnRecoveryCandidate, 0, len(items))
	seen := make(map[identity.ThreadID]struct{}, len(items))
	for index, item := range items {
		threadID, err := identity.ParseThreadID(strings.TrimSpace(item.ThreadID))
		if err != nil {
			return nil, fmt.Errorf("%w: recovery candidate %d thread identity: %v", ErrAuthorityCorrupt, index, err)
		}
		parentID := identity.ThreadID(strings.TrimSpace(item.ParentThreadID))
		if parentID != "" {
			if _, err := identity.ParseThreadID(parentID.String()); err != nil {
				return nil, fmt.Errorf("%w: recovery candidate %d parent identity: %v", ErrAuthorityCorrupt, index, err)
			}
			if parentID == threadID {
				return nil, fmt.Errorf("%w: recovery candidate %q is its own parent", ErrAuthorityCorrupt, threadID)
			}
		}
		if _, duplicate := seen[threadID]; duplicate {
			return nil, fmt.Errorf("%w: recovery candidate %q is duplicated", ErrAuthorityCorrupt, threadID)
		}
		seen[threadID] = struct{}{}
		out = append(out, InterruptedTurnRecoveryCandidate{ThreadID: threadID, ParentThreadID: parentID})
	}
	return out, nil
}
