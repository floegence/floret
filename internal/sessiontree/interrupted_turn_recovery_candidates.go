package sessiontree

import (
	"context"
	"errors"
	"sort"
	"strings"
)

// InterruptedTurnRecoveryCandidate identifies one thread whose canonical
// authority currently contains a recoverable turn lease. ParentThreadID is
// empty for a root thread and names the direct parent for a child thread.
type InterruptedTurnRecoveryCandidate struct {
	ThreadID       string
	ParentThreadID string
}

// InterruptedTurnRecoveryCandidateRepo is the read-only discovery contract
// used by hosts before they bind exact recovery authority.
type InterruptedTurnRecoveryCandidateRepo interface {
	ListInterruptedTurnRecoveryCandidates(context.Context) ([]InterruptedTurnRecoveryCandidate, error)
}

// ListInterruptedTurnRecoveryCandidates returns every active turn lease from
// one canonical domain snapshot in deterministic creation order. Discovery
// does not grant recovery authority; callers must still bind the exact proof
// through InterruptedTurnRecovery.
func (r *MemoryRepo) ListInterruptedTurnRecoveryCandidates(_ context.Context) ([]InterruptedTurnRecoveryCandidate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	candidates := make([]InterruptedTurnRecoveryCandidate, 0, len(r.leases))
	for threadID, lease := range r.leases {
		if lease.Purpose != TurnLeasePurposeTurn {
			continue
		}
		meta, ok := r.threads[threadID]
		if !ok {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("recovery lease references a missing thread"))
		}
		path, err := pathLocked(r.threads, r.entries, threadID, meta.LeafID)
		if err != nil {
			return nil, err
		}
		if err := ValidateThreadAuthoritySnapshot(meta, path, &lease, r.authorityClaims[threadID], r.leaseGeneration[threadID]); err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		candidates = append(candidates, InterruptedTurnRecoveryCandidate{
			ThreadID: threadID, ParentThreadID: strings.TrimSpace(meta.ParentThreadID),
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := r.threads[candidates[i].ThreadID]
		right := r.threads[candidates[j].ThreadID]
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.After(right.CreatedAt)
		}
		return candidates[i].ThreadID < candidates[j].ThreadID
	})
	return candidates, nil
}

func (r *FileRepo) ListInterruptedTurnRecoveryCandidates(ctx context.Context) ([]InterruptedTurnRecoveryCandidate, error) {
	if r == nil || r.mem == nil {
		return nil, errors.New("file session tree repo is nil")
	}
	return r.mem.ListInterruptedTurnRecoveryCandidates(ctx)
}
