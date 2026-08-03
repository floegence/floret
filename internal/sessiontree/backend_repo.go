package sessiontree

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/v3/internal/storagecodec"
	"github.com/floegence/floret/v3/storage/spi"
)

const backendDomainNamespace = "floret.domain"

var backendStateKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))

// BackendRepo executes the canonical session-tree semantics inside Backend
// snapshot and serializable transactions.
type BackendRepo struct {
	backend  spi.Backend
	now      func() time.Time
	policy   LeasePolicy
	mu       sync.Mutex
	changeMu sync.Mutex
	change   chan struct{}
}

// NewBackendRepo initializes or validates the canonical session-tree state.
func NewBackendRepo(ctx context.Context, backend spi.Backend, policy LeasePolicy, now func() time.Time) (*BackendRepo, error) {
	if backend == nil || now == nil {
		return nil, errors.New("backend repo requires backend and clock")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	repo := &BackendRepo{backend: backend, now: now, policy: policy, change: make(chan struct{})}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		memory, found, migrated, err := repo.load(tx)
		if err != nil {
			return err
		}
		if !found {
			memory, err = NewMemoryRepoWithLeasePolicy(policy, now)
			if err != nil {
				return err
			}
			return repo.save(tx, memory)
		}
		repo.policy = memory.AuthorityLeasePolicy()
		if migrated {
			return repo.save(tx, memory)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return repo, nil
}

func (repo *BackendRepo) load(tx spi.ReadTx) (*MemoryRepo, bool, bool, error) {
	encoded, err := tx.Get(backendDomainNamespace, backendStateKey)
	if errors.Is(err, spi.ErrNotFound) {
		return nil, false, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}
	payload, err := storagecodec.DecodeEnvelope(encoded, "sessiontree")
	if err != nil {
		return nil, false, false, err
	}
	memory, migrated, err := decodeMemoryState(payload, repo.now)
	if err != nil {
		return nil, false, false, err
	}
	return memory, true, migrated, nil
}

func (repo *BackendRepo) save(tx spi.WriteTx, memory *MemoryRepo) error {
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		return err
	}
	encoded, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		return err
	}
	return tx.Put(backendDomainNamespace, backendStateKey, encoded)
}

func (repo *BackendRepo) view(ctx context.Context, read func(*MemoryRepo) error) error {
	return repo.ViewDomain(ctx, func(memory *MemoryRepo, _ spi.ReadTx) error { return read(memory) })
}

// ViewDomain executes one read against an exact backend snapshot.
func (repo *BackendRepo) ViewDomain(ctx context.Context, read func(*MemoryRepo, spi.ReadTx) error) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
		memory, found, _, err := repo.load(tx)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("session-tree state is missing")
		}
		return read(memory, tx)
	})
}

func (repo *BackendRepo) update(ctx context.Context, mutate func(*MemoryRepo) error) error {
	return repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error { return mutate(memory) })
}

// UpdateDomain executes one session-tree mutation and related domain writes in
// the same serializable backend transaction.
func (repo *BackendRepo) UpdateDomain(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	return repo.updateDomain(ctx, mutate)
}

func (repo *BackendRepo) updateDomain(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	changed := false
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		memory, found, _, err := repo.load(tx)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("session-tree state is missing")
		}
		before := memory.revisionFacts()
		if err := mutate(memory, tx); err != nil {
			return err
		}
		changed = memory.advanceThreadRevisions(before)
		return repo.save(tx, memory)
	})
	if err == nil && changed {
		repo.signalChange()
	}
	return err
}

// AuthorityLeasePolicy returns the immutable persisted lease policy.
func (repo *BackendRepo) AuthorityLeasePolicy() LeasePolicy {
	return repo.policy
}

func (repo *BackendRepo) signalChange() {
	repo.changeMu.Lock()
	close(repo.change)
	repo.change = make(chan struct{})
	repo.changeMu.Unlock()
}

func (repo *BackendRepo) changes() <-chan struct{} {
	repo.changeMu.Lock()
	defer repo.changeMu.Unlock()
	return repo.change
}

var errApprovalPending = errors.New("approval decision is pending")

// WaitApprovalDecision waits without holding a backend transaction open.
func (repo *BackendRepo) WaitApprovalDecision(ctx context.Context, approvalID string) (result WaitApprovalDecisionResult, err error) {
	for {
		changed := repo.changes()
		err = repo.view(ctx, func(memory *MemoryRepo) error {
			record, readErr := memory.Approval(ctx, approvalID)
			if readErr != nil {
				return readErr
			}
			if record.State == ApprovalRequested {
				return errApprovalPending
			}
			result, readErr = memory.WaitApprovalDecision(ctx, approvalID)
			return readErr
		})
		if !errors.Is(err, errApprovalPending) {
			return result, err
		}
		select {
		case <-ctx.Done():
			return WaitApprovalDecisionResult{}, ctx.Err()
		case <-changed:
		}
	}
}
