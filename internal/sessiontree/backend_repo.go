package sessiontree

import (
	"bytes"
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
	// rootInventoryEncoded is compared with the durable record before serving
	// the decoded projection. Root inventory reads are frequent and the record
	// can contain large activity payloads, so decoding it on every poll causes
	// avoidable CPU and allocation pressure.
	rootInventoryEncoded []byte
	rootInventoryItems   []RootThreadInventoryItem
	// domainEncoded/domainMemory cache one exact, validated durable snapshot.
	// The encoded bytes remain authoritative: every view reads them from the
	// backend transaction before deciding whether the decoded projection can be
	// reused.
	domainEncoded []byte
	domainMemory  *MemoryRepo
	domainDirty   bool
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
	var committedInventory []RootThreadInventoryItem
	var committedInventoryEncoded []byte
	var committedMemory *MemoryRepo
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
			committedInventoryEncoded, err = repo.save(tx, memory)
			if err != nil {
				return err
			}
			committedInventory, err = memory.rootThreadInventoryLocked()
			if err != nil {
				return err
			}
			if err := attachRootThreadInventoryProjectionFingerprints(committedInventory); err != nil {
				return err
			}
			committedMemory = memory
			return nil
		}
		repo.policy = memory.AuthorityLeasePolicy()
		if migrated {
			committedInventoryEncoded, err = repo.save(tx, memory)
			if err != nil {
				return err
			}
			committedInventory, err = memory.rootThreadInventoryLocked()
			if err != nil {
				return err
			}
			if err := attachRootThreadInventoryProjectionFingerprints(committedInventory); err != nil {
				return err
			}
			committedMemory = memory
			return nil
		}
		if err := repo.verifyRootThreadInventory(tx, memory); err != nil {
			return err
		}
		committedInventory, err = memory.rootThreadInventoryLocked()
		if err != nil {
			return err
		}
		committedInventoryEncoded, err = tx.Get(backendDomainNamespace, backendRootThreadInventoryKey)
		committedMemory = memory
		return err
	}); err != nil {
		return nil, err
	}
	repo.rootInventoryEncoded = bytes.Clone(committedInventoryEncoded)
	repo.rootInventoryItems = cloneRootThreadInventoryItems(committedInventory)
	repo.domainMemory = committedMemory
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		encoded, err := tx.Get(backendDomainNamespace, backendStateKey)
		if err != nil {
			return err
		}
		repo.domainEncoded = bytes.Clone(encoded)
		return nil
	}); err != nil {
		return nil, err
	}
	repo.domainDirty = false
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

func (repo *BackendRepo) loadView(tx spi.ReadTx) (*MemoryRepo, bool, []byte, bool, error) {
	encoded, err := tx.Get(backendDomainNamespace, backendStateKey)
	if errors.Is(err, spi.ErrNotFound) {
		return nil, false, nil, false, nil
	}
	if err != nil {
		return nil, false, nil, false, err
	}
	if repo.domainMemory != nil && bytes.Equal(encoded, repo.domainEncoded) {
		return repo.domainMemory, true, nil, true, nil
	}
	payload, err := storagecodec.DecodeEnvelope(encoded, "sessiontree")
	if err != nil {
		return nil, false, nil, false, err
	}
	memory, _, err := decodeMemoryState(payload, repo.now)
	if err != nil {
		return nil, false, nil, false, err
	}
	return memory, true, encoded, false, nil
}

func (repo *BackendRepo) save(tx spi.WriteTx, memory *MemoryRepo) ([]byte, error) {
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		return nil, err
	}
	encoded, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		return nil, err
	}
	inventory, err := encodeRootThreadInventory(memory)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(backendDomainNamespace, backendStateKey, encoded); err != nil {
		return nil, err
	}
	if err := tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, inventory); err != nil {
		return nil, err
	}
	return inventory, nil
}

func (repo *BackendRepo) verifyRootThreadInventory(tx spi.ReadTx, memory *MemoryRepo) error {
	persisted, err := tx.Get(backendDomainNamespace, backendRootThreadInventoryKey)
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	want, err := encodeRootThreadInventory(memory)
	if err != nil {
		return err
	}
	if !bytes.Equal(persisted, want) {
		return errors.Join(ErrAuthorityCorrupt, errors.New("root thread inventory does not match canonical domain"))
	}
	return nil
}

func (repo *BackendRepo) view(ctx context.Context, read func(*MemoryRepo) error) error {
	return repo.ViewDomain(ctx, func(memory *MemoryRepo, _ spi.ReadTx) error { return read(memory) })
}

// ViewDomain executes one read against an exact backend snapshot. The read
// callback must be observational only; durable mutations belong in
// UpdateDomain. This permits the validated snapshot to be reused by the
// frequent read projections without exposing an alternate mutable copy.
func (repo *BackendRepo) ViewDomain(ctx context.Context, read func(*MemoryRepo, spi.ReadTx) error) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
		if repo.domainDirty && repo.domainMemory != nil {
			return read(repo.domainMemory, tx)
		}
		memory, found, encoded, cacheHit, err := repo.loadView(tx)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("session-tree state is missing")
		}
		if err := read(memory, tx); err != nil {
			return err
		}
		if !cacheHit {
			repo.domainEncoded = bytes.Clone(encoded)
			repo.domainMemory = memory
		}
		return nil
	})
}

// Checkpoint flushes the current in-process authority to the durable backend.
// It is intentionally explicit so ordinary live mutations can stay on the
// actor's memory path while shutdown and semantic barriers remain recoverable.
func (repo *BackendRepo) Checkpoint(ctx context.Context) error {
	if repo == nil {
		return errors.New("backend repository is required")
	}
	return repo.UpdateDomain(ctx, func(*MemoryRepo, spi.WriteTx) error { return nil })
}

func (repo *BackendRepo) update(ctx context.Context, mutate func(*MemoryRepo) error) error {
	return repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error { return mutate(memory) })
}

// updateMemory applies a mutation to the live in-process authority without
// encoding the complete session-tree state. Callers must use a durable
// request ledger or a later semantic checkpoint when recovery requires the
// mutation to survive a process crash.
func (repo *BackendRepo) updateMemory(ctx context.Context, mutate func(*MemoryRepo) error) error {
	if repo == nil || mutate == nil {
		return errors.New("backend memory mutation requires repository and callback")
	}
	if ctx == nil {
		return errors.New("backend memory mutation context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.domainMemory == nil {
		return errors.New("session-tree state is missing")
	}
	before := repo.domainMemory.revisionFacts()
	if err := mutate(repo.domainMemory); err != nil {
		return err
	}
	changed := repo.domainMemory.advanceThreadRevisions(before)
	items, err := repo.domainMemory.rootThreadInventoryLocked()
	if err != nil {
		return err
	}
	if err := attachRootThreadInventoryProjectionFingerprints(items); err != nil {
		return err
	}
	repo.rootInventoryItems = cloneRootThreadInventoryItems(items)
	repo.domainDirty = true
	if changed {
		repo.signalChange()
	}
	return nil
}

// AdmitTurn is intentionally memory-first. The runtime request ledger owns
// the stable logical identity and recovery replay; the full domain checkpoint
// is deferred until a semantic barrier such as effect intent or turn finish.
func (repo *BackendRepo) AdmitTurn(ctx context.Context, request AdmitTurnRequest) (result AdmitTurnResult, err error) {
	err = repo.updateMemory(ctx, func(memory *MemoryRepo) error {
		result, err = memory.AdmitTurn(ctx, request)
		return err
	})
	return result, err
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
	var committedInventory []RootThreadInventoryItem
	var committedInventoryEncoded []byte
	var committedMemory *MemoryRepo
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		var memory *MemoryRepo
		if repo.domainMemory != nil {
			encoded, encodeErr := repo.domainMemory.EncodeMemoryState()
			if encodeErr != nil {
				return encodeErr
			}
			decoded, decodeErr := DecodeMemoryState(encoded, repo.now)
			if decodeErr != nil {
				return decodeErr
			}
			memory = decoded
		} else {
			loaded, found, _, loadErr := repo.load(tx)
			if loadErr != nil {
				return loadErr
			}
			if !found {
				return errors.New("session-tree state is missing")
			}
			memory = loaded
		}
		before := memory.revisionFacts()
		if err := mutate(memory, tx); err != nil {
			return err
		}
		changed = memory.advanceThreadRevisions(before)
		var saveErr error
		committedInventoryEncoded, saveErr = repo.save(tx, memory)
		if saveErr != nil {
			return saveErr
		}
		var inventoryErr error
		committedInventory, inventoryErr = memory.rootThreadInventoryLocked()
		if inventoryErr != nil {
			return inventoryErr
		}
		if err := attachRootThreadInventoryProjectionFingerprints(committedInventory); err != nil {
			return err
		}
		committedMemory = memory
		return nil
	})
	if err == nil {
		repo.rootInventoryEncoded = bytes.Clone(committedInventoryEncoded)
		repo.rootInventoryItems = cloneRootThreadInventoryItems(committedInventory)
		repo.domainMemory = committedMemory
		repo.domainDirty = false
		if viewErr := repo.backend.View(ctx, func(tx spi.ReadTx) error {
			encoded, getErr := tx.Get(backendDomainNamespace, backendStateKey)
			if getErr != nil {
				return getErr
			}
			repo.domainEncoded = bytes.Clone(encoded)
			return nil
		}); viewErr != nil {
			err = viewErr
		}
	}
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
