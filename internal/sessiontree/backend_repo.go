package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/v5/internal/storagecodec"
	"github.com/floegence/floret/v5/storage/spi"
)

const backendDomainNamespace = "floret.domain"

var backendStateKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))

// BackendRepo executes the canonical session-tree semantics inside Backend
// snapshot and serializable transactions.
type BackendRepo struct {
	backend spi.Backend
	now     func() time.Time
	mu      sync.Mutex
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
	// journalBase is the exact memory-state payload represented by the latest
	// durable checkpoint plus all committed journal frames. Memory-only live
	// mutations intentionally do not advance it.
	journalBase  []byte
	domainDirty  bool
	journalSeq   uint64
	journalBytes int64
}

// NewBackendRepo initializes or validates the canonical session-tree state.
func NewBackendRepo(ctx context.Context, backend spi.Backend, now func() time.Time) (*BackendRepo, error) {
	if backend == nil || now == nil {
		return nil, errors.New("backend repo requires backend and clock")
	}
	var repo *BackendRepo
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		var err error
		repo, err = NewBackendRepoInTransaction(ctx, backend, tx, now)
		return err
	}); err != nil {
		return nil, err
	}
	return repo, nil
}

// NewBackendRepoInTransaction initializes and fully validates the canonical
// session-tree state using the caller's startup transaction. The returned
// repository retains backend for ordinary operations after that transaction
// commits.
func NewBackendRepoInTransaction(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time) (*BackendRepo, error) {
	if ctx == nil || backend == nil || tx == nil || now == nil {
		return nil, errors.New("backend repo transaction requires context, backend, transaction, and clock")
	}
	repo := &BackendRepo{backend: backend, now: now}
	var committedInventory []RootThreadInventoryItem
	var committedInventoryEncoded []byte
	var committedMemory *MemoryRepo
	var committedJournal backendDomainJournalUsage
	if err := func() error {
		memory, found, migrated, err := repo.load(tx)
		if err != nil {
			return err
		}
		if !found {
			memory = newMemoryRepo(now)
			_, committedInventoryEncoded, err = repo.save(tx, memory)
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
		if migrated {
			if records, scanErr := scanBackendDomainJournal(ctx, tx); scanErr != nil {
				return scanErr
			} else if len(records) != 0 {
				return errors.Join(ErrAuthorityCorrupt, errors.New("domain migration cannot replay a newer recovery journal"))
			}
			_, committedInventoryEncoded, err = repo.save(tx, memory)
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
		memory, committedJournal, err = replayBackendDomainJournal(ctx, tx, memory, repo.now)
		if err != nil {
			return err
		}
		repaired, err := repairLegacyUTF8EntryProjections(memory)
		if err != nil {
			return err
		}
		if repaired {
			persistedInventory, readErr := tx.Get(backendDomainNamespace, backendRootThreadInventoryKey)
			if readErr != nil {
				return errors.Join(ErrAuthorityCorrupt, readErr)
			}
			if err := verifyLegacyUTF8RootThreadInventory(persistedInventory, memory); err != nil {
				return err
			}
			_, committedInventoryEncoded, err = repo.save(tx, memory)
			if err != nil {
				return err
			}
			if err := clearBackendDomainJournal(ctx, tx); err != nil {
				return err
			}
			committedJournal = backendDomainJournalUsage{}
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
	}(); err != nil {
		return nil, err
	}
	repo.rootInventoryEncoded = bytes.Clone(committedInventoryEncoded)
	repo.rootInventoryItems = cloneRootThreadInventoryItems(committedInventory)
	repo.domainMemory = committedMemory
	journalBase, err := committedMemory.EncodeMemoryState()
	if err != nil {
		return nil, err
	}
	repo.journalBase = journalBase
	encoded, err := tx.Get(backendDomainNamespace, backendStateKey)
	if err != nil {
		return nil, err
	}
	repo.domainEncoded = bytes.Clone(encoded)
	repo.domainDirty = committedJournal.Sequence > 0
	repo.journalSeq = committedJournal.Sequence
	repo.journalBytes = committedJournal.Bytes
	return repo, nil
}

// VerifyCurrentStateInTransaction verifies the final current-schema domain
// invariant without rewriting canonical bytes.
func (repo *BackendRepo) VerifyCurrentStateInTransaction(ctx context.Context, tx spi.ReadTx) error {
	if repo == nil || tx == nil || ctx == nil {
		return errors.New("backend repository verification requires context, repository, and transaction")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	checkpoint, found, migrated, err := repo.load(tx)
	if err != nil {
		return err
	}
	if !found || migrated {
		return errors.Join(ErrAuthorityCorrupt, errors.New("session-tree state is not current after migration"))
	}
	// Startup may be reopening a store whose latest committed mutations are
	// still represented by the recovery journal. Verify the same replayed
	// authority that NewBackendRepoInTransaction hydrated, rather than comparing
	// the stale checkpoint bytes with the current inventory projection.
	memory, _, err := replayBackendDomainJournal(ctx, tx, checkpoint, repo.now)
	if err != nil {
		return err
	}
	return repo.verifyRootThreadInventory(tx, memory)
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

func (repo *BackendRepo) save(tx spi.WriteTx, memory *MemoryRepo) ([]byte, []byte, error) {
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		return nil, nil, err
	}
	encoded, err := storagecodec.EncodeEnvelope("sessiontree", payload)
	if err != nil {
		return nil, nil, err
	}
	inventory, err := encodeRootThreadInventory(memory)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Put(backendDomainNamespace, backendStateKey, encoded); err != nil {
		return nil, nil, err
	}
	if err := tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, inventory); err != nil {
		return nil, nil, err
	}
	return encoded, inventory, nil
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
	if repo.domainDirty && repo.domainMemory != nil {
		return read(repo.domainMemory, nil)
	}
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
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

// ViewDomainWithRecords joins the live memory authority with ancillary
// durable records in one read transaction. Hot thread projections should use
// ViewDomain so they never enter SQL while the process authority is live.
func (repo *BackendRepo) ViewDomainWithRecords(ctx context.Context, read func(*MemoryRepo, spi.ReadTx) error) error {
	if repo == nil || read == nil {
		return errors.New("backend domain record view requires repository and callback")
	}
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
	return repo.CheckpointDomain(ctx, nil)
}

// CheckpointDomain folds the current checkpoint plus recovery journal into one
// new checkpoint. Additional durable records may join the same transaction.
func (repo *BackendRepo) CheckpointDomain(ctx context.Context, checkpoint func(spi.WriteTx) error) error {
	if repo == nil {
		return errors.New("backend repository is required")
	}
	if ctx == nil {
		return errors.New("backend checkpoint context is required")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.domainMemory == nil {
		return errors.New("session-tree state is missing")
	}
	var committedInventory []RootThreadInventoryItem
	var committedInventoryEncoded []byte
	var committedDomainEncoded []byte
	var committedJournalBase []byte
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		if checkpoint != nil {
			if err := checkpoint(tx); err != nil {
				return err
			}
		}
		var encodeErr error
		committedJournalBase, encodeErr = repo.domainMemory.EncodeMemoryState()
		if encodeErr != nil {
			return encodeErr
		}
		var saveErr error
		committedDomainEncoded, committedInventoryEncoded, saveErr = repo.save(tx, repo.domainMemory)
		if saveErr != nil {
			return saveErr
		}
		committedInventory, saveErr = repo.domainMemory.rootThreadInventoryLocked()
		if saveErr != nil {
			return saveErr
		}
		if err := attachRootThreadInventoryProjectionFingerprints(committedInventory); err != nil {
			return err
		}
		return clearBackendDomainJournal(ctx, tx)
	})
	if err != nil {
		return err
	}
	repo.rootInventoryEncoded = bytes.Clone(committedInventoryEncoded)
	repo.rootInventoryItems = cloneRootThreadInventoryItems(committedInventory)
	repo.domainDirty = false
	repo.journalSeq = 0
	repo.journalBytes = 0
	repo.journalBase = bytes.Clone(committedJournalBase)
	repo.domainEncoded = bytes.Clone(committedDomainEncoded)
	return nil
}

func (repo *BackendRepo) update(ctx context.Context, mutate func(*MemoryRepo) error) error {
	return repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error { return mutate(memory) })
}

// AcceptTurn commits the canonical user/queue boundary before provider
// dispatch. It is a normal domain transaction, not a memory-only receipt.
func (repo *BackendRepo) AcceptTurn(ctx context.Context, request AcceptTurnRequest) (result AcceptTurnResult, err error) {
	err = repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error {
		result, err = memory.AcceptTurn(ctx, request)
		return err
	})
	return result, err
}

func (repo *BackendRepo) ReadAcceptedTurn(ctx context.Context, threadID, turnID, runID string) (AcceptTurnResult, bool, error) {
	var result AcceptTurnResult
	var found bool
	err := repo.view(ctx, func(memory *MemoryRepo) error {
		var readErr error
		result, found, readErr = memory.ReadAcceptedTurn(ctx, threadID, turnID, runID)
		return readErr
	})
	return result, found, err
}

// UpdateDomain executes one session-tree mutation and related domain writes in
// the same serializable backend transaction.
func (repo *BackendRepo) UpdateDomain(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	return repo.updateDomain(ctx, mutate, false)
}

// CheckpointDomainUpdate applies one mutation and folds all recovery frames
// into the canonical checkpoint in the same backend transaction.
func (repo *BackendRepo) CheckpointDomainUpdate(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	return repo.updateDomain(ctx, mutate, true)
}

func (repo *BackendRepo) updateDomain(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error, forceCheckpoint bool) error {
	if repo == nil || mutate == nil {
		return errors.New("backend domain mutation requires repository and callback")
	}
	if ctx == nil {
		return errors.New("backend domain mutation context is required")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	changed := false
	journaled := false
	var committedInventory []RootThreadInventoryItem
	var committedInventoryEncoded []byte
	var committedMemory *MemoryRepo
	var committedSequence uint64
	var committedJournalBytes int64
	var committedDomainEncoded []byte
	var committedJournalBase []byte
	checkpointed := false
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		if repo.domainMemory == nil {
			return errors.New("session-tree state is missing")
		}
		beforeEncoded, encodeErr := repo.domainMemory.EncodeMemoryState()
		if encodeErr != nil {
			return encodeErr
		}
		memory, decodeErr := DecodeMemoryState(beforeEncoded, repo.now)
		if decodeErr != nil {
			return decodeErr
		}
		if err := mutate(memory, tx); err != nil {
			return err
		}
		afterEncoded, encodeErr := memory.EncodeMemoryState()
		if encodeErr != nil {
			return encodeErr
		}
		journalBase := repo.journalBase
		if len(journalBase) == 0 {
			journalBase = beforeEncoded
		}
		frame, domainChanged, frameErr := buildBackendDomainJournalFrame(repo.journalSeq+1, journalBase, afterEncoded)
		if frameErr != nil {
			return frameErr
		}
		if domainChanged {
			changed = true
			encodedFrame, marshalErr := encodeBackendDomainJournalFrame(frame)
			if marshalErr != nil {
				return marshalErr
			}
			checkpointed = forceCheckpoint || (backendDomainJournalUsage{Sequence: repo.journalSeq, Bytes: repo.journalBytes}).requiresCheckpoint()
			if checkpointed {
				committedDomainEncoded, committedInventoryEncoded, encodeErr = repo.save(tx, memory)
				if encodeErr != nil {
					return encodeErr
				}
				if clearErr := clearBackendDomainJournal(ctx, tx); clearErr != nil {
					return clearErr
				}
			} else {
				if putErr := tx.Put(backendDomainJournalNamespace, backendJournalKey(frame.Sequence), encodedFrame); putErr != nil {
					return putErr
				}
				committedInventoryEncoded, encodeErr = encodeRootThreadInventory(memory)
				if encodeErr != nil {
					return encodeErr
				}
				if putErr := tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, committedInventoryEncoded); putErr != nil {
					return putErr
				}
				committedSequence = frame.Sequence
				committedJournalBytes = repo.journalBytes + int64(len(encodedFrame))
			}
			committedInventory, encodeErr = memory.rootThreadInventoryLocked()
			if encodeErr != nil {
				return encodeErr
			}
			if fingerprintErr := attachRootThreadInventoryProjectionFingerprints(committedInventory); fingerprintErr != nil {
				return fingerprintErr
			}
			journaled = true
		}
		committedJournalBase = bytes.Clone(afterEncoded)
		committedMemory = memory
		return nil
	})
	if err == nil {
		if journaled {
			repo.rootInventoryEncoded = bytes.Clone(committedInventoryEncoded)
			repo.rootInventoryItems = cloneRootThreadInventoryItems(committedInventory)
			repo.domainDirty = !checkpointed
			repo.journalSeq = committedSequence
			repo.journalBytes = committedJournalBytes
			repo.journalBase = bytes.Clone(committedJournalBase)
			if checkpointed {
				repo.domainEncoded = bytes.Clone(committedDomainEncoded)
			}
		}
		repo.domainMemory = committedMemory
	}
	_ = changed
	return err
}
