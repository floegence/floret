package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const backendDomainNamespace = "floret.domain"

var backendStateKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))

// BackendRepo owns the validated in-memory session tree and commits each
// affected durable record inside one serializable Backend transaction.
type BackendRepo struct {
	backend                              spi.Backend
	now                                  func() time.Time
	mu                                   sync.Mutex
	domainMemory                         *MemoryRepo
	startupProgress                      StartupProgress
	startupPhase                         StartupPhase
	requiresPersistedStartupVerification bool
}

// StartupPhase identifies product-neutral work performed while opening the
// canonical session-tree authority.
type StartupPhase string

const (
	StartupPhaseMigrating StartupPhase = "migrating"
	StartupPhaseVerifying StartupPhase = "verifying"
)

// StartupProgress observes startup phase changes synchronously.
type StartupProgress func(StartupPhase)

type backendDomainSource uint8

const (
	backendDomainSourceFresh backendDomainSource = iota
	backendDomainSourceV8
	backendDomainSourceV7
	backendDomainSourceV6
	backendDomainSourceLegacy
)

// NewBackendRepo initializes or validates the canonical session-tree state.
func NewBackendRepo(ctx context.Context, backend spi.Backend, now func() time.Time) (*BackendRepo, error) {
	if backend == nil || now == nil {
		return nil, errors.New("backend repo requires backend and clock")
	}
	var repo *BackendRepo
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		var err error
		repo, err = NewBackendRepoInTransaction(ctx, backend, tx, now, nil)
		if err != nil {
			return err
		}
		return repo.VerifyCurrentStateInTransaction(ctx, tx)
	}); err != nil {
		return nil, err
	}
	return repo, nil
}

// NewBackendRepoInTransaction initializes and fully validates the canonical
// session-tree state using the caller's startup transaction. The returned
// repository retains backend for ordinary operations after that transaction
// commits.
func NewBackendRepoInTransaction(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time, progress StartupProgress) (*BackendRepo, error) {
	if ctx == nil || backend == nil || tx == nil || now == nil {
		return nil, errors.New("backend repo transaction requires context, backend, transaction, and clock")
	}
	repo := &BackendRepo{backend: backend, now: now, startupProgress: progress}
	source, err := detectBackendDomainSource(ctx, tx)
	if err != nil {
		return nil, err
	}
	if source == backendDomainSourceV8 {
		repo.reportStartupPhase(StartupPhaseVerifying)
		memory, found, err := loadBackendDomainV8(ctx, tx, now)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("detected session-tree v8 authority is missing"))
		}
		if hasUnknownEffectTurns(memory) {
			before := memory
			memory = cloneMemoryRepoForBackendUpdate(before)
			if err := convergeUnknownEffectTurns(ctx, memory); err != nil {
				return nil, err
			}
			changed, err := persistBackendDomainV8Changes(tx, before, memory)
			if err != nil {
				return nil, err
			}
			repo.requiresPersistedStartupVerification = changed
		}
		repo.domainMemory = memory
		return repo, nil
	}
	if source == backendDomainSourceV7 {
		repo.reportStartupPhase(StartupPhaseMigrating)
		memory, found, err := loadBackendDomainV7(ctx, tx, now)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("detected session-tree v7 authority is missing"))
		}
		if err := migrateBackendDomainV7ToV8(ctx, memory); err != nil {
			return nil, err
		}
		if err := saveCompleteBackendDomainV8(tx, memory); err != nil {
			return nil, err
		}
		if err := deleteAllBackendDomainV7(ctx, tx); err != nil {
			return nil, err
		}
		repo.domainMemory = memory
		repo.requiresPersistedStartupVerification = true
		return repo, nil
	}
	if source == backendDomainSourceV6 {
		repo.reportStartupPhase(StartupPhaseMigrating)
		memory, found, err := loadBackendDomainV6(ctx, tx, now)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("detected session-tree v6 authority is missing"))
		}
		if err := migrateBackendDomainV6ToV7(ctx, memory); err != nil {
			return nil, err
		}
		if err := migrateBackendDomainV7ToV8(ctx, memory); err != nil {
			return nil, err
		}
		if err := saveCompleteBackendDomainV8(tx, memory); err != nil {
			return nil, err
		}
		if err := deleteAllBackendDomainV6(ctx, tx); err != nil {
			return nil, err
		}
		repo.domainMemory = memory
		repo.requiresPersistedStartupVerification = true
		return repo, nil
	}
	legacyFound := source == backendDomainSourceLegacy
	var memory *MemoryRepo
	if legacyFound {
		repo.reportStartupPhase(StartupPhaseMigrating)
		var migrated bool
		memory, legacyFound, migrated, err = repo.load(tx)
		if err != nil {
			return nil, err
		}
		if !legacyFound {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("legacy session-tree fragments are missing canonical state"))
		}
		records, scanErr := scanBackendDomainJournal(ctx, tx)
		if scanErr != nil {
			return nil, scanErr
		}
		if migrated && len(records) != 0 {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("legacy domain migration cannot replay a newer recovery journal"))
		}
		if !migrated {
			memory, _, err = replayBackendDomainJournal(ctx, tx, memory, repo.now)
			if err != nil {
				return nil, err
			}
		}
		if !migrated {
			repaired, repairErr := repairLegacyUTF8EntryProjections(memory)
			if repairErr != nil {
				return nil, repairErr
			}
			if repaired {
				persistedInventory, readErr := tx.Get(backendDomainNamespace, backendRootThreadInventoryKey)
				if readErr != nil {
					return nil, errors.Join(ErrAuthorityCorrupt, readErr)
				}
				if err := verifyLegacyUTF8RootThreadInventory(persistedInventory, memory); err != nil {
					return nil, err
				}
			} else if err := repo.verifyRootThreadInventory(tx, memory); err != nil {
				return nil, err
			}
		}
	} else {
		repo.reportStartupPhase(StartupPhaseVerifying)
		memory = newMemoryRepo(now)
	}
	if err := migrateBackendDomainV6ToV7(ctx, memory); err != nil {
		return nil, err
	}
	if err := migrateBackendDomainV7ToV8(ctx, memory); err != nil {
		return nil, err
	}
	if err := saveCompleteBackendDomainV8(tx, memory); err != nil {
		return nil, err
	}
	if legacyFound {
		if err := deleteLegacyBackendDomain(ctx, tx); err != nil {
			return nil, err
		}
	}
	repo.domainMemory = memory
	repo.requiresPersistedStartupVerification = true
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
	if !repo.requiresPersistedStartupVerification {
		return nil
	}
	repo.reportStartupPhase(StartupPhaseVerifying)
	memory, found, err := loadBackendDomainV8(ctx, tx, repo.now)
	if err != nil {
		return err
	}
	if !found {
		return errors.Join(ErrAuthorityCorrupt, errors.New("session-tree state is not current after migration"))
	}
	if err := rejectPreV8BackendDomain(ctx, tx); err != nil {
		return err
	}
	if err := validateBackendDomainV8Memory(memory); err != nil {
		return err
	}
	repo.requiresPersistedStartupVerification = false
	return nil
}

func (repo *BackendRepo) reportStartupPhase(phase StartupPhase) {
	if repo.startupPhase == phase {
		return
	}
	repo.startupPhase = phase
	if repo.startupProgress != nil {
		repo.startupProgress(phase)
	}
}

func detectBackendDomainSource(ctx context.Context, tx spi.ReadTx) (backendDomainSource, error) {
	if err := ctx.Err(); err != nil {
		return backendDomainSourceFresh, err
	}
	present := make([]backendDomainSource, 0, 4)
	for _, candidate := range []struct {
		source    backendDomainSource
		namespace string
	}{
		{source: backendDomainSourceV8, namespace: backendDomainV8Namespace},
		{source: backendDomainSourceV7, namespace: backendDomainV7Namespace},
		{source: backendDomainSourceV6, namespace: backendDomainV6Namespace},
	} {
		found, err := backendNamespaceHasRecords(tx, candidate.namespace)
		if err != nil {
			return backendDomainSourceFresh, err
		}
		if found {
			present = append(present, candidate.source)
		}
	}
	legacy, err := legacyBackendDomainHasRecords(tx)
	if err != nil {
		return backendDomainSourceFresh, err
	}
	if legacy {
		present = append(present, backendDomainSourceLegacy)
	}
	if len(present) > 1 {
		return backendDomainSourceFresh, errors.Join(ErrAuthorityCorrupt, errors.New("session-tree store contains mixed domain formats"))
	}
	if len(present) == 0 {
		return backendDomainSourceFresh, nil
	}
	return present[0], nil
}

func backendNamespaceHasRecords(tx spi.ReadTx, namespace string) (bool, error) {
	page, err := tx.Scan(spi.ScanRequest{Namespace: namespace, Limit: 1})
	if err != nil {
		return false, err
	}
	return len(page.Records) != 0, nil
}

func legacyBackendDomainHasRecords(tx spi.ReadTx) (bool, error) {
	for _, key := range [][]byte{backendStateKey, backendRootThreadInventoryKey} {
		if _, err := tx.Get(backendDomainNamespace, key); err == nil {
			return true, nil
		} else if !errors.Is(err, spi.ErrNotFound) {
			return false, err
		}
	}
	return backendNamespaceHasRecords(tx, backendDomainJournalNamespace)
}

func rejectPreV8BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV7(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v7 domain records"))
	}
	return rejectLegacyBackendDomain(ctx, tx)
}

func rejectLegacyBackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV6(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v6 domain records"))
	}
	return rejectMonolithicBackendDomain(ctx, tx)
}

func rejectMonolithicBackendDomain(ctx context.Context, tx spi.ReadTx) error {
	for _, key := range [][]byte{backendStateKey, backendRootThreadInventoryKey} {
		if _, err := tx.Get(backendDomainNamespace, key); err == nil {
			return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains legacy domain records"))
		} else if !errors.Is(err, spi.ErrNotFound) {
			return err
		}
	}
	records, err := scanBackendDomainJournal(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains legacy recovery journal"))
	}
	return nil
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

// ViewDomain executes one read against the current validated in-memory
// authority. The callback must be observational only; durable mutations belong
// in UpdateDomain.
func (repo *BackendRepo) ViewDomain(ctx context.Context, read func(*MemoryRepo, spi.ReadTx) error) error {
	if repo == nil || ctx == nil || read == nil {
		return errors.New("backend domain view requires context, repository, and callback")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.domainMemory == nil {
		return errors.New("session-tree state is missing")
	}
	return read(repo.domainMemory, nil)
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
		if repo.domainMemory == nil {
			return errors.New("session-tree state is missing")
		}
		return read(repo.domainMemory, tx)
	})
}

// Checkpoint joins any ancillary durable records to an explicit transaction.
// Session-tree mutations are already durable at this boundary.
func (repo *BackendRepo) Checkpoint(ctx context.Context) error {
	if repo == nil {
		return errors.New("backend repository is required")
	}
	return repo.CheckpointDomain(ctx, nil)
}

// CheckpointDomain commits additional durable records after all preceding
// session-tree record mutations.
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
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		if checkpoint != nil {
			return checkpoint(tx)
		}
		return nil
	})
	return err
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
	return repo.updateDomain(ctx, mutate)
}

// CheckpointDomainUpdate applies one mutation at a semantic checkpoint. In v7
// it shares the same affected-record transaction as every other domain update.
func (repo *BackendRepo) CheckpointDomainUpdate(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	return repo.updateDomain(ctx, mutate)
}

func (repo *BackendRepo) updateDomain(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	if repo == nil || mutate == nil {
		return errors.New("backend domain mutation requires repository and callback")
	}
	if ctx == nil {
		return errors.New("backend domain mutation context is required")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	var committedMemory *MemoryRepo
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		if repo.domainMemory == nil {
			return errors.New("session-tree state is missing")
		}
		memory := cloneMemoryRepoForBackendUpdate(repo.domainMemory)
		if err := mutate(memory, tx); err != nil {
			return err
		}
		if err := validateBackendDomainV7Memory(memory); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if _, err := persistBackendDomainV8Changes(tx, repo.domainMemory, memory); err != nil {
			return err
		}
		committedMemory = memory
		return nil
	})
	if err == nil {
		repo.domainMemory = committedMemory
	}
	return err
}
