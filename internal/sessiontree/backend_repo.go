package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const backendDomainNamespace = "floret.domain"

var backendStateKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))

// BackendRepo owns the validated in-memory session tree and commits each
// affected durable record inside one serializable Backend transaction.
type BackendRepo struct {
	backend spi.Backend
	now     func() time.Time
	format  backendDomainFormat
	// migrationSource identifies an older canonical format retained as the
	// authoritative source while v32 records are published per thread.
	migrationSource                      *backendDomainFormat
	waitForReady                         func(context.Context) error
	delegate                             atomic.Pointer[BackendRepo]
	mu                                   sync.Mutex
	migrationGate                        migrationGate
	domainMemory                         *MemoryRepo
	threadScoped                         bool
	parentEvents                         bool
	threadMemoryID                       string
	threadMemory                         *MemoryRepo
	verifiedThreadLeaves                 map[string]string
	queryPreparationCursor               []byte
	threadQuerySnapshots                 *querySnapshots
	queryThreads                         map[string]*queryThread
	parentEventObserver                  func([]string)
	startupProgress                      StartupProgress
	startupPhase                         StartupPhase
	requiresPersistedStartupVerification bool
}

// NewDeferredBackendRepo returns an uninitialized repository whose operations
// wait for the caller-owned startup coordinator before touching domain state.
// It is used only by runtime.Open while canonical migration runs in the
// background.
func NewDeferredBackendRepo(backend spi.Backend, now func() time.Time, wait func(context.Context) error) *BackendRepo {
	return &BackendRepo{backend: backend, now: now, waitForReady: wait}
}

// NewCurrentBackendRepo returns a thread-scoped owner for a store whose v31
// format has already been identified by a bounded manifest read. Thread data
// remains validated when it is first prepared or used.
func NewCurrentBackendRepo(backend spi.Backend, now func() time.Time, parentEvents bool) (*BackendRepo, error) {
	return NewBackendRepoForFormat(backend, now, parentEvents, backendDomainV31Format)
}

func NewBackendRepoForFormat(backend spi.Backend, now func() time.Time, parentEvents bool, format backendDomainFormat) (*BackendRepo, error) {
	if backend == nil || now == nil {
		return nil, errors.New("current backend repo requires backend and clock")
	}
	if format.version < backendDomainV31Version || format.validate == nil {
		return nil, errors.New("current backend repo requires a supported current domain format")
	}
	return &BackendRepo{
		backend: backend, now: now, format: format, parentEvents: parentEvents, threadScoped: true,
		verifiedThreadLeaves: map[string]string{}, threadQuerySnapshots: &querySnapshots{snapshots: map[string]*querySnapshot{}},
	}, nil
}

func (repo *BackendRepo) domainFormat() backendDomainFormat {
	if repo == nil || repo.format.version == 0 {
		return backendDomainV31Format
	}
	return repo.format
}

// IsCurrentBackendRepoInTransaction performs bounded format recognition. It
// does not scan entries or validate the store-wide thread inventory.
func IsCurrentBackendRepoInTransaction(ctx context.Context, tx spi.ReadTx) (bool, error) {
	source, err := detectBackendDomainSource(ctx, tx)
	return source >= backendDomainSourceV27 && source <= backendDomainSourceV32, err
}

func NewCurrentBackendRepoInTransaction(ctx context.Context, backend spi.Backend, tx spi.ReadTx, now func() time.Time, parentEvents bool) (*BackendRepo, bool, error) {
	if ctx == nil || backend == nil || tx == nil || now == nil {
		return nil, false, errors.New("current backend repo requires context, backend, transaction, and clock")
	}
	source, err := detectBackendDomainSource(ctx, tx)
	if err != nil || (source < backendDomainSourceV27 || source > backendDomainSourceV32) {
		return nil, false, err
	}
	format := backendDomainV32Format
	var migrationSource *backendDomainFormat
	if source == backendDomainSourceV32 {
		manifest, err := readV32Manifest(tx)
		if err != nil {
			return nil, false, err
		}
		if manifest.SourceVersion != 0 {
			retained, err := migrationSourceFormat(manifest.SourceVersion)
			if err != nil {
				return nil, false, err
			}
			migrationSource = &retained
			if _, found, err := readBackendDomainRecord(tx, retained, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0); err != nil || !found {
				return nil, false, errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	} else {
		writable, ok := tx.(spi.WriteTx)
		if !ok {
			return nil, false, errors.New("starting thread migration requires a write transaction")
		}
		retained, err := migrationSourceFormat(backendDomainV27Version + int(source-backendDomainSourceV27))
		if err != nil {
			return nil, false, err
		}
		record, found, err := readBackendDomainRecord(tx, retained, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0)
		if err != nil || !found {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		var manifest backendDomainManifest
		if err := decodeBackendDomainValue(retained, record, &manifest); err != nil || manifest.Version != retained.version || manifest.Sequence < 0 || manifest.SourceVersion != 0 || manifest.MigrationPhase != "" || len(manifest.MigrationCursor) != 0 {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := putBackendDomainRecordContext(ctx, writable, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: backendDomainV32Version, Sequence: manifest.Sequence, SourceVersion: retained.version}); err != nil {
			return nil, false, err
		}
		migrationSource = &retained
	}
	repo, err := NewBackendRepoForFormat(backend, now, parentEvents, format)
	if repo != nil {
		repo.migrationSource = migrationSource
	}
	return repo, err == nil, err
}

func IsFreshBackendRepoInTransaction(ctx context.Context, tx spi.ReadTx) (bool, error) {
	source, err := detectBackendDomainSource(ctx, tx)
	if err != nil || source != backendDomainSourceFresh {
		return false, err
	}
	canonicalBodiesFound, err := backendNamespaceHasRecords(tx, canonicalBodyNamespace)
	if err != nil {
		return false, err
	}
	return !canonicalBodiesFound, nil
}

// InstallDelegate publishes a fully initialized repository behind the stable
// deferred repository pointer held by runtime services.
func (repo *BackendRepo) InstallDelegate(delegate *BackendRepo) error {
	if repo == nil || delegate == nil || repo == delegate {
		return errors.New("deferred repository requires a distinct initialized delegate")
	}
	repo.delegate.Store(delegate)
	if repo.parentEventObserver != nil {
		delegate.SetParentEventObserver(repo.parentEventObserver)
	}
	return nil
}

// StartupPhase identifies product-neutral work performed while opening the
// canonical session-tree authority.
type StartupPhase string

const (
	StartupPhaseMigrating StartupPhase = "migrating"
	StartupPhaseVerifying StartupPhase = "verifying"
	StartupPhaseIndexing  StartupPhase = "indexing"
)

// StartupProgress observes startup phase changes synchronously.
type StartupProgress func(StartupPhase)

type backendDomainSource uint8

const (
	backendDomainSourceFresh backendDomainSource = iota
	backendDomainSourceV15
	backendDomainSourceV16
	backendDomainSourceV17
	backendDomainSourceV18
	backendDomainSourceV19
	backendDomainSourceV20
	backendDomainSourceV21
	backendDomainSourceV22
	backendDomainSourceV23
	backendDomainSourceV24
	backendDomainSourceV25
	backendDomainSourceV26
	backendDomainSourceV27
	backendDomainSourceV28
	backendDomainSourceV29
	backendDomainSourceV30
	backendDomainSourceV31
	backendDomainSourceV32
	backendDomainSourceV14
	backendDomainSourceV13
	backendDomainSourceV12
	backendDomainSourceV11
	backendDomainSourceV10
	backendDomainSourceV9
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
		if _, err := storagebridge.PrepareStartup(tx, nil); err != nil {
			return err
		}
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

// newBackendRepoThroughV29 preserves every released migration through schema 29.
func newBackendRepoThroughV29(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time, progress StartupProgress) (*BackendRepo, error) {
	if ctx == nil || backend == nil || tx == nil || now == nil {
		return nil, errors.New("backend repo transaction requires context, backend, transaction, and clock")
	}
	repo := &BackendRepo{backend: backend, now: now, startupProgress: progress}
	source, err := detectBackendDomainSource(ctx, tx)
	if err != nil {
		return nil, err
	}
	if source == backendDomainSourceV29 {
		repo.reportStartupPhase(StartupPhaseVerifying)
		memory, found, err := loadBackendDomainV29(ctx, tx, now)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("detected session-tree v29 authority is missing"))
		}
		memory.parentEvents = parentEventsEnabled(ctx)
		if hasUnknownEffectTurns(memory) {
			before := memory
			memory = cloneMemoryRepoForStartup(before)
			memory.parentEvents = parentEventsEnabled(ctx)
			if err := convergeUnknownEffectTurns(ctx, memory); err != nil {
				return nil, err
			}
			changed, err := persistBackendDomainV29Changes(tx, before, memory)
			if err != nil {
				return nil, err
			}
			repo.requiresPersistedStartupVerification = changed
		}
		memory.parentEvents = parentEventsEnabled(ctx)
		repo.domainMemory = memory
		return repo, nil
	}
	if lineage, found := historicalSegmentedLineage(source); found {
		repo.reportStartupPhase(StartupPhaseMigrating)
		memory, found, err := lineage.load(ctx, tx, now)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrAuthorityCorrupt
		}
		if err := migrateHistoricalMemoryThroughV29(ctx, memory, lineage.version); err != nil {
			return nil, err
		}
		if lineage.remove != nil {
			if err := lineage.remove(ctx, tx); err != nil {
				return nil, err
			}
		}
		if err := saveCompleteBackendDomainV29(tx, memory); err != nil {
			return nil, err
		}
		memory.parentEvents = parentEventsEnabled(ctx)
		repo.domainMemory, repo.requiresPersistedStartupVerification = memory, true
		return repo, nil
	}
	legacyFound := source == backendDomainSourceLegacy
	var memory *MemoryRepo
	if legacyFound {
		repo.reportStartupPhase(StartupPhaseMigrating)
		var migrated bool
		memory, legacyFound, migrated, err = repo.load(ctx, tx)
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
	from := 25
	if legacyFound {
		from = 6
	}
	if err := migrateHistoricalMemoryThroughV29(ctx, memory, from); err != nil {
		return nil, err
	}
	if err := saveCompleteBackendDomainV29(tx, memory); err != nil {
		return nil, err
	}
	if legacyFound {
		if err := deleteLegacyBackendDomain(ctx, tx); err != nil {
			return nil, err
		}
	}
	memory.parentEvents = parentEventsEnabled(ctx)
	repo.domainMemory = memory
	repo.requiresPersistedStartupVerification = true
	return repo, nil
}

// VerifyCurrentStateInTransaction verifies the final current-schema domain
// invariant without rewriting canonical bytes.
func (repo *BackendRepo) VerifyCurrentStateInTransaction(ctx context.Context, tx spi.ReadTx) error {
	if repo != nil && repo.domainMemory != nil {
		repo.domainMemory.bodyTx, repo.domainMemory.bodyContext = tx, ctx
		defer func() { repo.domainMemory.bodyTx, repo.domainMemory.bodyContext = nil, nil }()
	}
	if err := repo.verifyPersistedStateInTransaction(ctx, tx); err != nil {
		return err
	}
	if err := repo.prepareQueryStorage(ctx, tx, false); err != nil {
		return err
	}
	repo.domainMemory.compactBodiesLocked(repo.backend)
	return nil
}

// VerifyInspectionStateInTransaction retains canonical and projection checks
// without preparing or modifying derived source-locator metadata.
func (repo *BackendRepo) VerifyInspectionStateInTransaction(ctx context.Context, tx spi.ReadTx) error {
	if repo != nil && repo.domainMemory != nil {
		repo.domainMemory.bodyTx, repo.domainMemory.bodyContext = tx, ctx
		defer func() { repo.domainMemory.bodyTx, repo.domainMemory.bodyContext = nil, nil }()
	}
	if err := repo.verifyPersistedStateInTransaction(ctx, tx); err != nil {
		return err
	}
	if err := repo.prepareQueryStorage(ctx, tx, true); err != nil {
		return err
	}
	repo.domainMemory.compactBodiesLocked(repo.backend)
	return nil
}

func (repo *BackendRepo) verifyPersistedStateInTransaction(ctx context.Context, tx spi.ReadTx) error {
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
	memory, found, err := loadBackendDomain(ctx, tx, repo.now, repo.domainFormat())
	if err != nil {
		return err
	}
	if !found {
		return errors.Join(ErrAuthorityCorrupt, errors.New("session-tree state is not current after migration"))
	}
	if err := rejectPreV16BackendDomain(ctx, tx); err != nil {
		return err
	}
	memory.bodyTx, memory.bodyContext = tx, ctx
	defer func() { memory.bodyTx, memory.bodyContext = nil, nil }()
	if err := validateBackendDomainV29Memory(memory); err != nil {
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
		{source: backendDomainSourceV32, namespace: backendDomainV32Namespace},
		{source: backendDomainSourceV16, namespace: backendDomainV16Namespace},
		{source: backendDomainSourceV15, namespace: backendDomainV15Namespace},
		{source: backendDomainSourceV14, namespace: backendDomainV14Namespace},
		{source: backendDomainSourceV13, namespace: backendDomainV13Namespace},
		{source: backendDomainSourceV12, namespace: backendDomainV12Namespace},
		{source: backendDomainSourceV11, namespace: backendDomainV11Namespace},
		{source: backendDomainSourceV10, namespace: backendDomainV10Namespace},
		{source: backendDomainSourceV9, namespace: backendDomainV9Namespace},
		{source: backendDomainSourceV8, namespace: backendDomainV8Namespace},
		{source: backendDomainSourceV7, namespace: backendDomainV7Namespace},
		{source: backendDomainSourceV6, namespace: backendDomainV6Namespace},
	} {
		found, err := backendNamespaceHasRecords(tx, candidate.namespace)
		if err != nil {
			return backendDomainSourceFresh, err
		}
		if found {
			if candidate.source == backendDomainSourceV32 {
				raw, err := tx.Get(candidate.namespace, backendDomainKey(backendDomainRecordManifest))
				if err != nil {
					return backendDomainSourceFresh, err
				}
				if _, err := decodeBackendDomainRecord(backendDomainV32Format, raw); err != nil {
					return backendDomainSourceFresh, errors.Join(ErrAuthorityCorrupt, err)
				}
			}
			if candidate.source == backendDomainSourceV16 {
				raw, err := tx.Get(candidate.namespace, backendDomainKey(backendDomainRecordManifest))
				if err != nil {
					return backendDomainSourceFresh, err
				}
				if _, err := decodeBackendDomainRecord(backendDomainV17Format, raw); err == nil {
					candidate.source = backendDomainSourceV17
				}
				if _, err := decodeBackendDomainRecord(backendDomainV18Format, raw); err == nil {
					candidate.source = backendDomainSourceV18
				}
				if _, err := decodeBackendDomainRecord(backendDomainV19Format, raw); err == nil {
					candidate.source = backendDomainSourceV19
				}
				if _, err := decodeBackendDomainRecord(backendDomainV20Format, raw); err == nil {
					candidate.source = backendDomainSourceV20
				}
				if _, err := decodeBackendDomainRecord(backendDomainV21Format, raw); err == nil {
					candidate.source = backendDomainSourceV21
				}
				if _, err := decodeBackendDomainRecord(backendDomainV22Format, raw); err == nil {
					candidate.source = backendDomainSourceV22
				}
				if _, err := decodeBackendDomainRecord(backendDomainV23Format, raw); err == nil {
					candidate.source = backendDomainSourceV23
				}
				if _, err := decodeBackendDomainRecord(backendDomainV24Format, raw); err == nil {
					candidate.source = backendDomainSourceV24
				}
				if _, err := decodeBackendDomainRecord(backendDomainV25Format, raw); err == nil {
					candidate.source = backendDomainSourceV25
				}
				if _, err := decodeBackendDomainRecord(backendDomainV26Format, raw); err == nil {
					candidate.source = backendDomainSourceV26
				}
				if _, err := decodeBackendDomainRecord(backendDomainV27Format, raw); err == nil {
					candidate.source = backendDomainSourceV27
				}
				if _, err := decodeBackendDomainRecord(backendDomainV28Format, raw); err == nil {
					candidate.source = backendDomainSourceV28
				}
				if _, err := decodeBackendDomainRecord(backendDomainV29Format, raw); err == nil {
					candidate.source = backendDomainSourceV29
				}
				if _, err := decodeBackendDomainRecord(backendDomainV30Format, raw); err == nil {
					candidate.source = backendDomainSourceV30
				}
				if _, err := decodeBackendDomainRecord(backendDomainV31Format, raw); err == nil {
					candidate.source = backendDomainSourceV31
				}
			}
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
		// A v31 source plus a v32 manifest carrying source_version=31 is the
		// resumable per-thread migration state. The older namespace remains
		// authoritative until each thread publishes successfully.
		if len(present) == 2 && present[0] == backendDomainSourceV32 && (present[1] >= backendDomainSourceV27 && present[1] <= backendDomainSourceV31) {
			raw, readErr := tx.Get(backendDomainV32Namespace, backendDomainKey(backendDomainRecordManifest))
			if readErr == nil {
				record, decodeErr := decodeBackendDomainRecord(backendDomainV32Format, raw)
				if decodeErr == nil {
					var manifest backendDomainManifest
					if decodeErr = decodeBackendDomainValue(backendDomainV32Format, record, &manifest); decodeErr == nil && manifest.SourceVersion == backendDomainV27Version+int(present[1]-backendDomainSourceV27) {
						return backendDomainSourceV32, nil
					}
				}
			}
		}
		return backendDomainSourceFresh, errors.Join(ErrAuthorityCorrupt, errors.New("session-tree store contains mixed domain formats"))
	}
	if len(present) == 0 {
		return backendDomainSourceFresh, nil
	}
	return present[0], nil
}

func backendNamespaceHasRecords(tx spi.ReadTx, namespace string) (bool, error) {
	return storagecodec.NamespaceHasRecords(tx, namespace)
}

func legacyBackendDomainHasRecords(tx spi.ReadTx) (bool, error) {
	for _, key := range [][]byte{backendStateKey, backendRootThreadInventoryKey} {
		if found, err := storagecodec.RecordExists(tx, backendDomainNamespace, key); err != nil || found {
			return found, err
		}
	}
	return backendNamespaceHasRecords(tx, backendDomainJournalNamespace)
}

func rejectPreV14BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV13(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v13 domain records"))
	}
	return rejectPreV13BackendDomain(ctx, tx)
}

func rejectPreV13BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV12(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v12 domain records"))
	}
	return rejectPreV12BackendDomain(ctx, tx)
}

func rejectPreV12BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV11(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v11 domain records"))
	}
	return rejectPreV11BackendDomain(ctx, tx)
}

func rejectPreV11BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV10(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v10 domain records"))
	}
	return rejectPreV10BackendDomain(ctx, tx)
}

func rejectPreV10BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV9(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v9 domain records"))
	}
	return rejectPreV9BackendDomain(ctx, tx)
}

func rejectPreV9BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV8(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return errors.Join(ErrAuthorityCorrupt, errors.New("current session-tree store retains v8 domain records"))
	}
	return rejectPreV8BackendDomain(ctx, tx)
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

func (repo *BackendRepo) load(ctx context.Context, tx spi.ReadTx) (*MemoryRepo, bool, bool, error) {
	reader, err := storagecodec.OpenRecord(ctx, tx, backendDomainNamespace, backendStateKey)
	if errors.Is(err, spi.ErrNotFound) {
		return nil, false, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}
	var state memoryState
	err = storagecodec.DecodeEnvelopeStream(reader, "sessiontree", func(decoder *json.Decoder) error {
		var err error
		state, err = decodeMemoryStateValue(decoder)
		return err
	})
	if err != nil {
		return nil, false, false, err
	}
	memory, migrated, err := migrateDecodedMemoryState(state, repo.now)
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

func (repo *BackendRepo) viewThread(ctx context.Context, threadID string, read func(*MemoryRepo) error) error {
	if repo == nil {
		return errors.New("thread-scoped backend view requires repository")
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.viewThread(ctx, threadID, read)
	}
	if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil {
		return err
	}
	if !repo.threadScoped {
		return repo.view(ctx, read)
	}
	if ctx == nil || read == nil || strings.TrimSpace(threadID) == "" {
		return errors.New("thread-scoped backend view requires context, thread id, and callback")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		memory, err := repo.threadMemoryForTransaction(ctx, tx, threadID)
		if err != nil {
			return err
		}
		memory.querySnapshots = repo.threadQuerySnapshots
		if memory.querySnapshots == nil {
			memory.querySnapshots = &querySnapshots{snapshots: map[string]*querySnapshot{}}
		}
		memory.bodyTx, memory.bodyContext = tx, ctx
		defer func() {
			memory.bodyTx, memory.bodyContext = nil, nil
			repo.threadQuerySnapshots = memory.querySnapshots
		}()
		if err := repo.verifyThreadView(ctx, tx, memory, threadID); err != nil {
			return err
		}
		repo.threadMemoryID, repo.threadMemory = threadID, memory
		readErr := read(memory)
		return readErr
	})
	return err
}

func (repo *BackendRepo) parentEventsEnabled() bool {
	if repo.domainMemory != nil {
		return repo.domainMemory.parentEvents
	}
	return repo.parentEvents
}

func (repo *BackendRepo) updateThread(ctx context.Context, threadID string, mutate func(*MemoryRepo) error) error {
	if repo == nil {
		return errors.New("thread-scoped backend mutation requires repository")
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.updateThread(ctx, threadID, mutate)
	}
	if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil {
		return err
	}
	if !repo.threadScoped {
		return repo.update(ctx, mutate)
	}
	if ctx == nil || mutate == nil || strings.TrimSpace(threadID) == "" {
		return errors.New("thread-scoped backend mutation requires context, thread id, and callback")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.updateThreadInTransaction(ctx, threadID, mutate)
}

// EnableThreadScopedBackend drops the startup-wide journal snapshot. Runtime
// operations then hydrate one canonical thread at a time from backend records.
func (repo *BackendRepo) EnableThreadScopedBackend() {
	if repo == nil {
		return
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		delegate.EnableThreadScopedBackend()
		return
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.threadScoped {
		return
	}
	repo.threadScoped = true
	repo.verifiedThreadLeaves = make(map[string]string)
	if repo.domainMemory != nil {
		repo.parentEvents = repo.domainMemory.parentEvents
		repo.threadQuerySnapshots = repo.domainMemory.querySnapshots
		repo.domainMemory = nil
	}
	repo.threadMemoryID, repo.threadMemory = "", nil
}

// ViewDomain reads canonical metadata and hydrates requested immutable bodies
// in one backend snapshot. Durable mutations belong in UpdateDomain.
func (repo *BackendRepo) ViewDomain(ctx context.Context, read func(*MemoryRepo, spi.ReadTx) error) error {
	if repo == nil || ctx == nil || read == nil {
		return errors.New("backend domain view requires context, repository, and callback")
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ViewDomain(ctx, read)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.domainMemory == nil {
		if !repo.threadScoped {
			return errors.New("session-tree state is missing")
		}
		return repo.backend.View(ctx, func(tx spi.ReadTx) error {
			memory, found, err := loadBackendDomain(ctx, tx, repo.now, repo.domainFormat())
			if err != nil {
				return err
			}
			if !found {
				return ErrAuthorityCorrupt
			}
			memory.bodyBackend, memory.bodyTx, memory.bodyContext = repo.backend, tx, ctx
			memory.querySnapshots = repo.threadQuerySnapshots
			defer func() {
				memory.bodyTx, memory.bodyContext = nil, nil
				repo.threadQuerySnapshots = memory.querySnapshots
			}()
			return read(memory, tx)
		})
	}
	if repo.domainMemory.bodyBackend == nil {
		return read(repo.domainMemory, nil)
	}
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
		memory := repo.domainMemory
		memory.bodyTx, memory.bodyContext = tx, ctx
		defer func() { memory.bodyTx, memory.bodyContext = nil, nil }()
		return read(memory, tx)
	})
}

// ViewDomainWithRecords joins canonical metadata, requested bodies and ancillary
// durable records in one read transaction.
func (repo *BackendRepo) ViewDomainWithRecords(ctx context.Context, read func(*MemoryRepo, spi.ReadTx) error) error {
	if repo == nil || read == nil {
		return errors.New("backend domain record view requires repository and callback")
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ViewDomainWithRecords(ctx, read)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
		if repo.domainMemory == nil {
			if !repo.threadScoped {
				return errors.New("session-tree state is missing")
			}
			memory, found, err := loadBackendDomain(ctx, tx, repo.now, repo.domainFormat())
			if err != nil {
				return err
			}
			if !found {
				return ErrAuthorityCorrupt
			}
			memory.bodyBackend, memory.bodyTx, memory.bodyContext = repo.backend, tx, ctx
			memory.querySnapshots = repo.threadQuerySnapshots
			defer func() {
				memory.bodyTx, memory.bodyContext = nil, nil
				repo.threadQuerySnapshots = memory.querySnapshots
			}()
			return read(memory, tx)
		}
		memory := repo.domainMemory
		memory.bodyTx, memory.bodyContext = tx, ctx
		defer func() { memory.bodyTx, memory.bodyContext = nil, nil }()
		return read(memory, tx)
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
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.CheckpointDomain(ctx, checkpoint)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.domainMemory == nil && !repo.threadScoped {
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
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.updateDomain(ctx, mutate)
	}
	if repo.threadScoped && repo.domainMemory == nil {
		return repo.updateLoadedDomain(ctx, mutate)
	}
	repo.mu.Lock()
	var observed []string
	committedSuccess := false
	defer func() {
		observer := repo.parentEventObserver
		repo.mu.Unlock()
		if observer != nil && committedSuccess && len(observed) > 0 {
			observer(observed)
		}
	}()
	var committed *backendDomainMutation
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		if repo.domainMemory == nil {
			return errors.New("session-tree state is missing")
		}
		mutation := newBackendDomainMutation(repo.domainMemory)
		mutation.format = repo.domainFormat()
		mutation.memory.bodyTx, mutation.memory.bodyContext = tx, ctx
		// Restore the borrowed maps before the callback returns, including on a
		// panic. A failed backend commit must leave the prior memory authoritative.
		defer mutation.install(repo.domainMemory)
		if err := mutate(mutation.memory, tx); err != nil {
			return err
		}
		mutation.appendParentEvents()
		observed = mutation.observedParentEventThreads()
		if err := mutation.validate(); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := mutation.persist(ctx, tx); err != nil {
			return err
		}
		if err := mutation.persistQuery(ctx, tx, repo.queryThreads); err != nil {
			return err
		}
		for threadID, previous := range mutation.journals {
			entries := mutation.memory.entries[threadID]
			for ordinal := len(previous.entries); ordinal < len(entries); ordinal++ {
				entries[ordinal] = compactCanonicalEntry(entries[ordinal])
			}
		}
		committed = mutation.snapshot()
		return nil
	})
	if err == nil {
		committedSuccess = true
		committed.install(repo.domainMemory)
		repo.installQueryThreads(committed.queryThreads)
		for id := range committed.threads {
			repo.domainMemory.invalidateDeletedQuerySnapshots(id)
		}
		for id := range committed.journals {
			repo.domainMemory.invalidateDeletedQuerySnapshots(id)
		}
	}
	return err
}

// UpdateThreadDomain commits one thread-scoped mutation and related records in
// the same transaction. Runtime kernels use it for durable turn boundaries.
func (repo *BackendRepo) UpdateThreadDomain(ctx context.Context, threadID string, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	if repo == nil || mutate == nil {
		return errors.New("thread domain mutation requires repository and callback")
	}
	return repo.updateThread(ctx, threadID, func(memory *MemoryRepo) error {
		return mutate(memory, memory.bodyTx.(spi.WriteTx))
	})
}

func (repo *BackendRepo) updateLoadedDomain(ctx context.Context, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	var memory *MemoryRepo
	var committed *backendDomainMutation
	var observed []string
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		var found bool
		var err error
		memory, found, err = loadBackendDomain(ctx, tx, repo.now, repo.domainFormat())
		if err != nil {
			return err
		}
		if !found {
			return ErrAuthorityCorrupt
		}
		memory.bodyBackend, memory.bodyTx, memory.bodyContext = repo.backend, tx, ctx
		memory.parentEvents = repo.parentEvents
		memory.querySnapshots = repo.threadQuerySnapshots
		mutation := newBackendDomainMutation(memory)
		mutation.format = repo.domainFormat()
		defer mutation.install(memory)
		if err := mutate(mutation.memory, tx); err != nil {
			return err
		}
		mutation.appendParentEvents()
		observed = mutation.observedParentEventThreads()
		if err := mutation.validate(); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := mutation.persist(ctx, tx); err != nil {
			return err
		}
		if err := mutation.persistQuery(ctx, tx, repo.queryThreads); err != nil {
			return err
		}
		for threadID, previous := range mutation.journals {
			for ordinal := len(previous.entries); ordinal < len(mutation.memory.entries[threadID]); ordinal++ {
				mutation.memory.entries[threadID][ordinal] = compactCanonicalEntry(mutation.memory.entries[threadID][ordinal])
			}
		}
		committed = mutation.snapshot()
		return nil
	})
	if err != nil {
		return err
	}
	committed.install(memory)
	repo.threadQuerySnapshots = memory.querySnapshots
	repo.installQueryThreads(committed.queryThreads)
	for id := range committed.threads {
		memory.invalidateDeletedQuerySnapshots(id)
	}
	for id := range committed.journals {
		memory.invalidateDeletedQuerySnapshots(id)
	}
	for id := range committed.threads {
		if meta, found := memory.threads[id]; found {
			repo.rememberVerifiedThread(id, meta.LeafID)
		} else {
			delete(repo.verifiedThreadLeaves, id)
		}
	}
	for id := range committed.journals {
		if meta, found := memory.threads[id]; found {
			repo.rememberVerifiedThread(id, meta.LeafID)
		}
	}
	observer := repo.parentEventObserver
	if observer != nil && len(observed) > 0 {
		go observer(observed)
	}
	return nil
}

func rejectPreV15BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV14(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return ErrAuthorityCorrupt
	}
	return rejectPreV14BackendDomain(ctx, tx)
}

func rejectPreV16BackendDomain(ctx context.Context, tx spi.ReadTx) error {
	records, err := scanBackendDomainV15(ctx, tx)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return ErrAuthorityCorrupt
	}
	return rejectPreV15BackendDomain(ctx, tx)
}

// SetParentEventObserver installs the runtime's observation-only commit callback.
// It runs after canonical commit and after releasing the repository lock.
func (repo *BackendRepo) SetParentEventObserver(observer func([]string)) {
	if delegate := repo.delegate.Load(); delegate != nil {
		delegate.SetParentEventObserver(observer)
		return
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	repo.parentEventObserver = observer
}
