package sessiontree

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV31Namespace = backendDomainV30Namespace
	backendDomainV31Version   = 31
)

var backendDomainV31Format = backendDomainFormat{
	label: "v31", namespace: backendDomainV31Namespace,
	envelope: "sessiontree-v31-record", version: backendDomainV31Version,
	scanLimit: 256, validate: validateBackendDomainV29Memory,
}

// NewBackendRepoInTransaction preserves every contiguous migration and moves
// structured activity bytes into the existing canonical body-block owner.
func NewBackendRepoInTransaction(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time, progress StartupProgress) (*BackendRepo, error) {
	if ctx == nil || backend == nil || tx == nil || now == nil {
		return nil, errors.New("backend repo transaction requires context, backend, transaction, and clock")
	}
	source, err := detectBackendDomainSource(ctx, tx)
	if err != nil {
		return nil, err
	}
	if source == backendDomainSourceV31 {
		repo := &BackendRepo{backend: backend, now: now, startupProgress: progress}
		repo.reportStartupPhase(StartupPhaseVerifying)
		memory, found, err := loadBackendDomain(ctx, tx, now, backendDomainV31Format)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrAuthorityCorrupt
		}
		memory.parentEvents = parentEventsEnabled(ctx)
		memory.bodyBackend, memory.bodyTx, memory.bodyContext = backend, tx, ctx
		defer func() { memory.bodyTx, memory.bodyContext = nil, nil }()
		if hasUnknownEffectTurns(memory) {
			before := memory
			memory = cloneMemoryRepoForStartup(before)
			memory.parentEvents = parentEventsEnabled(ctx)
			if err := convergeUnknownEffectTurns(ctx, memory); err != nil {
				return nil, err
			}
			changed, err := persistBackendDomainChangesContext(ctx, tx, before, memory, backendDomainV31Format)
			if err != nil {
				return nil, err
			}
			repo.requiresPersistedStartupVerification = changed
		}
		repo.domainMemory = memory
		return repo, nil
	}
	if source == backendDomainSourceFresh {
		if found, err := backendNamespaceHasRecords(tx, canonicalBodyNamespace); err != nil {
			return nil, err
		} else if found {
			return nil, ErrAuthorityCorrupt
		}
		repo := &BackendRepo{backend: backend, now: now, startupProgress: progress, domainMemory: newMemoryRepo(now)}
		repo.domainMemory.parentEvents = parentEventsEnabled(ctx)
		if err := saveCompleteBackendDomainContext(ctx, tx, repo.domainMemory, backendDomainV31Format); err != nil {
			return nil, err
		}
		repo.requiresPersistedStartupVerification = true
		return repo, nil
	}
	repo, err := newBackendRepoThroughV30(ctx, backend, tx, now, progress)
	if err != nil {
		return nil, err
	}
	repo.reportStartupPhase(StartupPhaseMigrating)
	if err := validateBackendDomainV29Memory(repo.domainMemory); err != nil {
		return nil, err
	}
	if err := deleteAllBackendDomain(ctx, tx, backendDomainV30Format); err != nil {
		return nil, err
	}
	if err := deleteCanonicalBodyRecords(ctx, tx); err != nil {
		return nil, err
	}
	if err := saveCompleteBackendDomainContext(ctx, tx, repo.domainMemory, backendDomainV31Format); err != nil {
		return nil, err
	}
	repo.requiresPersistedStartupVerification = true
	return repo, nil
}
