package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV30Namespace = backendDomainV29Namespace
	backendDomainV30Version   = 30
)

var backendDomainV30Format = backendDomainFormat{
	label: "v30", namespace: backendDomainV30Namespace,
	envelope: "sessiontree-v30-record", version: backendDomainV30Version,
	scanLimit: 256, validate: validateBackendDomainV29Memory,
}

// newBackendRepoThroughV30 preserves the released migration chain, then
// installs segmented canonical bodies in the same startup transaction.
func newBackendRepoThroughV30(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time, progress StartupProgress) (*BackendRepo, error) {
	if ctx == nil || backend == nil || tx == nil || now == nil {
		return nil, errors.New("backend repo transaction requires context, backend, transaction, and clock")
	}
	source, err := detectBackendDomainSource(ctx, tx)
	if err != nil {
		return nil, err
	}
	if source == backendDomainSourceV30 {
		repo := &BackendRepo{backend: backend, now: now, startupProgress: progress}
		repo.reportStartupPhase(StartupPhaseVerifying)
		memory, found, err := loadBackendDomain(ctx, tx, now, backendDomainV30Format)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrAuthorityCorrupt
		}
		memory.parentEvents = parentEventsEnabled(ctx)
		if hasUnknownEffectTurns(memory) {
			before := memory
			memory = cloneMemoryRepoForStartup(before)
			memory.parentEvents = parentEventsEnabled(ctx)
			if err := convergeUnknownEffectTurns(ctx, memory); err != nil {
				return nil, err
			}
			changed, err := persistBackendDomainChangesContext(ctx, tx, before, memory, backendDomainV30Format)
			if err != nil {
				return nil, err
			}
			repo.requiresPersistedStartupVerification = changed
		}
		repo.domainMemory = memory
		return repo, nil
	}
	if found, err := backendNamespaceHasRecords(tx, canonicalBodyNamespace); err != nil {
		return nil, err
	} else if found {
		return nil, ErrAuthorityCorrupt
	}
	if source == backendDomainSourceFresh {
		repo := &BackendRepo{backend: backend, now: now, startupProgress: progress, domainMemory: newMemoryRepo(now)}
		repo.domainMemory.parentEvents = parentEventsEnabled(ctx)
		if err := saveCompleteBackendDomainContext(ctx, tx, repo.domainMemory, backendDomainV30Format); err != nil {
			return nil, err
		}
		repo.requiresPersistedStartupVerification = true
		return repo, nil
	}
	repo, err := newBackendRepoThroughV29(ctx, backend, tx, now, progress)
	if err != nil {
		return nil, err
	}
	repo.reportStartupPhase(StartupPhaseMigrating)
	if err := migrateBackendDomainV29ToV30(ctx, repo.domainMemory); err != nil {
		return nil, err
	}
	if err := deleteAllBackendDomain(ctx, tx, backendDomainV29Format); err != nil {
		return nil, err
	}
	if err := saveCompleteBackendDomainContext(ctx, tx, repo.domainMemory, backendDomainV30Format); err != nil {
		return nil, err
	}
	repo.requiresPersistedStartupVerification = true
	return repo, nil
}

func migrateBackendDomainV29ToV30(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return validateBackendDomainV29Memory(memory)
}

// Body references are derived only from validated owning entries. Orphans,
// extra blocks, missing blocks and incorrect sharing counts fail closed.
func verifyCanonicalBodyRecords(ctx context.Context, tx spi.ReadTx, references map[string]canonicalBodyRecord) error {
	expected := 0
	for _, body := range references {
		expected += 1 + (body.Bytes-1)/canonicalBodyBlockBytes + 1
	}
	seen := 0
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: canonicalBodyNamespace, After: after, Limit: 256})
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Exact regenerated keys reject unknown tuple components and tags.
			components, err := decodeCanonicalBodyKey(record.Key)
			if err != nil {
				return err
			}
			body, ok := references[components.digest]
			if !ok {
				return ErrAuthorityCorrupt
			}
			if components.block {
				if components.ordinal < 0 || components.ordinal >= (body.Bytes-1)/canonicalBodyBlockBytes+1 {
					return ErrAuthorityCorrupt
				}
				if !bytes.Equal(record.Key, canonicalBodyBlockKey(body.SHA256, components.ordinal)) {
					return ErrAuthorityCorrupt
				}
			} else {
				if !bytes.Equal(record.Key, canonicalBodyKey(body.SHA256)) {
					return ErrAuthorityCorrupt
				}
				actual, err := readCanonicalBodyRecord(tx, body.canonicalBodyRef)
				if err != nil || actual != body {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
			}
			seen++
		}
		if !page.HasMore {
			break
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
	if seen != expected {
		return ErrAuthorityCorrupt
	}
	return nil
}
