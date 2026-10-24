package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) CreateThread(ctx context.Context, meta ThreadMeta) (ThreadMeta, error) {
	created, _, err := repo.createCanonicalThread(ctx, meta, nil)
	return created, err
}

func (repo *BackendRepo) CreateThreadWithInitialEntry(ctx context.Context, meta ThreadMeta, initial Entry) (ThreadMeta, Entry, error) {
	return repo.createCanonicalThread(ctx, meta, &initial)
}

func (repo *BackendRepo) createCanonicalThread(ctx context.Context, input ThreadMeta, initial *Entry) (ThreadMeta, Entry, error) {
	if repo == nil || ctx == nil {
		return ThreadMeta{}, Entry{}, spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return ThreadMeta{}, Entry{}, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.createCanonicalThread(ctx, input, initial)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		var created ThreadMeta
		var entry Entry
		err := repo.updateNewThread(ctx, input.ID, input.ParentThreadID, func(memory *MemoryRepo) error {
			var err error
			if initial == nil {
				created, err = memory.CreateThread(ctx, input)
			} else {
				created, entry, err = memory.CreateThreadWithInitialEntry(ctx, input, *initial)
			}
			return err
		})
		return created, entry, err
	}
	if input.ParentThreadID != "" {
		if err := repo.prepareThreadIfNeeded(ctx, input.ParentThreadID); err != nil {
			return ThreadMeta{}, Entry{}, err
		}
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	mutation := &canonicalJournalMutation{ctx: ctx, repo: repo}
	var saved Entry
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		meta := input
		if meta.ID == "" {
			manifest, err := readV32Manifest(tx)
			if err != nil {
				return err
			}
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				if manifest.Sequence == math.MaxInt64 {
					return ErrAuthorityCorrupt
				}
				manifest.Sequence++
				meta.ID = fmt.Sprintf("thread-%d", manifest.Sequence)
				err := repo.canonicalThreadIDAvailable(tx, meta.ID)
				if err == nil {
					break
				}
				if !errors.Is(err, ErrThreadExists) && !errors.Is(err, ErrThreadDeleted) {
					return err
				}
			}
			if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
				return err
			}
		} else if err := repo.canonicalThreadIDAvailable(tx, meta.ID); err != nil {
			return err
		}
		var err error
		meta, err = prepareNewThreadMeta(meta, repo.now)
		if err != nil {
			return err
		}
		if meta.LeafID != "" {
			return ErrAuthorityCorrupt
		}
		seen := map[string]bool{meta.ID: true}
		for parentID := strings.TrimSpace(meta.ParentThreadID); parentID != ""; {
			if err := ctx.Err(); err != nil {
				return err
			}
			if seen[parentID] {
				return ErrInvalidThreadAuthority
			}
			seen[parentID] = true
			parent, err := readPublishedThreadMeta(tx, backendDomainV32Format, parentID)
			if err != nil {
				return errors.Join(ErrInvalidThreadAuthority, err)
			}
			if parentID == strings.TrimSpace(meta.ParentThreadID) {
				if err := lifecycleRejectsWrite(parent); err != nil {
					return err
				}
			}
			parentID = strings.TrimSpace(parent.ParentThreadID)
		}
		state, err := initializeCanonicalThread(ctx, tx, meta)
		if err != nil {
			return err
		}
		mutation.tx, mutation.meta, mutation.query = tx, meta, state
		if initial != nil {
			entry := cloneEntry(*initial)
			entry.ThreadID = meta.ID
			saved, err = mutation.append(entry, AppendOptions{Now: meta.CreatedAt})
			if err != nil {
				return err
			}
		}
		if err := mutation.publishParentEvents(); err != nil {
			return err
		}
		return ctx.Err()
	})
	if err != nil {
		return ThreadMeta{}, Entry{}, err
	}
	repo.installJournalMutation(mutation)
	return mutation.meta, saved, nil
}

// The retained namespace is consulted only to reserve source identities during
// migration. A new thread never takes ownership of an unpublished old thread.
func (repo *BackendRepo) canonicalThreadIDAvailable(tx spi.ReadTx, threadID string) error {
	formats := []backendDomainFormat{backendDomainV32Format}
	if repo.migrationSource != nil {
		formats = append(formats, *repo.migrationSource)
	}
	for _, format := range formats {
		for _, kind := range []string{backendDomainRecordThread, backendDomainRecordTombstone} {
			_, found, err := readBackendDomainRecord(tx, format, backendDomainKey(kind, threadID), kind, threadID, "", 0)
			if err != nil {
				return err
			}
			if found {
				if kind == backendDomainRecordTombstone {
					return ErrThreadDeleted
				}
				return ErrThreadExists
			}
		}
	}
	return nil
}

func initializeCanonicalThread(ctx context.Context, tx spi.WriteTx, meta ThreadMeta) (*queryThread, error) {
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, meta.ID), backendDomainRecordThread, meta.ID, "", 0, meta); err != nil {
		return nil, err
	}
	if meta.ParentThreadID == "" {
		if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordRootThread, meta.ID), backendDomainRecordRootThread, meta.ID, "", 0, backendDomainRootThread{ThreadID: meta.ID}); err != nil {
			return nil, err
		}
	}
	if err := putJournalCount(tx, meta.ID, 0); err != nil {
		return nil, err
	}
	if err := putItemJSON(tx, itemManifestKey(meta.ID), itemMembershipManifest{}); err != nil {
		return nil, err
	}
	if err := putItemJSON(tx, contextProjectionKey(meta.ID), storedContextProjection{}); err != nil {
		return nil, err
	}
	if err := putItemJSON(tx, modelContextSourcesKey(meta.ID), storedModelContextSources{}); err != nil {
		return nil, err
	}
	if err := putItemJSON(tx, windowSourcesKey(meta.ID), storedWindowSources{}); err != nil {
		return nil, err
	}
	state := newQueryThread()
	projector := &queryProjector{}
	projector.finish(state, meta.ID, meta)
	for _, record := range state.records {
		document, err := queryDocument(record)
		if err != nil {
			return nil, err
		}
		if err := queryMetadataWriter(tx).putDocument(document); err != nil {
			return nil, err
		}
	}
	if err := putQueryManifest(tx, meta.ID, ""); err != nil {
		return nil, err
	}
	return state, nil
}
