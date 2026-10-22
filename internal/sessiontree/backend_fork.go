package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) Fork(ctx context.Context, opts ForkOptions) (ThreadMeta, error) {
	meta, _, err := repo.forkCanonicalThread(ctx, opts, nil)
	return meta, err
}

func (repo *BackendRepo) ForkWithInitialEntry(ctx context.Context, opts ForkOptions, initial Entry) (ThreadMeta, Entry, error) {
	return repo.forkCanonicalThread(ctx, opts, &initial)
}

func (repo *BackendRepo) forkCanonicalThread(ctx context.Context, opts ForkOptions, initial *Entry) (ThreadMeta, Entry, error) {
	if repo == nil || ctx == nil {
		return ThreadMeta{}, Entry{}, spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return ThreadMeta{}, Entry{}, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.forkCanonicalThread(ctx, opts, initial)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		var meta ThreadMeta
		var entry Entry
		err := repo.updateThread(ctx, opts.SourceThreadID, func(memory *MemoryRepo) error {
			var err error
			if initial == nil {
				meta, err = memory.Fork(ctx, opts)
			} else {
				meta, entry, err = memory.ForkWithInitialEntry(ctx, opts, *initial)
			}
			return err
		})
		return meta, entry, err
	}
	if err := repo.prepareThreadIfNeeded(ctx, opts.SourceThreadID); err != nil {
		return ThreadMeta{}, Entry{}, err
	}
	if opts.DestinationMeta != nil && opts.DestinationMeta.ParentThreadID != "" {
		if err := repo.prepareThreadIfNeeded(ctx, strings.TrimSpace(opts.DestinationMeta.ParentThreadID)); err != nil {
			return ThreadMeta{}, Entry{}, err
		}
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	mutation := &canonicalJournalMutation{ctx: ctx, repo: repo}
	var saved Entry
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		mutation.tx = tx
		if err := mutation.forkThread(opts); err != nil {
			return err
		}
		if initial != nil {
			entry := cloneEntry(*initial)
			entry.ThreadID = mutation.meta.ID
			var err error
			saved, err = mutation.append(entry, AppendOptions{Now: opts.Now})
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

func (mutation *canonicalJournalMutation) forkThread(opts ForkOptions) error {
	ctx, tx, repo := mutation.ctx, mutation.tx, mutation.repo
	source, err := readPublishedThreadMeta(tx, backendDomainV32Format, opts.SourceThreadID)
	if err != nil {
		return err
	}
	if source.IsClosed() {
		return ErrThreadClosed
	}
	targetID := opts.EntryID
	if targetID == "" && !opts.EntryIDPinned {
		targetID = source.LeafID
	}
	if opts.Position == ForkBefore {
		entry, err := readCanonicalEntryMetadata(tx, source.ID, targetID)
		if err != nil {
			return err
		}
		targetID = entry.ParentID
	}
	if opts.NewThreadID != "" {
		existing, err := readPublishedThreadMeta(tx, backendDomainV32Format, opts.NewThreadID)
		if err == nil {
			if !forkDestinationMatches(existing, opts, targetID) {
				return ErrThreadExists
			}
			mutation.meta, mutation.query = existing, repo.queryThreads[existing.ID]
			return nil
		}
		if errors.Is(err, ErrThreadDeleted) {
			return ErrForkDestinationConflict
		}
		if !errors.Is(err, ErrThreadNotFound) {
			return err
		}
	}
	manifest, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	newID := opts.NewThreadID
	if newID == "" {
		for {
			if manifest.Sequence == math.MaxInt64 {
				return ErrAuthorityCorrupt
			}
			manifest.Sequence++
			newID = fmt.Sprintf("%s-fork-%d", source.ID, manifest.Sequence)
			err := repo.canonicalThreadIDAvailable(tx, newID)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrThreadExists) && !errors.Is(err, ErrThreadDeleted) {
				return err
			}
		}
	} else if err := repo.canonicalThreadIDAvailable(tx, newID); err != nil {
		return err
	}
	path, err := canonicalCompletePath(ctx, tx, source.ID, targetID)
	if err != nil {
		return err
	}
	reader := canonicalArtifactReader(tx)
	actual, err := reader.artifactClosureLocked(source.ID, newID, path)
	if err != nil {
		return err
	}
	closure := artifact.CloneClosure(opts.ArtifactClosure)
	if artifact.IsZeroClosure(closure) {
		closure = actual
	}
	if artifact.ValidateClosure(closure) != nil || !artifact.EqualClosure(closure, actual) {
		return ErrStaleAuthority
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	meta := ThreadMeta{ID: newID, ForkedFromThreadID: source.ID, ForkedFromEntryID: targetID, OriginRequestKey: strings.TrimSpace(opts.OriginRequestKey), OriginFingerprint: strings.TrimSpace(opts.OriginFingerprint), CreatedAt: now, UpdatedAt: now}
	applyForkDestinationMeta(&meta, opts.DestinationMeta)
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return err
	}
	if meta.ParentThreadID != "" {
		parent, err := readPublishedThreadMeta(tx, backendDomainV32Format, meta.ParentThreadID)
		if err != nil {
			return errors.Join(ErrInvalidThreadAuthority, err)
		}
		if parent.IsClosed() {
			return ErrThreadClosed
		}
	}
	var allocationErr error
	meta, entries, aliases, err := projectForkPath(ctx, path, meta, now, func() string {
		if manifest.Sequence == math.MaxInt64 {
			allocationErr = ErrAuthorityCorrupt
			return ""
		}
		manifest.Sequence++
		return fmt.Sprintf("%s-entry-%d", newID, manifest.Sequence)
	})
	if err != nil || allocationErr != nil {
		return errors.Join(err, allocationErr)
	}
	artifacts, err := reader.stageArtifactForkLocked(closure, aliases, now)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Type == EntryUserMessage && fallbackThreadTitle(entry.Message) != "" {
			meta, _, err = installFallbackThreadTitle(meta, entry.Message, now)
			if err != nil {
				return err
			}
			break
		}
	}
	empty := meta
	empty.LeafID = ""
	if _, err := initializeCanonicalThread(ctx, tx, empty); err != nil {
		return err
	}
	for ordinal, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return err
		}
		if err := putCanonicalStoredEntry(ctx, tx, backendDomainV32Format, backendDomainEntryKey(newID, ordinal), entry.ID, newID, ordinal, entry); err != nil {
			return err
		}
	}
	for key, record := range artifacts {
		if err := reader.validateArtifactRecordLocked(record); err != nil {
			return err
		}
		if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0, record); err != nil {
			return err
		}
	}
	if record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTodo, source.ID), backendDomainRecordTodo, source.ID, "", 0); err != nil {
		return err
	} else if found {
		var todo AgentTodoState
		if err := decodeBackendDomainValue(backendDomainV32Format, record, &todo); err != nil || todo.ThreadID != source.ID || ValidateAgentTodoItems(todo.Items) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		todo.ThreadID = newID
		if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTodo, newID), backendDomainRecordTodo, newID, "", 0, todo); err != nil {
			return err
		}
	}
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
		return err
	}
	if err := persistThreadMetadata(ctx, tx, meta); err != nil {
		return err
	}
	mutation.meta = meta
	mutation.query, err = rebuildCanonicalQuery(ctx, tx, meta)
	return err
}
