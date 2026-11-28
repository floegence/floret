package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) prepareMemberParentEvents(ctx context.Context, thread string) (bool, error) {
	var parent string
	err := repo.viewThreadRecords(ctx, thread, func(tx spi.ReadTx) error {
		meta, err := readPublishedThreadMeta(tx, repo.domainFormat(), thread)
		if err != nil || meta.ParentThreadID == "" {
			return err
		}
		count, err := readJournalCount(tx, thread)
		if err != nil {
			return err
		}
		prepared, err := parentRecoveryPrepared(tx, thread, count)
		if !prepared {
			parent = meta.ParentThreadID
		}
		return err
	})
	if err != nil || parent == "" {
		return false, err
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	var appended bool
	err = storagebridge.UpdateRelatedThread(ctx, repo.backend, identity.ThreadID(thread), identity.ThreadID(parent), func(tx spi.WriteTx) error {
		meta, err := readPublishedThreadMeta(tx, repo.domainFormat(), thread)
		if err != nil || meta.ParentThreadID != parent {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		destination, err := storagebridge.RelatedWriter(tx, identity.ThreadID(parent))
		if err != nil {
			return err
		}
		if _, err := readPublishedThreadMeta(destination, repo.domainFormat(), parent); err != nil {
			return err
		}
		count, err := readJournalCount(tx, thread)
		if err != nil {
			return err
		}
		prepared, err := parentRecoveryPrepared(tx, thread, count)
		if err != nil || prepared {
			return err
		}
		journal := member.From(tx)
		if journal == nil {
			return ErrInvalidThreadAuthority
		}
		var lastTurn string
		var pending map[string]bool
		err = journal.VisitInteractionSources(func(event member.EntryEvent) error {
			entry, err := materializeMemberEntry(journal, event)
			if err != nil {
				return fmt.Errorf("read interaction source: %w", err)
			}
			receipt, found, err := canonicalAutomaticParentEvent(ctx, tx, meta, entry)
			if err != nil || !found || receipt.InteractionID == "" {
				return err
			}
			if pending == nil || entry.TurnID != lastTurn {
				selection, err := canonicalRuntimeTurnEntries(ctx, tx, thread, entry.TurnID)
				if err != nil {
					return err
				}
				pending = map[string]bool{}
				lastTurn = entry.TurnID
				for _, interaction := range runtimePendingInteractionsIn(selection, entry.TurnID) {
					pending[interaction.ID] = true
				}
			}
			if !pending[receipt.InteractionID] {
				return nil
			}
			added, err := appendCanonicalParentEvent(ctx, destination, receipt)
			if err != nil {
				return fmt.Errorf("verify or publish recovered receipt: %w", err)
			}
			appended = appended || added
			return nil
		})
		if err != nil {
			return fmt.Errorf("recover parent interaction: %w", err)
		}
		return writeParentRecoverySource(tx, thread, count)
	})
	if err != nil {
		return false, threadRouteError(err)
	}
	if appended {
		delete(repo.queryThreads, parent)
		delete(repo.verifiedThreadLeaves, parent)
		if repo.parentEventObserver != nil {
			go repo.parentEventObserver([]string{parent})
		}
	}
	return appended, nil
}
