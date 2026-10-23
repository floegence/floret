package sessiontree

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// Canonical entries are immutable. Truncation removes the tail in reverse
// journal order, restoring locators to the preceding immutable source.
func removeTailEntryLocators(ctx context.Context, tx spi.WriteTx, threadID string, ordinal int, entry Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	count, err := readJournalCount(tx, threadID)
	if err != nil || count != ordinal+1 {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	locator, err := readEntryLocator(tx, threadID, entry.ID)
	if err != nil || locator.Ordinal != ordinal {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	restore := func(key []byte, previous string) error {
		if previous == "" {
			return deleteCanonicalOptionalRecord(tx, backendEntryIndexNamespace, key)
		}
		prior, err := readEntryLocator(tx, threadID, previous)
		if err != nil || prior.Ordinal >= ordinal {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return tx.Put(backendEntryIndexNamespace, key, []byte(previous))
	}
	if entry.Type == EntryEffectAttempt {
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			return err
		}
		key := backendDomainKey("effect_owner", attempt.EffectAttemptID)
		owner, err := readEffectSourceLocator(tx, attempt.EffectAttemptID)
		if err != nil || owner.ThreadID != threadID || owner.EntryID != entry.ID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if locator.EffectPrevious == "" {
			if err := tx.Delete(backendEntryIndexNamespace, key); err != nil {
				return err
			}
		} else {
			previous, err := readCanonicalEntryMetadata(tx, threadID, locator.EffectPrevious)
			if err != nil {
				return err
			}
			prior, err := decodeEffectAttempt(previous)
			if err != nil || prior.EffectAttemptID != attempt.EffectAttemptID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			raw, err := json.Marshal(effectSourceLocator{ThreadID: threadID, EntryID: previous.ID})
			if err != nil {
				return err
			}
			if err := tx.Put(backendEntryIndexNamespace, key, raw); err != nil {
				return err
			}
		}
		if err := restore(backendDomainKey("effect", threadID, attempt.EffectAttemptID), locator.EffectPrevious); err != nil {
			return err
		}
	}
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		if err := restore(backendDomainKey("request", threadID, entry.RequestKey), locator.RequestPrevious); err != nil {
			return err
		}
	}
	if queueID, ok := canonicalQueueSource(entry); ok {
		if err := restore(backendDomainKey("queue", threadID, queueID), locator.QueuePrevious); err != nil {
			return err
		}
	}
	if entry.Type == EntryTurnMarker || entry.Type == EntryCancelRequested {
		if locator.TurnPrevious == nil {
			return ErrAuthorityCorrupt
		}
		key := backendDomainKey("turn", threadID, entry.TurnID)
		if *locator.TurnPrevious == (turnEntryLocator{}) {
			if err := deleteCanonicalOptionalRecord(tx, backendEntryIndexNamespace, key); err != nil {
				return err
			}
		} else {
			raw, err := json.Marshal(locator.TurnPrevious)
			if err != nil {
				return err
			}
			if err := tx.Put(backendEntryIndexNamespace, key, raw); err != nil {
				return err
			}
		}
		if err := restore(backendDomainKey("active_turn", threadID, entry.TurnID), locator.ActivePrevious); err != nil {
			return err
		}
	}
	if entry.TurnID != "" {
		if err := tx.Delete(backendEntryIndexNamespace, turnMemberLocatorKey(threadID, entry.TurnID, ordinal)); err != nil {
			return err
		}
	}
	for _, sourceID := range entrySourceIDs(entry) {
		if err := tx.Delete(backendEntryIndexNamespace, sourceLocatorKey(threadID, sourceID, ordinal)); err != nil {
			return err
		}
	}
	if err := tx.Delete(backendEntryIndexNamespace, entryLocatorKey(threadID, entry.ID)); err != nil {
		return err
	}
	if err := removeSparseSource(tx, threadID, ordinal, entry); err != nil {
		return err
	}
	return putJournalCount(tx, threadID, ordinal)
}
