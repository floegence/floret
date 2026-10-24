package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const backendEntryIndexNamespace = "floret.domain.sessiontree.locators.v32"

// entryLocator contains only canonical source positions and ancestry. It is
// rebuilt from immutable journal records and commits with its source entry.
type entryLocator struct {
	Ordinal         int               `json:"ordinal"`
	ID              string            `json:"id"`
	Parent          string            `json:"parent,omitempty"`
	Skip            string            `json:"skip,omitempty"`
	Depth           int64             `json:"depth"`
	QueuePrevious   string            `json:"queue_previous,omitempty"`
	EffectPrevious  string            `json:"effect_previous,omitempty"`
	RequestPrevious string            `json:"request_previous,omitempty"`
	TurnPrevious    *turnEntryLocator `json:"turn_previous,omitempty"`
	ActivePrevious  string            `json:"active_previous,omitempty"`
}

func entryLocatorKey(threadID, entryID string) []byte {
	return backendDomainKey("entry", threadID, entryID)
}

func sourceLocatorKey(threadID, sourceID string, ordinal int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("source"), storagecodec.TupleString(threadID), storagecodec.TupleString(sourceID), storagecodec.TupleOrdinal(uint64(ordinal)))
}

func turnMemberLocatorKey(threadID, turnID string, ordinal int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("turn_member"), storagecodec.TupleString(threadID), storagecodec.TupleString(turnID), storagecodec.TupleOrdinal(uint64(ordinal)))
}

func entrySourceIDs(entry Entry) []string {
	var ids []string
	for _, id := range []string{entry.Message.ContentSourceID, entry.Message.ReasoningSourceID} {
		if id != "" && !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids
}

func readEntryLocator(tx spi.ReadTx, threadID, entryID string) (entryLocator, error) {
	raw, err := tx.Get(backendEntryIndexNamespace, entryLocatorKey(threadID, entryID))
	if errors.Is(err, spi.ErrNotFound) {
		return entryLocator{}, ErrEntryNotFound
	}
	if err != nil {
		return entryLocator{}, err
	}
	var locator entryLocator
	if err := decodeCanonicalBodyJSON(raw, &locator); err != nil || locator.ID != entryID || locator.Ordinal < 0 || locator.Depth < 1 || (locator.Depth == 1) != (locator.Parent == "") {
		return entryLocator{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return locator, nil
}

func entryAncestor(ctx context.Context, tx spi.ReadTx, threadID string, node entryLocator, depth int64) (entryLocator, error) {
	for node.Depth > depth {
		if err := ctx.Err(); err != nil {
			return entryLocator{}, err
		}
		id, want := node.Parent, node.Depth-1
		if skipDepth := node.Depth & (node.Depth - 1); node.Skip != "" && skipDepth >= depth {
			id, want = node.Skip, skipDepth
		}
		parent, err := readEntryLocator(tx, threadID, id)
		if err != nil || parent.Depth != want || parent.Ordinal >= node.Ordinal {
			return entryLocator{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		node = parent
	}
	return node, nil
}

func putEntryLocator(ctx context.Context, tx spi.WriteTx, threadID string, ordinal int, entry Entry) error {
	locator := entryLocator{Ordinal: ordinal, ID: entry.ID, Parent: entry.ParentID, Depth: entry.PathDepth}
	if queueID, ok := canonicalQueueSource(entry); ok {
		previous, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("queue", threadID, queueID))
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		locator.QueuePrevious = string(previous)
		if locator.QueuePrevious != "" {
			prior, err := readEntryLocator(tx, threadID, locator.QueuePrevious)
			if err != nil || prior.Ordinal >= ordinal {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	}
	if entry.Type == EntryEffectAttempt {
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			return err
		}
		previous, err := readEffectSourceLocator(tx, attempt.EffectAttemptID)
		if err == nil {
			if previous.ThreadID != threadID {
				return ErrAuthorityCorrupt
			}
			locator.EffectPrevious = previous.EntryID
		} else if !errors.Is(err, ErrEffectAttemptNotFound) {
			return err
		}
	}
	if entry.Type == EntryTurnMarker || entry.Type == EntryCancelRequested {
		previous, err := readTurnEntryLocator(tx, threadID, entry.TurnID)
		if err != nil {
			return err
		}
		locator.TurnPrevious = &previous
		active, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("active_turn", threadID, entry.TurnID))
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		locator.ActivePrevious = string(active)
	}
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		previous, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("request", threadID, entry.RequestKey))
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		locator.RequestPrevious = string(previous)
	}
	if entry.ThreadID != threadID || entry.ID == "" || ordinal < 0 || locator.Depth < 1 || (locator.Depth == 1) != (locator.Parent == "") {
		return ErrAuthorityCorrupt
	}
	if locator.Parent != "" {
		parent, err := readEntryLocator(tx, threadID, locator.Parent)
		if err != nil || parent.Depth+1 != locator.Depth || parent.Ordinal >= ordinal {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if depth := locator.Depth & (locator.Depth - 1); depth > 0 {
			ancestor, err := entryAncestor(ctx, tx, threadID, parent, depth)
			if err != nil {
				return err
			}
			locator.Skip = ancestor.ID
		}
	}
	raw, err := json.Marshal(locator)
	if err != nil {
		return err
	}
	if err := tx.Put(backendEntryIndexNamespace, entryLocatorKey(threadID, entry.ID), raw); err != nil {
		return err
	}
	for _, sourceID := range entrySourceIDs(entry) {
		if err := tx.Put(backendEntryIndexNamespace, sourceLocatorKey(threadID, sourceID, ordinal), []byte(entry.ID)); err != nil {
			return err
		}
	}
	if err := putTurnEntryLocator(tx, threadID, entry); err != nil {
		return err
	}
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		if err := tx.Put(backendEntryIndexNamespace, backendDomainKey("request", threadID, entry.RequestKey), []byte(entry.ID)); err != nil {
			return err
		}
	}
	if entry.Type == EntryEffectAttempt {
		if err := putEffectSourceLocator(tx, entry); err != nil {
			return err
		}
	}
	if queueID, ok := canonicalQueueSource(entry); ok {
		if err := tx.Put(backendEntryIndexNamespace, backendDomainKey("queue", threadID, queueID), []byte(entry.ID)); err != nil {
			return err
		}
	}
	if entry.Type == EntryTurnMarker && strings.TrimSpace(entry.TurnID) != "" {
		key := backendDomainKey("active_turn", threadID, entry.TurnID)
		switch entry.TurnStatus {
		case TurnStarted, TurnSavePoint, TurnWaiting:
			if err := tx.Put(backendEntryIndexNamespace, key, []byte(entry.ID)); err != nil {
				return err
			}
		case TurnCompleted, TurnFailed, TurnAborted:
			if err := tx.Delete(backendEntryIndexNamespace, key); err != nil && !errors.Is(err, spi.ErrNotFound) {
				return err
			}
		}
	}
	if entry.TurnID != "" {
		if err := tx.Put(backendEntryIndexNamespace, turnMemberLocatorKey(threadID, entry.TurnID, ordinal), []byte(entry.ID)); err != nil {
			return err
		}
	}
	if err := appendSparseSource(tx, threadID, ordinal, entry); err != nil {
		return err
	}
	if err := appendContextSourceMetadata(tx, entry, ordinal); err != nil {
		return err
	}
	if err := appendProjectionSources(tx, entry, ordinal); err != nil {
		return err
	}
	if err := putJournalCount(tx, threadID, ordinal+1); err != nil {
		return err
	}
	membership := &canonicalMembershipAppend{}
	if err := appendModelContextSources(ctx, tx, entry, membership); err != nil {
		return err
	}
	if err := appendCanonicalContextProjection(ctx, tx, entry); err != nil {
		return fmt.Errorf("context projection for source %q: %w", entry.ID, err)
	}
	if err := appendWindowSources(ctx, tx, entry, membership); err != nil {
		if !errors.Is(err, errItemMembershipSource) {
			return fmt.Errorf("window sources for %q: %w", entry.ID, err)
		}
		if err := deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, windowSourcesKey(threadID)); err != nil {
			return err
		}
	}
	if err := appendCanonicalItemMembership(ctx, tx, entry, membership); err != nil {
		if errors.Is(err, errItemMembershipSource) {
			return deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, itemManifestKey(threadID))
		}
		return fmt.Errorf("item membership for source %q: %w", entry.ID, err)
	}
	return nil
}

// These are source locators only. Execution authority remains in the immutable
// marker entries, which every caller reads and validates before acting.
type turnEntryLocator struct {
	Started      string `json:"started,omitempty"`
	Terminal     string `json:"terminal,omitempty"`
	Identity     string `json:"identity,omitempty"`
	Cancellation string `json:"cancellation,omitempty"`
}

func readTurnEntryLocator(tx spi.ReadTx, threadID, turnID string) (turnEntryLocator, error) {
	raw, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("turn", threadID, turnID))
	if errors.Is(err, spi.ErrNotFound) {
		return turnEntryLocator{}, nil
	}
	if err != nil {
		return turnEntryLocator{}, err
	}
	var value turnEntryLocator
	if err := decodeCanonicalBodyJSON(raw, &value); err != nil {
		return value, errors.Join(ErrAuthorityCorrupt, err)
	}
	return value, nil
}

func putTurnEntryLocator(tx spi.WriteTx, threadID string, entry Entry) error {
	if (entry.Type != EntryTurnMarker && entry.Type != EntryCancelRequested) || strings.TrimSpace(entry.TurnID) == "" {
		return nil
	}
	value, err := readTurnEntryLocator(tx, threadID, entry.TurnID)
	if err != nil {
		return err
	}
	value = observeTurnEntryLocator(value, entry)
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.Put(backendEntryIndexNamespace, backendDomainKey("turn", threadID, entry.TurnID), raw)
}

func observeTurnEntryLocator(value turnEntryLocator, entry Entry) turnEntryLocator {
	if (entry.Type != EntryTurnMarker && entry.Type != EntryCancelRequested) || strings.TrimSpace(entry.TurnID) == "" {
		return value
	}
	if entry.Type == EntryTurnMarker {
		if entry.TurnStatus == TurnStarted {
			value.Started = entry.ID
		}
		if terminalTurnMarker(entry.TurnStatus) {
			value.Terminal = entry.ID
		}
	} else {
		value.Cancellation = entry.ID
	}
	if strings.TrimSpace(entry.RunID) != "" {
		value.Identity = entry.ID
	}
	return value
}

func putJournalCount(tx spi.WriteTx, threadID string, count int) error {
	raw, err := json.Marshal(count)
	if err != nil {
		return err
	}
	return tx.Put(backendEntryIndexNamespace, backendDomainKey("count", threadID), raw)
}

func readJournalCount(tx spi.ReadTx, threadID string) (int, error) {
	raw, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("count", threadID))
	if err != nil {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	var count int
	if err := decodeCanonicalBodyJSON(raw, &count); err != nil || count < 0 {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return count, nil
}

func readLocatedEntry(tx spi.ReadTx, format backendDomainFormat, threadID string, locator entryLocator) (Entry, error) {

	encoded, err := tx.Get(format.namespace, backendDomainEntryKey(threadID, locator.Ordinal))
	if err != nil {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	record, stored, err := decodeCanonicalEntryRecord(format, encoded)
	if err != nil || record.ID != locator.ID || record.ThreadID != threadID || record.Ordinal != locator.Ordinal {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err != nil || stored.Entry.ParentID != locator.Parent || stored.Entry.PathDepth != locator.Depth {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	entry := stored.Entry
	entry.bodyFields = stored.Fields
	return entry, nil
}

func verifyThreadEntryLocators(ctx context.Context, tx spi.ReadTx, memory *MemoryRepo) error {
	for threadID, entries := range memory.entries {
		if err := verifyCanonicalThreadLocators(ctx, tx, threadID, len(entries)); err != nil {
			return err
		}
	}
	return nil
}

// verifyCanonicalThreadLocators proves correspondence in both directions. Each
// source is checked once and each locator chain accounts for its exact members;
// memory use is independent of the number of historical turns or requests.
func verifyCanonicalThreadLocators(ctx context.Context, tx spi.ReadTx, threadID string, expectedCount int) error {
	count, err := readJournalCount(tx, threadID)
	if err != nil || count != expectedCount {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	members, sources, queues, effects, requests := 0, 0, 0, 0, 0
	for ordinal := 0; ordinal < count; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := readCanonicalEntryOrdinal(tx, threadID, ordinal)
		if err != nil {
			return err
		}
		locator, err := readEntryLocator(tx, threadID, entry.ID)
		if err != nil || locator.Ordinal != ordinal || locator.Parent != entry.ParentID || locator.Depth != entry.PathDepth {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		wantSkip := ""
		if entry.ParentID != "" {
			parent, err := readEntryLocator(tx, threadID, entry.ParentID)
			if err != nil || parent.Ordinal >= ordinal || parent.Depth+1 != entry.PathDepth {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if depth := entry.PathDepth & (entry.PathDepth - 1); depth > 0 {
				ancestor, err := entryAncestor(ctx, tx, threadID, parent, depth)
				if err != nil || ancestor.Depth != depth {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				wantSkip = ancestor.ID
			}
		}
		if locator.Skip != wantSkip {
			return ErrAuthorityCorrupt
		}
		for _, sourceID := range entrySourceIDs(entry) {
			sources++
			id, err := tx.Get(backendEntryIndexNamespace, sourceLocatorKey(threadID, sourceID, ordinal))
			if err != nil || string(id) != entry.ID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if _, ok := canonicalQueueSource(entry); ok {
			queues++
		} else if locator.QueuePrevious != "" {
			return ErrAuthorityCorrupt
		}
		if entry.Type == EntryEffectAttempt {
			effects++
		} else if locator.EffectPrevious != "" {
			return ErrAuthorityCorrupt
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			requests++
		} else if locator.RequestPrevious != "" {
			return ErrAuthorityCorrupt
		}
		if entry.Type == EntryTurnMarker || entry.Type == EntryCancelRequested {
			if locator.TurnPrevious == nil {
				return ErrAuthorityCorrupt
			}
			if entry.TurnID != "" {
				value, err := readTurnEntryLocator(tx, threadID, entry.TurnID)
				if err != nil || value == (turnEntryLocator{}) {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
			} else if *locator.TurnPrevious != (turnEntryLocator{}) || locator.ActivePrevious != "" {
				return ErrAuthorityCorrupt
			}
		} else if locator.TurnPrevious != nil || locator.ActivePrevious != "" {
			return ErrAuthorityCorrupt
		}
		if entry.TurnID != "" {
			members++
			id, err := tx.Get(backendEntryIndexNamespace, turnMemberLocatorKey(threadID, entry.TurnID, ordinal))
			if err != nil || string(id) != entry.ID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	}
	activeCount := 0
	_, err = visitThreadLocatorRows(ctx, tx, "turn", threadID, func(row spi.Record) error {
		var actual turnEntryLocator
		if err := decodeCanonicalBodyJSON(row.Value, &actual); err != nil || actual == (turnEntryLocator{}) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		sourceID := actual.Started
		if sourceID == "" {
			sourceID = actual.Identity
		}
		if sourceID == "" {
			sourceID = actual.Terminal
		}
		if sourceID == "" {
			sourceID = actual.Cancellation
		}
		source, err := readCanonicalEntryMetadata(tx, threadID, sourceID)
		if err != nil || source.TurnID == "" || !bytes.Equal(row.Key, backendDomainKey("turn", threadID, source.TurnID)) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		value := turnEntryLocator{}
		active := ""
		if err := visitCanonicalTurnEntries(ctx, tx, threadID, source.TurnID, func(entry Entry) error {
			if entry.Type != EntryTurnMarker && entry.Type != EntryCancelRequested {
				return nil
			}
			locator, err := readEntryLocator(tx, threadID, entry.ID)
			if err != nil || locator.TurnPrevious == nil || *locator.TurnPrevious != value || locator.ActivePrevious != active {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			value = observeTurnEntryLocator(value, entry)
			if entry.Type == EntryTurnMarker {
				switch entry.TurnStatus {
				case TurnStarted, TurnSavePoint, TurnWaiting:
					active = entry.ID
				case TurnCompleted, TurnFailed, TurnAborted:
					active = ""
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if actual != value {
			return ErrAuthorityCorrupt
		}
		raw, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("active_turn", threadID, source.TurnID))
		if active == "" {
			if !errors.Is(err, spi.ErrNotFound) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		} else {
			if err != nil || string(raw) != active {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			activeCount++
		}
		return nil
	})
	if err != nil {
		return err
	}
	for kind, expected := range map[string]int{"queue": queues, "effect": effects, "request": requests} {
		visited := 0
		_, err := visitThreadLocatorRows(ctx, tx, kind, threadID, func(row spi.Record) error {
			id, before := string(row.Value), count
			for id != "" {
				if err := ctx.Err(); err != nil {
					return err
				}
				locator, err := readEntryLocator(tx, threadID, id)
				if err != nil || locator.Ordinal >= before {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				entry, err := readLocatedEntry(tx, backendDomainV32Format, threadID, locator)
				if err != nil {
					return err
				}
				key := ""
				switch kind {
				case "queue":
					var found bool
					key, found = canonicalQueueSource(entry)
					if !found {
						return ErrAuthorityCorrupt
					}
					id = locator.QueuePrevious
				case "request":
					if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted {
						return ErrAuthorityCorrupt
					}
					key, id = entry.RequestKey, locator.RequestPrevious
				case "effect":
					attempt, err := decodeEffectAttempt(entry)
					if err != nil {
						return err
					}
					key, id = attempt.EffectAttemptID, locator.EffectPrevious
					if before == count {
						owner, err := readEffectSourceLocator(tx, key)
						if err != nil || owner.ThreadID != threadID || owner.EntryID != entry.ID {
							return errors.Join(ErrAuthorityCorrupt, err)
						}
					}
				}
				if !bytes.Equal(row.Key, backendDomainKey(kind, threadID, key)) {
					return ErrAuthorityCorrupt
				}
				before = locator.Ordinal
				visited++
				if visited > expected {
					return ErrAuthorityCorrupt
				}
			}
			return nil
		})
		if err != nil || visited != expected {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	for kind, expected := range map[string]int{"entry": count, "source": sources, "turn_member": members, "active_turn": activeCount} {
		actual, err := visitThreadLocatorRows(ctx, tx, kind, threadID, nil)
		if err != nil || actual != expected {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	return nil
}

func visitThreadLocatorRows(ctx context.Context, tx spi.ReadTx, kind, threadID string, visit func(spi.Record) error) (int, error) {
	prefix := backendDomainKey(kind, threadID)
	after, count := []byte(nil), 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendEntryIndexNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return 0, err
		}
		for _, row := range page.Records {
			if visit != nil {
				if err := visit(row); err != nil {
					return 0, err
				}
			}
			count++
		}
		if !page.HasMore {
			return count, nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return 0, ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}
