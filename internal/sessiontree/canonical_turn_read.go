package sessiontree

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
)

var ErrCanonicalTurnNotFound = errors.New("session tree canonical turn not found")

// CanonicalTurnRead is one admitted canonical turn on the current active path.
type CanonicalTurnRead struct {
	Turn           CanonicalTurn
	LatestTurn     CanonicalTurn
	ThroughOrdinal int64
	HasRetryTarget bool
}

// CanonicalTurnReadRepo reads one admitted turn without scanning a public page.
type CanonicalTurnReadRepo interface {
	ReadCanonicalTurn(context.Context, string, string) (CanonicalTurnRead, error)
}

// ValidateCanonicalTurnReadStructure validates the canonical journal shape.
func ValidateCanonicalTurnReadStructure(turn CanonicalTurn, threadID string) error {
	threadID = strings.TrimSpace(threadID)
	turnID := strings.TrimSpace(turn.TurnID)
	runID := strings.TrimSpace(turn.RunID)
	if threadID == "" || turnID == "" || runID == "" {
		return ErrAuthorityCorrupt
	}
	entries := make([]Entry, 0, len(turn.Entries))
	for _, item := range turn.Entries {
		if item.Ordinal <= 0 || item.Entry.PathDepth != item.Ordinal {
			return ErrAuthorityCorrupt
		}
		entries = append(entries, item.Entry)
	}
	if err := ValidateCanonicalTurnEntries(entries, threadID, turnID, runID); err != nil {
		return err
	}
	if len(entries) == 0 || entries[0].ID != turn.StartedEntryID || entries[0].PathDepth != turn.StartedOrdinal {
		return ErrAuthorityCorrupt
	}
	retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entries[0])
	if err != nil {
		return err
	}
	if !sameCanonicalTurnRetrySource(turn.RetrySource, retrySource) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func sameCanonicalTurnRetrySource(left, right *CanonicalTurnRetrySource) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return strings.TrimSpace(left.TurnID) == strings.TrimSpace(right.TurnID) &&
		strings.TrimSpace(left.EntryID) == strings.TrimSpace(right.EntryID)
}

func (r *MemoryRepo) ReadCanonicalTurn(_ context.Context, threadID, turnID string) (CanonicalTurnRead, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	if threadID == "" || turnID == "" {
		return CanonicalTurnRead{}, ErrCanonicalTurnNotFound
	}
	meta, ok := r.threads[threadID]
	if !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return CanonicalTurnRead{}, ErrThreadDeleted
		}
		return CanonicalTurnRead{}, ErrThreadNotFound
	}
	return r.canonicalTurnReaderLocked().read(threadID, turnID, meta.LeafID)
}

func (r canonicalTurnReader) read(threadID, turnID, leafID string) (CanonicalTurnRead, error) {
	turn, err := r.readCanonicalTurnLocked(threadID, turnID, leafID)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	latestTurnID, err := r.latestCanonicalTurnIDLocked(threadID, leafID)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	latestTurn, err := r.readCanonicalTurnLocked(threadID, latestTurnID, leafID)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	_, through, err := r.entry(threadID, leafID)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	hasRetryTarget, err := r.canonicalTurnRetryEligibilityLocked(threadID, latestTurn)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	return CanonicalTurnRead{
		Turn: turn, LatestTurn: latestTurn, ThroughOrdinal: through, HasRetryTarget: hasRetryTarget,
	}, nil
}

func (r canonicalTurnReader) readCanonicalTurnLocked(threadID, turnID, leafID string) (CanonicalTurn, error) {
	entries, err := r.turn(threadID, turnID)
	if err != nil {
		return CanonicalTurn{}, err
	}
	if len(entries) == 0 {
		return CanonicalTurn{}, ErrCanonicalTurnNotFound
	}
	all := make([]CanonicalTurnPathEntry, 0, len(entries))
	var started Entry
	startedCount := 0
	for _, entry := range entries {
		if entry.ThreadID != threadID || strings.TrimSpace(entry.TurnID) != turnID || entry.PathDepth <= 0 {
			return CanonicalTurn{}, ErrAuthorityCorrupt
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return CanonicalTurn{}, err
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			started = entry
			startedCount++
		}
		all = append(all, CanonicalTurnPathEntry{Entry: cloneEntry(entry), Ordinal: entry.PathDepth})
	}
	if startedCount == 0 {
		return CanonicalTurn{}, ErrAuthorityCorrupt
	}
	if startedCount != 1 {
		return CanonicalTurn{}, ErrAuthorityCorrupt
	}
	if leafID == "" {
		return CanonicalTurn{}, ErrAuthorityCorrupt
	}
	active, err := r.canonicalTurnActiveAncestorLocked(threadID, leafID, started.ID)
	if err != nil {
		return CanonicalTurn{}, err
	}
	if !active {
		return CanonicalTurn{}, ErrCanonicalTurnNotFound
	}
	pathEntries := make([]CanonicalTurnPathEntry, 0, len(all))
	for _, item := range all {
		onPath, err := r.canonicalTurnActiveAncestorLocked(threadID, leafID, item.Entry.ID)
		if err != nil {
			return CanonicalTurn{}, err
		}
		if onPath {
			pathEntries = append(pathEntries, item)
		}
	}
	pathEntries = canonicalTurnPathEntriesForRead(pathEntries)
	retrySource, err := CanonicalTurnRetrySourceForStartedEntry(started)
	if err != nil {
		return CanonicalTurn{}, err
	}
	userCount := canonicalTurnUserEntryCount(pathEntries)
	contextOnly := started.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction
	typed := started.Metadata[TurnKindMetadataKey] != ""
	if retrySource == nil && userCount == 0 && !typed {
		return CanonicalTurn{}, ErrCanonicalTurnNotFound
	}
	if !typed && retrySource == nil && userCount != 1 || retrySource != nil && userCount != 0 || contextOnly && (retrySource != nil || userCount != 0) {
		return CanonicalTurn{}, ErrAuthorityCorrupt
	}
	turn := CanonicalTurn{
		TurnID: turnID, RunID: strings.TrimSpace(started.RunID),
		StartedEntryID: started.ID, StartedOrdinal: started.PathDepth,
		RetrySource: cloneCanonicalTurnRetrySource(retrySource), Entries: pathEntries,
	}
	linkEntries := make([]Entry, len(pathEntries))
	for index, item := range pathEntries {
		linkEntries[index] = item.Entry
	}
	if err := r.links(threadID, linkEntries); err != nil {
		return CanonicalTurn{}, err
	}
	if err := ValidateCanonicalTurnReadStructure(turn, threadID); err != nil {
		return CanonicalTurn{}, err
	}
	if retrySource != nil {
		eligible, err := r.retrySourceHasRetryEligibleDurableInputLocked(
			threadID, turn.TurnID, turn.RunID, turn.StartedEntryID, *retrySource,
		)
		if err != nil {
			return CanonicalTurn{}, err
		}
		if !eligible {
			return CanonicalTurn{}, ErrAuthorityCorrupt
		}
	}
	return turn, nil
}

func (r canonicalTurnReader) canonicalTurnActiveAncestorLocked(threadID, descendantID, sourceID string) (bool, error) {
	active, err := r.ancestor(threadID, descendantID, sourceID)
	if errors.Is(err, ErrEntryNotFound) || errors.Is(err, ErrInvalidParent) {
		return false, ErrAuthorityCorrupt
	}
	return active, err
}

func (r canonicalTurnReader) latestCanonicalTurnIDLocked(threadID, leafID string) (string, error) {
	if leafID == "" {
		return "", ErrAuthorityCorrupt
	}
	userTurns := make(map[string]struct{})
	seenTurns := make(map[string]struct{})
	for entryID := leafID; entryID != ""; {
		entry, _, err := r.entry(threadID, entryID)
		if err != nil {
			return "", err
		}
		turnID := strings.TrimSpace(entry.TurnID)
		if isCanonicalUserEntry(entry) && turnID != "" {
			userTurns[turnID] = struct{}{}
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			if turnID == "" || strings.TrimSpace(entry.RunID) == "" {
				return "", ErrAuthorityCorrupt
			}
			if _, duplicate := seenTurns[turnID]; duplicate {
				return "", ErrAuthorityCorrupt
			}
			seenTurns[turnID] = struct{}{}
			retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return "", err
			}
			if _, admitted := userTurns[turnID]; admitted || retrySource != nil || entry.Metadata[TurnKindMetadataKey] == TurnKindGoalContinuation {
				return turnID, nil
			}
		}
		entryID = entry.ParentID
	}
	return "", ErrAuthorityCorrupt
}

func (r canonicalTurnReader) canonicalTurnRetryEligibilityLocked(threadID string, turn CanonicalTurn) (bool, error) {
	if turn.Entries[0].Entry.Metadata[TurnKindMetadataKey] == TurnKindGoalContinuation {
		return true, nil
	}
	if turn.RetrySource != nil {
		return r.retrySourceHasRetryEligibleDurableInputLocked(
			threadID, turn.TurnID, turn.RunID, turn.StartedEntryID, *turn.RetrySource,
		)
	}
	for index := len(turn.Entries) - 1; index >= 0; index-- {
		entry := turn.Entries[index].Entry
		if isCanonicalUserEntry(entry) {
			return session.HasRetryEligibleDurableInput(entry.Message), nil
		}
	}
	return false, ErrAuthorityCorrupt
}

func canonicalTurnUserEntryCount(entries []CanonicalTurnPathEntry) int {
	count := 0
	for _, item := range entries {
		if isCanonicalUserEntry(item.Entry) {
			count++
		}
	}
	return count
}
