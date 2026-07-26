package sessiontree

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/internal/session"
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

// CanonicalTurnAdmissionFact validates persisted execution authority when it
// exists for a normal turn and whenever a retry turn omits a user entry.
type CanonicalTurnAdmissionFact struct {
	ThreadID      string
	TurnID        string
	RunID         string
	TurnStartedID string
	UserMessageID string
	BaseLeafID    string
}

func ValidateCanonicalTurnReadAuthority(turn CanonicalTurn, admission CanonicalTurnAdmissionFact) error {
	threadID := strings.TrimSpace(admission.ThreadID)
	turnID := strings.TrimSpace(turn.TurnID)
	runID := strings.TrimSpace(turn.RunID)
	if threadID == "" || turnID == "" || runID == "" ||
		strings.TrimSpace(admission.TurnID) != turnID || strings.TrimSpace(admission.RunID) != runID ||
		strings.TrimSpace(admission.TurnStartedID) != strings.TrimSpace(turn.StartedEntryID) {
		return ErrAuthorityCorrupt
	}
	if err := ValidateCanonicalTurnReadStructure(turn, threadID); err != nil {
		return err
	}
	entries := make([]Entry, 0, len(turn.Entries))
	for _, item := range turn.Entries {
		entries = append(entries, item.Entry)
	}
	retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entries[0])
	if err != nil {
		return err
	}
	userEntryID := ""
	userCount := 0
	for _, entry := range entries {
		if entry.Type == EntryUserMessage {
			userCount++
			userEntryID = entry.ID
		}
	}
	if retrySource != nil {
		if userCount != 0 || strings.TrimSpace(admission.UserMessageID) != "" ||
			strings.TrimSpace(admission.BaseLeafID) != retrySource.EntryID ||
			strings.TrimSpace(entries[0].ParentID) == "" {
			return ErrAuthorityCorrupt
		}
		return nil
	}
	if userCount != 1 || strings.TrimSpace(admission.UserMessageID) != userEntryID ||
		strings.TrimSpace(admission.BaseLeafID) != strings.TrimSpace(entries[0].ParentID) {
		return ErrAuthorityCorrupt
	}
	return nil
}

// ValidateCanonicalTurnReadStructure validates journal shape independently of
// optional execution-admission authority.
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
	turn, err := r.readCanonicalTurnLocked(threadID, turnID, meta.LeafID)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	latestTurnID, err := r.latestCanonicalTurnIDLocked(threadID, meta.LeafID)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	latestTurn, err := r.readCanonicalTurnLocked(threadID, latestTurnID, meta.LeafID)
	if err != nil {
		return CanonicalTurnRead{}, err
	}
	_, through, err := r.canonicalTurnEntryLocked(threadID, meta.LeafID)
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

func (r *MemoryRepo) readCanonicalTurnLocked(threadID, turnID, leafID string) (CanonicalTurn, error) {
	admission, hasAdmission := r.turnAdmissions[turnAdmissionKey(threadID, turnID)]
	ordinals := r.turnEntryOrdinals[threadID][turnID]
	if len(ordinals) == 0 {
		if hasAdmission {
			return CanonicalTurn{}, ErrAuthorityCorrupt
		}
		return CanonicalTurn{}, ErrCanonicalTurnNotFound
	}
	if r.turnEntryCounts[threadID][turnID] != len(ordinals) {
		return CanonicalTurn{}, ErrAuthorityCorrupt
	}
	all := make([]CanonicalTurnPathEntry, 0, len(ordinals))
	var started Entry
	startedCount := 0
	for _, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= len(r.entries[threadID]) {
			return CanonicalTurn{}, ErrAuthorityCorrupt
		}
		entry := r.entries[threadID][ordinal]
		depth, exists := r.entryDepths[threadID][entry.ID]
		if !exists || entry.ThreadID != threadID || strings.TrimSpace(entry.TurnID) != turnID || entry.PathDepth != depth {
			return CanonicalTurn{}, ErrAuthorityCorrupt
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return CanonicalTurn{}, err
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			started = entry
			startedCount++
		}
		all = append(all, CanonicalTurnPathEntry{Entry: cloneEntry(entry), Ordinal: depth})
	}
	if startedCount == 0 {
		return CanonicalTurn{}, ErrAuthorityCorrupt
	}
	if startedCount != 1 {
		return CanonicalTurn{}, ErrAuthorityCorrupt
	}
	if leafID == "" {
		return CanonicalTurn{}, ErrCanonicalTurnNotFound
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
	if !hasAdmission {
		userCount := canonicalTurnUserEntryCount(pathEntries)
		if retrySource == nil && userCount == 0 {
			return CanonicalTurn{}, ErrCanonicalTurnNotFound
		}
		if retrySource != nil || userCount != 1 {
			return CanonicalTurn{}, ErrAuthorityCorrupt
		}
	}
	turn := CanonicalTurn{
		TurnID: turnID, RunID: strings.TrimSpace(started.Metadata["run_id"]),
		StartedEntryID: started.ID, StartedOrdinal: started.PathDepth,
		RetrySource: cloneCanonicalTurnRetrySource(retrySource), Entries: pathEntries,
	}
	if err := ValidateCanonicalTurnReadStructure(turn, threadID); err != nil {
		return CanonicalTurn{}, err
	}
	if hasAdmission {
		if err := ValidateCanonicalTurnReadAuthority(turn, CanonicalTurnAdmissionFact{
			ThreadID: admission.ThreadID, TurnID: admission.TurnID, RunID: admission.RunID,
			TurnStartedID: admission.TurnStartedID, UserMessageID: admission.UserMessageID, BaseLeafID: admission.BaseLeafID,
		}); err != nil {
			return CanonicalTurn{}, err
		}
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

func (r *MemoryRepo) canonicalTurnActiveAncestorLocked(threadID, descendantID, sourceID string) (bool, error) {
	active, err := r.retrySourceIsAncestorLocked(threadID, descendantID, sourceID)
	if errors.Is(err, ErrEntryNotFound) || errors.Is(err, ErrInvalidParent) {
		return false, ErrAuthorityCorrupt
	}
	return active, err
}

func (r *MemoryRepo) latestCanonicalTurnIDLocked(threadID, leafID string) (string, error) {
	if leafID == "" {
		return "", ErrAuthorityCorrupt
	}
	userTurns := make(map[string]struct{})
	seenTurns := make(map[string]struct{})
	for entryID := leafID; entryID != ""; {
		entry, _, err := r.canonicalTurnEntryLocked(threadID, entryID)
		if err != nil {
			return "", err
		}
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type == EntryUserMessage && turnID != "" {
			userTurns[turnID] = struct{}{}
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			if turnID == "" || strings.TrimSpace(entry.Metadata["run_id"]) == "" {
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
			if _, admitted := userTurns[turnID]; admitted || retrySource != nil {
				return turnID, nil
			}
		}
		entryID = entry.ParentID
	}
	return "", ErrAuthorityCorrupt
}

func (r *MemoryRepo) canonicalTurnRetryEligibilityLocked(threadID string, turn CanonicalTurn) (bool, error) {
	if turn.RetrySource != nil {
		return r.retrySourceHasRetryEligibleDurableInputLocked(
			threadID, turn.TurnID, turn.RunID, turn.StartedEntryID, *turn.RetrySource,
		)
	}
	for index := len(turn.Entries) - 1; index >= 0; index-- {
		entry := turn.Entries[index].Entry
		if entry.Type == EntryUserMessage {
			return session.HasRetryEligibleDurableInput(entry.Message), nil
		}
	}
	return false, ErrAuthorityCorrupt
}

func canonicalTurnUserEntryCount(entries []CanonicalTurnPathEntry) int {
	count := 0
	for _, item := range entries {
		if item.Entry.Type == EntryUserMessage {
			count++
		}
	}
	return count
}
