package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/internal/session"
)

type CanonicalTurnBeforeCursor struct {
	EntryID string
}

type CanonicalTurnSinceCursor struct {
	EntryID string
}

type ListCanonicalTurnsOptions struct {
	ThreadID     string
	BeforeCursor *CanonicalTurnBeforeCursor
	SinceCursor  *CanonicalTurnSinceCursor
	Tail         int
	Limit        int
}

type CanonicalTurnPathEntry struct {
	Entry   Entry
	Ordinal int64
}

type CanonicalTurn struct {
	TurnID         string
	RunID          string
	StartedEntryID string
	StartedOrdinal int64
	RetrySource    *CanonicalTurnRetrySource
	Entries        []CanonicalTurnPathEntry
}

type CanonicalTurnRetrySource struct {
	TurnID  string
	EntryID string
}

type CanonicalTurnsPage struct {
	Turns          []CanonicalTurn
	BeforeCursor   *CanonicalTurnBeforeCursor
	SinceCursor    CanonicalTurnSinceCursor
	HasMore        bool
	ThroughOrdinal int64
	LatestTurnID   string
	HasRetryTarget bool
}

type canonicalTurnCandidate struct {
	turnID         string
	runID          string
	startedEntryID string
	ordinal        int64
	startedIndex   int
	retrySource    *CanonicalTurnRetrySource
}

func CanonicalTurnRetrySourceForStartedEntry(entry Entry) (*CanonicalTurnRetrySource, error) {
	if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted {
		return nil, ErrAuthorityCorrupt
	}
	rawTurnID := entry.Metadata[RetrySourceTurnIDMetadataKey]
	rawEntryID := entry.Metadata[RetrySourceEntryIDMetadataKey]
	turnID := strings.TrimSpace(rawTurnID)
	entryID := strings.TrimSpace(rawEntryID)
	if turnID == "" && entryID == "" {
		return nil, nil
	}
	if turnID == "" || entryID == "" || rawTurnID != turnID || rawEntryID != entryID || turnID == strings.TrimSpace(entry.TurnID) {
		return nil, ErrAuthorityCorrupt
	}
	return &CanonicalTurnRetrySource{TurnID: turnID, EntryID: entryID}, nil
}

type CanonicalTurnPageRepo interface {
	ListCanonicalTurns(context.Context, ListCanonicalTurnsOptions) (CanonicalTurnsPage, error)
}

func ValidateListCanonicalTurnsOptions(opts ListCanonicalTurnsOptions) error {
	if strings.TrimSpace(opts.ThreadID) == "" {
		return errors.New("canonical turn page requires thread identity")
	}
	if opts.Tail < 0 || opts.Limit < 0 {
		return errors.New("canonical turn pagination values must be non-negative")
	}
	modes := 0
	if opts.BeforeCursor != nil {
		modes++
		if strings.TrimSpace(opts.BeforeCursor.EntryID) == "" {
			return errors.New("canonical turn before cursor requires entry identity")
		}
	}
	if opts.SinceCursor != nil {
		modes++
		if strings.TrimSpace(opts.SinceCursor.EntryID) == "" {
			return errors.New("canonical turn since cursor requires entry identity")
		}
	}
	if opts.Tail > 0 {
		modes++
	}
	if modes != 1 {
		return errors.New("canonical turn before, since, and tail pagination require exactly one mode")
	}
	if opts.Tail > 0 && opts.Limit > 0 {
		return errors.New("canonical turn tail pagination uses tail as its page size")
	}
	if opts.Tail == 0 && opts.Limit == 0 {
		return errors.New("canonical turn cursor page limit is required")
	}
	return nil
}

func (r *MemoryRepo) ListCanonicalTurns(_ context.Context, opts ListCanonicalTurnsOptions) (CanonicalTurnsPage, error) {
	if err := ValidateListCanonicalTurnsOptions(opts); err != nil {
		return CanonicalTurnsPage{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID := strings.TrimSpace(opts.ThreadID)
	meta, ok := r.threads[threadID]
	if !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return CanonicalTurnsPage{}, ErrThreadDeleted
		}
		return CanonicalTurnsPage{}, ErrThreadNotFound
	}
	if opts.SinceCursor != nil {
		return r.listCanonicalTurnsSinceLocked(threadID, meta.LeafID, strings.TrimSpace(opts.SinceCursor.EntryID), opts.Limit)
	}
	limit := opts.Tail
	startEntryID := meta.LeafID
	if opts.BeforeCursor != nil {
		limit = opts.Limit
		cursorEntry, _, err := r.canonicalTurnEntryLocked(threadID, strings.TrimSpace(opts.BeforeCursor.EntryID))
		if err != nil {
			if errors.Is(err, ErrEntryNotFound) {
				return CanonicalTurnsPage{}, ErrStaleCanonicalTurnCursor
			}
			return CanonicalTurnsPage{}, err
		}
		startEntryID = cursorEntry.ParentID
	}
	return r.listCanonicalTurnsBackwardLocked(threadID, startEntryID, limit)
}

func (r *MemoryRepo) listCanonicalTurnsBackwardLocked(threadID, startEntryID string, limit int) (CanonicalTurnsPage, error) {
	page := CanonicalTurnsPage{SinceCursor: CanonicalTurnSinceCursor{EntryID: startEntryID}}
	if startEntryID == "" {
		return page, nil
	}
	_, startDepth, err := r.canonicalTurnEntryLocked(threadID, startEntryID)
	if err != nil {
		return CanonicalTurnsPage{}, err
	}
	page.ThroughOrdinal = startDepth

	windowNewestFirst := make([]CanonicalTurnPathEntry, 0, limit*4+1)
	candidatesNewestFirst := make([]canonicalTurnCandidate, 0, limit+1)
	userTurns := make(map[string]struct{}, limit+1)
	seenTurns := make(map[string]struct{}, limit+1)
	expectedDepth := startDepth
	for entryID := startEntryID; entryID != ""; {
		entry, depth, err := r.canonicalTurnEntryLocked(threadID, entryID)
		if err != nil {
			return CanonicalTurnsPage{}, err
		}
		if depth != expectedDepth {
			return CanonicalTurnsPage{}, ErrInvalidParent
		}
		expectedDepth--
		windowNewestFirst = append(windowNewestFirst, CanonicalTurnPathEntry{Entry: entry, Ordinal: depth})
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type == EntryUserMessage && turnID != "" {
			userTurns[turnID] = struct{}{}
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if turnID == "" || runID == "" {
				return CanonicalTurnsPage{}, ErrAuthorityCorrupt
			}
			if _, duplicate := seenTurns[turnID]; duplicate {
				return CanonicalTurnsPage{}, ErrAuthorityCorrupt
			}
			seenTurns[turnID] = struct{}{}
			retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return CanonicalTurnsPage{}, err
			}
			if _, visible := userTurns[turnID]; visible || retrySource != nil {
				candidatesNewestFirst = append(candidatesNewestFirst, canonicalTurnCandidate{
					turnID: turnID, runID: runID, startedEntryID: entry.ID, ordinal: depth, retrySource: retrySource,
				})
				if len(candidatesNewestFirst) > limit {
					break
				}
			}
		}
		entryID = entry.ParentID
	}

	page.HasMore = len(candidatesNewestFirst) > limit
	selectedNewestFirst := candidatesNewestFirst
	if page.HasMore {
		selectedNewestFirst = selectedNewestFirst[:limit]
	}
	if len(selectedNewestFirst) == 0 {
		return page, nil
	}
	page.LatestTurnID = selectedNewestFirst[0].turnID
	if page.HasMore {
		oldest := selectedNewestFirst[len(selectedNewestFirst)-1]
		page.BeforeCursor = &CanonicalTurnBeforeCursor{EntryID: oldest.startedEntryID}
	}
	selected := append([]canonicalTurnCandidate(nil), selectedNewestFirst...)
	slices.Reverse(selected)
	built, err := buildCanonicalTurnsPage(threadID, page, selected, windowNewestFirst, true)
	if err != nil {
		return CanonicalTurnsPage{}, err
	}
	return r.withCanonicalPageRetryEligibilityLocked(threadID, built)
}

func (r *MemoryRepo) listCanonicalTurnsSinceLocked(threadID, leafEntryID, anchorEntryID string, limit int) (CanonicalTurnsPage, error) {
	page := CanonicalTurnsPage{SinceCursor: CanonicalTurnSinceCursor{EntryID: anchorEntryID}}
	anchor, anchorDepth, err := r.canonicalTurnEntryLocked(threadID, anchorEntryID)
	if err != nil {
		if errors.Is(err, ErrEntryNotFound) {
			return CanonicalTurnsPage{}, ErrStaleCanonicalTurnCursor
		}
		return CanonicalTurnsPage{}, err
	}
	if leafEntryID == anchorEntryID {
		page.ThroughOrdinal = anchorDepth
		return page, nil
	}
	if leafEntryID == "" {
		return CanonicalTurnsPage{}, ErrStaleCanonicalTurnCursor
	}
	_, leafDepth, err := r.canonicalTurnEntryLocked(threadID, leafEntryID)
	if err != nil {
		return CanonicalTurnsPage{}, err
	}
	if leafDepth <= anchorDepth {
		return CanonicalTurnsPage{}, ErrStaleCanonicalTurnCursor
	}

	deltaNewestFirst := make([]CanonicalTurnPathEntry, 0, leafDepth-anchorDepth)
	expectedDepth := leafDepth
	for entryID := leafEntryID; entryID != anchorEntryID; {
		if entryID == "" {
			return CanonicalTurnsPage{}, ErrStaleCanonicalTurnCursor
		}
		entry, depth, err := r.canonicalTurnEntryLocked(threadID, entryID)
		if err != nil {
			return CanonicalTurnsPage{}, err
		}
		if depth <= anchorDepth {
			return CanonicalTurnsPage{}, ErrStaleCanonicalTurnCursor
		}
		if depth != expectedDepth {
			return CanonicalTurnsPage{}, ErrInvalidParent
		}
		expectedDepth--
		deltaNewestFirst = append(deltaNewestFirst, CanonicalTurnPathEntry{Entry: entry, Ordinal: depth})
		entryID = entry.ParentID
	}
	delta := append([]CanonicalTurnPathEntry(nil), deltaNewestFirst...)
	slices.Reverse(delta)
	window := delta

	type pendingTurn struct {
		candidate canonicalTurnCandidate
		visible   bool
	}
	pending := make(map[string]*pendingTurn)
	candidates := make([]canonicalTurnCandidate, 0, limit+1)
	if canonicalDeltaContinuesTurn(anchor, delta) {
		prefix, continued, err := r.canonicalTurnPrefixLocked(threadID, anchor, anchorDepth)
		if err != nil {
			return CanonicalTurnsPage{}, err
		}
		window = append(prefix, delta...)
		candidates = append(candidates, continued)
	}
	incompleteStartedIndex := -1
	for index, item := range delta {
		entry := item.Entry
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if turnID == "" || runID == "" {
				return CanonicalTurnsPage{}, ErrAuthorityCorrupt
			}
			if _, duplicate := pending[turnID]; duplicate {
				return CanonicalTurnsPage{}, ErrAuthorityCorrupt
			}
			retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return CanonicalTurnsPage{}, err
			}
			state := &pendingTurn{candidate: canonicalTurnCandidate{
				turnID: turnID, runID: runID, startedEntryID: entry.ID, ordinal: item.Ordinal, startedIndex: index, retrySource: retrySource,
			}}
			pending[turnID] = state
			if retrySource != nil {
				state.visible = true
				candidates = append(candidates, state.candidate)
			}
			continue
		}
		if entry.Type == EntryUserMessage && turnID != "" {
			state := pending[turnID]
			if state == nil {
				continue
			}
			if !state.visible {
				state.visible = true
				candidates = append(candidates, state.candidate)
			}
		}
	}
	for _, state := range pending {
		if !state.visible && (incompleteStartedIndex < 0 || state.candidate.startedIndex < incompleteStartedIndex) {
			incompleteStartedIndex = state.candidate.startedIndex
		}
	}

	page.HasMore = len(candidates) > limit
	selected := candidates
	boundaryIndex := len(delta) - 1
	if page.HasMore {
		selected = selected[:limit]
		boundaryIndex = candidates[limit].startedIndex - 1
	} else if incompleteStartedIndex >= 0 {
		boundaryIndex = incompleteStartedIndex - 1
	}
	if boundaryIndex >= 0 {
		page.SinceCursor.EntryID = delta[boundaryIndex].Entry.ID
		page.ThroughOrdinal = delta[boundaryIndex].Ordinal
	} else {
		page.ThroughOrdinal = anchorDepth
	}
	if len(selected) == 0 {
		return page, nil
	}
	page.LatestTurnID = selected[len(selected)-1].turnID
	built, err := buildCanonicalTurnsPage(threadID, page, selected, window, false)
	if err != nil {
		return CanonicalTurnsPage{}, err
	}
	return r.withCanonicalPageRetryEligibilityLocked(threadID, built)
}

func canonicalDeltaContinuesTurn(anchor Entry, delta []CanonicalTurnPathEntry) bool {
	turnID := strings.TrimSpace(anchor.TurnID)
	if turnID == "" {
		return false
	}
	for _, item := range delta {
		if strings.TrimSpace(item.Entry.TurnID) == turnID {
			return true
		}
	}
	return false
}

func (r *MemoryRepo) canonicalTurnPrefixLocked(threadID string, anchor Entry, anchorDepth int64) ([]CanonicalTurnPathEntry, canonicalTurnCandidate, error) {
	turnID := strings.TrimSpace(anchor.TurnID)
	if turnID == "" || anchorDepth <= 0 {
		return nil, canonicalTurnCandidate{}, ErrAuthorityCorrupt
	}
	prefixNewestFirst := make([]CanonicalTurnPathEntry, 0, 8)
	expectedDepth := anchorDepth
	for entryID := anchor.ID; entryID != ""; {
		entry, depth, err := r.canonicalTurnEntryLocked(threadID, entryID)
		if err != nil {
			return nil, canonicalTurnCandidate{}, err
		}
		if depth != expectedDepth {
			return nil, canonicalTurnCandidate{}, ErrInvalidParent
		}
		if strings.TrimSpace(entry.TurnID) != turnID {
			return nil, canonicalTurnCandidate{}, ErrAuthorityCorrupt
		}
		prefixNewestFirst = append(prefixNewestFirst, CanonicalTurnPathEntry{Entry: entry, Ordinal: depth})
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if runID == "" {
				return nil, canonicalTurnCandidate{}, ErrAuthorityCorrupt
			}
			retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return nil, canonicalTurnCandidate{}, err
			}
			prefix := append([]CanonicalTurnPathEntry(nil), prefixNewestFirst...)
			slices.Reverse(prefix)
			visible := retrySource != nil
			for _, item := range prefix {
				if item.Entry.Type == EntryUserMessage && strings.TrimSpace(item.Entry.TurnID) == turnID {
					visible = true
					break
				}
			}
			if !visible {
				return nil, canonicalTurnCandidate{}, ErrAuthorityCorrupt
			}
			return prefix, canonicalTurnCandidate{
				turnID: turnID, runID: runID, startedEntryID: entry.ID, ordinal: depth, retrySource: retrySource,
			}, nil
		}
		entryID = entry.ParentID
		expectedDepth--
	}
	return nil, canonicalTurnCandidate{}, ErrAuthorityCorrupt
}

func (r *MemoryRepo) canonicalTurnEntryLocked(threadID, entryID string) (Entry, int64, error) {
	ordinal, ok := r.entryOrdinals[threadID][entryID]
	entries := r.entries[threadID]
	if !ok || ordinal < 0 || ordinal >= len(entries) {
		return Entry{}, 0, ErrEntryNotFound
	}
	entry := entries[ordinal]
	depth, ok := r.entryDepths[threadID][entryID]
	if !ok || depth <= 0 || entry.ID != entryID || entry.ThreadID != threadID || entry.PathDepth != depth {
		return Entry{}, 0, ErrAuthorityCorrupt
	}
	if err := ValidateEntryIntegrity(entry); err != nil {
		return Entry{}, 0, err
	}
	return cloneEntry(entry), depth, nil
}

func buildCanonicalTurnsPage(threadID string, page CanonicalTurnsPage, selected []canonicalTurnCandidate, window []CanonicalTurnPathEntry, windowNewestFirst bool) (CanonicalTurnsPage, error) {
	page.Turns = make([]CanonicalTurn, len(selected))
	selectedByTurnID := make(map[string]int, len(selected))
	for index, item := range selected {
		page.Turns[index] = CanonicalTurn{
			TurnID: item.turnID, RunID: item.runID, StartedEntryID: item.startedEntryID, StartedOrdinal: item.ordinal,
			RetrySource: cloneCanonicalTurnRetrySource(item.retrySource),
		}
		selectedByTurnID[item.turnID] = index
	}
	appendEntry := func(item CanonicalTurnPathEntry) {
		turnIndex, ok := selectedByTurnID[item.Entry.TurnID]
		if ok {
			page.Turns[turnIndex].Entries = append(page.Turns[turnIndex].Entries, item)
		}
	}
	if windowNewestFirst {
		for index := len(window) - 1; index >= 0; index-- {
			appendEntry(window[index])
		}
	} else {
		for _, item := range window {
			appendEntry(item)
		}
	}
	for index := range page.Turns {
		turn := &page.Turns[index]
		turn.Entries = canonicalTurnPathEntriesForRead(turn.Entries)
		entries := make([]Entry, 0, len(turn.Entries))
		for _, item := range turn.Entries {
			entries = append(entries, item.Entry)
		}
		if err := ValidateCanonicalTurnEntries(entries, threadID, turn.TurnID, turn.RunID); err != nil {
			return CanonicalTurnsPage{}, fmt.Errorf("validate canonical turn page %q: %w", turn.TurnID, err)
		}
	}
	return page, nil
}

func (r *MemoryRepo) withCanonicalPageRetryEligibilityLocked(threadID string, page CanonicalTurnsPage) (CanonicalTurnsPage, error) {
	if len(page.Turns) == 0 {
		return page, nil
	}
	latest := page.Turns[len(page.Turns)-1]
	if latest.TurnID != page.LatestTurnID {
		return CanonicalTurnsPage{}, ErrAuthorityCorrupt
	}
	if latest.RetrySource == nil {
		for index := len(latest.Entries) - 1; index >= 0; index-- {
			entry := latest.Entries[index].Entry
			if entry.Type != EntryUserMessage {
				continue
			}
			if entry.Message.Role != session.User {
				return CanonicalTurnsPage{}, ErrAuthorityCorrupt
			}
			page.HasRetryTarget = session.HasRetryEligibleDurableInput(entry.Message)
			return page, nil
		}
		return CanonicalTurnsPage{}, ErrAuthorityCorrupt
	}
	eligible, err := r.retrySourceHasRetryEligibleDurableInputLocked(
		threadID, latest.TurnID, latest.RunID, latest.StartedEntryID, *latest.RetrySource,
	)
	if err != nil {
		return CanonicalTurnsPage{}, err
	}
	page.HasRetryTarget = eligible
	return page, nil
}

func (r *MemoryRepo) retrySourceHasRetryEligibleDurableInputLocked(threadID, retryTurnID, retryRunID, retryStartedEntryID string, source CanonicalTurnRetrySource) (bool, error) {
	visited := make(map[string]struct{})
	for {
		turnID := strings.TrimSpace(source.TurnID)
		entryID := strings.TrimSpace(source.EntryID)
		if turnID == "" || entryID == "" {
			return false, ErrAuthorityCorrupt
		}
		key := turnID + "\x00" + entryID
		if _, duplicate := visited[key]; duplicate {
			return false, ErrAuthorityCorrupt
		}
		visited[key] = struct{}{}
		retryStarted, _, err := r.canonicalTurnEntryLocked(threadID, retryStartedEntryID)
		if err != nil {
			return false, err
		}
		if err := ValidateRetryStartedEntry(retryStarted, threadID, retryTurnID, retryRunID, retryStartedEntryID, source); err != nil {
			return false, err
		}
		admission, ok := r.turnAdmissions[turnAdmissionKey(threadID, retryTurnID)]
		if !ok || admission.ThreadID != threadID || admission.TurnID != retryTurnID || admission.RunID != retryRunID ||
			admission.TurnStartedID != retryStartedEntryID || admission.UserMessageID != "" || admission.BaseLeafID != entryID {
			return false, ErrAuthorityCorrupt
		}
		ordinals := r.turnEntryOrdinals[threadID][turnID]
		if len(ordinals) == 0 || r.turnEntryCounts[threadID][turnID] != len(ordinals) {
			return false, ErrAuthorityCorrupt
		}
		entries := r.entries[threadID]
		canonical := make([]Entry, 0, len(ordinals))
		for _, ordinal := range ordinals {
			if ordinal < 0 || ordinal >= len(entries) {
				return false, ErrAuthorityCorrupt
			}
			entry := entries[ordinal]
			if entry.ThreadID != threadID || strings.TrimSpace(entry.TurnID) != turnID || entry.PathDepth != r.entryDepths[threadID][entry.ID] {
				return false, ErrAuthorityCorrupt
			}
			if err := ValidateEntryIntegrity(entry); err != nil {
				return false, err
			}
			canonical = append(canonical, cloneEntry(entry))
		}
		canonical = CanonicalTurnEntriesForRead(canonical)
		eligible, next, startedEntryID, runID, err := ValidateCanonicalRetrySourceTurn(canonical, threadID, source)
		if err != nil {
			return false, err
		}
		ancestor, err := r.retrySourceIsAncestorLocked(threadID, retryStarted.ParentID, entryID)
		if err != nil {
			return false, err
		}
		if !ancestor {
			return false, ErrAuthorityCorrupt
		}
		if next == nil {
			return eligible, nil
		}
		retryTurnID, retryRunID, retryStartedEntryID, source = turnID, runID, startedEntryID, *next
	}
}

func ValidateRetryStartedEntry(entry Entry, threadID, turnID, runID, startedEntryID string, source CanonicalTurnRetrySource) error {
	if err := ValidateEntryIntegrity(entry); err != nil {
		return err
	}
	if entry.ID != startedEntryID || entry.ThreadID != threadID || entry.TurnID != turnID ||
		entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted || strings.TrimSpace(entry.ParentID) == "" ||
		entry.Metadata["run_id"] != runID {
		return ErrAuthorityCorrupt
	}
	stored, err := CanonicalTurnRetrySourceForStartedEntry(entry)
	if err != nil {
		return err
	}
	if stored == nil || stored.TurnID != strings.TrimSpace(source.TurnID) || stored.EntryID != strings.TrimSpace(source.EntryID) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func (r *MemoryRepo) retrySourceIsAncestorLocked(threadID, descendantID, sourceID string) (bool, error) {
	sourceDepth, ok := r.entryDepths[threadID][sourceID]
	if !ok || sourceDepth <= 0 {
		return false, ErrAuthorityCorrupt
	}
	descendantDepth, ok := r.entryDepths[threadID][descendantID]
	if !ok || descendantDepth <= 0 {
		return false, ErrAuthorityCorrupt
	}
	if descendantDepth < sourceDepth {
		return false, nil
	}
	entryID := descendantID
	for descendantDepth > sourceDepth {
		entry, depth, err := r.canonicalTurnEntryLocked(threadID, entryID)
		if err != nil {
			return false, err
		}
		if depth != descendantDepth || strings.TrimSpace(entry.ParentID) == "" {
			return false, ErrAuthorityCorrupt
		}
		entryID = entry.ParentID
		descendantDepth--
	}
	return entryID == sourceID, nil
}

func ValidateCanonicalRetrySourceTurn(entries []Entry, threadID string, source CanonicalTurnRetrySource) (bool, *CanonicalTurnRetrySource, string, string, error) {
	turnID := strings.TrimSpace(source.TurnID)
	entryID := strings.TrimSpace(source.EntryID)
	if turnID == "" || entryID == "" || len(entries) == 0 {
		return false, nil, "", "", ErrAuthorityCorrupt
	}
	started := entries[0]
	if started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted {
		return false, nil, "", "", ErrAuthorityCorrupt
	}
	runID := strings.TrimSpace(started.Metadata["run_id"])
	if err := ValidateCanonicalTurnEntries(entries, threadID, turnID, runID); err != nil {
		return false, nil, "", "", err
	}
	retrySource, err := CanonicalTurnRetrySourceForStartedEntry(started)
	if err != nil {
		return false, nil, "", "", err
	}
	sourceIndex := -1
	userIndex := -1
	for index, entry := range entries {
		if entry.ID == entryID {
			if sourceIndex >= 0 {
				return false, nil, "", "", ErrAuthorityCorrupt
			}
			sourceIndex = index
		}
		if entry.Type == EntryUserMessage {
			if userIndex >= 0 || entry.Message.Role != session.User {
				return false, nil, "", "", ErrAuthorityCorrupt
			}
			userIndex = index
		}
	}
	if sourceIndex < 0 {
		return false, nil, "", "", ErrAuthorityCorrupt
	}
	sourceEntry := entries[sourceIndex]
	if sourceEntry.Type == EntryUserMessage {
		if userIndex != sourceIndex || retrySource != nil {
			return false, nil, "", "", ErrAuthorityCorrupt
		}
		return session.HasRetryEligibleDurableInput(sourceEntry.Message), nil, started.ID, runID, nil
	}
	if sourceIndex+1 >= len(entries) {
		return false, nil, "", "", ErrAuthorityCorrupt
	}
	savePoint := entries[sourceIndex+1]
	if savePoint.Type != EntryTurnMarker || savePoint.TurnStatus != TurnSavePoint || savePoint.ParentID != sourceEntry.ID || savePoint.TurnID != turnID {
		return false, nil, "", "", ErrAuthorityCorrupt
	}
	if userIndex >= 0 {
		if userIndex > sourceIndex || retrySource != nil {
			return false, nil, "", "", ErrAuthorityCorrupt
		}
		return session.HasRetryEligibleDurableInput(entries[userIndex].Message), nil, started.ID, runID, nil
	}
	if retrySource == nil {
		return false, nil, "", "", ErrAuthorityCorrupt
	}
	return false, retrySource, started.ID, runID, nil
}

type forkRetryIndexedEntry struct {
	entry Entry
	index int
}

// ValidateForkRetryAuthorityPath validates the complete staged destination path
// before a fork publishes entries or retry admission facts.
func ValidateForkRetryAuthorityPath(path []Entry, threadID string) error {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return ErrAuthorityCorrupt
	}
	byID := make(map[string]forkRetryIndexedEntry, len(path))
	byTurn := make(map[string][]Entry)
	for index, entry := range path {
		if entry.ThreadID != threadID || strings.TrimSpace(entry.ID) == "" {
			return ErrAuthorityCorrupt
		}
		if _, duplicate := byID[entry.ID]; duplicate {
			return ErrAuthorityCorrupt
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return err
		}
		if index == 0 {
			if entry.ParentID != "" {
				return ErrAuthorityCorrupt
			}
		} else if entry.ParentID != path[index-1].ID {
			return ErrAuthorityCorrupt
		}
		byID[entry.ID] = forkRetryIndexedEntry{entry: entry, index: index}
		if turnID := strings.TrimSpace(entry.TurnID); turnID != "" {
			byTurn[turnID] = append(byTurn[turnID], entry)
		}
	}
	for index, entry := range path {
		if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted {
			continue
		}
		source, err := CanonicalTurnRetrySourceForStartedEntry(entry)
		if err != nil {
			return err
		}
		if source == nil {
			continue
		}
		if err := validateForkRetryAuthorityChain(byID, byTurn, entry, index, *source); err != nil {
			return err
		}
	}
	return nil
}

func validateForkRetryAuthorityChain(byID map[string]forkRetryIndexedEntry, byTurn map[string][]Entry, retryStarted Entry, retryStartedIndex int, source CanonicalTurnRetrySource) error {
	visited := make(map[string]struct{})
	for {
		turnID := strings.TrimSpace(source.TurnID)
		entryID := strings.TrimSpace(source.EntryID)
		key := turnID + "\x00" + entryID
		if turnID == "" || entryID == "" {
			return ErrAuthorityCorrupt
		}
		if _, duplicate := visited[key]; duplicate {
			return ErrAuthorityCorrupt
		}
		visited[key] = struct{}{}
		if err := ValidateRetryStartedEntry(
			retryStarted, retryStarted.ThreadID, retryStarted.TurnID, strings.TrimSpace(retryStarted.Metadata["run_id"]), retryStarted.ID, source,
		); err != nil {
			return err
		}
		target, ok := byID[entryID]
		if !ok || target.index >= retryStartedIndex {
			return ErrAuthorityCorrupt
		}
		entries := CanonicalTurnEntriesForRead(byTurn[turnID])
		eligible, next, sourceStartedID, sourceRunID, err := ValidateCanonicalRetrySourceTurn(entries, retryStarted.ThreadID, source)
		if err != nil {
			return err
		}
		if next == nil {
			if !eligible {
				return ErrAuthorityCorrupt
			}
			return nil
		}
		sourceStarted, ok := byID[sourceStartedID]
		if !ok || sourceStarted.index >= retryStartedIndex || strings.TrimSpace(sourceRunID) == "" {
			return ErrAuthorityCorrupt
		}
		retryStarted = sourceStarted.entry
		retryStartedIndex = sourceStarted.index
		source = *next
	}
}

func (r *MemoryRepo) rebuildRetryAdmissionFactsLocked(threadID string, entries []Entry) error {
	// FileRepo has no atomic turn-admission capability. On reopen it derives the
	// read-only retry facts needed by canonical page and fork validation from its
	// durable journal. MemoryRepo and SQLite runtime forks validate their stored
	// admission ledgers before constructing destination authority.
	for _, entry := range entries {
		if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted {
			continue
		}
		retrySource, err := CanonicalTurnRetrySourceForStartedEntry(entry)
		if err != nil {
			return err
		}
		if retrySource == nil {
			continue
		}
		runID := strings.TrimSpace(entry.Metadata["run_id"])
		if runID == "" {
			return ErrAuthorityCorrupt
		}
		r.turnAdmissions[turnAdmissionKey(threadID, entry.TurnID)] = turnAdmissionLedger{
			ThreadID: threadID, TurnID: entry.TurnID, RunID: runID, TurnStartedID: entry.ID, BaseLeafID: retrySource.EntryID,
		}
	}
	return nil
}

func cloneCanonicalTurnRetrySource(source *CanonicalTurnRetrySource) *CanonicalTurnRetrySource {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func canonicalTurnPathEntriesForRead(entries []CanonicalTurnPathEntry) []CanonicalTurnPathEntry {
	hasExecutionTerminal := false
	for _, item := range entries {
		entry := item.Entry
		if entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) && entry.Metadata["authority_kind"] != "branch_boundary" {
			hasExecutionTerminal = true
			break
		}
	}
	if !hasExecutionTerminal {
		return entries
	}
	filtered := make([]CanonicalTurnPathEntry, 0, len(entries))
	for _, item := range entries {
		entry := item.Entry
		if entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) && entry.Metadata["authority_kind"] == "branch_boundary" {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}
