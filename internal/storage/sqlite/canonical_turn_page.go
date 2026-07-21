package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

const minimumCanonicalAncestorChunk = 64

type sqliteCanonicalCandidate struct {
	turnID         string
	runID          string
	startedEntryID string
	ordinal        int64
	startedIndex   int
	retrySource    *sessiontree.CanonicalTurnRetrySource
}

func (s *Store) ListCanonicalTurns(ctx context.Context, opts sessiontree.ListCanonicalTurnsOptions) (sessiontree.CanonicalTurnsPage, error) {
	if err := sessiontree.ValidateListCanonicalTurnsOptions(opts); err != nil {
		return sessiontree.CanonicalTurnsPage{}, err
	}
	var page sessiontree.CanonicalTurnsPage
	err := s.withRead(ctx, func(q sqlRunner) error {
		var err error
		page, err = listSQLiteCanonicalTurnsWithRunner(ctx, q, opts)
		return err
	})
	return page, err
}

func listSQLiteCanonicalTurnsWithRunner(ctx context.Context, q sqlRunner, opts sessiontree.ListCanonicalTurnsOptions) (sessiontree.CanonicalTurnsPage, error) {
	threadID := strings.TrimSpace(opts.ThreadID)
	meta, err := loadThread(ctx, q, threadID)
	if errors.Is(err, sessiontree.ErrThreadNotFound) {
		if _, tombstoneErr := loadThreadTombstone(ctx, q, threadID); tombstoneErr == nil {
			return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrThreadDeleted
		} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
			return sessiontree.CanonicalTurnsPage{}, tombstoneErr
		}
	}
	if err != nil {
		return sessiontree.CanonicalTurnsPage{}, err
	}
	if opts.SinceCursor != nil {
		return listSQLiteCanonicalTurnsSince(ctx, q, threadID, meta.LeafID, strings.TrimSpace(opts.SinceCursor.EntryID), opts.Limit)
	}
	limit := opts.Tail
	startEntryID := strings.TrimSpace(meta.LeafID)
	if opts.BeforeCursor != nil {
		limit = opts.Limit
		cursor, cursorErr := loadEntry(ctx, q, threadID, strings.TrimSpace(opts.BeforeCursor.EntryID))
		if errors.Is(cursorErr, sql.ErrNoRows) {
			return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrStaleCanonicalTurnCursor
		}
		if cursorErr != nil {
			return sessiontree.CanonicalTurnsPage{}, cursorErr
		}
		startEntryID = cursor.ParentID
	}
	return listSQLiteCanonicalTurnsBackward(ctx, q, threadID, startEntryID, limit)
}

func listSQLiteCanonicalTurnsBackward(ctx context.Context, q sqlRunner, threadID, startEntryID string, limit int) (sessiontree.CanonicalTurnsPage, error) {
	page := sessiontree.CanonicalTurnsPage{SinceCursor: sessiontree.CanonicalTurnSinceCursor{EntryID: startEntryID}}
	if startEntryID == "" {
		return page, nil
	}
	chunkSize := (limit + 1) * 8
	if chunkSize < minimumCanonicalAncestorChunk {
		chunkSize = minimumCanonicalAncestorChunk
	}
	windowNewestFirst := make([]sessiontree.CanonicalTurnPathEntry, 0, chunkSize)
	candidatesNewestFirst := make([]sqliteCanonicalCandidate, 0, limit+1)
	userTurns := make(map[string]struct{}, limit+1)
	seenTurns := make(map[string]struct{}, limit+1)
	nextEntryID := startEntryID
	expectedStartDepth := int64(0)
	for nextEntryID != "" {
		entries, next, err := loadSQLiteAncestorChunk(ctx, q, threadID, nextEntryID, chunkSize)
		if err != nil {
			return sessiontree.CanonicalTurnsPage{}, err
		}
		if page.ThroughOrdinal == 0 {
			page.ThroughOrdinal = entries[0].PathDepth
		}
		if expectedStartDepth != 0 && entries[0].PathDepth != expectedStartDepth {
			return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrInvalidParent
		}
		for _, entry := range entries {
			windowNewestFirst = append(windowNewestFirst, sessiontree.CanonicalTurnPathEntry{Entry: entry, Ordinal: entry.PathDepth})
			turnID := strings.TrimSpace(entry.TurnID)
			if entry.Type == sessiontree.EntryUserMessage && turnID != "" {
				userTurns[turnID] = struct{}{}
			}
			if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
				continue
			}
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if turnID == "" || runID == "" {
				return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
			}
			if _, duplicate := seenTurns[turnID]; duplicate {
				return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
			}
			seenTurns[turnID] = struct{}{}
			retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return sessiontree.CanonicalTurnsPage{}, err
			}
			if _, visible := userTurns[turnID]; visible || retrySource != nil {
				candidatesNewestFirst = append(candidatesNewestFirst, sqliteCanonicalCandidate{
					turnID: turnID, runID: runID, startedEntryID: entry.ID, ordinal: entry.PathDepth, retrySource: retrySource,
				})
				if len(candidatesNewestFirst) > limit {
					nextEntryID = ""
					break
				}
			}
		}
		if len(candidatesNewestFirst) > limit {
			break
		}
		nextEntryID = next
		if nextEntryID != "" {
			expectedStartDepth = entries[len(entries)-1].PathDepth - 1
			if expectedStartDepth <= 0 {
				return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrInvalidParent
			}
		}
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
		page.BeforeCursor = &sessiontree.CanonicalTurnBeforeCursor{EntryID: oldest.startedEntryID}
	}
	selected := append([]sqliteCanonicalCandidate(nil), selectedNewestFirst...)
	slices.Reverse(selected)
	built, err := buildSQLiteCanonicalTurnsPage(threadID, page, selected, windowNewestFirst, true)
	if err != nil {
		return sessiontree.CanonicalTurnsPage{}, err
	}
	return withSQLiteCanonicalPageRetryEligibility(ctx, q, threadID, built)
}

func listSQLiteCanonicalTurnsSince(ctx context.Context, q sqlRunner, threadID, leafEntryID, anchorEntryID string, limit int) (sessiontree.CanonicalTurnsPage, error) {
	page := sessiontree.CanonicalTurnsPage{SinceCursor: sessiontree.CanonicalTurnSinceCursor{EntryID: anchorEntryID}}
	anchor, err := loadEntry(ctx, q, threadID, anchorEntryID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrStaleCanonicalTurnCursor
	}
	if err != nil {
		return sessiontree.CanonicalTurnsPage{}, err
	}
	if anchor.PathDepth <= 0 {
		return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
	}
	if err := sessiontree.ValidateEntryIntegrity(anchor); err != nil {
		return sessiontree.CanonicalTurnsPage{}, err
	}
	anchorDepth := anchor.PathDepth
	if leafEntryID == anchorEntryID {
		page.ThroughOrdinal = anchorDepth
		return page, nil
	}
	if leafEntryID == "" {
		return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrStaleCanonicalTurnCursor
	}

	chunkSize := (limit + 1) * 8
	if chunkSize < minimumCanonicalAncestorChunk {
		chunkSize = minimumCanonicalAncestorChunk
	}
	deltaNewestFirst := make([]sessiontree.CanonicalTurnPathEntry, 0, chunkSize)
	nextEntryID := leafEntryID
	foundAnchor := false
	expectedStartDepth := int64(0)
	for nextEntryID != "" && !foundAnchor {
		entries, next, err := loadSQLiteAncestorChunk(ctx, q, threadID, nextEntryID, chunkSize)
		if err != nil {
			return sessiontree.CanonicalTurnsPage{}, err
		}
		if expectedStartDepth != 0 && entries[0].PathDepth != expectedStartDepth {
			return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrInvalidParent
		}
		for _, entry := range entries {
			if entry.ID == anchorEntryID {
				foundAnchor = true
				break
			}
			if entry.PathDepth <= anchorDepth {
				return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrStaleCanonicalTurnCursor
			}
			deltaNewestFirst = append(deltaNewestFirst, sessiontree.CanonicalTurnPathEntry{Entry: entry, Ordinal: entry.PathDepth})
		}
		nextEntryID = next
		if nextEntryID != "" {
			expectedStartDepth = entries[len(entries)-1].PathDepth - 1
		}
	}
	if !foundAnchor {
		return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrStaleCanonicalTurnCursor
	}
	delta := append([]sessiontree.CanonicalTurnPathEntry(nil), deltaNewestFirst...)
	slices.Reverse(delta)
	window := delta

	type pendingTurn struct {
		candidate sqliteCanonicalCandidate
		visible   bool
	}
	pending := make(map[string]*pendingTurn)
	candidates := make([]sqliteCanonicalCandidate, 0, limit+1)
	if sqliteCanonicalDeltaContinuesTurn(anchor, delta) {
		prefix, continued, err := loadSQLiteCanonicalTurnPrefix(ctx, q, threadID, anchor)
		if err != nil {
			return sessiontree.CanonicalTurnsPage{}, err
		}
		window = append(prefix, delta...)
		candidates = append(candidates, continued)
	}
	incompleteStartedIndex := -1
	for index, item := range delta {
		entry := item.Entry
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if turnID == "" || runID == "" {
				return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
			}
			if _, duplicate := pending[turnID]; duplicate {
				return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
			}
			retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return sessiontree.CanonicalTurnsPage{}, err
			}
			state := &pendingTurn{candidate: sqliteCanonicalCandidate{
				turnID: turnID, runID: runID, startedEntryID: entry.ID, ordinal: item.Ordinal, startedIndex: index, retrySource: retrySource,
			}}
			pending[turnID] = state
			if retrySource != nil {
				state.visible = true
				candidates = append(candidates, state.candidate)
			}
			continue
		}
		if entry.Type == sessiontree.EntryUserMessage && turnID != "" {
			state := pending[turnID]
			if state != nil && !state.visible {
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
	built, err := buildSQLiteCanonicalTurnsPage(threadID, page, selected, window, false)
	if err != nil {
		return sessiontree.CanonicalTurnsPage{}, err
	}
	return withSQLiteCanonicalPageRetryEligibility(ctx, q, threadID, built)
}

func sqliteCanonicalDeltaContinuesTurn(anchor sessiontree.Entry, delta []sessiontree.CanonicalTurnPathEntry) bool {
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

func loadSQLiteCanonicalTurnPrefix(ctx context.Context, q sqlRunner, threadID string, anchor sessiontree.Entry) ([]sessiontree.CanonicalTurnPathEntry, sqliteCanonicalCandidate, error) {
	turnID := strings.TrimSpace(anchor.TurnID)
	if turnID == "" || anchor.PathDepth <= 0 {
		return nil, sqliteCanonicalCandidate{}, sessiontree.ErrAuthorityCorrupt
	}
	prefixNewestFirst := make([]sessiontree.CanonicalTurnPathEntry, 0, minimumCanonicalAncestorChunk)
	nextEntryID := anchor.ID
	expectedStartDepth := anchor.PathDepth
	for nextEntryID != "" {
		entries, next, err := loadSQLiteAncestorChunk(ctx, q, threadID, nextEntryID, minimumCanonicalAncestorChunk)
		if err != nil {
			return nil, sqliteCanonicalCandidate{}, err
		}
		if entries[0].PathDepth != expectedStartDepth {
			return nil, sqliteCanonicalCandidate{}, sessiontree.ErrInvalidParent
		}
		for _, entry := range entries {
			if strings.TrimSpace(entry.TurnID) != turnID {
				return nil, sqliteCanonicalCandidate{}, sessiontree.ErrAuthorityCorrupt
			}
			prefixNewestFirst = append(prefixNewestFirst, sessiontree.CanonicalTurnPathEntry{Entry: entry, Ordinal: entry.PathDepth})
			if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
				continue
			}
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if runID == "" {
				return nil, sqliteCanonicalCandidate{}, sessiontree.ErrAuthorityCorrupt
			}
			retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return nil, sqliteCanonicalCandidate{}, err
			}
			prefix := append([]sessiontree.CanonicalTurnPathEntry(nil), prefixNewestFirst...)
			slices.Reverse(prefix)
			visible := retrySource != nil
			for _, item := range prefix {
				if item.Entry.Type == sessiontree.EntryUserMessage && strings.TrimSpace(item.Entry.TurnID) == turnID {
					visible = true
					break
				}
			}
			if !visible {
				return nil, sqliteCanonicalCandidate{}, sessiontree.ErrAuthorityCorrupt
			}
			return prefix, sqliteCanonicalCandidate{
				turnID: turnID, runID: runID, startedEntryID: entry.ID, ordinal: entry.PathDepth, retrySource: retrySource,
			}, nil
		}
		nextEntryID = next
		if nextEntryID != "" {
			expectedStartDepth = entries[len(entries)-1].PathDepth - 1
			if expectedStartDepth <= 0 {
				return nil, sqliteCanonicalCandidate{}, sessiontree.ErrInvalidParent
			}
		}
	}
	return nil, sqliteCanonicalCandidate{}, sessiontree.ErrAuthorityCorrupt
}

func loadSQLiteAncestorChunk(ctx context.Context, q sqlRunner, threadID, startEntryID string, limit int) ([]sessiontree.Entry, string, error) {
	rows, err := q.QueryContext(ctx, sqliteAncestorChunkQuery(), threadID, startEntryID, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	entries := make([]sessiontree.Entry, 0, limit)
	seen := make(map[string]struct{}, limit)
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, "", err
		}
		if entry.ThreadID != threadID || entry.PathDepth <= 0 {
			return nil, "", sessiontree.ErrAuthorityCorrupt
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return nil, "", sessiontree.ErrInvalidParent
		}
		if len(entries) > 0 {
			newer := entries[len(entries)-1]
			if newer.ParentID != entry.ID || newer.PathDepth != entry.PathDepth+1 {
				return nil, "", sessiontree.ErrInvalidParent
			}
		}
		if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
			return nil, "", err
		}
		seen[entry.ID] = struct{}{}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(entries) == 0 {
		return nil, "", sessiontree.ErrEntryNotFound
	}
	last := entries[len(entries)-1]
	if last.ParentID == "" {
		if last.PathDepth != 1 {
			return nil, "", sessiontree.ErrInvalidParent
		}
		return entries, "", nil
	}
	if len(entries) < limit {
		return nil, "", sessiontree.ErrInvalidParent
	}
	return entries, last.ParentID, nil
}

func sqliteAncestorChunkQuery() string {
	return `WITH RECURSIVE ancestors(step, ` + entryColumns + `) AS (
		SELECT 1, ` + qualifiedEntryColumns("entry") + `
		FROM entries entry
		WHERE entry.thread_id = ? AND entry.id = ?
		UNION ALL
		SELECT ancestors.step + 1, ` + qualifiedEntryColumns("parent") + `
		FROM entries parent
		JOIN ancestors ON ancestors.thread_id = parent.thread_id AND ancestors.parent_id = parent.id
		WHERE ancestors.step < ?
	)
	SELECT ` + entryColumns + ` FROM ancestors`
}

func buildSQLiteCanonicalTurnsPage(threadID string, page sessiontree.CanonicalTurnsPage, selected []sqliteCanonicalCandidate, window []sessiontree.CanonicalTurnPathEntry, windowNewestFirst bool) (sessiontree.CanonicalTurnsPage, error) {
	page.Turns = make([]sessiontree.CanonicalTurn, len(selected))
	selectedByTurnID := make(map[string]int, len(selected))
	for index, item := range selected {
		page.Turns[index] = sessiontree.CanonicalTurn{
			TurnID: item.turnID, RunID: item.runID, StartedEntryID: item.startedEntryID, StartedOrdinal: item.ordinal,
			RetrySource: cloneSQLiteCanonicalTurnRetrySource(item.retrySource),
		}
		selectedByTurnID[item.turnID] = index
	}
	appendEntry := func(item sessiontree.CanonicalTurnPathEntry) {
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
		turn.Entries = canonicalSQLiteTurnPathEntriesForRead(turn.Entries)
		entries := make([]sessiontree.Entry, 0, len(turn.Entries))
		for _, item := range turn.Entries {
			entries = append(entries, item.Entry)
		}
		if err := sessiontree.ValidateCanonicalTurnEntries(entries, threadID, turn.TurnID, turn.RunID); err != nil {
			return sessiontree.CanonicalTurnsPage{}, fmt.Errorf("validate sqlite canonical turn page %q: %w", turn.TurnID, err)
		}
	}
	return page, nil
}

func withSQLiteCanonicalPageRetryEligibility(ctx context.Context, q sqlRunner, threadID string, page sessiontree.CanonicalTurnsPage) (sessiontree.CanonicalTurnsPage, error) {
	if len(page.Turns) == 0 {
		return page, nil
	}
	latest := page.Turns[len(page.Turns)-1]
	if latest.TurnID != page.LatestTurnID {
		return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
	}
	if latest.RetrySource == nil {
		for index := len(latest.Entries) - 1; index >= 0; index-- {
			entry := latest.Entries[index].Entry
			if entry.Type != sessiontree.EntryUserMessage {
				continue
			}
			if entry.Message.Role != session.User {
				return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
			}
			page.HasRetryTarget = session.HasRetryEligibleDurableInput(entry.Message)
			return page, nil
		}
		return sessiontree.CanonicalTurnsPage{}, sessiontree.ErrAuthorityCorrupt
	}
	eligible, err := sqliteRetrySourceHasRetryEligibleDurableInput(
		ctx, q, threadID, latest.TurnID, latest.RunID, latest.StartedEntryID, *latest.RetrySource,
	)
	if err != nil {
		return sessiontree.CanonicalTurnsPage{}, err
	}
	page.HasRetryTarget = eligible
	return page, nil
}

func sqliteRetrySourceHasRetryEligibleDurableInput(ctx context.Context, q sqlRunner, threadID, retryTurnID, retryRunID, retryStartedEntryID string, source sessiontree.CanonicalTurnRetrySource) (bool, error) {
	visited := make(map[string]struct{})
	for {
		turnID := strings.TrimSpace(source.TurnID)
		entryID := strings.TrimSpace(source.EntryID)
		if turnID == "" || entryID == "" {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		key := turnID + "\x00" + entryID
		if _, duplicate := visited[key]; duplicate {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		visited[key] = struct{}{}
		retryStarted, err := loadEntry(ctx, q, threadID, retryStartedEntryID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, sessiontree.ErrAuthorityCorrupt
			}
			return false, err
		}
		if err := sessiontree.ValidateRetryStartedEntry(retryStarted, threadID, retryTurnID, retryRunID, retryStartedEntryID, source); err != nil {
			return false, err
		}
		admission, found, err := loadSQLiteTurnAdmission(ctx, q, threadID, retryTurnID)
		if err != nil {
			return false, err
		}
		if !found || admission.ThreadID != threadID || admission.TurnID != retryTurnID || admission.RunID != retryRunID ||
			admission.TurnStartedID != retryStartedEntryID || admission.UserMessageID != "" || admission.BaseLeafID != entryID {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		entries, err := loadSQLiteRetrySourceTurn(ctx, q, threadID, turnID)
		if err != nil {
			return false, err
		}
		entries = sessiontree.CanonicalTurnEntriesForRead(entries)
		eligible, next, startedEntryID, runID, err := sessiontree.ValidateCanonicalRetrySourceTurn(entries, threadID, source)
		if err != nil {
			return false, err
		}
		ancestor, err := sqliteRetrySourceIsAncestor(ctx, q, threadID, retryStarted.ParentID, entryID, retrySourceDepth(entries, entryID))
		if err != nil {
			return false, err
		}
		if !ancestor {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		if next == nil {
			return eligible, nil
		}
		retryTurnID, retryRunID, retryStartedEntryID, source = turnID, runID, startedEntryID, *next
	}
}

func retrySourceDepth(entries []sessiontree.Entry, sourceEntryID string) int64 {
	for _, entry := range entries {
		if entry.ID == sourceEntryID {
			return entry.PathDepth
		}
	}
	return 0
}

func sqliteRetrySourceIsAncestor(ctx context.Context, q sqlRunner, threadID, descendantID, sourceID string, sourceDepth int64) (bool, error) {
	if strings.TrimSpace(descendantID) == "" || strings.TrimSpace(sourceID) == "" || sourceDepth <= 0 {
		return false, sessiontree.ErrAuthorityCorrupt
	}
	var ancestor int
	if err := q.QueryRowContext(ctx, sqliteRetrySourceAncestorQuery(), threadID, descendantID, threadID, sourceDepth, sourceID, sourceDepth).Scan(&ancestor); err != nil {
		return false, err
	}
	return ancestor == 1, nil
}

func sqliteRetrySourceAncestorQuery() string {
	return `WITH RECURSIVE ancestors(id, parent_id, path_depth) AS (
		SELECT id, parent_id, path_depth FROM entries WHERE thread_id = ? AND id = ?
		UNION ALL
		SELECT parent.id, parent.parent_id, parent.path_depth
		FROM entries parent
		JOIN ancestors ON parent.thread_id = ? AND ancestors.parent_id = parent.id AND parent.path_depth = ancestors.path_depth - 1
		WHERE ancestors.path_depth > ?
	)
	SELECT EXISTS(SELECT 1 FROM ancestors WHERE id = ? AND path_depth = ?)`
}

func loadSQLiteRetrySourceTurn(ctx context.Context, q sqlRunner, threadID, turnID string) ([]sessiontree.Entry, error) {
	rows, err := q.QueryContext(ctx, sqliteRetrySourceTurnQuery(), threadID, turnID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]sessiontree.Entry, 0, 8)
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, sessiontree.ErrAuthorityCorrupt
	}
	return entries, nil
}

func sqliteRetrySourceTurnQuery() string {
	return `SELECT ` + entryColumns + ` FROM entries INDEXED BY entries_turn_ordinal_idx
		WHERE thread_id = ? AND turn_id = ? ORDER BY ordinal`
}

func cloneSQLiteCanonicalTurnRetrySource(source *sessiontree.CanonicalTurnRetrySource) *sessiontree.CanonicalTurnRetrySource {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func canonicalSQLiteTurnPathEntriesForRead(entries []sessiontree.CanonicalTurnPathEntry) []sessiontree.CanonicalTurnPathEntry {
	hasExecutionTerminal := false
	for _, item := range entries {
		entry := item.Entry
		if entry.Type == sessiontree.EntryTurnMarker && isSQLiteTerminalTurnMarker(entry.TurnStatus) && entry.Metadata["authority_kind"] != "branch_boundary" {
			hasExecutionTerminal = true
			break
		}
	}
	if !hasExecutionTerminal {
		return entries
	}
	filtered := make([]sessiontree.CanonicalTurnPathEntry, 0, len(entries))
	for _, item := range entries {
		entry := item.Entry
		if entry.Type == sessiontree.EntryTurnMarker && isSQLiteTerminalTurnMarker(entry.TurnStatus) && entry.Metadata["authority_kind"] == "branch_boundary" {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func isSQLiteTerminalTurnMarker(status sessiontree.TurnMarkerStatus) bool {
	switch status {
	case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
		return true
	default:
		return false
	}
}
