package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessiontree"
)

func (s *Store) ReadCanonicalTurn(ctx context.Context, threadID, turnID string) (sessiontree.CanonicalTurnRead, error) {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	if threadID == "" || turnID == "" {
		return sessiontree.CanonicalTurnRead{}, sessiontree.ErrCanonicalTurnNotFound
	}
	var result sessiontree.CanonicalTurnRead
	err := s.withRead(ctx, func(q sqlRunner) error {
		meta, err := loadThread(ctx, q, threadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadThreadTombstone(ctx, q, threadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
				return tombstoneErr
			}
		}
		if err != nil {
			return err
		}
		turn, err := readSQLiteCanonicalTurnWithRunner(ctx, q, threadID, turnID, meta.LeafID)
		if err != nil {
			return err
		}
		leaf, err := loadEntry(ctx, q, threadID, meta.LeafID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return err
		}
		if err := sessiontree.ValidateEntryIntegrity(leaf); err != nil || leaf.PathDepth <= 0 {
			return sessiontree.ErrAuthorityCorrupt
		}
		latestTurnID, err := loadSQLiteLatestCanonicalTurnID(ctx, q, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		latestTurn, err := readSQLiteCanonicalTurnWithRunner(ctx, q, threadID, latestTurnID, meta.LeafID)
		if err != nil {
			return err
		}
		hasRetryTarget, err := sqliteCanonicalTurnRetryEligibility(ctx, q, threadID, latestTurn)
		if err != nil {
			return err
		}
		result = sessiontree.CanonicalTurnRead{
			Turn: turn, LatestTurn: latestTurn, ThroughOrdinal: leaf.PathDepth, HasRetryTarget: hasRetryTarget,
		}
		return nil
	})
	return result, err
}

func readSQLiteCanonicalTurnWithRunner(ctx context.Context, q sqlRunner, threadID, turnID, leafID string) (sessiontree.CanonicalTurn, error) {
	admission, hasAdmission, err := loadSQLiteTurnAdmission(ctx, q, threadID, turnID)
	if err != nil {
		return sessiontree.CanonicalTurn{}, err
	}
	entries, err := loadSQLiteCanonicalTurnReadEntries(ctx, q, threadID, turnID)
	if err != nil {
		return sessiontree.CanonicalTurn{}, err
	}
	if len(entries) == 0 {
		if hasAdmission {
			return sessiontree.CanonicalTurn{}, sessiontree.ErrAuthorityCorrupt
		}
		return sessiontree.CanonicalTurn{}, sessiontree.ErrCanonicalTurnNotFound
	}
	var started sessiontree.Entry
	startedCount := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			started = entry
			startedCount++
		}
	}
	if startedCount != 1 {
		return sessiontree.CanonicalTurn{}, sessiontree.ErrAuthorityCorrupt
	}
	if strings.TrimSpace(leafID) == "" {
		return sessiontree.CanonicalTurn{}, sessiontree.ErrAuthorityCorrupt
	}
	active, err := sqliteCanonicalEntryIsActiveAncestor(ctx, q, threadID, leafID, started.ID, started.PathDepth)
	if err != nil {
		return sessiontree.CanonicalTurn{}, err
	}
	if !active {
		return sessiontree.CanonicalTurn{}, sessiontree.ErrCanonicalTurnNotFound
	}
	pathEntries := make([]sessiontree.CanonicalTurnPathEntry, 0, len(entries))
	for _, entry := range entries {
		onPath, err := sqliteCanonicalEntryIsActiveAncestor(ctx, q, threadID, leafID, entry.ID, entry.PathDepth)
		if err != nil {
			return sessiontree.CanonicalTurn{}, err
		}
		if onPath {
			pathEntries = append(pathEntries, sessiontree.CanonicalTurnPathEntry{Entry: entry, Ordinal: entry.PathDepth})
		}
	}
	pathEntries = canonicalSQLiteTurnPathEntriesForRead(pathEntries)
	retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(started)
	if err != nil {
		return sessiontree.CanonicalTurn{}, err
	}
	if !hasAdmission {
		userCount := sqliteCanonicalTurnUserEntryCount(pathEntries)
		if retrySource == nil && userCount == 0 {
			return sessiontree.CanonicalTurn{}, sessiontree.ErrCanonicalTurnNotFound
		}
		if retrySource != nil || userCount != 1 {
			return sessiontree.CanonicalTurn{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	turn := sessiontree.CanonicalTurn{
		TurnID: turnID, RunID: strings.TrimSpace(started.Metadata["run_id"]),
		StartedEntryID: started.ID, StartedOrdinal: started.PathDepth,
		RetrySource: cloneSQLiteCanonicalTurnRetrySource(retrySource), Entries: pathEntries,
	}
	if err := sessiontree.ValidateCanonicalTurnReadStructure(turn, threadID); err != nil {
		return sessiontree.CanonicalTurn{}, err
	}
	if hasAdmission {
		if err := sessiontree.ValidateCanonicalTurnReadAuthority(turn, sessiontree.CanonicalTurnAdmissionFact{
			ThreadID: admission.ThreadID, TurnID: admission.TurnID, RunID: admission.RunID,
			TurnStartedID: admission.TurnStartedID, UserMessageID: admission.UserMessageID, BaseLeafID: admission.BaseLeafID,
		}); err != nil {
			return sessiontree.CanonicalTurn{}, err
		}
	}
	if retrySource != nil {
		eligible, err := sqliteRetrySourceHasRetryEligibleDurableInput(
			ctx, q, threadID, turn.TurnID, turn.RunID, turn.StartedEntryID, *retrySource,
		)
		if err != nil {
			return sessiontree.CanonicalTurn{}, err
		}
		if !eligible {
			return sessiontree.CanonicalTurn{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	return turn, nil
}

func sqliteCanonicalEntryIsActiveAncestor(ctx context.Context, q sqlRunner, threadID, leafID, sourceID string, sourceDepth int64) (bool, error) {
	if strings.TrimSpace(leafID) == "" || strings.TrimSpace(sourceID) == "" || sourceDepth <= 0 {
		return false, sessiontree.ErrAuthorityCorrupt
	}
	nextEntryID := leafID
	expectedStartDepth := int64(0)
	for nextEntryID != "" {
		limit := minimumCanonicalAncestorChunk
		if expectedStartDepth != 0 {
			remaining := expectedStartDepth - sourceDepth + 1
			if remaining <= 0 {
				return false, nil
			}
			if int64(limit) > remaining {
				limit = int(remaining)
			}
		}
		entries, next, err := loadSQLiteAncestorChunk(ctx, q, threadID, nextEntryID, limit)
		if errors.Is(err, sessiontree.ErrEntryNotFound) || errors.Is(err, sessiontree.ErrInvalidParent) {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return false, err
		}
		if expectedStartDepth != 0 && entries[0].PathDepth != expectedStartDepth {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		for _, entry := range entries {
			if entry.PathDepth == sourceDepth {
				return entry.ID == sourceID, nil
			}
			if entry.PathDepth < sourceDepth {
				return false, nil
			}
		}
		nextEntryID = next
		if nextEntryID != "" {
			expectedStartDepth = entries[len(entries)-1].PathDepth - 1
			if expectedStartDepth <= 0 {
				return false, sessiontree.ErrAuthorityCorrupt
			}
		}
	}
	return false, nil
}

func loadSQLiteLatestCanonicalTurnID(ctx context.Context, q sqlRunner, threadID, leafID string) (string, error) {
	if strings.TrimSpace(leafID) == "" {
		return "", sessiontree.ErrAuthorityCorrupt
	}
	userTurns := make(map[string]struct{})
	seenTurns := make(map[string]struct{})
	nextEntryID := leafID
	expectedStartDepth := int64(0)
	for nextEntryID != "" {
		entries, next, err := loadSQLiteAncestorChunk(ctx, q, threadID, nextEntryID, minimumCanonicalAncestorChunk)
		if err != nil {
			return "", err
		}
		if expectedStartDepth != 0 && entries[0].PathDepth != expectedStartDepth {
			return "", sessiontree.ErrInvalidParent
		}
		for _, entry := range entries {
			turnID := strings.TrimSpace(entry.TurnID)
			if entry.Type == sessiontree.EntryUserMessage && turnID != "" {
				userTurns[turnID] = struct{}{}
			}
			if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
				continue
			}
			if turnID == "" || strings.TrimSpace(entry.Metadata["run_id"]) == "" {
				return "", sessiontree.ErrAuthorityCorrupt
			}
			if _, duplicate := seenTurns[turnID]; duplicate {
				return "", sessiontree.ErrAuthorityCorrupt
			}
			seenTurns[turnID] = struct{}{}
			retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return "", err
			}
			if _, admitted := userTurns[turnID]; admitted || retrySource != nil {
				return turnID, nil
			}
		}
		nextEntryID = next
		if nextEntryID != "" {
			expectedStartDepth = entries[len(entries)-1].PathDepth - 1
		}
	}
	return "", sessiontree.ErrAuthorityCorrupt
}

func sqliteCanonicalTurnRetryEligibility(ctx context.Context, q sqlRunner, threadID string, turn sessiontree.CanonicalTurn) (bool, error) {
	if turn.RetrySource != nil {
		return sqliteRetrySourceHasRetryEligibleDurableInput(
			ctx, q, threadID, turn.TurnID, turn.RunID, turn.StartedEntryID, *turn.RetrySource,
		)
	}
	for index := len(turn.Entries) - 1; index >= 0; index-- {
		entry := turn.Entries[index].Entry
		if entry.Type == sessiontree.EntryUserMessage {
			return session.HasRetryEligibleDurableInput(entry.Message), nil
		}
	}
	return false, sessiontree.ErrAuthorityCorrupt
}

func loadSQLiteCanonicalTurnReadEntries(ctx context.Context, q sqlRunner, threadID, turnID string) ([]sessiontree.Entry, error) {
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
	return entries, nil
}

func sqliteCanonicalTurnUserEntryCount(entries []sessiontree.CanonicalTurnPathEntry) int {
	count := 0
	for _, item := range entries {
		if item.Entry.Type == sessiontree.EntryUserMessage {
			count++
		}
	}
	return count
}
