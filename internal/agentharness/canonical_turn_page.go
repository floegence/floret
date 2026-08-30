package agentharness

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v6/internal/sessionlifecycle"
	"github.com/floegence/floret/v6/internal/sessiontree"
)

type CanonicalTurnDetail struct {
	TurnID         string
	RunID          string
	StartedOrdinal int64
	RetrySource    *sessiontree.CanonicalTurnRetrySource
	Events         []ThreadDetailEvent
}

type CanonicalTurnDetailsPage struct {
	Turns             []CanonicalTurnDetail
	BeforeCursor      *sessiontree.CanonicalTurnBeforeCursor
	SinceCursor       sessiontree.CanonicalTurnSinceCursor
	HasMore           bool
	ThroughOrdinal    int64
	LatestTurnID      string
	LatestStatus      string
	LatestRecoverable bool
	LatestCanRetry    bool
	GeneratedAt       time.Time
}

type CanonicalTurnDetailRead struct {
	Turn              CanonicalTurnDetail
	ThroughOrdinal    int64
	LatestTurnID      string
	LatestStatus      string
	LatestRecoverable bool
	LatestCanRetry    bool
}

func (h *AgentHarness) ListCanonicalTurnDetailEvents(ctx context.Context, opts sessiontree.ListCanonicalTurnsOptions, includeRaw bool) (CanonicalTurnDetailsPage, error) {
	if h == nil || h.options.Repo == nil {
		return CanonicalTurnDetailsPage{}, errors.New("agent harness is not initialized")
	}
	repo, ok := h.options.Repo.(sessiontree.CanonicalTurnPageRepo)
	if !ok {
		return CanonicalTurnDetailsPage{}, errors.New("session tree repo does not support canonical turn pages")
	}
	canonical, err := repo.ListCanonicalTurns(ctx, opts)
	if err != nil {
		return CanonicalTurnDetailsPage{}, err
	}
	page := CanonicalTurnDetailsPage{
		Turns:          make([]CanonicalTurnDetail, 0, len(canonical.Turns)),
		BeforeCursor:   canonical.BeforeCursor,
		SinceCursor:    canonical.SinceCursor,
		HasMore:        canonical.HasMore,
		ThroughOrdinal: canonical.ThroughOrdinal,
		LatestTurnID:   canonical.LatestTurnID,
		GeneratedAt:    h.now(),
	}
	var latestEntries []sessiontree.Entry
	for _, turn := range canonical.Turns {
		detail, entries, err := h.canonicalTurnDetail(ctx, turn, includeRaw)
		if err != nil {
			return CanonicalTurnDetailsPage{}, err
		}
		if turn.TurnID == canonical.LatestTurnID {
			latestEntries = entries
		}
		page.Turns = append(page.Turns, detail)
	}
	if len(latestEntries) == 0 {
		return page, nil
	}
	phase := sessionlifecycle.PhaseIdle
	if !canonicalTurnEntriesHaveTerminal(latestEntries) {
		phase = sessionlifecycle.PhaseTurn
	}
	lifecycle := sessionlifecycle.Derive(latestEntries, phase)
	if lifecycle.LatestTurnID() != canonical.LatestTurnID {
		return CanonicalTurnDetailsPage{}, sessiontree.ErrAuthorityCorrupt
	}
	page.LatestStatus = lifecycle.Status()
	page.LatestRecoverable = lifecycle.Recoverable()
	page.LatestCanRetry = canonical.HasRetryTarget
	return page, nil
}

func (h *AgentHarness) ReadCanonicalTurnDetailEvents(ctx context.Context, threadID, turnID string, includeRaw bool) (CanonicalTurnDetailRead, error) {
	if h == nil || h.options.Repo == nil {
		return CanonicalTurnDetailRead{}, errors.New("agent harness is not initialized")
	}
	repo, ok := h.options.Repo.(sessiontree.CanonicalTurnReadRepo)
	if !ok {
		return CanonicalTurnDetailRead{}, errors.New("session tree repo does not support exact canonical turn reads")
	}
	threadID = strings.TrimSpace(threadID)
	read, err := repo.ReadCanonicalTurn(ctx, threadID, strings.TrimSpace(turnID))
	if err != nil {
		return CanonicalTurnDetailRead{}, err
	}
	detail, _, err := h.canonicalTurnDetail(ctx, read.Turn, includeRaw)
	if err != nil {
		return CanonicalTurnDetailRead{}, err
	}
	latestEntries := make([]sessiontree.Entry, 0, len(read.LatestTurn.Entries))
	for _, item := range read.LatestTurn.Entries {
		latestEntries = append(latestEntries, item.Entry)
	}
	if len(latestEntries) == 0 || read.LatestTurn.TurnID == "" {
		return CanonicalTurnDetailRead{}, sessiontree.ErrAuthorityCorrupt
	}
	phase := sessionlifecycle.PhaseIdle
	if !canonicalTurnEntriesHaveTerminal(latestEntries) {
		phase = sessionlifecycle.PhaseTurn
	}
	lifecycle := sessionlifecycle.Derive(latestEntries, phase)
	if lifecycle.LatestTurnID() != read.LatestTurn.TurnID {
		return CanonicalTurnDetailRead{}, sessiontree.ErrAuthorityCorrupt
	}
	return CanonicalTurnDetailRead{
		Turn: detail, ThroughOrdinal: read.ThroughOrdinal,
		LatestTurnID: read.LatestTurn.TurnID, LatestStatus: lifecycle.Status(),
		LatestRecoverable: lifecycle.Recoverable(), LatestCanRetry: read.HasRetryTarget,
	}, nil
}

func (h *AgentHarness) canonicalTurnDetail(ctx context.Context, turn sessiontree.CanonicalTurn, includeRaw bool) (CanonicalTurnDetail, []sessiontree.Entry, error) {
	entries := make([]sessiontree.Entry, 0, len(turn.Entries))
	for _, item := range turn.Entries {
		entries = append(entries, item.Entry)
	}
	activityContext := threadDetailActivityContext{
		resultCallIDs: threadDetailResultCallIDs(entries),
		runIDs:        threadDetailTurnRunIDs(entries),
	}
	detail := CanonicalTurnDetail{
		TurnID: turn.TurnID, RunID: turn.RunID, StartedOrdinal: turn.StartedOrdinal,
		RetrySource: cloneCanonicalTurnRetrySource(turn.RetrySource),
	}
	for index, item := range turn.Entries {
		event, visible := h.threadDetailEvent(entries[index], item.Ordinal, includeRaw, activityContext)
		if visible {
			detail.Events = append(detail.Events, event)
		}
	}
	return detail, entries, nil
}

func cloneCanonicalTurnRetrySource(source *sessiontree.CanonicalTurnRetrySource) *sessiontree.CanonicalTurnRetrySource {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func canonicalTurnEntriesHaveTerminal(entries []sessiontree.Entry) bool {
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryTurnMarker {
			continue
		}
		switch entry.TurnStatus {
		case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
			return true
		}
	}
	return false
}
