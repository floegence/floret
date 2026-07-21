package agentharness

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/internal/sessiontree"
)

type CanonicalTurnDetail struct {
	TurnID         string
	RunID          string
	StartedOrdinal int64
	RetrySource    *sessiontree.CanonicalTurnRetrySource
	Events         []SubAgentDetailEvent
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
		entries := make([]sessiontree.Entry, 0, len(turn.Entries))
		for _, item := range turn.Entries {
			entries = append(entries, item.Entry)
		}
		activityContext := subAgentDetailActivityContext{
			resultCallIDs: subAgentDetailResultCallIDs(entries),
			runIDs:        subAgentDetailTurnRunIDs(entries),
		}
		detail := CanonicalTurnDetail{
			TurnID: turn.TurnID, RunID: turn.RunID, StartedOrdinal: turn.StartedOrdinal,
			RetrySource: cloneCanonicalTurnRetrySource(turn.RetrySource),
		}
		for _, item := range turn.Entries {
			event, visible := h.subAgentDetailEvent(item.Entry, item.Ordinal, includeRaw, activityContext)
			if visible {
				detail.Events = append(detail.Events, event)
			}
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
		thread := h.cacheThread(strings.TrimSpace(opts.ThreadID))
		thread.mu.Lock()
		localPhase := thread.phase
		thread.mu.Unlock()
		phase, err = thread.canonicalThreadPhase(ctx, localPhase)
		if err != nil {
			return CanonicalTurnDetailsPage{}, err
		}
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
