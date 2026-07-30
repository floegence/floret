package agentharness

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v3/internal/sessionlifecycle"
	"github.com/floegence/floret/v3/internal/sessiontree"
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
		thread := h.cacheThread(threadID)
		thread.mu.Lock()
		localPhase := thread.phase
		thread.mu.Unlock()
		phase, err = thread.canonicalThreadPhase(ctx, localPhase)
		if err != nil {
			return CanonicalTurnDetailRead{}, err
		}
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
		entry, err := h.restoreCanonicalSubAgentUserMessageOrigin(ctx, item.Entry, turn.RunID)
		if err != nil {
			return CanonicalTurnDetail{}, nil, err
		}
		entries = append(entries, entry)
	}
	activityContext := subAgentDetailActivityContext{
		resultCallIDs: subAgentDetailResultCallIDs(entries),
		runIDs:        subAgentDetailTurnRunIDs(entries),
	}
	detail := CanonicalTurnDetail{
		TurnID: turn.TurnID, RunID: turn.RunID, StartedOrdinal: turn.StartedOrdinal,
		RetrySource: cloneCanonicalTurnRetrySource(turn.RetrySource),
	}
	for index, item := range turn.Entries {
		event, visible := h.subAgentDetailEvent(entries[index], item.Ordinal, includeRaw, activityContext)
		if visible {
			detail.Events = append(detail.Events, event)
		}
	}
	return detail, entries, nil
}

func (h *AgentHarness) restoreCanonicalSubAgentUserMessageOrigin(ctx context.Context, entry sessiontree.Entry, runID string) (sessiontree.Entry, error) {
	if entry.Type != sessiontree.EntryUserMessage {
		return entry, nil
	}
	if _, present := entry.Metadata[sessiontree.SubAgentUserMessageOriginMetadataKey]; present {
		return entry, nil
	}
	rawInputID, present := entry.Metadata[sessiontree.SubAgentInputIDMetadataKey]
	if !present {
		return entry, nil
	}
	inputID := strings.TrimSpace(rawInputID)
	if inputID == "" || inputID != rawInputID {
		return sessiontree.Entry{}, sessiontree.ErrAuthorityCorrupt
	}
	reader, ok := h.options.Repo.(sessiontree.SubAgentInputReadRepo)
	if !ok {
		return sessiontree.Entry{}, errors.New("session tree repo does not support subagent input reads")
	}
	input, found, err := reader.ReadSubAgentInput(ctx, inputID)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if !found || input.SubAgentInputID != inputID || input.State != sessiontree.SubAgentInputAdmitted {
		return sessiontree.Entry{}, sessiontree.ErrAuthorityCorrupt
	}
	authorityEntry, authorityRunID, err := h.resolveInheritedSubAgentInputAuthority(ctx, entry, runID, input)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if input.ChildThreadID != authorityEntry.ThreadID || input.AdmittedTurnID != authorityEntry.TurnID ||
		input.AdmittedRunID != strings.TrimSpace(authorityRunID) {
		return sessiontree.Entry{}, sessiontree.ErrAuthorityCorrupt
	}
	origin, err := sessiontree.SubAgentUserMessageOrigin(input.RequestKind)
	if err != nil {
		return sessiontree.Entry{}, sessiontree.ErrAuthorityCorrupt
	}
	entry.Metadata = cloneStringMap(entry.Metadata)
	entry.Metadata[sessiontree.SubAgentUserMessageOriginMetadataKey] = origin
	return entry, nil
}

func (h *AgentHarness) resolveInheritedSubAgentInputAuthority(ctx context.Context, entry sessiontree.Entry, runID string, input sessiontree.SubAgentInputRecord) (sessiontree.Entry, string, error) {
	authorityEntry := entry
	authorityRunID := strings.TrimSpace(runID)
	visited := map[string]struct{}{}
	for authorityEntry.ThreadID != input.ChildThreadID {
		threadID := strings.TrimSpace(authorityEntry.ThreadID)
		if threadID == "" {
			return sessiontree.Entry{}, "", sessiontree.ErrAuthorityCorrupt
		}
		if _, duplicate := visited[threadID]; duplicate {
			return sessiontree.Entry{}, "", sessiontree.ErrAuthorityCorrupt
		}
		visited[threadID] = struct{}{}

		meta, err := h.options.Repo.Thread(ctx, threadID)
		if err != nil {
			return sessiontree.Entry{}, "", err
		}
		sourceThreadID := strings.TrimSpace(meta.ForkedFromThreadID)
		sourceLeafID := strings.TrimSpace(meta.ForkedFromEntryID)
		if meta.ID != threadID || strings.TrimSpace(meta.ParentThreadID) != sourceThreadID ||
			strings.TrimSpace(meta.ForkMode) != string(SubAgentForkFullPath) || sourceThreadID == "" || sourceLeafID == "" {
			return sessiontree.Entry{}, "", sessiontree.ErrAuthorityCorrupt
		}
		destinationPrefix, err := h.options.Repo.Path(ctx, threadID, authorityEntry.ID)
		if err != nil {
			return sessiontree.Entry{}, "", err
		}
		sourcePath, err := h.options.Repo.Path(ctx, sourceThreadID, sourceLeafID)
		if err != nil {
			return sessiontree.Entry{}, "", err
		}
		ordinal := len(destinationPrefix)
		if ordinal == 0 || ordinal > len(sourcePath) {
			return sessiontree.Entry{}, "", sessiontree.ErrAuthorityCorrupt
		}
		sourceEntry := sourcePath[ordinal-1]
		if sourceEntry.Type != sessiontree.EntryUserMessage ||
			strings.TrimSpace(sourceEntry.Metadata[sessiontree.SubAgentInputIDMetadataKey]) != input.SubAgentInputID ||
			!messagesEqualForDelta(sourceEntry.Message, authorityEntry.Message) {
			return sessiontree.Entry{}, "", sessiontree.ErrAuthorityCorrupt
		}
		authorityEntry = sourceEntry
		authorityRunID = subAgentDetailActivityContext{
			runIDs: subAgentDetailTurnRunIDs(sourcePath),
		}.runIDForTurn(sourceEntry.TurnID)
	}
	return authorityEntry, authorityRunID, nil
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
