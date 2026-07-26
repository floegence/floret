package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/sessiontree"
)

// RecoverInterruptedTurn atomically takes over and finalizes the exact proof bound at host construction.
func (h *InterruptedTurnRecoveryHost) RecoverInterruptedTurn(ctx context.Context) (RecoverInterruptedTurnResult, error) {
	if h == nil || h.store == nil || h.harness == nil || h.threadID == "" {
		return RecoverInterruptedTurnResult{}, errors.New("interrupted turn recovery host is required")
	}
	operationCtx, done, err := beginHostOperationContext(h.store, ctx)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	defer done()
	result, err := h.harness.RecoverInterruptedTurn(operationCtx, agentharness.RecoverInterruptedTurnOptions{
		ThreadID: string(h.threadID), ParentThreadID: string(h.parentThreadID), ExpectedLease: h.expectedLease,
	})
	if err != nil {
		mapped := runtimeHostError(err)
		if errors.Is(err, sessiontree.ErrRequestConflict) {
			mapped = fmt.Errorf("%w: %w", ErrAuthorityCorrupt, err)
		}
		if errors.Is(mapped, ErrRecoveryTargetResolved) {
			h.markInterruptedRecoveryFactoryResolved()
		}
		return RecoverInterruptedTurnResult{}, mapped
	}
	h.markInterruptedRecoveryFactoryResolved()
	detail, found, readErr := h.harness.ReadTurnDetailEvents(operationCtx, result.ThreadID, result.TurnID, result.RunID, true)
	if readErr != nil {
		return RecoverInterruptedTurnResult{}, runtimeHostError(readErr)
	}
	if !found {
		return RecoverInterruptedTurnResult{}, fmt.Errorf("%w: interrupted recovery terminal turn is missing", ErrAuthorityCorrupt)
	}
	failure := canonicalTurnFailure(threadDetailEvents(detail.Events))
	status := interruptedRecoveryTurnStatus(result.Status, failure)
	if err := validateThreadTurnFailureForStatus(status, failure); err != nil {
		return RecoverInterruptedTurnResult{}, fmt.Errorf("%w: %v", ErrAuthorityCorrupt, err)
	}
	return RecoverInterruptedTurnResult{
		ThreadID: ThreadID(result.ThreadID), TurnID: TurnID(result.TurnID), RunID: RunID(result.RunID), Status: status, Failure: failure, Replayed: result.Replayed,
	}, nil
}

func interruptedRecoveryTurnStatus(marker sessiontree.TurnMarkerStatus, failure *ThreadTurnFailure) TurnStatus {
	switch marker {
	case sessiontree.TurnAborted:
		if failure != nil && failure.Code == ThreadTurnFailureInterrupted {
			return TurnStatusInterrupted
		}
		return TurnStatusCancelled
	case sessiontree.TurnFailed:
		return TurnStatusFailed
	default:
		return ""
	}
}

func (h *InterruptedTurnRecoveryHost) markInterruptedRecoveryFactoryResolved() {
	if h == nil || h.factoryState == nil {
		return
	}
	h.factoryState.mu.Lock()
	h.factoryState.resolved = true
	h.factoryState.mu.Unlock()
}

const (
	defaultThreadTurnsLimit = 50
	maxThreadTurnsLimit     = 200
)

var (
	ErrInvalidThreadTurnCursor = errors.New("floret thread turn cursor is invalid")
	ErrStaleThreadTurnCursor   = errors.New("floret thread turn cursor is stale")
)

// ThreadTurnCursor is an opaque position in one thread's canonical turn path.
// Hosts may persist and compare the token, but must not parse or modify it.
type ThreadTurnCursor string

type ListThreadTurnsRequest struct {
	ThreadID     ThreadID          `json:"thread_id"`
	BeforeCursor *ThreadTurnCursor `json:"before_cursor,omitempty"`
	SinceCursor  *ThreadTurnCursor `json:"since_cursor,omitempty"`
	Tail         int               `json:"tail,omitempty"`
	Limit        int               `json:"limit,omitempty"`
}

type ThreadTurnsPage struct {
	ThreadID       ThreadID             `json:"thread_id"`
	Turns          []ThreadTurnSnapshot `json:"turns"`
	BeforeCursor   *ThreadTurnCursor    `json:"before_cursor,omitempty"`
	SinceCursor    ThreadTurnCursor     `json:"since_cursor"`
	HasMore        bool                 `json:"has_more,omitempty"`
	ThroughOrdinal int64                `json:"through_ordinal"`
	GeneratedAt    time.Time            `json:"generated_at"`
}

type ThreadOverview struct {
	Thread     ThreadSnapshot      `json:"thread"`
	LatestTurn *ThreadTurnSnapshot `json:"latest_turn,omitempty"`
}

type ThreadTurnSnapshot struct {
	TurnID    TurnID    `json:"turn_id"`
	RunID     RunID     `json:"run_id"`
	Ordinal   int64     `json:"ordinal"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// UserEntryID is the opaque identity of the admitted canonical user Entry.
	// It is a presentation anchor, not authorization or a storage access handle.
	UserEntryID     string                 `json:"user_entry_id,omitempty"`
	UserInput       string                 `json:"user_input,omitempty"`
	UserAttachments []MessageAttachment    `json:"user_attachments,omitempty"`
	UserReferences  []MessageReference     `json:"user_references,omitempty"`
	RetrySource     *ThreadTurnRetrySource `json:"retry_source,omitempty"`
	Status          TurnStatus             `json:"status"`
	Failure         *ThreadTurnFailure     `json:"failure,omitempty"`
	Recoverable     bool                   `json:"recoverable"`
	CanRetry        bool                   `json:"can_retry"`
	Projection      ThreadTurnProjection   `json:"projection"`
	ControlSignals  []ThreadControlSignal  `json:"control_signals,omitempty"`
	ThroughOrdinal  int64                  `json:"through_ordinal"`
}

type ThreadTurnRetrySource struct {
	// TurnID is the canonical source turn. Its internal journal anchor remains
	// private to Floret.
	TurnID TurnID `json:"turn_id"`
}

const threadTurnCursorVersion = 1

const (
	threadTurnCursorModeBefore = "before"
	threadTurnCursorModeSince  = "since"
)

type threadTurnCursorPayload struct {
	Version  int    `json:"version"`
	ThreadID string `json:"thread_id"`
	Mode     string `json:"mode"`
	EntryID  string `json:"entry_id"`
}

type ThreadControlSignal struct {
	Name        string         `json:"name"`
	CallID      string         `json:"call_id"`
	Disposition string         `json:"disposition,omitempty"`
	Text        string         `json:"text,omitempty"`
	ArgsHash    string         `json:"args_hash,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

func (h *providerHost) ListThreadTurns(ctx context.Context, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	return listThreadTurns(ctx, h.harness, req)
}

func (h *ThreadReadHost) ListThreadTurns(ctx context.Context, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadTurnsPage{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, req.ThreadID, "thread read host"); err != nil {
		return ThreadTurnsPage{}, err
	}
	return listThreadTurns(ctx, h.harness, req)
}

// ListThreadTurns returns canonical typed turns for one complete descendant of
// the parent bound to this read host.
func (h *SubAgentReadHost) ListThreadTurns(ctx context.Context, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	if h == nil {
		return ThreadTurnsPage{}, errors.New("subagent read host is required")
	}
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadTurnsPage{}, err
	}
	defer done()
	if h.harness == nil || strings.TrimSpace(string(h.parentThreadID)) == "" {
		return ThreadTurnsPage{}, errors.New("subagent read host is invalid")
	}
	if err := h.harness.ValidateSubAgentDescendantAuthority(ctx, string(h.parentThreadID), string(req.ThreadID)); err != nil {
		return ThreadTurnsPage{}, runtimeHostError(err)
	}
	return listThreadTurns(ctx, h.harness, req)
}

func (h *providerHost) ReadLatestThreadTurn(ctx context.Context, threadID ThreadID) (ThreadTurnSnapshot, error) {
	return readLatestThreadTurn(ctx, h.harness, threadID)
}

func (h *ThreadReadHost) ReadLatestThreadTurn(ctx context.Context, threadID ThreadID) (ThreadTurnSnapshot, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, threadID, "thread read host"); err != nil {
		return ThreadTurnSnapshot{}, err
	}
	return readLatestThreadTurn(ctx, h.harness, threadID)
}

func (h *providerHost) ReadThreadOverview(ctx context.Context, threadID ThreadID) (ThreadOverview, error) {
	return readThreadOverview(ctx, h.harness, threadID)
}

func (h *ThreadReadHost) ReadThreadOverview(ctx context.Context, threadID ThreadID) (ThreadOverview, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadOverview{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, threadID, "thread read host"); err != nil {
		return ThreadOverview{}, err
	}
	return readThreadOverview(ctx, h.harness, threadID)
}

func readThreadOverview(ctx context.Context, harness *agentharness.AgentHarness, threadID ThreadID) (ThreadOverview, error) {
	if strings.TrimSpace(string(threadID)) == "" {
		return ThreadOverview{}, errors.New("thread id is required")
	}
	overview, err := harness.ReadThreadOverview(ctx, string(threadID))
	if err != nil {
		return ThreadOverview{}, runtimeHostError(err)
	}
	thread := threadSnapshot(overview.Thread)
	if err := thread.Validate(); err != nil {
		return ThreadOverview{}, fmt.Errorf("%w: invalid public thread snapshot: %v", ErrAuthorityCorrupt, err)
	}
	events := threadDetailEvents(overview.LatestTurn.Events)
	turns, _, err := projectThreadTurnSnapshots(threadID, events)
	if err != nil {
		return ThreadOverview{}, err
	}
	if len(turns) > 1 {
		return ThreadOverview{}, fmt.Errorf("thread overview latest turn query returned %d turns", len(turns))
	}
	result := ThreadOverview{Thread: thread}
	if len(turns) == 1 {
		latest := turns[0]
		applyLatestThreadLifecycle(&latest, thread)
		if latest.ThroughOrdinal > thread.ThroughOrdinal {
			return ThreadOverview{}, fmt.Errorf("%w: latest turn revision exceeds thread revision", ErrJournalInvariant)
		}
		result.LatestTurn = &latest
	}
	return result, nil
}

func readLatestThreadTurn(ctx context.Context, harness *agentharness.AgentHarness, threadID ThreadID) (ThreadTurnSnapshot, error) {
	if strings.TrimSpace(string(threadID)) == "" {
		return ThreadTurnSnapshot{}, errors.New("thread id is required")
	}
	detail, err := harness.ReadLatestThreadDetailEvents(ctx, string(threadID), true)
	if err != nil {
		return ThreadTurnSnapshot{}, runtimeHostError(err)
	}
	turns, _, err := projectThreadTurnSnapshots(threadID, threadDetailEvents(detail.Events))
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
	if len(turns) == 0 {
		return ThreadTurnSnapshot{}, ErrTurnNotFound
	}
	if len(turns) != 1 {
		return ThreadTurnSnapshot{}, fmt.Errorf("latest thread turn query returned %d turns", len(turns))
	}
	thread, err := harness.ReadThread(ctx, string(threadID))
	if err != nil {
		return ThreadTurnSnapshot{}, runtimeHostError(err)
	}
	publicThread := threadSnapshot(thread)
	if err := publicThread.Validate(); err != nil {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: invalid public thread snapshot: %v", ErrAuthorityCorrupt, err)
	}
	latest := turns[0]
	applyLatestThreadLifecycle(&latest, publicThread)
	return latest, nil
}

func listThreadTurns(ctx context.Context, harness *agentharness.AgentHarness, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return ThreadTurnsPage{}, errors.New("thread id is required")
	}
	if req.Tail < 0 || req.Limit < 0 {
		return ThreadTurnsPage{}, errors.New("thread turn pagination values must be non-negative")
	}
	modes := 0
	if req.BeforeCursor != nil {
		modes++
	}
	if req.SinceCursor != nil {
		modes++
	}
	if req.Tail > 0 {
		modes++
	}
	if modes > 1 {
		return ThreadTurnsPage{}, errors.New("before, since, and tail pagination modes are mutually exclusive")
	}
	if req.Tail > 0 && req.Limit > 0 {
		return ThreadTurnsPage{}, errors.New("tail pagination uses tail as its page size")
	}
	limit := req.Limit
	if req.Tail > 0 {
		limit = req.Tail
	}
	if limit == 0 {
		limit = defaultThreadTurnsLimit
	}
	if limit > maxThreadTurnsLimit {
		return ThreadTurnsPage{}, fmt.Errorf("thread turn page size must not exceed %d", maxThreadTurnsLimit)
	}

	canonicalLimit := limit
	tail := req.Tail
	if modes == 0 {
		tail = limit
	}
	if tail > 0 {
		canonicalLimit = 0
	}
	var beforeCursor *sessiontree.CanonicalTurnBeforeCursor
	if req.BeforeCursor != nil {
		payload, err := decodeThreadTurnCursor(*req.BeforeCursor, req.ThreadID, threadTurnCursorModeBefore)
		if err != nil {
			return ThreadTurnsPage{}, err
		}
		beforeCursor = &sessiontree.CanonicalTurnBeforeCursor{EntryID: payload.EntryID}
	}
	var sinceCursor *sessiontree.CanonicalTurnSinceCursor
	if req.SinceCursor != nil {
		payload, err := decodeThreadTurnCursor(*req.SinceCursor, req.ThreadID, threadTurnCursorModeSince)
		if err != nil {
			return ThreadTurnsPage{}, err
		}
		sinceCursor = &sessiontree.CanonicalTurnSinceCursor{EntryID: payload.EntryID}
	}
	detailPage, err := harness.ListCanonicalTurnDetailEvents(ctx, sessiontree.ListCanonicalTurnsOptions{
		ThreadID: string(req.ThreadID), BeforeCursor: beforeCursor, SinceCursor: sinceCursor, Tail: tail, Limit: canonicalLimit,
	}, true)
	if err != nil {
		if errors.Is(err, sessiontree.ErrStaleCanonicalTurnCursor) {
			return ThreadTurnsPage{}, fmt.Errorf("%w: %w", ErrStaleThreadTurnCursor, err)
		}
		return ThreadTurnsPage{}, runtimeHostError(err)
	}
	turns := make([]ThreadTurnSnapshot, 0, len(detailPage.Turns))
	for _, detail := range detailPage.Turns {
		turn, err := projectCanonicalThreadTurnSnapshot(req.ThreadID, detail)
		if err != nil {
			return ThreadTurnsPage{}, err
		}
		if turn.TurnID == TurnID(detailPage.LatestTurnID) {
			applyLatestThreadLifecycle(&turn, ThreadSnapshot{
				LatestTurnID: TurnID(detailPage.LatestTurnID), Status: ThreadStatus(detailPage.LatestStatus),
				Recoverable: detailPage.LatestRecoverable, CanRetry: detailPage.LatestCanRetry,
			})
		}
		if err := validateThreadTurnFailureForStatus(turn.Status, turn.Failure); err != nil {
			return ThreadTurnsPage{}, fmt.Errorf("%w: canonical turn %q failure state is invalid: %v", ErrAuthorityCorrupt, turn.TurnID, err)
		}
		turns = append(turns, turn)
	}
	page := ThreadTurnsPage{
		ThreadID:       req.ThreadID,
		Turns:          turns,
		HasMore:        detailPage.HasMore,
		ThroughOrdinal: detailPage.ThroughOrdinal,
		GeneratedAt:    detailPage.GeneratedAt,
	}
	if strings.TrimSpace(detailPage.SinceCursor.EntryID) != "" {
		encoded, err := encodeThreadTurnCursor(req.ThreadID, threadTurnCursorModeSince, detailPage.SinceCursor.EntryID)
		if err != nil {
			return ThreadTurnsPage{}, err
		}
		page.SinceCursor = encoded
	}
	if detailPage.BeforeCursor != nil {
		encoded, err := encodeThreadTurnCursor(req.ThreadID, threadTurnCursorModeBefore, detailPage.BeforeCursor.EntryID)
		if err != nil {
			return ThreadTurnsPage{}, err
		}
		page.BeforeCursor = &encoded
	}
	return page, nil
}

func encodeThreadTurnCursor(threadID ThreadID, mode, entryID string) (ThreadTurnCursor, error) {
	payload := threadTurnCursorPayload{
		Version:  threadTurnCursorVersion,
		ThreadID: strings.TrimSpace(string(threadID)),
		Mode:     mode,
		EntryID:  strings.TrimSpace(entryID),
	}
	if payload.ThreadID == "" || payload.ThreadID != string(threadID) ||
		(mode != threadTurnCursorModeBefore && mode != threadTurnCursorModeSince) || payload.EntryID == "" || payload.EntryID != entryID {
		return "", fmt.Errorf("%w: cursor payload is incomplete", ErrAuthorityCorrupt)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: encode thread turn cursor: %v", ErrAuthorityCorrupt, err)
	}
	return ThreadTurnCursor(base64.RawURLEncoding.EncodeToString(raw)), nil
}

func decodeThreadTurnCursor(cursor ThreadTurnCursor, threadID ThreadID, mode string) (threadTurnCursorPayload, error) {
	rawCursor := string(cursor)
	if strings.TrimSpace(rawCursor) == "" || rawCursor != strings.TrimSpace(rawCursor) {
		return threadTurnCursorPayload{}, fmt.Errorf("%w: cursor token is required", ErrInvalidThreadTurnCursor)
	}
	raw, err := base64.RawURLEncoding.DecodeString(rawCursor)
	if err != nil {
		return threadTurnCursorPayload{}, fmt.Errorf("%w: malformed token", ErrInvalidThreadTurnCursor)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var payload threadTurnCursorPayload
	if err := decoder.Decode(&payload); err != nil {
		return threadTurnCursorPayload{}, fmt.Errorf("%w: malformed payload", ErrInvalidThreadTurnCursor)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return threadTurnCursorPayload{}, fmt.Errorf("%w: trailing payload", ErrInvalidThreadTurnCursor)
	}
	expectedThreadID := strings.TrimSpace(string(threadID))
	if expectedThreadID == "" || expectedThreadID != string(threadID) || payload.Version != threadTurnCursorVersion ||
		payload.ThreadID != expectedThreadID || payload.Mode != mode ||
		(mode != threadTurnCursorModeBefore && mode != threadTurnCursorModeSince) ||
		strings.TrimSpace(payload.EntryID) == "" || payload.EntryID != strings.TrimSpace(payload.EntryID) {
		return threadTurnCursorPayload{}, fmt.Errorf("%w: cursor scope does not match request", ErrInvalidThreadTurnCursor)
	}
	return payload, nil
}

func projectCanonicalThreadTurnSnapshot(threadID ThreadID, detail agentharness.CanonicalTurnDetail) (ThreadTurnSnapshot, error) {
	events := threadDetailEvents(detail.Events)
	turnID := TurnID(strings.TrimSpace(detail.TurnID))
	runID := RunID(strings.TrimSpace(detail.RunID))
	startedRunID, ordinal, startedAt := threadTurnStartedIdentity(events)
	if turnID == "" || runID == "" || startedRunID != runID || ordinal != detail.StartedOrdinal || ordinal <= 0 || startedAt.IsZero() {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: canonical turn %q has an invalid started identity", ErrAuthorityCorrupt, detail.TurnID)
	}
	userEntryID, userInput, userAttachments, userReferences := canonicalTurnUserInput(events, turnID)
	retryAuthority, err := readThreadTurnRetryAuthority(events, turnID)
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
	detailRetryAuthority := runtimeCanonicalTurnRetryAuthority(detail.RetrySource)
	if !sameThreadTurnRetryAuthority(retryAuthority, detailRetryAuthority) {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: canonical turn %q retry source is inconsistent", ErrAuthorityCorrupt, turnID)
	}
	if retryAuthority == nil && strings.TrimSpace(userEntryID) == "" {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: canonical turn %q has no user admission", ErrAuthorityCorrupt, turnID)
	}
	if retryAuthority != nil && strings.TrimSpace(userEntryID) != "" {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: retry turn %q duplicated its source user admission", ErrAuthorityCorrupt, turnID)
	}
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: threadID,
		TurnID:   turnID,
		RunID:    runID,
		TraceID:  TraceID(runID),
		Events:   events,
	})
	if err := projection.Validate(); err != nil {
		return ThreadTurnSnapshot{}, fmt.Errorf("project turn %q: %w", turnID, err)
	}
	turn := ThreadTurnSnapshot{
		TurnID:          turnID,
		RunID:           runID,
		Ordinal:         ordinal,
		StartedAt:       startedAt,
		UpdatedAt:       events[len(events)-1].CreatedAt,
		UserEntryID:     userEntryID,
		UserInput:       userInput,
		UserAttachments: userAttachments,
		UserReferences:  userReferences,
		RetrySource:     publicThreadTurnRetrySource(retryAuthority),
		Status:          projection.Status,
		Failure:         canonicalTurnFailure(events),
		Projection:      projection,
		ControlSignals:  threadTurnControlSignals(events),
		ThroughOrdinal:  projection.ThroughOrdinal,
	}
	turn.Status = canonicalTurnStatus(turn.Status, turn.Failure)
	if err := validateThreadTurnFailureForStatus(turn.Status, turn.Failure); err != nil {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: canonical turn %q failure state is invalid: %v", ErrAuthorityCorrupt, turnID, err)
	}
	return turn, nil
}

func applyLatestThreadLifecycle(turn *ThreadTurnSnapshot, thread ThreadSnapshot) {
	if turn == nil || turn.TurnID == "" || turn.TurnID != thread.LatestTurnID {
		return
	}
	turn.Recoverable = thread.Recoverable
	turn.CanRetry = thread.CanRetry
	if thread.Status == ThreadStatusInterrupted && turn.Status == TurnStatusRunning {
		turn.Status = TurnStatusInterrupted
		turn.Failure = &ThreadTurnFailure{
			Code:    ThreadTurnFailureInterrupted,
			Message: sessiontree.InterruptedTurnFailureMessage,
		}
	}
}

func projectThreadTurnSnapshots(threadID ThreadID, events []ThreadDetailEvent) ([]ThreadTurnSnapshot, int64, error) {
	turnOrder := make([]TurnID, 0)
	byTurn := make(map[TurnID][]ThreadDetailEvent)
	seen := make(map[TurnID]bool)
	var through int64
	for _, event := range events {
		if event.Ordinal > through {
			through = event.Ordinal
		}
		turnID := event.TurnID
		if strings.TrimSpace(string(turnID)) == "" {
			continue
		}
		if event.TurnMarker != nil && event.TurnMarker.Status == string(sessiontree.TurnStarted) && !seen[turnID] {
			seen[turnID] = true
			turnOrder = append(turnOrder, turnID)
		}
		byTurn[turnID] = append(byTurn[turnID], event)
	}

	turns := make([]ThreadTurnSnapshot, 0, len(turnOrder))
	for _, turnID := range turnOrder {
		turnEvents := byTurn[turnID]
		runID, ordinal, startedAt := threadTurnStartedIdentity(turnEvents)
		if strings.TrimSpace(string(runID)) == "" || ordinal <= 0 || startedAt.IsZero() {
			return nil, 0, fmt.Errorf("turn %q has an invalid started marker", turnID)
		}
		userEntryID, userInput, userAttachments, userReferences := canonicalTurnUserInput(events, turnID)
		retryAuthority, err := readThreadTurnRetryAuthority(turnEvents, turnID)
		if err != nil {
			return nil, 0, err
		}
		if retryAuthority == nil && strings.TrimSpace(userEntryID) == "" {
			return nil, 0, fmt.Errorf("%w: canonical turn %q has no user admission", ErrAuthorityCorrupt, turnID)
		}
		if retryAuthority != nil && strings.TrimSpace(userEntryID) != "" {
			return nil, 0, fmt.Errorf("%w: retry turn %q duplicated its source user admission", ErrAuthorityCorrupt, turnID)
		}
		projection := ProjectThreadTurn(ProjectThreadTurnRequest{
			ThreadID: threadID,
			TurnID:   turnID,
			RunID:    runID,
			TraceID:  TraceID(runID),
			Events:   turnEvents,
		})
		if err := projection.Validate(); err != nil {
			return nil, 0, fmt.Errorf("project turn %q: %w", turnID, err)
		}
		turn := ThreadTurnSnapshot{
			TurnID:          turnID,
			RunID:           runID,
			Ordinal:         ordinal,
			StartedAt:       startedAt,
			UpdatedAt:       turnEvents[len(turnEvents)-1].CreatedAt,
			UserEntryID:     userEntryID,
			UserInput:       userInput,
			UserAttachments: userAttachments,
			UserReferences:  userReferences,
			RetrySource:     publicThreadTurnRetrySource(retryAuthority),
			Status:          projection.Status,
			Failure:         canonicalTurnFailure(turnEvents),
			Projection:      projection,
			ControlSignals:  threadTurnControlSignals(turnEvents),
			ThroughOrdinal:  projection.ThroughOrdinal,
		}
		turn.Status = canonicalTurnStatus(turn.Status, turn.Failure)
		if err := validateThreadTurnFailureForStatus(turn.Status, turn.Failure); err != nil {
			return nil, 0, fmt.Errorf("%w: canonical turn %q failure state is invalid: %v", ErrAuthorityCorrupt, turnID, err)
		}
		turns = append(turns, turn)
	}
	return turns, through, nil
}

type threadTurnRetryAuthority struct {
	TurnID  TurnID
	EntryID string
}

func readThreadTurnRetryAuthority(events []ThreadDetailEvent, turnID TurnID) (*threadTurnRetryAuthority, error) {
	for _, event := range events {
		if event.TurnID != turnID || event.TurnMarker == nil || event.TurnMarker.Status != string(sessiontree.TurnStarted) {
			continue
		}
		rawTurnID := event.TurnMarker.Metadata[sessiontree.RetrySourceTurnIDMetadataKey]
		rawEntryID := event.TurnMarker.Metadata[sessiontree.RetrySourceEntryIDMetadataKey]
		sourceTurnID := strings.TrimSpace(rawTurnID)
		sourceEntryID := strings.TrimSpace(rawEntryID)
		if sourceTurnID == "" && sourceEntryID == "" {
			return nil, nil
		}
		if sourceTurnID == "" || sourceEntryID == "" || rawTurnID != sourceTurnID || rawEntryID != sourceEntryID || TurnID(sourceTurnID) == turnID {
			return nil, fmt.Errorf("%w: canonical turn %q has an invalid retry source", ErrAuthorityCorrupt, turnID)
		}
		return &threadTurnRetryAuthority{TurnID: TurnID(sourceTurnID), EntryID: sourceEntryID}, nil
	}
	return nil, fmt.Errorf("%w: canonical turn %q has no started marker", ErrAuthorityCorrupt, turnID)
}

func runtimeCanonicalTurnRetryAuthority(source *sessiontree.CanonicalTurnRetrySource) *threadTurnRetryAuthority {
	if source == nil {
		return nil
	}
	return &threadTurnRetryAuthority{TurnID: TurnID(source.TurnID), EntryID: source.EntryID}
}

func sameThreadTurnRetryAuthority(first, second *threadTurnRetryAuthority) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return first.TurnID == second.TurnID && first.EntryID == second.EntryID
}

func publicThreadTurnRetrySource(source *threadTurnRetryAuthority) *ThreadTurnRetrySource {
	if source == nil {
		return nil
	}
	return &ThreadTurnRetrySource{TurnID: source.TurnID}
}

func canonicalTurnStatus(status TurnStatus, failure *ThreadTurnFailure) TurnStatus {
	if status == TurnStatusCancelled && failure != nil && failure.Code == ThreadTurnFailureInterrupted {
		return TurnStatusInterrupted
	}
	return status
}

func threadTurnStartedIdentity(events []ThreadDetailEvent) (RunID, int64, time.Time) {
	for _, event := range events {
		if event.TurnMarker == nil || event.TurnMarker.Status != string(sessiontree.TurnStarted) {
			continue
		}
		return RunID(strings.TrimSpace(event.TurnMarker.Metadata["run_id"])), event.Ordinal, event.CreatedAt
	}
	return "", 0, time.Time{}
}

func canonicalTurnUserInput(events []ThreadDetailEvent, turnID TurnID) (string, string, []MessageAttachment, []MessageReference) {
	for _, event := range events {
		if event.TurnID == turnID && event.Kind == ThreadDetailEventUserMessage && event.Message != nil {
			return event.ID, event.Message.Content, cloneMessageAttachments(event.Message.Attachments), append([]MessageReference(nil), event.Message.References...)
		}
	}
	return "", "", nil, nil
}

func canonicalTurnFailure(events []ThreadDetailEvent) *ThreadTurnFailure {
	message := ""
	terminalMessage := ""
	code := ThreadTurnFailureCode("")
	for _, event := range events {
		if event.Kind == ThreadDetailEventError && strings.TrimSpace(event.Error) != "" {
			message = strings.TrimSpace(event.Error)
		}
		if event.Kind == ThreadDetailEventTurnMarker && event.TurnMarker != nil {
			if value := strings.TrimSpace(event.TurnMarker.Metadata[sessiontree.TurnFailureCodeMetadataKey]); value != "" {
				code = ThreadTurnFailureCode(value)
				terminalMessage = strings.TrimSpace(event.TurnMarker.Metadata["failure_reason"])
			}
		}
	}
	if terminalMessage != "" {
		message = terminalMessage
	}
	if message == "" && code == "" {
		return nil
	}
	return &ThreadTurnFailure{Code: code, Message: message}
}

func threadTurnControlSignals(events []ThreadDetailEvent) []ThreadControlSignal {
	out := make([]ThreadControlSignal, 0)
	for _, event := range events {
		if event.Kind != ThreadDetailEventToolCall || event.ToolCall == nil || event.ToolCall.ControlSignal == nil {
			continue
		}
		signal := event.ToolCall.ControlSignal
		out = append(out, ThreadControlSignal{
			Name:        signal.Name,
			CallID:      signal.CallID,
			Disposition: signal.Disposition,
			Text:        signal.Text,
			ArgsHash:    signal.ArgsHash,
			Payload:     cloneAnyMap(signal.Payload),
		})
	}
	return out
}
