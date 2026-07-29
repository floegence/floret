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

	"github.com/floegence/floret/v2/internal/agentharness"
	"github.com/floegence/floret/v2/internal/session"
	"github.com/floegence/floret/v2/internal/sessiontree"
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
	out := RecoverInterruptedTurnResult{
		ThreadID: ThreadID(result.ThreadID), TurnID: TurnID(result.TurnID), RunID: RunID(result.RunID), Status: status, Failure: failure, Replayed: result.Replayed,
	}
	if err := out.Validate(); err != nil {
		return RecoverInterruptedTurnResult{}, invalidPublicResult("interrupted recovery result", err)
	}
	return out, nil
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

// ReadThreadTurnRequest identifies one canonical turn on a thread's current
// active path. It is a Go host contract, not a wire schema.
type ReadThreadTurnRequest struct {
	ThreadID ThreadID
	TurnID   TurnID
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

// ThreadUserMessageOrigin identifies how Floret admitted one canonical user
// message. Hosts may use it for presentation, but it is not authorization or a
// storage locator.
type ThreadUserMessageOrigin string

const (
	ThreadUserMessageOriginUser                  ThreadUserMessageOrigin = "user"
	ThreadUserMessageOriginDelegatedMission      ThreadUserMessageOrigin = "delegated_mission"
	ThreadUserMessageOriginSubAgentInput         ThreadUserMessageOrigin = "subagent_input"
	ThreadUserMessageOriginPendingToolCompletion ThreadUserMessageOrigin = "pending_tool_completion"
)

type ThreadTurnSnapshot struct {
	TurnID    TurnID    `json:"turn_id"`
	RunID     RunID     `json:"run_id"`
	Ordinal   int64     `json:"ordinal"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// UserEntryID is the opaque identity of the admitted canonical user Entry.
	// It is a presentation anchor, not authorization or a storage access handle.
	UserEntryID       string                  `json:"user_entry_id,omitempty"`
	UserMessageOrigin ThreadUserMessageOrigin `json:"user_message_origin,omitempty"`
	UserInput         string                  `json:"user_input,omitempty"`
	UserAttachments   []MessageAttachment     `json:"user_attachments,omitempty"`
	UserReferences    []MessageReference      `json:"user_references,omitempty"`
	RetrySource       *ThreadTurnRetrySource  `json:"retry_source,omitempty"`
	Status            TurnStatus              `json:"status"`
	Failure           *ThreadTurnFailure      `json:"failure,omitempty"`
	Recoverable       bool                    `json:"recoverable"`
	CanRetry          bool                    `json:"can_retry"`
	Projection        ThreadTurnProjection    `json:"projection"`
	ControlSignals    []ThreadControlSignal   `json:"control_signals,omitempty"`
	ThroughOrdinal    int64                   `json:"through_ordinal"`
}

type ThreadTurnRetrySource struct {
	// TurnID is the canonical source turn. Its internal journal anchor remains
	// private to Floret.
	TurnID TurnID `json:"turn_id"`
}

// Validate checks the self-contained public turn snapshot shape. Durable path
// and admission authority are validated before this DTO is projected.
func (s ThreadTurnSnapshot) Validate() error {
	if strings.TrimSpace(string(s.TurnID)) == "" || string(s.TurnID) != strings.TrimSpace(string(s.TurnID)) ||
		strings.TrimSpace(string(s.RunID)) == "" || string(s.RunID) != strings.TrimSpace(string(s.RunID)) {
		return errors.New("thread turn snapshot identity is incomplete or not trim-stable")
	}
	if s.Ordinal <= 0 || s.ThroughOrdinal < s.Ordinal {
		return errors.New("thread turn snapshot ordinals are invalid")
	}
	if s.StartedAt.IsZero() || s.UpdatedAt.IsZero() || s.UpdatedAt.Before(s.StartedAt) ||
		s.StartedAt != s.StartedAt.UTC() || s.UpdatedAt != s.UpdatedAt.UTC() {
		return errors.New("thread turn snapshot timestamps are invalid")
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported thread turn status %q", s.Status)
	}
	if err := validateThreadTurnFailureForStatus(s.Status, s.Failure); err != nil {
		return err
	}
	if err := s.Projection.Validate(); err != nil {
		return fmt.Errorf("thread turn projection: %w", err)
	}
	if s.Projection.ProjectedAt.IsZero() || s.Projection.ProjectedAt != s.Projection.ProjectedAt.UTC() {
		return errors.New("thread turn projection requires a UTC projection time")
	}
	if s.Projection.TurnID != s.TurnID || s.Projection.RunID != s.RunID || s.Projection.ThroughOrdinal != s.ThroughOrdinal {
		return errors.New("thread turn snapshot projection identity or ordinal is inconsistent")
	}
	if err := validateThreadTurnSnapshotStatus(s); err != nil {
		return err
	}
	if s.Recoverable && s.Status != TurnStatusInterrupted {
		return errors.New("thread turn recoverability conflicts with status")
	}
	if s.RetrySource == nil {
		if strings.TrimSpace(s.UserEntryID) == "" || s.UserEntryID != strings.TrimSpace(s.UserEntryID) {
			return errors.New("normal thread turn requires a trim-stable user entry identity")
		}
		if !s.UserMessageOrigin.valid() {
			return fmt.Errorf("unsupported thread user message origin %q", s.UserMessageOrigin)
		}
		if strings.TrimSpace(s.UserInput) == "" && len(s.UserAttachments) == 0 && len(s.UserReferences) == 0 {
			return errors.New("normal thread turn requires canonical user input")
		}
	} else {
		if strings.TrimSpace(string(s.RetrySource.TurnID)) == "" || string(s.RetrySource.TurnID) != strings.TrimSpace(string(s.RetrySource.TurnID)) ||
			s.RetrySource.TurnID == s.TurnID {
			return errors.New("retry turn source identity is invalid")
		}
		if s.UserEntryID != "" || s.UserMessageOrigin != "" || s.UserInput != "" ||
			len(s.UserAttachments) != 0 || len(s.UserReferences) != 0 {
			return errors.New("retry turn must not duplicate canonical user facts")
		}
	}
	if err := session.ValidateStoredMessageAttachments(sessionMessageAttachments(s.UserAttachments)); err != nil {
		return fmt.Errorf("stored thread turn attachments: %w", err)
	}
	if err := validateMessageReferences(s.UserReferences); err != nil {
		return fmt.Errorf("stored thread turn references: %w", err)
	}
	for index, signal := range s.ControlSignals {
		if strings.TrimSpace(signal.Name) == "" || signal.Name != strings.TrimSpace(signal.Name) ||
			strings.TrimSpace(signal.CallID) == "" || signal.CallID != strings.TrimSpace(signal.CallID) ||
			strings.TrimSpace(signal.ArgsHash) == "" || signal.ArgsHash != strings.TrimSpace(signal.ArgsHash) {
			return fmt.Errorf("thread control signal %d has incomplete identity", index)
		}
		switch signal.Disposition {
		case string(SignalContinue), string(SignalWaiting), string(SignalTerminal):
		default:
			return fmt.Errorf("thread control signal %d has unsupported disposition %q", index, signal.Disposition)
		}
	}
	return nil
}

func (o ThreadUserMessageOrigin) valid() bool {
	switch o {
	case ThreadUserMessageOriginUser, ThreadUserMessageOriginDelegatedMission,
		ThreadUserMessageOriginSubAgentInput, ThreadUserMessageOriginPendingToolCompletion:
		return true
	default:
		return false
	}
}

func validateThreadTurnSnapshotStatus(snapshot ThreadTurnSnapshot) error {
	if snapshot.Status == snapshot.Projection.Status {
		return nil
	}
	if snapshot.Status == TurnStatusInterrupted && snapshot.Failure != nil && snapshot.Failure.Code == ThreadTurnFailureInterrupted &&
		(snapshot.Projection.Status == TurnStatusRunning || snapshot.Projection.Status == TurnStatusCancelled) {
		return nil
	}
	return fmt.Errorf("thread turn status %q conflicts with projection status %q", snapshot.Status, snapshot.Projection.Status)
}

// Validate checks one public turn page without consulting persisted state.
func (p ThreadTurnsPage) Validate() error {
	if strings.TrimSpace(string(p.ThreadID)) == "" || string(p.ThreadID) != strings.TrimSpace(string(p.ThreadID)) {
		return errors.New("thread turn page requires a trim-stable thread identity")
	}
	if p.ThroughOrdinal < 0 || p.GeneratedAt.IsZero() || p.GeneratedAt != p.GeneratedAt.UTC() {
		return errors.New("thread turn page boundary or generation time is invalid")
	}
	if p.HasMore && len(p.Turns) == 0 {
		return errors.New("thread turn page cannot continue without returned turns")
	}
	if p.BeforeCursor != nil && !p.HasMore {
		return errors.New("thread turn page before cursor requires more history")
	}
	if p.BeforeCursor != nil && (strings.TrimSpace(string(*p.BeforeCursor)) == "" || string(*p.BeforeCursor) != strings.TrimSpace(string(*p.BeforeCursor))) {
		return errors.New("thread turn page before cursor is invalid")
	}
	if p.SinceCursor != "" && (strings.TrimSpace(string(p.SinceCursor)) == "" || string(p.SinceCursor) != strings.TrimSpace(string(p.SinceCursor))) {
		return errors.New("thread turn page since cursor is invalid")
	}
	var previous int64
	seen := make(map[TurnID]struct{}, len(p.Turns))
	for index, turn := range p.Turns {
		if err := turn.Validate(); err != nil {
			return fmt.Errorf("thread turn page item %d: %w", index, err)
		}
		if turn.Projection.ThreadID != p.ThreadID || turn.Ordinal <= previous || turn.ThroughOrdinal > p.ThroughOrdinal {
			return fmt.Errorf("thread turn page item %d has inconsistent identity or ordinal", index)
		}
		if _, duplicate := seen[turn.TurnID]; duplicate {
			return fmt.Errorf("thread turn page contains duplicate turn %q", turn.TurnID)
		}
		seen[turn.TurnID] = struct{}{}
		previous = turn.Ordinal
	}
	return nil
}

// Validate checks the self-contained public overview shape.
func (o ThreadOverview) Validate() error {
	if err := o.Thread.Validate(); err != nil {
		return fmt.Errorf("thread overview snapshot: %w", err)
	}
	if o.LatestTurn == nil {
		if o.Thread.LatestTurnID != "" || o.Thread.LatestRunID != "" {
			return errors.New("thread overview is missing its latest turn")
		}
		return nil
	}
	if err := o.LatestTurn.Validate(); err != nil {
		return fmt.Errorf("thread overview latest turn: %w", err)
	}
	if o.LatestTurn.Projection.ThreadID != o.Thread.ID || o.LatestTurn.TurnID != o.Thread.LatestTurnID ||
		o.LatestTurn.RunID != o.Thread.LatestRunID || o.LatestTurn.ThroughOrdinal > o.Thread.ThroughOrdinal {
		return errors.New("thread overview latest turn is inconsistent with its thread snapshot")
	}
	return nil
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

// ReadThreadTurn returns one canonical turn bound to this root read host.
func (h *ThreadReadHost) ReadThreadTurn(ctx context.Context, req ReadThreadTurnRequest) (ThreadTurnSnapshot, error) {
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
	defer done()
	if err := validateBoundRootThreadAuthority(ctx, h.store, h.threadID, req.ThreadID, "thread read host"); err != nil {
		return ThreadTurnSnapshot{}, err
	}
	return readThreadTurn(ctx, h.harness, req)
}

// ReadThreadTurn returns one canonical turn for a complete descendant of the
// parent bound to this read host.
func (h *SubAgentReadHost) ReadThreadTurn(ctx context.Context, req ReadThreadTurnRequest) (ThreadTurnSnapshot, error) {
	if h == nil {
		return ThreadTurnSnapshot{}, errors.New("subagent read host is required")
	}
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
	defer done()
	if h.harness == nil || strings.TrimSpace(string(h.parentThreadID)) == "" {
		return ThreadTurnSnapshot{}, errors.New("subagent read host is invalid")
	}
	if err := h.harness.ValidateSubAgentDescendantAuthority(ctx, string(h.parentThreadID), string(req.ThreadID)); err != nil {
		return ThreadTurnSnapshot{}, runtimeHostError(err)
	}
	return readThreadTurn(ctx, h.harness, req)
}

func readThreadTurn(ctx context.Context, harness *agentharness.AgentHarness, req ReadThreadTurnRequest) (ThreadTurnSnapshot, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return ThreadTurnSnapshot{}, errors.New("thread id is required")
	}
	if strings.TrimSpace(string(req.TurnID)) == "" {
		return ThreadTurnSnapshot{}, errors.New("turn id is required")
	}
	read, err := harness.ReadCanonicalTurnDetailEvents(ctx, string(req.ThreadID), string(req.TurnID), true)
	if err != nil {
		return ThreadTurnSnapshot{}, runtimeHostError(err)
	}
	detail := read.Turn
	if detail.TurnID != string(req.TurnID) {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: exact canonical turn identity changed", ErrAuthorityCorrupt)
	}
	turn, err := projectCanonicalThreadTurnSnapshot(req.ThreadID, detail)
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
	if turn.ThroughOrdinal > read.ThroughOrdinal {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: exact canonical turn boundary is inconsistent", ErrAuthorityCorrupt)
	}
	if turn.TurnID == TurnID(read.LatestTurnID) {
		applyLatestThreadLifecycle(&turn, ThreadSnapshot{
			LatestTurnID: TurnID(read.LatestTurnID), Status: ThreadStatus(read.LatestStatus),
			Recoverable: read.LatestRecoverable, CanRetry: read.LatestCanRetry,
		})
	}
	if err := turn.Validate(); err != nil {
		return ThreadTurnSnapshot{}, invalidPublicResult("thread turn snapshot", err)
	}
	return turn, nil
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
		return ThreadOverview{}, invalidPublicResult("thread snapshot", err)
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
	if err := result.Validate(); err != nil {
		return ThreadOverview{}, invalidPublicResult("thread overview", err)
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
		return ThreadTurnSnapshot{}, invalidPublicResult("thread snapshot", err)
	}
	latest := turns[0]
	if latest.TurnID != publicThread.LatestTurnID || latest.RunID != publicThread.LatestRunID || latest.ThroughOrdinal > publicThread.ThroughOrdinal {
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: latest canonical turn is inconsistent with its thread snapshot", ErrAuthorityCorrupt)
	}
	applyLatestThreadLifecycle(&latest, publicThread)
	if err := latest.Validate(); err != nil {
		return ThreadTurnSnapshot{}, invalidPublicResult("thread turn snapshot", err)
	}
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
		GeneratedAt:    detailPage.GeneratedAt.UTC(),
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
	if err := page.Validate(); err != nil {
		return ThreadTurnsPage{}, invalidPublicResult("thread turns page", err)
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
	userEntryID, userMessageOrigin, userInput, userAttachments, userReferences, err := canonicalTurnUserInput(events, turnID)
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
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
		return ThreadTurnSnapshot{}, fmt.Errorf("%w: project turn %q: %v", ErrAuthorityCorrupt, turnID, err)
	}
	turn := ThreadTurnSnapshot{
		TurnID:            turnID,
		RunID:             runID,
		Ordinal:           ordinal,
		StartedAt:         startedAt.UTC(),
		UpdatedAt:         events[len(events)-1].CreatedAt.UTC(),
		UserEntryID:       userEntryID,
		UserMessageOrigin: userMessageOrigin,
		UserInput:         userInput,
		UserAttachments:   userAttachments,
		UserReferences:    userReferences,
		RetrySource:       publicThreadTurnRetrySource(retryAuthority),
		Status:            projection.Status,
		Failure:           canonicalTurnFailure(events),
		Projection:        projection,
		ControlSignals:    threadTurnControlSignals(events),
		ThroughOrdinal:    projection.ThroughOrdinal,
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
			return nil, 0, fmt.Errorf("%w: turn %q has an invalid started marker", ErrAuthorityCorrupt, turnID)
		}
		userEntryID, userMessageOrigin, userInput, userAttachments, userReferences, err := canonicalTurnUserInput(events, turnID)
		if err != nil {
			return nil, 0, err
		}
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
			return nil, 0, fmt.Errorf("%w: project turn %q: %v", ErrAuthorityCorrupt, turnID, err)
		}
		turn := ThreadTurnSnapshot{
			TurnID:            turnID,
			RunID:             runID,
			Ordinal:           ordinal,
			StartedAt:         startedAt.UTC(),
			UpdatedAt:         turnEvents[len(turnEvents)-1].CreatedAt.UTC(),
			UserEntryID:       userEntryID,
			UserMessageOrigin: userMessageOrigin,
			UserInput:         userInput,
			UserAttachments:   userAttachments,
			UserReferences:    userReferences,
			RetrySource:       publicThreadTurnRetrySource(retryAuthority),
			Status:            projection.Status,
			Failure:           canonicalTurnFailure(turnEvents),
			Projection:        projection,
			ControlSignals:    threadTurnControlSignals(turnEvents),
			ThroughOrdinal:    projection.ThroughOrdinal,
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

func canonicalTurnUserInput(events []ThreadDetailEvent, turnID TurnID) (string, ThreadUserMessageOrigin, string, []MessageAttachment, []MessageReference, error) {
	for _, event := range events {
		if event.TurnID == turnID && event.Kind == ThreadDetailEventUserMessage && event.Message != nil {
			origin, err := threadUserMessageOrigin(event.Metadata)
			if err != nil {
				return "", "", "", nil, nil, fmt.Errorf("%w: canonical turn %q has invalid user message origin: %v", ErrAuthorityCorrupt, turnID, err)
			}
			return event.ID, origin, event.Message.Content, cloneMessageAttachments(event.Message.Attachments), append([]MessageReference(nil), event.Message.References...), nil
		}
	}
	return "", "", "", nil, nil, nil
}

func threadUserMessageOrigin(metadata map[string]string) (ThreadUserMessageOrigin, error) {
	raw, ok := metadata[sessiontree.SubAgentUserMessageOriginMetadataKey]
	if !ok {
		return ThreadUserMessageOriginUser, nil
	}
	if raw == "" || raw != strings.TrimSpace(raw) {
		return "", errors.New("origin is empty or not normalized")
	}
	switch raw {
	case sessiontree.SubAgentUserMessageOriginDelegatedMission:
		return ThreadUserMessageOriginDelegatedMission, nil
	case sessiontree.SubAgentUserMessageOriginInput:
		return ThreadUserMessageOriginSubAgentInput, nil
	case sessiontree.SubAgentUserMessageOriginPendingToolCompletion:
		return ThreadUserMessageOriginPendingToolCompletion, nil
	default:
		return "", fmt.Errorf("unsupported origin %q", raw)
	}
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
