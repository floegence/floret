package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/sessiontree"
)

const (
	defaultThreadTurnsLimit = 50
	maxThreadTurnsLimit     = 200
)

type ListThreadTurnsRequest struct {
	ThreadID      ThreadID `json:"thread_id"`
	BeforeOrdinal int64    `json:"before_ordinal,omitempty"`
	AfterOrdinal  int64    `json:"after_ordinal,omitempty"`
	Tail          int      `json:"tail,omitempty"`
	Limit         int      `json:"limit,omitempty"`
}

type ThreadTurnsPage struct {
	ThreadID       ThreadID             `json:"thread_id"`
	Turns          []ThreadTurnSnapshot `json:"turns"`
	HasMore        bool                 `json:"has_more,omitempty"`
	ThroughOrdinal int64                `json:"through_ordinal"`
	GeneratedAt    time.Time            `json:"generated_at"`
}

type ThreadTurnSnapshot struct {
	TurnID         TurnID                `json:"turn_id"`
	RunID          RunID                 `json:"run_id"`
	Ordinal        int64                 `json:"ordinal"`
	StartedAt      time.Time             `json:"started_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
	UserEntryID    string                `json:"user_entry_id,omitempty"`
	UserInput      string                `json:"user_input,omitempty"`
	Status         TurnStatus            `json:"status"`
	Failure        string                `json:"failure,omitempty"`
	Projection     ThreadTurnProjection  `json:"projection"`
	ControlSignals []ThreadControlSignal `json:"control_signals,omitempty"`
	ThroughOrdinal int64                 `json:"through_ordinal"`
}

type ThreadControlSignal struct {
	Name        string         `json:"name"`
	CallID      string         `json:"call_id"`
	Disposition string         `json:"disposition,omitempty"`
	Text        string         `json:"text,omitempty"`
	ArgsHash    string         `json:"args_hash,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

func (h *Host) ListThreadTurns(ctx context.Context, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	return listThreadTurns(ctx, h.harness, req)
}

func (h *ThreadMaintenanceHost) ListThreadTurns(ctx context.Context, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	return listThreadTurns(ctx, h.harness, req)
}

func (h *Host) ReadLatestThreadTurn(ctx context.Context, threadID ThreadID) (ThreadTurnSnapshot, error) {
	return readLatestThreadTurn(ctx, h.harness, threadID)
}

func (h *ThreadMaintenanceHost) ReadLatestThreadTurn(ctx context.Context, threadID ThreadID) (ThreadTurnSnapshot, error) {
	return readLatestThreadTurn(ctx, h.harness, threadID)
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
	return turns[0], nil
}

func listThreadTurns(ctx context.Context, harness *agentharness.AgentHarness, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return ThreadTurnsPage{}, errors.New("thread id is required")
	}
	if req.BeforeOrdinal < 0 || req.AfterOrdinal < 0 || req.Tail < 0 || req.Limit < 0 {
		return ThreadTurnsPage{}, errors.New("thread turn pagination values must be non-negative")
	}
	modes := 0
	if req.BeforeOrdinal > 0 {
		modes++
	}
	if req.AfterOrdinal > 0 {
		modes++
	}
	if req.Tail > 0 {
		modes++
	}
	if modes > 1 {
		return ThreadTurnsPage{}, errors.New("before, after, and tail pagination modes are mutually exclusive")
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

	events, err := listAllRawThreadDetailEvents(ctx, harness, string(req.ThreadID))
	if err != nil {
		return ThreadTurnsPage{}, runtimeHostError(err)
	}
	turns, through, err := projectThreadTurnSnapshots(req.ThreadID, events)
	if err != nil {
		return ThreadTurnsPage{}, err
	}
	pageTurns, hasMore := pageThreadTurnSnapshots(turns, req, limit)
	return ThreadTurnsPage{
		ThreadID:       req.ThreadID,
		Turns:          pageTurns,
		HasMore:        hasMore,
		ThroughOrdinal: through,
		GeneratedAt:    time.Now().UTC(),
	}, nil
}

func listAllRawThreadDetailEvents(ctx context.Context, harness *agentharness.AgentHarness, threadID string) ([]ThreadDetailEvent, error) {
	var out []ThreadDetailEvent
	var afterOrdinal int64
	for {
		detail, err := harness.ListThreadDetailEvents(ctx, agentharness.ListThreadDetailEventsOptions{
			ThreadID:     threadID,
			AfterOrdinal: afterOrdinal,
			Limit:        agentharness.MaxThreadDetailEventLimit,
			IncludeRaw:   true,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, threadDetailEvents(detail.Events)...)
		if !detail.HasMore {
			return out, nil
		}
		if detail.NextOrdinal <= afterOrdinal {
			return nil, fmt.Errorf("thread detail pagination did not advance after ordinal %d", afterOrdinal)
		}
		afterOrdinal = detail.NextOrdinal
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
		userEntryID, userInput := canonicalTurnUserInput(events, turnID)
		if strings.TrimSpace(userEntryID) == "" {
			continue
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
		turns = append(turns, ThreadTurnSnapshot{
			TurnID:         turnID,
			RunID:          runID,
			Ordinal:        ordinal,
			StartedAt:      startedAt,
			UpdatedAt:      turnEvents[len(turnEvents)-1].CreatedAt,
			UserEntryID:    userEntryID,
			UserInput:      userInput,
			Status:         projection.Status,
			Failure:        canonicalTurnFailure(turnEvents),
			Projection:     projection,
			ControlSignals: threadTurnControlSignals(turnEvents),
			ThroughOrdinal: projection.ThroughOrdinal,
		})
	}
	return turns, through, nil
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

func canonicalTurnUserInput(events []ThreadDetailEvent, turnID TurnID) (string, string) {
	for _, event := range events {
		if event.TurnID == turnID && event.Kind == ThreadDetailEventUserMessage && event.Message != nil {
			return event.ID, event.Message.Content
		}
	}
	return "", ""
}

func canonicalTurnFailure(events []ThreadDetailEvent) string {
	failure := ""
	for _, event := range events {
		if event.Kind == ThreadDetailEventError && strings.TrimSpace(event.Error) != "" {
			failure = event.Error
		}
	}
	return failure
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

func pageThreadTurnSnapshots(turns []ThreadTurnSnapshot, req ListThreadTurnsRequest, limit int) ([]ThreadTurnSnapshot, bool) {
	if req.Tail > 0 {
		start := len(turns) - limit
		if start < 0 {
			start = 0
		}
		return append([]ThreadTurnSnapshot(nil), turns[start:]...), start > 0
	}
	if req.BeforeOrdinal > 0 {
		end := 0
		for end < len(turns) && turns[end].Ordinal < req.BeforeOrdinal {
			end++
		}
		start := end - limit
		if start < 0 {
			start = 0
		}
		return append([]ThreadTurnSnapshot(nil), turns[start:end]...), start > 0
	}
	start := 0
	for start < len(turns) && turns[start].Ordinal <= req.AfterOrdinal {
		start++
	}
	end := start + limit
	if end > len(turns) {
		end = len(turns)
	}
	return append([]ThreadTurnSnapshot(nil), turns[start:end]...), end < len(turns)
}
