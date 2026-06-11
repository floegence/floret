package sessionlifecycle

import (
	"testing"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestDeriveLifecycleTable(t *testing.T) {
	for _, tc := range []struct {
		name          string
		phase         string
		path          []sessiontree.Entry
		status        string
		latestTurnID  string
		recoverable   bool
		appendable    bool
		waitingPrompt string
	}{
		{
			name:       "empty idle",
			phase:      PhaseIdle,
			status:     "idle",
			appendable: true,
		},
		{
			name:         "started turn phase",
			phase:        PhaseTurn,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil)),
			status:       "running",
			latestTurnID: "turn-1",
		},
		{
			name:         "started idle phase",
			phase:        PhaseIdle,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil)),
			status:       "interrupted",
			latestTurnID: "turn-1",
			recoverable:  true,
		},
		{
			name:         "save point keeps running when phase turn",
			phase:        PhaseTurn,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil), marker("turn-1", sessiontree.TurnSavePoint, map[string]string{"reason": "tool_result_batch"})),
			status:       "running",
			latestTurnID: "turn-1",
		},
		{
			name:         "save point keeps interrupted when phase idle",
			phase:        PhaseIdle,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil), marker("turn-1", sessiontree.TurnSavePoint, map[string]string{"reason": "tool_result_batch"})),
			status:       "interrupted",
			latestTurnID: "turn-1",
			recoverable:  true,
		},
		{
			name:         "completed",
			phase:        PhaseIdle,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil), marker("turn-1", sessiontree.TurnCompleted, nil)),
			status:       "completed",
			latestTurnID: "turn-1",
			appendable:   true,
		},
		{
			name:          "waiting ask user prompt",
			phase:         PhaseIdle,
			path:          entries(marker("turn-1", sessiontree.TurnStarted, nil), toolCall("turn-1", "ask_user", `{"question":"Which file?"}`), marker("turn-1", sessiontree.TurnWaiting, nil)),
			status:        "waiting",
			latestTurnID:  "turn-1",
			appendable:    true,
			waitingPrompt: "Which file?",
		},
		{
			name:         "waiting malformed ask user hides raw args",
			phase:        PhaseIdle,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil), toolCall("turn-1", "ask_user", `{"question":`), marker("turn-1", sessiontree.TurnWaiting, nil)),
			status:       "waiting",
			latestTurnID: "turn-1",
			appendable:   true,
		},
		{
			name:         "failed",
			phase:        PhaseIdle,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil), marker("turn-1", sessiontree.TurnFailed, nil)),
			status:       "failed",
			latestTurnID: "turn-1",
		},
		{
			name:         "cancelled",
			phase:        PhaseIdle,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil), marker("turn-1", sessiontree.TurnAborted, nil)),
			status:       "cancelled",
			latestTurnID: "turn-1",
		},
		{
			name:         "recoverable aborted is interrupted",
			phase:        PhaseIdle,
			path:         entries(marker("turn-1", sessiontree.TurnStarted, nil), marker("turn-1", sessiontree.TurnAborted, map[string]string{"recoverable": "true"})),
			status:       "interrupted",
			latestTurnID: "turn-1",
			recoverable:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Derive(tc.path, tc.phase)
			if got.Status() != tc.status || got.Phase() != normalizeExpectedPhase(tc.phase) || got.LatestTurnID() != tc.latestTurnID ||
				got.Recoverable() != tc.recoverable || got.CanAppendMessage() != tc.appendable || got.WaitingPrompt() != tc.waitingPrompt {
				t.Fatalf("Derive() = status=%q phase=%q latest=%q recoverable=%v appendable=%v waiting=%q",
					got.Status(), got.Phase(), got.LatestTurnID(), got.Recoverable(), got.CanAppendMessage(), got.WaitingPrompt())
			}
		})
	}
}

func TestIsRunningStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
		phase  string
		want   bool
	}{
		{name: "running status", status: "running", phase: PhaseIdle, want: true},
		{name: "turn phase", status: "idle", phase: PhaseTurn, want: true},
		{name: "unknown status turn phase", status: "unexpected", phase: PhaseTurn, want: true},
		{name: "completed idle", status: "completed", phase: PhaseIdle},
		{name: "interrupted idle", status: "interrupted", phase: PhaseIdle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRunningStatus(tc.status, tc.phase); got != tc.want {
				t.Fatalf("IsRunningStatus(%q, %q) = %v, want %v", tc.status, tc.phase, got, tc.want)
			}
		})
	}
}

func entries(items ...sessiontree.Entry) []sessiontree.Entry {
	return items
}

func marker(turnID string, status sessiontree.TurnMarkerStatus, metadata map[string]string) sessiontree.Entry {
	return sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnID: turnID, TurnStatus: status, Metadata: metadata}
}

func toolCall(turnID, name, args string) sessiontree.Entry {
	return sessiontree.Entry{
		Type:   sessiontree.EntryToolCall,
		TurnID: turnID,
		Message: session.Message{
			Role:       session.Assistant,
			Content:    "tool_call",
			ToolName:   name,
			ToolArgs:   args,
			ToolCallID: "call-1",
		},
	}
}

func normalizeExpectedPhase(value string) string {
	if value == PhaseTurn {
		return PhaseTurn
	}
	return PhaseIdle
}
