package sessionlifecycle

import (
	"github.com/floegence/floret/control"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/sessiontree"
)

type status string

const (
	statusIdle        status = "idle"
	statusRunning     status = "running"
	statusCompleted   status = "completed"
	statusWaiting     status = "waiting"
	statusFailed      status = "failed"
	statusCancelled   status = "cancelled"
	statusInterrupted status = "interrupted"
)

type phase string

const (
	phaseIdle phase = "idle"
	phaseTurn phase = "turn"
)

const (
	PhaseIdle = string(phaseIdle)
	PhaseTurn = string(phaseTurn)
)

type Lifecycle struct {
	status        status
	phase         phase
	latestTurnID  string
	recoverable   bool
	waitingPrompt string
}

func (l Lifecycle) Status() string {
	return string(l.status)
}

func (l Lifecycle) Phase() string {
	return string(l.phase)
}

func (l Lifecycle) LatestTurnID() string {
	return l.latestTurnID
}

func (l Lifecycle) Recoverable() bool {
	return l.recoverable
}

func (l Lifecycle) WaitingPrompt() string {
	return l.waitingPrompt
}

func (l Lifecycle) CanAppendMessage() bool {
	return l.status == statusIdle || l.status == statusCompleted || l.status == statusWaiting
}

func (l Lifecycle) IsRunning() bool {
	return isRunning(l.status, l.phase)
}

// IMPORTANT: SessionLifecycle is the only host/UI boundary for session status,
// recoverability, and appendability. Do not derive these decisions directly from
// engine status, thread phase, sessiontree markers, or inspector transitions.
func Derive(path []sessiontree.Entry, rawPhase string) Lifecycle {
	lifecycle := Lifecycle{status: statusIdle, phase: normalizePhase(rawPhase)}
	started := map[string]bool{}
	terminal := map[string]bool{}
	for _, entry := range path {
		if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus == "" {
			continue
		}
		if entry.TurnID != "" {
			lifecycle.latestTurnID = entry.TurnID
		}
		switch entry.TurnStatus {
		case sessiontree.TurnStarted:
			if entry.TurnID != "" {
				started[entry.TurnID] = true
			}
			lifecycle.status = statusForStarted(lifecycle.phase)
			lifecycle.waitingPrompt = ""
			lifecycle.recoverable = lifecycle.status == statusInterrupted
		case sessiontree.TurnCompleted:
			if entry.TurnID != "" {
				terminal[entry.TurnID] = true
			}
			lifecycle.status = statusCompleted
			lifecycle.waitingPrompt = ""
			lifecycle.recoverable = false
		case sessiontree.TurnWaiting:
			if entry.TurnID != "" {
				terminal[entry.TurnID] = true
			}
			lifecycle.status = statusWaiting
			lifecycle.waitingPrompt = waitingPromptForTurn(path, entry.TurnID)
			lifecycle.recoverable = false
		case sessiontree.TurnFailed:
			if entry.TurnID != "" {
				terminal[entry.TurnID] = true
			}
			lifecycle.status = statusFailed
			lifecycle.waitingPrompt = ""
			lifecycle.recoverable = false
		case sessiontree.TurnAborted:
			if entry.TurnID != "" {
				terminal[entry.TurnID] = true
			}
			lifecycle.waitingPrompt = ""
			lifecycle.recoverable = entry.Metadata["recoverable"] == "true"
			if lifecycle.recoverable {
				lifecycle.status = statusInterrupted
			} else {
				lifecycle.status = statusCancelled
			}
		}
	}
	if lifecycle.latestTurnID != "" && started[lifecycle.latestTurnID] && !terminal[lifecycle.latestTurnID] {
		lifecycle.status = statusForStarted(lifecycle.phase)
		lifecycle.waitingPrompt = ""
		lifecycle.recoverable = lifecycle.status == statusInterrupted
	}
	return lifecycle
}

func IsRunningStatus(rawStatus, rawPhase string) bool {
	return isRunning(normalizeStatus(rawStatus), normalizePhase(rawPhase))
}

func MarkerForEngineStatus(status engine.Status) sessiontree.TurnMarkerStatus {
	switch status {
	case engine.Completed:
		return sessiontree.TurnCompleted
	case engine.Waiting:
		return sessiontree.TurnWaiting
	case engine.Cancelled:
		return sessiontree.TurnAborted
	default:
		return sessiontree.TurnFailed
	}
}

func normalizePhase(raw string) phase {
	if raw == string(phaseTurn) {
		return phaseTurn
	}
	return phaseIdle
}

func normalizeStatus(raw string) status {
	switch raw {
	case string(statusRunning):
		return statusRunning
	case string(statusCompleted):
		return statusCompleted
	case string(statusWaiting):
		return statusWaiting
	case string(statusFailed):
		return statusFailed
	case string(statusCancelled):
		return statusCancelled
	case string(statusInterrupted):
		return statusInterrupted
	default:
		return statusIdle
	}
}

func isRunning(currentStatus status, currentPhase phase) bool {
	return currentStatus == statusRunning || currentPhase == phaseTurn
}

func statusForStarted(current phase) status {
	if current == phaseTurn {
		return statusRunning
	}
	return statusInterrupted
}

func waitingPromptForTurn(path []sessiontree.Entry, turnID string) string {
	for i := len(path) - 1; i >= 0; i-- {
		entry := path[i]
		if entry.TurnID != turnID || entry.Type != sessiontree.EntryToolCall {
			continue
		}
		if entry.Message.ToolName == "ask_user" {
			if signal, ok, err := control.Project(provider.ToolCall{Name: entry.Message.ToolName, Args: entry.Message.ToolArgs}); ok && err == nil {
				return signal.Prompt
			}
			return entry.Message.ToolArgs
		}
	}
	return ""
}
