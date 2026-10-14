package sessionlifecycle

import (
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/internal/control"
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/sessiontree"
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
	PhaseIdle         = string(phaseIdle)
	PhaseTurn         = string(phaseTurn)
	StatusIdle        = string(statusIdle)
	StatusRunning     = string(statusRunning)
	StatusCompleted   = string(statusCompleted)
	StatusWaiting     = string(statusWaiting)
	StatusFailed      = string(statusFailed)
	StatusCancelled   = string(statusCancelled)
	StatusInterrupted = string(statusInterrupted)
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

// ValidateProjection validates the public lifecycle facts emitted from Derive
// without reconstructing journal state or retry eligibility.
func ValidateProjection(rawStatus, rawPhase, latestTurnID, waitingPrompt string, recoverable, canAppendMessage bool) error {
	currentStatus, ok := strictStatus(rawStatus)
	if !ok {
		return fmt.Errorf("unsupported thread status %q", rawStatus)
	}
	currentPhase, ok := strictPhase(rawPhase)
	if !ok {
		return fmt.Errorf("unsupported thread phase %q", rawPhase)
	}
	if currentStatus == statusRunning && currentPhase != phaseTurn {
		return errors.New("running thread status requires active turn phase")
	}
	if currentStatus == statusIdle && latestTurnID != "" {
		return errors.New("idle thread must not identify a latest turn")
	}
	if currentStatus != statusIdle && strings.TrimSpace(latestTurnID) == "" {
		return errors.New("non-idle thread requires latest turn id")
	}
	if recoverable != (currentStatus == statusInterrupted) {
		return errors.New("thread recoverability conflicts with status")
	}
	wantAppend := currentStatus == statusIdle || currentStatus == statusCompleted || currentStatus == statusWaiting
	if canAppendMessage != wantAppend {
		return errors.New("thread append capability conflicts with status")
	}
	if currentStatus != statusWaiting && waitingPrompt != "" {
		return errors.New("thread waiting prompt requires waiting status")
	}
	return nil
}

func Running(latestTurnID string) Lifecycle {
	return Lifecycle{status: statusRunning, phase: phaseTurn, latestTurnID: latestTurnID}
}

// IMPORTANT: SessionLifecycle is the only host/UI boundary for session status,
// recoverability, and appendability. Do not derive these decisions directly from
// engine status, thread phase, sessiontree markers, or inspector transitions.
func Derive(path []sessiontree.Entry, rawPhase string) Lifecycle {
	phase := normalizePhase(rawPhase)
	facts := sessiontree.DeriveCanonicalLifecycle(path, phase == phaseTurn)
	result := Lifecycle{status: status(facts.Status), phase: phase, latestTurnID: facts.LatestTurnID, recoverable: facts.Recoverable}
	if result.status == statusWaiting {
		result.waitingPrompt = waitingPromptForTurn(path, facts.LatestTurnID)
	}
	return result
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

func strictPhase(raw string) (phase, bool) {
	switch raw {
	case string(phaseIdle):
		return phaseIdle, true
	case string(phaseTurn):
		return phaseTurn, true
	default:
		return "", false
	}
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

func strictStatus(raw string) (status, bool) {
	switch raw {
	case string(statusIdle):
		return statusIdle, true
	case string(statusRunning):
		return statusRunning, true
	case string(statusCompleted):
		return statusCompleted, true
	case string(statusWaiting):
		return statusWaiting, true
	case string(statusFailed):
		return statusFailed, true
	case string(statusCancelled):
		return statusCancelled, true
	case string(statusInterrupted):
		return statusInterrupted, true
	default:
		return "", false
	}
}

func isRunning(currentStatus status, currentPhase phase) bool {
	return currentStatus == statusRunning || currentPhase == phaseTurn
}

func waitingPromptForTurn(path []sessiontree.Entry, turnID string) string {
	for i := len(path) - 1; i >= 0; i-- {
		entry := path[i]
		if entry.TurnID != turnID {
			continue
		}
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
			return entry.Message.ToolResult.InputRequired.Summary
		}
		if entry.Type != sessiontree.EntryToolCall {
			continue
		}
		if entry.Message.ToolName == "ask_user" {
			if signal, ok, err := control.Project(provider.ToolCall{Name: entry.Message.ToolName, Args: entry.Message.ToolArgs}); ok && err == nil {
				return signal.Prompt
			}
			return ""
		}
	}
	return ""
}
