package agentharness

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/sessiontree"
)

func validateTurnTerminalOutcome(threadID, turnID, runID string, outcome *sessiontree.TurnTerminalOutcome) error {
	if outcome == nil {
		return errors.New("turn terminal outcome is required")
	}
	terminal := outcome.Terminal
	markerRunID := strings.TrimSpace(terminal.Metadata["run_id"])
	if terminal.Type != sessiontree.EntryTurnMarker || terminal.ThreadID != threadID || terminal.TurnID != turnID ||
		(markerRunID != "" && markerRunID != runID) {
		return errors.New("turn terminal identity is invalid")
	}
	failureCode := strings.TrimSpace(terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey])
	failureMessage := ""
	if outcome.Failure != nil {
		failure := *outcome.Failure
		if failure.Type != sessiontree.EntryRunFailure || failure.ThreadID != threadID || failure.TurnID != turnID ||
			strings.TrimSpace(failure.Error) == "" || terminal.ParentID != failure.ID {
			return errors.New("turn failure entry is invalid")
		}
		failureMessage = strings.TrimSpace(failure.Error)
	}
	switch terminal.TurnStatus {
	case sessiontree.TurnCompleted, sessiontree.TurnWaiting:
		if outcome.Failure != nil || failureCode != "" {
			return errors.New("successful or waiting turn must not include a failure")
		}
	case sessiontree.TurnFailed:
		if failureMessage == "" || !sessiontree.ValidTurnFailureCode(failureCode) ||
			failureCode == sessiontree.TurnFailureCancelled || failureCode == sessiontree.TurnFailureInterrupted {
			return errors.New("failed turn requires a matching failure entry and code")
		}
	case sessiontree.TurnAborted:
		if failureMessage == "" || (failureCode != sessiontree.TurnFailureCancelled && failureCode != sessiontree.TurnFailureInterrupted) {
			return errors.New("aborted turn requires a cancelled or interrupted failure")
		}
	default:
		return fmt.Errorf("invalid terminal turn status %q", terminal.TurnStatus)
	}
	return nil
}

func turnFailureCode(status engine.Status, err error, origin engine.FailureOrigin) (string, error) {
	if err == nil {
		return "", nil
	}
	if errors.Is(err, sessiontree.ErrEffectOutcomeUnknown) {
		return sessiontree.TurnFailureEffectOutcomeUnknown, nil
	}
	if status == engine.Cancelled || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sessiontree.TurnFailureCancelled, nil
	}
	if errors.Is(err, ErrAuthorizationUnavailable) {
		return sessiontree.TurnFailureAuthorizationUnavailable, nil
	}
	if errors.Is(err, ErrAuthorizationContract) || errors.Is(err, ErrInvalidAuthorizationProof) || errors.Is(err, ErrEffectDispatchConsumed) {
		return sessiontree.TurnFailureAuthorizationContract, nil
	}
	var committed *CommittedEffectError
	if errors.As(err, &committed) {
		return sessiontree.TurnFailureToolDispatch, nil
	}
	switch origin {
	case engine.FailureOriginCancelled:
		return sessiontree.TurnFailureCancelled, nil
	case engine.FailureOriginProvider:
		return sessiontree.TurnFailureProvider, nil
	case engine.FailureOriginToolDispatch:
		return sessiontree.TurnFailureToolDispatch, nil
	case engine.FailureOriginStorage:
		return sessiontree.TurnFailureStorage, nil
	case engine.FailureOriginContract:
		return sessiontree.TurnFailureEngineContract, nil
	case engine.FailureOriginNone:
		return "", errors.New("failure origin is required")
	default:
		return "", fmt.Errorf("invalid failure origin %q", origin)
	}
}
