package runtime

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/internal/sessiontree"
)

const (
	threadTitleStatusPending = ThreadTitleStatus(sessiontree.ThreadTitlePending)
	threadTitleStatusReady   = ThreadTitleStatus(sessiontree.ThreadTitleReady)
	threadTitleStatusFailed  = ThreadTitleStatus(sessiontree.ThreadTitleFailed)

	threadTitleSourceHost     = ThreadTitleSource(sessiontree.ThreadTitleSourceHost)
	threadTitleSourceProvider = ThreadTitleSource(sessiontree.ThreadTitleSourceProvider)
)

// ThreadTitleStatus is the finite lifecycle state of a canonical thread title.
// The zero value means that no title generation or host title has been recorded.
type ThreadTitleStatus string

const (
	ThreadTitleStatusUnset   ThreadTitleStatus = ""
	ThreadTitleStatusPending ThreadTitleStatus = threadTitleStatusPending
	ThreadTitleStatusReady   ThreadTitleStatus = threadTitleStatusReady
	ThreadTitleStatusFailed  ThreadTitleStatus = threadTitleStatusFailed
)

// Valid reports whether the status is a supported public title state. The zero
// value is valid and represents a thread without title state.
func (s ThreadTitleStatus) Valid() bool {
	switch s {
	case ThreadTitleStatusUnset, ThreadTitleStatusPending, ThreadTitleStatusReady, ThreadTitleStatusFailed:
		return true
	default:
		return false
	}
}

// ParseThreadTitleStatus validates raw public title status text.
func ParseThreadTitleStatus(raw string) (ThreadTitleStatus, error) {
	status := ThreadTitleStatus(raw)
	if !status.Valid() {
		return "", fmt.Errorf("unsupported thread title status %q", raw)
	}
	return status, nil
}

// ThreadTitleSource identifies who committed a ready canonical thread title.
// The zero value is valid while a title is unset, pending, or failed.
type ThreadTitleSource string

const (
	ThreadTitleSourceUnset    ThreadTitleSource = ""
	ThreadTitleSourceHost     ThreadTitleSource = threadTitleSourceHost
	ThreadTitleSourceProvider ThreadTitleSource = threadTitleSourceProvider
)

// Valid reports whether the source is supported. The zero value represents no
// committed title source.
func (s ThreadTitleSource) Valid() bool {
	switch s {
	case ThreadTitleSourceUnset, ThreadTitleSourceHost, ThreadTitleSourceProvider:
		return true
	default:
		return false
	}
}

// ParseThreadTitleSource validates raw public title source text.
func ParseThreadTitleSource(raw string) (ThreadTitleSource, error) {
	source := ThreadTitleSource(raw)
	if !source.Valid() {
		return "", fmt.Errorf("unsupported thread title source %q", raw)
	}
	return source, nil
}

// Valid reports whether the phase is part of the public thread lifecycle.
func (p ThreadPhase) Valid() bool {
	return p == ThreadPhaseIdle || p == ThreadPhaseTurn
}

// Valid reports whether the status is part of the public thread lifecycle.
func (s ThreadStatus) Valid() bool {
	switch s {
	case ThreadStatusIdle, ThreadStatusRunning, ThreadStatusCompleted, ThreadStatusWaiting,
		ThreadStatusFailed, ThreadStatusCancelled, ThreadStatusInterrupted:
		return true
	default:
		return false
	}
}

// Validate checks the complete public thread snapshot contract.
func (s ThreadSnapshot) Validate() error {
	if err := validateThreadSnapshotState(
		s.ID, s.Title, s.TitleStatus, s.TitleSource, s.TitleUpdatedAt, s.TitleError,
		s.TitleGeneration, s.CreatedAt, s.UpdatedAt, s.Phase, s.Status,
		s.LatestTurnID, s.WaitingPrompt, s.Recoverable, s.CanAppendMessage,
	); err != nil {
		return err
	}
	if s.ThroughOrdinal < 0 {
		return errors.New("thread snapshot through ordinal must not be negative")
	}
	if (s.LatestTurnID == "") != (s.LatestRunID == "") {
		return errors.New("thread snapshot latest turn and run identities must appear together")
	}
	if s.LatestTurnID != "" && s.ThroughOrdinal <= 0 {
		return errors.New("thread snapshot latest execution requires positive through ordinal")
	}
	if s.LatestRunID != "" && string(s.LatestRunID) != strings.TrimSpace(string(s.LatestRunID)) {
		return errors.New("thread snapshot requires trim-stable latest run id")
	}
	return nil
}

// Validate checks the complete public transcript-free thread summary contract.
func (s ThreadSummary) Validate() error {
	return validateThreadSnapshotState(
		s.ID, s.Title, s.TitleStatus, s.TitleSource, s.TitleUpdatedAt, s.TitleError,
		s.TitleGeneration, s.CreatedAt, s.UpdatedAt, s.Phase, s.Status,
		s.LatestTurnID, s.WaitingPrompt, s.Recoverable, s.CanAppendMessage,
	)
}

func validateThreadSnapshotResult(snapshot ThreadSnapshot) (ThreadSnapshot, error) {
	if err := snapshot.Validate(); err != nil {
		return ThreadSnapshot{}, fmt.Errorf("%w: invalid public thread snapshot: %v", ErrAuthorityCorrupt, err)
	}
	return snapshot, nil
}

func validateThreadSummaryResult(summary ThreadSummary) (ThreadSummary, error) {
	if err := summary.Validate(); err != nil {
		return ThreadSummary{}, fmt.Errorf("%w: invalid public thread summary: %v", ErrAuthorityCorrupt, err)
	}
	return summary, nil
}

func validateThreadSnapshotState(
	threadID ThreadID,
	title string,
	titleStatusRaw string,
	titleSourceRaw string,
	titleUpdatedAt time.Time,
	titleError string,
	titleGeneration int64,
	createdAt time.Time,
	updatedAt time.Time,
	phase ThreadPhase,
	status ThreadStatus,
	latestTurnID TurnID,
	waitingPrompt string,
	recoverable bool,
	canAppendMessage bool,
) error {
	if strings.TrimSpace(string(threadID)) == "" || string(threadID) != strings.TrimSpace(string(threadID)) {
		return errors.New("thread snapshot requires trim-stable thread id")
	}
	if createdAt.IsZero() || updatedAt.IsZero() || updatedAt.Before(createdAt) {
		return errors.New("thread snapshot requires ordered creation and update times")
	}
	if latestTurnID != "" && string(latestTurnID) != strings.TrimSpace(string(latestTurnID)) {
		return errors.New("thread snapshot requires trim-stable latest turn id")
	}
	if strings.TrimSpace(waitingPrompt) != waitingPrompt {
		return errors.New("thread snapshot waiting prompt must be trim-stable")
	}
	if err := sessionlifecycle.ValidateProjection(
		string(status), string(phase), string(latestTurnID), waitingPrompt, recoverable, canAppendMessage,
	); err != nil {
		return err
	}
	return validateThreadTitleSnapshot(title, titleStatusRaw, titleSourceRaw, titleUpdatedAt, titleError, titleGeneration)
}

func validateThreadTitleSnapshot(title, statusRaw, sourceRaw string, updatedAt time.Time, titleError string, generation int64) error {
	status, err := ParseThreadTitleStatus(statusRaw)
	if err != nil {
		return err
	}
	source, err := ParseThreadTitleSource(sourceRaw)
	if err != nil {
		return err
	}
	return sessiontree.ValidateThreadTitleProjection(sessiontree.ThreadTitleProjection{
		Title: title, Status: sessiontree.ThreadTitleStatus(status), Source: sessiontree.ThreadTitleSource(source),
		UpdatedAt: updatedAt, Error: titleError, Generation: generation,
	})
}
