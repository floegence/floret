package runtime

import (
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v2/observation"
)

func (s SubAgentStatus) valid() bool {
	switch s {
	case SubAgentStatusIdle, SubAgentStatusRunning, SubAgentStatusWaiting,
		SubAgentStatusCompleted, SubAgentStatusFailed, SubAgentStatusCancelled,
		SubAgentStatusInterrupted, SubAgentStatusClosing, SubAgentStatusClosed:
		return true
	default:
		return false
	}
}

func (m SubAgentForkMode) valid() bool {
	return m == SubAgentForkNone || m == SubAgentForkFullPath
}

// Validate checks one self-contained public SubAgent projection.
func (s SubAgentSnapshot) Validate() error {
	if !trimStableNonEmpty(string(s.ThreadID)) || !trimStableNonEmpty(string(s.ParentThreadID)) || s.ThreadID == s.ParentThreadID {
		return errors.New("subagent snapshot requires distinct trim-stable thread and parent identities")
	}
	if !trimStableNonEmpty(s.Path) || !trimStableNonEmpty(s.TaskName) {
		return errors.New("subagent snapshot requires path and task name")
	}
	for name, value := range map[string]string{
		"parent turn id": string(s.ParentTurnID), "host profile ref": s.HostProfileRef,
		"latest turn id": string(s.LatestTurnID), "waiting prompt": s.WaitingPrompt,
	} {
		if value != strings.TrimSpace(value) {
			return fmt.Errorf("subagent snapshot %s must be trim-stable", name)
		}
	}
	if !s.ForkMode.valid() || !s.Status.valid() {
		return errors.New("subagent snapshot has an unsupported fork mode or status")
	}
	if s.QueuedInputs < 0 || s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return errors.New("subagent snapshot has invalid counters or timestamps")
	}
	if s.Closed != (s.Status == SubAgentStatusClosed) {
		return errors.New("subagent snapshot closed state is inconsistent")
	}
	if s.Closed && (s.CanSendInput || s.CanInterrupt || s.CanClose) {
		return errors.New("closed subagent snapshot exposes lifecycle actions")
	}
	return nil
}

// Validate checks one public SubAgent wait result.
func (r WaitSubAgentsResult) Validate() error {
	seen := make(map[ThreadID]struct{}, len(r.Snapshots))
	for index, snapshot := range r.Snapshots {
		if err := snapshot.Validate(); err != nil {
			return fmt.Errorf("subagent wait snapshot %d: %w", index, err)
		}
		if _, duplicate := seen[snapshot.ThreadID]; duplicate {
			return fmt.Errorf("subagent wait result repeats thread %q", snapshot.ThreadID)
		}
		seen[snapshot.ThreadID] = struct{}{}
	}
	return nil
}

// Validate checks one canonical Agent todo projection.
func (s ThreadAgentTodoState) Validate() error {
	if !trimStableNonEmpty(string(s.ThreadID)) || s.Version < 0 {
		return errors.New("agent todo state requires a thread identity and non-negative version")
	}
	metadataPresent := !s.UpdatedAt.IsZero() || s.UpdatedByTurnID != "" || s.UpdatedByRunID != "" || s.UpdatedByToolCall != ""
	if metadataPresent && (s.UpdatedAt.IsZero() || !trimStableNonEmpty(string(s.UpdatedByTurnID)) ||
		!trimStableNonEmpty(string(s.UpdatedByRunID)) || !trimStableNonEmpty(s.UpdatedByToolCall)) {
		return errors.New("agent todo state update metadata is incomplete")
	}
	if !metadataPresent && s.Version != 0 {
		return errors.New("versioned agent todo state requires update metadata")
	}
	seen := make(map[string]struct{}, len(s.Items))
	for index, item := range s.Items {
		if !trimStableNonEmpty(item.ID) || !trimStableNonEmpty(item.Content) || !item.Status.Valid() {
			return fmt.Errorf("agent todo item %d is invalid", index)
		}
		if _, duplicate := seen[item.ID]; duplicate {
			return fmt.Errorf("agent todo state repeats item %q", item.ID)
		}
		seen[item.ID] = struct{}{}
	}
	return nil
}

// Validate checks one public detail-event page.
func (p ThreadDetailEvents) Validate() error {
	if p.NextOrdinal < 0 || p.RetainedFrom < 0 || p.GeneratedAt.IsZero() || p.GeneratedAt != p.GeneratedAt.UTC() {
		return errors.New("thread detail page boundary or generation time is invalid")
	}
	var threadID ThreadID
	var previous int64
	seen := make(map[string]struct{}, len(p.Events))
	for index, event := range p.Events {
		if err := validateThreadDetailEvent(event); err != nil {
			return fmt.Errorf("thread detail event %d: %w", index, err)
		}
		if index > 0 && event.Ordinal <= previous {
			return errors.New("thread detail events are not strictly ordered")
		}
		if threadID == "" {
			threadID = event.ThreadID
		} else if event.ThreadID != threadID {
			return errors.New("thread detail page mixes thread identities")
		}
		if _, duplicate := seen[event.ID]; duplicate {
			return fmt.Errorf("thread detail page repeats event %q", event.ID)
		}
		seen[event.ID] = struct{}{}
		previous = event.Ordinal
	}
	if len(p.Events) > 0 && p.NextOrdinal < previous {
		return errors.New("thread detail page next ordinal regresses")
	}
	if p.HasMore && len(p.Events) == 0 {
		return errors.New("thread detail page cannot continue without events")
	}
	return nil
}

func validateThreadDetailEvent(event ThreadDetailEvent) error {
	if !trimStableNonEmpty(event.ID) || event.Ordinal <= 0 || !trimStableNonEmpty(string(event.ThreadID)) || event.CreatedAt.IsZero() {
		return errors.New("detail event identity, ordinal, or timestamp is invalid")
	}
	if event.ParentID != strings.TrimSpace(event.ParentID) || string(event.TurnID) != strings.TrimSpace(string(event.TurnID)) ||
		string(event.RunID) != strings.TrimSpace(string(event.RunID)) {
		return errors.New("detail event identities must be trim-stable")
	}
	switch event.Kind {
	case ThreadDetailEventUserMessage, ThreadDetailEventAssistantMessage, ThreadDetailEventToolCall,
		ThreadDetailEventToolDispatch, ThreadDetailEventToolActivity, ThreadDetailEventToolResult,
		ThreadDetailEventTurnMarker, ThreadDetailEventCompaction, ThreadDetailEventError,
		ThreadDetailEventApproval, ThreadDetailEventInput, ThreadDetailEventCustom:
		return nil
	default:
		return fmt.Errorf("unsupported detail event kind %q", event.Kind)
	}
}

// Validate checks one public SubAgent activity projection.
func (r SubAgentActivityTimelineResult) Validate() error {
	if r.GeneratedAt.IsZero() || r.GeneratedAt != r.GeneratedAt.UTC() {
		return errors.New("subagent activity result requires a UTC generation time")
	}
	if err := observation.ValidateActivityTimeline(r.Timeline); err != nil {
		return fmt.Errorf("subagent activity timeline: %w", err)
	}
	return nil
}

// Validate checks one public SubAgent detail page.
func (d SubAgentDetail) Validate() error {
	if err := d.Snapshot.Validate(); err != nil {
		return fmt.Errorf("subagent detail snapshot: %w", err)
	}
	page := ThreadDetailEvents{Events: d.Events, NextOrdinal: d.NextOrdinal, HasMore: d.HasMore, RetainedFrom: d.RetainedFrom, GeneratedAt: d.GeneratedAt}
	if err := page.Validate(); err != nil {
		return fmt.Errorf("subagent detail events: %w", err)
	}
	for _, event := range d.Events {
		if event.ThreadID != d.Snapshot.ThreadID {
			return errors.New("subagent detail event identity mismatch")
		}
	}
	if err := observation.ValidateActivityTimeline(d.ActivityTimeline); err != nil {
		return fmt.Errorf("subagent detail activity timeline: %w", err)
	}
	if d.ActivityTimeline.ThreadID != string(d.Snapshot.ThreadID) {
		return errors.New("subagent detail activity identity mismatch")
	}
	if err := d.Context.Validate(); err != nil {
		return fmt.Errorf("subagent detail context: %w", err)
	}
	if d.Context.ThreadID != d.Snapshot.ThreadID {
		return errors.New("subagent detail context identity mismatch")
	}
	return nil
}

func trimStableNonEmpty(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}

func invalidPublicResult(name string, err error) error {
	return &ContractError{Contract: strings.TrimSpace(name), Err: err}
}

func validateSubAgentSnapshotResult(snapshot SubAgentSnapshot) (SubAgentSnapshot, error) {
	if err := snapshot.Validate(); err != nil {
		return SubAgentSnapshot{}, invalidPublicResult("subagent snapshot", err)
	}
	return snapshot, nil
}

func validateSubAgentSnapshotsResult(snapshots []SubAgentSnapshot) ([]SubAgentSnapshot, error) {
	for index, snapshot := range snapshots {
		if err := snapshot.Validate(); err != nil {
			return nil, invalidPublicResult(fmt.Sprintf("subagent snapshot %d", index), err)
		}
	}
	return snapshots, nil
}
