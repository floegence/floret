package sessiontree

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ProjectionSourceReader locates canonical presentation dependencies for exact
// source entries on one frozen path. It does not project provider context or
// certify an item window: membership and public ordinals have a separate owner.
type ProjectionSourceReader interface {
	ProjectionSources(context.Context, string, string, []string) (ProjectionSourceSet, error)
}

// ProjectionSourceScanner resolves and materializes one dependency closure in
// a single read transaction. The field selector runs on detached metadata and
// must not call repository methods. The result remains explicitly partial.
type ProjectionSourceScanner interface {
	ReadProjectionSources(context.Context, string, string, []string, func(PathMetadata, bool) []string) (PathProjectionPage, error)
}

type ProjectionSourceSet struct {
	LeafID  string
	Sources []PathMetadata
}

// projectionSourceGroup identifies canonical records that must be available
// together to interpret an item. It does not classify presentation, retain a
// body, or decide lifecycle state; the canonical reducers still own those rules.
type projectionSourceGroup struct {
	Kind string `json:"kind"`
	Turn string `json:"turn,omitempty"`
	Run  string `json:"run,omitempty"`
	ID   string `json:"id"`
}

func projectionSourceGroups(entry Entry) ([]projectionSourceGroup, error) {
	var groups []projectionSourceGroup
	add := func(kind, turn, run, id string) {
		if id == "" {
			return
		}
		group := projectionSourceGroup{kind, turn, run, id}
		for _, existing := range groups {
			if existing == group {
				return
			}
		}
		groups = append(groups, group)
	}
	for _, source := range entrySourceIDs(entry) {
		add("output", "", "", source)
	}
	if windowTurnSource(entry) {
		add("window_turn", entry.TurnID, "", entry.TurnID)
	}
	switch entry.Type {
	case EntryToolCall, EntryToolResult:
		add("tool", entry.TurnID, entry.RunID, entry.Message.ToolCallID)
		add("invocation", entry.TurnID, "", entry.Message.ToolCallID)
		if entry.Message.ToolName == "goal" {
			add("goal", "", "", "history")
		}
		if entry.Message.ControlSignal != nil {
			add("interaction", entry.TurnID, entry.RunID, entry.Message.ControlSignal.CallID)
			add("parent_interaction", entry.TurnID, "", strings.TrimSpace(entry.Message.ControlSignal.CallID))
		}
		if entry.Type == EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
			add("interaction", entry.TurnID, entry.RunID, ToolInputInteractionID(entry.ID))
			add("parent_interaction", entry.TurnID, "", ToolInputInteractionID(entry.ID))
			add("input_batch", entry.TurnID, entry.RunID, entry.RunID)
		}
	case EntryCustom:
		switch entry.Metadata["kind"] {
		case HostedToolEntryKind:
			add("hosted", entry.TurnID, entry.RunID, entry.Message.ToolCallID)
		case "tool_dispatch", "tool_activity", "subagent_approval", "pending_tool_settlement":
			id := entry.Message.ToolCallID
			if id == "" {
				id = entry.Metadata["tool_id"]
			}
			add("tool", entry.TurnID, entry.RunID, id)
			add("invocation", entry.TurnID, "", id)
		}
	case EntryTurnMarker, EntryCancelRequested, EntryRunFailure:
		if !windowNeutralSavepoint(entry) {
			add("turn_boundary", entry.TurnID, "", entry.TurnID)
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnWaiting {
			add("input_batch", entry.TurnID, entry.RunID, entry.RunID)
			add("input_ready", entry.TurnID, entry.RunID, entry.RunID)
		}
		if entry.Metadata[GoalCompletedMetadataKey] != "" {
			add("goal", "", "", "history")
		}
	case EntryGoal:
		add("goal", "", "", "history")
		var change GoalChange
		if json.Unmarshal(entry.Payload, &change) != nil {
			return nil, ErrAuthorityCorrupt
		}
		add("goal_version", "", "", fmt.Sprint(change.State.Version))
		add("goal_mutation", entry.TurnID, entry.RunID, entry.RequestKey)
	case EntryGoalReview:
		add("goal", "", "", "history")
		var review GoalReview
		if json.Unmarshal(entry.Payload, &review) != nil {
			return nil, ErrAuthorityCorrupt
		}
		add("goal_review", entry.TurnID, entry.RunID, goalReviewSourceIdentity(review.Version, review.Outcome, review.Evidence))
	case EntryUserMessage:
		if entry.Metadata[SubAgentUserMessageOriginMetadataKey] == SubAgentUserMessageOriginDelegatedMission {
			add("goal", "", "", "history")
		}
	case EntrySteeringRequested:
		add("steering", entry.TurnID, "", entry.ID)
	case EntrySteeringApplied, EntrySteeringCancelled:
		resolution, err := DecodeSteeringResolution(entry)
		if err != nil {
			return nil, err
		}
		add("steering", entry.TurnID, "", resolution.SteeringID)
	case EntryInteractionAsked:
		add("parent_interaction", entry.TurnID, "", strings.TrimPrefix(entry.ID, "interaction-requested:"))
		var interaction struct {
			ID         string `json:"id"`
			ToolCallID string `json:"tool_call_id"`
		}
		if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
			return nil, ErrAuthorityCorrupt
		}
		add("interaction", entry.TurnID, entry.RunID, interaction.ID)
		add("tool", entry.TurnID, entry.RunID, interaction.ToolCallID)
	case EntryInteractionDone:
		id := strings.TrimPrefix(entry.ID, "interaction-resolved:")
		add("parent_interaction", entry.TurnID, "", id)
		if id != entry.ID {
			add("interaction", entry.TurnID, entry.RunID, id)
			add("tool", entry.TurnID, entry.RunID, id)
		}
	}
	return groups, nil
}

func projectionCandidate(entry Entry) bool {
	switch entry.Type {
	case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult, EntryGoal, EntrySteeringRequested, EntryInteractionAsked:
		return true
	case EntryTurnMarker:
		return entry.Metadata[GoalCompletedMetadataKey] != ""
	case EntryCustom:
		return entry.Metadata["kind"] == HostedToolEntryKind
	}
	return false
}
