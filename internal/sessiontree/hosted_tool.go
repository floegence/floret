package sessiontree

import (
	"errors"
	"strconv"
	"strings"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/tools"
)

const HostedToolEntryKind = "hosted_tool"

// NewHostedToolEntry records provider-owned activity without adding a local
// tool exchange to model context. The containing entry owns execution identity.
func NewHostedToolEntry(ev observation.Event) (Entry, error) {
	entry := Entry{
		ThreadID: ev.ThreadID.String(), TurnID: ev.TurnID.String(), RunID: ev.RunID.String(),
		Type: EntryCustom, CreatedAt: ev.ObservedAt,
		Message:  session.Message{ToolCallID: ev.ToolID, ToolName: ev.ToolName, Activity: tools.CloneActivityPresentation(ev.Activity)},
		Metadata: map[string]string{"kind": HostedToolEntryKind, "type": string(ev.Type)},
	}
	if ev.Error != "" || ev.Metadata["error_present"] == true {
		entry.Metadata["error_present"] = "true"
	}
	if count, ok := ev.Metadata["result_count"].(int); ok && count > 0 {
		entry.Metadata["result_count"] = strconv.Itoa(count)
	}
	_, err := HostedToolObservation(entry)
	return entry, err
}

// HostedToolObservation decodes only canonical hosted activity. Raw arguments,
// result bodies and provider continuation receipts are never stored here.
func HostedToolObservation(entry Entry) (observation.Event, error) {
	typ := observation.EventType(entry.Metadata["type"])
	if entry.Type != EntryCustom || entry.Metadata["kind"] != HostedToolEntryKind ||
		(typ != observation.EventTypeHostedToolCall && typ != observation.EventTypeHostedToolResult) ||
		strings.TrimSpace(entry.ThreadID) == "" || strings.TrimSpace(entry.TurnID) == "" || strings.TrimSpace(entry.RunID) == "" ||
		strings.TrimSpace(entry.Message.ToolCallID) == "" || strings.TrimSpace(entry.Message.ToolName) == "" || entry.Message.Role != "" {
		return observation.Event{}, errors.New("invalid canonical hosted tool activity")
	}
	if entry.Message.Activity != nil {
		if err := entry.Message.Activity.Validate(); err != nil {
			return observation.Event{}, err
		}
	}
	metadata := map[string]any{}
	if value := entry.Metadata["error_present"]; value != "" {
		if value != "true" || typ != observation.EventTypeHostedToolResult {
			return observation.Event{}, errors.New("invalid hosted tool error state")
		}
		metadata["error_present"] = true
	}
	if value := entry.Metadata["result_count"]; value != "" {
		count, err := strconv.Atoi(value)
		if err != nil || count <= 0 {
			return observation.Event{}, errors.New("invalid hosted tool result count")
		}
		metadata["result_count"] = count
	}
	return observation.Event{
		Type: typ, ThreadID: identity.ThreadID(entry.ThreadID), TurnID: identity.TurnID(entry.TurnID), RunID: identity.RunID(entry.RunID),
		ToolID: entry.Message.ToolCallID, ToolName: entry.Message.ToolName, ToolKind: "hosted", ObservedAt: entry.CreatedAt,
		Activity: tools.CloneActivityPresentation(entry.Message.Activity), Metadata: metadata,
	}, nil
}
