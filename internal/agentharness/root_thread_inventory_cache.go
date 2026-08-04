package agentharness

import (
	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/observation"
	"github.com/floegence/floret/v3/tools"
)

type rootThreadInventoryProjectionCacheEntry struct {
	revision    sessiontree.ThreadRevision
	fingerprint [32]byte
	phase       string
	overview    ThreadOverview
}

func (h *AgentHarness) rootThreadInventoryProjection(
	threadID string,
	revision sessiontree.ThreadRevision,
	fingerprint [32]byte,
	phase string,
) (ThreadOverview, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.rootThreadInventoryProjectionCache[threadID]
	if !ok || entry.revision != revision || entry.fingerprint != fingerprint || entry.phase != phase {
		return ThreadOverview{}, false
	}
	return cloneThreadOverview(entry.overview), true
}

func (h *AgentHarness) rememberRootThreadInventoryProjection(
	threadID string,
	revision sessiontree.ThreadRevision,
	fingerprint [32]byte,
	phase string,
	overview ThreadOverview,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rootThreadInventoryProjectionCache == nil {
		h.rootThreadInventoryProjectionCache = make(map[string]rootThreadInventoryProjectionCacheEntry)
	}
	h.rootThreadInventoryProjectionCache[threadID] = rootThreadInventoryProjectionCacheEntry{
		revision: revision, fingerprint: fingerprint, phase: phase, overview: cloneThreadOverview(overview),
	}
}

func cloneThreadOverview(in ThreadOverview) ThreadOverview {
	out := in
	out.Thread.Messages = make([]ThreadMessage, len(in.Thread.Messages))
	for index, message := range in.Thread.Messages {
		out.Thread.Messages[index] = cloneThreadMessage(message)
	}
	out.LatestTurn = cloneThreadDetailEvents(in.LatestTurn)
	return out
}

func cloneThreadMessage(in ThreadMessage) ThreadMessage {
	out := in
	out.Attachments = session.CloneMessageAttachments(in.Attachments)
	out.References = append([]session.MessageReference(nil), in.References...)
	return out
}

func cloneThreadDetailEvents(in ThreadDetailEvents) ThreadDetailEvents {
	out := in
	out.Events = make([]SubAgentDetailEvent, len(in.Events))
	for index, event := range in.Events {
		out.Events[index] = cloneSubAgentDetailEvent(event)
	}
	return out
}

func cloneSubAgentDetailEvent(in SubAgentDetailEvent) SubAgentDetailEvent {
	out := in
	out.Metadata = cloneStringMap(in.Metadata)
	if in.Message != nil {
		message := *in.Message
		message.Attachments = session.CloneMessageAttachments(in.Message.Attachments)
		message.References = append([]session.MessageReference(nil), in.Message.References...)
		message.Activity = tools.CloneActivityPresentation(in.Message.Activity)
		out.Message = &message
	}
	if in.ToolCall != nil {
		call := *in.ToolCall
		if in.ToolCall.ControlSignal != nil {
			signal := *in.ToolCall.ControlSignal
			signal.Payload = cloneAnyMap(in.ToolCall.ControlSignal.Payload)
			call.ControlSignal = &signal
		}
		out.ToolCall = &call
	}
	if in.ToolResult != nil {
		result := *in.ToolResult
		if in.ToolResult.FullOutput != nil {
			fullOutput := *in.ToolResult.FullOutput
			result.FullOutput = &fullOutput
		}
		out.ToolResult = &result
	}
	if in.Approval != nil {
		approval := *in.Approval
		approval.Metadata = cloneStringMap(in.Approval.Metadata)
		out.Approval = &approval
	}
	if in.TurnMarker != nil {
		marker := *in.TurnMarker
		marker.Metadata = cloneStringMap(in.TurnMarker.Metadata)
		out.TurnMarker = &marker
	}
	if in.Compaction != nil {
		compaction := *in.Compaction
		compaction.KeptUserEntryIDs = append([]string(nil), in.Compaction.KeptUserEntryIDs...)
		compaction.Metadata = cloneStringMap(in.Compaction.Metadata)
		out.Compaction = &compaction
	}
	out.ActivityTimeline = observation.CloneActivityTimeline(in.ActivityTimeline)
	return out
}
