package testui

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
)

func publicRunResponse(resp RunResponse) RunResponse {
	resp.Summary = event.Redact(resp.Summary)
	resp.Output = event.Redact(resp.Output)
	resp.Error = event.Redact(resp.Error)
	if resp.Agent != nil {
		agent := publicAgentRun(*resp.Agent)
		resp.Agent = &agent
	}
	for i := range resp.Parts {
		resp.Parts[i] = publicRunResponse(resp.Parts[i])
	}
	return resp
}

func publicAgentRun(run AgentRun) AgentRun {
	run.Output = event.Redact(run.Output)
	run.Events = publicEvents(run.Events, false)
	if run.Artifacts != nil {
		out := make(map[string]ArtifactSnapshot, len(run.Artifacts))
		for key, artifact := range run.Artifacts {
			artifact.Path = event.SafePathLabel(artifact.Path)
			if key == "trace" {
				artifact.Content = sanitizeTraceArtifact(artifact.Content)
			} else if key == "oracle_log" {
				artifact.Content = event.Redact(artifact.Content)
			}
			out[key] = artifact
		}
		run.Artifacts = out
	}
	return run
}

func publicAgentRunResponse(resp AgentRunResponse, debugRaw bool) AgentRunResponse {
	resp.Profile = stripProfileSecret(resp.Profile)
	if !debugRaw {
		resp.Summary = event.Redact(resp.Summary)
		resp.Output = event.Redact(resp.Output)
		resp.Error = event.Redact(resp.Error)
		resp.WaitingPrompt = event.Redact(resp.WaitingPrompt)
		resp.Diagnostics = publicStringMap(resp.Diagnostics)
	}
	resp.Events = publicEvents(resp.Events, debugRaw)
	resp.HarnessEvents = publicHarnessEvents(resp.HarnessEvents, debugRaw)
	resp.Session = publicAgentSessionSnapshot(resp.Session, debugRaw)
	resp.Observation = publicAgentObservation(resp.Observation, debugRaw)
	return resp
}

func sanitizeTraceArtifact(content string) string {
	if content == "" {
		return ""
	}
	var out bytes.Buffer
	writer := event.NewTraceWriter(&out)
	dec := json.NewDecoder(bytes.NewReader([]byte(content)))
	for {
		var ev event.Event
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return event.Redact(content)
		}
		writer.Emit(ev)
	}
	return out.String()
}

func publicAgentSessionSnapshot(snapshot AgentSessionSnapshot, debugRaw bool) AgentSessionSnapshot {
	snapshot.Profile = stripProfileSecret(snapshot.Profile)
	if debugRaw {
		return snapshot
	}
	snapshot.SystemPrompt = ""
	for i := range snapshot.HostedTools {
		snapshot.HostedTools[i].Parameters = nil
		snapshot.HostedTools[i].Options = nil
	}
	snapshot.WaitingPrompt = event.Redact(snapshot.WaitingPrompt)
	for i := range snapshot.Turns {
		snapshot.Turns[i].Output = event.Redact(snapshot.Turns[i].Output)
		snapshot.Turns[i].Error = event.Redact(snapshot.Turns[i].Error)
	}
	snapshot.ActiveContext = publicObservedMessages(snapshot.ActiveContext)
	snapshot.PathEntries = publicObservedEntries(snapshot.PathEntries)
	snapshot.AllEntries = publicObservedEntries(snapshot.AllEntries)
	snapshot.Observation = publicAgentObservation(snapshot.Observation, false)
	return snapshot
}

func publicAgentObservation(observation AgentObservation, debugRaw bool) AgentObservation {
	if debugRaw {
		return observation
	}
	for i := range observation.ProviderRequests {
		observation.ProviderRequests[i] = publicObservedProviderRequest(observation.ProviderRequests[i])
	}
	for i := range observation.ProviderEvents {
		observation.ProviderEvents[i] = publicObservedProviderEvent(observation.ProviderEvents[i])
	}
	observation.SessionMessages = publicObservedMessages(observation.SessionMessages)
	observation.ActiveContext = publicObservedMessages(observation.ActiveContext)
	observation.PathEntries = publicObservedEntries(observation.PathEntries)
	for i := range observation.Transitions {
		observation.Transitions[i].Details = event.Redact(observation.Transitions[i].Details)
	}
	return observation
}

func publicObservedProviderRequest(req ObservedProviderRequest) ObservedProviderRequest {
	req.Messages = publicObservedMessages(req.Messages)
	for i := range req.RawSegments {
		req.RawSegments[i].Raw = ""
		req.RawSegments[i].RawPreview = ""
	}
	for i := range req.Tools {
		req.Tools[i] = publicToolDefinition(req.Tools[i])
	}
	for i := range req.HostedTools {
		req.HostedTools[i].Parameters = nil
		req.HostedTools[i].Options = nil
	}
	return req
}

func publicToolDefinition(def provider.ToolDefinition) provider.ToolDefinition {
	schemaHash := stableHashAny(map[string]any{
		"input_schema":  def.InputSchema,
		"output_schema": def.OutputSchema,
	})
	def.Annotations = publicToolAnnotations(def.Annotations)
	def.Annotations["schema_hash"] = schemaHash
	return def
}

func publicToolAnnotations(in map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		switch key {
		case "source", "effects", "read_only", "destructive", "open_world", "parallel_safe", "permission_mode", "mcp_server", "mcp_tool", "transport", "read_path":
			out[key] = value
		}
	}
	return out
}

func publicObservedProviderEvent(ev ObservedProviderEvent) ObservedProviderEvent {
	ev.Text = ""
	ev.Reasoning = ""
	ev.ToolCalls = publicProviderToolCalls(ev.ToolCalls)
	ev.Reason = event.Redact(ev.Reason)
	return ev
}

func publicProviderToolCalls(calls []provider.ToolCall) []provider.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	out := make([]provider.ToolCall, len(calls))
	for i, call := range calls {
		out[i] = provider.ToolCall{ID: call.ID, Name: call.Name}
		if call.Args != "" {
			out[i].Args = "[redacted]"
		}
	}
	return out
}

func publicObservedEntries(entries []ObservedSessionEntry) []ObservedSessionEntry {
	out := append([]ObservedSessionEntry(nil), entries...)
	for i := range out {
		out[i].Message = publicObservedMessage(out[i].Message)
		if out[i].Type == sessiontree.EntryRunFailure {
			out[i].Error = event.Redact(out[i].Error)
		}
		if out[i].Type == sessiontree.EntryCompaction {
			out[i].Summary = ""
		}
		out[i].Metadata = publicMetadata(out[i].Metadata)
	}
	return out
}

func publicObservedMessages(messages []ObservedSessionMessage) []ObservedSessionMessage {
	out := append([]ObservedSessionMessage(nil), messages...)
	for i := range out {
		out[i] = publicObservedMessage(out[i])
	}
	return out
}

func publicObservedMessage(msg ObservedSessionMessage) ObservedSessionMessage {
	msg.Reasoning = ""
	msg.ToolArgs = ""
	if msg.Role == string(session.Tool) {
		msg.Content = ""
	}
	if msg.Role == string(session.Assistant) && msg.ToolName != "" {
		msg.Content = "tool_call"
	}
	if msg.Kind == string(session.MessageKindCompactionSummary) {
		msg.Content = ""
	}
	msg.Content = event.Redact(msg.Content)
	return msg
}

func publicMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		if publicMetadataValueIsSafe(key, value) {
			out[key] = value
		} else {
			out[key] = publicMetadataRedactedLabel(value)
		}
	}
	return out
}

func publicMetadataRedactedLabel(value string) string {
	if value == "" {
		return ""
	}
	return "[redacted]#" + event.StableHash(event.Redact(value))[:12]
}

func publicMetadataValueIsSafe(key, value string) bool {
	switch key {
	case "source", "previous_tools", "selected_tools", "status", "phase", "completion_reason", "continuation_reason", "finish_reason", "raw_finish_reason", "turn_status", "entry_type":
		return value == "" || isSafeTokenList(value)
	case "finish_inferred":
		return value == "true" || value == "false" || value == ""
	case "batch_index", "batch_size", "exit_code", "matches", "bytes", "line_start", "line_end", "replacements", "count", "result_count", "compaction_generation":
		return value == "" || isSafeDecimal(value)
	default:
		return false
	}
}

func isSafeTokenList(value string) bool {
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' || r == ',' || r == ':' {
			continue
		}
		return false
	}
	return true
}

func isSafeDecimal(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func publicEvents(events []event.Event, debugRaw bool) []event.Event {
	out := make([]event.Event, len(events))
	for i, ev := range events {
		if debugRaw {
			out[i] = ev
		} else {
			out[i] = event.Sanitize(ev)
		}
	}
	return out
}

func publicHarnessEvents(events []agentharness.HarnessEvent, debugRaw bool) []agentharness.HarnessEvent {
	if debugRaw {
		return events
	}
	out := append([]agentharness.HarnessEvent(nil), events...)
	for i := range out {
		out[i].Message = event.Redact(out[i].Message)
		out[i].Status = event.Redact(out[i].Status)
		out[i].Metadata = publicMetadata(out[i].Metadata)
	}
	return out
}

func publicStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = event.Redact(value)
	}
	return out
}

func publicAgentStreamEvent(ev AgentStreamEvent, debugRaw bool) AgentStreamEvent {
	if debugRaw {
		return ev
	}
	if ev.Entry != nil {
		entry := publicObservedEntries([]ObservedSessionEntry{*ev.Entry})[0]
		ev.Entry = &entry
	}
	if ev.ProviderRequest != nil {
		req := publicObservedProviderRequest(*ev.ProviderRequest)
		ev.ProviderRequest = &req
	}
	if ev.ProviderEvent != nil {
		providerEvent := publicObservedProviderEvent(*ev.ProviderEvent)
		ev.ProviderEvent = &providerEvent
	}
	if ev.EngineEvent != nil {
		engineEvent := event.Sanitize(*ev.EngineEvent)
		ev.EngineEvent = &engineEvent
	}
	if ev.Snapshot != nil {
		snapshot := publicAgentSessionSnapshot(*ev.Snapshot, false)
		ev.Snapshot = &snapshot
	}
	if ev.Result != nil {
		result := publicAgentRunResponse(*ev.Result, false)
		ev.Result = &result
	}
	switch ev.Type {
	case AgentStreamProviderDelta, AgentStreamToolResult:
		ev.Message = ""
	case AgentStreamToolCall:
		if ev.EngineEvent == nil {
			ev.Message = ""
		}
	}
	ev.Message = event.Redact(ev.Message)
	ev.Error = event.Redact(ev.Error)
	ev.Metadata = publicMetadata(ev.Metadata)
	return ev
}

func stableHashAny(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		data = []byte("{}")
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
