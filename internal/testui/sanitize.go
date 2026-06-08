package testui

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"

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
	if debugRaw {
		resp.Summary = event.SafePathRefsText(resp.Summary)
		resp.Output = event.SafePathRefsText(resp.Output)
		resp.Error = event.SafePathRefsText(resp.Error)
		resp.WaitingPrompt = event.SafePathRefsText(resp.WaitingPrompt)
		resp.Diagnostics = pathSafeMetadata(resp.Diagnostics)
	} else {
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
		return pathSafeAgentSessionSnapshot(snapshot)
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
	snapshot.ContextProjection = publicObservedContextProjection(snapshot.ContextProjection)
	snapshot.PathEntries = publicObservedEntries(snapshot.PathEntries)
	snapshot.AllEntries = publicObservedEntries(snapshot.AllEntries)
	snapshot.Observation = publicAgentObservation(snapshot.Observation, false)
	return snapshot
}

func publicAgentObservation(observation AgentObservation, debugRaw bool) AgentObservation {
	if debugRaw {
		return pathSafeAgentObservation(observation)
	}
	for i := range observation.ProviderRequests {
		observation.ProviderRequests[i] = publicObservedProviderRequest(observation.ProviderRequests[i])
	}
	for i := range observation.ProviderEvents {
		observation.ProviderEvents[i] = publicObservedProviderEvent(observation.ProviderEvents[i])
	}
	observation.SessionMessages = publicObservedMessages(observation.SessionMessages)
	observation.ActiveContext = publicObservedMessages(observation.ActiveContext)
	observation.ContextProjection = publicObservedContextProjection(observation.ContextProjection)
	observation.PathEntries = publicObservedEntries(observation.PathEntries)
	for i := range observation.Transitions {
		observation.Transitions[i].Details = event.Redact(observation.Transitions[i].Details)
	}
	return observation
}

func pathSafeAgentSessionSnapshot(snapshot AgentSessionSnapshot) AgentSessionSnapshot {
	snapshot.SystemPrompt = event.SafePathRefsText(snapshot.SystemPrompt)
	snapshot.WaitingPrompt = event.SafePathRefsText(snapshot.WaitingPrompt)
	for i := range snapshot.HostedTools {
		snapshot.HostedTools[i].Parameters = pathSafeAnyMap(snapshot.HostedTools[i].Parameters)
		snapshot.HostedTools[i].Options = pathSafeAnyMap(snapshot.HostedTools[i].Options)
	}
	snapshot.Capabilities = pathSafeCapabilityState(snapshot.Capabilities)
	for i := range snapshot.Turns {
		snapshot.Turns[i].Output = event.SafePathRefsText(snapshot.Turns[i].Output)
		snapshot.Turns[i].Error = event.SafePathRefsText(snapshot.Turns[i].Error)
	}
	snapshot.ActiveContext = pathSafeObservedMessages(snapshot.ActiveContext)
	snapshot.ContextProjection = pathSafeObservedContextProjection(snapshot.ContextProjection)
	for i := range snapshot.PathEntries {
		snapshot.PathEntries[i] = pathSafeObservedEntry(snapshot.PathEntries[i])
	}
	for i := range snapshot.AllEntries {
		snapshot.AllEntries[i] = pathSafeObservedEntry(snapshot.AllEntries[i])
	}
	snapshot.Observation = pathSafeAgentObservation(snapshot.Observation)
	return snapshot
}

func pathSafeAgentObservation(observation AgentObservation) AgentObservation {
	for i := range observation.ProviderRequests {
		observation.ProviderRequests[i] = pathSafeObservedProviderRequest(observation.ProviderRequests[i])
	}
	for i := range observation.ProviderEvents {
		observation.ProviderEvents[i] = pathSafeObservedProviderEvent(observation.ProviderEvents[i])
	}
	observation.SessionMessages = pathSafeObservedMessages(observation.SessionMessages)
	observation.ActiveContext = pathSafeObservedMessages(observation.ActiveContext)
	observation.ContextProjection = pathSafeObservedContextProjection(observation.ContextProjection)
	for i := range observation.PathEntries {
		observation.PathEntries[i] = pathSafeObservedEntry(observation.PathEntries[i])
	}
	for i := range observation.Transitions {
		observation.Transitions[i].Details = event.SafePathRefsText(observation.Transitions[i].Details)
	}
	observation.Diagnostics = pathSafeMetadata(observation.Diagnostics)
	return observation
}

func pathSafeObservedProviderRequest(req ObservedProviderRequest) ObservedProviderRequest {
	req.Messages = pathSafeObservedMessages(req.Messages)
	for i := range req.RawSegments {
		req.RawSegments[i].Raw = event.SafePathRefsText(req.RawSegments[i].Raw)
		req.RawSegments[i].RawPreview = event.SafePathRefsText(req.RawSegments[i].RawPreview)
	}
	for i := range req.Tools {
		req.Tools[i].Annotations = pathSafeAnyMap(req.Tools[i].Annotations)
	}
	for i := range req.HostedTools {
		req.HostedTools[i].Parameters = pathSafeAnyMap(req.HostedTools[i].Parameters)
		req.HostedTools[i].Options = pathSafeAnyMap(req.HostedTools[i].Options)
	}
	return req
}

func pathSafeObservedProviderEvent(ev ObservedProviderEvent) ObservedProviderEvent {
	ev.Text = event.SafePathRefsText(ev.Text)
	ev.Reasoning = event.SafePathRefsText(ev.Reasoning)
	for i := range ev.ToolCalls {
		ev.ToolCalls[i].Args = event.SafePathRefsText(ev.ToolCalls[i].Args)
		ev.ToolCalls[i].Reasoning = event.SafePathRefsText(ev.ToolCalls[i].Reasoning)
	}
	if ev.HostedResult != nil {
		result := pathSafeHostedToolResult(*ev.HostedResult)
		ev.HostedResult = &result
	}
	ev.Metadata = pathSafeMetadata(ev.Metadata)
	ev.Reason = event.SafePathRefsText(ev.Reason)
	return ev
}

func pathSafeHostedToolResult(result provider.HostedToolResultData) provider.HostedToolResultData {
	result.Text = event.SafePathRefsText(result.Text)
	for i := range result.Results {
		result.Results[i].Title = event.SafePathRefsText(result.Results[i].Title)
		result.Results[i].URL = event.SafePathRefsText(result.Results[i].URL)
		result.Results[i].Snippet = event.SafePathRefsText(result.Results[i].Snippet)
		result.Results[i].Source = event.SafePathRefsText(result.Results[i].Source)
		result.Results[i].Metadata = pathSafeAnyMap(result.Results[i].Metadata)
	}
	if result.Error != nil {
		result.Error.Message = event.SafePathRefsText(result.Error.Message)
	}
	result.Metadata = pathSafeAnyMap(result.Metadata)
	return result
}

func pathSafeCapabilityState(state CapabilityState) CapabilityState {
	for i := range state.SkillSources {
		state.SkillSources[i].Root = event.SafePathLabel(state.SkillSources[i].Root)
	}
	for i := range state.Diagnostics {
		state.Diagnostics[i].Message = pathSafeFreeformText(state.Diagnostics[i].Message)
		state.Diagnostics[i].NextAction = pathSafeFreeformText(state.Diagnostics[i].NextAction)
	}
	for i := range state.MCPServers {
		state.MCPServers[i].NextAction = pathSafeFreeformText(state.MCPServers[i].NextAction)
	}
	return state
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
	ev.HostedResult = nil
	ev.Metadata = publicMetadata(ev.Metadata)
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

func publicObservedContextProjection(projection ObservedContextProjection) ObservedContextProjection {
	projection.Messages = publicObservedMessages(projection.Messages)
	for i := range projection.Segments {
		if projection.Segments[i].MessageIndex >= 0 && projection.Segments[i].MessageIndex < len(projection.Messages) {
			msg := projection.Messages[projection.Segments[i].MessageIndex]
			if msg.Content != "" {
				projection.Segments[i].UIPreview = preview(msg.Content, 240)
			} else {
				projection.Segments[i].UIPreview = ""
			}
		} else {
			projection.Segments[i].UIPreview = event.Redact(projection.Segments[i].UIPreview)
		}
	}
	return projection
}

func pathSafeObservedEntries(entries []ObservedSessionEntry) []ObservedSessionEntry {
	out := append([]ObservedSessionEntry(nil), entries...)
	for i := range out {
		out[i] = pathSafeObservedEntry(out[i])
	}
	return out
}

func pathSafeObservedEntry(entry ObservedSessionEntry) ObservedSessionEntry {
	entry.Message = pathSafeObservedMessage(entry.Message)
	entry.Summary = event.SafePathRefsText(entry.Summary)
	entry.Error = event.SafePathRefsText(entry.Error)
	entry.CompactionReason = event.SafePathRefsText(entry.CompactionReason)
	entry.Metadata = pathSafeMetadata(entry.Metadata)
	return entry
}

func pathSafeObservedMessages(messages []ObservedSessionMessage) []ObservedSessionMessage {
	out := append([]ObservedSessionMessage(nil), messages...)
	for i := range out {
		out[i] = pathSafeObservedMessage(out[i])
	}
	return out
}

func pathSafeObservedContextProjection(projection ObservedContextProjection) ObservedContextProjection {
	projection.Messages = pathSafeObservedMessages(projection.Messages)
	for i := range projection.Segments {
		projection.Segments[i].UIPreview = event.SafePathRefsText(projection.Segments[i].UIPreview)
	}
	return projection
}

func pathSafeObservedMessage(msg ObservedSessionMessage) ObservedSessionMessage {
	msg.Content = event.SafePathRefsText(msg.Content)
	msg.Reasoning = event.SafePathRefsText(msg.Reasoning)
	msg.ToolArgs = event.SafePathRefsText(msg.ToolArgs)
	return msg
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

func pathSafeMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		if metadataKeyIsPath(key) {
			out[key] = event.SafePathLabel(value)
		} else {
			out[key] = event.SafePathRefsText(value)
		}
	}
	return out
}

func pathSafeAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch typed := value.(type) {
		case string:
			if metadataKeyIsPath(key) {
				out[key] = event.SafePathLabel(typed)
			} else {
				out[key] = event.SafePathRefsText(typed)
			}
		case map[string]string:
			out[key] = pathSafeMetadata(typed)
		case map[string]any:
			out[key] = pathSafeAnyMap(typed)
		case []string:
			items := make([]string, len(typed))
			for i, item := range typed {
				if metadataKeyIsPath(key) {
					items[i] = event.SafePathLabel(item)
				} else {
					items[i] = event.SafePathRefsText(item)
				}
			}
			out[key] = items
		case []any:
			items := make([]any, len(typed))
			for i, item := range typed {
				items[i] = pathSafeAnyValue(key, item)
			}
			out[key] = items
		default:
			out[key] = value
		}
	}
	return out
}

func pathSafeAnyValue(key string, value any) any {
	switch typed := value.(type) {
	case string:
		if metadataKeyIsPath(key) {
			return event.SafePathLabel(typed)
		}
		return event.SafePathRefsText(typed)
	case map[string]string:
		return pathSafeMetadata(typed)
	case map[string]any:
		return pathSafeAnyMap(typed)
	case []string:
		out := make([]string, len(typed))
		for i, item := range typed {
			if metadataKeyIsPath(key) {
				out[i] = event.SafePathLabel(item)
			} else {
				out[i] = event.SafePathRefsText(item)
			}
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = pathSafeAnyValue(key, item)
		}
		return out
	default:
		return value
	}
}

func metadataKeyIsPath(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	return key == "path" ||
		key == "workdir" ||
		key == "cwd" ||
		key == "dir" ||
		key == "directory" ||
		strings.HasSuffix(key, "_path") ||
		strings.HasSuffix(key, "_dir") ||
		strings.HasSuffix(key, "_directory")
}

func publicMetadataRedactedLabel(value string) string {
	if value == "" {
		return ""
	}
	return "[redacted]#" + event.StableHash(event.Redact(value))[:12]
}

func publicMetadataValueIsSafe(key, value string) bool {
	switch key {
	case "source", "previous_tools", "selected_tools", "status", "phase", "completion_reason", "continuation_reason", "finish_reason", "raw_finish_reason", "turn_status", "entry_type", "result_hash", "error_code":
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
			out[i] = event.SanitizePathRefs(ev)
		} else {
			out[i] = event.Sanitize(ev)
		}
	}
	return out
}

func publicHarnessEvents(events []agentharness.HarnessEvent, debugRaw bool) []agentharness.HarnessEvent {
	out := append([]agentharness.HarnessEvent(nil), events...)
	for i := range out {
		if debugRaw {
			out[i].Message = event.SafePathRefsText(out[i].Message)
			out[i].Status = event.SafePathRefsText(out[i].Status)
			out[i].Metadata = pathSafeMetadata(out[i].Metadata)
		} else {
			out[i].Message = event.Redact(out[i].Message)
			out[i].Status = event.Redact(out[i].Status)
			out[i].Metadata = publicMetadata(out[i].Metadata)
		}
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
		return pathSafeAgentStreamEvent(ev)
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

func pathSafeAgentStreamEvent(ev AgentStreamEvent) AgentStreamEvent {
	if ev.Entry != nil {
		entry := pathSafeObservedEntry(*ev.Entry)
		ev.Entry = &entry
	}
	if ev.ProviderRequest != nil {
		req := pathSafeObservedProviderRequest(*ev.ProviderRequest)
		ev.ProviderRequest = &req
	}
	if ev.ProviderEvent != nil {
		providerEvent := pathSafeObservedProviderEvent(*ev.ProviderEvent)
		ev.ProviderEvent = &providerEvent
	}
	if ev.EngineEvent != nil {
		engineEvent := event.SanitizePathRefs(*ev.EngineEvent)
		ev.EngineEvent = &engineEvent
	}
	if ev.Snapshot != nil {
		snapshot := pathSafeAgentSessionSnapshot(*ev.Snapshot)
		ev.Snapshot = &snapshot
	}
	if ev.Result != nil {
		result := pathSafeAgentRunResponse(*ev.Result)
		ev.Result = &result
	}
	ev.Message = event.SafePathRefsText(ev.Message)
	ev.Error = event.SafePathRefsText(ev.Error)
	ev.Metadata = pathSafeMetadata(ev.Metadata)
	return ev
}

func pathSafeAgentRunResponse(resp AgentRunResponse) AgentRunResponse {
	resp.Summary = event.SafePathRefsText(resp.Summary)
	resp.Output = event.SafePathRefsText(resp.Output)
	resp.Error = event.SafePathRefsText(resp.Error)
	resp.WaitingPrompt = event.SafePathRefsText(resp.WaitingPrompt)
	resp.Events = publicEvents(resp.Events, true)
	resp.HarnessEvents = publicHarnessEvents(resp.HarnessEvents, true)
	resp.Diagnostics = pathSafeMetadata(resp.Diagnostics)
	resp.Session = pathSafeAgentSessionSnapshot(resp.Session)
	resp.Observation = pathSafeAgentObservation(resp.Observation)
	return resp
}

func pathSafeFreeformText(value string) string {
	return event.SafePathRefsText(value)
}

func stableHashAny(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		data = []byte("{}")
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
