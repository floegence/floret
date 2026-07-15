package observation

import "fmt"

// EventType identifies one sanitized runtime lifecycle fact.
type EventType string

const (
	EventTypeStepStart              EventType = "step_start"
	EventTypeProviderRequest        EventType = "provider_request"
	EventTypeProviderDelta          EventType = "provider_delta"
	EventTypeProviderReasoning      EventType = "provider_reasoning"
	EventTypeProviderToolCallStart  EventType = "provider_tool_call_start"
	EventTypeProviderToolCallDelta  EventType = "provider_tool_call_delta"
	EventTypeProviderToolCallEnd    EventType = "provider_tool_call_end"
	EventTypeProviderUsage          EventType = "provider_usage"
	EventTypeProviderSources        EventType = "provider_sources"
	EventTypeProviderFinish         EventType = "provider_finish"
	EventTypeProviderRetry          EventType = "provider_retry"
	EventTypeToolCall               EventType = "tool_call"
	EventTypeToolDispatchStarted    EventType = "tool_dispatch_started"
	EventTypeToolActivityUpdated    EventType = "tool_activity_updated"
	EventTypeToolResult             EventType = "tool_result"
	EventTypeToolApprovalRequested  EventType = "tool_approval_requested"
	EventTypeToolApprovalApproved   EventType = "tool_approval_approved"
	EventTypeToolApprovalRejected   EventType = "tool_approval_rejected"
	EventTypeToolApprovalTimedOut   EventType = "tool_approval_timed_out"
	EventTypeToolApprovalCanceled   EventType = "tool_approval_canceled"
	EventTypeHostedToolCall         EventType = "hosted_tool_call"
	EventTypeHostedToolResult       EventType = "hosted_tool_result"
	EventTypeMCPServerConnecting    EventType = "mcp_server_connecting"
	EventTypeMCPServerReady         EventType = "mcp_server_ready"
	EventTypeMCPServerFailed        EventType = "mcp_server_failed"
	EventTypeMCPToolsListed         EventType = "mcp_tools_listed"
	EventTypeMCPToolCall            EventType = "mcp_tool_call"
	EventTypeMCPToolResult          EventType = "mcp_tool_result"
	EventTypeSkillDetected          EventType = "skill_detected"
	EventTypeSkillLoaded            EventType = "skill_loaded"
	EventTypeSkillBlocked           EventType = "skill_blocked"
	EventTypeSkillInstallRequired   EventType = "skill_install_required"
	EventTypeSkillDisclosureApplied EventType = "skill_disclosure_applied"
	EventTypeContextCompact         EventType = "context_compact"
	EventTypeContextCompactDebug    EventType = "context_compact_debug"
	EventTypeContextContinue        EventType = "context_continue"
	EventTypeThreadEntryCommitted   EventType = "thread_entry_committed"
	EventTypeControlSignal          EventType = "control_signal"
	EventTypeBudgetExceeded         EventType = "budget_exceeded"
	EventTypeStepEnd                EventType = "step_end"
	EventTypeRunEnd                 EventType = "run_end"
)

func (t EventType) Valid() bool {
	switch t {
	case EventTypeStepStart, EventTypeProviderRequest, EventTypeProviderDelta,
		EventTypeProviderReasoning, EventTypeProviderToolCallStart,
		EventTypeProviderToolCallDelta, EventTypeProviderToolCallEnd,
		EventTypeProviderUsage, EventTypeProviderSources, EventTypeProviderFinish,
		EventTypeProviderRetry, EventTypeToolCall, EventTypeToolDispatchStarted,
		EventTypeToolActivityUpdated, EventTypeToolResult,
		EventTypeToolApprovalRequested, EventTypeToolApprovalApproved,
		EventTypeToolApprovalRejected, EventTypeToolApprovalTimedOut,
		EventTypeToolApprovalCanceled, EventTypeHostedToolCall,
		EventTypeHostedToolResult, EventTypeMCPServerConnecting,
		EventTypeMCPServerReady, EventTypeMCPServerFailed, EventTypeMCPToolsListed,
		EventTypeMCPToolCall, EventTypeMCPToolResult, EventTypeSkillDetected,
		EventTypeSkillLoaded, EventTypeSkillBlocked, EventTypeSkillInstallRequired,
		EventTypeSkillDisclosureApplied, EventTypeContextCompact,
		EventTypeContextCompactDebug, EventTypeContextContinue,
		EventTypeThreadEntryCommitted, EventTypeControlSignal,
		EventTypeBudgetExceeded, EventTypeStepEnd, EventTypeRunEnd:
		return true
	default:
		return false
	}
}

func (e Event) Validate() error {
	if !e.Type.Valid() {
		return fmt.Errorf("unsupported observation event type %q", e.Type)
	}
	if e.Compaction != nil {
		if err := e.Compaction.Validate(); err != nil {
			return fmt.Errorf("invalid compaction event: %w", err)
		}
	}
	if e.CompactionDebug != nil {
		if err := e.CompactionDebug.Validate(); err != nil {
			return fmt.Errorf("invalid compaction debug event: %w", err)
		}
	}
	return nil
}
