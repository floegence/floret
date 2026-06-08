package testui

import (
	"time"

	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/provider/catalog"
	"github.com/floegence/floret/session/contextpolicy"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/testing/eval"
)

type ConfigInfo struct {
	Provider     string        `json:"provider"`
	Model        string        `json:"model"`
	EnvFile      string        `json:"env_file"`
	EnvFileFound bool          `json:"env_file_found"`
	LiveProvider bool          `json:"live_provider"`
	BaseURL      string        `json:"base_url,omitempty"`
	Storage      storageStatus `json:"storage"`
}

type RunRequest struct {
	Target    string `json:"target"`
	ProfileID string `json:"profile_id,omitempty"`
}

type RunResponse struct {
	ID         string           `json:"id"`
	Target     string           `json:"target"`
	Title      string           `json:"title"`
	Kind       string           `json:"kind"`
	Status     string           `json:"status"`
	StartedAt  time.Time        `json:"started_at"`
	FinishedAt time.Time        `json:"finished_at"`
	DurationMS int64            `json:"duration_ms"`
	Summary    string           `json:"summary"`
	Command    []string         `json:"command,omitempty"`
	ExitCode   int              `json:"exit_code,omitempty"`
	Output     string           `json:"output,omitempty"`
	Error      string           `json:"error,omitempty"`
	Packages   []PackageSummary `json:"packages,omitempty"`
	TestTotals TestTotals       `json:"test_totals,omitempty"`
	Agent      *AgentRun        `json:"agent,omitempty"`
	Parts      []RunResponse    `json:"parts,omitempty"`
}

type PackageSummary struct {
	Name       string  `json:"name"`
	Status     string  `json:"status"`
	ElapsedSec float64 `json:"elapsed_sec,omitempty"`
	Tests      int     `json:"tests,omitempty"`
	Passed     int     `json:"passed,omitempty"`
	Failed     int     `json:"failed,omitempty"`
	Skipped    int     `json:"skipped,omitempty"`
}

type TestTotals struct {
	Packages int `json:"packages"`
	Tests    int `json:"tests"`
	Passed   int `json:"passed"`
	Failed   int `json:"failed"`
	Skipped  int `json:"skipped"`
}

type AgentRun struct {
	EngineStatus string                      `json:"engine_status"`
	Output       string                      `json:"output"`
	Metrics      engine.RunMetrics           `json:"metrics"`
	Events       []event.Event               `json:"events"`
	Eval         *eval.Result                `json:"eval,omitempty"`
	Artifacts    map[string]ArtifactSnapshot `json:"artifacts,omitempty"`
	Config       ConfigInfo                  `json:"config,omitempty"`
}

type ArtifactSnapshot struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type ConfigState struct {
	EnvFile               string               `json:"env_file"`
	EnvFileFound          bool                 `json:"env_file_found"`
	ActiveProfileID       string               `json:"active_profile_id"`
	DebugRawEnabled       bool                 `json:"debug_raw_enabled"`
	Profiles              []ProviderProfile    `json:"profiles"`
	Catalog               []CatalogProvider    `json:"catalog"`
	ContextPolicyDefaults contextpolicy.Policy `json:"context_policy_defaults"`
	Tools                 []AgentToolOption    `json:"tools"`
	SearchWireShapes      []SearchWireShape    `json:"search_wire_shapes"`
	SearchProvider        SearchProviderInfo   `json:"search_provider"`
	Capabilities          CapabilityState      `json:"capabilities"`
	LocalTime             LocalTimeInfo        `json:"local_time"`
	Storage               storageStatus        `json:"storage"`
}

type CapabilityState struct {
	MCPServers   []MCPCapabilityState   `json:"mcp_servers"`
	SkillSources []SkillSourceState     `json:"skill_sources,omitempty"`
	Skills       []SkillCapabilityState `json:"skills"`
	Diagnostics  []CapabilityDiagnostic `json:"diagnostics,omitempty"`
}

type MCPCapabilityState struct {
	Name            string `json:"name"`
	Status          string `json:"status"`
	Transport       string `json:"transport,omitempty"`
	ToolCount       int    `json:"tool_count,omitempty"`
	PermissionMode  string `json:"permission_mode,omitempty"`
	FailureCategory string `json:"failure_category,omitempty"`
	NextAction      string `json:"next_action,omitempty"`
}

type SkillCapabilityState struct {
	Name         string `json:"name"`
	Description  string `json:"description,omitempty"`
	SourceKind   string `json:"source_kind,omitempty"`
	SourceLabel  string `json:"source_label,omitempty"`
	RelativePath string `json:"relative_path,omitempty"`
	ContentHash  string `json:"content_hash,omitempty"`
	License      string `json:"license,omitempty"`
	Status       string `json:"status"`
}

type SkillSourceState struct {
	Root       string `json:"root"`
	Kind       string `json:"kind"`
	Label      string `json:"label,omitempty"`
	Enabled    bool   `json:"enabled"`
	Managed    bool   `json:"managed"`
	SkillCount int    `json:"skill_count,omitempty"`
}

type CapabilityDiagnostic struct {
	Kind       string `json:"kind"`
	Capability string `json:"capability,omitempty"`
	SourceKind string `json:"source_kind,omitempty"`
	Message    string `json:"message"`
	NextAction string `json:"next_action,omitempty"`
}

type SkillInstallPreviewRequest struct {
	URL string `json:"url"`
}

type SkillInstallRequest struct {
	URL          string `json:"url"`
	PreviewToken string `json:"preview_token"`
	Replace      bool   `json:"replace,omitempty"`
}

type SkillInstallPreview struct {
	URL             string             `json:"url"`
	PreviewToken    string             `json:"preview_token"`
	Repo            string             `json:"repo"`
	Ref             string             `json:"ref"`
	SourcePath      string             `json:"source_path"`
	Name            string             `json:"name"`
	Description     string             `json:"description"`
	License         string             `json:"license,omitempty"`
	Files           []SkillInstallFile `json:"files"`
	TotalBytes      int64              `json:"total_bytes"`
	TargetPath      string             `json:"target_path"`
	ExistingHash    string             `json:"existing_hash,omitempty"`
	ContentHash     string             `json:"content_hash"`
	RequiresReplace bool               `json:"requires_replace"`
}

type SkillInstallFile struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type SkillInstallResponse struct {
	Skill        SkillInstallPreview `json:"skill"`
	Capabilities CapabilityState     `json:"capabilities"`
	SourceRoot   string              `json:"source_root"`
	EnvUpdated   bool                `json:"env_updated"`
}

type LocalTimeInfo struct {
	Now           string `json:"now"`
	TimeZone      string `json:"time_zone"`
	OffsetMinutes int    `json:"offset_minutes"`
	OffsetLabel   string `json:"offset_label"`
}

type SearchProviderInfo struct {
	Provider    string `json:"provider"`
	APIKeySet   bool   `json:"api_key_set"`
	Endpoint    string `json:"endpoint,omitempty"`
	EnvKey      string `json:"env_key"`
	EndpointKey string `json:"endpoint_key"`
	Capability  string `json:"capability"`
}

type SearchWireShape struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type ProviderProfile struct {
	ID           string               `json:"id"`
	Name         string               `json:"name"`
	Provider     string               `json:"provider"`
	Model        string               `json:"model"`
	BaseURL      string               `json:"base_url,omitempty"`
	APIKey       string               `json:"api_key,omitempty"`
	APIKeySet    bool                 `json:"api_key_set,omitempty"`
	FakeResponse string               `json:"fake_response,omitempty"`
	WebSearch    searchcap.Capability `json:"web_search,omitempty"`
}

type CatalogProvider = catalog.Provider

type SaveConfigRequest struct {
	ActiveProfileID string             `json:"active_profile_id"`
	Profiles        []ProviderProfile  `json:"profiles"`
	SearchProvider  SaveSearchProvider `json:"search_provider,omitempty"`
}

type SaveSearchProvider struct {
	Provider string  `json:"provider,omitempty"`
	APIKey   string  `json:"api_key,omitempty"`
	Endpoint *string `json:"endpoint,omitempty"`
}

type AgentRunRequest struct {
	ProfileID     string               `json:"profile_id"`
	Profile       ProviderProfile      `json:"profile,omitempty"`
	Message       string               `json:"message"`
	SystemPrompt  string               `json:"system_prompt"`
	SelectedTools []string             `json:"selected_tools,omitempty"`
	ToolMode      string               `json:"tool_mode,omitempty"`
	ContextPolicy contextpolicy.Policy `json:"context_policy,omitempty"`
	DebugRaw      bool                 `json:"debug_raw,omitempty"`
}

type AgentTurnRequest struct {
	Message  string `json:"message"`
	DebugRaw bool   `json:"debug_raw,omitempty"`
}

type AgentStreamEventType string

const (
	AgentStreamTurnStarted              AgentStreamEventType = "turn_started"
	AgentStreamUserMessageAppended      AgentStreamEventType = "user_message_appended"
	AgentStreamProviderRequest          AgentStreamEventType = "provider_request"
	AgentStreamProviderDelta            AgentStreamEventType = "provider_delta"
	AgentStreamAssistantMessageAppended AgentStreamEventType = "assistant_message_appended"
	AgentStreamToolCall                 AgentStreamEventType = "tool_call"
	AgentStreamToolResult               AgentStreamEventType = "tool_result"
	AgentStreamTurnSavePoint            AgentStreamEventType = "turn_save_point"
	AgentStreamSessionSnapshot          AgentStreamEventType = "session_snapshot"
	AgentStreamTurnCompleted            AgentStreamEventType = "turn_completed"
	AgentStreamTurnFailed               AgentStreamEventType = "turn_failed"
)

type AgentStreamEvent struct {
	Sequence        int64                    `json:"sequence"`
	Type            AgentStreamEventType     `json:"type"`
	SessionID       string                   `json:"session_id,omitempty"`
	TurnID          string                   `json:"turn_id,omitempty"`
	EntryID         string                   `json:"entry_id,omitempty"`
	Step            int                      `json:"step,omitempty"`
	At              time.Time                `json:"at"`
	Entry           *ObservedSessionEntry    `json:"entry,omitempty"`
	ProviderRequest *ObservedProviderRequest `json:"provider_request,omitempty"`
	ProviderEvent   *ObservedProviderEvent   `json:"provider_event,omitempty"`
	EngineEvent     *event.Event             `json:"engine_event,omitempty"`
	Snapshot        *AgentSessionSnapshot    `json:"session_snapshot,omitempty"`
	Result          *AgentRunResponse        `json:"result,omitempty"`
	Message         string                   `json:"message,omitempty"`
	Error           string                   `json:"error,omitempty"`
	Metadata        map[string]string        `json:"metadata,omitempty"`
}

type AgentStreamSink interface {
	EmitAgentStream(AgentStreamEvent)
}

type AgentToolsUpdateRequest struct {
	SelectedTools *[]string `json:"selected_tools"`
	Reason        string    `json:"reason,omitempty"`
}

type AgentInterfaceProbeRequest struct {
	ProfileID     string               `json:"profile_id,omitempty"`
	SelectedTools []string             `json:"selected_tools,omitempty"`
	ContextPolicy contextpolicy.Policy `json:"context_policy,omitempty"`
	DebugRaw      bool                 `json:"debug_raw,omitempty"`
}

type AgentRunResponse struct {
	StatusCode         int                         `json:"-"`
	ID                 string                      `json:"id"`
	Probe              bool                        `json:"probe,omitempty"`
	SessionID          string                      `json:"session_id"`
	TurnID             string                      `json:"turn_id"`
	Status             string                      `json:"status"`
	StartedAt          time.Time                   `json:"started_at"`
	FinishedAt         time.Time                   `json:"finished_at"`
	DurationMS         int64                       `json:"duration_ms"`
	Summary            string                      `json:"summary"`
	Output             string                      `json:"output"`
	Error              string                      `json:"error,omitempty"`
	Profile            ProviderProfile             `json:"profile"`
	Metrics            engine.RunMetrics           `json:"metrics"`
	Events             []event.Event               `json:"events"`
	HarnessEvents      []agentharness.HarnessEvent `json:"harness_events,omitempty"`
	CompletionReason   string                      `json:"completion_reason,omitempty"`
	ContinuationReason string                      `json:"continuation_reason,omitempty"`
	FinishReason       string                      `json:"finish_reason,omitempty"`
	RawFinishReason    string                      `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool                        `json:"finish_inferred,omitempty"`
	Diagnostics        map[string]string           `json:"diagnostics,omitempty"`
	CanAppendMessage   bool                        `json:"can_append_message"`
	WaitingPrompt      string                      `json:"waiting_prompt,omitempty"`
	Session            AgentSessionSnapshot        `json:"session"`
	Observation        AgentObservation            `json:"observation"`
}

type AgentObservation struct {
	ProviderRequests  []ObservedProviderRequest `json:"provider_requests"`
	ProviderEvents    []ObservedProviderEvent   `json:"provider_events"`
	SessionMessages   []ObservedSessionMessage  `json:"session_messages"`
	ActiveContext     []ObservedSessionMessage  `json:"active_context"`
	ContextProjection ObservedContextProjection `json:"context_projection,omitempty"`
	PathEntries       []ObservedSessionEntry    `json:"path_entries"`
	Transitions       []StateTransition         `json:"transitions"`
	Diagnostics       map[string]string         `json:"diagnostics,omitempty"`
}

type ObservedProviderRequest struct {
	RunID                   string                          `json:"run_id,omitempty"`
	SessionID               string                          `json:"session_id,omitempty"`
	ThreadID                string                          `json:"thread_id,omitempty"`
	TurnID                  string                          `json:"turn_id,omitempty"`
	Step                    int                             `json:"step"`
	Provider                string                          `json:"provider"`
	Model                   string                          `json:"model"`
	ObservedAt              time.Time                       `json:"observed_at"`
	Messages                []ObservedSessionMessage        `json:"messages"`
	Tools                   []provider.ToolDefinition       `json:"tools"`
	HostedTools             []provider.HostedToolDefinition `json:"hosted_tools,omitempty"`
	UnavailableCapabilities []string                        `json:"unavailable_capabilities,omitempty"`
	ContextUsage            contextpolicy.Usage             `json:"context_usage,omitempty"`
	RawSegments             []ObservedRawSegment            `json:"raw_segments,omitempty"`
	CacheSummary            ObservedCacheSummary            `json:"cache_summary,omitempty"`
}

type ObservedRawSegment struct {
	ID                   string            `json:"id"`
	RunID                string            `json:"run_id,omitempty"`
	SessionID            string            `json:"session_id,omitempty"`
	ThreadID             string            `json:"thread_id,omitempty"`
	TurnID               string            `json:"turn_id,omitempty"`
	EntryID              string            `json:"entry_id,omitempty"`
	ParentEntryID        string            `json:"parent_entry_id,omitempty"`
	Kind                 cache.SegmentKind `json:"kind"`
	Role                 string            `json:"role,omitempty"`
	SHA256               string            `json:"sha256"`
	ByteLength           int               `json:"byte_length"`
	Epoch                int               `json:"epoch,omitempty"`
	Sequence             int64             `json:"sequence,omitempty"`
	Reused               bool              `json:"reused"`
	FragmentType         string            `json:"fragment_type,omitempty"`
	StructuredRefID      string            `json:"structured_ref_id,omitempty"`
	CompactionGeneration int               `json:"compaction_generation,omitempty"`
	CompactionWindowID   string            `json:"compaction_window_id,omitempty"`
	CompactionEntryID    string            `json:"compaction_entry_id,omitempty"`
	Fingerprint          string            `json:"fingerprint,omitempty"`
	SchemaVersion        string            `json:"schema_version,omitempty"`
	AdapterVersion       string            `json:"adapter_version,omitempty"`
	Raw                  string            `json:"raw,omitempty"`
	RawTruncated         bool              `json:"raw_truncated,omitempty"`
	RawPreview           string            `json:"raw_preview,omitempty"`
}

type ObservedCacheSummary struct {
	Namespace            string `json:"namespace,omitempty"`
	Retention            string `json:"retention,omitempty"`
	PrefixHash           string `json:"prefix_hash,omitempty"`
	PayloadHash          string `json:"payload_hash,omitempty"`
	ToolsetID            string `json:"toolset_id,omitempty"`
	ToolsetEpoch         int    `json:"toolset_epoch,omitempty"`
	CompactionGeneration int    `json:"compaction_generation,omitempty"`
	CompactionWindowID   string `json:"compaction_window_id,omitempty"`
	CompactionEntryID    string `json:"compaction_entry_id,omitempty"`
	ReusedSegments       int    `json:"reused_segments,omitempty"`
	NewSegments          int    `json:"new_segments,omitempty"`
	CacheReadTokens      int64  `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens     int64  `json:"cache_write_tokens,omitempty"`
}

type ObservedProviderEvent struct {
	RunID        string                         `json:"run_id,omitempty"`
	SessionID    string                         `json:"session_id,omitempty"`
	Step         int                            `json:"step"`
	Type         provider.EventType             `json:"type"`
	ObservedAt   time.Time                      `json:"observed_at"`
	ResponseID   string                         `json:"response_id,omitempty"`
	Text         string                         `json:"text,omitempty"`
	Reasoning    string                         `json:"reasoning,omitempty"`
	ToolCalls    []provider.ToolCall            `json:"tool_calls,omitempty"`
	HostedResult *provider.HostedToolResultData `json:"hosted_result,omitempty"`
	Metadata     map[string]string              `json:"metadata,omitempty"`
	Reason       string                         `json:"reason,omitempty"`
	Usage        provider.Usage                 `json:"usage,omitempty"`
}

type ObservedArtifactRef struct {
	ID        string `json:"id,omitempty"`
	SafeLabel string `json:"safe_label,omitempty"`
	URL       string `json:"url,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

type ObservedToolResultView struct {
	Truncated     bool                 `json:"truncated,omitempty"`
	OriginalBytes int                  `json:"original_bytes,omitempty"`
	VisibleBytes  int                  `json:"visible_bytes,omitempty"`
	OriginalLines int                  `json:"original_lines,omitempty"`
	VisibleLines  int                  `json:"visible_lines,omitempty"`
	Strategy      string               `json:"strategy,omitempty"`
	ContentSHA256 string               `json:"content_sha256,omitempty"`
	FullOutput    *ObservedArtifactRef `json:"full_output,omitempty"`
}

type ObservedContextProjection struct {
	Messages []ObservedSessionMessage `json:"messages,omitempty"`
	Segments []ObservedContextSegment `json:"segments,omitempty"`
}

type ObservedContextSegment struct {
	EntryID       string                `json:"entry_id,omitempty"`
	EntryType     sessiontree.EntryType `json:"entry_type,omitempty"`
	MessageIndex  int                   `json:"message_index"`
	Role          string                `json:"role,omitempty"`
	ToolCallID    string                `json:"tool_call_id,omitempty"`
	ToolName      string                `json:"tool_name,omitempty"`
	TokenEstimate int64                 `json:"token_estimate,omitempty"`
	ArtifactRefs  []ObservedArtifactRef `json:"artifact_refs,omitempty"`
	UIPreview     string                `json:"ui_preview,omitempty"`
}

type ObservedSessionMessage struct {
	Role                 string                  `json:"role"`
	Content              string                  `json:"content,omitempty"`
	Reasoning            string                  `json:"reasoning,omitempty"`
	ToolCallID           string                  `json:"tool_call_id,omitempty"`
	ToolName             string                  `json:"tool_name,omitempty"`
	ToolArgs             string                  `json:"tool_args,omitempty"`
	Kind                 string                  `json:"kind,omitempty"`
	ToolResult           *ObservedToolResultView `json:"tool_result,omitempty"`
	EntryID              string                  `json:"entry_id,omitempty"`
	ParentEntryID        string                  `json:"parent_entry_id,omitempty"`
	CompactionID         string                  `json:"compaction_id,omitempty"`
	CompactionGeneration int                     `json:"compaction_generation,omitempty"`
	CompactionWindowID   string                  `json:"compaction_window_id,omitempty"`
}

type ObservedSessionEntry struct {
	ID                      string                       `json:"id"`
	ParentID                string                       `json:"parent_id,omitempty"`
	ThreadID                string                       `json:"thread_id,omitempty"`
	TurnID                  string                       `json:"turn_id,omitempty"`
	Type                    sessiontree.EntryType        `json:"type"`
	CreatedAt               time.Time                    `json:"created_at"`
	Message                 ObservedSessionMessage       `json:"message,omitempty"`
	TurnStatus              sessiontree.TurnMarkerStatus `json:"turn_status,omitempty"`
	CompactionID            string                       `json:"compaction_id,omitempty"`
	PreviousCompactionID    string                       `json:"previous_compaction_id,omitempty"`
	CompactedThroughEntryID string                       `json:"compacted_through_entry_id,omitempty"`
	SummarySchemaVersion    string                       `json:"summary_schema_version,omitempty"`
	CompactionGeneration    int                          `json:"compaction_generation,omitempty"`
	CompactionWindowID      string                       `json:"compaction_window_id,omitempty"`
	FirstKeptEntryID        string                       `json:"first_kept_entry_id,omitempty"`
	KeptUserEntryIDs        []string                     `json:"kept_user_entry_ids,omitempty"`
	Summary                 string                       `json:"summary,omitempty"`
	CompactionTrigger       string                       `json:"compaction_trigger,omitempty"`
	CompactionReason        string                       `json:"compaction_reason,omitempty"`
	CompactionPhase         string                       `json:"compaction_phase,omitempty"`
	TokensBefore            int64                        `json:"tokens_before,omitempty"`
	TokensAfterEstimate     int64                        `json:"tokens_after_estimate,omitempty"`
	ContextUsageBefore      contextpolicy.Usage          `json:"context_usage_before,omitempty"`
	ContextUsageAfter       contextpolicy.Usage          `json:"context_usage_after,omitempty"`
	Error                   string                       `json:"error,omitempty"`
	Metadata                map[string]string            `json:"metadata,omitempty"`
	RawHash                 string                       `json:"raw_hash,omitempty"`
}

type AgentSessionSnapshot struct {
	ID                      string                          `json:"id"`
	Status                  string                          `json:"status"`
	Phase                   string                          `json:"phase"`
	LeafID                  string                          `json:"leaf_id,omitempty"`
	CreatedAt               time.Time                       `json:"created_at"`
	UpdatedAt               time.Time                       `json:"updated_at"`
	Profile                 ProviderProfile                 `json:"profile"`
	SystemPrompt            string                          `json:"system_prompt"`
	SelectedTools           []string                        `json:"selected_tools"`
	HostedTools             []provider.HostedToolDefinition `json:"hosted_tools,omitempty"`
	UnavailableCapabilities []string                        `json:"unavailable_capabilities,omitempty"`
	Capabilities            CapabilityState                 `json:"capabilities"`
	ContextPolicy           contextpolicy.Policy            `json:"context_policy"`
	LatestTurnID            string                          `json:"latest_turn_id,omitempty"`
	WaitingPrompt           string                          `json:"waiting_prompt,omitempty"`
	Recoverable             bool                            `json:"recoverable,omitempty"`
	CanAppendMessage        bool                            `json:"can_append_message"`
	Turns                   []AgentTurnSummary              `json:"turns"`
	ActiveContext           []ObservedSessionMessage        `json:"active_context"`
	ContextProjection       ObservedContextProjection       `json:"context_projection,omitempty"`
	PathEntries             []ObservedSessionEntry          `json:"path_entries"`
	AllEntries              []ObservedSessionEntry          `json:"all_entries"`
	AggregateMetrics        engine.RunMetrics               `json:"aggregate_metrics"`
	Compactions             int                             `json:"compactions"`
	Observation             AgentObservation                `json:"observation,omitempty"`
}

type AgentTurnSummary struct {
	ID                 string            `json:"id"`
	Status             string            `json:"status"`
	Output             string            `json:"output,omitempty"`
	Error              string            `json:"error,omitempty"`
	StartedAt          time.Time         `json:"started_at,omitempty"`
	FinishedAt         time.Time         `json:"finished_at,omitempty"`
	Metrics            engine.RunMetrics `json:"metrics,omitempty"`
	CompletionReason   string            `json:"completion_reason,omitempty"`
	ContinuationReason string            `json:"continuation_reason,omitempty"`
	FinishReason       string            `json:"finish_reason,omitempty"`
	RawFinishReason    string            `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool              `json:"finish_inferred,omitempty"`
}

type StateTransition struct {
	At      time.Time `json:"at"`
	Step    int       `json:"step,omitempty"`
	From    string    `json:"from"`
	To      string    `json:"to"`
	Reason  string    `json:"reason,omitempty"`
	Details string    `json:"details,omitempty"`
}
