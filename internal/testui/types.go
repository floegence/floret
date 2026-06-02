package testui

import (
	"time"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/eval"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
)

type ConfigInfo struct {
	Provider     string `json:"provider"`
	Model        string `json:"model"`
	EnvFile      string `json:"env_file"`
	EnvFileFound bool   `json:"env_file_found"`
	LiveProvider bool   `json:"live_provider"`
	BaseURL      string `json:"base_url,omitempty"`
}

type RunRequest struct {
	Target string `json:"target"`
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
	EnvFile         string            `json:"env_file"`
	EnvFileFound    bool              `json:"env_file_found"`
	ActiveProfileID string            `json:"active_profile_id"`
	Profiles        []ProviderProfile `json:"profiles"`
	Catalog         []CatalogProvider `json:"catalog"`
}

type ProviderProfile struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Provider     string `json:"provider"`
	Model        string `json:"model"`
	BaseURL      string `json:"base_url,omitempty"`
	APIKey       string `json:"api_key,omitempty"`
	APIKeySet    bool   `json:"api_key_set,omitempty"`
	FakeResponse string `json:"fake_response,omitempty"`
}

type CatalogProvider = modelcatalog.Provider

type SaveConfigRequest struct {
	ActiveProfileID string            `json:"active_profile_id"`
	Profiles        []ProviderProfile `json:"profiles"`
}

type AgentRunRequest struct {
	ProfileID     string               `json:"profile_id"`
	Profile       ProviderProfile      `json:"profile,omitempty"`
	Message       string               `json:"message"`
	SystemPrompt  string               `json:"system_prompt"`
	ContextPolicy contextpolicy.Policy `json:"context_policy,omitempty"`
}

type AgentRunResponse struct {
	ID          string            `json:"id"`
	Status      string            `json:"status"`
	StartedAt   time.Time         `json:"started_at"`
	FinishedAt  time.Time         `json:"finished_at"`
	DurationMS  int64             `json:"duration_ms"`
	Summary     string            `json:"summary"`
	Output      string            `json:"output"`
	Error       string            `json:"error,omitempty"`
	Profile     ProviderProfile   `json:"profile"`
	Metrics     engine.RunMetrics `json:"metrics"`
	Events      []event.Event     `json:"events"`
	Observation AgentObservation  `json:"observation"`
}

type AgentObservation struct {
	ProviderRequests []ObservedProviderRequest `json:"provider_requests"`
	ProviderEvents   []ObservedProviderEvent   `json:"provider_events"`
	SessionMessages  []ObservedSessionMessage  `json:"session_messages"`
	Transitions      []StateTransition         `json:"transitions"`
}

type ObservedProviderRequest struct {
	Step         int                       `json:"step"`
	Provider     string                    `json:"provider"`
	Model        string                    `json:"model"`
	ObservedAt   time.Time                 `json:"observed_at"`
	Messages     []ObservedSessionMessage  `json:"messages"`
	Tools        []provider.ToolDefinition `json:"tools"`
	RawSegments  []ObservedRawSegment      `json:"raw_segments,omitempty"`
	CacheSummary ObservedCacheSummary      `json:"cache_summary,omitempty"`
}

type ObservedRawSegment struct {
	ID              string                  `json:"id"`
	Kind            promptcache.SegmentKind `json:"kind"`
	Role            string                  `json:"role,omitempty"`
	SHA256          string                  `json:"sha256"`
	ByteLength      int                     `json:"byte_length"`
	Epoch           int                     `json:"epoch,omitempty"`
	Sequence        int64                   `json:"sequence,omitempty"`
	Reused          bool                    `json:"reused"`
	FragmentType    string                  `json:"fragment_type,omitempty"`
	StructuredRefID string                  `json:"structured_ref_id,omitempty"`
	Fingerprint     string                  `json:"fingerprint,omitempty"`
	SchemaVersion   string                  `json:"schema_version,omitempty"`
	AdapterVersion  string                  `json:"adapter_version,omitempty"`
	Raw             string                  `json:"raw,omitempty"`
	RawTruncated    bool                    `json:"raw_truncated,omitempty"`
	RawPreview      string                  `json:"raw_preview,omitempty"`
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
	Step       int                 `json:"step"`
	Type       provider.EventType  `json:"type"`
	ObservedAt time.Time           `json:"observed_at"`
	ResponseID string              `json:"response_id,omitempty"`
	Text       string              `json:"text,omitempty"`
	ToolCalls  []provider.ToolCall `json:"tool_calls,omitempty"`
	Reason     string              `json:"reason,omitempty"`
	Usage      provider.Usage      `json:"usage,omitempty"`
}

type ObservedSessionMessage struct {
	Role       string `json:"role"`
	Content    string `json:"content,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	ToolName   string `json:"tool_name,omitempty"`
	ToolArgs   string `json:"tool_args,omitempty"`
}

type StateTransition struct {
	At      time.Time `json:"at"`
	Step    int       `json:"step,omitempty"`
	From    string    `json:"from"`
	To      string    `json:"to"`
	Reason  string    `json:"reason,omitempty"`
	Details string    `json:"details,omitempty"`
}
