package engine

import "github.com/floegence/floret/provider"

type StepMetrics struct {
	Step              int            `json:"step"`
	Provider          string         `json:"provider,omitempty"`
	Model             string         `json:"model,omitempty"`
	Usage             provider.Usage `json:"usage"`
	ProviderLatencyMS int64          `json:"provider_latency_ms,omitempty"`
	ToolLatencyMS     int64          `json:"tool_latency_ms,omitempty"`
	ToolCalls         int            `json:"tool_calls,omitempty"`
	Retries           int            `json:"retries,omitempty"`
}

type RunMetrics struct {
	Usage       provider.Usage `json:"usage"`
	Steps       int            `json:"steps"`
	LLMRequests int            `json:"llm_requests"`
	ToolCalls   int            `json:"tool_calls"`
	Compactions int            `json:"compactions"`
	Retries     int            `json:"retries"`
	WallTimeMS  int64          `json:"wall_time_ms,omitempty"`
}

type BudgetMetrics struct {
	Type  string     `json:"type"`
	Used  float64    `json:"used"`
	Limit float64    `json:"limit"`
	Run   RunMetrics `json:"run"`
}

func (m *RunMetrics) AddUsage(usage provider.Usage) {
	m.Usage = m.Usage.Add(usage)
}
