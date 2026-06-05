// Package engine implements the low-level prompt-first turn executor.
//
// Engine instances execute turns from local run state. Each Run and RunTurn
// builds local turn state and may execute concurrently when shared
// dependencies are safe for concurrent use. The supported concurrent scope is a
// distinct Options.SessionID, or a distinct Options.RunID when SessionID is
// omitted. Providers, prompt cache stores, session stores, tool registries,
// event sinks, approvers, and compaction managers supplied by callers must
// honor their own concurrency contracts.
//
// Hosts that need durable conversations should prefer runtime.NewHarness or
// agentharness.AgentHarness. Direct Engine use is intended for tests, eval
// runners, and specialized hosts that already own session persistence.
//
// Construct engines with New(Config). Provider-visible local tool definitions
// are derived from tools.Registry so registry validation, permission policy, and
// deny-tool hiding remain the single local-tool boundary.
package engine
