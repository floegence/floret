// Package cache records provider-visible prompt segments and request
// ledgers.
//
// Stores must be safe for concurrent BuildPlan, toolset, request, and response
// operations across different prompt scopes. A prompt scope is the explicit
// boundary for prompt segment reuse; durable threads normally use ThreadID as
// PromptScopeID, while standalone runs use RunID. Prompt cache ledgers may
// retain raw prompt text for provider reuse and debugging, so public host views
// must sanitize observations before exposing them.
package cache
