// Package promptcache records provider-visible prompt segments and request
// ledgers.
//
// Stores must be safe for concurrent BuildPlan, toolset, request, and response
// operations across different session scopes. A session scope is the durable
// thread/session ID used for segment reuse; different sessions must not share
// raw prompt segments or toolsets by accident. Prompt cache ledgers may retain
// raw prompt text for provider reuse and debugging, so public host views must
// sanitize observations before exposing them.
package promptcache
