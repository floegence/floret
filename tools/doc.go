// Package tools defines local tool registration, permission metadata, and
// authority-gated execution.
//
// Registry is safe for concurrent definition reads and tool execution.
// Registration validates tool names, effects, permission modes, and reserved
// control names. Strictly safe read-only tools may omit Permission.Mode and are
// exposed as allowed tools. Mutating, destructive, open-world, shell, and
// network tools must declare explicit permission behavior. Host authorization
// is supplied through runtime.EffectAuthorizationGate; this package does not
// expose a standalone approval callback. PermissionDeny tools
// remain callable only as explicit host-side disabled tools and are not exposed
// to providers. Ordinary calls returned in one model batch execute concurrently;
// the model expresses dependencies by emitting dependent calls in later turns.
package tools
