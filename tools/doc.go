// Package tools defines local tool registration, permission checks, and
// execution.
//
// Registry is safe for concurrent definition reads and tool execution.
// Registration validates tool names, effects, permission modes, and reserved
// control names. Strictly safe read-only tools may omit Permission.Mode and are
// exposed as allowed tools. Mutating, destructive, open-world, shell, and
// network tools must declare explicit permission behavior. PermissionDeny tools
// remain callable only as explicit host-side disabled tools and are not exposed
// to providers. Parallel scheduling is controlled by Definition.ParallelSafe,
// which is only valid for strictly read-only tools.
package tools
