// Package tools defines local tool registration, permission checks, and
// execution.
//
// Registry is safe for concurrent definition reads and tool execution.
// Registration validates tool names, effects, permission modes, and reserved
// control names. Unknown or blank permission modes fail closed; tools with
// mutating, destructive, open-world, or shell/network effects must declare
// explicit permission behavior. Parallel scheduling is controlled by
// Definition.ParallelSafe, which is only valid for strictly read-only tools.
package tools
