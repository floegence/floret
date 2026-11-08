---
type: contract
---
# Built-in tool names

Hosts may select input and Goal callable names with `runtime.WithAgentBuiltinToolNames`.
The harness installs one control projector and one Goal handler. Internal semantic
markers identify newly named built-ins; names are not general-purpose ownership
annotations. Published defaults stay unchanged. Original names remain historical
identities and are not execution aliases for custom names.

The contiguous schema 36 to 37 manifest edge fences earlier readers without
rewriting journal entries, effects, provider state or signatures. Copy and
projection paths preserve markers. Duplicate configured tools fail before dispatch.

See [public API](../../runtime/builtin_tool_names.go),
[installation](../../internal/agentharness/goal_tool.go),
[semantic classification](../../internal/session/builtin_tools.go) and
[migration](../../internal/sessiontree/backend_repo_v37.go).
