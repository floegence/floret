---
type: Architecture Concept
title: Execution Identities
description: Floret uses explicit identities for durable threads, turns, provider runs, prompt-cache scopes, and traces.
resource: /AGENTS.md
tags: [architecture, identity, runtime]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Execution identity is intentionally explicit. Code must not infer one identity
from another unless a package contract says so. This keeps durable conversation
storage, provider execution, prompt-cache reuse, and observation correlation
separate.

# Core Identities

* `ThreadID` identifies a durable conversation journal.
* `TurnID` identifies one user-facing turn.
* `RunID` identifies one engine/provider execution.
* `PromptScopeID` identifies the reuse boundary for prompt-cache records and
  provider ledgers.
* `TraceID` correlates events across a logical execution trace.
* `LogicalRequestID` may identify retries or transport attempts, but it must not
  replace `RunID`, `TurnID`, or `TraceID`.

# Maintenance Notes

A normal harness turn may use the same string for `RunID` and `TurnID`, but code
must not rely on that equality. Prompt-cache rows and JSON use
`prompt_scope_id`, not `run_id`, as the reuse boundary.

# Key Source Files

* [Repository Guide](/AGENTS.md)
* [Runtime IDs](/runtime/runtime.go)
* [Projected Turn Request](/runtime/projected_turn.go)
* [Prompt Cache](/internal/provider/cache/promptcache.go)
