---
type: Public API
title: runtime Package
description: The runtime package is the main public facade for hosted threads, child threads, projected turns, stores, events, and control signals.
resource: /runtime/runtime.go
tags: [api, runtime]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

`runtime` is the primary downstream integration package. Use `runtime.NewHost`
for Floret-managed durable conversations. Use `runtime.RunProjectedTurn` when a
host already owns conversation rows and wants Floret to execute one provider
loop over a transcript projection.

# Main Entry Points

* `NewHost` creates a durable conversation host.
* `NewMemoryStore` creates an in-memory runtime store for tests or ephemeral use.
* `OpenSQLiteStore` creates Floret-managed durable runtime storage.
* `RunProjectedTurn` executes one run from host-owned transcript projection.
* `ProjectedTurnOptions.ManualCompactions` lets a host request manual context
  compaction for an active projected run. Floret polls it at provider-loop safe
  points, emits compaction lifecycle events, and continues the same run.
* `CompactProjectedContext` runs a compaction-only maintenance operation for a
  host-owned transcript projection and returns compacted active transcript
  checkpoint metadata without creating natural assistant output.
* `SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`, and
  `CloseSubAgent` manage durable child threads under a hosted parent thread.
* `ReadSubAgentDetail` and `ListSubAgentDetailEvents` let a host read a
  parent-scoped, paginated child-thread execution timeline for human UI or
  audit surfaces without expanding `WaitSubAgents` payloads.
* `ModelGateway` lets a host supply model transport through
  `HostOptions.ModelGateway` or `ProjectedTurnOptions.ModelGateway` while
  Floret owns loop control, tool dispatch, and ledgers.
* `ReasoningSelection` carries provider-neutral reasoning intent for a run.
  `RunProjectedTurn` accepts it as an override, and `ModelRequest` forwards the
  effective selection to host-owned model gateways.
* `ModelEventToolCallStart`, `ModelEventToolCallDelta`, and
  `ModelEventToolCallEnd` expose model tool-call streaming as provider-neutral
  public runtime events. They identify the call being generated without carrying
  argument text; `ModelEventToolCalls` remains the final executable batch.

# Boundaries

The host should treat `runtime.Store` as opaque. Product data such as owners,
workspace metadata, pinned state, billing, and read watermarks belongs in the
host database keyed by `runtime.ThreadID`.

Subagents are parent-managed child threads, not a graph workflow framework and
not host-owned pending tool work. Each child uses its own durable `ThreadID` and
prompt scope; host products own agent profiles, permission policy, UI, and
orchestration prompts.

When `HostOptions.ModelGateway` is set, hosted parent turns, hosted child turns,
and hosted title generation all use the supplied model transport. The gateway is
still invoked with the concrete runtime identity for each request, so a child
turn uses the child `ThreadID` and prompt scope.

Child `ThreadID` is the lifecycle target for spawn, send, wait, list, and close.
Task names and agent paths are reference metadata and may repeat. Queued child
inputs are journal entries in the child thread, so host restart and storage
backends preserve pending work, cancellation, and consumption state.

`WaitSubAgents` is a bounded lifecycle wait. Its default wait is five minutes
and requests are capped at twenty minutes. Timeout returns snapshots and
`TimedOut=true`; it does not close, delete, or hide child detail. Child run
execution also has a configurable host timeout, defaulting to twenty minutes, so
a stuck child turn can be stopped while preserving its thread and journal.

Subagent detail reads are scoped by both parent and child `ThreadID`; a host
must prove parent ownership before a child timeline can be read. Detail events
are projected from the child thread's current journal path in ordinal order and
cover delegated input, messages, tool calls, tool results, approvals, turn
markers, lifecycle stops, compaction checkpoints, and run failures. This API is
for host UI and audit inspection. Hosts should keep model-facing subagent tool
results bounded and should not inject the full detail timeline into a parent
model context by default.

Detail pagination is bounded by a default and maximum limit and returns
`HasMore` plus `NextOrdinal`. By default, detail events expose bounded,
sanitized previews plus hashes, metadata, truncation facts, and artifact
references. Raw message content, reasoning, tool arguments, and full tool result
content are omitted unless a host sets `IncludeRaw` for an explicitly authorized
human/debug inspection surface, and hosts should not reuse that raw response as
model-facing tool output.

`StreamObservation` is for host rendering and diagnostics. It is not raw
provider wire data and must not carry prompt text, tool arguments, tool results,
local paths, or secrets.

Reasoning selection is request intent, not provider wire data. Floret normalizes
the public selection and provider adapters translate only values supported by the
selected model capability. Hosts that own model transport through `ModelGateway`
receive the effective selection and must render provider-specific payloads
outside Floret.

Projected manual compaction is a control surface, not a transcript message.
Hosts pass active-run requests through `ManualCompactionSource`; Floret decides
the safe point, runs the same compaction pipeline as automatic pressure
compaction with `manual/manual` trigger and reason, includes request correlation
in start/complete/failed/cancelled observations, and then continues the provider
loop. `ManualCompactionOperationID` exposes the public operation identity for a
known run id, provider-loop step, and manual request id so hosts can correlate
accepted manual work with later Floret observations without depending on
internal engine formatting. A failed manual compaction is observable but does not
by itself end the active run; a manual source poll failure is emitted as a safe
debug observation and the provider request continues.

Projected compactions also emit `runtime.Event.CompactionDebug` diagnostics.
Those events identify safe pipeline stages and include operation/request
correlation, token pressure, message counts, durations, provider-state kind, and
the next action without exposing local paths, secrets, prompt text, tool
payloads, or generated summaries. Compaction attempts emit begin diagnostics
before preflight checks and a terminal `preflight` failure when configuration or
circuit-breaker checks stop the operation before summary generation. Manual poll
errors use the `poll` stage. Cancelled compactions use cancelled lifecycle and
debug statuses, preserving operation/request correlation.

For idle host-owned threads, `CompactProjectedContext` is the public
compaction-only entry point. The result `ActiveTranscript` begins with a
`compaction_summary` checkpoint and preserves compaction id, generation, and
window metadata. Downstream hosts that own durable thread storage should persist
that active transcript or an equivalent checkpoint as their next projected
history source; opaque provider continuation state is carry-through state, not a
substitute for a thread-level checkpoint.

# Key Source Files

* [Runtime Host](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Core Control Helpers](/runtime/control.go)
