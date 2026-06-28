---
type: Public API
title: runtime Package
description: The runtime package is the main public facade for hosted threads, child threads, stores, events, control signals, and Floret-owned context lifecycle.
resource: /runtime/runtime.go
tags: [api, runtime]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

`runtime` is the primary downstream integration package. Use `runtime.NewHost`
for durable conversations. Hosts provide product input, tools, permissions, and
optional model transport; Floret owns provider loop execution, provider-visible
context assembly, trimming, summary generation, compaction checkpoints,
continuation state, and lifecycle observations.

# Main Entry Points

* `NewHost` creates a durable conversation host.
* `NewMemoryStore` creates an in-memory runtime store for tests or ephemeral use.
* `OpenSQLiteStore` creates Floret-managed durable runtime storage.
* `Host.EnsureThread` creates or recovers a hosted thread and returns
  transcript-free `ThreadSummary` lifecycle metadata.
* `Host.RunTurn` executes one hosted user-facing turn.
* `RunTurnRequest.ManualCompactions` lets a host request manual context
  compaction for an active hosted run. Floret polls it at provider-loop safe
  points, emits compaction lifecycle events, and continues the same run.
* `Host.CompactThread` runs a compaction-only maintenance operation for an idle
  hosted thread through `CompactThreadRequest` without creating natural
  assistant output.
* `SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`,
  `CloseSubAgent`, and `CloseSubAgents` manage durable child threads under a
  hosted parent thread.
* `ListSubAgentActivityTimeline` returns a parent-scoped,
  product-neutral `observation.ActivityTimeline` for hosted child lifecycle
  status without exposing child transcripts.
* `ReadSubAgentDetail` and `ListSubAgentDetailEvents` let a host read a
  parent-scoped, paginated child-thread execution timeline for human UI or
  audit surfaces without expanding `WaitSubAgents` payloads.
* `ListThreadDetailEvents` lets a host read the Floret-owned ordered execution
  transcript for a hosted thread without reading Floret storage internals.
* `ListPendingApprovals` returns the current product-neutral tool approvals
  waiting for a host decision on a thread.
* `DeleteThread` removes a Floret-owned thread tree from the engine store,
  including child threads, prompt cache scopes, and artifacts.
* `ModelGateway` lets a host supply model transport through
  `HostOptions.ModelGateway` while Floret owns loop control, tool dispatch,
  context lifecycle, and ledgers.
* `ToolSurfaceProvider` lets a host refresh the provider-visible local tools,
  hosted tools, system prompt, and host context at provider-loop safe points
  without adding product-specific policy concepts to Floret.
* `ReasoningSelection` carries provider-neutral reasoning intent for a run.
  `RunTurnRequest` carries it as turn intent, and `ModelRequest` forwards the
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

`SubAgentSnapshot.ForkMode` is the engine-owned fork contract for a child
thread. `none` starts the child with only the delegated mission, while
`full_path` forks the parent thread path into the child thread. Hosts may map
this to product UI terms, but should not persist a second engine fork state.

`WaitSubAgents` is a bounded lifecycle wait. Its default wait is five minutes
and requests are capped at twenty minutes. Timeout returns snapshots and
`TimedOut=true`; it does not close, delete, or hide child detail. Child run
execution also has a configurable host timeout, defaulting to twenty minutes, so
a stuck child turn can be stopped while preserving its thread and journal.

Stopping a parent run is an execution lifecycle decision, not a data retention
decision. Hosts that stop parent work should call `CloseSubAgents` for the
parent thread. Floret cancels unfinished child turns, cancels queued child
inputs, marks affected children closed, writes lifecycle detail, and keeps the
child histories readable. Completed, failed, cancelled, or already closed
children remain historical records.

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
model-facing tool output. Each projected row may also carry a canonical
`observation.ActivityTimeline` generated by Floret. Tool result details expose a
structured `status`; hosts should use that status and the row timeline instead
of inferring execution state from preview or result text.

Thread detail reads expose the current hosted thread journal path in entry
ordinal order. They cover user messages, assistant messages, tool calls, tool
results, turn markers, compaction checkpoints, approvals, custom entries, and
run failures. This is Floret's public read model for durable execution
transcript facts; downstream hosts may derive product UI caches from it, but
must not read Floret's store schema or rebuild execution ordering from separate
audit tables. Pagination uses `AfterOrdinal`, `Limit`, `HasMore`, and
`NextOrdinal`; raw content follows the same explicit `IncludeRaw` opt-in rule as
subagent detail reads. Thread detail events share the same row-level
`ActivityTimeline` projection and structured tool result `status` contract as
subagent detail events.

Pending approval snapshots are the current-state companion to the durable
approval audit trail. `ListPendingApprovals` can be called while a turn is active
and returns approval ids, tool call ids, tool names, effects, resources, labels,
host context, state, timing, and revision metadata from Floret's generic
approval lifecycle. Hosts own product modes, approval copy, UI placement,
authorization policy, and decision routing; Floret does not encode those
product concepts in the snapshot.

For provider continuations such as length truncation recovery, the detail stream
records the durable live prefix and any final suffix in ordinal order. It does
not expose a duplicated accumulated assistant snapshot as a separate transcript
fact.

Deleting a thread is a data lifecycle operation. `DeleteThread` deletes the
target thread and Floret-managed descendant child threads, plus their prompt
cache records and thread artifacts. Hosts should use this public API instead of
querying or mutating Floret storage tables directly.

`StreamObservation` is for host rendering and diagnostics. It is not raw
provider wire data and must not carry prompt text, tool arguments, tool results,
local paths, or secrets.

`runtime.Event.Committed` carries a `ThreadDetailEvent` after Floret has
successfully appended the corresponding journal entry. Hosts can render provider
text deltas as temporary live output, but durable display reconciliation should
use committed thread events or `ListThreadDetailEvents` so visible ordering
matches Floret's stored transcript.

Projected control signals are declared through `TurnSignalSpec`. Waiting
signals interrupt the turn with their projected prompt. Terminal signals complete
the turn with a human-visible output. A terminal signal may supply that output in
the signal payload, or it may rely on assistant text produced earlier in the
same provider step; if neither exists, the turn fails with a control-contract
error instead of inventing a completion.

Reasoning selection is request intent, not provider wire data. Floret normalizes
the public selection and provider adapters translate only values supported by the
selected model capability. Hosts that own model transport through `ModelGateway`
receive the effective selection and must render provider-specific payloads
outside Floret.

Dynamic tool surfaces are host-owned policy projection points. A host may set
`HostOptions.ToolSurfaceProvider` for a durable host or
`RunTurnRequest.ToolSurfaceProvider` for one turn. Floret invokes it before
provider requests, before ordinary local tool dispatch, and before compact-only
provider request rebuilds. The returned `ToolSurface` can supply a current
`tools.Registry`, explicit local tool definitions, provider-hosted tools, system
prompt text, host context, and optional epoch/reason metadata.

Floret does not interpret product modes or authorization labels inside that
surface. If a provider returns a local tool call that was visible in an older
request but is no longer in the refreshed registry, the normal Floret tool
permission and dispatch lifecycle rejects or gates it at dispatch time. Provider
requests and observations include the current toolset, hosted toolset, prompt
hash, epoch, and reason metadata so hosts can audit surface changes without
reading internal provider-cache records.

Manual compaction is a control surface, not a transcript message. Hosts pass
active-run requests through `ManualCompactionSource`; Floret decides the safe
point, runs the same compaction pipeline as automatic pressure compaction with
`manual/manual` trigger and reason, includes request correlation in
start/complete/failed/cancelled observations, and then continues the provider
loop when the manual compaction failed without cancellation.
Manual compaction is admission controlled by Floret. A valid manual request does
not force checkpoint creation: when the current Floret-owned context is below
the minimum useful compaction target, has no safe cut point, or would not shrink
after checkpoint overhead, Floret emits a terminal `noop` compaction observation
with request correlation and a coarse reason such as `context_too_small`,
`no_compactable_context`, or `insufficient_savings`. A no-op manual compaction
does not install a checkpoint, does not update continuation state, and does not
end the hosted run.
`ManualCompactionOperationID` exposes the public operation identity for a known
run id, provider-loop step, and manual request id so hosts can correlate
accepted manual work with later Floret observations without depending on
internal engine formatting. A failed manual compaction is observable but does
not by itself end the active run; a non-cancellation manual source poll failure
is emitted as a safe debug observation and the provider request continues.
Cancellation during manual polling or compaction is terminal for that hosted
turn and does not continue to a provider request.

Hosted compactions also emit `runtime.Event.CompactionDebug` diagnostics.
Those events identify safe pipeline stages and include operation/request
correlation, token pressure, message counts, durations, provider-state kind, and
the next action without exposing local paths, secrets, prompt text, tool
payloads, or generated summaries. Compaction attempts emit begin diagnostics
before preflight checks and a terminal `preflight` failure when configuration or
circuit-breaker checks stop the operation before summary generation. Manual poll
errors use the `poll` stage; non-cancellation poll errors continue to the
provider request, while cancellation poll errors use terminal `fail_turn`
semantics. Cancelled compactions use cancelled lifecycle and debug statuses,
preserving operation/request correlation without exposing raw request strings in
public runtime events.

For idle hosted threads, `Host.CompactThread` is the public compaction-only
entry point. The result reports status, metrics, safe lifecycle observations,
activity timeline, and opaque provider state. Hosts may persist opaque provider
state envelopes and pass them back unchanged, but they must not parse them,
rebuild provider-visible history, or treat context assembly reports as input for
the next turn.

# Key Source Files

* [Runtime Host](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Core Control Helpers](/runtime/control.go)
