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
for durable conversations. It returns the concrete `*runtime.Host` facade;
`NewThreadMaintenanceHost` returns the concrete
`*runtime.ThreadMaintenanceHost` facade. Hosts provide product input, tools,
permissions, and optional model transport; Floret owns provider loop execution,
provider-visible context assembly, trimming, summary generation, compaction
checkpoints, continuation state, and lifecycle observations.

Downstream packages that need substitution define local interfaces containing
only their actual capability methods. Floret does not publish a repository-wide
host interface for every runtime operation.

# Main Entry Points

* `NewHost` creates a durable conversation host.
* `NewThreadMaintenanceHost` creates a provider-free thread maintenance host for
  thread summary, turn projection read-back, pending tool settlement, child
  close, and thread deletion operations that do not run the model loop.
* `NewMemoryStore` creates an in-memory runtime store for tests or ephemeral use.
* `OpenSQLiteStore` creates Floret-managed durable runtime storage.
* `Host.EnsureThread` creates or recovers a hosted thread and returns
  transcript-free `ThreadSummary` lifecycle metadata.
* `Host.RunTurn` executes one hosted user-facing turn.
* `RunTurnRequest.Limits.MaxInputTokens` caps cumulative provider input tokens
  across every model request in one run. `MaxTotalTokens` independently caps
  cumulative total tokens; provider/context output limits remain per request.
* `RunTurnRequest.RunID`, `ThreadID`, and `TurnID` are required. `RunID`
  identifies the concrete engine/provider execution and must be supplied
  explicitly rather than inferred from the turn identity.
* `RunTurnRequest.ManualCompactions` lets a host request manual context
  compaction for an active hosted run. Floret polls it at provider-loop safe
  points, emits compaction lifecycle events, and continues the same run.
* `RunTurnRequest.SupplementalContext` carries host-provided current-turn
  context items. Floret renders them into provider requests for that turn only
  without modifying `Input`, durable thread history, working directory,
  permission state, tool approval state, or opaque provider continuation state.
  Hosts own source policy, redaction, and truncation before passing those items
  to Floret.
* `Host.CompactThread` runs a compaction-only maintenance operation for an idle
  hosted thread through `CompactThreadRequest` without creating natural
  assistant output.
* `SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`,
  `CloseSubAgent`, and `CloseSubAgents` manage durable child threads under a
  hosted parent thread.
* `ListSubAgentActivityTimeline` returns a parent-scoped,
  product-neutral `observation.ActivityTimeline` for hosted child lifecycle
  status without exposing child transcripts. Its payload includes durable
  child-thread identities such as `thread_id` and `subagent_id`, but not
  downstream product actions, routing targets, or UI runtime labels.
* `ReadSubAgentDetail` and `ListSubAgentDetailEvents` let a host read a
  parent-scoped, paginated child-thread execution timeline for human UI or
  audit surfaces without expanding `WaitSubAgents` payloads. Their top-level
  `activity_timeline` is the canonical current activity projection rebuilt from
  retained child detail events; paginated rows are ordered journal facts rather
  than live tool-state snapshots.
  Their top-level `context` block exposes neutral model-bound facts: provider
  and model identity, model-derived context policy, current context
  pressure/usage status, and public compaction lifecycle operations. Context
  window size comes from the resolved model capability and policy, not from
  parent thread, child thread, subagent, or fork mode.
* `ListThreadDetailEvents` lets a host read the Floret-owned ordered execution
  transcript for a hosted thread without reading Floret storage internals.
* `ProjectThreadTurn`, `ReadTurnProjection`, and `TurnResult.Projection` expose
  the product-neutral ordered assistant text, activity timeline, and
  control-signal segments for a hosted turn.
* `ListPendingApprovals` returns the current product-neutral tool approvals
  waiting for a host decision on a thread.
* `CompletePendingTool` requires the public completion `RunID` and uses it as
  the follow-up execution identity.
* `SettlePendingTool` records a host-owned pending tool outcome as a
  detail/activity event for the original turn without creating provider-visible
  context or running another model turn. `PendingToolSettlementTarget` carries
  the complete thread, turn, run, tool call, tool name, and handle identity; the
  result echoes that target and validates it against the settlement event and
  projection. Settlement is idempotent for the same target and status, rejects
  conflicting status for an already settled target, and may arrive before the
  original pending tool result row when host-owned work finishes immediately
  after the tool call is exposed.
* `tools.PendingToolResult.Handle` is the provider-visible continuation token.
  Pending metadata remains observation-only and is not rendered as a model-facing
  tool result field.
* `DeleteThread` removes a Floret-owned thread tree from the engine store,
  including child threads, prompt cache scopes, and artifacts.
* `ErrThreadNotFound`, `ErrTurnNotFound`, `ErrRunNotFound`, and
  `ErrSubAgentNotFound` are public sentinel errors for `errors.Is` checks on
  Host facade not-found responses.
* `TurnResult.ProjectionAvailability` reports `ready` or `unavailable`
  independently from execution error. An unavailable projection preserves
  terminal engine facts and carries diagnostic text in `ProjectionError`.
* `ModelGateway` lets a host supply model transport through
  `HostOptions.ModelGateway` while Floret owns loop control, tool dispatch,
  context lifecycle, and ledgers.
* `ModelGatewayIdentity` supplies the provider and model names for
  `ModelGateway` requests and ledgers. Gateway-backed hosts must not set
  provider transport fields such as provider, model, base URL, API key, or fake
  response in `HostOptions.Config`; pass the raw non-transport runtime config to
  `NewHost` and put transport identity only in `HostOptions.ModelGatewayIdentity`.
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

When `HostOptions.ModelGateway` is set, hosted parent and child turns use the
supplied model transport. Thread titles remain host-owned unless the host
explicitly selects `ThreadTitleModeProvider`; only that mode sends title requests
through the same gateway. The gateway is invoked with the concrete runtime
identity for each request, so a child turn uses the child `ThreadID` and prompt
scope. Provider/model identity comes from `HostOptions.ModelGatewayIdentity`;
gateway-backed hosts pass raw non-transport config to `NewHost` and keep provider
transport configuration out of `HostOptions.Config`.

Child `ThreadID` is the lifecycle target for spawn, send, wait, list, and close.
Task names, task descriptions, and agent paths are reference metadata and may
repeat. `SubAgentSnapshot.TaskDescription` records the parent-authored
responsibility or objective for the child as neutral lifecycle metadata; product
UI actions, routing ids, and display copy remain host-owned. Queued child inputs
are journal entries in the child thread, so host restart and storage backends
preserve pending work, cancellation, and consumption state.
After a provider-backed host process restarts, `ThreadMaintenanceHost.ListSubAgents`
is the canonical public reload source for a parent thread's child-thread list;
hosts must not inspect Floret storage tables or rebuild child identity from
transcript display projections.

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
transcript facts. `ThreadTurnProjection` is the public display projection over
those facts: `RunTurn`, `RetryTurn`, and `CompletePendingTool` return it on
`TurnResult`, `SettlePendingTool` returns it on `PendingToolSettlementResult`,
`runtime.Event.Projection` carries the current live projection on committed
thread-entry events, and hosts with a known `ThreadID`, `TurnID`, and `RunID`
may call `ReadTurnProjection` to rebuild the turn display projection from
durable Floret detail after reload. Runtime live, turn-result,
pending-settlement, and read-back projections are canonical current-turn
display projections built by the Floret host from raw-capable journal facts;
the default preview-only detail read model is for listing and inspection, not
for authoritative assistant markdown.
`ThreadTurnProjection.ThroughOrdinal` is the maximum durable detail-event
ordinal included in the projection. Hosts compare it only within the explicit
`ThreadID`, `TurnID`, and `RunID` identity tuple to reject duplicate or stale
updates. `ProjectedAt` records projection time but does not define ordering. A
projection whose turn-start marker is durable reports `Status=running`; the
status changes only when a completed, waiting, failed, or cancelled marker (or a
terminal error fact) is included.
`ReadTurnProjection` requires explicit `RunID` input instead of inferring it
from stored rows, because `RunID` is execution identity and not the thread or
turn storage identity. A missing thread is reported with `ErrThreadNotFound`; a
known thread with no matching turn detail is reported with `ErrTurnNotFound`;
and a turn whose durable detail does not record the requested run is reported
with `ErrRunNotFound`.
If final detail reading or projection attachment fails, `RunTurn`, `RetryTurn`,
and `CompletePendingTool` preserve their engine result, return
`ProjectionAvailability=unavailable`, and do not convert the projection failure
into an execution error. `SettlePendingTool` likewise preserves its committed
detail event when the subsequent projection read is unavailable.
`ForkThread` is the public runtime contract for host-visible conversation forks.
Its request requires a host-supplied `ForkOperationID`. Before creating any
target, Floret durably fixes the source leaf, destination thread IDs, complete
turn/run mappings, and terminal child-thread clone plan. Each target carries an
exact operation/node marker. Repeating the same operation and request resumes
only missing nodes or returns the stored result, including after SQLite reopen;
it never rereads a newer source leaf or regenerates identities. Reusing an
operation for a different request returns `ErrForkOperationConflict`; an
occupied or mismarked destination returns `ErrForkDestinationConflict`; and a
missing target from a completed operation returns
`ErrForkOperationTargetMissing`.

The result returns the operation ID and the destination `TurnID`/`RunID`
mapping needed for later `ReadTurnProjection` calls. Host products may persist
those public identity references with their own thread metadata, but must not
clone Floret storage tables or materialize Floret display projections into a
host shadow transcript. Provider-free `ThreadMaintenanceHost` exposes the same
contract so UI and restart maintenance can fork thread history without provider
configuration.
`ProjectThreadTurn` derives assistant text, control-signal segments, and turn
activity only from the ordered `ThreadDetailEvent` stream for the target turn.
It does not accept or merge an older aggregate activity timeline as an input.
Row-level `ActivityTimeline` values are treated only as already-sanitized
presentation metadata for their corresponding detail event; the final activity
state is re-reduced from ordered tool call, tool result, approval, turn marker,
and run-failure facts. Terminal turn markers apply across all activity timeline
segments in the turn: cancelled turns settle unresolved pending/running items to
`canceled`, and failed turns settle them to `error`. Successful or completed
turns do not imply completion of host-owned pending work. Save-point markers
and other non-terminal turn markers such as `started`, `waiting`, or unknown
queued states remain lifecycle facts and do not settle activity. The
`tool_result_batch` save-point reason is a projection boundary only: it closes
the current activity segment so the next tool batch can become a separate
segment, while later facts for the same tool invocation still merge back into
one activity item. Downstream hosts may map those product-neutral segments to
their own UI blocks, but must not read Floret's store schema, rebuild execution
ordering from separate audit tables, or call `observation.BuildActivityTimeline`
to create the main thread activity surface. Host live protocols, cursors,
replacement snapshots, and product timeline reducers remain downstream
concerns; they are not Floret runtime contracts. Pagination uses `AfterOrdinal`,
`Limit`, `HasMore`, and
`NextOrdinal`; raw content follows the same explicit `IncludeRaw` opt-in rule as
subagent detail reads. Thread detail events share the same row-level
`ActivityTimeline` projection and structured tool result `status` contract as
subagent detail events. Subagent detail context facts are read from Floret's
public journal projection and sanitized observation DTOs; raw provider requests,
prompt-cache internals, transcript windows, and trimming strategy fields remain
internal. Host-owned pending tool outcomes after a provider turn must be
reported through `SettlePendingTool`; that settlement is stored as a custom
journal entry projected as a `tool_result` detail event, updates the original
activity item for the same tool id, removes running-only metadata, and does not
enter provider-visible history. Turn projection treats that settlement as the
authoritative activity lifecycle fact even when run-end markers or the
original pending tool result are ordered before or after it.

`runtime.Event.ActivityTimeline` remains Floret's live lifecycle observation
for tool, approval, control, budget, and run-end facts. It is generated from the
same sanitized observation events used for terminal `TurnResult` activity, but
it is not the display segment ordering contract for a hosted turn. Hosts should
use `runtime.Event.Projection`, terminal `TurnResult.Projection`, settlement
projections, or `ReadTurnProjection` for the main thread display and treat the
aggregate live timeline as observation/diagnostic state. Tool call and tool
result detail entries preserve the original lifecycle event time rather than
the later batch append time; `ProjectThreadTurn` uses those event times plus
result duration facts to keep final item intervals consistent. Invocation
presentation stored on a tool call message is merged with result presentation
for the same tool id, so final rows keep command labels and payload while
adding terminal result chips and payload fields.

Terminal turn results, including cancelled turns, still return a bounded
`ThreadTurnProjection`. Failed and cancelled terminal markers settle unresolved
tool and approval activity in Floret's projection before the result is returned,
so impossible-to-continue rows do not remain decisionable. Caller cancellation
during live projection or turn finalization is still recorded as a cancelled
terminal fact rather than as a generic failure. Successful turns keep
host-owned pending work running until the host reports a terminal outcome
through `SettlePendingTool`. That settlement remains authoritative for the tool
id and updates the same projected activity item rather than adding a duplicate
row. A provider-backed Host may settle pending work through the same active
thread it already owns; this detail-only mutation does not reacquire the turn
lease or create another provider request. `ThreadMaintenanceHost` continues to
respect leases owned by active provider hosts. Runtime restart recovery also
reconciles active turn leases from the same
durable facts: a turn that already has terminal or interrupted evidence is
closed through Floret's ledger before the next `RunTurn` is admitted. Downstream
hosts should consume Floret projections and settlement results
to replace their product UI for the turn instead of synthesizing final tool
status from local audit records or live stream leftovers.

Pending approval snapshots are the current-state companion to the durable
approval audit trail. `ListPendingApprovals` can be called while a turn is active
and returns approval ids, tool call ids, tool names, effects, resources, labels,
host context, state, timing, revision, batch index, and batch size metadata from
Floret's generic approval lifecycle. Hosts own product modes, approval copy, UI placement,
authorization policy, and decision routing; Floret does not encode those
product concepts in the snapshot.

For provider continuations such as length truncation recovery, the detail stream
records the durable live prefix and any final suffix in ordinal order. It does
not expose a duplicated accumulated assistant snapshot as a separate transcript
fact.

Deleting a thread is a data lifecycle operation. `DeleteThread` deletes the
target thread and Floret-managed descendant child threads, plus their prompt
cache records and thread artifacts. Runtime resolves the complete tree before
issuing one storage delete operation. SQLite applies threads, entries, active
leases, metadata, artifacts, prompt segments, toolsets, provider requests, and
provider responses in one immediate transaction, so a failure rolls back the
whole tree without changing the public store schema. Hosts should use this
public API instead of querying or mutating Floret storage tables directly.

`NewThreadMaintenanceHost` is the provider-free constructor for maintenance
processes that share a Floret `Store` but do not need provider configuration.
It exposes `EnsureThread`, `ReadTurnProjection`, `SettlePendingTool`,
`ListSubAgents`, `ListSubAgentActivityTimeline`, `ReadSubAgentDetail`,
`ListSubAgentDetailEvents`, `CloseSubAgents`, `DeleteThread`, and `Close`
without accepting fake providers, model gateways, tools, or host UI options.
`ListSubAgents` on this facade returns the same durable parent-scoped child
snapshots after process restart as a provider-backed host would expose while
running.
Subagent detail reads from this facade return the same persisted model/context
facts as provider-backed hosts.
`ThreadMaintenanceHostOptions.Store` is required so maintenance code cannot
silently operate on an empty ephemeral store. The constructor returns the
independent concrete `*ThreadMaintenanceHost` facade, so reload, detail, pending
work settlement, cleanup, and deletion code can stay on the public runtime
boundary without pretending to be a model-running host.

Host facade not-found responses should be handled with `errors.Is` against
`runtime.ErrThreadNotFound`, `runtime.ErrTurnNotFound`,
`runtime.ErrRunNotFound`, or
`runtime.ErrSubAgentNotFound`. Hosts should not parse error strings or import
Floret internal package sentinels.

Fork replay failures should likewise use `errors.Is` with
`runtime.ErrForkOperationConflict`, `runtime.ErrForkDestinationConflict`, and
`runtime.ErrForkOperationTargetMissing`.

Hosts validate `ProjectionAvailability`, `Projection`, and `ProjectionError` as
one outcome. `ready` requires a valid projection and no projection error;
`unavailable` requires no projection and a non-empty diagnostic. Explicit
`ReadTurnProjection` remains the durable reload operation.

`StreamObservation` is for host rendering and diagnostics. It is not raw
provider wire data and must not carry prompt text, tool arguments, tool results,
local paths, or secrets.

`runtime.Event`, `observation.Event`, `TurnResult`, and `StreamObservation`
expose normalized `FinishReason`, separate `RawFinishReason`, `FinishInferred`,
and finite completion or continuation reasons as typed fields. Metadata is not a
secondary encoding of those decisions. `runtime.Event.Validate` recursively
validates stream observations, activity timelines, turn projections, and event
to projection identity. Non-empty unknown values, simultaneous completion and
continuation, and inferred finishes without a normalized reason fail validation.

`runtime.Event.Committed` carries a `ThreadDetailEvent` after Floret has
successfully appended the corresponding journal entry. Hosts can render provider
text deltas as temporary live output, but durable display reconciliation should
use committed thread events or `ListThreadDetailEvents` so visible ordering
matches Floret's stored transcript. Internal harness events such as
`entry_appended` and `turn_started` remain on `HarnessSink`; they are not runtime
observation events and never enter the public `EventSink`.

Projected control signals are declared through `TurnSignalSpec`. Waiting
signals interrupt the turn with their projected prompt. Terminal signals complete
the turn with a human-visible output. A terminal signal may supply that output in
the signal payload, or it may rely on assistant text produced earlier in the
same provider step; if neither exists, the turn fails with a control-contract
error instead of inventing a completion. Control signals are projected as control
activity and control-signal display segments; they are not ordinary local tool
execution records.

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
