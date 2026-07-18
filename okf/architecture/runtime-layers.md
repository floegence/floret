---
type: Architecture Concept
title: Runtime Layers
description: Runtime, AgentHarness, and Engine form separate layers for public hosting, durable conversation lifecycle, and single-run execution.
resource: /runtime/runtime.go
tags: [architecture, runtime, engine]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Floret separates public hosting, durable conversation lifecycle, and low-level
provider execution.

# Layer Responsibilities

`runtime.Host` is the provider-backed read and active-execution facade. It runs
turns, retries, compacts, settles active pending work, manages interactive child
threads, and returns host-safe snapshots. It does not create, title, fork,
delete, or bulk-close thread data.

Provider-free lifecycle transitions use narrow concrete capabilities. Bootstrap
code creates `ThreadCreateHost`, `ThreadTitleHost`, `ThreadForkHost`,
`ThreadDeleteHost`, `ThreadReadHost`, `SubAgentMaintenanceHost`, and the
`PendingToolSettlementHost` from one opaque `HostRuntime`. Each coordinator
receives only the handle for its transition; no coordinator receives a raw
`runtime.Store` or the opaque runtime token. `ThreadReadHost` is read-only and
is the reload source for canonical thread, turn, context, todo, and SubAgent
projections. Pending approval reads remain on the provider-backed `Host`, whose
active harness owns the current approval map.
Terminal execution facts and terminal display projection availability are
separate results. If durable detail cannot be read after a turn terminates,
the runtime preserves the engine result and reports projection status as
`unavailable` without changing the execution error. Pending-tool settlement
also preserves a committed settlement event when its projection read fails.
`HostOptions.Runtime` and `ThreadCapabilityOptions.Runtime` accept only the
opaque runtime token. `runtime.Store` is opened and closed by the bootstrap
owner; it is never stored in a coordinator or run object. Facades never close
an injected Store, so shutdown closes the Store exactly once after active work
ends.
Runtime resolves a target thread plus its descendants before submitting one
tree delete request to storage. The SQLite implementation deletes thread rows,
journal entries, active leases, Agent todo state, metadata, artifacts, prompt
scopes, and provider ledgers in one immediate transaction; the public schema and
`DeleteThread` signature remain stable.

Pending tool completion and pending tool settlement are intentionally separate.
`CompletePendingTool` creates a provider-visible follow-up turn when the model
should reason over a host-owned completion. `SettlePendingTool` appends only a
detail/activity event for the original turn when the host needs to update UI
state without resuming the provider loop. Settlement is keyed by the public
`PendingToolSettlementTarget`, which carries complete thread, turn, run, tool
call, tool name, and handle identity. It is idempotent for the same terminal
status and may be recorded before the pending result row when host-owned work
finishes faster than the provider turn can commit that row.
When the provider-backed `Host` already owns an active thread, settlement uses
that same active `AgentHarness` thread and does not re-enter turn admission. The
provider-free `PendingToolSettlementHost` is a dedicated coordinator capability;
it is not a general maintenance or read facade and must be bound to the one
owner responsible for settling that pending work.

The durable Floret journal, public turn pages and projections, pending approval
snapshot, and typed Agent todo state are the canonical Agent source. Hosts may
keep product audit and diagnostics, but must not persist a queryable copy of
conversation content, turn/run lifecycle, todo state, approval lifecycle, tool
status, arguments, results, or errors.

`HostOptions.ModelGateway` lets a host route hosted parent and child turns
through product-owned model transport while Floret still owns request
construction, provider loop control, ledgers, tool dispatch, and runtime events.
Title generation is host-owned by default, but title persistence is always
Floret-owned. `SetThreadTitle` is the explicit host write contract.
`ThreadTitleModeProvider` routes a dedicated Floret title request through the
same transport; a nil internal title generator means disabled rather than an
implicit provider fallback.
`HostOptions.ModelGatewayIdentity` supplies the provider/model identity for that
host-owned transport plus a required non-sensitive continuation compatibility
key. Gateway-backed hosts keep provider transport settings out
of `HostOptions.Config`, so fake provider configuration cannot leak into a
production gateway integration.
Before invoking that gateway, Floret projects the journal into typed model
messages. One assistant response carries text/reasoning plus its ordered
parallel tool-call group; tool results must follow in the same order with exact
call ID and tool name. Empty or invalid JSON arguments, duplicate IDs, orphaned
results, unresolved calls, and illegal adjacency fail before transport. The
gateway adapter only performs a direct wire-shape mapping.
User messages may also carry opaque attachment resource references. Floret
stores the association in the journal and prompt-cache snapshot while the
`ModelGateway` host resolves the resource into provider-native content. A native
provider host without that resolver rejects attachments before admission.

`AgentHarness` is the internal durable conversation layer. It owns threads,
parent-child thread lifecycle, turn lifecycle, retries, forks, titles, and
projection of an active journal path into one engine execution.
It also owns opaque provider continuation persistence in Floret Store. A turn
loads continuation only when the current journal leaf and compatibility key
match exactly; mismatch deletes the record. Fresh response state is committed
against the precomputed terminal entry identity before that terminal marker is
appended, while context-changing turns without fresh state clear the old record.
If projection, provider-state, failure, or terminal persistence fails, the turn
remains unfinished instead of returning a fabricated terminal result.
The public observation boundary carries normalized finish, raw finish,
inference, completion, and continuation as distinct typed facts. Hosts consume
those fields directly instead of interpreting metadata keys.
Public thread forks are durable operations rather than best-effort recursive
copies. AgentHarness prepares one immutable plan containing the pinned source
leaf, destination IDs, turn/run rewrites, terminal child list, and child
metadata patches before any target exists. Storage persists that plan and final
result independently from generic metadata. Replays execute exact marked nodes;
they do not infer success from destination existence or inspect a newer source
path.
Realtime turn projection and final engine-result backfill share one durable
journal. When a provider continuation appends more assistant text to the same
turn, the harness commits the live prefix at continuation boundaries and only
backfills the uncommitted suffix, preserving the ordered execution transcript
without duplicating accumulated provider output.
Durable compaction entries are committed only after `Engine` has rebuilt and
validated the compacted provider request, so a journal checkpoint is an
installed continuation boundary rather than a candidate summary.
Restart recovery uses the same durable ledger boundary. `ResumeThread` closes
unresolved provider-visible tool calls for interrupted turns before writing the
terminal interrupted marker. Only ordinary provider-visible calls receive
closure results; `MessageKindControlSignal` never does. The harness processes
the active lease's exact turn, or the active path's sole unfinished turn when no
lease exists. Multiple unfinished turns fail as a journal invariant. Recovery
does not scan inactive branches, move the leaf, or discard later turns. Active
turn lease reconciliation is owned by this layer as well: once durable evidence
shows that the leased turn is terminal, the harness releases that lease before
admitting the next turn.

`Engine` is the prompt-first single-run executor. It owns provider loop control,
tool invocation, compaction decisions, prompt-cache requests, metrics, and event
emission.
It checks cumulative `MaxInputTokens` after each provider usage merge before the
independent cumulative `MaxTotalTokens` limit. Per-request output limits remain
part of provider request/context policy and are not treated as run-level input
budget.
Ordinary calls in one model tool-call batch execute concurrently and may emit
each tool result as soon as that individual tool finishes, so hosts can observe
a pending tool result without waiting for a slower sibling. Durable harness
save points remain provider-safe boundaries: the harness commits the tool calls first, may
commit partial results for live observation, and writes the tool-result batch
save point only after all calls in that batch have matching results.
Provider-visible transcript messages for the batch are appended in the original
tool-call order after all sibling results are available, even though observation
events may arrive in completion order.
For terminal control signals, `Engine` normalizes the visible completion output
from the signal itself or from assistant text produced in the same provider
step. A terminal control signal with neither source is a contract error, so the
engine never fabricates a successful completion.

# Hosted Context Lifecycle

`runtime.Host` is the public facade for durable turns and context lifecycle.
The host supplies product input, tools, permissions, and optional model
transport; Floret owns provider-visible context assembly, trimming, summary
generation, checkpoint installation, provider continuation state, prompt-cache
ledgers, and lifecycle events.

`RunTurnRequest.Input` is the durable user message contract. Its text and opaque
attachment references enter the canonical journal together and remain attached
through provider projection, detail reads, compaction retention, and forks.
Resource bytes and resource lifecycle remain host-owned.

`RunTurnRequest.SupplementalContext` is the host-facing current-turn context
slot. AgentHarness forwards it to Engine as provider-request projection input,
and Engine renders it into each provider request for that turn without appending
it to the durable journal. The field is product-neutral: hosts decide which
source facts are safe, redacted, and truncated before calling Floret, while
Floret keeps `Input`, thread history ownership, runtime working directory,
permissions, approvals, and opaque provider state unchanged.

Active manual compaction flows through `RunTurnRequest.ManualCompactions`. The
host owns the user-facing command or policy that creates a request; `Engine`
owns polling at safe provider-loop points, summary generation, checkpoint
installation, and continuation of the same run. `Engine` also owns manual
compaction admission. If the current context is too small, has no safe cut
point, or would not shrink after checkpoint overhead, the manual request ends
with a `noop` observation rather than a checkpoint or a run failure. Hosts must
not provide target tokens, history ranges, or summary policy to override that
decision.

Idle compaction uses `Host.CompactThread` instead of pretending to be a user
turn. It requires a request ID and source, runs the compaction pipeline once,
and returns one canonical terminal compaction event, metrics, and activity
timeline. Floret clears continuation after a successful context change and
preserves it across noop/failed/cancelled operations only when provider-visible
context is unchanged. The host never receives or persists the opaque envelope
and must not rebuild provider-visible history from product messages, debug
reports, or checkpoint internals.

# Child Threads

Hosted subagents are durable child threads managed by `AgentHarness` and exposed
through `runtime.Host`. A parent can spawn, send input to, wait for, list, and
close child threads. The child runs as a normal Floret thread with its own
`ThreadID`, `TurnID`s, prompt scope, provider request ledger, and journal.
Floret persists neutral delegated-work metadata such as task name and task
description with the child lifecycle; downstream products decide how to render
that description and how to attach UI routing actions.
Queued input is represented in that journal as Floret lifecycle state, not as an
in-memory host queue, so restart recovery, wait semantics, and close semantics
derive from the same durable source.

Waiting for child threads is intentionally separate from reading child detail.
`WaitSubAgents` returns bounded snapshots and timeout state; it does not return
the child transcript, tool outputs, or detail timeline.
`ListSubAgentActivityTimeline` gives host UIs a parent-scoped activity summary
derived from child snapshots without exposing the child transcript.
`ReadSubAgentDetail` is the single public host API for parent-scoped, paginated
inspection of the child journal. It lets a product UI show complete child
execution detail while keeping parent model context small. Its top-level
activity timeline is rebuilt from retained child detail events on each read so
later tool results and pending-tool settlements replace earlier running updates;
row events remain the ordered child journal.
Its top-level context block carries neutral model-bound context facts:
provider/model identity, model-derived context policy, current pressure/usage
status, and public compaction operations. Fork mode and parent/child thread
identity affect lookup and journal ownership only; they do not define context
window size.

Hosts that need lifecycle metadata plus the latest admitted turn use
`ReadThreadOverview`, which projects both from one active path. Hosts create a
missing canonical journal only through `CreateThread`; transcript-free
`ReadThread` and `ThreadSummary` projections never create or recover one, and
the public runtime exposes no alternate start-or-create entry point.
Conversation bootstrap and pagination use `ListThreadTurns`, whose before,
after, and tail modes always
return admitted canonical turn ordinals in ascending order. A marker-only turn
remains outside the public turn list even if it is later cancelled or terminal;
only a canonical user entry admits it. The page through ordinal continues to cover the full read
boundary. Hosts that need to reload a single known turn may use
`ReadTurnProjection` with explicit `ThreadID`, `TurnID`, and `RunID`. Floret
rebuilds assistant text, verified control-signal payloads, and activity timelines
from durable detail; hosts do not assemble a shadow transcript.
Live, terminal-result, pending-tool settlement, and read-back projections share
one version rule: `ThroughOrdinal` is the greatest durable journal-detail
ordinal included by the common projector. Projection timestamps are diagnostic
only.

Closing a subagent stops current child execution and queued work. It preserves
the child thread and journal so the host can still read detail after close,
timeout, restart, or terminal completion.

This layer does not define product roles such as reviewer or worker beyond
opaque labels. Hosts own prompt policy, permissions, UI, and product workflow.

# Key Source Files

* [Runtime Host](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Agent Harness](/internal/agentharness/harness.go)
* [Replayable Forks](/internal/agentharness/fork_operation.go)
* [Engine](/internal/engine/engine.go)
