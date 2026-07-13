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

`runtime.Host` is the public durable conversation facade. It starts threads,
runs turns, retries, completes or settles pending tool work, manages durable
child threads, deletes thread data, and returns host-safe snapshots.
Terminal execution facts and terminal display projection availability are
separate results. If durable detail cannot be read after a turn terminates,
`RunTurn` preserves the engine result and wraps the detail failure with
`ErrTurnProjectionUnavailable` for host recovery.
`runtime.NewThreadMaintenanceHost` is the provider-free variant for maintenance
processes that share a Floret store but do not run provider turns. It exposes
thread summary recovery, turn projection read-back, pending tool settlement,
parent child-thread closing, and thread-tree deletion without accepting
provider, model, fake response, gateway, tools, or host UI configuration. Its
store option is required because maintenance paths must target an existing
Floret store deliberately.
Runtime resolves a target thread plus its descendants before submitting one
tree delete request to storage. The SQLite implementation deletes thread rows,
journal entries, active leases, metadata, artifacts, prompt scopes, and provider
ledgers in one immediate transaction; the public schema and `DeleteThread`
signature remain stable.

Pending tool completion and pending tool settlement are intentionally separate.
`CompletePendingTool` creates a provider-visible follow-up turn when the model
should reason over a host-owned completion. `SettlePendingTool` appends only a
detail/activity event for the original turn when the host needs to update UI
state without resuming the provider loop. Settlement is keyed by the public
turn/run/tool/handle identity, is idempotent for the same terminal status, and
may be recorded before the pending result row when host-owned work finishes
faster than the provider turn can commit that row.

`HostOptions.ModelGateway` lets a host route hosted parent turns, child turns,
and hosted title generation through product-owned model transport while Floret
still owns request construction, provider loop control, ledgers, tool dispatch,
and runtime events.
`HostOptions.ModelGatewayIdentity` supplies the provider/model identity for that
host-owned transport. Gateway-backed hosts keep provider transport settings out
of `HostOptions.Config`, so fake provider configuration cannot leak into a
production gateway integration.

`AgentHarness` is the internal durable conversation layer. It owns threads,
parent-child thread lifecycle, turn lifecycle, retries, forks, titles, and
projection of an active journal path into one engine execution.
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
terminal interrupted marker. If a later user message or failure already created
a provider-unsafe active branch, `AgentHarness` moves the active leaf back to
the last provider-safe ancestor, writes neutral terminal tool results on a new
branch, and leaves the old branch readable as historical ledger state. Active
turn lease reconciliation is owned by this layer as well: once durable evidence
shows that a turn is failed, cancelled, interrupted, or terminal, the harness
must complete the terminal ledger state and release the lease before admitting
the next turn.

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
turn. It runs the compaction pipeline once and returns status, metrics, safe
observations, activity timeline, and opaque provider state. The host persists
opaque envelopes unchanged and must not rebuild provider-visible history from
product messages, debug reports, or checkpoint internals.

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
`ReadSubAgentDetail` and `ListSubAgentDetailEvents` are separate public host
APIs for parent-scoped, paginated inspection of the child journal. They let a
product UI show complete child execution detail while keeping parent model
context small. Their top-level activity timeline is rebuilt from retained child
detail events on each read so later tool results and pending-tool settlements
replace earlier running updates; row events remain the ordered child journal.
Their top-level context block carries neutral model-bound context facts:
provider/model identity, model-derived context policy, current pressure/usage
status, and public compaction operations. Fork mode and parent/child thread
identity affect lookup and journal ownership only; they do not define context
window size.

Hosts that only need thread lifecycle metadata should use `EnsureThread` and
`ThreadSummary`. `ReadThread` exposes transcript messages and should not be the
default integration point for UI bootstrapping or subagent inspection.
Hosts that need to reload the display projection for a known hosted turn should
use `ReadTurnProjection` with explicit `ThreadID`, `TurnID`, and `RunID`. This
keeps execution identity host-supplied while Floret rebuilds assistant text,
control-signal segments, and activity timelines from durable detail.

Closing a subagent stops current child execution and queued work. It preserves
the child thread and journal so the host can still read detail after close,
timeout, restart, or terminal completion.

This layer does not define product roles such as reviewer or worker beyond
opaque labels. Hosts own prompt policy, permissions, UI, and product workflow.

# Key Source Files

* [Runtime Host](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Agent Harness](/internal/agentharness/harness.go)
* [Engine](/internal/engine/engine.go)
