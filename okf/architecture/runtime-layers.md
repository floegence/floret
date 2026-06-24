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
runs turns, retries, completes pending tool work, manages durable child threads,
deletes thread data, and returns host-safe snapshots.

`HostOptions.ModelGateway` lets a host route hosted parent turns, child turns,
and hosted title generation through product-owned model transport while Floret
still owns request construction, provider loop control, ledgers, tool dispatch,
and runtime events.

`AgentHarness` is the internal durable conversation layer. It owns threads,
parent-child thread lifecycle, turn lifecycle, retries, forks, titles, and
projection of an active journal path into one engine execution.
Durable compaction entries are committed only after `Engine` has rebuilt and
validated the compacted provider request, so a journal checkpoint is an
installed continuation boundary rather than a candidate summary.

`Engine` is the prompt-first single-run executor. It owns provider loop control,
tool invocation, compaction decisions, prompt-cache requests, metrics, and event
emission.

# Projected Turns

`runtime.RunProjectedTurn` supports hosts that already own durable conversation
rows. The host supplies a provider-visible transcript projection, while Floret
still owns the loop, local tools, context pressure, ledgers, and events. It uses
the same `runtime.ModelGateway` contract as hosted threads when the host supplies
one.

Active manual compaction in projected turns flows through
`ProjectedTurnOptions.ManualCompactions`. The host owns the user-facing command
or policy that creates a request; `Engine` owns polling at safe provider-loop
points, summary generation, checkpoint installation, and continuation of the
same run.

Idle projected compaction uses `runtime.CompactProjectedContext` instead of
pretending to be a user turn. It runs the compaction pipeline once and returns
the compacted active transcript plus lifecycle observations so the host can
persist a thread-level checkpoint in its own store.

# Child Threads

Hosted subagents are durable child threads managed by `AgentHarness` and exposed
through `runtime.Host`. A parent can spawn, send input to, wait for, list, and
close child threads. The child runs as a normal Floret thread with its own
`ThreadID`, `TurnID`s, prompt scope, provider request ledger, and journal.
Queued input is represented in that journal as Floret lifecycle state, not as an
in-memory host queue, so restart recovery, wait semantics, and close semantics
derive from the same durable source.

Waiting for child threads is intentionally separate from reading child detail.
`WaitSubAgents` returns bounded snapshots and timeout state; it does not return
the child transcript, tool outputs, or detail timeline. `ReadSubAgentDetail` and
`ListSubAgentDetailEvents` are separate public host APIs for parent-scoped,
paginated inspection of the child journal. They let a product UI show complete
child execution detail while keeping parent model context small.

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
