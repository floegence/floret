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
* `SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`, and
  `CloseSubAgent` manage durable child threads under a hosted parent thread.
* `ModelGateway` lets a host supply model transport while Floret owns loop
  control, tool dispatch, and ledgers.
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

Child `ThreadID` is the lifecycle target for spawn, send, wait, list, and close.
Task names and agent paths are reference metadata and may repeat. Queued child
inputs are journal entries in the child thread, so host restart and storage
backends preserve pending work, cancellation, and consumption state.

`StreamObservation` is for host rendering and diagnostics. It is not raw
provider wire data and must not carry prompt text, tool arguments, tool results,
local paths, or secrets.

# Key Source Files

* [Runtime Host](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Core Control Helpers](/runtime/control.go)
