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
runs turns, retries, completes pending tool work, deletes thread data, and
returns host-safe snapshots.

`AgentHarness` is the internal durable conversation layer. It owns threads,
turn lifecycle, retries, forks, titles, and projection of an active journal path
into one engine execution.

`Engine` is the prompt-first single-run executor. It owns provider loop control,
tool invocation, compaction decisions, prompt-cache requests, metrics, and event
emission.

# Projected Turns

`runtime.RunProjectedTurn` supports hosts that already own durable conversation
rows. The host supplies a provider-visible transcript projection, while Floret
still owns the loop, local tools, context pressure, ledgers, and events.

# Key Source Files

* [Runtime Host](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Agent Harness](/internal/agentharness/harness.go)
* [Engine](/internal/engine/engine.go)
