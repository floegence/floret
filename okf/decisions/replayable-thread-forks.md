---
type: Architecture Decision
title: Replayable Thread Forks
description: Public thread forks persist one immutable operation plan and execute exact marked targets.
resource: /internal/agentharness/fork_operation.go
tags: [decision, runtime, storage, fork]
timestamp: 2026-07-15T00:00:00Z
---

# Decision

Every public thread fork requires a `ForkOperationID`. Floret saves an immutable
plan before creating any target. The plan pins the source leaf, parent and
terminal-child destinations, turn/run identity rewrites, and child metadata
patches. Each target is identified by an exact operation/node marker.

# Reason

A cross-store host cannot distinguish a lost response from an uncommitted fork
by checking whether a destination thread happens to exist. Regenerating IDs or
reading a newer source leaf during retry can also create duplicate identities or
a semantically different copy. A durable plan makes retry behavior deterministic
across process restarts.

# Consequences

The memory and SQLite stores implement a dedicated fork-operation contract.
The current SQLite schema stores operation state, request fingerprint, plan,
terminal result or error, timestamps, and thread markers. A matching prepared
operation executes only missing marked nodes; a matching completed operation verifies all
targets and returns its stored result. Request reuse, unrelated destinations,
marker mismatch, and missing completed targets fail explicitly. Generic
metadata is not part of this protocol, and hosts coordinate their own product
records separately.

# Related

* [Runtime API](../api/runtime.md)
* [Runtime Layers](../architecture/runtime-layers.md)
* [Public API Boundary](public-api-boundary.md)
