---
type: Architecture Decision
title: Replayable Thread Forks
description: Public thread forks use one canonical request identity and exact source leaf.
resource: /runtime/thread_runtime.go
tags: [decision, runtime, storage, fork]
timestamp: 2026-08-18T00:00:00Z
---

# Decision

Every public thread fork carries a stable request key and fingerprint. Floret
validates the source thread and leaf, appends the canonical fork facts, and
returns the durable target projection. A replay with the same identity returns
the existing result; a changed source, target, or metadata request fails with a
conflict instead of creating another thread.

# Reason

A lost response must be distinguishable from a different fork request without
creating a second durable identity. The canonical journal and request authority
provide that distinction while keeping one owner for thread lifecycle state.

# Consequences

The runtime service uses the shared session-tree domain kernel for both memory
and physical backends. Hosts may keep product routing or authorization facts,
but must not persist a second fork lifecycle or reconstruct the target from a
product audit stream. A fork copies visible conversation history, but never
copies `EntryEffectAttempt`: effect authority remains bound to the source
thread. Removing those entries reconnects retained parents and recomputes the
destination journal depth and leaf before the fork commits.
Historical Turn and Run identities remain unchanged. Only the destination
Thread identity and entry references are remapped. Context payloads carry no
second execution identity; context snapshots reconstruct identity from their
canonical entries, including direct forks and forks of forks.

# Related

* [Runtime API](../api/runtime.md)
* [Runtime Layers](../architecture/runtime-layers.md)
* [Public API Boundary](public-api-boundary.md)
