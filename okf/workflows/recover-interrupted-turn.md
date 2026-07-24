---
type: Adoption Workflow
title: Recover an Interrupted Turn
description: Recover one exact Floret turn authority and settle host-owned pending work after restart.
resource: /cmd/examples/startup-recovery/main.go
tags: [workflow, adoption, recovery, authority]
timestamp: 2026-07-24T00:00:00Z
---

# Goal

Resume or reconcile an interrupted turn after process loss without fabricating
identity, following replacement authority, or replaying a domain side effect.

# Steps

1. Open the Store with the same validated lease policy used before interruption.
2. Configure the interrupted-turn recovery binder at the composition root and
   bind one exact root turn or parent-child turn owner.
3. Let Floret validate canonical admission, finish ledgers, lease generation,
   heartbeat, and expiry before creating recovery authority.
4. Handle typed not-found, resolved, busy, and invariant outcomes. Never poll
   around a busy thread or choose a different unfinished turn heuristically.
5. If the exact turn is terminal, use its canonical result/read model before
   settling any remaining host-owned pending work through the bound recovery
   capability. Reconcile external side effects by their host idempotency key.

# Verify

Run the [startup recovery example](/cmd/examples/startup-recovery) and the
terminal outcome contracts in [`florettest`](/florettest/doc.go). See the
[`runtime` API](../api/runtime.md) for exact authority outcomes.

# Boundary

Floret owns turn admission, execution identity, lease proof, canonical terminal
state, and pending-tool settlement facts. The host owns external job state and
domain idempotency; it must not infer recovery from an audit stream or rewrite
the Floret journal.
