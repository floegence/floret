---
type: Adoption Workflow
title: Compose a Durable Host
description: Construct one Store-owned composition root and distribute only exact authority-bound Floret capabilities.
resource: /cmd/examples/minimal-durable-host/main.go
tags: [workflow, adoption, composition-root, runtime]
timestamp: 2026-07-24T00:00:00Z
---

# Goal

Run one durable Agent thread while keeping Store lifetime and capability
issuance at the application's composition root.

# Steps

1. Generate a host-owned composition with `cmd/floret-host-init`, choosing the
   smallest profile that fits: `memory`, `durable-basic`, `approval`,
   `subagent`, or `production-recovery`. Review the dry-run before `--write`.
2. Open one `runtime.Store`; choose `NewMemoryStore` for deterministic tests or
   use `StartSQLiteStore` for the normal durable startup state machine. Keep
   lower-level inspect, migrate, verify, and exact open for operator workflows.
3. Call `ConfigureHostCapabilities` once and retain only the binders needed by
   this application. Do not pass `HostBootstrap` beyond its callback.
4. Bind creation to an absent `ThreadID` and `CreateIntentID`, create the
   thread, then bind turn and read capabilities to that exact thread.
5. Give coordinators only the generated local interface, bound factory, or
   handle for their task. Keep the Store and unbound binders private to the
   composition root.
6. Close the Store once, after application work has stopped.

`durable-basic` proves catalog provenance and blocks with a local typed recovery
error when interrupted work exists; it is not automatic production recovery.
`production-recovery` recursively discovers every direct-child edge, recovers
exact root or parent-child authority, reads canonical typed pending settlement
targets, and requires host reconciliation before readiness.

# Verify

Run the [minimal durable host example](/cmd/examples/minimal-durable-host) and
the generated smoke plus profile-specific behavior tests, then test advanced integration with
[`florettest`](/florettest/doc.go). The complete
capability contract is documented in the [`runtime` API](../api/runtime.md).

# Boundary

Floret owns admitted conversation, turn/run lifecycle, approvals, projections,
Agent todos, and provider state. The host owns users, routing, credentials,
product metadata, policy, UI, and retention decisions; it does not mirror
Floret's Agent state into a second query model.
