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

1. Open one `runtime.Store`; choose `NewMemoryStore` for deterministic tests or
   inspect, explicitly migrate when needed, verify, and call
   `OpenSQLiteStore` with the exact inspection-bound request for durable facts.
2. Call `ConfigureHostCapabilities` once and retain only the binders needed by
   this application. Do not pass `HostBootstrap` beyond its callback.
3. Bind creation to an absent `ThreadID` and `CreateIntentID`, create the
   thread, then bind turn and read capabilities to that exact thread.
4. Give coordinators only the bound factory or handle for their task. Keep the
   Store and unbound binders private to the composition root.
5. Close the Store once, after application work has stopped.

# Verify

Run the [minimal durable host example](/cmd/examples/minimal-durable-host) and
test the integration with [`florettest`](/florettest/doc.go). The complete
capability contract is documented in the [`runtime` API](../api/runtime.md).

# Boundary

Floret owns admitted conversation, turn/run lifecycle, approvals, projections,
Agent todos, and provider state. The host owns users, routing, credentials,
product metadata, policy, UI, and retention decisions; it does not mirror
Floret's Agent state into a second query model.
