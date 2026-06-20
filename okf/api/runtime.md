---
type: Public API
title: runtime Package
description: The runtime package is the main public facade for hosted threads, projected turns, stores, events, and control signals.
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
* `ModelGateway` lets a host supply model transport while Floret owns loop
  control, tool dispatch, and ledgers.

# Boundaries

The host should treat `runtime.Store` as opaque. Product data such as owners,
workspace metadata, pinned state, billing, and read watermarks belongs in the
host database keyed by `runtime.ThreadID`.

# Key Source Files

* [Runtime Host](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Core Control Helpers](/runtime/control.go)
