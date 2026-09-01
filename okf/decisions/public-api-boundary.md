---
type: Architecture Decision
title: V7 Public API Boundary
description: Use one provider gateway, one domain kernel, immutable Agents, and one typed ThreadService.
resource: /README.md
tags: [decision, public-api, v7]
timestamp: 2026-08-18T00:00:00Z
---

# Decision

The v7 module uses semantic import path `/v7`. Ordinary applications use
`identity`, `config`, `runtime`, `observation`, `tools`, official `provider`
constructors, and opaque `storage.Source` values. Provider transports and
`storage/spi` are advanced integration surfaces; `florettest` is test-only.

All model execution uses `provider.Gateway`. All durable implementations use
the same domain kernel over `storage.Backend`. `runtime.Agent` is immutable.
`runtime.Host` remains at the composition root and issues one typed
`ThreadService`. That service owns durable thread journals, actor projections,
turn lifecycle, queue and interaction commands, effects, forks, and deletion
fences. Initial reads use `View` and `History`; subscriptions are observation
only and reconnect through a fresh `View` baseline after a gap or close.

Canonical user input is accepted into the session-tree journal before a command
reports success or provider work begins. Live views are replaceable projections
of that journal and bounded in-memory execution state; they are never a second
durable lifecycle. Provider continuation and host audit data remain outside
the Floret storage contract.

Public additions require external-package tests, API baseline review, README
and OKF updates, changelog entry, backend conformance where relevant, and the
published-release adoption gate. Green compatibility tooling is not design
approval.

`runtime.Options.StartupProgress` is the sole startup-presentation addition in
v7. It reports product-neutral migration and verification phases
synchronously. Storage rows, counts, schema versions, and downstream UI policy
remain outside the public API.
