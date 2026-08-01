---
type: Architecture Decision
title: V3 Public API Boundary
description: Use one Gateway, one domain kernel, immutable Agents, bound threads, revision reads, and strict subscriptions.
resource: /README.md
tags: [decision, public-api, v3]
timestamp: 2026-07-29T00:00:00Z
---

# Decision

The v3 module uses semantic import path `/v3`. Ordinary applications use
`identity`, `config`, `runtime`, `observation`, `tools`, official `provider`
constructors, and opaque `storage.Source` values. Provider transports and
`storage/spi` are advanced integration surfaces; `florettest` is test-only.

All model execution uses `provider.Gateway`. All durable implementations use
the same domain kernel over `storage.Backend`. `runtime.Agent` is immutable.
`runtime.Host` remains at the composition root and issues bound thread handles.
The composition root converts them to read, lifecycle, execution, compaction,
and SubAgent capability interfaces before injection into services. Initial
queries use one atomic `ThreadReader.Bootstrap`; subscriptions are linearized
pull streams with explicit Gap resynchronization.

Receipt-first admission persists one immutable execution plan. Execution accepts
only the receipt and ephemeral `ExecutionContext`; canonical input is never
resubmitted by the host. Authoritative projections carry revision/provenance,
while offline derivation uses a distinct result type.

Production lifecycle identities are Floret-allocated. Deterministic injection
is test-only through `florettest.NewIDSource`. v3.0 direct methods remain deprecated
for one minor compatibility series; they are not the documented default and may
be removed only in the next major version.

Public additions require external-package tests, API baseline review, README
and OKF updates, changelog entry, backend conformance where relevant, and a
blank-module adoption gate. Green compatibility tooling is not design approval.
