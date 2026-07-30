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
`runtime.Host` remains at the composition root and exposes only bound thread
operations plus shutdown. Queries use exact thread revisions; subscriptions are
linearized pull streams with explicit Gap resynchronization.

v2 capability handles, caller-assigned lifecycle identities, unbound DTOs,
runtime legacy decoders, aliases, and fallback contracts are deleted rather
than deprecated. Older APIs remain only in their published tags.

Public additions require external-package tests, API baseline review, README
and OKF updates, changelog entry, backend conformance where relevant, and a
blank-module adoption gate. Green compatibility tooling is not design approval.
