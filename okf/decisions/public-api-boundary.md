---
type: Architecture Decision
title: V2 Public API Boundary
description: Use one Gateway, one Backend kernel, immutable Agents, and identity-bound Host handles.
resource: /README.md
tags: [decision, public-api, v2]
timestamp: 2026-07-29T00:00:00Z
---

# Decision

The v2 production packages are `config`, `provider`, `runtime`, `storage`,
`tools`, and `observation`; `florettest` is test-only. The module uses semantic
import path `/v2`.

All model execution uses `provider.Gateway`. All durable implementations use
the same domain kernel over `storage.Backend`. `runtime.Agent` is immutable.
`runtime.Host` remains at the composition root and issues identity-bound narrow
handles.

v1 bootstrap, binder, factory, Host-options, Store, fake provider, generator,
automatic migration, and adapter fallback contracts are deleted rather than
aliased or deprecated. v1 is preserved only by its Git tag.

Public additions require external-package tests, API baseline review, README
and OKF updates, changelog entry, backend conformance where relevant, and a
blank-module adoption gate. Green compatibility tooling is not design approval.
