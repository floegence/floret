---
type: Architecture Boundary
title: Floret And Host Boundary
description: Defines the single-source-of-truth and composition ownership rules for v2.
resource: /AGENTS.md
tags: [architecture, boundary, host]
timestamp: 2026-07-29T00:00:00Z
---

# Boundary

Floret owns provider-loop execution, tool dispatch, permission/resource/effect
and approval lifecycle, durable journals, canonical turns and projections,
titles, Agent todos, artifacts, context and compaction, SubAgent trees, pending
work settlement, opaque provider state, prompt cache, and runtime observation.

The host owns UI, credentials, provider routing, product authorization,
workspace policy, product resources, unadmitted commands, security audit,
transport diagnostics, and cross-store product intent.

The host must not persist a second queryable Agent lifecycle, provider-visible
message history, admitted reference mapping, approval queue, Todo state, or
SubAgent state. It rebuilds presentation from canonical reads. Ordered
`MessageReference` values are durable opaque facts; rich current-turn-only
material belongs in `SupplementalContext`.

Only the composition root retains `*runtime.Host`. It immediately hands local
services minimal interfaces or closures backed by identity-bound handles.
Services, ordinary runs, terminal processes, and SubAgent executors neither
retain Host nor recover it with type assertions.

Downstream production imports only `config`, `provider`, `runtime`, `storage`,
`tools`, and `observation`. `florettest` is test-only; `internal/*` is never a
host dependency.
