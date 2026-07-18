---
type: Architecture Decision
title: Public API Boundary
description: Floret exposes only a compact set of stable downstream packages and keeps implementation contracts internal.
resource: /README.md
tags: [decision, public-api, boundary]
timestamp: 2026-06-20T00:00:00Z
---

# Decision

Production downstream projects integrate through `config`, `runtime`, `tools`,
and `observation`. Implementation packages remain under `internal/`.

# Reason

The boundary lets Floret evolve provider loops, storage implementation,
compaction, prompt cache, testing harnesses, and event internals without making
those details downstream contracts.

# Consequences

New host-facing capabilities need public API, tests, README guidance, and OKF
updates. Contributor-facing documentation may explain internals, but downstream
examples must use public packages.
Durable cross-store coordination uses explicit public operation identities and
results; downstream hosts must not import internal storage contracts.
Runtime constructors return concrete facade pointers. Interface ownership stays
with the caller, which declares the smallest capability set needed by each
responsibility instead of inheriting a broad framework interface.

The durable runtime facade is intentionally split at lifecycle boundaries.
`HostRuntime` is an opaque bootstrap token; it has no exported methods and must
not be retained by a coordinator or run. `ThreadCreateHost`, `ThreadTitleHost`,
`ThreadForkHost`, `ThreadDeleteHost`, `ThreadReadHost`,
`SubAgentMaintenanceHost`, and `PendingToolSettlementHost` each expose only
their named operation. `Host` retains read and active execution methods but no
top-level create, title, fork, delete, or bulk child-close operation. This makes
canonical Agent lifecycle ownership visible in method sets instead of relying
on a downstream caller to ignore methods on a shared Store or facade.

# Related

* [Boundaries](../architecture/boundaries.md)
* [Change Public API](../workflows/change-public-api.md)
