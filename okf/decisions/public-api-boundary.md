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
Runtime constructors return concrete capability pointers. Interface ownership
stays with the caller, which declares the smallest capability set needed by
each responsibility instead of inheriting a framework interface.

The durable runtime capability surface is intentionally split at lifecycle boundaries.
`HostBootstrap` is an opaque composition-root token; it has no exported methods
and must not be retained by a coordinator or run. It issues exact factories,
whose sole `NewHost` method can create only the corresponding bound capability.
Provider-backed work is split into a
thread-bound `TurnExecutionHost`, a thread-bound `ThreadCompactionHost`, and a
parent-bound `SubAgentHost`. Provider-free `ThreadCreateHost`,
`ThreadTitleHost`, `ThreadForkHost`, `ThreadDeleteHost`, `ThreadReadHost`,
parent-bound `SubAgentMaintenanceHost`, and thread- or parent-bound
`PendingToolSettlementHost` expose only their named authority. Every bound
request keeps its explicit identity and fails on a mismatch. This makes
canonical Agent lifecycle ownership visible in method
sets and authority identities instead of relying on a downstream caller to
ignore methods on a shared Store or facade.

`ParentThreadID` means SubAgent ownership only. Ordinary fork lineage is stored
only in `ForkedFromThreadID`; child ownership metadata is written atomically
with a child fork. Root capabilities reject parent-owned threads, and root
deletion follows only the SubAgent ownership tree.

Authority changes are also serialized as one lifecycle boundary. Root create,
ordinary fork, SubAgent spawn, and root tree delete share one Store-level gate;
delete retains that authority through descendant discovery and storage commit.
This prevents a narrow method set from being undermined by concurrent mutations
through another legitimate capability. SQLite does not trust that in-process
snapshot: its delete contract accepts only the root identity and derives the
current ownership tree again inside the write transaction.

# Related

* [Boundaries](../architecture/boundaries.md)
* [Change Public API](../workflows/change-public-api.md)
