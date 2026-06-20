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

# Related

* [Boundaries](../architecture/boundaries.md)
* [Change Public API](../workflows/change-public-api.md)
