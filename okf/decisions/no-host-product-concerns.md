---
type: Architecture Decision
title: No Host Product Concerns in Core
description: Floret core remains product-neutral; host UI, policy, credentials, workspace state, and product-specific modes stay downstream.
resource: /AGENTS.md
tags: [decision, host-boundary, architecture]
timestamp: 2026-06-20T00:00:00Z
---

# Decision

Floret core must not encode downstream product concerns. Product-specific modes,
approval UI, workspace policy, provider credential storage, and durable product
metadata stay in the host.

# Reason

Floret is a reusable agent engine. Keeping product policy outside core preserves
small contracts, reusable runtime behavior, deterministic tests, and a stable
host integration boundary.

# Consequences

When a downstream host needs a new behavior, first decide whether it is a
general agent-engine contract. Product-specific behavior should stay in the
host or be exposed through host-provided tools and public runtime options.

# Related

* [Boundaries](../architecture/boundaries.md)
