---
type: Architecture
title: Host Capability Authority
description: Identity-bound v2 handles prevent authority mismatch by construction.
resource: /runtime/host_v2.go
tags: [architecture, authority, capabilities]
timestamp: 2026-07-29T00:00:00Z
---

# Host Capability Authority

`runtime.Host` is the only public capability issuer. Each issuance method fixes
the root thread, parent thread, create intent, Agent, or exact recovery target
before returning a concrete narrow handle. A handle method cannot select a
different authority and its request omits the bound identity.

Host and every issued handle share one lifetime fence. Closing Host prevents
new work, joins active runtime finalization, and closes the owned Backend.

Creation, ordinary fork, SubAgent publication, and deletion use explicit
durable operation identity and replay checks. Recovery binds the complete
target, including turn/run/tool identity or interrupted owner generation, before
settlement. Root capabilities reject parent-owned threads; SubAgent access is
validated against its exact parent.

The store-wide `ThreadInventory` is limited to bounded root discovery for
startup reconciliation. It does not grant run, mutation, or product visibility
authority.

Canonical read completeness does not widen authority. `ThreadReader` can read
every Floret-owned lifecycle projection only for its bound root.
`SubAgentReader` selects descendants only under its bound parent, while child
identity remains explicit because it is not the authority already fixed by the
handle. Active approvals, todos, retries, and pending-work settlement stay on
the Agent-bound execution handle instead of a provider-free mutation facade.

No public aggregate can reissue Host authority. Public constructors returning
capabilities are limited to `runtime.Open` and `runtime.NewAgent`.
