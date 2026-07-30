---
type: Architecture
title: Host Capability Authority
description: Bound v3 thread handles prevent authority mismatch by construction.
resource: /runtime/v3_host.go
tags: [architecture, authority, capabilities]
timestamp: 2026-07-29T00:00:00Z
---

# Host Capability Authority

`runtime.Host` exposes only `Threads`, `Thread`, and `Shutdown`. `Host.Thread`
binds one exact `ThreadID`; `Thread.Turns`, `Thread.SubAgents`, `Child`, and
`DescendantReader` derive narrower authority without accepting the same identity
again. Commands cannot select a different bound thread.
`Thread.Compact` uses that same exact-thread authority. `SubAgents.WaitSubAgents`
and `CloseSubAgent` remain restricted to direct children of the bound parent.

Host and every issued handle share one lifetime fence. Shutdown prevents new
work, cancels and joins managed execution, and closes owned storage.

Creation, ordinary fork, SubAgent publication, and deletion use explicit
durable operation identity and replay checks. Recovery binds the complete
target, including turn/run/tool identity or interrupted owner generation, before
settlement. Root capabilities reject parent-owned threads; SubAgent access is
validated against its exact parent.
The public recovery path preserves that order: `Thread` or `Child` first issues
a `PendingToolRecovery` or `InterruptedTurnRecovery` bound to the exact target
or proof, and only that narrower handle may settle or recover it.

`Threads.ListThreads` is a bounded batch snapshot for product inventory. It does
not grant mutation authority. Canonical read completeness does not widen
authority: exact-thread queries and subscriptions never aggregate child
execution. `Child` exposes detail and pending-target reads for one direct child;
`DescendantReader` exposes turn and artifact reads for one validated descendant
at any depth. Both remain scoped beneath their bound parent and fail closed for
unrelated, deleted, or corrupt ancestry.
Root overview, turn, todo, context, approval, projection, pending-target, and
direct-child queries remain methods on the exact bound `Thread`; direct-child
turn queries remain methods on the exact bound `Child`.

No public aggregate can recover or reissue composition-root authority.
