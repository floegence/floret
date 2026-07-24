---
type: Adoption Workflow
title: Render a Turn Projection
description: Render responsive live Agent activity while reloading canonical durable state from Floret.
resource: /runtime/thread_turn_projection.go
tags: [workflow, adoption, projection, events, ui]
timestamp: 2026-07-24T00:00:00Z
---

# Goal

Build a polished thread UI that updates immediately during a turn and converges
to Floret's canonical state after reconnect, dropped events, or process restart.

# Steps

1. Bootstrap from `ListThreadTurns`, `ReadThread`, `ReadApprovalQueue`, todo and
   context reads, then render `ThreadTurnProjection` segments in their supplied
   order.
2. Treat `runtime.Event` and `observation.Event` as transient hints. Validate
   each event and apply live projection updates only to matching thread, turn,
   and run identities.
3. Compare `ThroughOrdinal` only within those exact identities to discard stale
   or duplicate projections. Do not order by `ProjectedAt`.
4. Give waiting approvals and user-input requests clear primary actions; show
   running, failed, cancelled, pending, and completed tool states distinctly.
   Preserve stable row geometry while text and progress change.
5. On reconnect, gaps, validation failure, or unavailable terminal projection,
   reload the durable public snapshots/projections from Floret. Do not replay a
   stored event stream or rebuild activity from product audit records.

# Verify

Test fake-provider streaming, dropped/duplicated events, reconnect, approval,
cancellation, unavailable projection, and narrow/mobile layouts. The durable
contracts are documented in the [`runtime` API](../api/runtime.md), and event
lifetime is defined by [Observation and Events](../architecture/observation-events.md).

# Boundary

Floret owns product-neutral projection order and Agent lifecycle facts. The host
owns components, typography, spacing, responsive layout, localized copy,
accessibility, focus behavior, and product navigation; it does not persist a
second Agent lifecycle to support rendering.
