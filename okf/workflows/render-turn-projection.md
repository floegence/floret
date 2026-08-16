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

For the typed v4 `ThreadService`, render `ThreadView.Items` exactly in supplied
ordinal order. Use `ThreadItem.ID` as the row key, update matching items in
place, and treat `ThreadItemThinking` like any other ordered segment. Do not
append deprecated `AssistantDraft` or `ThinkingDraft` after the item list, and
do not sort by timestamp, kind, tool identity, or arrival time. `View`,
`Subscribe`, `History`, and reopen all derive the same order from Floret.

1. Call `ThreadReader.Bootstrap`, retain its revision and page cursor, then
   render canonical `ThreadTurnProjection` segments in their supplied order.
2. Treat `runtime.Event` and `observation.Event` as transient hints. Validate
   each event and apply `ProjectionDelta` with
   `ApplyThreadTurnProjectionDelta` only to matching thread, turn, run, and
   trace identities.
3. For a running delta, require `BaseThroughOrdinal` to match the locally
   applied `ThroughOrdinal`. Base-zero deltas are self-contained initial or
   terminal checkpoints and replace the local projection lineage. Do not order
   by `ProjectedAt` and do not guess across any other ordinal gap.
4. Give waiting approvals and user-input requests clear primary actions; show
   running, failed, cancelled, pending, and completed tool states distinctly.
   Preserve stable row geometry while text and progress change.
5. On reconnect, gaps, validation failure, or unavailable terminal projection,
   bootstrap again and reload authoritative projections from Floret. Do not replay a
   stored event stream or rebuild activity from product audit records.
6. Use `ThreadReader.ReadTurn` for a known canonical `TurnID`; use `ThreadReader.ListTurns`
   for bootstrap and history pagination. Validate returned snapshots before
   mapping them into host presentation state.

# Verify

Test fake-provider streaming, dropped, duplicated, and out-of-order deltas,
reconnect, approval, cancellation, unavailable projection, and narrow/mobile layouts. The durable
contracts are documented in the [`runtime` API](../api/runtime.md), and event
lifetime is defined by [Observation and Events](../architecture/observation-events.md).

# Boundary

Floret owns product-neutral projection order and Agent lifecycle facts. The host
owns components, typography, spacing, responsive layout, localized copy,
accessibility, focus behavior, and product navigation; it does not persist a
second Agent lifecycle to support rendering.

Use `ThreadReader.ReadAuthoritativeProjection` for application state. Use
`DeriveThreadTurn` only for offline analysis or tests; its derived provenance is
not canonical authority.
