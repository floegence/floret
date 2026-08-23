---
type: Adoption Workflow
title: Render a Turn Projection
description: Render responsive live Agent activity while reloading canonical durable state from Floret.
resource: /runtime/thread_runtime.go
tags: [workflow, adoption, projection, events, ui]
timestamp: 2026-08-18T00:00:00Z
---

# Goal

Build a polished thread UI that updates immediately during a turn and converges
to Floret's canonical state after reconnect, dropped events, or process restart.

# Steps

For the typed v4 `ThreadService`, render `ThreadView.Items` exactly in supplied
ordinal order. Use `ThreadItem.ID` as the row key, update matching items in
place, and treat thinking segments like any other ordered item. Do not append
draft fields after the item list or sort by timestamps, kind, tool identity, or
arrival time.

1. Call `ThreadService.View` and render the ordered items in the returned view.
2. Treat runtime and observation events as transient hints. Apply only events
   matching the current thread, turn, and run identity, and ignore stale view
   versions.
3. On a closed subscriber, a version gap, or an unavailable terminal item,
   reload `View` and `History`; never rebuild canonical state from a product
   audit stream.
4. At terminal settlement, use canonical ordered items as the text authority.
   Do not append `TurnResult.Output` as a second assistant item: it is the
   aggregate for the whole run, not a display segment.
5. Apply a canonical snapshot only when the view version captured before the
   read is still current; otherwise discard the stale snapshot and reload.
6. Give waiting approvals and user-input requests clear primary actions; show
   running, failed, cancelled, pending, and completed tool states distinctly.
7. Render the cumulative Activity presentation supplied for each stable tool
   item. Result facts may add status and output without removing safe display
   facts authored by the matching tool call.
8. Keep stable row geometry while text and progress change, including narrow
   layouts.

# Verify

Test fake-provider streaming, dropped and duplicated events, reconnect,
approval, cancellation, unavailable terminal projection, and narrow/mobile
layouts. The durable contracts are documented in the [runtime API](../api/runtime.md).

# Boundary

Floret owns product-neutral projection order and Agent lifecycle facts. The host
owns components, typography, spacing, responsive layout, localized copy,
accessibility, focus behavior, and product navigation; it does not persist a
second Agent lifecycle to support rendering.
