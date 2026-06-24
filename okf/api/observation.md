---
type: Public API
title: observation Package
description: The observation package provides host-facing DTOs derived from sanitized runtime events.
resource: /observation/doc.go
tags: [api, observation]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

`observation` turns selected sanitized runtime facts into UI-neutral DTOs.
Hosts can render progress, context pressure, compaction lifecycle, and activity
summaries without parsing assistant text or depending on implementation types.

# Main Entry Points

* `BuildActivityTimeline` creates a stable activity summary.
* `ContextStatusesFromObservations` combines projected request, provider usage,
  and event-derived context status.
* `CompactionEventsFromEvents` extracts compaction lifecycle facts.

`CompactionEvent` carries `OperationID` for one compaction attempt and optional
`RequestID` / `Source` when a downstream host requested manual compaction. Start,
complete, and failed observations for the same manual request keep the same
operation and request correlation so host UIs can update one progress item
instead of guessing from trigger text.

# Boundary

Observation records are not raw debug traces. They intentionally omit local
paths, tool arguments, tool results, provider payloads, and reasoning.

# Key Source Files

* [Observation Package](/observation/doc.go)
* [Activity Timeline](/observation/activity.go)
* [Context Status](/observation/context.go)
* [Compaction Events](/observation/compaction.go)
