---
type: Public API
title: observation Package
description: The observation package provides host-facing DTOs derived from sanitized runtime events.
resource: /observation/doc.go
tags: [api, observation]
timestamp: 2026-06-25T00:00:00Z
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
* `CompactionDebugEventsFromEvents` extracts safe compaction diagnostic facts.

`BuildActivityTimeline` owns the product-neutral terminal semantics for activity
items within one sanitized observation group. When a sanitized `run_end`
observation is present, cancelled runs produce `canceled` unresolved items and
failed runs produce `error` unresolved items. `runtime.ProjectThreadTurn`
applies terminal turn markers across all activity timeline segments for the
turn, so completed, failed, and cancelled terminal projections do not leave
pending or running activity behind. Host-owned pending work that finishes later
must be reported through the runtime `SettlePendingTool` API, which updates the
original activity item instead of creating a separate UI row.
Tool approval events are lifecycle updates on the tool activity item itself:
`tool_call`, approval request/resolution, and `tool_result` for the same tool id
collapse into one item instead of a separate approval row. A requested approval
therefore keeps the tool item `waiting`, blocking, and attention-worthy; approval
or denial then advances that same item.
Terminal tool-result settlements remove running-only pending metadata, payload
fields, and chips so downstream hosts do not carry stale active state into
terminal UI.

Tool result duration is part of the activity lifecycle fact. When a terminal
tool result carries a positive duration and the current item start is missing or
later than `ended_at - duration`, `BuildActivityTimeline` expands the interval
back to that duration-derived start. Activity validation rejects items whose
`ended_at_unix_ms` is earlier than `started_at_unix_ms`, so hosts never receive
negative or append-time-only execution intervals as valid activity facts.

`CompactionEvent` carries `OperationID` for one compaction attempt and optional
`RequestID` / `Source` when a downstream host requested manual compaction. Start,
complete, failed, and cancelled observations for the same manual request keep the
same operation and request correlation so host UIs can update one progress item
instead of guessing from trigger text.

`CompactionDebugEvent` carries the same `OperationID` / `RequestID` correlation
plus stage and status values for the compaction pipeline. Stages identify
poll, begin, preflight, generation attempts, projected request rebuilds, request
validation, and installation. Status may be running, ok, retrying, failed, or
cancelled. Debug observations may include token pressure, message counts,
duration, provider-state kind, the next action, and sanitized error text, but
they do not include prompt text, generated summary content, local paths, or raw
manual request strings. A failed compaction that stops before summary generation
still emits a terminal `preflight` debug observation so hosts can distinguish
configuration or circuit breaker failures from provider and validation failures.

# Boundary

Observation records are not raw debug traces. They intentionally omit local
paths, secrets, tool arguments, tool results, provider payloads, and reasoning.

# Key Source Files

* [Observation Package](/observation/doc.go)
* [Activity Timeline](/observation/activity.go)
* [Context Status](/observation/context.go)
* [Compaction Events](/observation/compaction.go)
* [Compaction Debug Events](/observation/compaction_debug.go)
