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
items within one sanitized observation group. Only explicit terminal `run_end`
observations settle unresolved items: cancelled runs produce `canceled`
unresolved items, failed runs produce `error` unresolved items, and successful
runs settle only work that is not host-owned pending work or unresolved
approval. Waiting, started, queued, checkpoint, or otherwise non-terminal
lifecycle observations do not imply tool completion. `runtime.ProjectThreadTurn`
applies failed and cancelled terminal markers across all activity timeline
segments for the turn. Successful turns keep host-owned pending work running
until the host reports the observed outcome through the runtime
`SettlePendingTool` API, which updates the original activity item instead of
creating a separate UI row.
Tool approval events are lifecycle updates on the tool activity item itself:
`tool_call`, approval request/resolution, and `tool_result` for the same tool id
collapse into one item instead of a separate approval row. A requested approval
therefore keeps the tool item `waiting`, blocking, and attention-worthy; approval
or denial then advances that same item. `requires_approval` records that the
tool invocation passed through approval; it is not the current decision-needed
flag. Only `approval_state=requested` with `status=waiting` means the item is
currently waiting for a decision. After approval, the item may be `pending`
before dispatch starts, then `running` or terminal when later lifecycle facts
arrive.
For local tools, `tool_call` is the queued model request and remains `pending`
until `tool_dispatch_started` records that Floret has passed validation,
permission, and approval gates and is about to invoke the handler. Batched
sibling calls that have not reached dispatch therefore stay pending while an
earlier sibling waits for approval.
While a dispatched local tool is still running, `tool_activity_updated` may
refresh the same item with sanitized public presentation payload and metadata.
Those updates preserve the item identity, start time, approval lifecycle, and
command label, and they never turn a terminal item back into running. The
terminal lifecycle fact remains `tool_result` or an explicit terminal run
settlement.
Terminal tool-result settlements remove running-only pending metadata, payload
fields, and chips so downstream hosts do not carry stale active state into
terminal UI.

`ActivityPresentation.Payload` is host-supplied public display data attached to
the product-neutral renderer and lifecycle fact. Floret validates that payload
shape for safe transport and preserves it through live and durable activity
projection, but it does not define tool-specific UI layout, copy, grouping, or
field priority. Downstream hosts remain responsible for interpreting terminal,
file, patch, web-search, question, completion, todo, and other renderer payloads
as product presentation.
When a local tool fails before handler dispatch and the host/tool handler did
not provide public result activity, the engine supplies a neutral error payload
on the result activity: `status=error` and `error.message` contain the sanitized
provider-visible framework failure reason. The result activity reuses the call
activity renderer when one was available, otherwise it uses `structured`. This
keeps framework-layer failures such as invalid arguments, denied approval,
resource extraction errors, and pre-dispatch panic recovery visible in activity
timelines without introducing downstream product UI semantics or treating
arbitrary tool output as public display copy.

Tool result duration is part of the activity lifecycle fact. When a terminal
tool result carries a positive duration, `BuildActivityTimeline` uses
`ended_at - duration` as the execution start instead of including queued or
approval wait time. Activity validation rejects items whose
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
